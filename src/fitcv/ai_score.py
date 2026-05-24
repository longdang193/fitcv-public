"""@meta
name: ai_score
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.ai_score.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

import json
import hashlib
import logging
import os
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from types import SimpleNamespace

from fitcv.config import (
    get_gemini_model,
    get_ranking_prompt_id,
    get_stage_runtime_concurrency,
    get_stage_runtime_sleep_secs,
    resolve_model_routing_part,
    sqlite_mode_enabled,
)
from fitcv.contracts import RANKING_AI_SCORE_PROMPT_SCHEMA_VERSION
from fitcv.persistence import build_bigquery_client, get_local_sqlite_path
from fitcv.prompts import render_prompt
from fitcv.ranking_contract import (
    DEFAULT_FIT_LABEL_STRONG_THRESHOLD,
    DEFAULT_FIT_LABEL_STRETCH_THRESHOLD,
    VALID_FIT_LABELS,
    fit_label_from_score,
)

logger = logging.getLogger(__name__)

# ── constants ─────────────────────────────────────────────────────────────────
_DEFAULT_STRONG_THRESHOLD = DEFAULT_FIT_LABEL_STRONG_THRESHOLD
_DEFAULT_STRETCH_THRESHOLD = DEFAULT_FIT_LABEL_STRETCH_THRESHOLD
_VALID_FIT_LABELS = VALID_FIT_LABELS

def _extract_openai_responses_text(body: dict[str, Any]) -> str:
    """Extract assistant text from OpenAI-compatible /responses payloads."""
    direct = str(body.get("output_text") or "").strip()
    if direct:
        return direct
    output = body.get("output")
    if not isinstance(output, list):
        return ""
    chunks: list[str] = []
    for item in output:
        if not isinstance(item, dict):
            continue
        for content_item in item.get("content") or []:
            if not isinstance(content_item, dict):
                continue
            text = str(content_item.get("text") or "").strip()
            if text:
                chunks.append(text)
    return "\n".join(chunks).strip()


def _stable_json_fingerprint(payload: dict[str, Any]) -> str:
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


def build_ai_score_contract_fingerprint(config: dict[str, Any]) -> dict[str, Any]:
    thresholds = dict(config.get("fit_label_thresholds") or {})
    payload = {
        "gemini_model": get_gemini_model(config),
        "prompt_schema_version": RANKING_AI_SCORE_PROMPT_SCHEMA_VERSION,
        "prompt_id": get_ranking_prompt_id(config),
        "strong_threshold": float(thresholds.get("strong", DEFAULT_FIT_LABEL_STRONG_THRESHOLD)),
        "stretch_threshold": float(thresholds.get("stretch", DEFAULT_FIT_LABEL_STRETCH_THRESHOLD)),
    }
    return {
        "payload": payload,
        "fingerprint": _stable_json_fingerprint(payload),
    }


def build_ai_score_input_fingerprint(
    job: dict[str, Any],
    candidate_summary: str,
    top_evidence: list[str],
    config: dict[str, Any],
) -> dict[str, Any]:
    from fitcv.embeddings import build_job_summary_text

    thresholds = dict(config.get("fit_label_thresholds") or {})
    prompt = build_scoring_prompt(
        jd_summary=build_job_summary_text(job),
        candidate_summary=candidate_summary,
        top_evidence=top_evidence[:2],
        strong_threshold=float(thresholds.get("strong", DEFAULT_FIT_LABEL_STRONG_THRESHOLD)),
        stretch_threshold=float(thresholds.get("stretch", DEFAULT_FIT_LABEL_STRETCH_THRESHOLD)),
        config=config,
    )
    contract_record = build_ai_score_contract_fingerprint(config)
    payload = {
        "job_url": str(job.get("job_url") or ""),
        "prompt": prompt,
        "contract_fingerprint": contract_record["fingerprint"],
    }
    return {
        "payload": payload,
        "fingerprint": _stable_json_fingerprint(payload),
    }


# ── prompt construction ────────────────────────────────────────────────────────

def build_scoring_prompt(
    jd_summary: str,
    candidate_summary: str,
    top_evidence: list[str],
    *,
    strong_threshold: float = DEFAULT_FIT_LABEL_STRONG_THRESHOLD,
    stretch_threshold: float = DEFAULT_FIT_LABEL_STRETCH_THRESHOLD,
    config: dict[str, Any] | None = None,
) -> str:
    """Build the structured reranking prompt for one job.

    Inputs:
        jd_summary        : labelled-section text from build_job_summary_text()
        candidate_summary : brief candidate paragraph (skills, experience level)
        top_evidence      : optional top 0-2 candidate evidence chunk_text strings

    Returns:
        A prompt string with rubric embedded. Model must return JSON only.
    """
    evidence_section = ""
    if top_evidence:
        bullets = "\n".join(f"  - {e}" for e in top_evidence)
        evidence_section = f"\n\nTop matched candidate evidence:\n{bullets}"
    prompt_id = get_ranking_prompt_id(config or {})
    return render_prompt(
        prompt_id,
        {
            "jd_summary": jd_summary,
            "candidate_summary": candidate_summary,
            "evidence_section": evidence_section,
            "strong_threshold": strong_threshold,
            "stretch_threshold": stretch_threshold,
        },
    ).text


# ── response parsing ──────────────────────────────────────────────────────────


def parse_score_response(response_text: str, config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Parse and validate the model's JSON scoring response.

    Handles:
    - Valid JSON
    - Markdown-fenced JSON (```json ... ```)
    - Missing fit_label → derived from ai_score
    - Unknown fit_label → mapped to "skip"
    - Score outside [0, 1] → clamped
    - Malformed JSON → safe defaults (ai_score=0.0, fit_label="skip")

    Returns:
        Dict with keys: ai_score, fit_label, score_reasoning,
                        matched_strengths, key_risks
    """
    _defaults: dict[str, Any] = {
        "ai_score": 0.0,
        "fit_label": "skip",
        "score_reasoning": "",
        "matched_strengths": [],
        "key_risks": [],
    }

    # Strip markdown fences if present
    text = response_text.strip()
    fence_match = re.search(r"```(?:json)?\s*([\s\S]+?)\s*```", text)
    if fence_match:
        text = fence_match.group(1).strip()

    try:
        data = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        logger.warning("parse_score_response: malformed JSON — returning defaults")
        failed = _defaults.copy()
        failed["score_reasoning"] = "Scoring response parse failure: malformed_json"
        failed["parser_status"] = "malformed_json"
        return failed

    if not isinstance(data, dict):
        failed = _defaults.copy()
        failed["score_reasoning"] = "Scoring response parse failure: non_object_payload"
        failed["parser_status"] = "non_object_payload"
        return failed

    # Clamp ai_score to [0.0, 1.0]
    try:
        raw_score = float(data.get("ai_score", 0.0))
    except (TypeError, ValueError):
        failed = _defaults.copy()
        failed["score_reasoning"] = "Scoring response parse failure: invalid_ai_score"
        failed["parser_status"] = "invalid_ai_score"
        return failed
    ai_score = max(0.0, min(1.0, raw_score))

    # Validate / derive fit_label
    fit_label = str(data.get("fit_label", "")).lower().strip()
    if fit_label not in VALID_FIT_LABELS:
        fit_label = fit_label_from_score(ai_score, config=config)

    return {
        "ai_score":          ai_score,
        "fit_label":         fit_label,
        "score_reasoning":   str(data.get("score_reasoning", "")),
        "matched_strengths": list(data.get("matched_strengths", []) or []),
        "key_risks":         list(data.get("key_risks", []) or []),
        "parser_status":     "ok",
    }


# ── integration: score one job ────────────────────────────────────────────────

def _make_genai_client(config: dict[str, Any]) -> Any:
    """Return a google.genai Client.

    Priority:
    1. GEMINI_API_KEY env var  → uses Gemini API (generativelanguage.googleapis.com)
       - Instant access, no Vertex AI Model Garden approval needed.
       - Get a key at https://aistudio.google.com/apikey
    2. GOOGLE_APPLICATION_CREDENTIALS → uses Vertex AI endpoint
       - Requires Vertex AI publisher model access for the project.
    """
    import httpx

    routing = resolve_model_routing_part("ranking_ai_score", model_fallback=get_gemini_model(config))

    provider_name = str(routing.get("provider") or "").strip().lower()
    allowed_http_providers = {"openai", "openai_compatible", "9router"}
    if provider_name not in allowed_http_providers:
        raise RuntimeError(
            "ranking_ai_score provider must be configured in control-plane model_routing.parts as "
            "one of: openai, openai_compatible, 9router."
        )

    if provider_name in allowed_http_providers:
        base_url = str(routing.get("base_url") or "").strip()
        if not base_url:
            raise RuntimeError("OpenAI-compatible reranker routing requires provider base_url in control-plane config.")
        api_key = (
            str(os.environ.get("OPENAI_API_KEY") or "").strip()
            or str(os.environ.get("OPENAI_COMPATIBLE_API_KEY") or "").strip()
        )
        if not api_key:
            raise RuntimeError(
                "Config-routed OpenAI-compatible provider for ranking_ai_score requires API key in env "
                "(OPENAI_API_KEY or OPENAI_COMPATIBLE_API_KEY)."
            )
        wire_api = str(routing.get("wire_api") or "").strip().lower() or "responses"
        timeout_seconds = float(str(routing.get("timeout_seconds") or "").strip() or "120")
        model_override = str(routing.get("model") or "").strip()
        if not model_override:
            raise RuntimeError(
                "ranking_ai_score model must be configured in control-plane model_routing.parts."
            )

        def _generate_content(*, model: str, contents: str) -> Any:
            # For OpenAI-compatible routing, provider/model routing is authoritative.
            del model
            resolved_model = model_override
            with httpx.Client(timeout=timeout_seconds) as client:
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                }
                if wire_api == "responses":
                    payload = {
                        "model": resolved_model,
                        "input": contents,
                        "text": {"format": {"type": "json_object"}},
                    }
                    try:
                        resp = client.post(f"{base_url.rstrip('/')}/responses", headers=headers, json=payload)
                        resp.raise_for_status()
                        body = dict(resp.json() or {})
                        text = _extract_openai_responses_text(body)
                    except httpx.HTTPStatusError as exc:
                        # Compatibility fallback: some OpenAI-compatible providers
                        # implement chat/completions but not responses.
                        if exc.response is None or exc.response.status_code != 404:
                            raise
                        payload = {
                            "model": resolved_model,
                            "messages": [{"role": "user", "content": contents}],
                            "temperature": 0.0,
                            "response_format": {"type": "json_object"},
                        }
                        resp = client.post(f"{base_url.rstrip('/')}/chat/completions", headers=headers, json=payload)
                        resp.raise_for_status()
                        body = dict(resp.json() or {})
                        text = str((((body.get("choices") or [{}])[0]).get("message") or {}).get("content") or "").strip()
                else:
                    payload = {
                        "model": resolved_model,
                        "messages": [{"role": "user", "content": contents}],
                        "temperature": 0.0,
                        "response_format": {"type": "json_object"},
                    }
                    resp = client.post(f"{base_url.rstrip('/')}/chat/completions", headers=headers, json=payload)
                    resp.raise_for_status()
                    body = dict(resp.json() or {})
                    text = str((((body.get("choices") or [{}])[0]).get("message") or {}).get("content") or "").strip()
            return SimpleNamespace(text=text)

        return SimpleNamespace(models=SimpleNamespace(generate_content=_generate_content))


def score_job(
    job: dict[str, Any],
    candidate_summary: str,
    top_evidence: list[str],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Score one job via Gemini and return a parsed score dict.

    Auth priority: GEMINI_API_KEY > GOOGLE_APPLICATION_CREDENTIALS (Vertex AI).
    See _make_genai_client() for details.
    """
    from fitcv.embeddings import build_job_summary_text

    jd_summary = build_job_summary_text(job)
    thresholds = dict(config.get("fit_label_thresholds") or {})
    prompt = build_scoring_prompt(
        jd_summary=jd_summary,
        candidate_summary=candidate_summary,
        top_evidence=top_evidence[:2],
        strong_threshold=float(thresholds.get("strong", DEFAULT_FIT_LABEL_STRONG_THRESHOLD)),
        stretch_threshold=float(thresholds.get("stretch", DEFAULT_FIT_LABEL_STRETCH_THRESHOLD)),
        config=config,
    )

    model_name = get_gemini_model(config)
    client = _make_genai_client(config)
    response = client.models.generate_content(model=model_name, contents=prompt)
    raw_text = str(response.text or "")

    result = parse_score_response(raw_text, config=config)
    result["job_url"] = str(job.get("job_url", ""))
    return result


# ── integration: batch score shortlist ───────────────────────────────────────

def run_ai_scoring(
    shortlist: list[dict[str, Any]],
    candidate_summary: str,
    config: dict[str, Any],
    top_n: int | None = None,
) -> list[dict[str, Any]]:
    """Score at most top_n shortlisted jobs.

    top_n defaults to config["pipeline"]["ai_score_top_n"] (50 if missing).
    sleep between calls prefers config["stage_runtime"]["ranking"]["sleep_secs"].
    Falls back to config["rerank_sleep_secs"] (0.5 if missing).

    shortlist: list of job dicts from VECTOR_SEARCH (must include job_url and
               structured JD fields). Each item may optionally include
               "top_evidence" (list[str]).

    Requires GOOGLE_APPLICATION_CREDENTIALS.
    """
    import time

    effective_top_n = (
        top_n
        if top_n is not None
        else int((config.get("pipeline") or {}).get("ai_score_top_n") or config.get("rerank_top_n", 50))
    )
    sleep_secs = get_stage_runtime_sleep_secs(
        config,
        stage="ranking",
        default=0.5,
        compatibility_fallback_key="rerank_sleep_secs",
    )
    ranking_concurrency = get_stage_runtime_concurrency(
        config,
        stage="ranking",
        default=1,
    )
    selected_jobs = shortlist[:effective_top_n]

    def _score_single(job: dict[str, Any]) -> dict[str, Any]:
        top_evidence = list(job.get("top_evidence", []) or [])[:2]
        try:
            return score_job(
                job=job,
                candidate_summary=candidate_summary,
                top_evidence=top_evidence,
                config=config,
            )
        except Exception as exc:  # noqa: BLE001
            return {
                "job_url": str(job.get("job_url", "")),
                "ai_score": 0.0, "fit_label": "skip",
                "score_reasoning": f"Scoring error: {exc}",
                "matched_strengths": [], "key_risks": [],
                "parser_status": "runtime_exception",
            }

    scored_by_index: dict[int, dict[str, Any]] = {}
    if ranking_concurrency <= 1:
        for i, job in enumerate(selected_jobs):
            scored_by_index[i] = _score_single(job)
            if i < len(selected_jobs) - 1:
                time.sleep(sleep_secs)
    else:
        with ThreadPoolExecutor(max_workers=ranking_concurrency) as executor:
            futures: dict[Any, int] = {}
            for i, job in enumerate(selected_jobs):
                futures[executor.submit(_score_single, job)] = i
                if i < len(selected_jobs) - 1:
                    time.sleep(sleep_secs)
            for future in as_completed(futures):
                scored_by_index[futures[future]] = future.result()

    scored: list[dict[str, Any]] = []
    for i in range(len(selected_jobs)):
        scored.append(scored_by_index[i])

    return scored


# ── integration: persist scores ───────────────────────────────────────────────



def _ensure_local_ai_score_results_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_score_results (
            job_url TEXT PRIMARY KEY,
            ai_score REAL NOT NULL,
            fit_label TEXT NOT NULL,
            score_reasoning TEXT NOT NULL,
            matched_strengths_json TEXT NOT NULL,
            key_risks_json TEXT NOT NULL,
            scored_at TEXT NOT NULL
        )
        """
    )
    conn.commit()



def store_ai_scores(
    scores: list[dict[str, Any]],
    config: dict[str, Any],
) -> None:
    """Insert AI scoring results into fitcv.ai_score_results."""
    if not scores:
        return

    now = datetime.now(tz=timezone.utc).isoformat()

    if sqlite_mode_enabled(config):
        db_path = Path(get_local_sqlite_path())
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            _ensure_local_ai_score_results_table(conn)
            conn.executemany(
                """
                INSERT INTO ai_score_results(
                    job_url,
                    ai_score,
                    fit_label,
                    score_reasoning,
                    matched_strengths_json,
                    key_risks_json,
                    scored_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_url) DO UPDATE SET
                    ai_score = excluded.ai_score,
                    fit_label = excluded.fit_label,
                    score_reasoning = excluded.score_reasoning,
                    matched_strengths_json = excluded.matched_strengths_json,
                    key_risks_json = excluded.key_risks_json,
                    scored_at = excluded.scored_at
                """,
                [
                    (
                        str(score["job_url"]),
                        float(score["ai_score"]),
                        str(score["fit_label"]),
                        str(score.get("score_reasoning") or ""),
                        json.dumps(list(score.get("matched_strengths") or []), ensure_ascii=False),
                        json.dumps(list(score.get("key_risks") or []), ensure_ascii=False),
                        now,
                    )
                    for score in scores
                ],
            )
            conn.commit()
        return


    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    client = build_bigquery_client(config)
    table_ref = f"{project}.{dataset}.ai_score_results"

    rows = [
        {
            "job_url": s["job_url"],
            "ai_score": s["ai_score"],
            "fit_label": s["fit_label"],
            "score_reasoning": s.get("score_reasoning", ""),
            "matched_strengths": s.get("matched_strengths", []),
            "key_risks": s.get("key_risks", []),
            "scored_at": now,
        }
        for s in scores
    ]

    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors for ai_score_results: {errors}")

