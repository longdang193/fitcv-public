"""@meta
name: vector_search
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.vector_search.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

import hashlib
import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, TypedDict

from fitcv.candidate import flatten_skills, infer_role_family
from fitcv.config import sqlite_mode_enabled
from fitcv.embeddings import generate_embedding, get_shortlist_embedding_model
from fitcv.shortlist_runtime import (
    build_bigquery_client,
    build_contract_fingerprint,
    configure_sqlite_connection,
    hash_payload,
    normalize_text_scalar,
    run_sqlite_io_retry,
    sqlite_path,
)

DEFAULT_RECENT_ROLE_COUNT = 3
DEFAULT_ROLE_FAMILY_HINT_COUNT = 3
DEFAULT_DOMAIN_HINT_COUNT = 5
DEFAULT_LOCATION_TYPE_HINT_COUNT = 3
CANDIDATE_QUERY_SCHEMA_VERSION = "shortlist_candidate_query_v1"
REUSED_CACHED_QUERY_EMBEDDING_STATUS = "reused_cached_query_embedding"
FRESH_QUERY_EMBEDDING_STATUS = "fresh_query_embedding"

logger = logging.getLogger(__name__)


class CandidateQueryEmbeddingRecord(TypedDict):
    text: str
    components: dict[str, Any]
    embedding: list[float]
    candidate_query_signature: str
    candidate_query_contract_fingerprint: str
    candidate_query_reuse_status: str


class CandidateQueryEmbeddingCacheRow(TypedDict):
    candidate_query_signature: str
    candidate_query_contract_fingerprint: str
    candidate_query_text: str
    candidate_query_components_json: str
    embedding: list[float]


def _build_candidate_query_embedding_record(
    *,
    text: str,
    components: dict[str, Any],
    embedding: list[float],
    candidate_query_signature: str,
    candidate_query_contract_fingerprint: str,
    candidate_query_reuse_status: str,
) -> CandidateQueryEmbeddingRecord:
    return {
        "text": text,
        "components": components,
        "embedding": embedding,
        "candidate_query_signature": candidate_query_signature,
        "candidate_query_contract_fingerprint": candidate_query_contract_fingerprint,
        "candidate_query_reuse_status": candidate_query_reuse_status,
    }





def _ensure_sqlite_vector_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_query_embeddings (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          candidate_query_signature TEXT NOT NULL,
          candidate_query_contract_fingerprint TEXT NOT NULL,
          candidate_query_text TEXT NOT NULL,
          candidate_query_components_json TEXT NOT NULL,
          embedding_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS vector_shortlist (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          job_url TEXT NOT NULL,
          vector_rank INTEGER NOT NULL,
          vector_similarity REAL NOT NULL,
          retrieval_strategy TEXT NOT NULL,
          retrieved_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_candidate_query_embeddings_sig_created ON candidate_query_embeddings(candidate_query_signature, created_at DESC)"
    )


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if not a or not b:
        return 0.0
    dim = min(len(a), len(b))
    if dim <= 0:
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for i in range(dim):
        av = float(a[i])
        bv = float(b[i])
        dot += av * bv
        na += av * av
        nb += bv * bv
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return dot / ((na ** 0.5) * (nb ** 0.5))


def _dedupe_shortlist_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the best-ranked row per job_url, preserving shortlist order."""
    deduped: list[dict[str, Any]] = []
    seen_job_urls: set[str] = set()
    for row in rows:
        job_url = str(row.get("job_url") or "")
        if not job_url or job_url in seen_job_urls:
            continue
        seen_job_urls.add(job_url)
        deduped.append(row)
    return deduped


# ── candidate query text ──────────────────────────────────────────────────────

def _append_unique_text(values: list[str], candidate: str, seen: set[str]) -> None:
    text = str(candidate or "").strip()
    if not text:
        return
    lowered = text.lower()
    if lowered in seen:
        return
    seen.add(lowered)
    values.append(text)


def _normalize_query_scalar(value: Any) -> str:
    return normalize_text_scalar(value)


def _shortlist_lexical_policy(config: dict[str, Any] | None) -> dict[str, Any]:
    policy = dict(((config or {}).get("shortlist_lexical") or {}))
    protected = dict(policy.get("protected_terms") or {})
    policy["protected_terms"] = protected
    return policy

def _iter_taxonomy_candidate_terms(config: dict[str, Any] | None) -> list[str]:
    cfg = config or {}
    candidates: list[str] = []
    for key in ("skill_synonyms", "domain_alias_map", "role_family_alias_map"):
        payload = cfg.get(key) or {}
        if not isinstance(payload, dict):
            continue
        for alias, canonical in payload.items():
            candidates.append(str(alias))
            candidates.append(str(canonical))
    return candidates

def build_protected_terms(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build deterministic protected-term set from config + taxonomy-derived candidates."""
    policy = _shortlist_lexical_policy(config)
    protected_cfg = dict(policy.get("protected_terms") or {})
    manual_seed_raw = list(protected_cfg.get("manual_seed") or [])
    manual_seed = {str(term).strip().lower() for term in manual_seed_raw if str(term).strip()}
    max_len = int(protected_cfg.get("max_len_auto_protect", 5) or 5)
    punctuation_markers = [str(marker) for marker in list(protected_cfg.get("punctuation_markers") or ["+", "#", "."])]
    stopword_exclusions = {
        str(term).strip().lower()
        for term in list(protected_cfg.get("stopword_exclusions") or [])
        if str(term).strip()
    }
    derive_from_taxonomy = bool(protected_cfg.get("derive_from_taxonomy", True))

    derived: set[str] = set()
    if derive_from_taxonomy:
        for raw_candidate in _iter_taxonomy_candidate_terms(config):
            candidate = str(raw_candidate or "").strip().lower()
            if not candidate:
                continue
            if " " in candidate:
                continue
            has_marker = any(marker and marker in candidate for marker in punctuation_markers)
            has_digit = any(ch.isdigit() for ch in candidate)
            include_candidate = (candidate in manual_seed) or (len(candidate) <= max_len) or has_marker or has_digit
            if not include_candidate:
                continue
            if candidate not in manual_seed and candidate in stopword_exclusions:
                continue
            derived.add(candidate)

    protected_terms = sorted(manual_seed.union(derived))
    payload_json, protected_terms_hash = hash_payload({"protected_terms": protected_terms})
    return {
        "protected_terms": protected_terms,
        "protected_terms_hash": protected_terms_hash,
        "protected_terms_count": len(protected_terms),
        "protected_terms_payload_json": payload_json,
    }


def _tokenize_lexical_text(text: str) -> list[str]:
    return [token for token in re.findall(r"[a-z0-9+#.]+", str(text).lower()) if token]

def _build_role_phrases(components: dict[str, Any]) -> list[str]:
    phrases: list[str] = []
    seen: set[str] = set()
    for field in ("headline", "target_role"):
        value = str(components.get(field) or "").strip().lower()
        tokens = _tokenize_lexical_text(value)
        for size in (2, 3):
            for idx in range(0, max(0, len(tokens) - size + 1)):
                phrase = " ".join(tokens[idx : idx + size]).strip()
                if phrase and phrase not in seen:
                    seen.add(phrase)
                    phrases.append(phrase)
    for role in list(components.get("recent_roles") or []):
        tokens = _tokenize_lexical_text(str(role or "").strip().lower())
        for size in (2, 3):
            for idx in range(0, max(0, len(tokens) - size + 1)):
                phrase = " ".join(tokens[idx : idx + size]).strip()
                if phrase and phrase not in seen:
                    seen.add(phrase)
                    phrases.append(phrase)
    return phrases

def build_weighted_bm25_query_terms(
    components: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build deterministic lexical term payload from canonical shortlist components."""
    lexical_cfg = _shortlist_lexical_policy(config)
    field_weights = dict(lexical_cfg.get("field_weights") or {})
    scoring_mode = str(lexical_cfg.get("scoring_mode") or "weighted_sum_fallback").strip().lower()
    if scoring_mode not in {"bm25f", "weighted_sum_fallback"}:
        scoring_mode = "weighted_sum_fallback"

    phrase_cfg = dict(lexical_cfg.get("phrase_boost") or {})
    phrase_boost_per_phrase = float(phrase_cfg.get("per_phrase", 0.2) or 0.2)
    phrase_boost_cap_ratio = float(phrase_cfg.get("cap_ratio_of_max_base", 0.2) or 0.2)

    protected = build_protected_terms(config)
    protected_set = set(list(protected.get("protected_terms") or []))

    field_values: dict[str, list[str]] = {
        "headline": [str(components.get("headline") or "")],
        "target_role": [str(components.get("target_role") or "")],
        "recent_roles": [str(item) for item in list(components.get("recent_roles") or [])],
        "skills": [str(item) for item in list(components.get("skills") or components.get("flattened_skills") or [])],
        "role_families": [str(item) for item in list(components.get("role_families") or components.get("role_family_hints") or [])],
        "domains": [str(item) for item in list(components.get("domains") or components.get("domain_hints") or [])],
        "location_types": [str(item) for item in list(components.get("location_types") or components.get("location_type_hints") or [])],
    }

    terms_by_field: dict[str, list[str]] = {}
    for field, values in field_values.items():
        tokens: list[str] = []
        for value in values:
            for token in _tokenize_lexical_text(value):
                if token in protected_set or len(token) > 1:
                    tokens.append(token)
        seen_tokens: set[str] = set()
        deduped_tokens: list[str] = []
        for token in tokens:
            if token in seen_tokens:
                continue
            seen_tokens.add(token)
            deduped_tokens.append(token)
        terms_by_field[field] = deduped_tokens

    role_phrases = _build_role_phrases(components)
    tie_break_order = ["lexical_base_score_desc", "phrase_hit_count_desc", "job_url_asc"]

    payload = {
        "terms_by_field": terms_by_field,
        "field_weights": field_weights,
        "role_phrases": role_phrases,
        "protected_terms": list(protected.get("protected_terms") or []),
        "scoring_mode": scoring_mode,
        "scoring_formula": (
            "bm25f_weighted" if scoring_mode == "bm25f"
            else "sum_f(weight_f * bm25_f(doc, query_terms_f))"
        ),
        "phrase_boost": {
            "per_phrase": phrase_boost_per_phrase,
            "cap_ratio_of_max_base": phrase_boost_cap_ratio,
            "accumulation": "sum_then_cap",
        },
        "tie_break_order": tie_break_order,
    }
    payload_json, bm25_terms_hash = hash_payload(payload)
    return {
        "payload": payload,
        "payload_json": payload_json,
        "bm25_terms_hash": bm25_terms_hash,
        "protected_terms_hash": str(protected.get("protected_terms_hash") or ""),
        "protected_terms_count": int(protected.get("protected_terms_count") or 0),
    }

def build_candidate_query_components(
    profile: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return bounded deterministic shortlist intent components (SSOT)."""
    max_skills = int((config or {}).get("vector_max_candidate_skills", 15))

    headline = str(profile.get("headline") or "").strip()
    prefs = profile.get("preferences", {}) or {}
    target_role = str(prefs.get("target_role") or "").strip()

    recent_roles: list[str] = []
    seen_recent_roles: set[str] = set()
    for experience in profile.get("experiences", []) or []:
        role = str(experience.get("role") or "").strip()
        if not role:
            continue
        _append_unique_text(recent_roles, role, seen_recent_roles)
        if len(recent_roles) >= DEFAULT_RECENT_ROLE_COUNT:
            break

    skills: list[str] = []
    seen_skills: set[str] = set()
    for skill in flatten_skills(profile):
        _append_unique_text(skills, skill, seen_skills)
        if len(skills) >= max_skills:
            break

    role_families: list[str] = []
    seen_role_families: set[str] = set()
    for role_family in prefs.get("role_families", []) or []:
        _append_unique_text(role_families, str(role_family), seen_role_families)
        if len(role_families) >= DEFAULT_ROLE_FAMILY_HINT_COUNT:
            break
    if len(role_families) < DEFAULT_ROLE_FAMILY_HINT_COUNT:
        inferred_target_family = infer_role_family(target_role, config=config)
        if inferred_target_family:
            _append_unique_text(role_families, inferred_target_family, seen_role_families)
    if len(role_families) < DEFAULT_ROLE_FAMILY_HINT_COUNT:
        for experience in profile.get("experiences", []) or []:
            explicit_family = str(experience.get("role_family") or "").strip()
            inferred_family = infer_role_family(
                str(experience.get("role") or ""),
                explicit_family=explicit_family or None,
                config=config,
            )
            if inferred_family:
                _append_unique_text(role_families, inferred_family, seen_role_families)
            if len(role_families) >= DEFAULT_ROLE_FAMILY_HINT_COUNT:
                break

    domains: list[str] = []
    seen_domains: set[str] = set()
    for domain in prefs.get("domains", []) or []:
        _append_unique_text(domains, str(domain), seen_domains)
        if len(domains) >= DEFAULT_DOMAIN_HINT_COUNT:
            break
    if len(domains) < DEFAULT_DOMAIN_HINT_COUNT:
        for experience in profile.get("experiences", []) or []:
            for domain_tag in experience.get("domain_tags", []) or []:
                _append_unique_text(domains, str(domain_tag), seen_domains)
                if len(domains) >= DEFAULT_DOMAIN_HINT_COUNT:
                    break
            if len(domains) >= DEFAULT_DOMAIN_HINT_COUNT:
                break
    if len(domains) < DEFAULT_DOMAIN_HINT_COUNT:
        for project in profile.get("projects", []) or []:
            for domain_tag in project.get("domain_tags", []) or []:
                _append_unique_text(domains, str(domain_tag), seen_domains)
                if len(domains) >= DEFAULT_DOMAIN_HINT_COUNT:
                    break
            if len(domains) >= DEFAULT_DOMAIN_HINT_COUNT:
                break

    location_types: list[str] = []
    seen_location_types: set[str] = set()
    for location_type in prefs.get("location_types", []) or []:
        _append_unique_text(location_types, str(location_type), seen_location_types)
        if len(location_types) >= DEFAULT_LOCATION_TYPE_HINT_COUNT:
            break

    return {
        "headline": headline,
        "target_role": target_role,
        "recent_roles": recent_roles,
        "skills": skills,
        "role_families": role_families,
        "domains": domains,
        "location_types": location_types,
        "role_family_hints": role_families,
        "flattened_skills": skills,
        "domain_hints": domains,
        "location_type_hints": location_types,
    }



def build_candidate_query_signature_record(components: dict[str, Any]) -> dict[str, Any]:
    """Return the stable shortlist query payload plus its hash signature."""
    payload = {
        "headline": _normalize_query_scalar(components.get("headline") or ""),
        "target_role": _normalize_query_scalar(components.get("target_role") or ""),
        "recent_roles": [
            _normalize_query_scalar(value)
            for value in list(components.get("recent_roles") or [])
            if _normalize_query_scalar(value)
        ],
        "skills": [
            _normalize_query_scalar(value)
            for value in list(components.get("skills") or components.get("flattened_skills") or [])
            if _normalize_query_scalar(value)
        ],
        "role_families": [
            _normalize_query_scalar(value)
            for value in list(components.get("role_families") or components.get("role_family_hints") or [])
            if _normalize_query_scalar(value)
        ],
        "domains": [
            _normalize_query_scalar(value)
            for value in list(components.get("domains") or components.get("domain_hints") or [])
            if _normalize_query_scalar(value)
        ],
        "location_types": [
            _normalize_query_scalar(value)
            for value in list(components.get("location_types") or components.get("location_type_hints") or [])
            if _normalize_query_scalar(value)
        ],
    }
    payload = {
        key: value
        for key, value in payload.items()
        if value not in ("", [], None)
    }
    payload_json, signature = hash_payload(payload)
    return {
        "payload": payload,
        "payload_json": payload_json,
        "signature": signature,
    }


def build_candidate_query_embedding_contract_fingerprint(config: dict[str, Any]) -> dict[str, Any]:
    """Fingerprint shortlist candidate-query embedding behavior to invalidate reuse."""
    payload = {
        "embedding_model": get_shortlist_embedding_model(config),
        "candidate_query_schema_version": CANDIDATE_QUERY_SCHEMA_VERSION,
    }
    fingerprint = build_contract_fingerprint(payload)
    return {
        "payload": payload,
        "fingerprint": fingerprint,
    }

def build_candidate_query_text(
    profile: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> str:
    """Build deterministic canonical shortlist query text from SSOT components."""
    components = build_candidate_query_components(profile, config)

    def _join(values: list[str]) -> str:
        return " | ".join(str(value).strip() for value in values if str(value).strip())

    lines = [
        f"Headline: {str(components.get('headline') or '').strip()}",
        f"Target Role: {str(components.get('target_role') or '').strip()}",
        f"Recent Roles: {_join(list(components.get('recent_roles') or []))}",
        f"Skills: {_join(list(components.get('skills') or []))}",
        f"Role Families: {_join(list(components.get('role_families') or []))}",
        f"Domains: {_join(list(components.get('domains') or []))}",
        f"Location Types: {_join(list(components.get('location_types') or []))}",
    ]
    return "\n".join(lines)


def _load_latest_candidate_query_embedding(
    *,
    client: Any,
    table_ref: str,
    candidate_query_signature: str,
) -> CandidateQueryEmbeddingCacheRow | None:
    """Fetch the latest cached shortlist candidate-query embedding for a signature."""
    if not candidate_query_signature:
        return None

    sql = f"""
SELECT
  candidate_query_signature,
  candidate_query_contract_fingerprint,
  candidate_query_text,
  candidate_query_components_json,
  embedding
FROM `{table_ref}`
WHERE candidate_query_signature = @candidate_query_signature
ORDER BY created_at DESC
LIMIT 1
""".strip()
    from google.cloud import bigquery  # type: ignore[import-untyped]

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("candidate_query_signature", "STRING", candidate_query_signature)
        ]
    )
    rows = list(client.query(sql, job_config=job_config).result())
    if not rows:
        return None
    row = rows[0]
    row_data: CandidateQueryEmbeddingCacheRow = {
        "candidate_query_signature": _normalize_query_scalar(
            getattr(row, "candidate_query_signature", "")
        ),
        "candidate_query_contract_fingerprint": _normalize_query_scalar(
            getattr(row, "candidate_query_contract_fingerprint", "")
        ),
        "candidate_query_text": _normalize_query_scalar(
            getattr(row, "candidate_query_text", "")
        ),
        "candidate_query_components_json": _normalize_query_scalar(
            getattr(row, "candidate_query_components_json", "")
        ),
        "embedding": list(getattr(row, "embedding", []) or []),
    }
    return row_data


def resolve_candidate_query_embedding(
    profile: dict[str, Any],
    config: dict[str, Any],
) -> CandidateQueryEmbeddingRecord:
    """Return the shortlist candidate query plus a reused or fresh embedding vector."""
    components = build_candidate_query_components(profile, config)
    query_text = build_candidate_query_text(profile, config)
    signature_record = build_candidate_query_signature_record(components)
    contract_record = build_candidate_query_embedding_contract_fingerprint(config)
    sqlite_mode = sqlite_mode_enabled(config)
    if sqlite_mode:
        with sqlite3.connect(sqlite_path(), timeout=30) as conn:
            configure_sqlite_connection(conn)
            _ensure_sqlite_vector_tables(conn)
            row = conn.execute(
                """
                SELECT embedding_json
                FROM candidate_query_embeddings
                WHERE candidate_query_signature = ?
                  AND candidate_query_contract_fingerprint = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (signature_record["signature"], contract_record["fingerprint"]),
            ).fetchone()
            if row and row[0]:
                try:
                    cached_embedding = list(json.loads(str(row[0])) or [])
                except Exception:
                    cached_embedding = []
                if cached_embedding:
                    return _build_candidate_query_embedding_record(
                        text=query_text,
                        components=components,
                        embedding=cached_embedding,
                        candidate_query_signature=signature_record["signature"],
                        candidate_query_contract_fingerprint=contract_record["fingerprint"],
                        candidate_query_reuse_status=REUSED_CACHED_QUERY_EMBEDDING_STATUS,
                    )
            embedding_vector = generate_embedding(query_text, config)
            now = datetime.now(tz=timezone.utc).isoformat()
            conn.execute(
                """
                INSERT INTO candidate_query_embeddings(
                  candidate_query_signature, candidate_query_contract_fingerprint,
                  candidate_query_text, candidate_query_components_json, embedding_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    signature_record["signature"],
                    contract_record["fingerprint"],
                    query_text,
                    signature_record["payload_json"],
                    json.dumps(embedding_vector),
                    now,
                ),
            )
            conn.commit()
        return _build_candidate_query_embedding_record(
            text=query_text,
            components=components,
            embedding=embedding_vector,
            candidate_query_signature=signature_record["signature"],
            candidate_query_contract_fingerprint=contract_record["fingerprint"],
            candidate_query_reuse_status=FRESH_QUERY_EMBEDDING_STATUS,
        )

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    client = build_bigquery_client(config)
    table_ref = f"{project}.{dataset}.candidate_query_embeddings"
    try:
        cached_row = _load_latest_candidate_query_embedding(
            client=client,
            table_ref=table_ref,
            candidate_query_signature=signature_record["signature"],
        )
    except Exception as exc:
        logger.warning("Candidate query embedding cache lookup failed; falling back to fresh embedding: %s", exc)
        cached_row = None

    if cached_row and (
        cached_row["candidate_query_contract_fingerprint"] == contract_record["fingerprint"]
    ):
        return _build_candidate_query_embedding_record(
            text=query_text,
            components=components,
            embedding=list(cached_row["embedding"] or []),
            candidate_query_signature=signature_record["signature"],
            candidate_query_contract_fingerprint=contract_record["fingerprint"],
            candidate_query_reuse_status=REUSED_CACHED_QUERY_EMBEDDING_STATUS,
        )

    embedding_vector = generate_embedding(query_text, config)
    now = datetime.now(tz=timezone.utc).isoformat()
    rows = [
        {
            "candidate_query_signature": signature_record["signature"],
            "candidate_query_contract_fingerprint": contract_record["fingerprint"],
            "candidate_query_text": query_text,
            "candidate_query_components_json": signature_record["payload_json"],
            "embedding": embedding_vector,
            "created_at": now,
        }
    ]
    try:
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            logger.warning("BigQuery insert errors for candidate_query_embeddings: %s", errors)
    except Exception as exc:
        logger.warning("Candidate query embedding cache insert failed; continuing with fresh embedding: %s", exc)
    return _build_candidate_query_embedding_record(
        text=query_text,
        components=components,
        embedding=embedding_vector,
        candidate_query_signature=signature_record["signature"],
        candidate_query_contract_fingerprint=contract_record["fingerprint"],
        candidate_query_reuse_status=FRESH_QUERY_EMBEDDING_STATUS,
    )


# ── VECTOR_SEARCH SQL builder ─────────────────────────────────────────────────

def build_vector_search_query(
    top_n: int,
    passed_job_urls: list[str],
    project: str = "PROJECT",
    dataset: str = "fitcv",
) -> str:
    """Return a BigQuery VECTOR_SEARCH SQL string.

    Design rules:
    - Only searches job_embeddings WHERE chunk_type = 'job_summary'
    - Only searches within the rule-filtered universe (passed_job_urls)
    - Enforces top_k = top_n
    - Returns job_url, vector_similarity (distance), vector_rank

    The caller is responsible for substituting @candidate_embedding with the
    actual embedding vector before executing.

    Args:
        top_n:            Maximum number of results to return.
        passed_job_urls:  Rule-filtered job URLs to restrict the search universe.
        project:          GCP project id (for table references).
        dataset:          BigQuery dataset name.

    Returns:
        A BigQuery SQL string (not yet executed).
    """
    temp_table_name = "_latest_job_embeddings"

    if passed_job_urls:
        latest_rows_query = f"""
CREATE TEMP TABLE {temp_table_name} AS
SELECT
  job_url,
  chunk_type,
  chunk_text,
  embedding,
  created_at
FROM (
  SELECT
    job_url,
    chunk_type,
    chunk_text,
    embedding,
    created_at,
    ROW_NUMBER() OVER (
      PARTITION BY job_url
      ORDER BY created_at DESC, chunk_text DESC, job_url DESC
    ) AS rn
  FROM `{project}.{dataset}.job_embeddings`
  WHERE chunk_type = 'job_summary' AND job_url IN UNNEST(@passed_job_urls)
)
WHERE rn = 1;
""".strip()
    else:
        latest_rows_query = f"""
CREATE TEMP TABLE {temp_table_name} AS
SELECT
  job_url,
  chunk_type,
  chunk_text,
  embedding,
  created_at
FROM `{project}.{dataset}.job_embeddings`
WHERE 1 = 0;
""".strip()

    return f"""
{latest_rows_query}

SELECT
  base.job_url                              AS job_url,
  1 - distance                              AS vector_similarity,
  RANK() OVER (ORDER BY distance ASC)       AS vector_rank
FROM
  VECTOR_SEARCH(
    TABLE {temp_table_name},
    'embedding',
    (SELECT @candidate_embedding AS embedding),
    top_k => {top_n},
    distance_type => 'COSINE'
  )
ORDER BY vector_rank
LIMIT {top_n}
""".strip()


# ── integration: run full retrieval pipeline ──────────────────────────────────

def run_vector_search(
    profile: dict[str, Any],
    passed_job_urls: list[str],
    config: dict[str, Any],
    top_n: int | None = None,
    *,
    include_debug: bool = False,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Generate candidate query embedding and execute VECTOR_SEARCH.

    top_n defaults to config["pipeline"]["vector_search_top_n"] (50 if missing).

    Steps:
    1. Build candidate query text (deterministic, no embedding call)
    2. Embed it via Vertex AI text-embedding-005
    3. Execute VECTOR_SEARCH over rule-filtered job universe
    4. Return shortlist rows

    Requires GOOGLE_APPLICATION_CREDENTIALS.
    Decorated with @pytest.mark.integration in tests.

    Returns:
        List of dicts with: job_url, vector_similarity, vector_rank.
        Returns [] if passed_job_urls is empty.
    """
    if not passed_job_urls:
        return []
    sqlite_mode = sqlite_mode_enabled(config)
    effective_top_n = (
        top_n
        if top_n is not None
        else int((config.get("pipeline") or {}).get("vector_search_top_n") or config.get("vector_top_n", 50))
    )
    if sqlite_mode:
        candidate_query_record = resolve_candidate_query_embedding(profile, config)
        candidate_embedding = list(candidate_query_record.get("embedding") or [])
        placeholders = ",".join(["?"] * len(passed_job_urls))
        rows: list[tuple[Any, ...]] = []
        with sqlite3.connect(sqlite_path(), timeout=30) as conn:
            configure_sqlite_connection(conn)
            query = f"""
            SELECT je.job_url, je.embedding_json
            FROM job_embeddings je
            JOIN (
              SELECT job_url, MAX(created_at) AS max_created
              FROM job_embeddings
              WHERE chunk_type = 'job_summary' AND job_url IN ({placeholders})
              GROUP BY job_url
            ) latest
              ON latest.job_url = je.job_url AND latest.max_created = je.created_at
            WHERE je.chunk_type = 'job_summary'
            """
            rows = list(conn.execute(query, tuple(passed_job_urls)).fetchall())
        scored = []
        for job_url, embedding_json in rows:
            try:
                job_embedding = list(json.loads(str(embedding_json)) or [])
            except Exception:
                job_embedding = []
            scored.append(
                {
                    "job_url": str(job_url),
                    "vector_similarity": _cosine_similarity(candidate_embedding, job_embedding),
                }
            )
        scored.sort(key=lambda item: float(item.get("vector_similarity") or 0.0), reverse=True)
        deduped = _dedupe_shortlist_rows(
            [
                {"job_url": row["job_url"], "vector_similarity": row["vector_similarity"], "vector_rank": idx + 1}
                for idx, row in enumerate(scored[:effective_top_n])
            ]
        )
        if include_debug:
            return {
                "rows": deduped,
                "candidate_query": {
                    key: value
                    for key, value in candidate_query_record.items()
                    if key != "embedding"
                },
            }
        return deduped

    from google.cloud import bigquery  # type: ignore[import-untyped]

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    client = build_bigquery_client(config)

    candidate_query_record = resolve_candidate_query_embedding(profile, config)
    embedding_vector = list(candidate_query_record.get("embedding") or [])

    sql = build_vector_search_query(
        top_n=effective_top_n,
        passed_job_urls=passed_job_urls,
        project=project,
        dataset=dataset,
    )

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("candidate_embedding", "FLOAT64", embedding_vector),
            bigquery.ArrayQueryParameter("passed_job_urls", "STRING", passed_job_urls),
        ]
    )

    rows = client.query(sql, job_config=job_config).result()
    shortlist = [
        {"job_url": row.job_url, "vector_similarity": row.vector_similarity, "vector_rank": row.vector_rank}
        for row in rows
    ]
    deduped = _dedupe_shortlist_rows(shortlist)
    if include_debug:
        return {
            "rows": deduped,
            "candidate_query": {
                key: value
                for key, value in candidate_query_record.items()
                if key != "embedding"
            },
        }
    return deduped


# ── integration: store shortlist ──────────────────────────────────────────────

def store_shortlist(
    shortlist: list[dict[str, Any]],
    config: dict[str, Any],
    retrieval_strategy: str | None = None,
) -> None:
    """Insert vector shortlist rows into fitcv.vector_shortlist.

    retrieval_strategy defaults to config["retrieval_strategy"] ("job_summary_v1" if missing).

    Requires GOOGLE_APPLICATION_CREDENTIALS.
    Decorated with @pytest.mark.integration in tests.
    """
    if not shortlist:
        return
    if sqlite_mode_enabled(config):
        effective_strategy = retrieval_strategy or str(config.get("retrieval_strategy", "job_summary_v1"))
        now = datetime.now(tz=timezone.utc).isoformat()
        def _write_shortlist() -> None:
            with sqlite3.connect(sqlite_path(), timeout=30) as conn:
                configure_sqlite_connection(conn)
                _ensure_sqlite_vector_tables(conn)
                conn.executemany(
                    """
                    INSERT INTO vector_shortlist(job_url, vector_rank, vector_similarity, retrieval_strategy, retrieved_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            str(item["job_url"]),
                            int(item["vector_rank"]),
                            float(item["vector_similarity"]),
                            effective_strategy,
                            now,
                        )
                        for item in shortlist
                    ],
                )
                conn.commit()

        run_sqlite_io_retry(_write_shortlist)
        return

    effective_strategy = retrieval_strategy or str(config.get("retrieval_strategy", "job_summary_v1"))

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    client = build_bigquery_client(config)
    table_ref = f"{project}.{dataset}.vector_shortlist"
    now = datetime.now(tz=timezone.utc).isoformat()

    rows = [
        {
            "job_url":            item["job_url"],
            "vector_rank":        item["vector_rank"],
            "vector_similarity":  item["vector_similarity"],
            "retrieval_strategy": effective_strategy,
            "retrieved_at":       now,
        }
        for item in shortlist
    ]

    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors for vector_shortlist: {errors}")








