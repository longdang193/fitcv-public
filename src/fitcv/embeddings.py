"""Generate and store semantic embeddings for jobs and candidate evidence.

Public API
----------
build_job_summary_text    : deterministic labelled-section text for one job
build_job_summary_chunk   : one-element list with chunk_type="job_summary"
build_candidate_chunks    : evidence chunks (one per project/bullet/achievement)
generate_embedding        : call Vertex AI text-embedding-005 (integration)
embed_and_store_jobs      : batch embed jobs → job_embeddings (integration)
embed_and_store_candidate : batch embed candidate → candidate_embeddings (integration)
"""

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from fitcv.config import get_embedding_model

JOB_SUMMARY_CHUNK_TYPE = "job_summary"
SHORTLIST_SUMMARY_SCHEMA_VERSION = "shortlist_job_summary_v2"
SHORTLIST_DEFAULT_EMBEDDING_MODEL = "text-embedding-005"
REUSED_CACHED_EMBEDDING_STATUS = "reused_cached_embedding"
FRESH_EMBEDDING_STATUS = "fresh_embedding"


def _normalize_summary_scalar(value: Any) -> str:
    """Collapse whitespace while preserving human-readable casing."""
    return " ".join(str(value or "").split()).strip()


def _canonicalize_for_hash(value: Any) -> Any:
    """Canonicalize nested values for deterministic hashing."""
    if isinstance(value, dict):
        return {
            key: _canonicalize_for_hash(value[key])
            for key in sorted(value)
        }
    if isinstance(value, list):
        return [_canonicalize_for_hash(item) for item in value]
    if isinstance(value, str):
        return value.casefold()
    return value


def _stable_sorted_unique_strings(values: list[Any]) -> list[str]:
    normalized_by_key: dict[str, str] = {}
    for value in values:
        normalized = _normalize_summary_scalar(value)
        if not normalized:
            continue
        normalized_by_key.setdefault(normalized.casefold(), normalized)
    return [
        normalized_by_key[key]
        for key in sorted(normalized_by_key)
    ]


def _preferred_skill_values(
    structured_jd: dict[str, Any],
    *,
    canonical_field: str,
    raw_field: str,
) -> list[str]:
    canonical_values = structured_jd.get(canonical_field) or []
    if canonical_values:
        return _stable_sorted_unique_strings(list(canonical_values))
    return _stable_sorted_unique_strings(list(structured_jd.get(raw_field) or []))


def build_job_summary_signature_payload(structured_jd: dict[str, Any]) -> dict[str, Any]:
    """Build the stable shortlist summary payload used for embedding reuse."""
    payload = {
        "title": _normalize_summary_scalar(structured_jd.get("title") or structured_jd.get("job_title") or ""),
        "location_type": _normalize_summary_scalar(structured_jd.get("location_type") or ""),
        "seniority": _normalize_summary_scalar(structured_jd.get("seniority") or ""),
        "job_family": _normalize_summary_scalar(structured_jd.get("job_family") or ""),
        "required_skills": _preferred_skill_values(
            structured_jd,
            canonical_field="required_skills_canonical",
            raw_field="required_skills",
        ),
        "preferred_skills": _preferred_skill_values(
            structured_jd,
            canonical_field="preferred_skills_canonical",
            raw_field="preferred_skills",
        ),
    }
    return {
        key: value
        for key, value in payload.items()
        if value not in ("", [], None)
    }


def build_job_summary_signature_record(structured_jd: dict[str, Any]) -> dict[str, Any]:
    """Return the stable shortlist summary payload plus its hash signature."""
    payload = build_job_summary_signature_payload(structured_jd)
    canonical_payload = _canonicalize_for_hash(payload)
    payload_json = json.dumps(canonical_payload, sort_keys=True, separators=(",", ":"))
    signature = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    return {
        "payload": payload,
        "payload_json": json.dumps(payload, sort_keys=True, separators=(",", ":")),
        "signature": signature,
    }


def get_shortlist_embedding_model(config: dict[str, Any]) -> str:
    """Return the embedding model identifier used for shortlist job summaries."""
    return str(config.get("shortlist_embedding_model") or get_embedding_model(config) or SHORTLIST_DEFAULT_EMBEDDING_MODEL)


def build_embedding_contract_fingerprint(config: dict[str, Any]) -> dict[str, Any]:
    """Fingerprint shortlist embedding behavior to invalidate reuse on contract drift."""
    payload = {
        "embedding_model": get_shortlist_embedding_model(config),
        "summary_schema_version": SHORTLIST_SUMMARY_SCHEMA_VERSION,
    }
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    fingerprint = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    return {
        "payload": payload,
        "fingerprint": fingerprint,
    }


def _load_latest_job_embedding_metadata(
    *,
    client: Any,
    table_ref: str,
    job_urls: list[str],
) -> dict[str, dict[str, str]]:
    """Fetch latest job-summary embedding metadata keyed by job_url."""
    if not job_urls:
        return {}

    url_list = ", ".join(f"'{url}'" for url in job_urls)
    sql = f"""
SELECT
  job_url,
  embedding_input_signature,
  embedding_contract_fingerprint
FROM (
  SELECT
    job_url,
    embedding_input_signature,
    embedding_contract_fingerprint,
    created_at,
    chunk_text,
    ROW_NUMBER() OVER (
      PARTITION BY job_url
      ORDER BY created_at DESC, chunk_text DESC, job_url DESC
    ) AS rn
  FROM `{table_ref}`
  WHERE chunk_type = '{JOB_SUMMARY_CHUNK_TYPE}' AND job_url IN ({url_list})
)
WHERE rn = 1
""".strip()
    rows = client.query(sql).result()
    metadata: dict[str, dict[str, str]] = {}
    for row in rows:
        job_url = _normalize_summary_scalar(getattr(row, "job_url", ""))
        if not job_url:
            continue
        metadata[job_url] = {
            "embedding_input_signature": _normalize_summary_scalar(
                getattr(row, "embedding_input_signature", "")
            ),
            "embedding_contract_fingerprint": _normalize_summary_scalar(
                getattr(row, "embedding_contract_fingerprint", "")
            ),
        }
    return metadata


# ── job summary text ──────────────────────────────────────────────────────────

def build_job_summary_text(structured_jd: dict[str, Any]) -> str:
    """Build a deterministic labelled-section string for embedding.

    Format (structured text gives better embedding quality than free join):

        Title: <title>
        Required skills: <comma-joined required_skills>
        Preferred skills: <comma-joined preferred_skills>
        Location type: <location_type>
        Seniority: <seniority>
        Job family: <job_family>

    All fields are optional; missing/empty fields are omitted from the output.
    """
    payload = build_job_summary_signature_payload(structured_jd)
    parts: list[str] = []

    def _append(label: str, value: str) -> None:
        if value:
            parts.append(f"{label}: {value}")

    _append("Title", str(payload.get("title") or ""))
    _append(
        "Required skills",
        ", ".join(payload.get("required_skills", []) or []),
    )
    _append(
        "Preferred skills",
        ", ".join(payload.get("preferred_skills", []) or []),
    )
    _append("Location type", str(payload.get("location_type") or ""))
    _append("Seniority", str(payload.get("seniority") or ""))
    _append("Job family", str(payload.get("job_family") or ""))

    return "\n".join(parts)


# ── job summary chunk ─────────────────────────────────────────────────────────

def build_job_summary_chunk(structured_jd: dict[str, Any]) -> list[dict[str, Any]]:
    """Return a list containing exactly one job_summary chunk.

    v1 rule: always one chunk per job used for VECTOR_SEARCH shortlist ranking.
    Named build_job_summary_chunk (not chunk_jd_by_section) to clearly
    reflect the single-chunk v1 design. Multi-chunk expansion is reserved for v2.

    Shape: [{"chunk_type": "job_summary", "chunk_text": <labelled text>}]
    """
    return [{
        "chunk_type": JOB_SUMMARY_CHUNK_TYPE,
        "chunk_text": build_job_summary_text(structured_jd),
    }]


# ── candidate evidence chunks ─────────────────────────────────────────────────

def _project_chunk_text(proj: dict[str, Any]) -> str:
    skills = ", ".join(proj.get("skills", []) or [])
    return (
        f"Project: {proj.get('name', '')}\n"
        f"Skills: {skills}\n"
        f"Business value: {proj.get('business_value', '')}"
    ).strip()


def _bullet_chunk_text(exp: dict[str, Any], bullet: dict[str, Any]) -> str:
    skills = ", ".join(bullet.get("skills", []) or [])
    impact = bullet.get("measurable_impact", "")
    text = (
        f"Role: {exp.get('role', '')} at {exp.get('company', '')}\n"
        f"Achievement: {bullet.get('text', '')}\n"
        f"Skills: {skills}"
    )
    if impact:
        text += f"\nImpact: {impact}"
    return text.strip()


def _achievement_chunk_text(ach: dict[str, Any]) -> str:
    return (
        f"Achievement: {ach.get('text', '')}\n"
        f"Category: {ach.get('category', '')}"
    ).strip()


def build_candidate_chunks(profile: dict[str, Any]) -> list[dict[str, Any]]:
    """Build evidence chunks for candidate embedding.

    v1 granularity (explicit, not vague):
    - One chunk per project        (evidence_type = "project")
    - One chunk per experience bullet (evidence_type = "experience_bullet")
    - One chunk per achievement    (evidence_type = "achievement")

    Each chunk has this shape:
        {
            "evidence_id":   str,  # unique chunk ID (e.g. proj_1, exp_1_bullet_0)
            "source_ref_id": str,  # originating YAML ID (exp_id/proj_id/ach_id)
            "evidence_type": str,  # project | experience_bullet | achievement
            "chunk_text":    str,  # human-readable text for embedding
        }
    """
    chunks: list[dict[str, Any]] = []

    # ── projects: one chunk each ──────────────────────────────────────────────
    for proj in profile.get("projects", []):
        proj_id = str(proj.get("id", ""))
        chunks.append({
            "evidence_id":   proj_id,
            "source_ref_id": proj_id,
            "evidence_type": "project",
            "chunk_text":    _project_chunk_text(proj),
        })

    # ── experience bullets: one chunk per bullet ──────────────────────────────
    for exp in profile.get("experiences", []):
        exp_id = str(exp.get("id", ""))
        for idx, bullet in enumerate(exp.get("bullets", [])):
            chunks.append({
                "evidence_id":   f"{exp_id}_bullet_{idx}",
                "source_ref_id": exp_id,
                "evidence_type": "experience_bullet",
                "chunk_text":    _bullet_chunk_text(exp, bullet),
            })

    # ── achievements: one chunk each ──────────────────────────────────────────
    for ach in profile.get("achievements", []):
        ach_id = str(ach.get("id", ""))
        chunks.append({
            "evidence_id":   ach_id,
            "source_ref_id": ach_id,
            "evidence_type": "achievement",
            "chunk_text":    _achievement_chunk_text(ach),
        })

    return chunks


# ── integration: Vertex AI embedding ─────────────────────────────────────────

def generate_embedding(
    text: str,
    config: dict[str, Any],
    model_name: str | None = None,
) -> list[float]:
    """Call Vertex AI text-embedding-005 and return the embedding vector.

    Requires GOOGLE_APPLICATION_CREDENTIALS.
    Marked @pytest.mark.integration in tests.
    """
    import vertexai  # type: ignore[import-untyped]
    from fitcv.config import get_vertex_location
    from vertexai.language_models import TextEmbeddingModel  # type: ignore[import-untyped]

    vertexai.init(
        project=str(config["gcp_project"]),
        location=get_vertex_location(config),
    )
    model = TextEmbeddingModel.from_pretrained(str(model_name or SHORTLIST_DEFAULT_EMBEDDING_MODEL))
    embeddings = model.get_embeddings([text])
    return embeddings[0].values  # type: ignore[return-value]


# ── integration: batch embed + store jobs ─────────────────────────────────────

def embed_and_store_jobs(
    structured_jobs: list[dict[str, Any]],
    config: dict[str, Any],
) -> int:
    """Embed each job's summary and insert into fitcv.job_embeddings.

    Requires GOOGLE_APPLICATION_CREDENTIALS.
    Marked @pytest.mark.integration in tests.

    Returns:
        Number of rows inserted.
    """
    if not structured_jobs:
        return 0

    import time

    from google.cloud import bigquery  # type: ignore[import-untyped]
    from google.oauth2 import service_account  # type: ignore[import-untyped]

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    key_path = str(config["service_account_key"])
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project, credentials=credentials)
    table_ref = f"{project}.{dataset}.job_embeddings"
    now = datetime.now(tz=timezone.utc).isoformat()
    embedding_contract = build_embedding_contract_fingerprint(config)
    job_urls = [
        str(job.get("job_url") or "")
        for job in structured_jobs
        if str(job.get("job_url") or "")
    ]
    latest_metadata_by_url = _load_latest_job_embedding_metadata(
        client=client,
        table_ref=table_ref,
        job_urls=job_urls,
    )

    rows: list[dict[str, Any]] = []
    for i, job in enumerate(structured_jobs):
        signature_record = build_job_summary_signature_record(job)
        job["embedding_input_signature"] = signature_record["signature"]
        job["embedding_contract_fingerprint"] = embedding_contract["fingerprint"]
        latest_metadata = latest_metadata_by_url.get(str(job.get("job_url") or ""))
        if latest_metadata and (
            latest_metadata.get("embedding_input_signature") == signature_record["signature"]
            and latest_metadata.get("embedding_contract_fingerprint") == embedding_contract["fingerprint"]
        ):
            job["embedding_reuse_status"] = REUSED_CACHED_EMBEDDING_STATUS
            continue
        chunk = build_job_summary_chunk(job)[0]
        vector = generate_embedding(chunk["chunk_text"], config)
        job["embedding_reuse_status"] = FRESH_EMBEDDING_STATUS
        rows.append({
            "job_url":    str(job.get("job_url", "")),
            "chunk_type": chunk["chunk_type"],
            "chunk_text": chunk["chunk_text"],
            "embedding":  vector,
            "created_at": now,
            "embedding_input_signature": signature_record["signature"],
            "embedding_contract_fingerprint": embedding_contract["fingerprint"],
            "embedding_input_signature_payload_json": signature_record["payload_json"],
        })
        if i < len(structured_jobs) - 1:
            time.sleep(0.5)  # stay within Vertex AI quota

    if rows:
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            raise RuntimeError(f"BigQuery insert errors for job_embeddings: {errors}")
    return len(rows)


# ── integration: batch embed + store candidate ────────────────────────────────

def embed_and_store_candidate(
    profile: dict[str, Any],
    config: dict[str, Any],
) -> int:
    """Embed candidate evidence chunks and insert into fitcv.candidate_embeddings.

    Requires GOOGLE_APPLICATION_CREDENTIALS.
    Marked @pytest.mark.integration in tests.

    Returns:
        Number of rows inserted.
    """
    import time

    from google.cloud import bigquery  # type: ignore[import-untyped]
    from google.oauth2 import service_account  # type: ignore[import-untyped]

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    key_path = str(config["service_account_key"])
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project, credentials=credentials)
    table_ref = f"{project}.{dataset}.candidate_embeddings"
    now = datetime.now(tz=timezone.utc).isoformat()

    candidate_chunks = build_candidate_chunks(profile)
    rows: list[dict[str, Any]] = []

    for i, chunk in enumerate(candidate_chunks):
        vector = generate_embedding(chunk["chunk_text"], config)
        rows.append({
            "evidence_id":   chunk["evidence_id"],
            "source_ref_id": chunk["source_ref_id"],
            "evidence_type": chunk["evidence_type"],
            "chunk_text":    chunk["chunk_text"],
            "embedding":     vector,
            "created_at":    now,
        })
        if i < len(candidate_chunks) - 1:
            time.sleep(0.5)

    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors for candidate_embeddings: {errors}")
    return len(rows)
