"""Semantic retrieval via BigQuery VECTOR_SEARCH.

v1 design (Option A — one candidate summary embedding):
- Build one candidate query text: headline + top skills + preferred domains
- Embed it with Vertex AI text-embedding-005
- Search fitcv.job_embeddings WHERE chunk_type = 'job_summary'
- Restrict to job_url IN (passed_job_urls) — the rule-filtered universe
- Return top-N results ranked by cosine similarity

Option B (multi-evidence aggregation) is deferred to v2.

Public API
----------
build_candidate_query_text : deterministic candidate query string (no embedding call)
build_vector_search_query  : BigQuery VECTOR_SEARCH SQL string
run_vector_search          : embed + query + return shortlist rows (integration)
store_shortlist            : insert into fitcv.vector_shortlist (integration)

Config keys consumed (from pipeline.yaml)
-----------------------------------------
config["vector_top_n"]              : default top_n for VECTOR_SEARCH (default 50)
config["vector_max_candidate_skills"]: max skills in candidate query text (default 15)
config["retrieval_strategy"]        : stored in vector_shortlist (default "job_summary_v1")
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

from fitcv.candidate import flatten_skills, infer_role_family
from fitcv.embeddings import generate_embedding, get_shortlist_embedding_model

DEFAULT_RECENT_ROLE_COUNT = 3
DEFAULT_ROLE_FAMILY_HINT_COUNT = 3
DEFAULT_DOMAIN_HINT_COUNT = 5
CANDIDATE_QUERY_SCHEMA_VERSION = "shortlist_candidate_query_v1"
REUSED_CACHED_QUERY_EMBEDDING_STATUS = "reused_cached_query_embedding"
FRESH_QUERY_EMBEDDING_STATUS = "fresh_query_embedding"

logger = logging.getLogger(__name__)


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
    return " ".join(str(value or "").split()).strip()


def _canonicalize_for_hash(value: Any) -> Any:
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


def build_candidate_query_components(
    profile: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the bounded deterministic component groups used for shortlist retrieval."""
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

    flattened_skills: list[str] = []
    seen_skills: set[str] = set()
    for skill in flatten_skills(profile):
        _append_unique_text(flattened_skills, skill, seen_skills)
        if len(flattened_skills) >= max_skills:
            break

    role_family_hints: list[str] = []
    seen_role_families: set[str] = set()
    for role_family in prefs.get("role_families", []) or []:
        _append_unique_text(role_family_hints, str(role_family), seen_role_families)
        if len(role_family_hints) >= DEFAULT_ROLE_FAMILY_HINT_COUNT:
            break
    if len(role_family_hints) < DEFAULT_ROLE_FAMILY_HINT_COUNT:
        inferred_target_family = infer_role_family(target_role)
        if inferred_target_family:
            _append_unique_text(role_family_hints, inferred_target_family, seen_role_families)
    if len(role_family_hints) < DEFAULT_ROLE_FAMILY_HINT_COUNT:
        for experience in profile.get("experiences", []) or []:
            explicit_family = str(experience.get("role_family") or "").strip()
            inferred_family = infer_role_family(
                str(experience.get("role") or ""),
                explicit_family=explicit_family or None,
            )
            if inferred_family:
                _append_unique_text(role_family_hints, inferred_family, seen_role_families)
            if len(role_family_hints) >= DEFAULT_ROLE_FAMILY_HINT_COUNT:
                break

    domain_hints: list[str] = []
    seen_domains: set[str] = set()
    for domain in prefs.get("domains", []) or []:
        _append_unique_text(domain_hints, str(domain), seen_domains)
        if len(domain_hints) >= DEFAULT_DOMAIN_HINT_COUNT:
            break
    if len(domain_hints) < DEFAULT_DOMAIN_HINT_COUNT:
        for experience in profile.get("experiences", []) or []:
            for domain_tag in experience.get("domain_tags", []) or []:
                _append_unique_text(domain_hints, str(domain_tag), seen_domains)
                if len(domain_hints) >= DEFAULT_DOMAIN_HINT_COUNT:
                    break
            if len(domain_hints) >= DEFAULT_DOMAIN_HINT_COUNT:
                break
    if len(domain_hints) < DEFAULT_DOMAIN_HINT_COUNT:
        for project in profile.get("projects", []) or []:
            for domain_tag in project.get("domain_tags", []) or []:
                _append_unique_text(domain_hints, str(domain_tag), seen_domains)
                if len(domain_hints) >= DEFAULT_DOMAIN_HINT_COUNT:
                    break
            if len(domain_hints) >= DEFAULT_DOMAIN_HINT_COUNT:
                break

    return {
        "headline": headline,
        "target_role": target_role,
        "recent_roles": recent_roles,
        "role_family_hints": role_family_hints,
        "flattened_skills": flattened_skills,
        "domain_hints": domain_hints,
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
        "role_family_hints": [
            _normalize_query_scalar(value)
            for value in list(components.get("role_family_hints") or [])
            if _normalize_query_scalar(value)
        ],
        "flattened_skills": [
            _normalize_query_scalar(value)
            for value in list(components.get("flattened_skills") or [])
            if _normalize_query_scalar(value)
        ],
        "domain_hints": [
            _normalize_query_scalar(value)
            for value in list(components.get("domain_hints") or [])
            if _normalize_query_scalar(value)
        ],
    }
    payload = {
        key: value
        for key, value in payload.items()
        if value not in ("", [], None)
    }
    canonical_payload = _canonicalize_for_hash(payload)
    payload_json = json.dumps(canonical_payload, sort_keys=True, separators=(",", ":"))
    signature = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    return {
        "payload": payload,
        "payload_json": json.dumps(payload, sort_keys=True, separators=(",", ":")),
        "signature": signature,
    }


def build_candidate_query_embedding_contract_fingerprint(config: dict[str, Any]) -> dict[str, Any]:
    """Fingerprint shortlist candidate-query embedding behavior to invalidate reuse."""
    payload = {
        "embedding_model": get_shortlist_embedding_model(config),
        "candidate_query_schema_version": CANDIDATE_QUERY_SCHEMA_VERSION,
    }
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    fingerprint = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    return {
        "payload": payload,
        "fingerprint": fingerprint,
    }

def build_candidate_query_text(
    profile: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> str:
    """Build the single candidate query string for v1 Option A retrieval.

    Combines: headline + target role + recent roles + top skills
    (up to vector_max_candidate_skills) + preferred domains.
    Deterministic — same profile always produces the same text.
    No embedding call; used as input to generate_embedding().

    Format:
        Candidate: <headline>
        Skills: <comma-joined skills>
        Target domains: <comma-joined domains>
    """
    components = build_candidate_query_components(profile, config)
    parts: list[str] = []

    headline = str(components.get("headline") or "").strip()
    if headline:
        parts.append(f"Candidate: {headline}")

    target_role = str(components.get("target_role") or "").strip()
    if target_role:
        parts.append(f"Target role: {target_role}")

    recent_roles = list(components.get("recent_roles") or [])
    if recent_roles:
        parts.append(f"Recent roles: {', '.join(recent_roles)}")

    role_family_hints = list(components.get("role_family_hints") or [])
    if role_family_hints:
        parts.append(f"Role families: {', '.join(role_family_hints)}")

    skill_names = list(components.get("flattened_skills") or [])
    if skill_names:
        parts.append(f"Skills: {', '.join(skill_names)}")

    domains = list(components.get("domain_hints") or [])
    if domains:
        parts.append(f"Domain hints: {', '.join(str(d) for d in domains)}")

    return "\n".join(parts)


def _load_latest_candidate_query_embedding(
    *,
    client: Any,
    table_ref: str,
    candidate_query_signature: str,
) -> dict[str, Any] | None:
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
    return {
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


def resolve_candidate_query_embedding(
    profile: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    """Return the shortlist candidate query plus a reused or fresh embedding vector."""
    from google.cloud import bigquery  # type: ignore[import-untyped]
    from google.oauth2 import service_account  # type: ignore[import-untyped]

    components = build_candidate_query_components(profile, config)
    query_text = build_candidate_query_text(profile, config)
    signature_record = build_candidate_query_signature_record(components)
    contract_record = build_candidate_query_embedding_contract_fingerprint(config)

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    key_path = str(config["service_account_key"])
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project, credentials=credentials)
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
        cached_row.get("candidate_query_contract_fingerprint") == contract_record["fingerprint"]
    ):
        return {
            "text": query_text,
            "components": components,
            "embedding": list(cached_row.get("embedding") or []),
            "candidate_query_signature": signature_record["signature"],
            "candidate_query_contract_fingerprint": contract_record["fingerprint"],
            "candidate_query_reuse_status": REUSED_CACHED_QUERY_EMBEDDING_STATUS,
        }

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
    return {
        "text": query_text,
        "components": components,
        "embedding": embedding_vector,
        "candidate_query_signature": signature_record["signature"],
        "candidate_query_contract_fingerprint": contract_record["fingerprint"],
        "candidate_query_reuse_status": FRESH_QUERY_EMBEDDING_STATUS,
    }


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
        url_list = ", ".join(f"'{u}'" for u in passed_job_urls)
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
  WHERE chunk_type = 'job_summary' AND job_url IN ({url_list})
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

    effective_top_n = (
        top_n
        if top_n is not None
        else int((config.get("pipeline") or {}).get("vector_search_top_n") or config.get("vector_top_n", 50))
    )

    from google.cloud import bigquery  # type: ignore[import-untyped]
    from google.oauth2 import service_account  # type: ignore[import-untyped]

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    key_path = str(config["service_account_key"])

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project, credentials=credentials)

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
            bigquery.ArrayQueryParameter("candidate_embedding", "FLOAT64", embedding_vector)
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

    effective_strategy = retrieval_strategy or str(config.get("retrieval_strategy", "job_summary_v1"))

    from google.cloud import bigquery  # type: ignore[import-untyped]
    from google.oauth2 import service_account  # type: ignore[import-untyped]

    project = str(config["gcp_project"])
    dataset = str(config["bigquery_dataset"])
    key_path = str(config["service_account_key"])

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(project=project, credentials=credentials)
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
