/* @bruin

type: bq.table
name: fitcv.run_structured_jobs
description: "Immutable, run-scoped enrichment outputs. One row per run_id + job_url. Append-only — never updated in place. Used for per-run debugging and inspection of what enrichment produced for a specific pipeline run."

columns:
  - name: run_id
    description: "Pipeline run identifier — FK to pipeline_runs.run_id"
  - name: job_url
    description: "LinkedIn job URL — part of logical composite key"
  - name: location_type
    description: "LLM-enriched: canonical remote / hybrid / onsite (lowercase only)"
  - name: location_type_raw
    description: "Raw enrichment text for location_type before canonical normalization"
  - name: seniority
    description: "LLM-enriched: normalized level from JD text — junior / mid / senior / lead"
  - name: seniority_raw
    description: "Raw enrichment text for seniority before canonical normalization"
  - name: domain
    description: "LLM-enriched: business/industry domain (e.g. banking, fintech, healthcare)"
  - name: domain_raw
    description: "Raw enrichment text for domain before canonical normalization"
  - name: job_family
    description: "LLM-enriched: role category (e.g. data_engineering, analytics, data_science)"
  - name: job_family_raw
    description: "Raw enrichment text for job_family before canonical normalization"
  - name: enrichment_version
    description: "Prompt/schema version used for this enrichment (e.g. v1)"
  - name: enrichment_model
    description: "Model name used for extraction (e.g. gemini-2.0-flash)"

@bruin */

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.run_structured_jobs` (
  run_id               STRING    NOT NULL  OPTIONS (description = "Pipeline run identifier — part of logical composite PK"),
  job_url              STRING    NOT NULL  OPTIONS (description = "LinkedIn job URL — part of logical composite PK"),
  title                STRING              OPTIONS (description = "Job title"),
  company_name         STRING              OPTIONS (description = "Company display name"),
  location             STRING              OPTIONS (description = "Free-text location string"),
  contract_type        STRING              OPTIONS (description = "Full-time / Part-time / Internship / Contract"),
  experience_level     STRING              OPTIONS (description = "Raw LinkedIn label — NOT the same as enriched seniority"),
  published_at         DATE                OPTIONS (description = "ISO date the job was published"),
  location_type_raw    STRING              OPTIONS (description = "Raw enrichment text for location_type before canonical normalization"),
  location_type        STRING              OPTIONS (description = "LLM: remote / hybrid / onsite"),
  seniority_raw        STRING              OPTIONS (description = "Raw enrichment text for seniority before canonical normalization"),
  seniority            STRING              OPTIONS (description = "LLM: junior / mid / senior / lead — inferred from JD text"),
  required_skills      ARRAY<STRING>       OPTIONS (description = "LLM-extracted required skills"),
  required_skills_canonical ARRAY<STRING>  OPTIONS (description = "Canonicalized required skill labels for downstream matching"),
  required_skill_entities_json STRING      OPTIONS (description = "JSON payload preserving raw required-skill phrases with canonical labels"),
  preferred_skills     ARRAY<STRING>       OPTIONS (description = "LLM-extracted preferred/nice-to-have skills"),
  preferred_skills_canonical ARRAY<STRING> OPTIONS (description = "Canonicalized preferred skill labels for downstream matching"),
  preferred_skill_entities_json STRING     OPTIONS (description = "JSON payload preserving raw preferred-skill phrases with canonical labels"),
  responsibilities     ARRAY<STRING>       OPTIONS (description = "LLM-extracted responsibilities"),
  responsibilities_canonical ARRAY<STRING> OPTIONS (description = "Canonicalized responsibility phrases for downstream reuse"),
  domain_raw           STRING              OPTIONS (description = "Raw enrichment text for domain before canonical normalization"),
  domain               STRING              OPTIONS (description = "LLM: business/industry domain"),
  tech_stack           ARRAY<STRING>       OPTIONS (description = "LLM-extracted technologies"),
  tech_stack_canonical ARRAY<STRING>       OPTIONS (description = "Canonicalized technology labels for downstream reuse"),
  years_experience_min INT64               OPTIONS (description = "LLM-extracted minimum years required"),
  years_experience_max INT64               OPTIONS (description = "LLM-extracted maximum years mentioned"),
  keywords             ARRAY<STRING>       OPTIONS (description = "LLM-extracted searchable keywords"),
  keywords_canonical   ARRAY<STRING>       OPTIONS (description = "Canonicalized keyword labels for downstream reuse"),
  job_family_raw       STRING              OPTIONS (description = "Raw enrichment text for job_family before canonical normalization"),
  job_family           STRING              OPTIONS (description = "LLM: role category — data_engineering / analytics / data_science / ml_engineering"),
  mapping_suggestions_json STRING          OPTIONS (description = "JSON payload of reviewable alias-to-canonical mapping suggestions produced during enrichment"),
  description_cleaned  STRING              OPTIONS (description = "Whitespace-normalized description text"),
  enrichment_version   STRING              OPTIONS (description = "Prompt/schema version, e.g. v1"),
  enrichment_model     STRING              OPTIONS (description = "Model name used, e.g. gemini-2.0-flash"),
  enriched_at          TIMESTAMP           OPTIONS (description = "Enrichment timestamp"),
  raw_job_fingerprint  STRING              OPTIONS (description = "Stable hash of normalized pre-enrichment raw-job inputs used for reuse lookup"),
  enrich_contract_fingerprint STRING       OPTIONS (description = "Stable hash of the effective enrich prompt/model/schema contract used for this row"),
  enrich_reuse_status  STRING              OPTIONS (description = "fresh_enrichment or reused_cached_enrichment provenance for this run-scoped row")
)
OPTIONS (
  description = "Immutable run-scoped enrichment outputs — one row per run_id + job_url, append-only"
);
