-- DDL for fitcv.cv_versions
-- Stores one record per generated CV version.
-- version_id is the PK; application_tracker.cv_version_id is the FK referencing this field.
-- CV markdown is stored directly here; no separate generated_cvs table (v1).

CREATE TABLE IF NOT EXISTS `fitcv-491123.fitcv.cv_versions` (
  version_id          STRING    NOT NULL OPTIONS(description="UUID4, PK of this generated CV record"),
  run_id              STRING             OPTIONS(description="Logical FK to pipeline_runs"),
  job_url             STRING    NOT NULL OPTIONS(description="LinkedIn job posting URL (FK → structured_jobs)"),
  enrichment_version  STRING             OPTIONS(description="Enrichment model/prompt version used"),
  vector_rank         INT64              OPTIONS(description="Rank from vector shortlist stage"),
  ai_score            FLOAT64            OPTIONS(description="AI scoring result [0.0, 1.0]"),
  final_score         FLOAT64            OPTIONS(description="Composite final score [0.0, 1.0]"),
  evidence_ids        ARRAY<STRING>      OPTIONS(description="UUIDs of evidence items selected (FK → evidence_selections.evidence_id)"),
  prompt_version      STRING             OPTIONS(description="CV generation prompt version identifier"),
  cv_prompt_version   STRING             OPTIONS(description="Structured CV generation prompt version identifier"),
  cv_generation_model STRING             OPTIONS(description="Model used to generate the structured CV artifact"),
  cv_schema_version   STRING             OPTIONS(description="Structured CV schema version, e.g. cv_doc_v1"),
  cv_structured_json  STRING             OPTIONS(description="JSON-serialised structured CV document"),
  cv_markdown         STRING    NOT NULL OPTIONS(description="Full generated CV in markdown format"),
  gap_summary         STRING             OPTIONS(description="JSON-serialised gap analysis result"),
  fit_classification  STRING             OPTIONS(description="strong | stretch | skip"),
  generated_at        TIMESTAMP NOT NULL
);
