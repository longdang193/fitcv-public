-- DDL for fitcv.evidence_selections
-- Stores the evidence items selected per job during Layer 4 personalization.
-- evidence_id references the stable UUID5 computed by evidence.normalise_evidence_item.

CREATE TABLE IF NOT EXISTS `fitcv-491123.fitcv.evidence_selections` (
  job_url        STRING    NOT NULL OPTIONS(description="LinkedIn job posting URL (FK → structured_jobs)"),
  evidence_id    STRING    NOT NULL OPTIONS(description="Stable UUID5 derived from evidence_type + name"),
  evidence_type  STRING    NOT NULL OPTIONS(description="project | achievement | experience_bullet"),
  name           STRING    NOT NULL OPTIONS(description="Human-readable label for the evidence item"),
  skills         ARRAY<STRING>      OPTIONS(description="Skills associated with this item (may be empty for achievements)"),
  business_value STRING             OPTIONS(description="Optional business impact text from the profile"),
  score          FLOAT64   NOT NULL OPTIONS(description="Weighted relevance score [0.0, 1.0]"),
  source_ref     STRING    NOT NULL OPTIONS(description="Profile key path (e.g. projects[0], achievements[1])"),
  selected_at    TIMESTAMP NOT NULL
);
