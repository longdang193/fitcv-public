-- DDL for fitcv.gap_analysis
-- Stores per-job skill gap results from Layer 4 gap analysis.
-- job_url is the business key (FK → structured_jobs).

CREATE TABLE IF NOT EXISTS `fitcv-491123.fitcv.gap_analysis` (
  job_url          STRING        NOT NULL OPTIONS(description="LinkedIn job posting URL (FK → structured_jobs)"),
  matched_skills   ARRAY<STRING>           OPTIONS(description="Required skills fully matched by the candidate"),
  partial_skills   ARRAY<STRING>           OPTIONS(description="Required skills partially matched (synonym or subset)"),
  missing_skills   ARRAY<STRING>           OPTIONS(description="Required skills absent from candidate profile"),
  years_risk       BOOL          NOT NULL  OPTIONS(description="True when candidate years_experience < years_required"),
  overclaim_risk   ARRAY<STRING>           OPTIONS(description="Human-readable risk strings (years_gap, leadership, etc.)"),
  analysed_at      TIMESTAMP     NOT NULL 
);
