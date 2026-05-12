/* @bruin

type: bq.table
name: fitcv.rule_filter_results
description: "Rule-based job filter results — passed and rejected jobs with rejection reasons."

@bruin */

CREATE TABLE IF NOT EXISTS fitcv.rule_filter_results (
  job_url       STRING    NOT NULL  OPTIONS (description = "FK → structured_jobs.job_url"),
  run_id        STRING              OPTIONS (description = "Pipeline run identifier for run-scoped inspection"),
  passed        BOOL                OPTIONS (description = "True if job passed all filters"),
  reasons       ARRAY<STRING>       OPTIONS (description = "Rejection reason codes (empty when passed = TRUE)"),
  marks_json    STRING              OPTIONS (description = "JSON-encoded non-blocking rule-filter marks for passed or rejected jobs"),
  filtered_at   TIMESTAMP           OPTIONS (description = "When this filter decision was made")
)
OPTIONS (description = "Rule-based job filter log. One row per job per filter run.");
