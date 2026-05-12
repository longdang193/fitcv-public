/* @bruin

type: bq.table
name: fitcv.candidate_skills
description: "Deduplicated skill inventory with level, years, and evidence pointers."

@bruin */

CREATE TABLE IF NOT EXISTS fitcv.candidate_skills (
  skill_name    STRING    NOT NULL  OPTIONS (description = "Canonical skill name"),
  level         STRING              OPTIONS (description = "beginner / intermediate / advanced"),
  years         INT64               OPTIONS (description = "Years of active use"),
  evidence_refs ARRAY<STRING>       OPTIONS (description = "IDs of experiences/projects that demonstrate this skill"),
  updated_at    TIMESTAMP
)
OPTIONS (description = "Candidate skill inventory");
