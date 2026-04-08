/* @bruin

type: bq.table
name: fitcv.candidate_achievements
description: "Quantified achievements from the candidate profile for evidence retrieval."

@bruin */

CREATE TABLE IF NOT EXISTS fitcv.candidate_achievements (
  achievement_id  STRING    NOT NULL  OPTIONS (description = "Achievement ID, e.g. ach_1"),
  text            STRING              OPTIONS (description = "Achievement description with metric"),
  category        STRING              OPTIONS (description = "performance / productivity / impact / leadership"),
  evidence_refs   ARRAY<STRING>       OPTIONS (description = "IDs of experiences/projects this achievement comes from"),
  updated_at      TIMESTAMP
)
OPTIONS (description = "Candidate quantified achievements");
