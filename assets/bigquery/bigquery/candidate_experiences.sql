/* @bruin

type: bq.table
name: fitcv.candidate_experiences
description: "Work experiences from the candidate profile, one row per bullet point."

@bruin */

CREATE TABLE IF NOT EXISTS fitcv.candidate_experiences (
  exp_id        STRING    NOT NULL  OPTIONS (description = "Experience ID, e.g. exp_1"),
  role          STRING              OPTIONS (description = "Job title / role"),
  company       STRING              OPTIONS (description = "Company name"),
  location      STRING              OPTIONS (description = "Office location"),
  start_date    STRING              OPTIONS (description = "Start date, YYYY-MM"),
  end_date      STRING              OPTIONS (description = "End date, YYYY-MM or present"),
  bullet_index  INT64               OPTIONS (description = "0-based bullet order within this experience"),
  bullet_text   STRING              OPTIONS (description = "Achievement / responsibility text"),
  skills        ARRAY<STRING>       OPTIONS (description = "Skills demonstrated in this bullet"),
  measurable_impact STRING          OPTIONS (description = "Quantified impact if present"),
  updated_at    TIMESTAMP
)
OPTIONS (description = "Candidate work experience bullets");
