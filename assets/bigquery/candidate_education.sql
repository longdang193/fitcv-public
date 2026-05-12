/* @bruin

type: bq.table
name: fitcv.candidate_education
description: "Education history from the candidate profile."

@bruin */

CREATE TABLE IF NOT EXISTS fitcv.candidate_education (
  degree      STRING    OPTIONS (description = "Degree name, e.g. B.Sc. Computer Science"),
  institution STRING    OPTIONS (description = "University or institution name"),
  year        INT64     OPTIONS (description = "Graduation year"),
  updated_at  TIMESTAMP
)
OPTIONS (description = "Candidate education history");
