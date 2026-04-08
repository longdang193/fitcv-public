/* @bruin

type: bq.table
name: fitcv.candidate_certifications
description: "Professional certifications from the candidate profile."

@bruin */

CREATE TABLE IF NOT EXISTS fitcv.candidate_certifications (
  name       STRING    OPTIONS (description = "Certification name, e.g. Google Professional Data Engineer"),
  issuer     STRING    OPTIONS (description = "Issuing organization"),
  year       INT64     OPTIONS (description = "Year obtained"),
  updated_at TIMESTAMP
)
OPTIONS (description = "Candidate professional certifications");
