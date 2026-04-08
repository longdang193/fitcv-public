/* @bruin

type: bq.table
name: fitcv.candidate_profile
description: "Top-level candidate profile: name, headline, summary, and preferences for job matching."

@bruin */

CREATE TABLE IF NOT EXISTS fitcv.candidate_profile (
  profile_id        STRING    NOT NULL  OPTIONS (description = "UUID — single active profile per run"),
  name              STRING              OPTIONS (description = "Full name"),
  headline          STRING              OPTIONS (description = "One-line professional headline"),
  summary           STRING              OPTIONS (description = "Multi-sentence professional summary"),
  location_types    ARRAY<STRING>       OPTIONS (description = "Preferred work types: remote / hybrid / onsite"),
  domains           ARRAY<STRING>       OPTIONS (description = "Target industry domains"),
  seniority_target  STRING              OPTIONS (description = "Target seniority: junior / mid / senior / lead"),
  exclude_contract_types    ARRAY<STRING>  OPTIONS (description = "Contract types to exclude (e.g. Internship)"),
  exclude_experience_levels ARRAY<STRING>  OPTIONS (description = "LinkedIn experience levels to exclude"),
  updated_at        TIMESTAMP           OPTIONS (description = "Last upserted at")
)
OPTIONS (description = "Top-level candidate profile");
