/* @bruin

type: bq.table
name: fitcv.candidate_projects
description: "Personal / side projects from the candidate profile."

@bruin */

CREATE TABLE IF NOT EXISTS fitcv.candidate_projects (
  project_id      STRING    NOT NULL  OPTIONS (description = "Project ID, e.g. proj_1"),
  name            STRING              OPTIONS (description = "Project name"),
  skills          ARRAY<STRING>       OPTIONS (description = "Skills used"),
  business_value  STRING              OPTIONS (description = "What it delivered / why it matters"),
  evidence        STRING              OPTIONS (description = "URL to repo, demo, or writeup"),
  updated_at      TIMESTAMP
)
OPTIONS (description = "Candidate personal and side projects");
