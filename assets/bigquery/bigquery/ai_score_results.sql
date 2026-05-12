/* @bruin

type: bq.table
name: fitcv.ai_score_results
description: "AI reranking results — one row per scored job from the vector shortlist."

columns:
  - name: job_url
    description: "FK → structured_jobs.job_url"
  - name: ai_score
    description: "Match score from 0.0 (no fit) to 1.0 (perfect fit)"
  - name: fit_label
    description: "strong (>=0.7) | stretch (0.4-0.69) | skip (<0.4)"
  - name: score_reasoning
    description: "Free-text explanation from the model"
  - name: matched_strengths
    description: "Candidate strengths relevant to this JD"
  - name: key_risks
    description: "Gaps or risks flagged by the model"
  - name: scored_at
    description: "Timestamp of scoring run"

@bruin */

CREATE TABLE IF NOT EXISTS fitcv.ai_score_results (
  job_url           STRING         NOT NULL  OPTIONS (description = "FK → structured_jobs.job_url"),
  ai_score          FLOAT64                  OPTIONS (description = "0.0 (no fit) – 1.0 (perfect fit)"),
  fit_label         STRING                   OPTIONS (description = "strong | stretch | skip"),
  score_reasoning   STRING                   OPTIONS (description = "Free-text explanation from the model"),
  matched_strengths ARRAY<STRING>            OPTIONS (description = "Candidate strengths relevant to this JD"),
  key_risks         ARRAY<STRING>            OPTIONS (description = "Gaps or risks flagged by the model"),
  scored_at         TIMESTAMP
)
OPTIONS (description = "AI reranking results — ML.GENERATE_TEXT scored shortlist jobs");
