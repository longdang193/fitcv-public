/* @bruin

type: bq.table
name: fitcv.vector_shortlist
description: "Semantic retrieval shortlist from VECTOR_SEARCH — one row per job per retrieval run."

columns:
  - name: job_url
    description: "FK → structured_jobs.job_url"
  - name: vector_rank
    description: "Rank within this retrieval run (1 = most similar to candidate query)"
  - name: vector_similarity
    description: "Cosine similarity between candidate query embedding and job summary embedding (higher = more similar)"
  - name: retrieval_strategy
    description: "v1: always job_summary_v1 (Option A — one candidate summary vector)"
  - name: retrieved_at
    description: "Timestamp of retrieval run"

@bruin */

CREATE TABLE IF NOT EXISTS fitcv.vector_shortlist (
  job_url             STRING         NOT NULL  OPTIONS (description = "FK → structured_jobs.job_url"),
  vector_rank         INT64                    OPTIONS (description = "Rank within retrieval run (1 = most similar)"),
  vector_similarity   FLOAT64                  OPTIONS (description = "Cosine similarity from VECTOR_SEARCH"),
  retrieval_strategy  STRING                   OPTIONS (description = "v1: job_summary_v1 (Option A — one candidate vector)"),
  retrieved_at        TIMESTAMP
)
OPTIONS (description = "Semantic retrieval shortlist — top-N jobs by vector similarity within rule-filtered universe");
