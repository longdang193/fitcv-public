/* @bruin

type: bq.table
name: fitcv.candidate_embeddings
description: "Semantic embeddings for candidate evidence — projects, experience bullets, achievements."

columns:
  - name: evidence_id
    description: "Unique chunk ID, e.g. proj_1, exp_1_bullet_0, ach_1"
  - name: source_ref_id
    description: "Maps back to originating YAML ID: exp_id, project_id, or achievement_id. Makes retrieval traceable."
  - name: evidence_type
    description: "project | experience_bullet | achievement"
  - name: chunk_text
    description: "Human-readable text that was embedded"
  - name: embedding
    description: "Dense vector from Vertex AI text-embedding-005"

@bruin */

CREATE TABLE IF NOT EXISTS fitcv.candidate_embeddings (
  evidence_id   STRING         NOT NULL  OPTIONS (description = "Unique chunk ID, e.g. proj_1 / exp_1_bullet_0 / ach_1"),
  source_ref_id STRING                   OPTIONS (description = "Originating YAML ID for traceability (exp_id / project_id / achievement_id)"),
  evidence_type STRING                   OPTIONS (description = "project | experience_bullet | achievement"),
  chunk_text    STRING                   OPTIONS (description = "Human-readable text that was embedded"),
  embedding     ARRAY<FLOAT64>           OPTIONS (description = "Dense vector from Vertex AI text-embedding-005"),
  created_at    TIMESTAMP
)
OPTIONS (description = "Candidate evidence semantic embeddings — one chunk per project/bullet/achievement");
