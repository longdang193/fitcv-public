/* @bruin

type: bq.table
name: fitcv.job_embeddings
description: "Semantic embeddings for job postings. v1: one row per job (chunk_type = job_summary) used for VECTOR_SEARCH shortlist ranking."

columns:
  - name: job_url
    description: "FK to fitcv.structured_jobs.job_url"
  - name: chunk_type
    description: "v1: always job_summary. Future: responsibilities, required_skills, etc."
  - name: chunk_text
    description: "The labelled-section text that was embedded"
  - name: embedding
    description: "Dense vector from Vertex AI text-embedding-005"
  - name: embedding_input_signature
    description: "Stable structured shortlist-summary hash used to decide whether job_summary embedding reuse is safe"
  - name: embedding_contract_fingerprint
    description: "Fingerprint of the shortlist embedding contract (model + summary schema version)"
  - name: embedding_input_signature_payload_json
    description: "Serialized shortlist embedding-input payload retained for debugging reuse decisions"

@bruin */

CREATE TABLE IF NOT EXISTS fitcv.job_embeddings (
  job_url                              STRING         NOT NULL  OPTIONS (description = "FK → structured_jobs.job_url"),
  chunk_type                           STRING                   OPTIONS (description = "v1: always job_summary"),
  chunk_text                           STRING                   OPTIONS (description = "Labelled-section text that was embedded"),
  embedding                            ARRAY<FLOAT64>           OPTIONS (description = "Dense vector from Vertex AI text-embedding-005"),
  embedding_input_signature            STRING                   OPTIONS (description = "Stable structured shortlist-summary hash used for embedding reuse"),
  embedding_contract_fingerprint       STRING                   OPTIONS (description = "Embedding contract fingerprint for shortlist reuse invalidation"),
  embedding_input_signature_payload_json STRING                 OPTIONS (description = "Serialized shortlist embedding-input payload for debugging"),
  created_at                           TIMESTAMP
)
OPTIONS (description = "Job posting semantic embeddings — one summary vector per job in v1");
