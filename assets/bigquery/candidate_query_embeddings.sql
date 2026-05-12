/* @bruin

type: bq.table
name: fitcv.candidate_query_embeddings
description: "Cached semantic embeddings for the single shortlist candidate query text."

columns:
  - name: candidate_query_signature
    description: "Stable deterministic hash of the bounded shortlist candidate-query components"
  - name: candidate_query_contract_fingerprint
    description: "Fingerprint of the shortlist candidate-query embedding contract (model + query schema version)"
  - name: candidate_query_text
    description: "The deterministic shortlist candidate query text that was embedded"
  - name: candidate_query_components_json
    description: "Serialized bounded shortlist candidate-query components retained for debugging"
  - name: embedding
    description: "Dense vector from Vertex AI text-embedding-005"

@bruin */

CREATE TABLE IF NOT EXISTS fitcv.candidate_query_embeddings (
  candidate_query_signature             STRING         NOT NULL OPTIONS (description = "Stable shortlist candidate-query signature"),
  candidate_query_contract_fingerprint  STRING                  OPTIONS (description = "Candidate-query embedding contract fingerprint"),
  candidate_query_text                  STRING                  OPTIONS (description = "Deterministic shortlist candidate query text"),
  candidate_query_components_json       STRING                  OPTIONS (description = "Serialized bounded candidate-query components for debugging"),
  embedding                             ARRAY<FLOAT64>          OPTIONS (description = "Dense vector from Vertex AI text-embedding-005"),
  created_at                            TIMESTAMP
)
OPTIONS (description = "Cached embeddings for the single shortlist candidate query text");

