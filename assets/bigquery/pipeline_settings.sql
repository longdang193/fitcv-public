CREATE TABLE IF NOT EXISTS `{project}.{dataset}.pipeline_settings` (
  setting_key        STRING    NOT NULL OPTIONS(description="Namespaced key, e.g. pipeline.final_top_n"),
  setting_value_json STRING    NOT NULL OPTIONS(description="JSON-encoded value, e.g. 10 or 0.40"),
  updated_by         STRING,
  updated_at         TIMESTAMP NOT NULL
);
