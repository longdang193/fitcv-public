CREATE TABLE IF NOT EXISTS `{project}.{dataset}.pipeline_run_events` (
  run_id       STRING    NOT NULL OPTIONS(description="FK -> pipeline_runs.run_id"),
  event_id     STRING    NOT NULL OPTIONS(description="UUID4 event identifier"),
  stage        STRING    NOT NULL,
  level        STRING    NOT NULL OPTIONS(description="info | warning | error"),
  message      STRING    NOT NULL,
  payload_json STRING,
  created_at   TIMESTAMP NOT NULL
);
