-- DDL for fitcv.application_tracker
-- Tracks the application status for each job/CV version pair.
-- tracker_id is the PK.
-- cv_version_id is a FK → cv_versions.version_id (explicit relationship).
-- status is a free string validated against config["application_statuses"] at write time.

CREATE TABLE IF NOT EXISTS `fitcv-491123.fitcv.application_tracker` (
  tracker_id      STRING    NOT NULL OPTIONS(description="UUID4, PK of this tracker row"),
  job_url         STRING    NOT NULL OPTIONS(description="LinkedIn job posting URL (FK → structured_jobs)"),
  cv_version_id   STRING             OPTIONS(description="FK → cv_versions.version_id; NULL if no CV generated yet"),
  status          STRING    NOT NULL OPTIONS(description="Application status; validated against config[application_statuses]"),
  notes           STRING             OPTIONS(description="Free-text notes (e.g. interview details, recruiter name)"),
  updated_at      TIMESTAMP NOT NULL
);
