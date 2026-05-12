/* @bruin

type: bq.table
name: fitcv.raw_jobs
description: "Raw LinkedIn job postings as ingested from the Apify scraper. Schema mirrors the scraper output 1:1 plus an ingestion audit column. job_url is the natural primary key."

columns:
  - name: job_url
    description: "LinkedIn job URL — natural primary key"
  - name: title
    description: "Job title as scraped"
  - name: location
    description: "Free-text location string, e.g. Berlin, Berlin, Germany"
  - name: posted_time
    description: "Relative human-readable time, e.g. 3 weeks ago"
  - name: published_at
    description: "ISO date when the job was published"
  - name: company_name
    description: "Company display name"
  - name: company_url
    description: "LinkedIn company page URL"
  - name: company_id
    description: "LinkedIn internal company ID"
  - name: description
    description: "Full job description text, newline-delimited"
  - name: applications_count
    description: "Raw applicants string, e.g. 61 applicants or Over 200 applicants"
  - name: contract_type
    description: "Full-time / Part-time / Internship / Contract"
  - name: experience_level
    description: "Entry level / Mid-Senior level / Associate / Director / Internship"
  - name: work_type
    description: "e.g. Information Technology"
  - name: sector
    description: "e.g. Banking"
  - name: salary
    description: "Raw salary string or empty string"
  - name: apply_url
    description: "External application URL"
  - name: apply_type
    description: "EASY_APPLY or EXTERNAL"
  - name: raw_json
    description: "Full original JSON object for auditability"
  - name: ingested_at
    description: "Pipeline ingestion timestamp"

@bruin */

CREATE TABLE IF NOT EXISTS fitcv.raw_jobs (
  job_url           STRING    NOT NULL OPTIONS (description = "LinkedIn job URL — natural primary key"),
  title             STRING    OPTIONS (description = "Job title as scraped"),
  location          STRING    OPTIONS (description = "Free-text location string"),
  posted_time       STRING    OPTIONS (description = "Relative time string, e.g. 3 weeks ago"),
  published_at      DATE      OPTIONS (description = "ISO date the job was published"),
  company_name      STRING    OPTIONS (description = "Company display name"),
  company_url       STRING    OPTIONS (description = "LinkedIn company page URL"),
  company_id        STRING    OPTIONS (description = "LinkedIn internal company ID"),
  description       STRING    OPTIONS (description = "Full JD text, newline-delimited"),
  applications_count STRING   OPTIONS (description = "Raw applicants string"),
  contract_type     STRING    OPTIONS (description = "Full-time / Part-time / Internship / Contract"),
  experience_level  STRING    OPTIONS (description = "Entry level / Mid-Senior level / Associate / Director / Internship"),
  work_type         STRING    OPTIONS (description = "e.g. Information Technology"),
  sector            STRING    OPTIONS (description = "e.g. Banking"),
  salary            STRING    OPTIONS (description = "Raw salary string or empty"),
  apply_url         STRING    OPTIONS (description = "External application URL"),
  apply_type        STRING    OPTIONS (description = "EASY_APPLY or EXTERNAL"),
  raw_json          JSON      OPTIONS (description = "Full original JSON for auditability"),
  ingested_at       TIMESTAMP OPTIONS (description = "Pipeline ingestion timestamp")
)
OPTIONS (
  description = "Raw LinkedIn job postings ingested from the Apify scraper"
);
