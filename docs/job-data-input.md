---
doc_id: job-data-input
doc_type: data-contract
explains:
  components:
    - src/fitcv/ingest.py
    - src/fitcv/normalize.py
    - src/fitcv_cp/app.py
---

# Job-Data Input Pipeline (LinkedIn via Apify)

This doc explains where FitCV job-post input data comes from, which shapes are
accepted, what fields are expected, and what normalization/cleaning happens
before downstream stages run.

## Data source and scraping setup

FitCV job inputs come from scraped LinkedIn job posts produced via Apify actor:

- `bebity/linkedin-jobs-scraper`

FitCV does **not** scrape LinkedIn directly. It consumes exported actor output.

Recommended scraping workflow (reproducible):

1. Configure and run actor in Apify (search terms, location, max items, etc.).
2. Treat resulting Apify dataset as snapshot for one pipeline run.
3. Export dataset items as JSON (top-level array).
4. Store exported JSON file (and actor input JSON) alongside run metadata.

## Input formats (what FitCV accepts)

### 1) File input (`jobs_path`, recommended)

Control-plane run trigger accepts `jobs_path` pointing to a JSON file with:

- UTF-8 JSON
- top-level value **must be a JSON array**
- each element **must be an object** representing one job

Example (scraper/camelCase keys):

```json
[
  {
    "jobUrl": "https://de.linkedin.com/jobs/view/...",
    "title": "Data Engineer",
    "companyName": "ACME",
    "description": "...",
    "contractType": "Full-time",
    "experienceLevel": "Mid-Senior level"
  }
]
```

### 2) Apify dataset fetch (engineering helper)

`src/fitcv/ingest.py` includes `fetch_from_apify(config)` which loads dataset
items via Apify REST API:

- required config keys: `apify_dataset_id`, `apify_token`
- request uses `format=json&clean=true`
- returns list of job dicts in same shape as file input

Note: control-plane `POST /runs` currently does not expose a first-class “Apify
dataset input mode”. For repeatable runs, prefer exporting dataset to a file and
triggering via `jobs_path`.

## Output formats (what FitCV produces from input)

FitCV preserves two parallel representations:

1. **Raw provenance** (ingest persistence)
   - `raw_json` stores original job object as JSON string
2. **Normalized working shape** (normalize stage)
   - stable snake_case keys
   - derived parse fields for downstream logic

## Key fields collected

### Required fields (scraper contract)

Per job object, FitCV expects these scraper keys to exist:

- `jobUrl`
- `title`
- `companyName`
- `description`
- `contractType`
- `experienceLevel`

### Common optional fields (when present)

Common scraper fields FitCV consumes/persists when present:

- `location`
- `postedTime` (human-relative text; not a reliable timestamp)
- `publishedAt` (date-like string; timezone may be missing)
- `companyUrl`, `companyId`
- `applicationsCount` (localized text; parsed best-effort)
- `contractType`, `experienceLevel`, `workType`, `sector`
- `salary` (string; parsed best-effort)
- `applyUrl`, `applyType`
- `posterFullName`, `posterProfileUrl` (often empty)

Unknown keys remain preserved inside `raw_json`.

## Cleaning and transformation steps

### Field mapping: camelCase → snake_case

Selected scraper keys map to internal snake_case keys:

- `jobUrl` → `job_url`
- `postedTime` → `posted_time`
- `publishedAt` → `published_at`
- `companyName` → `company_name`
- `companyUrl` → `company_url`
- `companyId` → `company_id`
- `applicationsCount` → `applications_count`
- `contractType` → `contract_type`
- `experienceLevel` → `experience_level`
- `workType` → `work_type`
- `applyUrl` → `apply_url`
- `applyType` → `apply_type`
- `posterFullName` → `poster_full_name`
- `posterProfileUrl` → `poster_profile_url`

Keys not in mapping are preserved under their original name.

### Normalize stage transformations

Per job, normalize stage applies:

- description whitespace normalization (collapse repeated whitespace/newlines)
- `applicationsCount` best-effort parsing → `applications_count_int: int | None`
  - supports localized forms like “61 applicants”, “Mehr als 200 Bewerber”
  - treats “Be among the first N …” (and localized variants) as `0`
- `salary` best-effort parsing → `salary_structured: {min,max,currency,period} | None`
  - supports ranges like `€50,000.00/yr - €70,000.00/yr`

### Deduplication

Normalize stage removes duplicates to avoid repeated downstream cost:

- exact dedupe by `job_url`
- near-dedupe by `(company_id, title, sha256(description))`
  - keeps first occurrence, excludes later near-duplicates

## How data is used in this project

The pipeline is staged narrowing and grounding:

- `normalize` stabilizes and deduplicates raw scraped input
- `enrich` derives structured fields primarily from `title` and `description`
- `rule_filter` applies deterministic exclusion rules
- `shortlist` / `ranking` compute fit signals and select best jobs for deeper work
- `cv_analysis` / `cv_generation` run only on jobs that pass upstream gates

Practical implication: input quality (especially `description`) strongly affects
downstream quality and cost.

## Limitations and reproducibility notes

### Limitations

- Scraped data can be incomplete, localized, or inconsistent across runs.
- Job posts churn; identical “roles” may appear under new URLs or edited text.
- `postedTime` is often relative text (e.g., “6 days ago”), not an absolute timestamp.
- Applicant count and salary parsing are best-effort; unrecognized formats yield `None`.
- Dedupe can intentionally drop “same JD, different URL” variants; use artifacts to inspect exclusions.

### Reproducibility checklist

For a repeatable run:

- record Apify actor input JSON + run timestamp
- record Apify dataset ID and/or keep exported JSON file unchanged (checksum recommended)
- trigger pipeline via `jobs_path` pointing to that exported JSON file
- keep run exports (especially `settings-used.json` and `export.json`) as the replay surface
