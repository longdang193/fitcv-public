# Bounded Parallel Enrichment — History

## Changelog

### 1.0.0 — active

- ThreadPoolExecutor-based parallel enrichment in `enrich.py`
- `enrichment_batch_size` / `enrichment_concurrency` config keys consumed from settings
- Global rate lock (`_ENRICH_RATE_LOCK`) prevents concurrent threads from exceeding Vertex AI quotas
- Admin UI fields for batch size and concurrency in `settings_schema.py`
- Conservative defaults: batch_size=10, concurrency=1
- Deterministic output order preserved across parallel batches
- Per-job failure isolation with non-recoverable error propagation
- 6 dedicated tests in `test_enrich.py`

### 0.1.0 — planned

- Feature concept: enrichment in bounded parallel batches with admin-controlled concurrency

## Post-Execution Review

- All capabilities from the planned contract are implemented and tested
- Global rate limiter was added beyond the original spec to handle `RESOURCE_EXHAUSTED` errors
- Default concurrency set to 1 (sequential) for safety — admin can increase via settings UI

