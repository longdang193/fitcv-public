from fitcv.pipeline_store import PipelineStore


def test_pipeline_store_delegates_to_injected_functions() -> None:
    captured: dict[str, object] = {}

    def _lookup(normalized_jobs, config, *, raw_job_fingerprints, enrich_contract_fingerprint):
        captured["lookup_jobs"] = normalized_jobs
        captured["lookup_fp"] = raw_job_fingerprints
        captured["lookup_contract"] = enrich_contract_fingerprint
        return {"https://example.com/job": {"job_url": "https://example.com/job"}}

    def _load_raw(rows, config):
        captured["raw_rows"] = rows

    def _load_candidate(profile, config):
        captured["candidate_profile"] = profile

    def _load(rows, config):
        captured["load_rows"] = rows

    def _load_run(rows, run_id, config):
        captured["load_run_rows"] = rows
        captured["run_id"] = run_id

    def _store_filter(result, run_id, config):
        captured["filter_result"] = result
        captured["filter_run_id"] = run_id

    def _store_shortlist(rows, config):
        captured["shortlist_rows"] = rows

    def _store_ranking(rows, config):
        captured["ranking_rows"] = rows

    def _embed_jobs(rows, config):
        captured["embed_rows"] = rows

    def _store_version(version, config):
        captured["cv_version"] = version

    store = PipelineStore(
        load_raw_jobs_fn=_load_raw,
        load_candidate_profile_fn=_load_candidate,
        lookup_reusable_structured_jobs_fn=_lookup,
        load_structured_jobs_fn=_load,
        load_run_structured_jobs_fn=_load_run,
        store_filter_results_fn=_store_filter,
        embed_and_store_jobs_fn=_embed_jobs,
        store_shortlist_fn=_store_shortlist,
        store_final_ranking_fn=_store_ranking,
        store_cv_version_fn=_store_version,
    )

    rows = store.lookup_reusable_structured_jobs(
        [{"job_url": "https://example.com/job"}],
        {"gcp_project": "p"},
        raw_job_fingerprints={"https://example.com/job": "fp1"},
        enrich_contract_fingerprint="contract1",
    )
    store.load_structured_jobs([{"job_url": "https://example.com/job"}], {})
    store.load_run_structured_jobs([{"job_url": "https://example.com/job"}], "run-1", {})
    store.load_raw_jobs([{"job_url": "https://example.com/job"}], {})
    store.load_candidate_profile({"name": "Candidate"}, {})
    store.store_filter_results({"passed": ["https://example.com/job"], "rejected": []}, "run-1", {})
    store.embed_and_store_jobs([{"job_url": "https://example.com/job"}], {})
    store.store_shortlist([{"job_url": "https://example.com/job", "vector_rank": 1, "vector_similarity": 0.9}], {})
    store.store_final_ranking([{"job_url": "https://example.com/job", "final_rank": 1, "final_score": 0.8}], {})
    store.store_cv_version({"version_id": "v1"}, {})

    assert "https://example.com/job" in rows
    assert captured["run_id"] == "run-1"
    assert captured["filter_run_id"] == "run-1"
    assert captured["candidate_profile"] == {"name": "Candidate"}
    assert captured["cv_version"] == {"version_id": "v1"}
