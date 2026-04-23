"""
@meta
type: test
scope: unit
domain: evidence
covers:
  - normalise_evidence_item: stable UUID, typed schema
  - score_evidence_item: weighted scoring
  - retrieve_evidence: ranking, top_k, all evidence types
excludes:
  - BigQuery integration (store_evidence_selection)
tags:
  - fast
  - ci-safe
"""

from fitcv import evidence as evidence_module
from fitcv.evidence import retrieve_evidence, retrieve_evidence_bundle, score_evidence_item


# ── schema and ordering ───────────────────────────────────────────────────────

def test_retrieve_evidence_returns_normalized_schema() -> None:
    """All returned items must have evidence_id, evidence_type, score, source_ref."""
    mock_profile = {
        "projects": [
            {"name": "GA4", "skills": ["SQL", "BigQuery"], "business_value": "analytics"},
            {"name": "ETL", "skills": ["Python", "Airflow"], "business_value": "automation"},
        ],
        "achievements": [{"text": "Reduced latency", "category": "performance"}],
    }
    jd_skills = ["SQL", "BigQuery"]
    evidence = retrieve_evidence(mock_profile, jd_skills, top_k=3)
    assert len(evidence) <= 3
    assert evidence[0]["name"] == "GA4"  # best match first
    for item in evidence:
        assert "evidence_id" in item
        assert "evidence_type" in item
        assert "score" in item
        assert "source_ref" in item


# ── evidence types ────────────────────────────────────────────────────────────

def test_retrieve_evidence_achievement_with_no_skills() -> None:
    """Achievements with no explicit skills still appear in ranked output."""
    mock_profile = {
        "projects": [],
        "achievements": [{"text": "Promoted to senior engineer", "category": "career"}],
    }
    evidence = retrieve_evidence(mock_profile, jd_skills=["SQL"], top_k=5)
    assert len(evidence) == 1
    assert evidence[0]["evidence_type"] == "achievement"


def test_retrieve_evidence_experience_bullets() -> None:
    """Experience bullets are included in the ranked pool."""
    mock_profile = {
        "projects": [],
        "achievements": [],
        "experiences": [{
            "role": "DE", "company": "Acme",
            "bullets": [{"text": "Built SQL pipelines", "skills": ["SQL"]}],
        }],
    }
    evidence = retrieve_evidence(mock_profile, jd_skills=["SQL"], top_k=5)
    assert len(evidence) == 1
    assert evidence[0]["evidence_type"] == "experience_entry"
    assert evidence[0]["role"] == "DE"
    assert evidence[0]["company"] == "Acme"
    assert evidence[0]["bullets"] == ["Built SQL pipelines"]


def test_retrieve_evidence_preserves_multiple_relevant_experience_entries() -> None:
    mock_profile = {
        "projects": [
            {"name": "GA4 Platform", "skills": ["BigQuery", "dbt"], "business_value": "analytics"},
            {"name": "Fraud Detection", "skills": ["Python", "SQL"], "business_value": "fraud"},
        ],
        "achievements": [{"text": "Reduced latency by 40%", "skills": ["BigQuery"]}],
        "experiences": [
            {
                "role": "Senior Data Engineer",
                "company": "Acme",
                "start": "2023-01",
                "end": "present",
                "bullets": [
                    {"text": "Built BigQuery pipelines", "skills": ["BigQuery", "SQL"]},
                    {"text": "Maintained dbt models", "skills": ["dbt", "SQL"]},
                ],
            },
            {
                "role": "Data Engineer",
                "company": "Fintech Startup",
                "start": "2021-06",
                "end": "2022-12",
                "bullets": [
                    {"text": "Implemented fraud detection features", "skills": ["Python", "SQL"]},
                    {"text": "Built self-service reporting", "skills": ["SQL"]},
                ],
            },
        ],
    }

    evidence = retrieve_evidence(mock_profile, jd_skills=["SQL", "BigQuery", "Python"], top_k=5)

    experience_entries = [item for item in evidence if item["evidence_type"] == "experience_entry"]
    assert len(experience_entries) >= 2
    assert experience_entries[0]["role"] == "Senior Data Engineer"
    assert experience_entries[1]["role"] == "Data Engineer"


def test_retrieve_evidence_project_entry_preserves_rich_fields() -> None:
    mock_profile = {
        "projects": [
            {
                "name": "FitCV",
                "duration": "2024-01 — present",
                "url": "https://example.com/fitcv",
                "skills": ["Python", "BigQuery"],
                "tech_stack": [
                    "Backend: Python, FastAPI",
                    "Data: BigQuery",
                    "AI: Gemini",
                ],
                "business_value": "Reduced CV tailoring time from 2 hours to 5 minutes.",
                "highlights": [
                    "Ingested 5000+ postings",
                    "Achieved 89% relevance score",
                    "Serves 20+ candidates",
                ],
            }
        ],
        "achievements": [],
        "experiences": [],
    }

    evidence = retrieve_evidence(mock_profile, jd_skills=["Python", "BigQuery", "Gemini"], top_k=5)

    assert len(evidence) == 1
    assert evidence[0]["evidence_type"] == "project_entry"
    assert evidence[0]["name"] == "FitCV"
    assert evidence[0]["duration"] == "2024-01 — present"
    assert evidence[0]["url"] == "https://example.com/fitcv"
    assert evidence[0]["business_value"] == "Reduced CV tailoring time from 2 hours to 5 minutes."
    assert evidence[0]["tech_stack"] == [
        "Backend: Python, FastAPI",
        "Data: BigQuery",
    ]
    assert evidence[0]["highlights"] == [
        "Ingested 5000+ postings",
        "Achieved 89% relevance score",
    ]


def test_retrieve_evidence_sparse_project_entry_is_valid() -> None:
    mock_profile = {
        "projects": [
            {
                "name": "Internal Reporting Tool",
                "duration": "2022",
                "skills": ["Python", "SQL"],
            }
        ],
        "achievements": [],
        "experiences": [],
    }

    evidence = retrieve_evidence(mock_profile, jd_skills=["Python"], top_k=5)

    assert len(evidence) == 1
    assert evidence[0]["evidence_type"] == "project_entry"
    assert evidence[0]["name"] == "Internal Reporting Tool"
    assert evidence[0]["duration"] == "2022"
    assert evidence[0]["skills"] == ["Python", "SQL"]
    assert evidence[0]["tech_stack"] == []
    assert evidence[0]["highlights"] == []
    assert evidence[0]["business_value"] == ""


def test_retrieve_evidence_preserves_multiple_relevant_project_entries() -> None:
    mock_profile = {
        "projects": [
            {
                "name": "FitCV",
                "skills": ["Python", "BigQuery", "Gemini"],
                "business_value": "Reduced CV tailoring time",
                "highlights": ["Ingested 5000+ postings"],
            },
            {
                "name": "Fraud Detection",
                "skills": ["Python", "Kafka", "SQL"],
                "business_value": "Processed 10000 transactions/minute",
                "highlights": ["94% precision"],
            },
        ],
        "achievements": [
            {"text": "Promoted to team lead", "skills": ["Leadership"]},
            {"text": "Published analytics package", "skills": ["Python"]},
        ],
        "experiences": [],
    }

    evidence = retrieve_evidence(mock_profile, jd_skills=["Python", "SQL", "BigQuery"], top_k=4)

    project_entries = [item for item in evidence if item["evidence_type"] == "project_entry"]
    assert len(project_entries) >= 2
    assert [item["name"] for item in project_entries[:2]] == ["FitCV", "Fraud Detection"]


def test_retrieve_evidence_caps_bullets_within_experience_entries() -> None:
    mock_profile = {
        "projects": [],
        "achievements": [],
        "experiences": [
            {
                "role": "Senior Data Engineer",
                "company": "Acme",
                "start": "2023-01",
                "end": "present",
                "bullets": [
                    {"text": "Built BigQuery pipelines", "skills": ["BigQuery", "SQL"]},
                    {"text": "Maintained dbt models", "skills": ["dbt", "SQL"]},
                    {"text": "Ran Airflow orchestration", "skills": ["Airflow"]},
                ],
            }
        ],
    }

    evidence = retrieve_evidence(mock_profile, jd_skills=["SQL", "BigQuery"], top_k=5)

    assert len(evidence) == 1
    assert evidence[0]["evidence_type"] == "experience_entry"
    assert len(evidence[0]["bullets"]) == 2
    assert evidence[0]["bullets"] == [
        "Built BigQuery pipelines",
        "Maintained dbt models",
    ]


def test_retrieve_evidence_selects_different_experience_bullets_for_different_jds() -> None:
    mock_profile = {
        "projects": [],
        "achievements": [],
        "experiences": [
            {
                "role": "Data Engineer",
                "company": "Fintech Startup",
                "start": "2021-06",
                "end": "2022-12",
                "bullets": [
                    {"text": "Built self-service Looker dashboards for KPI monitoring.", "skills": ["Looker", "Analytics"]},
                    {"text": "Automated KPI reporting workflows for analytics stakeholders.", "skills": ["Python", "Analytics"]},
                    {"text": "Implemented fraud detection features using BigQuery ML.", "skills": ["BigQuery ML", "Python"]},
                ],
            }
        ],
    }

    analytics_evidence = retrieve_evidence(mock_profile, jd_skills=["Analytics", "Looker", "Reporting"], top_k=5)
    ml_evidence = retrieve_evidence(mock_profile, jd_skills=["Python", "BigQuery ML", "Fraud"], top_k=5)

    analytics_bullets = analytics_evidence[0]["bullets"]
    ml_bullets = ml_evidence[0]["bullets"]

    assert analytics_bullets != ml_bullets
    assert "Built self-service Looker dashboards for KPI monitoring." in analytics_bullets
    assert "Implemented fraud detection features using BigQuery ML." in ml_bullets


# ── edge cases ────────────────────────────────────────────────────────────────

def test_retrieve_evidence_empty_jd_skills() -> None:
    """Empty JD skill list: items still returned (no crash), scores are low but defined."""
    mock_profile = {
        "projects": [{"name": "X", "skills": ["SQL"], "business_value": ""}],
        "achievements": [],
    }
    evidence = retrieve_evidence(mock_profile, jd_skills=[], top_k=5)
    assert len(evidence) == 1
    assert 0.0 <= evidence[0]["score"] <= 1.0


def test_retrieve_evidence_tie_breaking_is_deterministic() -> None:
    """Two items with identical scores must return in a stable, deterministic order."""
    mock_profile = {
        "projects": [
            {"name": "A", "skills": ["SQL"], "business_value": ""},
            {"name": "B", "skills": ["SQL"], "business_value": ""},
        ],
        "achievements": [],
    }
    ev1 = retrieve_evidence(mock_profile, jd_skills=["SQL"], top_k=5)
    ev2 = retrieve_evidence(mock_profile, jd_skills=["SQL"], top_k=5)
    assert [e["name"] for e in ev1] == [e["name"] for e in ev2]


def test_retrieve_evidence_bundle_merges_channels_and_dedupes_by_evidence_id() -> None:
    profile = {
        "preferences": {
            "target_role": "Data Analyst",
            "role_families": ["analytics"],
            "domains": ["banking"],
        },
        "experiences": [
            {
                "id": "exp_1",
                "role": "Business Data Analyst",
                "company": "Bank Corp",
                "role_family": "analytics",
                "domain_tags": ["banking"],
                "responsibility_themes": ["dashboarding", "kpi_reporting"],
                "bullets": [
                    {
                        "text": "Built KPI dashboards in Power BI and SQL for banking stakeholders.",
                        "skills": ["SQL", "Power BI"],
                    }
                ],
            }
        ],
        "projects": [],
        "achievements": [],
        "skills": [{"name": "SQL"}],
    }
    job = {
        "job_url": "https://example.com/job-1",
        "title": "Data Analyst - Retail Banking",
        "job_family": "analytics",
        "domain": "banking",
        "required_skills_canonical": ["sql"],
        "responsibilities": ["Build KPI dashboards for retail banking stakeholders"],
    }

    bundle = retrieve_evidence_bundle(profile, job, top_k=3)

    assert bundle["channel_counts"]["required_skill_support"] >= 1
    assert bundle["channel_counts"]["role_alignment"] >= 1
    assert bundle["channel_counts"]["domain_alignment"] >= 1
    assert bundle["channel_counts"]["responsibility_alignment"] >= 1
    assert bundle["merged_pool_size"] >= 1
    assert bundle["deduped_pool_size"] == 1
    assert len(bundle["selected_evidence"]) == 1
    assert bundle["selected_evidence_ids"] == [bundle["selected_evidence"][0]["evidence_id"]]
    assert set(bundle["selected_evidence"][0]["matched_channels"]) == {
        "required_skill_support",
        "role_alignment",
        "domain_alignment",
        "responsibility_alignment",
    }


def test_retrieve_evidence_bundle_returns_bounded_final_top_k_with_selection_reasons() -> None:
    profile = {
        "preferences": {
            "target_role": "Data Engineer",
            "role_families": ["data_engineering"],
            "domains": ["banking", "analytics"],
        },
        "experiences": [
            {
                "id": "exp_1",
                "role": "Data Engineer",
                "company": "Finbank",
                "role_family": "data_engineering",
                "domain_tags": ["banking"],
                "responsibility_themes": ["etl", "reporting_automation"],
                "bullets": [
                    {"text": "Built SQL ETL pipelines for banking reporting.", "skills": ["SQL", "ETL"]},
                ],
            }
        ],
        "projects": [
            {
                "id": "proj_1",
                "name": "Analytics Platform",
                "skills": ["Python", "dbt"],
                "domain_tags": ["analytics"],
                "responsibility_themes": ["dashboarding"],
                "business_value": "Supported KPI reporting across analytics teams.",
                "highlights": ["Created KPI dashboards for analytics stakeholders."],
            },
            {
                "id": "proj_2",
                "name": "Streaming Fraud Detection",
                "skills": ["Python", "Kafka"],
                "domain_tags": ["banking"],
                "responsibility_themes": ["fraud_detection"],
                "business_value": "Improved fraud detection in banking.",
                "highlights": ["Implemented real-time fraud detection features."],
            },
        ],
        "achievements": [
            {"id": "ach_1", "text": "Improved KPI reporting latency", "domain_tags": ["analytics"]},
        ],
        "skills": [{"name": "SQL"}, {"name": "Python"}],
    }
    job = {
        "job_url": "https://example.com/job-2",
        "title": "Senior Data Engineer",
        "job_family": "data_engineering",
        "domain": "banking",
        "required_skills_canonical": ["sql", "python"],
        "responsibilities": [
            "Build ETL pipelines",
            "Support KPI reporting for banking stakeholders",
        ],
    }

    bundle = retrieve_evidence_bundle(profile, job, top_k=2)

    assert len(bundle["selected_evidence"]) == 2
    assert len(bundle["selected_evidence_ids"]) == 2
    assert len(set(bundle["selected_evidence_ids"])) == 2
    for item in bundle["selected_evidence"]:
        assert item["selection_reasons"]
        assert item["matched_channels"]
        assert item["selection_score"] >= 0.0


def test_retrieve_evidence_bundle_uses_semantic_alignment_for_paraphrased_matches(monkeypatch) -> None:
    profile = {
        "preferences": {
            "target_role": "Data Analyst",
            "role_families": ["analytics"],
            "domains": ["banking"],
        },
        "experiences": [
            {
                "id": "exp_1",
                "role": "Analytics Specialist",
                "company": "Finance Co",
                "bullets": [
                    {
                        "text": "Built executive reporting that guided loan portfolio decisions.",
                        "skills": ["SQL", "Power BI"],
                    }
                ],
            }
        ],
        "projects": [],
        "achievements": [],
        "skills": [{"name": "SQL"}],
    }
    job = {
        "job_url": "https://example.com/job-3",
        "title": "Data Analyst - Retail Banking",
        "job_family": "analytics",
        "domain": "retail banking",
        "required_skills_canonical": ["sql"],
        "responsibilities": [
            "Translate raw data into recommendations for banking stakeholders",
        ],
    }
    config = {
        "cv_analysis": {
            "semantic_alignment": {
                "enabled": True,
                "model": "text-embedding-005",
                "required_skill_lexical_weight": 0.70,
                "required_skill_semantic_weight": 0.30,
                "role_lexical_weight": 0.60,
                "role_semantic_weight": 0.40,
                "responsibility_lexical_weight": 0.25,
                "responsibility_semantic_weight": 0.75,
                "domain_lexical_weight": 0.40,
                "domain_semantic_weight": 0.60,
                "channel_pool_size": 4,
            }
        }
    }

    vector_by_text = {
        "retail banking analytics": [1.0, 0.0, 0.0],
        "data analyst retail banking analytics": [0.9, 0.0, 0.0],
        "translate raw data into recommendations for banking stakeholders": [0.0, 1.0, 0.0],
        "analytics specialist finance co built executive reporting that guided loan portfolio decisions": [0.8, 0.9, 0.0],
    }

    def fake_generate_embedding(text: str, runtime_config: dict[str, object], model_name: str | None = None) -> list[float]:
        del runtime_config, model_name
        normalized = " ".join(str(text).lower().split())
        return vector_by_text.get(normalized, [0.0, 0.0, 1.0])

    monkeypatch.setattr(evidence_module, "generate_embedding", fake_generate_embedding)

    bundle = retrieve_evidence_bundle(profile, job, top_k=1, config=config)

    selected = bundle["selected_evidence"][0]
    responsibility_subscores = selected["channel_subscores"]["responsibility_alignment"]
    domain_subscores = selected["channel_subscores"]["domain_alignment"]

    assert selected["semantic_alignment"]["enabled"] is True
    assert selected["semantic_alignment"]["semantic_methods"]["required_skill_support"] == "embedding_similarity"
    assert selected["semantic_alignment"]["semantic_methods"]["role_alignment"] == "embedding_similarity"
    assert selected["semantic_alignment"]["semantic_methods"]["responsibility_alignment"] == "embedding_similarity"
    assert selected["semantic_alignment"]["semantic_methods"]["domain_alignment"] == "embedding_similarity"
    assert responsibility_subscores["semantic"] > 0.7
    assert responsibility_subscores["lexical"] == 0.0
    assert domain_subscores["semantic"] > 0.5
    assert domain_subscores["semantic"] > domain_subscores["lexical"]
    assert bundle["effective_channel_pool_size"] == 4
    assert bundle["selected_evidence_count"] == 1
    assert bundle["semantic_alignment"]["embedding_counts"]["candidate_evidence"]["fresh"] >= 1
    assert bundle["semantic_alignment"]["embedding_counts"]["job_context"]["fresh"] >= 1
    assert isinstance(bundle["unselected_top_candidates"], list)


def test_retrieve_evidence_bundle_uses_semantic_alignment_for_required_skill_support(monkeypatch) -> None:
    """@proves pipeline_performance.cv-analysis-now-uses-bounded-semantic-lift-for-required-skill-and-role-channels-instead-of-reserving-semantic-work-only-for-domain-and-responsibility-alignment"""
    profile = {
        "projects": [
            {
                "name": "Warehouse Schema Redesign",
                "skills": ["Schema Design"],
                "highlights": ["Restructured core warehouse entities for cleaner reporting."],
            }
        ],
        "experiences": [],
        "achievements": [],
        "skills": [],
    }
    job = {
        "job_url": "https://example.com/job-required-skill",
        "title": "",
        "job_family": "",
        "domain": "",
        "required_skills_canonical": ["data modeling"],
        "responsibilities": [],
    }
    config = {
        "cv_analysis": {
            "semantic_alignment": {
                "enabled": True,
                "model": "text-embedding-005",
                "required_skill_lexical_weight": 0.70,
                "required_skill_semantic_weight": 0.30,
                "role_lexical_weight": 0.60,
                "role_semantic_weight": 0.40,
                "responsibility_lexical_weight": 0.25,
                "responsibility_semantic_weight": 0.75,
                "domain_lexical_weight": 0.40,
                "domain_semantic_weight": 0.60,
                "channel_pool_size": 4,
            }
        }
    }

    def fake_generate_embedding(text: str, runtime_config: dict[str, object], model_name: str | None = None) -> list[float]:
        del runtime_config, model_name
        normalized = " ".join(str(text).lower().split())
        if normalized == "data modeling":
            return [1.0, 0.0, 0.0]
        if "schema design" in normalized and "warehouse schema redesign" in normalized:
            return [1.0, 0.0, 0.0]
        return [0.0, 0.0, 1.0]

    monkeypatch.setattr(evidence_module, "generate_embedding", fake_generate_embedding)

    bundle = retrieve_evidence_bundle(profile, job, top_k=1, config=config)

    selected = bundle["selected_evidence"][0]
    required_subscores = selected["channel_subscores"]["required_skill_support"]

    assert required_subscores["lexical"] == 0.0
    assert required_subscores["semantic"] > 0.9
    assert required_subscores["combined"] > 0.25
    assert selected["semantic_alignment"]["semantic_methods"]["required_skill_support"] == "embedding_similarity"


def test_retrieve_evidence_bundle_uses_semantic_alignment_for_role_alignment(monkeypatch) -> None:
    """@proves pipeline_performance.cv-analysis-now-uses-bounded-semantic-lift-for-required-skill-and-role-channels-instead-of-reserving-semantic-work-only-for-domain-and-responsibility-alignment"""
    profile = {
        "experiences": [
            {
                "role": "Decision Support Lead",
                "company": "Insight Co",
                "bullets": [{"text": "Guided reporting strategy for executive stakeholders.", "skills": []}],
            }
        ],
        "projects": [],
        "achievements": [],
        "skills": [],
    }
    job = {
        "job_url": "https://example.com/job-role",
        "title": "Business Intelligence Strategist",
        "job_family": "",
        "domain": "",
        "required_skills_canonical": [],
        "responsibilities": [],
    }
    config = {
        "cv_analysis": {
            "semantic_alignment": {
                "enabled": True,
                "model": "text-embedding-005",
                "required_skill_lexical_weight": 0.70,
                "required_skill_semantic_weight": 0.30,
                "role_lexical_weight": 0.60,
                "role_semantic_weight": 0.40,
                "responsibility_lexical_weight": 0.25,
                "responsibility_semantic_weight": 0.75,
                "domain_lexical_weight": 0.40,
                "domain_semantic_weight": 0.60,
                "channel_pool_size": 4,
            }
        }
    }

    def fake_generate_embedding(text: str, runtime_config: dict[str, object], model_name: str | None = None) -> list[float]:
        del runtime_config, model_name
        normalized = " ".join(str(text).lower().split())
        if normalized == "business intelligence strategist":
            return [0.0, 1.0, 0.0]
        if "decision support lead" in normalized and "guided reporting strategy" in normalized:
            return [0.0, 1.0, 0.0]
        return [0.0, 0.0, 1.0]

    monkeypatch.setattr(evidence_module, "generate_embedding", fake_generate_embedding)
    monkeypatch.setattr(evidence_module, "infer_role_family", lambda _text: None)

    bundle = retrieve_evidence_bundle(profile, job, top_k=1, config=config)

    selected = bundle["selected_evidence"][0]
    role_subscores = selected["channel_subscores"]["role_alignment"]

    assert role_subscores["lexical"] == 0.0
    assert role_subscores["semantic"] > 0.9
    assert role_subscores["combined"] > 0.35
    assert selected["semantic_alignment"]["semantic_methods"]["role_alignment"] == "embedding_similarity"


def test_retrieve_evidence_bundle_prefers_broader_channel_coverage_over_redundancy() -> None:
    profile = {
        "preferences": {
            "target_role": "Data Engineer",
            "role_families": ["data_engineering"],
            "domains": ["banking"],
        },
        "experiences": [
            {
                "id": "exp_1",
                "role": "Data Engineer",
                "company": "Bank One",
                "role_family": "data_engineering",
                "domain_tags": ["banking"],
                "responsibility_themes": ["etl", "stakeholder_reporting"],
                "bullets": [
                    {"text": "Built SQL ETL pipelines for banking reporting.", "skills": ["SQL", "ETL"]},
                ],
            },
            {
                "id": "exp_2",
                "role": "Data Engineer",
                "company": "Bank Two",
                "role_family": "data_engineering",
                "domain_tags": ["banking"],
                "responsibility_themes": ["etl"],
                "bullets": [
                    {"text": "Maintained SQL ETL pipelines.", "skills": ["SQL", "ETL"]},
                ],
            },
        ],
        "projects": [
            {
                "id": "proj_1",
                "name": "Stakeholder Reporting Platform",
                "skills": ["Python"],
                "domain_tags": ["banking"],
                "responsibility_themes": ["stakeholder_reporting"],
                "business_value": "Supported executive reporting for retail banking leaders.",
                "highlights": ["Delivered reporting used by banking stakeholders."],
            }
        ],
        "achievements": [],
        "skills": [{"name": "SQL"}, {"name": "Python"}],
    }
    job = {
        "job_url": "https://example.com/job-4",
        "title": "Senior Data Engineer",
        "job_family": "data_engineering",
        "domain": "banking",
        "required_skills_canonical": ["sql"],
        "responsibilities": [
            "Build ETL pipelines",
            "Support stakeholder reporting for banking teams",
        ],
    }

    bundle = retrieve_evidence_bundle(profile, job, top_k=2)
    selected_ids = bundle["selected_evidence_ids"]

    assert "exp_1" in selected_ids
    assert "proj_1" in selected_ids
    assert "exp_2" not in selected_ids
    assert bundle["selected_evidence_count"] == 2
    assert len(bundle["unselected_top_candidates"]) >= 1
    assert bundle["unselected_top_candidates"][0]["evidence_id"] == "exp_2"
