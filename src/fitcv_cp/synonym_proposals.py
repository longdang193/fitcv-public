"""@meta
name: synonym_proposals
type: module
domain: runtime
ownership: infrastructure
responsibility:
  - Module metadata placeholder for src.fitcv_cp.synonym_proposals.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

from __future__ import annotations

import datetime
import hashlib
import json
from typing import Any


def build_synonym_proposals_payload(
    *,
    run_id: str,
    summary: dict[str, Any],
    created_at: datetime.datetime,
    existing_payload_json: str | None = None,
    global_synonyms: dict[str, str] | None = None,
) -> str:
    existing_proposals_by_id: dict[str, dict[str, Any]] = {}
    if existing_payload_json:
        try:
            existing_payload = json.loads(existing_payload_json)
        except (TypeError, json.JSONDecodeError):
            existing_payload = None
        if isinstance(existing_payload, dict):
            for existing_proposal in list(existing_payload.get("proposals") or []):
                if not isinstance(existing_proposal, dict):
                    continue
                proposal_id = str(existing_proposal.get("proposal_id") or "").strip()
                if proposal_id:
                    existing_proposals_by_id[proposal_id] = existing_proposal

    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for suggestion in list(summary.get("mapping_suggestions") or []):
        if not isinstance(suggestion, dict):
            continue
        field = str(suggestion.get("field") or "skill").strip().lower() or "skill"
        alias = str(suggestion.get("alias") or "").strip().lower()
        canonical = str(suggestion.get("canonical") or "").strip().lower()
        if not alias or not canonical:
            continue
        bucket = grouped.setdefault(
            (field, alias),
            {
                "field": field,
                "alias": alias,
                "candidate_canonicals": {},
                "must_have_skills": set(),
                "job_refs": [],
                "occurrence_count": 0,
                "confidence_sum": 0.0,
            },
        )
        bucket["occurrence_count"] += 1
        confidence = float(suggestion.get("confidence") or 0.0)
        bucket["confidence_sum"] += confidence
        bucket["candidate_canonicals"][canonical] = bucket["candidate_canonicals"].get(canonical, 0) + 1
        must_have_skill = str(suggestion.get("must_have_skill") or "").strip().lower()
        if must_have_skill:
            bucket["must_have_skills"].add(must_have_skill)
        job_url = str(suggestion.get("job_url") or "").strip()
        if job_url and len(bucket["job_refs"]) < 5:
            bucket["job_refs"].append(
                {
                    "job_url": job_url,
                    "job_title": str(suggestion.get("job_title") or "").strip(),
                    "confidence": confidence,
                }
            )

    proposals: list[dict[str, Any]] = []
    normalized_global_synonyms: dict[str, str] = {}
    if isinstance(global_synonyms, dict):
        normalized_global_synonyms = {
            str(alias).strip().lower(): str(canonical).strip().lower()
            for alias, canonical in global_synonyms.items()
            if str(alias).strip() and str(canonical).strip()
        }
    suppressed_as_already_global_count = 0
    suppressed_examples: list[dict[str, str]] = []
    for (_field_alias_key, bucket) in grouped.items():
        field = str(bucket.get("field") or "skill")
        alias = str(bucket.get("alias") or "")
        ranked_canonicals = sorted(
            bucket["candidate_canonicals"].items(),
            key=lambda item: (-int(item[1]), item[0]),
        )
        candidate_canonicals = [canonical for canonical, _count in ranked_canonicals]
        primary_canonical = candidate_canonicals[0]
        has_conflict = len(candidate_canonicals) > 1
        proposal_family = "conflict_bundle" if has_conflict else "alias_to_canonical_mapping"
        occurrence_count = int(bucket["occurrence_count"])
        avg_confidence = float(bucket["confidence_sum"]) / occurrence_count if occurrence_count else 0.0
        identity_seed = f"{run_id}:{field}:{alias}:{'|'.join(candidate_canonicals)}:{proposal_family}"
        proposal_id = f"synprop-{hashlib.sha1(identity_seed.encode('utf-8')).hexdigest()[:12]}"
        existing_proposal = existing_proposals_by_id.get(proposal_id) or {}
        global_canonical = normalized_global_synonyms.get(alias) if field == "skill" else None
        if global_canonical and global_canonical == primary_canonical:
            suppressed_as_already_global_count += 1
            if len(suppressed_examples) < 10:
                suppressed_examples.append({"field": field, "alias": alias, "canonical": primary_canonical})
            continue
        proposals.append(
            {
                "proposal_id": proposal_id,
                "run_id": run_id,
                "field": field,
                "alias": alias,
                "canonical": primary_canonical,
                "candidate_aliases": [alias],
                "candidate_canonicals": candidate_canonicals,
                "confidence": round(avg_confidence, 6),
                "rationale": {
                    "kind": "alias_conflict" if has_conflict else "repeated_alias_mapping",
                    "occurrence_count": occurrence_count,
                    "distinct_canonical_count": len(candidate_canonicals),
                },
                "evidence_summary": {
                    "occurrence_count": occurrence_count,
                    "average_confidence": round(avg_confidence, 6),
                    "must_have_skills": sorted(bucket["must_have_skills"]),
                    "sample_job_refs": list(bucket["job_refs"]),
                },
                "conflict_summary": {
                    "has_conflict": has_conflict,
                    "conflicting_canonicals": candidate_canonicals[1:],
                },
                "proposal_status": str(existing_proposal.get("proposal_status") or "proposed_unreviewed"),
                "proposal_scope": "run_scoped_overlay_candidate",
                "proposal_family": proposal_family,
                "source_artifact_refs": {
                    "run_id": run_id,
                    "artifact_type": "mapping_suggestions",
                },
                "review_history": list(existing_proposal.get("review_history") or []),
            }
        )
    proposals.sort(key=lambda item: (-float(item["confidence"]), str(item["alias"])))
    payload = {
        "run_id": run_id,
        "synonym_proposals_schema_version": "synonym_proposals_v1",
        "created_at": created_at.isoformat(),
        "proposal_generation_status": "generated" if proposals else "not_applicable",
        "persistence_status": "persisted" if proposals else "not_applicable",
        "proposals": proposals,
    }
    payload["synonym_proposals_trace"] = _build_synonym_proposals_trace_payload(
        run_id=run_id,
        created_at=created_at,
        proposal_generation_status=str(payload["proposal_generation_status"] or ""),
        persistence_status=str(payload["persistence_status"] or ""),
        proposals=proposals,
        suppression_summary={
            "suppressed_as_already_global_count": suppressed_as_already_global_count,
            "generated_for_review_count": len(proposals),
            "suppressed_examples": suppressed_examples,
            "suppression_source": (
                "run_effective_skill_synonyms"
                if normalized_global_synonyms
                else "none"
            ),
        },
    )
    return json.dumps(payload, ensure_ascii=False)


def _build_synonym_proposals_trace_payload(
    *,
    run_id: str,
    created_at: datetime.datetime,
    proposal_generation_status: str,
    persistence_status: str,
    proposals: list[dict[str, Any]],
    suppression_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if proposal_generation_status == "not_applicable":
        return {
            "run_id": run_id,
            "trace_schema_version": "agentic_step_trace_run_v1",
            "trace_family": "agentic_step_trace",
            "step_id": "synonym_proposals",
            "created_at": created_at.isoformat(),
            "trace_status": "not_applicable",
            "trace_summary": {
                "records_total": 0,
                "present_records": 0,
                "proposal_count": 0,
                "suppressed_as_already_global_count": int(
                    (suppression_summary or {}).get("suppressed_as_already_global_count") or 0
                ),
                "generated_for_review_count": int(
                    (suppression_summary or {}).get("generated_for_review_count") or 0
                ),
                "suppression_source": str((suppression_summary or {}).get("suppression_source") or "none"),
            },
            "records": [],
            "degradation": {},
            "artifact_refs": {},
            "suppression_examples": list((suppression_summary or {}).get("suppressed_examples") or []),
        }

    trace_records: list[dict[str, Any]] = []
    for proposal in proposals:
        if not isinstance(proposal, dict):
            continue
        proposal_id = str(proposal.get("proposal_id") or "").strip()
        alias = str(proposal.get("alias") or "").strip()
        trace_records.append(
            {
                "trace_schema_version": "agentic_step_trace_record_v1",
                "trace_family": "agentic_step_trace",
                "step_id": "synonym_proposals",
                "trace_status": "completed",
                "record_id": proposal_id or alias,
                "scope_type": "alias",
                "scope_key": alias,
                "status": str(proposal.get("proposal_status") or "proposed_unreviewed"),
                "runtime_provenance": {
                    "runtime_path": "fitcv_synonym_proposal_builder_builtin",
                    "provider": "fitcv_builtin",
                    "mode_source": "mapping_suggestions_to_synonym_proposals",
                },
                "attempts": [
                    {
                        "attempt_index": 1,
                        "attempt_type": "proposal_generation",
                        "attempt_status": "completed",
                        "provider_status": "completed",
                    }
                ],
                "input_summary": {
                    "alias": alias,
                    "candidate_canonicals_count": len(list(proposal.get("candidate_canonicals") or [])),
                },
                "output_summary": {
                    "proposal_family": str(proposal.get("proposal_family") or ""),
                    "proposal_scope": str(proposal.get("proposal_scope") or ""),
                    "confidence": float(proposal.get("confidence") or 0.0),
                },
                "validation_summary": {"status": "not_run"},
                "repair_summary": {"repair_attempted": False, "repair_attempts": 0},
                "error_summary": None,
            }
        )

    trace_status = "completed"
    degradation: dict[str, Any] = {}
    if persistence_status == "bundle_only_degraded":
        trace_status = "degraded"
        degradation = {"reason": "synonym_proposals_bundle_only_degraded"}
    elif persistence_status == "failed":
        trace_status = "degraded"
        degradation = {"reason": "synonym_proposals_persistence_failed"}
    elif not trace_records:
        trace_status = "partial"
        degradation = {"reason": "proposal_generation_without_trace_records"}

    return {
        "run_id": run_id,
        "trace_schema_version": "agentic_step_trace_run_v1",
        "trace_family": "agentic_step_trace",
        "step_id": "synonym_proposals",
        "created_at": created_at.isoformat(),
        "trace_status": trace_status,
        "trace_summary": {
            "records_total": len(proposals),
            "present_records": len(trace_records),
            "proposal_count": len(proposals),
            "suppressed_as_already_global_count": int(
                (suppression_summary or {}).get("suppressed_as_already_global_count") or 0
            ),
            "generated_for_review_count": int(
                (suppression_summary or {}).get("generated_for_review_count") or len(proposals)
            ),
            "suppression_source": str((suppression_summary or {}).get("suppression_source") or "none"),
        },
        "records": trace_records,
        "degradation": degradation,
        "artifact_refs": {
            "proposal_artifact": "synonym-proposals.json",
            "stage_artifact": "enrich.json",
        },
        "suppression_examples": list((suppression_summary or {}).get("suppressed_examples") or []),
    }

