"""@meta
name: cv_generator
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Module metadata placeholder for src.fitcv.cv_generator.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

import json
import re
import textwrap
from copy import deepcopy
from types import SimpleNamespace
from typing import Any

from jinja2 import BaseLoader, Environment, TemplateError

from fitcv.candidate_name_policy import resolved_candidate_profile_name
from fitcv.candidate import flatten_skills
from fitcv.placeholder_policy import (
    is_placeholder_token as _shared_is_placeholder_token,
    normalize_placeholder_token as _shared_normalize_placeholder_token,
)
from fitcv.config import (
    CV_SECTION_KEY_TO_NAME,
    CV_STRUCTURED_SECTION_KEYS,
    get_cv_generation_model,
    get_cv_generation_structured_prompt_id,
    get_required_structured_section_keys,
)
from fitcv.contracts import (
    ANALYSIS_CHANNEL_DEFINITIONS,
    DOMAIN_ALIGNMENT_CHANNEL,
    REQUIRED_SKILL_SUPPORT_CHANNEL,
    RESPONSIBILITY_ALIGNMENT_CHANNEL,
    ROLE_ALIGNMENT_CHANNEL,
    STRUCTURED_CV_SCHEMA_VERSION,
)
from fitcv.runtime_routing import resolve_cv_generation_routing, resolve_openai_compatible_api_key
from fitcv.prompts import render_prompt
from fitcv.section_policy import (
    certification_evidence_lines,
    certification_policy_decisions,
)
from fitcv.rule_filter import canonicalize_skill
from fitcv.runtime_routing import resolve_cv_generation_routing, resolve_openai_compatible_api_key

# ── template variant map ─────────────────────────────────────────────────────
# Maps job_family values (populated by enrichment) to styling hints.
# No new classification is performed here — job_family is read as-is.

_TEMPLATE_VARIANTS: dict[str, str] = {
    "data_engineering": "engineering",
    "analytics":        "analytics",
    "data_science":     "science",
    "ml_engineering":   "engineering",
}

_DEFAULT_VARIANT = "standard"
DEFAULT_CV_LOCALE = "en"
DEFAULT_SUPPORTING_EVIDENCE_PER_ROLE = 1
LEGACY_MARKDOWN_PROMPT_ID = "cv_generation.write.v1"
_EDUCATION_PLACEHOLDER_TOKENS = {
    "",
    "none",
    "null",
    "n/a",
    "na",
    "not specified",
    "not provided",
    "unknown",
}

def _build_openai_compat_client(config: dict[str, Any]) -> Any | None:
    routing = resolve_cv_generation_routing(config)
    provider_name = routing.provider
    allowed_http_providers = {"openai", "openai_compatible", "9router"}
    if provider_name not in allowed_http_providers:
        raise RuntimeError(
            "cv_generation_structured_write provider must be configured in control-plane model_routing.parts as "
            "one of: openai, openai_compatible, 9router."
        )
    base_url = routing.base_url
    if not base_url:
        raise RuntimeError("OpenAI-compatible CV generation routing requires provider base_url in control-plane config.")
    api_key = resolve_openai_compatible_api_key()
    if not api_key:
        raise RuntimeError("OpenAI-compatible CV generation routing requires API key in env.")
    wire_api = routing.wire_api
    timeout_seconds = routing.timeout_seconds
    model_override = routing.model
    if not model_override:
        raise RuntimeError(
            "cv_generation_structured_write model must be configured in control-plane model_routing.parts."
        )

    import httpx

    def _generate_content(*, model: str, contents: str) -> Any:
        del model
        resolved_model = model_override
        with httpx.Client(timeout=timeout_seconds) as client:
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }
            if wire_api == "responses":
                payload = {
                    "model": resolved_model,
                    "input": contents,
                    "text": {"format": {"type": "json_object"}},
                }
                try:
                    resp = client.post(f"{base_url.rstrip('/')}/responses", headers=headers, json=payload)
                    resp.raise_for_status()
                    body = dict(resp.json() or {})
                    text = str(body.get("output_text") or "").strip()
                    if not text:
                        output = body.get("output")
                        if isinstance(output, list):
                            chunks: list[str] = []
                            for item in output:
                                if not isinstance(item, dict):
                                    continue
                                for content_item in item.get("content") or []:
                                    if not isinstance(content_item, dict):
                                        continue
                                    chunk = str(content_item.get("text") or "").strip()
                                    if chunk:
                                        chunks.append(chunk)
                            text = "\n".join(chunks).strip()
                except httpx.HTTPStatusError as exc:
                    if exc.response is None or exc.response.status_code != 404:
                        raise
                    payload = {
                        "model": resolved_model,
                        "messages": [{"role": "user", "content": contents}],
                        "temperature": 0.0,
                        "response_format": {"type": "json_object"},
                    }
                    resp = client.post(f"{base_url.rstrip('/')}/chat/completions", headers=headers, json=payload)
                    resp.raise_for_status()
                    body = dict(resp.json() or {})
                    text = str((((body.get("choices") or [{}])[0]).get("message") or {}).get("content") or "").strip()
            else:
                payload = {
                    "model": resolved_model,
                    "messages": [{"role": "user", "content": contents}],
                    "temperature": 0.0,
                    "response_format": {"type": "json_object"},
                }
                resp = client.post(f"{base_url.rstrip('/')}/chat/completions", headers=headers, json=payload)
                resp.raise_for_status()
                body = dict(resp.json() or {})
                text = str((((body.get("choices") or [{}])[0]).get("message") or {}).get("content") or "").strip()
        return SimpleNamespace(text=text)

    return SimpleNamespace(models=SimpleNamespace(generate_content=_generate_content))

def _make_generation_client(config: dict[str, Any]) -> Any:
    return _build_openai_compat_client(config)


# ── template variant selector ─────────────────────────────────────────────────

def select_template_variant(jd: dict[str, Any]) -> str:
    """Return a template variant name for the given enriched job description.

    Reads ``jd["job_family"]`` (populated by the enrichment stage).
    No new classification is performed — the value is used as a lookup key only.
    Unknown or missing job_family → ``"standard"`` (safe default).
    """
    family = str(jd.get("job_family") or "").strip().lower()
    return _TEMPLATE_VARIANTS.get(family, _DEFAULT_VARIANT)


def _get_enabled_section_names(config: dict[str, Any] | None) -> list[str]:
    if config is None:
        return []
    composition = (config.get("cv") or {}).get("composition") or {}
    enabled_sections: list[str] = []
    for section_key, section_cfg in composition.items():
        if isinstance(section_cfg, dict) and section_cfg.get("enabled", True):
            enabled_sections.append(CV_SECTION_KEY_TO_NAME.get(section_key, section_key.title()))
    return enabled_sections




def _filter_template_by_enabled_sections(template: str, enabled_sections: list[str]) -> str:
    if not enabled_sections:
        return template

    preamble_lines: list[str] = []
    section_blocks: dict[str, list[str]] = {}
    current_section: str | None = None
    has_seen_section = False

    for line in template.splitlines():
        if line.startswith("## "):
            has_seen_section = True
            current_section = line[3:].strip()
            section_blocks[current_section] = [line]
            continue
        if not has_seen_section:
            preamble_lines.append(line)
            continue
        if current_section is not None:
            section_blocks[current_section].append(line)

    filtered_blocks: list[str] = []
    for section_name in enabled_sections:
        block = section_blocks.get(section_name)
        if block:
            filtered_blocks.append("\n".join(block).strip())

    if not filtered_blocks:
        return template

    preamble = "\n".join(preamble_lines).strip()
    parts = [part for part in [preamble, *filtered_blocks] if part]
    return "\n\n".join(parts) + "\n"


def _format_certification_lines(profile: dict[str, Any] | None) -> list[str]:
    if not profile:
        return []

    lines: list[str] = []
    for cert in profile.get("certifications") or []:
        if not isinstance(cert, dict):
            continue
        name = str(cert.get("name") or "").strip()
        if not name:
            continue
        issuer = str(cert.get("issuer") or "").strip()
        year = cert.get("year")
        parts = [name]
        if issuer:
            parts.append(issuer)
        line = " — ".join(parts)
        if year:
            line = f"{line} ({year})"
        lines.append(line)
    return lines


def _format_language_lines(profile: dict[str, Any] | None) -> list[str]:
    if not profile:
        return []

    lines: list[str] = []
    for language in profile.get("languages") or []:
        if not isinstance(language, dict):
            continue
        name = str(language.get("name") or "").strip()
        if not name:
            continue
        if language.get("native"):
            lines.append(f"{name} (native)")
            continue

        proficiency_parts: list[str] = []
        for label, key in (("read", "read"), ("write", "write"), ("speak", "speak")):
            level = str(language.get(key) or "").strip()
            if level:
                proficiency_parts.append(f"{label}: {level}")

        line = name
        if proficiency_parts:
            line = f"{line} ({', '.join(proficiency_parts)})"
        notes = str(language.get("notes") or "").strip()
        if notes:
            line = f"{line} — {notes}"
        lines.append(line)
    return lines

def _format_education_lines(profile: dict[str, Any] | None) -> list[str]:
    if not profile:
        return []

    lines: list[str] = []
    for education in profile.get("education") or []:
        if not isinstance(education, dict):
            continue
        degree = str(education.get("degree") or "").strip()
        institution = str(education.get("institution") or "").strip()
        if not degree and not institution:
            continue

        field = str(education.get("field") or "").strip()
        start = str(education.get("start") or "").strip()
        end = str(education.get("end") or "").strip()

        parts = [part for part in (degree, institution) if part]
        line = " — ".join(parts)
        if field:
            line = f"{line} ({field})"
        if start or end:
            line = f"{line} [{ '–'.join(part for part in (start, end) if part) }]"
        lines.append(line)
    return lines


def _format_evidence_block(item: dict[str, Any]) -> str:
    evidence_type = str(item.get("evidence_type") or "")
    if evidence_type == "experience_entry":
        role = str(item.get("role") or "").strip()
        company = str(item.get("company") or "").strip()
        start = str(item.get("start") or "").strip()
        end = str(item.get("end") or "").strip()
        dates = " to ".join(part for part in (start, end) if part) or "(dates unavailable)"
        bullet_lines = [
            f"- {bullet}"
            for bullet in item.get("bullets") or []
            if isinstance(bullet, str) and bullet.strip()
        ]
        skills = ", ".join(str(skill).strip() for skill in item.get("skills") or [] if str(skill).strip())
        parts = [
            "Experience Entry",
            f"Role: {role or '(role unavailable)'}",
            f"Company: {company or '(company unavailable)'}",
            f"Dates: {dates}",
        ]
        if skills:
            parts.append(f"Skills: {skills}")
        if bullet_lines:
            parts.append("Relevant bullets:")
            parts.extend(bullet_lines)
        return "\n".join(parts)
    if evidence_type == "project_entry":
        name = str(item.get("name") or "").strip()
        duration = str(item.get("duration") or "").strip()
        business_value = str(item.get("business_value") or "").strip()
        tech_stack = [
            f"- {line}"
            for line in item.get("tech_stack") or []
            if isinstance(line, str) and line.strip()
        ]
        highlights = [
            f"- {line}"
            for line in item.get("highlights") or []
            if isinstance(line, str) and line.strip()
        ]
        parts = [
            "Project Entry",
            f"Name: {name or '(name unavailable)'}",
        ]
        if duration:
            parts.append(f"Duration: {duration}")
        if business_value:
            parts.append(f"Business value: {business_value}")
        if tech_stack:
            parts.append("Relevant stack:")
            parts.extend(tech_stack)
        if highlights:
            parts.append("Relevant highlights:")
            parts.extend(highlights)
        return "\n".join(parts)

    name = str(item.get("name", "Unknown"))
    skills = ", ".join(item.get("skills") or [])
    return f"- {name}: {skills}"


def _supporting_evidence_priority(evidence_type: str) -> int:
    priorities = {
        "achievement": 0,
        "project_entry": 1,
        "project": 2,
    }
    return priorities.get(evidence_type, 9)


def _supporting_evidence_score(
    *,
    experience_item: dict[str, Any],
    support_item: dict[str, Any],
    jd_skills: list[str],
) -> int:
    experience_skills = {
        str(skill).strip().lower()
        for skill in experience_item.get("skills") or []
        if str(skill).strip()
    }
    support_skills = {
        str(skill).strip().lower()
        for skill in support_item.get("skills") or []
        if str(skill).strip()
    }
    jd_skill_set = {str(skill).strip().lower() for skill in jd_skills if str(skill).strip()}
    return len(experience_skills & support_skills) + len(jd_skill_set & support_skills)


def _format_supporting_evidence_line(item: dict[str, Any]) -> str:
    evidence_type = str(item.get("evidence_type") or "")
    name = str(item.get("name") or "").strip() or "Unknown"
    label = {
        "achievement": "Achievement",
        "project_entry": "Related project",
        "project": "Related project",
    }.get(evidence_type, "Supporting evidence")
    return f"- {label}: {name}"


def _select_supporting_evidence_for_experience(
    *,
    experience_item: dict[str, Any],
    evidence: list[dict[str, Any]],
    jd_skills: list[str],
    enabled_section_names: list[str],
) -> list[dict[str, Any]]:
    projects_enabled = "Projects" in enabled_section_names
    candidates: list[dict[str, Any]] = []
    for item in evidence:
        evidence_type = str(item.get("evidence_type") or "")
        if evidence_type not in {"achievement", "project_entry", "project"}:
            continue
        if evidence_type in {"project_entry", "project"} and projects_enabled:
            continue
        score = _supporting_evidence_score(
            experience_item=experience_item,
            support_item=item,
            jd_skills=jd_skills,
        )
        if score <= 0:
            continue
        candidates.append(item)

    candidates.sort(
        key=lambda item: (
            -_supporting_evidence_score(
                experience_item=experience_item,
                support_item=item,
                jd_skills=jd_skills,
            ),
            _supporting_evidence_priority(str(item.get("evidence_type") or "")),
            str(item.get("name") or ""),
        )
    )
    return candidates[:DEFAULT_SUPPORTING_EVIDENCE_PER_ROLE]


def _build_selected_evidence_lines(
    evidence: list[dict[str, Any]],
    *,
    jd_skills: list[str],
    enabled_section_names: list[str],
) -> str:
    lines: list[str] = []
    has_experience_entries = any(str(item.get("evidence_type") or "") == "experience_entry" for item in evidence)
    for item in evidence:
        evidence_type = str(item.get("evidence_type") or "")
        if evidence_type == "achievement" and has_experience_entries:
            continue
        if evidence_type in {"project_entry", "project"} and has_experience_entries and "Projects" not in enabled_section_names:
            continue

        block = _format_evidence_block(item)
        matched_channels = _coerce_string_list(item.get("matched_channels"))
        selection_reasons = _coerce_string_list(item.get("selection_reasons"))
        metadata_lines: list[str] = []
        if matched_channels:
            metadata_lines.append(
                "Matched channels: " + ", ".join(matched_channels)
            )
        if selection_reasons:
            metadata_lines.append(
                "Selection reasons: " + ", ".join(selection_reasons)
            )
        if evidence_type == "experience_entry":
            supporting_items = _select_supporting_evidence_for_experience(
                experience_item=item,
                evidence=evidence,
                jd_skills=jd_skills,
                enabled_section_names=enabled_section_names,
            )
            if supporting_items:
                support_lines = [_format_supporting_evidence_line(support_item) for support_item in supporting_items]
                metadata_lines.extend(["Supporting evidence:", *support_lines])
        if metadata_lines:
            block = "\n".join([block, *metadata_lines])
        lines.append(block)
    return "\n".join(lines) or "(none)"


def _build_evidence_usage_guidance(evidence: list[dict[str, Any]]) -> str:
    matched_channels = {
        channel
        for item in evidence
        for channel in _coerce_string_list(item.get("matched_channels"))
    }
    selection_reasons = {
        reason
        for item in evidence
        for reason in _coerce_string_list(item.get("selection_reasons"))
    }
    channels_or_reasons = matched_channels | selection_reasons
    if not channels_or_reasons:
        return "(no channel-aware evidence guidance available)"

    guidance_by_channel = {
        REQUIRED_SKILL_SUPPORT_CHANNEL: f"Use evidence tagged `{REQUIRED_SKILL_SUPPORT_CHANNEL}` to justify concrete technical and skills claims.",
        ROLE_ALIGNMENT_CHANNEL: f"Use evidence tagged `{ROLE_ALIGNMENT_CHANNEL}` to shape the summary, headline, and role positioning.",
        DOMAIN_ALIGNMENT_CHANNEL: f"Use evidence tagged `{DOMAIN_ALIGNMENT_CHANNEL}` only for grounded domain familiarity or business-context claims.",
        RESPONSIBILITY_ALIGNMENT_CHANNEL: f"Use evidence tagged `{RESPONSIBILITY_ALIGNMENT_CHANNEL}` to craft experience bullets around similar work and outcomes.",
    }
    guidance_lines = [
        guidance_by_channel[channel_definition.channel_id]
        for channel_definition in ANALYSIS_CHANNEL_DEFINITIONS.values()
        if channel_definition.channel_id in channels_or_reasons
    ]
    guidance_lines.append(
        "Prefer evidence selected by multiple channels when deciding what to emphasize in the final CV."
    )
    return "\n".join(f"- {line}" for line in guidance_lines)


def _profile_contact_value(profile: dict[str, Any] | None, key: str) -> str | None:
    if not profile:
        return None
    direct_value = profile.get(key)
    if isinstance(direct_value, str) and direct_value.strip():
        return direct_value.strip()
    contact = profile.get("contact")
    if isinstance(contact, dict):
        nested_value = contact.get(key)
        if isinstance(nested_value, str) and nested_value.strip():
            return nested_value.strip()
    return None


def _coerce_string_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    result: list[str] = []
    for value in values:
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                result.append(stripped)
    return result


def _build_generation_prompt_context(
    *,
    jd: dict[str, Any],
    evidence: list[dict[str, Any]],
    gap: dict[str, Any],
    template: str,
    profile: dict[str, Any] | None,
    config: dict[str, Any] | None,
    evidence_selection_summary: dict[str, Any] | None,
    repair_missing_sections: list[str] | None,
) -> dict[str, str]:
    title = str(jd.get("title") or "")
    required_skills = list(jd.get("required_skills") or [])
    candidate_name = resolved_candidate_profile_name(profile)

    enabled_section_names = _get_enabled_section_names(config)

    allowed_skill_set: set[str] = set()
    for item in evidence:
        for raw_skill in item.get("skills") or []:
            text = str(raw_skill or "").strip()
            if not text:
                continue
            allowed_skill_set.add(canonicalize_skill(text, config or {}))
    allowed_skills = sorted(skill for skill in allowed_skill_set if skill)

    allowed_certifications: list[str] = []
    if "Certifications" in enabled_section_names and not allowed_certifications:
        enabled_section_names = [name for name in enabled_section_names if name != "Certifications"]

    evidence_lines = _build_selected_evidence_lines(
        evidence,
        jd_skills=required_skills,
        enabled_section_names=enabled_section_names,
    )
    evidence_usage_guidance = _build_evidence_usage_guidance(evidence)

    matched_skills = list(gap.get("matched") or [])
    missing_skills = list(gap.get("missing") or [])

    constraint_lines: list[str] = []
    if missing_skills:
        constraint_lines.append(
            "Do NOT claim the candidate has the following skills "
            f"(they are missing from their profile): {', '.join(missing_skills)}"
        )
    do_not_claim = [str(item) for item in list(gap.get("do_not_claim") or []) if str(item)]
    if do_not_claim:
        constraint_lines.append(
            "Do NOT claim the following unsupported items: "
            + ", ".join(do_not_claim)
        )
    if matched_skills:
        constraint_lines.append(
            f"The candidate does have: {', '.join(matched_skills)}"
        )
    if profile:
        if candidate_name:
            constraint_lines.append(
                f"Use this exact candidate name in the header: {candidate_name}"
            )
            constraint_lines.append(
                "Do not output placeholder names such as Candidate Name, [Candidate Name], Your Name, or [Your Name]."
            )
        known_employers = [
            str(exp.get("company") or "")
            for exp in (profile.get("experiences") or [])
            if exp.get("company")
        ]
        known_projects = [
            str(project.get("name") or "")
            for project in (profile.get("projects") or [])
            if project.get("name")
        ]
        if known_employers:
            constraint_lines.append(
                "Do not invent employer names. Only use employers from the candidate profile: "
                + ", ".join(known_employers)
            )
        if known_projects:
            constraint_lines.append(
                "Do not invent project names. Only use project names from the candidate profile: "
                + ", ".join(known_projects)
            )
        if allowed_skills:
            constraint_lines.append(
                "Allowed Skills (selected-evidence only): " + ", ".join(allowed_skills)
            )
            constraint_lines.append(
                "In the Skills section, list only skills from Allowed Skills. Do not add profile-only skills."
            )
    if enabled_section_names:
        constraint_lines.append(
            "The generated CV MUST include these sections in this order: "
            + ", ".join(enabled_section_names)
        )
        constraint_lines.append(
            "Use markdown headings exactly: '# {Candidate Name}', optional one-line subtitle, then each required section as '## Section'."
        )
        constraint_lines.append(
            "Use '- ' as the only bullet marker and keep exactly one blank line between top-level sections."
        )
    if repair_missing_sections:
        constraint_lines.append(
            "Regenerate the CV and fix the previous structural failure by including these missing sections with grounded content: "
            + ", ".join(repair_missing_sections)
        )
        constraint_lines.append(
            "Do not leave required sections empty. Each required section must contain at least one grounded line or bullet."
        )

    if config is not None:
        composition = (config.get("cv") or {}).get("composition") or {}
        for section_key, section_cfg in composition.items():
            if isinstance(section_cfg, dict) and not section_cfg.get("enabled", True):
                display_name = CV_SECTION_KEY_TO_NAME.get(section_key, section_key.title())
                constraint_lines.append(
                    f"Do NOT include a '{display_name}' section."
                )
    requirement_coverage = [
        dict(item)
        for item in list(gap.get("requirement_coverage") or [])
        if isinstance(item, dict)
    ]
    if requirement_coverage:
        supported = [
            str(item.get("requirement") or "").strip()
            for item in requirement_coverage
            if str(item.get("support_strength") or "").strip().lower() == "supported"
            and str(item.get("requirement") or "").strip()
        ]
        unsupported = [
            str(item.get("requirement") or "").strip()
            for item in requirement_coverage
            if str(item.get("support_strength") or "").strip().lower() == "unsupported"
            and str(item.get("requirement") or "").strip()
        ]
        if supported:
            constraint_lines.append(
                "Prioritize these strongly supported requirements: " + ", ".join(supported)
            )
        if unsupported:
            constraint_lines.append(
                "Treat these requirements as unsupported unless explicit evidence is present: "
                + ", ".join(unsupported)
            )
    section_confidence_hints = dict(gap.get("section_confidence_hints") or {})
    if section_confidence_hints:
        hints_text = ", ".join(
            f"{section}={str(level)}"
            for section, level in section_confidence_hints.items()
            if str(section).strip() and str(level).strip()
        )
        if hints_text:
            constraint_lines.append("Section confidence hints: " + hints_text)
    if any(str(item.get("evidence_type") or "") == "experience_entry" for item in evidence):
        constraint_lines.append(
            "For the Experience section, emphasize the bullets most relevant to the target JD."
        )
        constraint_lines.append(
            "For the Experience section, summarize or combine grounded facts where helpful."
        )
    if evidence:
        constraint_lines.append(
            "Stay within the selected evidence bundle for this job."
        )
        constraint_lines.append(
            "If a responsibility, domain, or role-positioning claim is not supported by the selected evidence, omit it."
        )

    constraints = "\n".join(constraint_lines) or "(no specific constraints)"
    filtered_template = _filter_template_by_enabled_sections(template, enabled_section_names)
    section_evidence_lines: list[str] = []
    if "Certifications" in enabled_section_names:
        cert_policy = certification_policy_decisions(
            config=config,
            profile=profile,
            evidence_selected_certifications=[],
        )
        certification_lines = certification_evidence_lines(cert_policy)
        if certification_lines:
            section_evidence_lines.append(
                "Use these candidate certifications when filling the Certifications section:\n"
                + "\n".join(f"- {line}" for line in certification_lines)
            )
    if "Languages" in enabled_section_names:
        language_lines = _format_language_lines(profile)
        if language_lines:
            section_evidence_lines.append(
                "Use these candidate languages when filling the Languages section:\n"
                + "\n".join(f"- {line}" for line in language_lines)
            )
    if "Education" in enabled_section_names:
        education_lines = _format_education_lines(profile)
        if education_lines:
            section_evidence_lines.append(
                "Use these candidate education entries when filling the Education section:\n"
                + "\n".join(f"- {line}" for line in education_lines)
            )
    section_evidence = "\n\n".join(section_evidence_lines) or "(no additional section-specific evidence)"
    analysis_summary_lines: list[str] = []
    if evidence_selection_summary:
        selected_count = evidence_selection_summary.get("selected_evidence_count")
        if selected_count is not None:
            analysis_summary_lines.append(
                f"Selected evidence count: {selected_count}"
            )
        selected_ids = _coerce_string_list(
            evidence_selection_summary.get("selected_evidence_ids")
        )
        if selected_ids:
            analysis_summary_lines.append(
                "Selected evidence ids: " + ", ".join(selected_ids)
            )
        channel_counts = evidence_selection_summary.get("channel_counts")
        if isinstance(channel_counts, dict) and channel_counts:
            ordered_counts = ", ".join(
                f"{key}={channel_counts[key]}"
                for key in sorted(channel_counts)
            )
            analysis_summary_lines.append(
                "Evidence channel counts: " + ordered_counts
            )
    analysis_summary = "\n".join(analysis_summary_lines) or "(no analysis summary available)"
    return {
        "title": title,
        "required_skills": ", ".join(required_skills) or "(none specified)",
        "selected_evidence": evidence_lines,
        "evidence_usage_guidance": evidence_usage_guidance,
        "analysis_summary": analysis_summary,
        "constraints": constraints,
        "allowed_skills": ", ".join(allowed_skills) or "(none)",
        "allowed_certifications": ", ".join(allowed_certifications) or "(none)",
        "section_evidence": section_evidence,
        "output_template": filtered_template,
    }


def _build_default_sections(
    *,
    profile: dict[str, Any] | None,
    jd: dict[str, Any],
) -> dict[str, Any]:
    return {
        "header": {
            "name": str((profile or {}).get("name") or "").strip(),
            "title": str(jd.get("title") or "").strip(),
            "location": _profile_contact_value(profile, "location"),
            "contact": {
                "email": _profile_contact_value(profile, "email"),
                "phone": _profile_contact_value(profile, "phone"),
                "linkedin": _profile_contact_value(profile, "linkedin"),
            },
        },
        "summary": {"text": ""},
        "experience": [],
        "projects": [],
        "education": [],
        "skills": {"groups": []},
        "certifications": [],
        "publications": [],
        "languages": [],
    }


def build_empty_structured_cv(
    *,
    jd: dict[str, Any],
    profile: dict[str, Any] | None,
    config: dict[str, Any],
    fit_classification: str,
) -> dict[str, Any]:
    """Return the canonical empty structured CV document for one generation run."""
    cv_cfg = config.get("cv") or {}
    return {
        "schema_version": STRUCTURED_CV_SCHEMA_VERSION,
        "preset": str(cv_cfg.get("preset") or ""),
        "locale": str(cv_cfg.get("locale") or DEFAULT_CV_LOCALE),
        "job_url": str(jd.get("job_url") or ""),
        "fit_classification": str(fit_classification or "unclassified"),
        "target_role": str(jd.get("title") or ""),
        "sections": _build_default_sections(profile=profile, jd=jd),
    }


def build_live_structured_cv_response_schema(config: dict[str, Any] | None = None) -> dict[str, Any]:
    nullable_string_schema = {
        "anyOf": [
            {"type": "string"},
            {"type": "null"},
        ]
    }
    bullet_schema = {
        "type": "string",
    }
    required_sections = [
        "header",
        "summary",
        "experience",
        "projects",
        "education",
        "skills",
        "certifications",
        "publications",
        "languages",
    ]
    if config is not None:
        required_sections = ["header", *get_required_structured_section_keys(config)]
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["sections"],
        "properties": {
            "sections": {
                "type": "object",
                "additionalProperties": False,
                "required": required_sections,
                "properties": {
                    "header": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["name", "title", "location", "contact"],
                        "properties": {
                            "name": {"type": "string"},
                            "title": {"type": "string"},
                            "location": nullable_string_schema,
                            "contact": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": ["email", "phone", "linkedin"],
                                "properties": {
                                    "email": nullable_string_schema,
                                    "phone": nullable_string_schema,
                                    "linkedin": nullable_string_schema,
                                },
                            },
                        },
                    },
                    "summary": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["text"],
                        "properties": {
                            "text": {"type": "string"},
                        },
                    },
                    "experience": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["role", "company", "start", "end", "location", "bullets"],
                            "properties": {
                                "role": {"type": "string"},
                                "company": {"type": "string"},
                                "start": nullable_string_schema,
                                "end": nullable_string_schema,
                                "location": nullable_string_schema,
                                "bullets": {"type": "array", "items": bullet_schema},
                            },
                        },
                    },
                    "projects": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["name", "context", "bullets"],
                            "properties": {
                                "name": {"type": "string"},
                                "context": nullable_string_schema,
                                "bullets": {"type": "array", "items": bullet_schema},
                            },
                        },
                    },
                    "education": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["degree", "institution", "field", "start", "end"],
                            "properties": {
                                "degree": {"type": "string"},
                                "institution": {"type": "string"},
                                "field": nullable_string_schema,
                                "start": nullable_string_schema,
                                "end": nullable_string_schema,
                            },
                        },
                    },
                    "skills": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["groups"],
                        "properties": {
                            "groups": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "required": ["label", "items"],
                                    "properties": {
                                        "label": {"type": "string"},
                                        "items": {"type": "array", "items": {"type": "string"}},
                                    },
                                },
                            },
                        },
                    },
                    "certifications": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["name", "issuer", "year"],
                            "properties": {
                                "name": {"type": "string"},
                                "issuer": nullable_string_schema,
                                "year": nullable_string_schema,
                            },
                        },
                    },
                    "publications": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["title", "publisher", "year"],
                            "properties": {
                                "title": {"type": "string"},
                                "publisher": nullable_string_schema,
                                "year": nullable_string_schema,
                            },
                        },
                    },
                    "languages": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["name", "level"],
                            "properties": {
                                "name": {"type": "string"},
                                "level": nullable_string_schema,
                            },
                        },
                    },
                },
            },
        },
    }

def validate_structured_cv(
    structured_cv: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> None:
    """Validate the canonical structured CV document shape."""
    required_keys = (
        "schema_version",
        "preset",
        "locale",
        "job_url",
        "fit_classification",
        "target_role",
        "sections",
    )
    missing_keys = [key for key in required_keys if key not in structured_cv]
    if missing_keys:
        raise ValueError(f"Structured CV missing required keys: {', '.join(missing_keys)}")

    sections = structured_cv.get("sections")
    if not isinstance(sections, dict):
        raise ValueError("Structured CV sections must be an object")

    required_sections = set(CV_STRUCTURED_SECTION_KEYS)
    if config is not None:
        required_sections = {"header", *get_required_structured_section_keys(config)}
    missing_sections = [key for key in required_sections if key not in sections]
    if missing_sections:
        raise ValueError(f"Structured CV missing required sections: {', '.join(missing_sections)}")

    if not isinstance(sections["header"], dict):
        raise ValueError("Structured CV header section must be an object")
    if "summary" in sections and not isinstance(sections["summary"], dict):
        raise ValueError("Structured CV summary section must be an object")
    if "skills" in sections and not isinstance(sections["skills"], dict):
        raise ValueError("Structured CV skills section must be an object")

    for list_section in ("experience", "projects", "education", "certifications", "publications", "languages"):
        if list_section in sections and not isinstance(sections[list_section], list):
            raise ValueError(f"Structured CV {list_section} section must be a list")

    if "skills" in sections:
        groups = sections["skills"].get("groups")
        if not isinstance(groups, list):
            raise ValueError("Structured CV skills.groups must be a list")
        for group in groups:
            if not isinstance(group, dict):
                raise ValueError("Structured CV skills.groups entries must be objects")
            if not isinstance(group.get("label"), str) or not group.get("label", "").strip():
                raise ValueError("Structured CV skills.groups entries require a non-empty label")
            if not isinstance(group.get("items"), list):
                raise ValueError("Structured CV skills.groups entries require list items")
            if not all(isinstance(item, str) for item in group["items"]):
                raise ValueError("Structured CV skills.groups items must be strings")


def _extract_json_payload(response_text: str) -> dict[str, Any]:
    stripped = response_text.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 3:
            stripped = "\n".join(lines[1:-1]).strip()
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("Model response did not contain a JSON object") from None
        parsed = json.loads(stripped[start:end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("Structured CV response must be a JSON object")
    return parsed


def _normalize_contact(raw_header: dict[str, Any], default_header: dict[str, Any]) -> dict[str, Any]:
    raw_contact = raw_header.get("contact")
    default_contact = default_header["contact"]
    if not isinstance(raw_contact, dict):
        raw_contact = {}
    return {
        "email": str(raw_contact.get("email") or default_contact.get("email") or "").strip() or None,
        "phone": str(raw_contact.get("phone") or default_contact.get("phone") or "").strip() or None,
        "linkedin": str(raw_contact.get("linkedin") or default_contact.get("linkedin") or "").strip() or None,
    }


def _normalize_bullets(values: Any) -> list[str]:
    return _coerce_string_list(values)


def _coerce_object_list(values: Any) -> list[dict[str, Any]]:
    if not isinstance(values, list):
        return []
    result: list[dict[str, Any]] = []
    for value in values:
        if isinstance(value, dict):
            result.append(value)
    return result


def _normalize_placeholder_token(value: Any) -> str:
    return _shared_normalize_placeholder_token(value)


def _is_placeholder_token(value: Any) -> bool:
    return _shared_is_placeholder_token(value)


def _is_synthetic_education_entry(entry: dict[str, Any]) -> bool:
    degree = entry.get("degree")
    institution = entry.get("institution")
    field = entry.get("field")
    start = entry.get("start")
    end = entry.get("end")
    all_placeholder = all(
        _is_placeholder_token(value)
        for value in (degree, institution, field, start, end)
    )
    if all_placeholder:
        return True
    if _is_placeholder_token(degree) and _is_placeholder_token(institution):
        if _is_placeholder_token(start) and _is_placeholder_token(end):
            return True
    return False


def _section_enabled(config: dict[str, Any], section_key: str) -> bool:
    composition = ((config.get("cv") or {}).get("composition") or {})
    section_cfg = composition.get(section_key)
    if not isinstance(section_cfg, dict):
        return True
    return bool(section_cfg.get("enabled", True))

def _is_synthetic_experience_entry(entry: dict[str, Any]) -> bool:
    bullets = [bullet for bullet in (entry.get("bullets") or []) if isinstance(bullet, str)]
    if any(not _is_placeholder_token(bullet) for bullet in bullets):
        return False
    return all(
        _is_placeholder_token(value)
        for value in (
            entry.get("role"),
            entry.get("company"),
            entry.get("start"),
            entry.get("end"),
            entry.get("location"),
        )
    )

def _is_synthetic_projects_entry(entry: dict[str, Any]) -> bool:
    bullets = [bullet for bullet in (entry.get("bullets") or []) if isinstance(bullet, str)]
    if any(not _is_placeholder_token(bullet) for bullet in bullets):
        return False
    return _is_placeholder_token(entry.get("name")) and _is_placeholder_token(entry.get("context"))

def _is_synthetic_certifications_entry(entry: dict[str, Any]) -> bool:
    return all(
        _is_placeholder_token(value)
        for value in (entry.get("name"), entry.get("issuer"), entry.get("year"))
    )

def _is_synthetic_publications_entry(entry: dict[str, Any]) -> bool:
    return all(
        _is_placeholder_token(value)
        for value in (entry.get("title"), entry.get("publisher"), entry.get("year"))
    )

def _is_synthetic_languages_entry(entry: dict[str, Any]) -> bool:
    return all(
        _is_placeholder_token(value)
        for value in (entry.get("name"), entry.get("level"))
    )

def _sanitize_section_entries(
    section_key: str,
    entries: list[dict[str, Any]],
    *,
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    if not _section_enabled(config, section_key):
        return []
    checks = {
        "education": _is_synthetic_education_entry,
        "experience": _is_synthetic_experience_entry,
        "projects": _is_synthetic_projects_entry,
        "certifications": _is_synthetic_certifications_entry,
        "publications": _is_synthetic_publications_entry,
        "languages": _is_synthetic_languages_entry,
    }
    checker = checks.get(section_key)
    if checker is None:
        return entries
    return [entry for entry in entries if not checker(entry)]


def _sanitize_education_entries(
    education_entries: list[dict[str, Any]],
    *,
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    return _sanitize_section_entries("education", education_entries, config=config)

def _normalize_header_section(
    raw_sections: dict[str, Any],
    default_sections: dict[str, Any],
) -> dict[str, Any]:
    raw_header = raw_sections.get("header")
    if not isinstance(raw_header, dict):
        raw_header = {}
    return {
        "name": str(raw_header.get("name") or default_sections["header"]["name"]).strip(),
        "title": str(raw_header.get("title") or default_sections["header"]["title"]).strip(),
        "location": str(raw_header.get("location") or default_sections["header"]["location"] or "").strip() or None,
        "contact": _normalize_contact(raw_header, default_sections["header"]),
    }

def _normalize_summary_section(raw_sections: dict[str, Any]) -> dict[str, str]:
    raw_summary = raw_sections.get("summary")
    if isinstance(raw_summary, dict):
        summary_text = str(raw_summary.get("text") or "").strip()
    elif isinstance(raw_summary, str):
        summary_text = raw_summary.strip()
    else:
        summary_text = ""
    return {"text": summary_text}

def _normalize_experience_section(raw_sections: dict[str, Any], *, config: dict[str, Any]) -> list[dict[str, Any]]:
    experience_entries: list[dict[str, Any]] = []
    for item in _coerce_object_list(raw_sections.get("experience")):
        experience_entries.append(
            {
                "role": str(item.get("role") or "").strip(),
                "company": str(item.get("company") or "").strip(),
                "start": str(item.get("start") or "").strip() or None,
                "end": str(item.get("end") or "").strip() or None,
                "location": str(item.get("location") or "").strip() or None,
                "bullets": _normalize_bullets(item.get("bullets")),
            }
        )
    return _sanitize_section_entries("experience", experience_entries, config=config)

def _normalize_projects_section(raw_sections: dict[str, Any], *, config: dict[str, Any]) -> list[dict[str, Any]]:
    project_entries: list[dict[str, Any]] = []
    for item in _coerce_object_list(raw_sections.get("projects")):
        project_entries.append(
            {
                "name": str(item.get("name") or "").strip(),
                "context": str(item.get("context") or "").strip() or None,
                "bullets": _normalize_bullets(item.get("bullets")),
            }
        )
    return _sanitize_section_entries("projects", project_entries, config=config)

def _normalize_education_section(raw_sections: dict[str, Any], *, config: dict[str, Any]) -> list[dict[str, Any]]:
    education_entries: list[dict[str, Any]] = []
    for item in _coerce_object_list(raw_sections.get("education")):
        education_entries.append(
            {
                "degree": str(item.get("degree") or "").strip(),
                "institution": str(item.get("institution") or "").strip(),
                "field": str(item.get("field") or "").strip() or None,
                "start": str(item.get("start") or "").strip() or None,
                "end": str(item.get("end") or "").strip() or None,
            }
        )
    return _sanitize_education_entries(education_entries, config=config)

def _normalize_skills_section(raw_sections: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    raw_skills = raw_sections.get("skills")
    raw_groups = raw_skills.get("groups") if isinstance(raw_skills, dict) else []
    normalized_groups: list[dict[str, Any]] = []
    if isinstance(raw_groups, list):
        for group in raw_groups:
            if not isinstance(group, dict):
                continue
            normalized_groups.append(
                {
                    "label": str(group.get("label") or "").strip(),
                    "items": _coerce_string_list(group.get("items")),
                }
            )
    return {"groups": normalized_groups}

def _normalize_certifications_section(raw_sections: dict[str, Any], *, config: dict[str, Any]) -> list[dict[str, Any]]:
    certification_entries: list[dict[str, Any]] = []
    for item in _coerce_object_list(raw_sections.get("certifications")):
        certification_entries.append(
            {
                "name": str(item.get("name") or "").strip(),
                "issuer": str(item.get("issuer") or "").strip() or None,
                "year": str(item.get("year") or "").strip() or None,
            }
        )
    return _sanitize_section_entries("certifications", certification_entries, config=config)

def _normalize_publications_section(raw_sections: dict[str, Any], *, config: dict[str, Any]) -> list[dict[str, Any]]:
    publication_entries: list[dict[str, Any]] = []
    for item in _coerce_object_list(raw_sections.get("publications")):
        publication_entries.append(
            {
                "title": str(item.get("title") or "").strip(),
                "publisher": str(item.get("publisher") or "").strip() or None,
                "year": str(item.get("year") or "").strip() or None,
            }
        )
    return _sanitize_section_entries("publications", publication_entries, config=config)

def _normalize_languages_section(raw_sections: dict[str, Any], *, config: dict[str, Any]) -> list[dict[str, Any]]:
    language_entries: list[dict[str, Any]] = []
    for item in _coerce_object_list(raw_sections.get("languages")):
        language_entries.append(
            {
                "name": str(item.get("name") or "").strip(),
                "level": str(item.get("level") or "").strip() or None,
            }
        )
    return _sanitize_section_entries("languages", language_entries, config=config)


def _normalize_structured_cv(
    raw_structured_cv: dict[str, Any],
    *,
    jd: dict[str, Any],
    profile: dict[str, Any] | None,
    config: dict[str, Any],
    fit_classification: str,
) -> dict[str, Any]:
    normalized = build_empty_structured_cv(
        jd=jd,
        profile=profile,
        config=config,
        fit_classification=fit_classification,
    )
    raw_sections = raw_structured_cv.get("sections")
    if not isinstance(raw_sections, dict):
        raw_sections = {}
    default_sections = normalized["sections"]

    normalized["sections"]["header"] = _normalize_header_section(raw_sections, default_sections)
    normalized["sections"]["summary"] = _normalize_summary_section(raw_sections)

    normalized["sections"]["experience"] = _normalize_experience_section(raw_sections, config=config)
    normalized["sections"]["projects"] = _normalize_projects_section(raw_sections, config=config)
    normalized["sections"]["education"] = _normalize_education_section(raw_sections, config=config)
    normalized["sections"]["skills"] = _normalize_skills_section(raw_sections)

    normalized["sections"]["certifications"] = _normalize_certifications_section(raw_sections, config=config)
    normalized["sections"]["publications"] = _normalize_publications_section(raw_sections, config=config)
    normalized["sections"]["languages"] = _normalize_languages_section(raw_sections, config=config)

    validate_structured_cv(normalized, config=config)
    return normalized


def build_structured_generation_prompt(
    jd: dict[str, Any],
    evidence: list[dict[str, Any]],
    gap: dict[str, Any],
    template: str,
    profile: dict[str, Any] | None = None,
    *,
    config: dict[str, Any] | None = None,
    evidence_selection_summary: dict[str, Any] | None = None,
    repair_missing_sections: list[str] | None = None,
) -> str:
    structured_schema = textwrap.dedent(
        """\
        {
          "sections": {
            "header": {
              "name": "...",
              "title": "...",
              "location": null,
              "contact": {"email": null, "phone": null, "linkedin": null}
            },
            "summary": {"text": "..."},
            "experience": [{"role": "...", "company": "...", "start": null, "end": null, "location": null, "bullets": ["..."]}],
            "projects": [{"name": "...", "context": null, "bullets": ["..."]}],
            "education": [{"degree": "...", "institution": "...", "field": null, "start": null, "end": null}],
            "skills": {"groups": [{"label": "...", "items": ["..."]}]},
            "certifications": [{"name": "...", "issuer": null, "year": null}],
            "publications": [{"title": "...", "publisher": null, "year": null}],
            "languages": [{"name": "...", "level": null}]
          }
        }
        """
    )
    prompt_context = _build_generation_prompt_context(
        jd=jd,
        evidence=evidence,
        gap=gap,
        template=template,
        profile=profile,
        config=config,
        evidence_selection_summary=evidence_selection_summary,
        repair_missing_sections=repair_missing_sections,
    )
    prompt_id = get_cv_generation_structured_prompt_id(config or {})
    return render_prompt(
        prompt_id,
        {
            **prompt_context,
            "structured_schema": structured_schema,
            "output_instruction": (
                "Write only valid JSON matching the schema below. "
                "Do not add commentary. Do not wrap the JSON in markdown code fences."
            ),
        },
    ).text


# ── prompt assembly ───────────────────────────────────────────────────────────

def build_generation_prompt(
    jd: dict[str, Any],
    evidence: list[dict[str, Any]],
    gap: dict[str, Any],
    template: str,
    profile: dict[str, Any] | None = None,
    *,
    config: dict[str, Any] | None = None,
    evidence_selection_summary: dict[str, Any] | None = None,
    repair_missing_sections: list[str] | None = None,
) -> str:
    """Assemble the full LLM prompt for CV generation.

    The prompt contains:
    - JD context (title + required skills)
    - Selected evidence items (name + skills)
    - Gap constraints (matched and missing skills)
    - The Jinja2 template string as the output format guide

    Returns a plain string suitable for sending to an LLM as a single user message.
    """
    prompt_context = _build_generation_prompt_context(
        jd=jd,
        evidence=evidence,
        gap=gap,
        template=template,
        profile=profile,
        config=config,
        evidence_selection_summary=evidence_selection_summary,
        repair_missing_sections=repair_missing_sections,
    )
    return render_prompt(
        LEGACY_MARKDOWN_PROMPT_ID,
        {
            **prompt_context,
            "output_instruction": "Write only the completed CV markdown. Do not add commentary.",
        },
    ).text


# ── template rendering ────────────────────────────────────────────────────────

def render_cv_template(
    template_str: str,
    selected_skills: list[str],
    selected_experiences: list[dict[str, Any]],
    selected_projects: list[dict[str, Any]],
    candidate: dict[str, Any],
    headline: str,
    summary: str,
    selected_education: list[dict[str, Any]] | None = None,
    selected_publications: list[dict[str, Any]] | None = None,
    selected_certifications: list[dict[str, Any]] | None = None,
    selected_languages: list[dict[str, Any]] | None = None,
) -> str:
    """Render a Jinja2 CV template with the selected evidence slots.

    Args:
        template_str:          Jinja2 template source string.
        selected_skills:       Skills to populate the Skills section.
        selected_experiences:  Experience dicts (role, company, start, end, bullets).
        selected_projects:     Project dicts (name, description).
        candidate:             Candidate metadata dict (must include ``name``).
        headline:              One-line professional headline.
        summary:               Professional summary paragraph.

    Returns the rendered markdown string.
    Raises TemplateError on rendering failure (propagated to caller).
    """
    env = Environment(loader=BaseLoader(), autoescape=False)  # noqa: S701 — output is markdown, not HTML
    tmpl = env.from_string(template_str)
    return tmpl.render(
        selected_skills=selected_skills,
        selected_experiences=selected_experiences,
        selected_projects=selected_projects,
        selected_education=selected_education or [],
        selected_publications=selected_publications or [],
        selected_certifications=selected_certifications or [],
        selected_languages=selected_languages or [],
        candidate=candidate,
        headline=headline,
        summary=summary,
    )

def _normalize_cv_markdown(markdown: str) -> str:
    """Deterministically normalize rendered CV markdown formatting."""
    text = str(markdown or "").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"(?m)^\s*[\*\u2022]\s+", "- ", text)
    text = re.sub(r"(?m)^\s*-\s{2,}", "- ", text)
    raw_lines = [line.rstrip() for line in text.split("\n")]
    lines: list[str] = []
    for idx, line in enumerate(raw_lines):
        stripped = line.strip()
        if stripped:
            lines.append(line)
            continue
        prev_nonempty = ""
        next_nonempty = ""
        for prev_idx in range(idx - 1, -1, -1):
            if raw_lines[prev_idx].strip():
                prev_nonempty = raw_lines[prev_idx].strip()
                break
        for next_idx in range(idx + 1, len(raw_lines)):
            if raw_lines[next_idx].strip():
                next_nonempty = raw_lines[next_idx].strip()
                break
        # Drop empty lines between adjacent list bullets for compact, consistent lists.
        if prev_nonempty.startswith("- ") and next_nonempty.startswith("- "):
            continue
        lines.append(line)

    normalized_lines: list[str] = []
    previous_was_blank = False
    for line in lines:
        is_blank = not line.strip()
        if is_blank:
            if not previous_was_blank:
                normalized_lines.append("")
            previous_was_blank = True
            continue
        normalized_lines.append(line)
        previous_was_blank = False
    text = "\n".join(normalized_lines).strip()
    text = re.sub(r"\n{2,}(## )", r"\n\n\1", text)
    text = re.sub(r"(?<!\n)\n(## )", r"\n\n\1", text)
    return text.strip() + "\n"


# ── LLM generation (integration) ─────────────────────────────────────────────

def _resolve_template_path(config: dict[str, Any]) -> str:
    """Resolve the CV template path from the preset-based config.

    Priority:
    1. config["_template_path"] — test shim (used by generate_cv test fixtures)
    2. cv_presets.get_template_path(config["cv"]["preset"])
    3. Fallback to flat cv_template_path for legacy compatibility
    """
    if "_template_path" in config:
        return str(config["_template_path"])
    cv_cfg = config.get("cv") or {}
    preset = str(cv_cfg.get("preset", ""))
    if preset:
        from fitcv.cv_presets import get_template_path

        return get_template_path(preset)
    return str(config.get("cv_template_path", "templates/cv_template.md"))


def render_cv_markdown(structured_cv: dict[str, Any], config: dict[str, Any]) -> str:
    """Render markdown from a validated structured CV document."""
    import pathlib

    structured_cv = deepcopy(structured_cv)
    sections = structured_cv.get("sections")
    if isinstance(sections, dict):
        for section_key in ("experience", "projects", "education", "certifications", "publications", "languages"):
            section_items = sections.get(section_key)
            if not isinstance(section_items, list):
                continue
            sections[section_key] = _sanitize_section_entries(
                section_key,
                [item for item in section_items if isinstance(item, dict)],
                config=config,
            )
    validate_structured_cv(structured_cv, config=config)
    template_path = _resolve_template_path(config)
    template_str = pathlib.Path(template_path).read_text(encoding="utf-8")
    enabled_section_names = _get_enabled_section_names(config)
    template_str = _filter_template_by_enabled_sections(template_str, enabled_section_names)
    sections = structured_cv["sections"]
    header = sections["header"]
    flattened_skills = [
        item
        for group in sections["skills"]["groups"]
        for item in group["items"]
    ]
    selected_projects: list[dict[str, Any]] = []
    for project in sections["projects"]:
        context = str(project.get("context") or "").strip()
        bullets = [str(bullet).strip() for bullet in (project.get("bullets") or []) if str(bullet).strip()]
        description_lines: list[str] = []
        if context:
            description_lines.append(context)
        if bullets:
            description_lines.extend(f"- {bullet}" for bullet in bullets)
        selected_projects.append(
            {
                "name": project.get("name"),
                "description": "\n".join(description_lines).strip(),
            }
        )
    selected_education = [
        {
            "degree": item.get("degree"),
            "institution": item.get("institution"),
            "field": item.get("field"),
            "start": item.get("start"),
            "end": item.get("end"),
        }
        for item in sections["education"]
    ]
    selected_publications = [
        {
            "title": item.get("title"),
            "publisher": item.get("publisher"),
            "year": item.get("year"),
        }
        for item in sections["publications"]
    ]
    selected_certifications = [
        {
            "name": item.get("name"),
            "issuer": item.get("issuer"),
            "year": item.get("year"),
        }
        for item in sections["certifications"]
    ]
    selected_languages = [
        {
            "name": item.get("name"),
            "level": item.get("level"),
        }
        for item in sections["languages"]
    ]
    rendered = render_cv_template(
        template_str=template_str,
        selected_skills=flattened_skills,
        selected_experiences=sections["experience"],
        selected_projects=selected_projects,
        selected_education=selected_education,
        selected_publications=selected_publications,
        selected_certifications=selected_certifications,
        selected_languages=selected_languages,
        candidate={"name": header.get("name", "")},
        headline=str(header.get("title") or ""),
        summary=str(sections["summary"].get("text") or ""),
    )
    return _normalize_cv_markdown(rendered)


def generate_structured_cv(
    jd: dict[str, Any],
    evidence: list[dict[str, Any]],
    gap: dict[str, Any],
    profile: dict[str, Any],
    config: dict[str, Any],
    *,
    fit_classification: str,
    evidence_selection_summary: dict[str, Any] | None = None,
    repair_missing_sections: list[str] | None = None,
) -> dict[str, Any]:
    """Call the LLM to generate a structured CV document."""
    import pathlib

    template_path = _resolve_template_path(config)
    template_str = pathlib.Path(template_path).read_text(encoding="utf-8")
    prompt = build_structured_generation_prompt(
        jd=jd,
        evidence=evidence,
        gap=gap,
        template=template_str,
        profile=profile,
        config=config,
        evidence_selection_summary=evidence_selection_summary,
        repair_missing_sections=repair_missing_sections,
    )

    client = _make_generation_client(config)

    model_name = get_cv_generation_model(config)
    response = client.models.generate_content(model=model_name, contents=prompt)
    response_payload = _extract_json_payload(str(response.text))
    return _normalize_structured_cv(
        response_payload,
        jd=jd,
        profile=profile,
        config=config,
        fit_classification=fit_classification,
    )


def generate_cv(
    jd: dict[str, Any],
    evidence: list[dict[str, Any]],
    gap: dict[str, Any],
    profile: dict[str, Any],
    config: dict[str, Any],
    *,
    fit_classification: str = "unclassified",
    evidence_selection_summary: dict[str, Any] | None = None,
    repair_missing_sections: list[str] | None = None,
) -> dict[str, Any]:
    """Generate structured CV content and render markdown from it.

    Temporary compatibility wrapper during rollout.
    """
    structured_cv = generate_structured_cv(
        jd=jd,
        evidence=evidence,
        gap=gap,
        profile=profile,
        config=config,
        fit_classification=fit_classification,
        evidence_selection_summary=evidence_selection_summary,
        repair_missing_sections=repair_missing_sections,
    )
    markdown = render_cv_markdown(structured_cv, config)
    return {
        "structured_cv": structured_cv,
        "markdown": markdown,
    }






