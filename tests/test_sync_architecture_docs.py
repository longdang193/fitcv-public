"""
@meta
type: test
scope: unit
domain: docs
covers:
  - option-b phase-2 architecture sync rollout
excludes:
  - full code metadata backfill
tags:
  - fast
  - ci-safe
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "sync_architecture_docs.py"
GENERATOR_PATH = REPO_ROOT / "tools" / "docs" / "generate_architecture_metadata.py"
VALIDATOR_PATH = REPO_ROOT / "scripts" / "validate_adoption_shape.py"
FORMATTER_PATH = REPO_ROOT / "scripts" / "format_contract_yaml.py"
AUDIT_PATH = REPO_ROOT / "scripts" / "audit_architecture_linkage.py"


class IndentedSafeDumper(yaml.SafeDumper):
    def increase_indent(self, flow: bool = False, indentless: bool = False) -> object:
        return super().increase_indent(flow, False)


def load_sync_module():
    spec = importlib.util.spec_from_file_location("sync_architecture_docs", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_yaml(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.dump(
            payload,
            Dumper=IndentedSafeDumper,
            sort_keys=False,
            default_flow_style=False,
            allow_unicode=False,
            width=10_000,
        ),
        encoding="utf-8",
    )


def read_yaml(path: Path) -> object:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def build_minimal_repo(tmp_path: Path) -> Path:
    repo_root = tmp_path / "repo"
    write_yaml(
        repo_root / "docs" / "features" / "cv_system" / "feature.source.yaml",
        {
            "feature_id": "cv_system",
            "name": "CV System",
            "status": "active",
            "type": "add",
            "summary": "Pilot source for CV generation lifecycle ownership.",
            "invariants": [],
            "domains": ["pipeline", "cv_generation"],
            "depends_on": ["admin_control_plane_core"],
            "capabilities": [
                {
                    "capability_id": "cv_system.structured-cv-generation",
                    "statement": "Generate structured CV artifacts from grounded evidence.",
                    "state": "active",
                }
            ],
            "stage_participation": [
                {
                    "stage_id": "cv_analysis",
                    "capability_ids": ["cv_system.structured-cv-generation"],
                    "role": "primary",
                }
            ],
        },
    )
    write_yaml(
        repo_root / "docs" / "stages" / "cv_analysis.source.yaml",
        {
            "stage_id": "cv_analysis",
            "name": "CV Analysis",
            "summary": "Pilot source for the pre-generation evidence stage.",
            "depends_on": ["ranking"],
            "primary_features": ["cv_system"],
            "related_features": ["inspection_debugging"],
            "inputs": ["ranked jobs", "candidate profile"],
            "outputs": ["generation-ready jobs"],
        },
    )
    write_yaml(
        repo_root / "docs" / "features" / "admin_control_plane_core" / "feature.source.yaml",
        {
            "feature_id": "admin_control_plane_core",
            "name": "Admin Control Plane Core",
            "status": "active",
            "type": "add",
            "summary": "Own the admin API and web surface for pipeline operations.",
            "invariants": [],
            "domains": ["admin_ui"],
            "depends_on": [],
            "capabilities": [
                {
                    "capability_id": "admin_control_plane_core.fastapi-web-server",
                    "statement": "Serve the admin control plane over FastAPI.",
                    "state": "active",
                },
                {
                    "capability_id": "admin_control_plane_core.jinja2-admin-pages",
                    "statement": "Render admin pages with Jinja2 templates.",
                    "state": "active",
                },
            ],
        },
    )
    (repo_root / "docs" / "features" / "cv_system" / "history.md").write_text(
        "# History\n\n## Human Notes\nLegacy CV note.\n", encoding="utf-8"
    )
    (repo_root / "docs" / "features" / "admin_control_plane_core" / "history.md").write_text(
        "# History\n\n## Human Notes\nLegacy admin note.\n", encoding="utf-8"
    )
    (repo_root / "docs" / "generated").mkdir(parents=True, exist_ok=True)
    (repo_root / "docs" / "operating_system").mkdir(parents=True, exist_ok=True)
    (repo_root / "docs" / "superpowers" / "specs").mkdir(parents=True, exist_ok=True)
    (repo_root / "docs" / "superpowers" / "plans").mkdir(parents=True, exist_ok=True)
    (repo_root / "scripts").mkdir(parents=True, exist_ok=True)
    (repo_root / "tests").mkdir(parents=True, exist_ok=True)
    (repo_root / "tools" / "docs").mkdir(parents=True, exist_ok=True)
    (repo_root / "config" / "runtime").mkdir(parents=True, exist_ok=True)
    (repo_root / "docs" / "superpowers" / "archive" / "specs").mkdir(parents=True, exist_ok=True)
    (repo_root / "docs" / "superpowers" / "archive" / "plans").mkdir(parents=True, exist_ok=True)
    for relative_path, content in {
        "docs/setup.md": (
            "# Setup\n\n"
            "Install the required dependencies, confirm tool versions, provision prerequisites, and bootstrap in order.\n"
        ),
        "docs/configuration.md": (
            "# Configuration\n\n"
            "Each environment variable and config file has profile defaults, override rules, ownership, and repo_config guidance.\n"
        ),
        "docs/usage.md": (
            "# Usage\n\n"
            "Use the command entrypoint for the operator workflow, developer flow, and run loop.\n"
        ),
        "docs/pipeline.md": (
            "# Pipeline\n\n"
            "The stage workflow documents the processing flow, sequence, handoff, and step order.\n"
        ),
        "docs/architecture.md": (
            "# Architecture\n\n"
            "The architecture captures each component boundary, integration point, information flow, and control flow.\n"
        ),
        "docs/intent/README.md": "# Intent\n",
        "docs/intent/project-charter.md": "# Project Charter\n",
        "docs/intent/stakeholders.md": "# Stakeholders\n",
        "docs/intent/success-outcomes.md": "# Success Outcomes\n",
        "docs/intent/constraints-and-non-goals.md": "# Constraints And Non-Goals\n",
    }.items():
        target = repo_root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    write_yaml(
        repo_root / "repo_config" / "adoption-mode.yaml",
        {
            "adoption_mode": "managed_architecture_metadata",
            "managed_architecture_metadata": True,
            "legacy_feature_contracts": False,
            "architecture_generator": "scripts/sync_architecture_docs.py",
            "starter_sync": {
                "starter_baseline_ref": "a5d2d85b3174cde84f90df26642385b429e3c194",
                "last_shared_surface_review_at": "2026-04-22",
                "reviewed_surface_classes": [
                    "repo_config",
                    "operating_system_docs",
                    "skills",
                    "adapters",
                    "generated_instruction_surfaces",
                    "validation_and_sync_scripts",
                ],
                "divergences": [
                    {
                        "path": "docs/features/*/history.md",
                        "class": "operating_system_docs",
                        "status": "customized",
                        "rationale": "Feature histories have not yet been migrated to the starter partial-generated history pattern.",
                    }
                ],
            },
        },
    )
    (repo_root / "scripts" / "validate_adoption_shape.py").write_text(
        VALIDATOR_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (repo_root / "scripts" / "format_contract_yaml.py").write_text(
        FORMATTER_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (repo_root / "scripts" / "audit_architecture_linkage.py").write_text(
        AUDIT_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (repo_root / "tools" / "docs" / "generate_architecture_metadata.py").write_text(
        GENERATOR_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    for test_name in (
        "test_architecture_metadata_generation.py",
        "test_architecture_linkage_audit.py",
        "test_format_contract_yaml.py",
        "test_validate_adoption_shape.py",
        "test_setup_hooks.py",
    ):
        (repo_root / "tests" / test_name).write_text(
            '"""\n@meta\ntype: test\nscope: unit\ndomain: docs\n"""\n\n'
            "def test_placeholder() -> None:\n"
            "    assert True\n",
            encoding="utf-8",
        )
    (repo_root / "scripts" / "cv_writer.py").write_text(
        '"""\n'
        "@meta\n"
        "name: cv_writer\n"
        "type: script\n"
        "domain: cv_generation\n"
        "capabilities: []\n"
        '"""\n\n'
        "def build_cv() -> None:\n"
        '    """\n'
        "    @capability cv_system.structured-cv-generation\n"
        '    """\n'
        "    return None\n\n"
        "def main() -> None:\n"
        "    build_cv()\n"
        "    return None\n",
        encoding="utf-8",
    )
    (repo_root / "tests" / "test_cv_writer.py").write_text(
        '"""\n'
        "@meta\n"
        "type: test\n"
        "scope: unit\n"
        "domain: cv_generation\n"
        '"""\n\n'
        "# @proves cv_system.structured-cv-generation\n"
        "def test_placeholder() -> None:\n"
        "    assert True\n",
        encoding="utf-8",
    )
    (repo_root / "docs" / "superpowers" / "archive" / "specs" / "2026-04-22-cv-system-spec.md").write_text(
        "---\n"
        "artifact_type: spec\n"
        "related_features:\n"
        "  - cv_system\n"
        "---\n\n"
        "# CV System Spec\n",
        encoding="utf-8",
    )
    (repo_root / "docs" / "superpowers" / "archive" / "plans" / "2026-04-22-cv-system-plan.md").write_text(
        "---\n"
        "artifact_type: plan\n"
        "status: completed\n"
        "completed_at: 2026-04-22T20:45:00+02:00\n"
        "change_id: 2026-04-22-cv-system-lineage\n"
        "related_features:\n"
        "  - cv_system\n"
        "affects:\n"
        "  capabilities:\n"
        "    - cv_system.structured-cv-generation\n"
        "verification:\n"
        "  - pytest tests/test_cv_writer.py\n"
        "outcome:\n"
        "  summary: CV generation lineage metadata is now explicit.\n"
        "---\n\n"
        "# CV System Plan\n",
        encoding="utf-8",
    )
    (repo_root / "config" / "runtime" / "prompts.yaml").write_text(
        "# @architecture\n"
        "# owner: cv_system\n"
        "# features:\n"
        "#   - cv_system\n"
        "# stages:\n"
        "#   - cv_analysis\n"
        "# capabilities:\n"
        "#   - cv_system.structured-cv-generation\n"
        "# role: config\n"
        "# canonical: true\n\n"
        "prompts:\n"
        "  cv_generation:\n"
        "    structured_write:\n"
        "      prompt_id: cv_generation.structured_write.v1\n",
        encoding="utf-8",
    )
    return repo_root


def test_sync_script_exists() -> None:
    assert SCRIPT_PATH.exists(), "Expected scripts/sync_architecture_docs.py to exist."


def test_sync_script_writes_feature_and_stage_outputs(tmp_path: Path) -> None:
    repo_root = build_minimal_repo(tmp_path)
    sync_module = load_sync_module()

    exit_code = sync_module.main(["--repo-root", str(repo_root)])

    assert exit_code == 0

    feature_contract = read_yaml(repo_root / "docs" / "features" / "cv_system" / "cv_system.yaml")
    assert feature_contract["feature_id"] == "cv_system"
    assert feature_contract["name"] == "CV System"
    assert feature_contract["capabilities"][0]["capability_id"] == (
        "cv_system.structured-cv-generation"
    )
    assert feature_contract["refs"]["history"] == ["docs/features/cv_system/history.md"]
    assert feature_contract["refs"]["spec"] == ["docs/superpowers/archive/specs/2026-04-22-cv-system-spec.md"]
    assert feature_contract["refs"]["plan"] == ["docs/superpowers/archive/plans/2026-04-22-cv-system-plan.md"]

    admin_contract = read_yaml(
        repo_root / "docs" / "features" / "admin_control_plane_core" / "admin_control_plane_core.yaml"
    )
    assert admin_contract["capabilities"][0]["capability_id"] == (
        "admin_control_plane_core.fastapi-web-server"
    )

    lineage = read_yaml(repo_root / "docs" / "features" / "cv_system" / "lineage.generated.yaml")
    raw_lineage = (repo_root / "docs" / "features" / "cv_system" / "lineage.generated.yaml").read_text(
        encoding="utf-8"
    )
    assert lineage["feature_id"] == "cv_system"
    assert lineage["source"] == "docs/features/cv_system/feature.source.yaml"
    assert set(lineage) == {"feature_id", "source", "invariants", "capabilities", "timeline"}
    assert "&id" not in raw_lineage
    assert "*id" not in raw_lineage
    capability_lineage = lineage["capabilities"]["cv_system.structured-cv-generation"]
    assert capability_lineage["state"] == "active"
    assert capability_lineage["statement"] == "Generate structured CV artifacts from grounded evidence."
    assert capability_lineage["code"] == [
        {
            "path": "scripts/cv_writer.py",
            "confidence": "high",
            "source": ["python_capability"],
            "symbols": ["build_cv"],
        }
    ]
    assert capability_lineage["tests"] == [
        {
            "path": "tests/test_cv_writer.py",
            "confidence": "high",
            "source": ["python_proves"],
        }
    ]
    assert capability_lineage["docs"] == ["docs/features/cv_system/history.md"]
    assert capability_lineage["configs"] == ["config/runtime/prompts.yaml"]
    assert capability_lineage["components"] == []
    assert capability_lineage["component_evidence"] == []
    assert capability_lineage["specs"] == ["docs/superpowers/archive/specs/2026-04-22-cv-system-spec.md"]
    assert capability_lineage["plans"] == ["docs/superpowers/archive/plans/2026-04-22-cv-system-plan.md"]
    assert capability_lineage["completeness_status"] == "complete"
    assert lineage["timeline"] == [
        {
            "completed_at": "2026-04-22T20:45:00+02:00",
            "source_plan": "docs/superpowers/archive/plans/2026-04-22-cv-system-plan.md",
            "change_id": "2026-04-22-cv-system-lineage",
            "summary": "CV System Plan",
            "capabilities": ["cv_system.structured-cv-generation"],
            "verification": ["pytest tests/test_cv_writer.py"],
            "outcome": "CV generation lineage metadata is now explicit.",
        }
    ]

    stage_contract = read_yaml(repo_root / "docs" / "stages" / "cv_analysis.yaml")
    assert stage_contract["cv_analysis"]["primary_features"] == ["cv_system"]


def test_sync_script_refreshes_generated_discovery_outputs(tmp_path: Path) -> None:
    repo_root = build_minimal_repo(tmp_path)
    sync_module = load_sync_module()

    exit_code = sync_module.main(["--repo-root", str(repo_root)])

    assert exit_code == 0

    architecture_dag = read_yaml(repo_root / "docs" / "generated" / "architecture_dag.yaml")
    node_ids = {entry["id"] for entry in architecture_dag["nodes"]}
    assert "admin_control_plane_core" in node_ids
    assert "cv_analysis" in node_ids
    assert "cv_system.structured-cv-generation" in node_ids

    edges = architecture_dag["edges"]
    assert {"from": "cv_system", "to": "admin_control_plane_core", "type": "depends_on"} in edges
    assert {"from": "cv_system", "to": "cv_analysis", "type": "participates_in", "role": "primary", "capability_ids": ["cv_system.structured-cv-generation"]} in edges

    capability_lineage = read_yaml(repo_root / "docs" / "generated" / "capability_lineage.yaml")
    cv_feature = capability_lineage["features"]["cv_system"]
    assert cv_feature["summary"] == "Pilot source for CV generation lifecycle ownership."
    capability = cv_feature["capabilities"]["cv_system.structured-cv-generation"]
    assert capability["statement"] == "Generate structured CV artifacts from grounded evidence."
    assert capability["code"] == [
        {
            "path": "scripts/cv_writer.py",
            "confidence": "high",
            "source": ["python_capability"],
            "symbols": ["build_cv"],
        }
    ]
    assert capability["configs"] == ["config/runtime/prompts.yaml"]
    assert capability["components"] == []


def test_sync_script_check_mode_reports_legacy_generated_outputs_as_stale(tmp_path: Path) -> None:
    repo_root = build_minimal_repo(tmp_path)
    sync_module = load_sync_module()

    assert sync_module.main(["--repo-root", str(repo_root)]) == 0
    (repo_root / "docs" / "generated" / "features_index.yaml").write_text("legacy: true\n", encoding="utf-8")

    process = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--repo-root", str(repo_root), "--check"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert process.returncode == 1
    assert "docs/generated/features_index.yaml" in process.stdout


def test_sync_script_check_mode_detects_stale_outputs(tmp_path: Path) -> None:
    repo_root = build_minimal_repo(tmp_path)
    sync_module = load_sync_module()

    assert sync_module.main(["--repo-root", str(repo_root)]) == 0
    generated_contract = repo_root / "docs" / "features" / "cv_system" / "cv_system.yaml"
    generated_contract.write_text("stale: true\n", encoding="utf-8")

    process = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--repo-root", str(repo_root), "--check"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert process.returncode == 1
    assert "stale generated file" in process.stdout.lower()


def test_sync_script_rejects_legacy_string_capabilities(tmp_path: Path) -> None:
    repo_root = build_minimal_repo(tmp_path)

    feature_source_path = repo_root / "docs" / "features" / "cv_system" / "feature.source.yaml"
    payload = yaml.safe_load(feature_source_path.read_text(encoding="utf-8"))
    payload["capabilities"] = ["Structured CV Generation"]
    feature_source_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    process = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--repo-root", str(repo_root)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert process.returncode == 1
    assert "must be a mapping" in (process.stderr + process.stdout)


def test_sync_script_normalizes_generated_summary_and_statement_text(tmp_path: Path) -> None:
    repo_root = build_minimal_repo(tmp_path)
    sync_module = load_sync_module()

    source_path = repo_root / "docs" / "features" / "cv_system" / "feature.source.yaml"
    payload = yaml.safe_load(source_path.read_text(encoding="utf-8"))
    payload["summary"] = "Normalized summary with trailing space.   \n\n"
    payload["capabilities"][0]["statement"] = "Statement with trailing space.   \n\n"
    write_yaml(source_path, payload)

    assert sync_module.main(["--repo-root", str(repo_root)]) == 0

    contract = read_yaml(repo_root / "docs" / "features" / "cv_system" / "cv_system.yaml")
    assert contract["summary"] == "Normalized summary with trailing space."
    assert contract["capabilities"][0]["statement"] == "Statement with trailing space."


def test_sync_script_emits_empty_timeline_when_plan_lacks_completed_metadata(tmp_path: Path) -> None:
    repo_root = build_minimal_repo(tmp_path)
    sync_module = load_sync_module()

    plan_path = repo_root / "docs" / "superpowers" / "archive" / "plans" / "2026-04-22-cv-system-plan.md"
    plan_path.write_text(
        "---\n"
        "artifact_type: plan\n"
        "related_features:\n"
        "  - cv_system\n"
        "---\n\n"
        "# CV System Plan\n",
        encoding="utf-8",
    )

    assert sync_module.main(["--repo-root", str(repo_root)]) == 0

    lineage = read_yaml(repo_root / "docs" / "features" / "cv_system" / "lineage.generated.yaml")
    assert lineage["timeline"] == []
