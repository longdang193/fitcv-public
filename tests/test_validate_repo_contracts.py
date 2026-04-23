"""
@meta
type: test
scope: unit
domain: docs
covers:
  - repo contract validation
excludes:
  - full project integration beyond focused validator scenarios
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


class IndentedSafeDumper(yaml.SafeDumper):
    def increase_indent(self, flow: bool = False, indentless: bool = False) -> object:
        return super().increase_indent(flow, False)


REPO_ROOT = Path(__file__).resolve().parents[1]
VALIDATOR_PATH = REPO_ROOT / "scripts" / "validate_repo_contracts.py"
REPO_CONFIG_VALIDATOR_PATH = REPO_ROOT / "scripts" / "validate_repo_config.py"
GENERATOR_PATH = REPO_ROOT / "tools" / "docs" / "generate_architecture_metadata.py"
FORMATTER_PATH = REPO_ROOT / "scripts" / "format_contract_yaml.py"
AUDIT_PATH = REPO_ROOT / "scripts" / "audit_architecture_linkage.py"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
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


def build_repo(tmp_path: Path) -> Path:
    repo_root = tmp_path / "repo"
    write_yaml(
        repo_root / "docs" / "features" / "cv_system" / "feature.source.yaml",
        {
            "feature_id": "cv_system",
            "name": "CV System",
            "status": "active",
            "type": "add",
            "summary": "Own the CV-writing lifecycle.",
            "invariants": [],
            "domains": ["pipeline"],
            "depends_on": [],
            "capabilities": [
                {
                    "capability_id": "cv_system.structured-cv-generation",
                    "statement": "Generate CV artifacts.",
                    "state": "active",
                }
            ],
            "stage_participation": [
                {
                    "stage_id": "cv_analysis",
                    "role": "primary",
                    "capability_ids": ["cv_system.structured-cv-generation"],
                }
            ],
        },
    )
    (repo_root / "docs" / "features" / "cv_system" / "history.md").write_text(
        "# History\n\nManual note only.\n", encoding="utf-8"
    )
    write_yaml(
        repo_root / "docs" / "stages" / "cv_analysis.source.yaml",
        {
            "stage_id": "cv_analysis",
            "name": "CV Analysis",
            "summary": "Prepare ranked jobs for writing.",
            "depends_on": [],
            "primary_features": ["cv_system"],
            "related_features": [],
            "inputs": ["ranked jobs"],
            "outputs": ["generation-ready jobs"],
        },
    )
    for relative_path, content in {
        "docs/setup.md": (
            "# Setup\n\n"
            "Install dependencies, confirm tool versions, provision prerequisites, and run bootstrap in order.\n"
        ),
        "docs/configuration.md": (
            "# Configuration\n\n"
            "Each environment variable and config file has profile defaults, override rules, ownership, and repo_config guidance.\n"
        ),
        "docs/usage.md": (
            "# Usage\n\n"
            "The command entrypoint supports the operator workflow, developer flow, and run loop.\n"
        ),
        "docs/pipeline.md": (
            "# Pipeline\n\n"
            "Each stage in the workflow documents its step sequence, handoff, and processing flow.\n"
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
                "starter_baseline_ref": "a1dd288e9d37f4a0870d088fcb9431f3c60a72a1",
                "last_shared_surface_review_at": "2026-04-22",
                "reviewed_surface_classes": [
                    "repo_config",
                    "operating_system_docs",
                    "skills",
                    "adapters",
                    "generated_instruction_surfaces",
                    "validation_and_sync_scripts",
                ],
                "divergences": [],
            },
        },
    )
    (repo_root / "docs" / "operating_system").mkdir(parents=True, exist_ok=True)
    (repo_root / "docs" / "superpowers" / "specs").mkdir(parents=True, exist_ok=True)
    (repo_root / "docs" / "superpowers" / "plans").mkdir(parents=True, exist_ok=True)
    (repo_root / "config").mkdir(parents=True, exist_ok=True)
    (repo_root / "config" / "runtime").mkdir(parents=True, exist_ok=True)
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
        "prompts:\n  cv_generation:\n    structured_write:\n      prompt_id: cv_generation.structured_write.v1\n",
        encoding="utf-8",
    )
    (repo_root / "repo_config" / "publication-config.json").write_text(
        '{\n'
        '  "publicPaths": ["README.md"],\n'
        '  "forbiddenPaths": [".agents"],\n'
        '  "requiredPaths": ["README.md"],\n'
        '  "allowedGeneratedPaths": [],\n'
        '  "scrubPrivateReferencePaths": ["docs/features"]\n'
        '}\n',
        encoding="utf-8",
    )
    (repo_root / "repo_config" / "agent-adapter-mappings.json").write_text(
        '[\n'
        '  {\n'
        '    "source": "agent-core/adapters/codex/root-AGENTS.template.md",\n'
        '    "destination": "AGENTS.md",\n'
        '    "prefix": "#"\n'
        '  }\n'
        ']\n',
        encoding="utf-8",
    )
    adapter_template = (
        repo_root / "agent-core" / "adapters" / "codex" / "root-AGENTS.template.md"
    )
    adapter_template.parent.mkdir(parents=True, exist_ok=True)
    adapter_template.write_text("# template\n", encoding="utf-8")
    scripts_dir = repo_root / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    (repo_root / "tools" / "docs").mkdir(parents=True, exist_ok=True)
    (scripts_dir / "sync_architecture_docs.py").write_text(
        (REPO_ROOT / "scripts" / "sync_architecture_docs.py").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (scripts_dir / "validate_adoption_shape.py").write_text(
        (REPO_ROOT / "scripts" / "validate_adoption_shape.py").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (scripts_dir / "validate_repo_config.py").write_text(
        REPO_CONFIG_VALIDATOR_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (scripts_dir / "format_contract_yaml.py").write_text(
        FORMATTER_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (scripts_dir / "audit_architecture_linkage.py").write_text(
        AUDIT_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (scripts_dir / "validate_repo_contracts.py").write_text(
        VALIDATOR_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (repo_root / "tools" / "docs" / "generate_architecture_metadata.py").write_text(
        GENERATOR_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    tests_dir = repo_root / "tests"
    tests_dir.mkdir(parents=True, exist_ok=True)
    (tests_dir / "test_validate_adoption_shape.py").write_text(
        '"""\n'
        "@meta\n"
        "type: test\n"
        "scope: unit\n"
        "domain: docs\n"
        '"""\n\n'
        "def test_placeholder():\n    assert True\n",
        encoding="utf-8",
    )
    (tests_dir / "test_validate_repo_config.py").write_text(
        '"""\n'
        "@meta\n"
        "type: test\n"
        "scope: unit\n"
        "domain: config\n"
        '"""\n\n'
        "def test_placeholder():\n    assert True\n",
        encoding="utf-8",
    )
    (tests_dir / "test_validate_repo_contracts.py").write_text(
        '"""\n'
        "@meta\n"
        "type: test\n"
        "scope: unit\n"
        "domain: docs\n"
        '"""\n\n'
        "def test_placeholder():\n    assert True\n",
        encoding="utf-8",
    )
    for test_name in (
        "test_architecture_metadata_generation.py",
        "test_architecture_linkage_audit.py",
        "test_format_contract_yaml.py",
        "test_setup_hooks.py",
    ):
        (tests_dir / test_name).write_text(
            '"""\n@meta\ntype: test\nscope: unit\ndomain: docs\n"""\n\n'
            "def test_placeholder():\n    assert True\n",
            encoding="utf-8",
        )
    return repo_root


def test_validator_script_exists() -> None:
    assert VALIDATOR_PATH.exists()


def test_repo_contract_validator_fast_passes_for_current_managed_history_shape(tmp_path: Path) -> None:
    repo_root = build_repo(tmp_path)
    sync_process = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "sync_architecture_docs.py"), "--repo-root", str(repo_root)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert sync_process.returncode == 0
    validator_module = load_module(VALIDATOR_PATH, "validate_repo_contracts")

    assert validator_module.main(["--repo-root", str(repo_root), "--fast"]) == 0


def test_repo_contract_validator_fails_when_config_metadata_is_missing(tmp_path: Path) -> None:
    repo_root = build_repo(tmp_path)
    config_path = repo_root / "config" / "runtime" / "prompts.yaml"
    config_path.write_text("prompts: {}\n", encoding="utf-8")

    process = subprocess.run(
        [sys.executable, str(VALIDATOR_PATH), "--repo-root", str(repo_root), "--fast"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert process.returncode == 1
    assert "missing_required_metadata" in process.stdout
    assert "config/runtime/prompts.yaml" in process.stdout
