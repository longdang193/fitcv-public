"""
@meta
type: test
scope: unit
domain: docs
covers:
  - Adoption mode validation for starter, managed, and legacy architecture-doc states
  - Guardrails against method-layer pseudo-features and mixed feature contract shapes
excludes:
  - Full downstream project migration
  - Architecture metadata generation
  - CI workflow wiring

tags:
  - fast
  - ci-safe
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
VALIDATOR = REPO_ROOT / "scripts" / "validate_adoption_shape.py"
POLICY = REPO_ROOT / "scripts" / "validator_policy.py"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path.name}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


SHARED_POLICY = load_module("validator_policy_for_adoption_shape_tests", POLICY)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def run_validator(repo_root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(VALIDATOR), "--repo-root", str(repo_root)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def write_adoption_mode(
    root: Path,
    mode: str,
    managed: bool,
    legacy: bool,
    generator: str = "none",
    starter_sync: str = "",
    extra: str = "",
) -> None:
    write_text(
        root / "repo_config" / "adoption-mode.yaml",
        f"""adoption_mode: {mode}
managed_architecture_metadata: {str(managed).lower()}
legacy_feature_contracts: {str(legacy).lower()}
architecture_generator: {generator}
{starter_sync}\
{extra}""",
    )


def managed_starter_sync_block(extra: str = "") -> str:
    return """starter_sync:
  starter_baseline_ref: starter@2026-04-21
  last_shared_surface_review_at: "2026-04-21"
  reviewed_surface_classes:
    - repo_config
    - operating_system_docs
    - skills
    - adapters
    - generated_instruction_surfaces
    - validation_and_sync_scripts
""" + extra


def required_root_doc_text(relative_path: str) -> str:
    docs = {
        "docs/setup.md": """---
doc_id: setup
doc_type: setup-guide
explains:
  features:
    - workspace-bootstrap
  stages:
    - data_prep
    - data_validate
---

# Setup

Dependencies and install prerequisites define the bootstrap path.
""",
        "docs/configuration.md": """---
doc_id: configuration
doc_type: operator-guide
explains:
  features:
    - sample-feature
  configs:
    - configs/runtime.yaml
---

# Configuration

Configuration covers environment variables, config files, defaults, and override ownership.
""",
        "docs/usage.md": """---
doc_id: usage
doc_type: operator-guide
explains:
  features:
    - sample-feature
  stages:
    - data_prep
    - serving
---

# Usage

Use the documented commands and entrypoints in the normal developer workflow.
""",
        "docs/pipeline.md": """---
doc_id: pipeline
doc_type: operator-guide
explains:
  features:
    - sample-feature
  stages:
    - data_prep
    - serving
---

# Pipeline

The workflow stages and handoff sequence describe the processing flow.
""",
        "docs/architecture.md": """---
doc_id: architecture
doc_type: architecture-guide
explains:
  features:
    - sample-feature
  stages:
    - data_prep
    - serving
---

# Architecture

Major components, boundaries, and information flow define the system integration shape.
""",
    }
    return docs[relative_path]


def seed_required_managed_mode_surface(root: Path, starter_sync: str | None = None) -> None:
    seed_required_folder_surface(root)
    write_adoption_mode(
        root,
        "managed_architecture_metadata",
        managed=True,
        legacy=False,
        generator="scripts/sync_architecture_docs.py",
        starter_sync=managed_starter_sync_block() if starter_sync is None else starter_sync,
    )
    for relative_path in (
        "docs/setup.md",
        "docs/configuration.md",
        "docs/usage.md",
        "docs/pipeline.md",
        "docs/architecture.md",
    ):
        write_text(root / relative_path, required_root_doc_text(relative_path))


def seed_managed_feature_folder(
    root: Path,
    feature_id: str = "sample-feature",
    *,
    include_source: bool = True,
    include_contract: bool = True,
    include_lineage: bool = True,
    include_history: bool = True,
) -> Path:
    folder = root / "docs" / "features" / feature_id
    if include_source:
        write_text(
            folder / "feature.source.yaml",
            f"""feature_id: {feature_id}
name: Sample Feature
status: active
type: workflow
summary: Sample summary.
invariants: []
domains: []
depends_on: []
capabilities: []
stage_participation: []
lineage_exceptions: []
""",
        )
    if include_contract:
        write_text(
            folder / f"{feature_id}.yaml",
            f"""# GENERATED FILE - do not edit directly.
# Source: docs/features/{feature_id}/feature.source.yaml
feature_id: {feature_id}
name: Sample Feature
status: active
type: workflow
summary: Sample summary.
invariants: []
domains: []
depends_on: []
capabilities: []
refs:
  code: []
  tests: []
  specs: []
  plans: []
  docs: []
  configs: []
  components: []
revision: 1
latest_change_id: 2026-04-22-sample-change
last_updated_at: "2026-04-22T10:30:00+02:00"
""",
        )
    if include_lineage:
        write_text(
            folder / "lineage.generated.yaml",
            f"""# GENERATED FILE - do not edit directly.
feature_id: {feature_id}
source: docs/features/{feature_id}/feature.source.yaml
invariants: {{}}
capabilities: {{}}
timeline: []
""",
        )
    if include_history:
        write_text(
            folder / "history.md",
            f"""# History

{SHARED_POLICY.GENERATED_HISTORY_START_MARKER}
{SHARED_POLICY.GENERATED_HISTORY_END_MARKER}

{SHARED_POLICY.HUMAN_NOTES_HEADING}
""",
        )
    return folder


def seed_generated_stage_contract(root: Path, stage_id: str = "sample_stage") -> Path:
    path = root / "docs" / "stages" / f"{stage_id}.yaml"
    write_text(
        path,
        f"""# GENERATED FILE - do not edit directly.
# Source: docs/stages/{stage_id}.source.yaml
stage_id: {stage_id}
name: Sample Stage
status: active
purpose: Run the sample stage.
feature_refs: []
capability_refs: []
code_refs: []
test_refs: []
doc_refs: []
config_refs: []
component_refs: []
""",
    )
    return path


def seed_stage_source(root: Path, stage_id: str = "sample_stage") -> Path:
    path = root / "docs" / "stages" / f"{stage_id}.source.yaml"
    write_text(
        path,
        f"""stage_id: {stage_id}
name: Sample Stage
status: active
purpose: Run the sample stage.
primary_features:
  - sample-feature
supporting_features: []
inputs:
  - validated records
outputs:
  - reporting-ready outputs
notes:
  - Stage ownership is declared here.
""",
    )
    return path


def seed_generated_discovery(root: Path) -> None:
    write_text(
        root / "docs" / "generated" / "capability_lineage.yaml",
        """# GENERATED FILE - do not edit directly.
features: {}
""",
    )
    write_text(
        root / "docs" / "generated" / "architecture_dag.yaml",
        """# GENERATED FILE - do not edit directly.
nodes: []
edges: []
""",
    )


def test_validator_passes_for_current_starter_repo() -> None:
    result = run_validator(REPO_ROOT)

    assert result.returncode == 0
    assert "adoption shape validation passed" in result.stdout.lower()


def test_validator_passes_for_minimal_managed_feature_folder(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path)
    seed_generated_discovery(tmp_path)

    result = run_validator(tmp_path)

    assert result.returncode == 0


def test_validator_rejects_mode_boolean_mismatch(tmp_path: Path) -> None:
    write_adoption_mode(tmp_path, "managed_architecture_metadata", managed=False, legacy=False)

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "booleans do not match" in result.stdout.lower()


def test_validator_rejects_method_layer_pseudo_feature(tmp_path: Path) -> None:
    write_adoption_mode(tmp_path, "legacy_compatibility", managed=False, legacy=True)
    write_text(
        tmp_path / "docs" / "features" / "repo-operating-system.yaml",
        """repo-operating-system:
  depends_on: []
  capabilities: []
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "method-layer pseudo-feature" in result.stdout.lower()
    assert "repo-operating-system" in result.stdout


def test_validator_rejects_flat_feature_yaml_in_managed_mode(tmp_path: Path) -> None:
    write_adoption_mode(
        tmp_path,
        "managed_architecture_metadata",
        managed=True,
        legacy=False,
        generator="scripts/sync_architecture_docs.py",
        starter_sync=managed_starter_sync_block(),
    )
    write_text(tmp_path / "docs" / "features" / "data-pipeline.yaml", "depends_on: []\n")

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "flat authoritative feature yaml" in result.stdout.lower()


def test_validator_rejects_missing_feature_source_in_managed_mode(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path, include_source=False)

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "managed feature folder is missing feature.source.yaml" in result.stdout.lower()


def test_validator_rejects_missing_generated_feature_contract_in_managed_mode(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path, include_contract=False)

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "managed feature folder is missing sample-feature.yaml" in result.stdout.lower()


def test_validator_rejects_missing_lineage_generated_in_managed_mode(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path, include_lineage=False)

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "managed feature folder is missing lineage.generated.yaml" in result.stdout.lower()


def test_validator_rejects_missing_history_in_managed_mode(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path, include_history=False)

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "managed feature folder is missing history.md" in result.stdout.lower()


def test_validator_rejects_generated_feature_contract_missing_required_keys(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path, include_contract=False)
    seed_generated_discovery(tmp_path)
    write_text(
        tmp_path / "docs" / "features" / "sample-feature" / "sample-feature.yaml",
        """# GENERATED FILE - do not edit directly.
# Source: docs/features/sample-feature/feature.source.yaml
feature_id: sample-feature
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "generated feature contract is missing required keys" in result.stdout.lower()


def test_validator_rejects_generated_feature_contract_legacy_capability_shape(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path, include_contract=False)
    seed_generated_discovery(tmp_path)
    write_text(
        tmp_path / "docs" / "features" / "sample-feature" / "sample-feature.yaml",
        """# GENERATED FILE - do not edit directly.
# Source: docs/features/sample-feature/feature.source.yaml
feature_id: sample-feature
name: Sample Feature
status: active
type: workflow
summary: Sample summary.
invariants: []
domains: []
depends_on: []
capabilities:
  - sample-feature.submit-job
refs:
  code: []
  tests: []
  specs: []
  plans: []
  docs: []
  configs: []
  components: []
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "generated feature contract capabilities[0] must be a mapping" in result.stdout.lower()


def test_validator_rejects_generated_feature_contract_missing_freshness_with_empty_timeline(
    tmp_path: Path,
) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path, include_contract=False)
    seed_generated_discovery(tmp_path)
    write_text(
        tmp_path / "docs" / "features" / "sample-feature" / "sample-feature.yaml",
        """# GENERATED FILE - do not edit directly.
# Source: docs/features/sample-feature/feature.source.yaml
feature_id: sample-feature
name: Sample Feature
status: active
type: workflow
summary: Sample summary.
invariants: []
domains: []
depends_on: []
capabilities: []
refs:
  code: []
  tests: []
  specs: []
  plans: []
  docs: []
  configs: []
  components: []
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "generated feature contract is missing required freshness metadata" in result.stdout.lower()


def test_validator_rejects_generated_feature_contract_partial_freshness_metadata(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path, include_contract=False)
    seed_generated_discovery(tmp_path)
    write_text(
        tmp_path / "docs" / "features" / "sample-feature" / "sample-feature.yaml",
        """# GENERATED FILE - do not edit directly.
# Source: docs/features/sample-feature/feature.source.yaml
feature_id: sample-feature
name: Sample Feature
status: active
type: workflow
summary: Sample summary.
invariants: []
domains: []
depends_on: []
capabilities: []
refs:
  code: []
  tests: []
  specs: []
  plans: []
  docs: []
  configs: []
  components: []
revision: 1
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "generated feature contract is missing required freshness metadata" in result.stdout.lower()


def test_validator_rejects_generated_feature_contract_invalid_freshness_metadata(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path, include_contract=False)
    seed_generated_discovery(tmp_path)
    write_text(
        tmp_path / "docs" / "features" / "sample-feature" / "sample-feature.yaml",
        """# GENERATED FILE - do not edit directly.
# Source: docs/features/sample-feature/feature.source.yaml
feature_id: sample-feature
name: Sample Feature
status: active
type: workflow
summary: Sample summary.
invariants: []
domains: []
depends_on: []
capabilities: []
refs:
  code: []
  tests: []
  specs: []
  plans: []
  docs: []
  configs: []
  components: []
revision: one
latest_change_id: ""
last_updated_at: ""
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "generated feature contract revision must be an integer" in result.stdout.lower()
    assert "generated feature contract latest_change_id must be a non-empty canonical concise string" in result.stdout.lower()
    assert "generated feature contract last_updated_at must be a non-empty canonical concise string" in result.stdout.lower()


def test_validator_accepts_generated_stage_contract_shape(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path)
    seed_generated_stage_contract(tmp_path)
    seed_generated_discovery(tmp_path)

    result = run_validator(tmp_path)

    assert result.returncode == 0


def test_validator_rejects_nested_legacy_stage_contract_shape(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path)
    seed_generated_discovery(tmp_path)
    write_text(
        tmp_path / "docs" / "stages" / "sample_stage.yaml",
        """# GENERATED FILE - do not edit directly.
sample_stage:
  name: Sample Stage
  summary: Legacy nested contract.
  refs:
    docs: []
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "generated stage contract uses a legacy nested stage_id wrapper" in result.stdout.lower()


def test_validator_rejects_generated_stage_contract_invalid_workflow_position_type(
    tmp_path: Path,
) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path)
    seed_generated_discovery(tmp_path)
    write_text(
        tmp_path / "docs" / "stages" / "sample_stage.yaml",
        """# GENERATED FILE - do not edit directly.
# Source: docs/stages/sample_stage.source.yaml
stage_id: sample_stage
name: Sample Stage
status: active
purpose: Run the sample stage.
workflow_position:
  - pre_training
feature_refs: []
capability_refs: []
code_refs: []
test_refs: []
doc_refs: []
config_refs: []
component_refs: []
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "generated stage contract workflow_position must be a non-empty canonical concise string" in result.stdout.lower()


def test_validator_rejects_history_without_generated_boundaries(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path)
    seed_generated_discovery(tmp_path)
    write_text(
        tmp_path / "docs" / "features" / "sample-feature" / "history.md",
        "# Sample Feature History\n\nOnly freeform notes.\n",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "history.md is missing the generated history start marker" in result.stdout.lower()
    assert "history.md is missing the generated history end marker" in result.stdout.lower()
    assert "history.md is missing the human notes heading" in result.stdout.lower()


def test_validator_rejects_generated_discovery_missing_required_keys(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path)
    write_text(
        tmp_path / "docs" / "generated" / "capability_lineage.yaml",
        """# GENERATED FILE - do not edit directly.
summary: {}
""",
    )
    write_text(
        tmp_path / "docs" / "generated" / "architecture_dag.yaml",
        """# GENERATED FILE - do not edit directly.
nodes: []
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "capability_lineage.yaml is missing required top-level keys" in result.stdout.lower()
    assert "architecture_dag.yaml is missing required top-level keys" in result.stdout.lower()


def test_validator_rejects_managed_contract_beside_flat_contract_in_legacy_mode(
    tmp_path: Path,
) -> None:
    write_adoption_mode(
        tmp_path,
        "legacy_compatibility",
        managed=False,
        legacy=True,
        extra="""migration_follow_up:
  required: true
  target: docs/superpowers/plans/migrate.md
""",
    )
    write_text(tmp_path / "docs" / "features" / "data-pipeline.yaml", "depends_on: []\n")
    write_text(
        tmp_path / "docs" / "features" / "data-pipeline" / "data-pipeline.yaml",
        "depends_on: []\n",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "beside a flat authoritative contract" in result.stdout.lower()


def test_validator_rejects_invalid_capability_ids(tmp_path: Path) -> None:
    write_adoption_mode(tmp_path, "legacy_compatibility", managed=False, legacy=True)
    write_text(
        tmp_path / "docs" / "features" / "data-pipeline.yaml",
        """depends_on: []
capability_ids:
  - "Staging: flatten GA4 events"
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "invalid capability id" in result.stdout.lower()


def test_validator_rejects_missing_required_root_docs(tmp_path: Path) -> None:
    write_adoption_mode(
        tmp_path,
        "managed_architecture_metadata",
        managed=True,
        legacy=False,
        generator="scripts/sync_architecture_docs.py",
        starter_sync=managed_starter_sync_block(),
    )
    write_text(
        tmp_path / "docs" / "features" / "data-pipeline" / "feature.source.yaml",
        """feature_id: data-pipeline
name: Data Pipeline
status: active
type: workflow
summary: Pipeline summary.
invariants: []
domains: []
depends_on: []
capabilities: []
stage_participation: []
lineage_exceptions: []
""",
    )
    write_text(tmp_path / "docs" / "features" / "data-pipeline" / "history.md", "# History\n")
    write_text(tmp_path / "docs" / "features" / "data-pipeline" / "lineage.generated.yaml", "features: {}\n")
    write_text(tmp_path / "docs" / "features" / "data-pipeline" / "data-pipeline.yaml", "feature_id: data-pipeline\n")

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "missing required root project doc" in result.stdout.lower()
    assert "docs/setup.md" in result.stdout


def test_validator_rejects_missing_required_root_doc_frontmatter_in_managed_mode(
    tmp_path: Path,
) -> None:
    seed_required_managed_mode_surface(tmp_path)
    write_text(
        tmp_path / "docs" / "pipeline.md",
        "# Pipeline\nThe workflow stages and handoff sequence describe the processing flow.\n",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "managed required root doc must include frontmatter metadata" in result.stdout.lower()
    assert "docs/pipeline.md" in result.stdout


def test_validator_rejects_pipeline_frontmatter_without_stage_links_in_managed_mode(
    tmp_path: Path,
) -> None:
    seed_required_managed_mode_surface(tmp_path)
    write_text(
        tmp_path / "docs" / "pipeline.md",
        """---
doc_id: pipeline
doc_type: operator-guide
explains:
  features:
    - sample-feature
---

# Pipeline

The workflow stages and handoff sequence describe the processing flow.
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "pipeline doc must explain one or more stages" in result.stdout.lower()
    assert "docs/pipeline.md" in result.stdout


def test_validator_rejects_malformed_optional_root_doc_frontmatter_in_managed_mode(
    tmp_path: Path,
) -> None:
    seed_required_managed_mode_surface(tmp_path)
    write_text(
        tmp_path / "docs" / "dataset.md",
        """---
doc_id: dataset
doc_type: dataset-guide
explains: not-a-mapping
---

# Dataset

Dataset sources, schemas, and provenance guidance.
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "managed optional root doc must declare explains metadata" in result.stdout.lower()
    assert "docs/dataset.md" in result.stdout


def test_validator_rejects_optional_root_doc_without_required_links_in_managed_mode(
    tmp_path: Path,
) -> None:
    seed_required_managed_mode_surface(tmp_path)
    write_text(
        tmp_path / "docs" / "api.md",
        """---
doc_id: api
doc_type: api-guide
explains:
  configs:
    - configs/runtime.yaml
---

# API

API surfaces, service contracts, and endpoint guidance.
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "api doc must explain one or more features, capabilities, or components" in result.stdout.lower()
    assert "docs/api.md" in result.stdout


def test_validator_rejects_misplaced_optional_root_doc_frontmatter_in_managed_mode(
    tmp_path: Path,
) -> None:
    seed_required_managed_mode_surface(tmp_path)
    write_text(
        tmp_path / "docs" / "testing.md",
        """
---
doc_id: testing
doc_type: testing-guide
explains:
  features:
    - sample-feature
---

# Testing

Testing strategy, test layers, and release-gating verification guidance.
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "frontmatter must start at the first byte" in result.stdout.lower()
    assert "docs/testing.md" in result.stdout


def test_validator_rejects_missing_required_project_folder(tmp_path: Path) -> None:
    write_adoption_mode(
        tmp_path,
        "starter_method_only",
        managed=False,
        legacy=False,
    )
    for relative_path in (
        "docs/setup.md",
        "docs/configuration.md",
        "docs/usage.md",
        "docs/pipeline.md",
        "docs/architecture.md",
        "docs/intent/README.md",
    ):
        write_text(tmp_path / relative_path, "# placeholder\n")
    (tmp_path / "docs" / "operating_system").mkdir(parents=True, exist_ok=True)
    (tmp_path / "docs" / "superpowers" / "specs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "repo_config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "scripts").mkdir(parents=True, exist_ok=True)
    # Intentionally omit tests/ to exercise the required-folder rule.

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "missing required project folder" in result.stdout.lower()
    assert "tests/" in result.stdout


def test_validator_rejects_intent_folder_without_markdown_files(tmp_path: Path) -> None:
    write_adoption_mode(
        tmp_path,
        "starter_method_only",
        managed=False,
        legacy=False,
    )
    for relative_path in (
        "docs/setup.md",
        "docs/configuration.md",
        "docs/usage.md",
        "docs/pipeline.md",
        "docs/architecture.md",
    ):
        write_text(tmp_path / relative_path, "# placeholder\n")
    (tmp_path / "docs" / "intent").mkdir(parents=True, exist_ok=True)
    write_text(tmp_path / "docs" / "intent" / "notes.txt", "placeholder\n")
    (tmp_path / "docs" / "operating_system").mkdir(parents=True, exist_ok=True)
    (tmp_path / "docs" / "superpowers" / "specs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "docs" / "superpowers" / "plans").mkdir(parents=True, exist_ok=True)
    (tmp_path / "repo_config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "scripts").mkdir(parents=True, exist_ok=True)
    (tmp_path / "tests").mkdir(parents=True, exist_ok=True)

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "intent layer must contain at least one markdown file" in result.stdout.lower()
    assert "docs/intent/" in result.stdout


def seed_required_folder_surface(root: Path) -> None:
    for relative_path in (
        "docs/intent/README.md",
        "docs/operating_system/repo-governance.md",
        "docs/operating_system/doc-system-lifecycle.md",
        "docs/superpowers/specs/README.md",
        "docs/superpowers/plans/README.md",
        "repo_config/adoption-mode.yaml",
        "scripts/README.md",
        "tests/README.md",
    ):
        if relative_path == "repo_config/adoption-mode.yaml":
            write_adoption_mode(
                root,
                "starter_method_only",
                managed=False,
                legacy=False,
            )
        else:
            write_text(root / relative_path, "# placeholder\n")


MODE_A_TEMPLATE_FILES = (
    "README.md",
    "docs/setup.md",
    "docs/configuration.md",
    "docs/usage.md",
    "docs/pipeline.md",
    "docs/architecture.md",
    "docs/intent/README.md",
    "docs/intent/project-charter.md",
    "docs/intent/constraints-and-non-goals.md",
    "docs/intent/stakeholders.md",
    "docs/intent/success-outcomes.md",
    "repo_config/adoption-mode.yaml",
    "repo_config/publication-config.json",
    "repo_config/agent-adapter-mappings.json",
    "configs/starter-runtime.yaml",
    "scripts/README.md",
    "tests/README.md",
)


def seed_mode_a_template_pack(root: Path) -> None:
    write_text(
        root / "docs" / "superpowers" / "specs" / "2026-04-23-mode-a-project-template-pack-spec.md",
        "# Mode A Project Template Pack Spec\n",
    )
    template_root = root / "docs" / "project_templates" / "mode-a"
    for relative_path in MODE_A_TEMPLATE_FILES:
        if relative_path == "repo_config/adoption-mode.yaml":
            write_text(
                template_root / relative_path,
                """adoption_mode: starter_method_only
managed_architecture_metadata: false
legacy_feature_contracts: false
architecture_generator: none
notes: >
  Starter-method-only project template.
""",
            )
        elif relative_path == "repo_config/publication-config.json":
            write_text(template_root / relative_path, '{"publicPaths":["README.md"]}\n')
        elif relative_path == "repo_config/agent-adapter-mappings.json":
            write_text(template_root / relative_path, "[]\n")
        elif relative_path == "configs/starter-runtime.yaml":
            write_text(template_root / relative_path, "runtime:\n  environment: dev\n")
        else:
            write_text(template_root / relative_path, "# Template\n\nFill this project-specific template.\n")


def test_validator_rejects_missing_mode_a_template_pack_file(tmp_path: Path) -> None:
    seed_required_folder_surface(tmp_path)
    seed_mode_a_template_pack(tmp_path)
    (tmp_path / "docs" / "project_templates" / "mode-a" / "docs" / "usage.md").unlink()

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "mode a project template pack is missing a required file" in result.stdout.lower()
    assert "docs/project_templates/mode-a/docs/usage.md" in result.stdout


def test_validator_rejects_wrong_mode_a_template_adoption_mode(tmp_path: Path) -> None:
    seed_required_folder_surface(tmp_path)
    seed_mode_a_template_pack(tmp_path)
    write_text(
        tmp_path / "docs" / "project_templates" / "mode-a" / "repo_config" / "adoption-mode.yaml",
        """adoption_mode: managed_architecture_metadata
managed_architecture_metadata: true
legacy_feature_contracts: false
architecture_generator: scripts/sync_architecture_docs.py
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "mode a adoption-mode template must set adoption_mode: starter_method_only" in result.stdout.lower()
    assert "mode a adoption-mode template must set managed_architecture_metadata: false" in result.stdout.lower()
    assert "mode a adoption-mode template must set architecture_generator: none" in result.stdout.lower()


def test_validator_rejects_managed_markers_in_mode_a_template_pack(tmp_path: Path) -> None:
    seed_required_folder_surface(tmp_path)
    seed_mode_a_template_pack(tmp_path)
    write_text(
        tmp_path / "docs" / "project_templates" / "mode-a" / "docs" / "pipeline.md",
        "# Pipeline\n\nDo not put @capability metadata here.\n",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "mode a project template contains managed architecture metadata" in result.stdout.lower()
    assert "docs/project_templates/mode-a/docs/pipeline.md" in result.stdout


def test_validator_rejects_feature_source_summary_with_blank_line_padding(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path)
    seed_generated_discovery(tmp_path)
    write_text(
        tmp_path / "docs" / "features" / "sample-feature" / "feature.source.yaml",
        """feature_id: sample-feature
name: Sample Feature
status: active
type: workflow
summary: >
  Sample summary.

invariants: []
domains: []
depends_on: []
capabilities: []
stage_participation: []
lineage_exceptions: []
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "feature.source.yaml summary must be a canonical concise string" in result.stdout.lower()


def test_validator_rejects_feature_source_duplicate_list_items(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path)
    seed_generated_discovery(tmp_path)
    write_text(
        tmp_path / "docs" / "features" / "sample-feature" / "feature.source.yaml",
        """feature_id: sample-feature
name: Sample Feature
status: active
type: workflow
summary: Sample summary.
invariants: []
domains:
  - billing
  - billing
depends_on:
  - upstream-source
  - upstream-source
capabilities: []
stage_participation:
  - stage_id: analytics
    role: primary
    capability_ids:
      - sample-feature.submit-job
      - sample-feature.submit-job
      - ""
lineage_exceptions: []
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "feature.source.yaml domains contains duplicate value" in result.stdout.lower()
    assert "feature.source.yaml depends_on contains duplicate value" in result.stdout.lower()
    assert "feature.source.yaml stage_participation[0].capability_ids contains duplicate value" in result.stdout.lower()
    assert "feature.source.yaml stage_participation[0].capability_ids[2] must be a non-empty canonical string item" in result.stdout.lower()


def test_validator_rejects_generated_feature_contract_noncanonical_summary(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path)
    seed_generated_discovery(tmp_path)
    write_text(
        tmp_path / "docs" / "features" / "sample-feature" / "sample-feature.yaml",
        """# GENERATED FILE - do not edit directly.
# Source: docs/features/sample-feature/feature.source.yaml
feature_id: sample-feature
name: Sample Feature
status: active
type: workflow
summary: >
  Sample summary.

invariants: []
domains: []
depends_on: []
capabilities: []
refs:
  code: []
  tests: []
  specs: []
  plans: []
  docs: []
  configs: []
  components: []
revision: 1
latest_change_id: 2026-04-22-sample-change
last_updated_at: "2026-04-22T10:30:00+02:00"
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "generated feature contract summary must be a canonical concise string" in result.stdout.lower()


def test_validator_rejects_stage_source_and_contract_canonical_style_drift(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path)
    seed_generated_discovery(tmp_path)
    write_text(
        tmp_path / "docs" / "stages" / "sample_stage.source.yaml",
        """stage_id: sample_stage
name: Sample Stage
status: active
purpose: >
  Run the sample stage.

primary_features:
  - sample-feature
  - sample-feature
supporting_features:
  - ""
inputs:
  - validated records
outputs:
  - reporting-ready outputs
notes:
  - Stage ownership is declared here.
""",
    )
    write_text(
        tmp_path / "docs" / "stages" / "sample_stage.yaml",
        """# GENERATED FILE - do not edit directly.
# Source: docs/stages/sample_stage.source.yaml
stage_id: sample_stage
name: Sample Stage
status: active
purpose: >
  Run the sample stage.

feature_refs:
  - sample-feature
  - sample-feature
capability_refs: []
code_refs: []
test_refs: []
doc_refs: []
config_refs: []
component_refs: []
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "stage.source.yaml purpose must be a canonical concise string" in result.stdout.lower()
    assert "stage.source.yaml primary_features contains duplicate value" in result.stdout.lower()
    assert "generated stage contract purpose must be a canonical concise string" in result.stdout.lower()
    assert "generated stage contract feature_refs contains duplicate value" in result.stdout.lower()


def test_validator_rejects_managed_root_doc_noncanonical_frontmatter(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path)
    seed_stage_source(tmp_path, "data_prep")
    seed_generated_stage_contract(tmp_path, "data_prep")
    seed_generated_discovery(tmp_path)
    write_text(
        tmp_path / "docs" / "pipeline.md",
        """---
doc_id: pipeline
doc_type: "operator-guide "
explains:
  stages:
    - data_prep
    - data_prep
    - ""
---

# Pipeline

The workflow stages and handoff sequence describe the processing flow.
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "managed required root doc doc_type must be a canonical concise string" in result.stdout.lower()
    assert "managed required root doc explains.stages contains duplicate value" in result.stdout.lower()
    assert "managed required root doc explains.stages[2] must be a non-empty canonical string item" in result.stdout.lower()


def test_validator_rejects_noncanonical_repo_relative_paths(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path)
    seed_generated_discovery(tmp_path)
    write_text(
        tmp_path / "docs" / "features" / "sample-feature" / "lineage.generated.yaml",
        """# GENERATED FILE - do not edit directly.
feature_id: sample-feature
source: docs\\features\\sample-feature\\feature.source.yaml
invariants: {}
capabilities: {}
timeline:
  - completed_at: "2026-04-22T10:30:00+02:00"
    source_plan: " docs/superpowers/plans/2026-04-22-sample-plan.md "
    change_id: 2026-04-22-sample-change
    summary: Add sample capability metadata.
    capabilities:
      - sample-feature.submit-job
    verification:
      - python -m pytest
    outcome: Passed
""",
    )
    write_text(
        tmp_path / "docs" / "superpowers" / "plans" / "2026-04-22-sample-plan.md",
        "# Sample Plan\n",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "lineage.generated.yaml source must be a canonical repo-relative path" in result.stdout.lower()
    assert "timeline[0].source_plan must be a canonical repo-relative path" in result.stdout.lower()


def test_validator_rejects_unsorted_feature_source_unordered_lists(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path)
    seed_managed_feature_folder(tmp_path, feature_id="alpha-upstream")
    seed_managed_feature_folder(tmp_path, feature_id="zeta-upstream")
    seed_generated_discovery(tmp_path)
    write_text(
        tmp_path / "docs" / "features" / "sample-feature" / "feature.source.yaml",
        """feature_id: sample-feature
name: Sample Feature
status: active
type: workflow
summary: Sample summary.
invariants: []
domains:
  - serving
  - analytics
depends_on:
  - zeta-upstream
  - alpha-upstream
capabilities: []
stage_participation:
  - stage_id: analytics
    role: primary
    capability_ids:
      - sample-feature.zeta-capability
      - sample-feature.alpha-capability
lineage_exceptions:
  - refs-gap
  - docs-gap
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "feature.source.yaml domains must use canonical lexical order" in result.stdout.lower()
    assert "feature.source.yaml depends_on must use canonical lexical order" in result.stdout.lower()
    assert "feature.source.yaml stage_participation[0].capability_ids must use canonical lexical order" in result.stdout.lower()
    assert "feature.source.yaml lineage_exceptions must use canonical lexical order" in result.stdout.lower()


def test_validator_rejects_unsorted_stage_source_lists(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path)
    seed_generated_discovery(tmp_path)
    write_text(
        tmp_path / "docs" / "stages" / "sample_stage.source.yaml",
        """stage_id: sample_stage
name: Sample Stage
status: active
purpose: Run the sample stage.
primary_features:
  - zeta-feature
  - alpha-feature
supporting_features:
  - support-z
  - support-a
inputs:
  - zeta records
  - alpha records
outputs:
  - zeta outputs
  - alpha outputs
notes:
  - Keep note order human-owned.
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "stage.source.yaml primary_features must use canonical lexical order" in result.stdout.lower()
    assert "stage.source.yaml supporting_features must use canonical lexical order" in result.stdout.lower()
    assert "stage.source.yaml inputs must use canonical lexical order" in result.stdout.lower()
    assert "stage.source.yaml outputs must use canonical lexical order" in result.stdout.lower()


def test_validator_rejects_unsorted_managed_root_doc_explains_lists(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path)
    seed_generated_discovery(tmp_path)
    write_text(
        tmp_path / "docs" / "architecture.md",
        """---
doc_id: architecture
doc_type: architecture-guide
explains:
  features:
    - sample-zeta
    - sample-alpha
  stages:
    - serving
    - data_prep
  components:
    - src/zeta_component.py
    - src/alpha_component.py
---

# Architecture

Major components, boundaries, and information flow define the system integration shape.
""",
    )
    write_text(tmp_path / "src" / "alpha_component.py", "print('alpha')\n")
    write_text(tmp_path / "src" / "zeta_component.py", "print('zeta')\n")

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "managed required root doc explains.features must use canonical lexical order" in result.stdout.lower()
    assert "managed required root doc explains.stages must use canonical lexical order" in result.stdout.lower()
    assert "managed required root doc explains.components must use canonical lexical order" in result.stdout.lower()


def test_validator_accepts_sorted_phase_2a_managed_lists(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path)
    seed_managed_feature_folder(tmp_path, feature_id="alpha-upstream")
    seed_managed_feature_folder(tmp_path, feature_id="zeta-upstream")
    seed_stage_source(tmp_path, "sample_stage")
    seed_generated_stage_contract(tmp_path, "sample_stage")
    seed_generated_discovery(tmp_path)
    write_text(tmp_path / "src" / "alpha_component.py", "print('alpha')\n")
    write_text(tmp_path / "src" / "zeta_component.py", "print('zeta')\n")
    write_text(
        tmp_path / "docs" / "features" / "sample-feature" / "feature.source.yaml",
        """feature_id: sample-feature
name: Sample Feature
status: active
type: workflow
summary: Sample summary.
invariants: []
domains:
  - analytics
  - serving
depends_on:
  - alpha-upstream
  - zeta-upstream
capabilities: []
stage_participation:
  - stage_id: analytics
    role: primary
    capability_ids:
      - sample-feature.alpha-capability
      - sample-feature.zeta-capability
lineage_exceptions:
  - docs-gap
  - refs-gap
""",
    )
    write_text(
        tmp_path / "docs" / "stages" / "sample_stage.source.yaml",
        """stage_id: sample_stage
name: Sample Stage
status: active
purpose: Run the sample stage.
primary_features:
  - alpha-feature
  - zeta-feature
supporting_features:
  - support-a
  - support-z
inputs:
  - alpha records
  - zeta records
outputs:
  - alpha outputs
  - zeta outputs
notes:
  - Keep note order human-owned.
""",
    )
    write_text(
        tmp_path / "docs" / "architecture.md",
        """---
doc_id: architecture
doc_type: architecture-guide
explains:
  components:
    - src/alpha_component.py
    - src/zeta_component.py
  features:
    - sample-alpha
    - sample-zeta
  stages:
    - data_prep
    - serving
---

# Architecture

Major components, boundaries, and information flow define the system integration shape.
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 0, result.stdout


def test_validator_accepts_complete_mode_a_template_pack(tmp_path: Path) -> None:
    seed_required_folder_surface(tmp_path)
    for relative_path in (
        "docs/setup.md",
        "docs/configuration.md",
        "docs/usage.md",
        "docs/pipeline.md",
        "docs/architecture.md",
    ):
        write_text(tmp_path / relative_path, required_root_doc_text(relative_path))
    seed_mode_a_template_pack(tmp_path)

    result = run_validator(tmp_path)

    assert result.returncode == 0, result.stdout


def test_validator_rejects_heading_only_required_doc(tmp_path: Path) -> None:
    seed_required_folder_surface(tmp_path)
    write_text(tmp_path / "docs" / "setup.md", "# Setup\n")
    write_text(
        tmp_path / "docs" / "configuration.md",
        "# Configuration\nConfiguration covers environment variables and profiles.\n",
    )
    write_text(
        tmp_path / "docs" / "usage.md",
        "# Usage\nUse the main command entrypoints in the normal run flow.\n",
    )
    write_text(
        tmp_path / "docs" / "pipeline.md",
        "# Pipeline\nThe workflow stages describe the end-to-end processing flow.\n",
    )
    write_text(
        tmp_path / "docs" / "architecture.md",
        "# Architecture\nMajor components and information flow define the system boundaries.\n",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "required doc must contain more than a heading" in result.stdout.lower()
    assert "docs/setup.md" in result.stdout


def test_validator_rejects_placeholder_only_required_doc(tmp_path: Path) -> None:
    seed_required_folder_surface(tmp_path)
    write_text(
        tmp_path / "docs" / "setup.md",
        "# Setup\nTODO placeholder. Fill this in later.\n",
    )
    write_text(
        tmp_path / "docs" / "configuration.md",
        "# Configuration\nConfiguration covers environment variables and profiles.\n",
    )
    write_text(
        tmp_path / "docs" / "usage.md",
        "# Usage\nUse the main command entrypoints in the normal run flow.\n",
    )
    write_text(
        tmp_path / "docs" / "pipeline.md",
        "# Pipeline\nThe workflow stages describe the end-to-end processing flow.\n",
    )
    write_text(
        tmp_path / "docs" / "architecture.md",
        "# Architecture\nMajor components and information flow define the system boundaries.\n",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "required doc is still placeholder-only" in result.stdout.lower()
    assert "docs/setup.md" in result.stdout


def test_validator_rejects_required_doc_without_semantic_coverage(tmp_path: Path) -> None:
    seed_required_folder_surface(tmp_path)
    write_text(
        tmp_path / "docs" / "setup.md",
        "# Setup\nDependencies and provisioning steps define the bootstrap order.\n",
    )
    write_text(
        tmp_path / "docs" / "configuration.md",
        "# Configuration\nConfiguration covers environment variables and profiles.\n",
    )
    write_text(
        tmp_path / "docs" / "usage.md",
        "# Usage\nThis document talks in general terms about collaboration habits and project background without explaining how to operate the system after setup.\n",
    )
    write_text(
        tmp_path / "docs" / "pipeline.md",
        "# Pipeline\nThe workflow stages describe the end-to-end processing flow.\n",
    )
    write_text(
        tmp_path / "docs" / "architecture.md",
        "# Architecture\nMajor components and information flow define the system boundaries.\n",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "required doc is missing expected semantic coverage" in result.stdout.lower()
    assert "docs/usage.md" in result.stdout


def test_validator_rejects_bare_capability_ids_in_managed_metadata_template(
    tmp_path: Path,
) -> None:
    write_adoption_mode(
        tmp_path,
        "starter_method_only",
        managed=False,
        legacy=False,
    )
    for relative_path in (
        "docs/setup.md",
        "docs/configuration.md",
        "docs/usage.md",
        "docs/pipeline.md",
        "docs/architecture.md",
    ):
        write_text(tmp_path / relative_path, "# placeholder\n")
    write_text(
        tmp_path / "docs" / "architecture_templates" / "feature.source.yaml",
        """feature_id: billing-insights
name: Billing Insights
status: active
type: workflow
summary: Summarize billing activity for operator reporting.
domains:
  - billing
depends_on: []
invariants:
  - invariant_id: billing-inputs-validated
    name: Billing Inputs Validated
    statement: Billing reports use validated billing records only.
    state: active
capabilities:
  - capability_id: billing-insights.billing-revenue-summary
    name: Billing Revenue Summary
    summary: Summarize billed revenue by account and reporting period.
    state: active
stage_participation:
  - stage_id: analytics
    role: primary
    capability_ids:
      - billing-revenue-summary
lineage_exceptions: []
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "template capability_ids must use feature-qualified ids" in result.stdout.lower()
    assert "docs/architecture_templates/feature.source.yaml" in result.stdout


def test_validator_rejects_bare_capability_ids_in_yaml_architecture_template(
    tmp_path: Path,
) -> None:
    write_adoption_mode(
        tmp_path,
        "starter_method_only",
        managed=False,
        legacy=False,
    )
    for relative_path in (
        "docs/setup.md",
        "docs/configuration.md",
        "docs/usage.md",
        "docs/pipeline.md",
        "docs/architecture.md",
    ):
        write_text(tmp_path / relative_path, "# placeholder\n")
    write_text(
        tmp_path / "docs" / "architecture_templates" / "yaml-architecture.yaml",
        """# @architecture
# owner: billing-insights
# features:
#   - billing-insights
# stages:
#   - analytics
# capabilities:
#   - billing-revenue-summary
# role: config
# canonical: true
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "yaml architecture template capabilities must use feature-qualified ids" in result.stdout.lower()
    assert "docs/architecture_templates/yaml-architecture.yaml" in result.stdout


def test_validator_rejects_bare_capability_ids_in_markdown_frontmatter_template(
    tmp_path: Path,
) -> None:
    write_adoption_mode(
        tmp_path,
        "starter_method_only",
        managed=False,
        legacy=False,
    )
    for relative_path in (
        "docs/setup.md",
        "docs/configuration.md",
        "docs/usage.md",
        "docs/pipeline.md",
        "docs/architecture.md",
    ):
        write_text(tmp_path / relative_path, "# placeholder\n")
    write_text(
        tmp_path / "docs" / "architecture_templates" / "markdown-frontmatter.md",
        """# Markdown Frontmatter Template

```md
---
doc_id: billing-insights-operator-guide
doc_type: guide
explains:
  features:
    - billing-insights
  capabilities:
    - billing-revenue-summary
  stages:
    - analytics
---
```
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "frontmatter template capabilities must use feature-qualified ids" in result.stdout.lower()
    assert "docs/architecture_templates/markdown-frontmatter.md" in result.stdout


def test_validator_rejects_missing_starter_sync_record_in_managed_mode(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path, starter_sync="")

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "requires a starter_sync record" in result.stdout.lower()


def test_validator_rejects_incomplete_starter_sync_record_in_managed_mode(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(
        tmp_path,
        starter_sync="""starter_sync:
  starter_baseline_ref: ""
  last_shared_surface_review_at: not-a-date
  reviewed_surface_classes:
    - repo_config
  divergences:
    - path: ""
      class: not_a_real_class
      status: custom
      rationale: ""
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "starter_sync.starter_baseline_ref must be a non-empty string" in result.stdout
    assert "starter_sync.last_shared_surface_review_at must be an iso-8601 date or timestamp" in result.stdout.lower()
    assert "starter_sync.reviewed_surface_classes is missing required mode b surface classes" in result.stdout.lower()
    assert "starter_sync.divergences[0].class must be one of the reviewed surface classes" in result.stdout.lower()


def test_validator_rejects_legacy_lineage_generated_shape_in_managed_mode(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path, include_lineage=False)
    write_text(
        tmp_path / "docs" / "features" / "sample-feature" / "lineage.generated.yaml",
        """feature_id: sample-feature
source: docs/features/sample-feature/feature.source.yaml
generated_contract: docs/features/sample-feature/sample-feature.yaml
naming_policy:
  feature_id_format: kebab
capability_shape: structured
capability_ids:
  - sample-feature.submit-job
capabilities:
  - capability_id: sample-feature.submit-job
refs_by_type:
  spec: []
timeline: []
invariants: {}
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "lineage.generated.yaml must include the generated-file header" in result.stdout.lower()
    assert "uses legacy summary-style top-level keys" in result.stdout.lower()
    assert "capabilities must be a mapping keyed by capability id" in result.stdout.lower()


def test_validator_rejects_lineage_generated_missing_required_top_level_keys(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path, include_lineage=False)
    write_text(
        tmp_path / "docs" / "features" / "sample-feature" / "lineage.generated.yaml",
        """# GENERATED FILE - do not edit directly.
feature_id: sample-feature
source: docs/features/sample-feature/feature.source.yaml
capabilities: {}
timeline: []
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "lineage.generated.yaml is missing required top-level keys" in result.stdout.lower()


def test_validator_accepts_rich_lineage_generated_shape(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path, include_lineage=False)
    write_text(
        tmp_path / "docs" / "features" / "sample-feature" / "sample-feature.yaml",
        """# GENERATED FILE - do not edit directly.
# Source: docs/features/sample-feature/feature.source.yaml
feature_id: sample-feature
name: Sample Feature
status: active
type: workflow
summary: Sample summary.
invariants: []
domains: []
depends_on: []
capabilities: []
refs:
  code: []
  tests: []
  specs: []
  plans: []
  docs: []
  configs: []
  components: []
revision: 1
latest_change_id: 2026-04-22-sample-change
last_updated_at: "2026-04-22T10:30:00+02:00"
""",
    )
    write_text(tmp_path / "docs" / "sample.md", "# Sample Doc\nMeaningful doc body.\n")
    write_text(
        tmp_path / "docs" / "superpowers" / "plans" / "2026-04-22-sample-plan.md",
        "# Sample Plan\nCompleted plan body.\n",
    )
    write_text(
        tmp_path / "docs" / "features" / "sample-feature" / "lineage.generated.yaml",
        """# GENERATED FILE - do not edit directly.
feature_id: sample-feature
source: docs/features/sample-feature/feature.source.yaml
invariants: {}
capabilities:
  sample-feature.submit-job:
    state: active
    statement: Submit the sample job.
    satisfies: []
    code:
      - path: docs/sample.md
        confidence: high
        source:
          - python_meta
    tests:
      - path: docs/sample.md
        confidence: high
        source:
          - python_proves
        symbols:
          - test_submit_job
    docs:
      - docs/sample.md
    docs_evidence:
      - path: docs/sample.md
        confidence: high
        source:
          - docs_frontmatter
    configs: []
    config_evidence: []
    components: []
    component_evidence: []
    specs: []
    plans:
      - docs/superpowers/plans/2026-04-22-sample-plan.md
    evidence_gaps: []
    allowed_evidence_gaps: []
    lineage_exception_reason: null
    unresolved_evidence_gaps: []
    completeness_status: complete
timeline:
  - completed_at: "2026-04-22T10:30:00+02:00"
    source_plan: docs/superpowers/plans/2026-04-22-sample-plan.md
    change_id: 2026-04-22-sample-change
    summary: Add sample capability metadata.
    capabilities:
      - sample-feature.submit-job
    verification:
      - pytest tests/test_sample.py
    outcome: Sample capability now has explicit lineage metadata.
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 0


def test_validator_rejects_string_list_code_and_tests_in_lineage_generated(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path, include_lineage=False)
    write_text(
        tmp_path / "docs" / "features" / "sample-feature" / "lineage.generated.yaml",
        """# GENERATED FILE - do not edit directly.
feature_id: sample-feature
source: docs/features/sample-feature/feature.source.yaml
invariants: {}
capabilities:
  sample-feature.submit-job:
    state: active
    statement: Submit the sample job.
    satisfies: []
    code:
      - docs/setup.md
    tests:
      - docs/setup.md
    docs: []
    docs_evidence: []
    configs: []
    config_evidence: []
    components: []
    component_evidence: []
    specs: []
    plans: []
    evidence_gaps: []
    allowed_evidence_gaps: []
    lineage_exception_reason: null
    unresolved_evidence_gaps: []
    completeness_status: complete
timeline: []
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "code[0] must be a mapping" in result.stdout.lower()
    assert "tests[0] must be a mapping" in result.stdout.lower()


def test_validator_rejects_legacy_kind_path_timeline_entries(tmp_path: Path) -> None:
    seed_required_managed_mode_surface(tmp_path)
    seed_managed_feature_folder(tmp_path, include_lineage=False)
    write_text(
        tmp_path / "docs" / "features" / "sample-feature" / "lineage.generated.yaml",
        """# GENERATED FILE - do not edit directly.
feature_id: sample-feature
source: docs/features/sample-feature/feature.source.yaml
invariants: {}
capabilities: {}
timeline:
  - kind: plan
    path: docs/superpowers/plans/2026-04-22-sample-plan.md
""",
    )

    result = run_validator(tmp_path)

    assert result.returncode == 1
    assert "timeline[0] is missing required keys" in result.stdout.lower()

