---
doc_id: usage
doc_type: operator-guide
explains:
  features:
    - trigger_run_management
  stages:
    - cv_generation
    - normalize
---

# Usage

FitCV supports two main public-facing usage modes: operator workflows and local
developer workflows.

## Operator Workflow

1. Open the admin UI at `http://localhost:8000/admin/runs`.
2. Trigger a run from file upload, pasted JSON, or path input.
3. Choose `Run All` or `Stage by Stage`.
4. Inspect run health, stage progress, and downloadable artifacts.
5. Review stage-owned diagnostics before acting on generated CV outputs.

## Local Developer Workflow

1. Update code, settings, or docs in your working checkout.
2. Run the web and worker services locally or with Docker.
3. Use the generated architecture docs when you need a quick view of stages,
   features, and capabilities.
4. Run the relevant tests before sharing or deploying changes.

## Related Surfaces

- [pipeline.md](pipeline.md)
- [FitCV-pipeline.md](FitCV-pipeline.md)
- [docs/generated/architecture_dag.yaml](generated/architecture_dag.yaml)
- [docs/generated/capability_lineage.yaml](generated/capability_lineage.yaml)
