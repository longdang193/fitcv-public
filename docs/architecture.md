---
doc_id: architecture
doc_type: architecture-guide
explains:
  features:
    - cv_system
    - inspection_debugging
  stages:
    - cv_analysis
    - cv_generation
  components:
    - src/fitcv
    - src/fitcv_cp
---

# Architecture

FitCV combines an operator-facing control plane with a staged job-processing
pipeline and a generated architecture-doc layer.

## Runtime Architecture

- `src/fitcv_cp/` owns the FastAPI admin UI, run lifecycle actions, inspection surfaces, and settings UI.
- `src/fitcv/` owns the pipeline stages, CV analysis/generation behavior, and supporting runtime logic.
- Redis and RQ provide background execution for pipeline runs.
- BigQuery persists run state, events, structured jobs, and generated artifacts.

The main runtime boundary is between the operator-facing control-plane component and the background pipeline component. Their integration happens through Redis, RQ, persisted run state, and shared artifact storage. Information flow moves from trigger input through staged processing into stored results, while control flow moves through queue dispatch, worker execution, checkpoints, and run-lifecycle actions.

## Documentation Architecture

- `docs/features/<feature_id>/feature.source.yaml` is the human-owned feature source.
- `docs/features/<feature_id>/<feature_id>.yaml` is the generated feature contract.
- `docs/features/<feature_id>/lineage.generated.yaml` is generated feature evidence.
- `docs/stages/<stage_id>.source.yaml` is the human-owned stage source.
- `docs/stages/<stage_id>.yaml` is the generated stage contract.
- `docs/generated/architecture_dag.yaml` is the generated topology/discovery surface.
- `docs/generated/capability_lineage.yaml` is the generated capability evidence summary.

## Regeneration

Refresh generated architecture docs with:

```powershell
python scripts/sync_architecture_docs.py
```
