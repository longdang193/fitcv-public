---
doc_id: architecture
doc_type: architecture-guide
explains:
  features:
    - cv_system
    - inspection_debugging
    - settings_system
    - trigger_run_management
  components:
    - src/fitcv
    - src/fitcv_cp
---

# Architecture

FitCV architecture has four cross-cutting layers:

1. control plane (`src/fitcv_cp`)
2. pipeline runtime (`src/fitcv`)
3. backend/provider adapters and runtime routing
4. managed architecture metadata + generated contract outputs

## Runtime Surfaces

### Control Plane

Owns trigger/lifecycle APIs, admin UI, run-detail inspection, settings surfaces, orchestration binding, and run/event persistence adapters.

### Pipeline Runtime

Owns stage execution, ranking and CV lanes, validation, artifact emission, and stage-level truth.

## Portability and Routing

- backend portability: sqlite and bigquery execution paths are selected through control-plane backend runtime resolution
- provider portability: model routing is config/runtime controlled (with optional `FITCV_LANGGRAPH_*` env overrides) and resolved at runtime
- secrets: runtime credentials are supplied via environment variables

## Orchestration and Observability

- queue orchestration is supported by default with persisted run/orchestration bindings
- structured run events and stage artifacts back operator inspection flows
- operator-facing exports are primary inspection evidence surfaces

## Related Docs

- [setup.md](setup.md)
- [configuration.md](configuration.md)
- [pipeline.md](pipeline.md)
- [component_boundaries.md](component_boundaries.md)
