---
doc_id: pipeline
doc_type: architecture-guide
explains:
  stages:
    - cv_analysis
    - cv_generation
    - enrich
    - normalize
    - ranking
    - rule_filter
    - shortlist
---

# Pipeline

FitCV processes jobs through these ordered stages:

`normalize -> enrich -> rule_filter -> shortlist -> ranking -> cv_analysis -> cv_generation`

## Purpose

The pipeline narrows noisy raw job inputs into a smaller set of grounded, reviewable application outputs while preserving strong operator visibility.

## Ownership Model

- stage boundaries live in `docs/stages/*.source.yaml`
- generated stage contracts live in `docs/stages/*.yaml`
- cross-stage capabilities live in `docs/features/*/feature.source.yaml`

## Recommended References

- [FitCV-pipeline.md](FitCV-pipeline.md) for the operator-facing explainer and mental model
- [docs/generated/architecture_dag.yaml](generated/architecture_dag.yaml) for the generated stage, feature, and dependency topology
- [docs/generated/capability_lineage.yaml](generated/capability_lineage.yaml) for the generated capability-level evidence summary
