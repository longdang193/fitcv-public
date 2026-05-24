"""@meta
name: pipeline_stages
type: package
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Stage-module package for progressive FitCV pipeline refactor (Task 6 / A5).
inputs:
  - Called by src.fitcv.pipeline when stage implementations are migrated.
outputs:
  - Stage helpers/types with stable boundaries.
lifecycle:
  - status: active
"""
