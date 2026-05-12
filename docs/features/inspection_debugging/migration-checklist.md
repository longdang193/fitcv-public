# Inspection Debugging Migration Checklist

This checklist brings `docs/features/inspection_debugging/` from the legacy managed-metadata shape to the current migration target used by newer adopted projects.

## Goal

Make this feature folder match the newer canonical pattern used by `customer-churn-prediction-azureml/docs/features/churn-data-preparation/`.

That target shape keeps human-owned inputs small, pushes derived views into generated files, and makes `lineage.generated.yaml` machine-verifiable.

## Current Legacy Signals In This Folder

The current `inspection_debugging` folder still shows the older format:

- `feature.source.yaml` mixes canonical inputs with downstream or duplicated fields such as `owner`, `primary_stage`, `stages`, `refs`, and `keywords`.
- capability entries still use legacy `name` and `summary` fields instead of the newer source shape.
- `inspection_debugging.yaml` still mirrors the older generated contract format.
- `lineage.generated.yaml` still uses the older summary-style schema with keys like `generated_contract`, `naming_policy`, `capability_shape`, `capability_ids`, `refs`, and `refs_by_type`.
- `history.md` is still a manual changelog-style document instead of the partial-generated history pattern.

## Migration Target

After migration, this folder should look and behave like the newer feature-folder contract:

- `feature.source.yaml` contains only human-owned feature inputs.
- generated files derive downstream views from that source instead of re-entering the same information.
- `lineage.generated.yaml` uses the explicit generated schema enforced by the starter validator.
- `history.md` separates generated status/timeline content from human notes.

## Target File Expectations

### `feature.source.yaml`

Keep this file limited to upstream human-authored feature inputs such as:

- `feature_id`
- `name`
- `status`
- `type`
- `summary`
- `invariants`
- `domains`
- `depends_on`
- `capabilities`
- `stage_participation`

Do not keep or re-add these legacy fields in the source file:

- `owner`
- `primary_stage`
- `stages`
- `refs`
- `keywords`

### Capability entries inside `feature.source.yaml`

Each capability entry should use the newer source contract:

```yaml
capabilities:
  - capability_id: inspection_debugging.some_capability
    statement: Short statement of what the capability does.
    state: implemented
```

Notes:

- `capability_id` should stay downstream-of-feature, for example `inspection_debugging.some_capability`.
- do not use bare capability IDs.
- do not keep legacy `name` or `summary` fields in managed examples.

### `inspection_debugging.yaml`

This file should be fully generated from source and reflect the current starter-generated contract rather than a source-copy with legacy fields.

### `lineage.generated.yaml`

This file should use the explicit generated schema used by the newer target folder.

Expected top-level structure:

```yaml
# GENERATED FILE - do not edit directly.
feature_id: inspection_debugging
source:
  feature: docs/features/inspection_debugging/feature.source.yaml
  generated_at: ...
  generator: ...
invariants:
  ...
capabilities:
  inspection_debugging.some_capability:
    source: docs/features/inspection_debugging/feature.source.yaml
    stage_participation:
      - ...
    depends_on:
      - ...
    invariants:
      - ...
timeline:
  status: ...
  updated_at: ...
```

The migrated file should not contain the old legacy summary keys:

- `generated_contract`
- `naming_policy`
- `capability_shape`
- `capability_ids`
- `refs`
- `refs_by_type`

Also, `capabilities` should be a mapping keyed by capability ID, not a list.

### `history.md`

Convert this from a fully manual changelog style into the partial-generated pattern:

- keep generated status/history sections in generated blocks
- keep a dedicated human-authored notes section
- avoid duplicating data that already lives in source metadata or generated lineage

## Recommended Migration Steps

1. Update the repo-local architecture generator so it can emit the current feature contract and lineage schema.
2. Shrink `feature.source.yaml` to the upstream human-owned fields only.
3. Replace legacy capability entries:
   - move `name`/`summary` content into `statement`
   - add `state`
   - ensure each `capability_id` is namespaced under `inspection_debugging.`
4. Replace `primary_stage` and `stages` with the newer `stage_participation` structure.
5. Move duplicated or downstream information out of the source file instead of re-entering it there.
6. Regenerate `inspection_debugging.yaml` using the updated generator.
7. Regenerate `lineage.generated.yaml` using the explicit canonical schema.
8. Convert `history.md` to the partial-generated pattern and preserve any truly human-only notes.
9. Run the repo validation and sync checks so the migrated folder is verified by automation.

## Suggested Mapping Notes For This Repo

This repo currently records a starter-sync divergence for lowercase underscore feature IDs. That repo-local choice can remain if intentional, but it should stay a naming-policy divergence only.

It should not be used as a reason to keep the legacy lineage schema or the older source-field shape.

## Done Criteria

This migration is complete when all of the following are true:

- `feature.source.yaml` contains only the human-owned upstream fields.
- all capability IDs are feature-prefixed and no bare IDs remain.
- `inspection_debugging.yaml` is regenerated in the current format.
- `lineage.generated.yaml` matches the canonical generated schema.
- `history.md` follows the partial-generated pattern.
- repo sync and validation checks pass without needing legacy exceptions for this folder.
