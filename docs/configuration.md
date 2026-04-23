---
doc_id: configuration
doc_type: operator-guide
explains:
  features:
    - settings_system
  configs:
    - config/runtime/pipeline.yaml
---

# Configuration

FitCV splits configuration ownership by runtime purpose.

## Runtime Product Configuration

`config/` owns product and pipeline runtime defaults such as:

- environment-specific YAML
- runtime policies
- taxonomy/config-room data used by the pipeline

Operators can override supported runtime settings through the admin UI, but the runtime configuration model still resolves from the config layer plus persisted settings snapshots.

## Generated Architecture Configuration

Managed architecture metadata uses:

- `docs/features/<feature_id>/feature.source.yaml`
- `docs/stages/<stage_id>.source.yaml`

Generated contracts and discovery are refreshed with:

```powershell
python scripts/sync_architecture_docs.py
```
