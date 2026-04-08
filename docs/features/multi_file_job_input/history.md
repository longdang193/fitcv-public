# Multi-File Job Input — History

## Changelog

### 1.0.0 — active

- `jobs_files: list[UploadFile]` parameter in trigger endpoint (`app.py`)
- Legacy single-file `jobs_file` parameter preserved for backward compatibility
- Per-file server-side JSON validation with descriptive error messages
- Canonical merge preserving file order into a single immutable snapshot
- All-or-nothing rejection on validation failure
- UI file input with `multiple` attribute in `runs_list.html`
- JavaScript FormData loop appending each file as `jobs_files`

### 0.1.0 — planned

- Feature concept: upload multiple JSON job files per trigger request

## Post-Execution Review

- All capabilities from the planned contract are implemented
- Backward compatibility maintained via legacy `jobs_file` single-file fallback
- Empty-array-after-merge edge case handled with explicit 400 error

