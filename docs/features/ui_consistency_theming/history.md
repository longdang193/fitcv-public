# UI Consistency & Theming — History

## Changelog

### 1.0.0 — active

- CSS custom properties design tokens in `base.html` (`:root[data-theme="dark"]` / `:root[data-theme="light"]`)
- Dark/light theme toggle with localStorage persistence (`fitcv-theme` key)
- Flash-free theme application via inline `<script>` before CSS renders
- Shared component classes: `.btn-secondary`, `.inspection-card`, `.tab-btn`
- Consistent action hierarchy across all admin pages
- Human-readable section headings
- Attached-tab inspection card pattern in `run_detail.html`
- Responsive wrapping

### 0.5.0 — building

- Settings and run detail composition consistency
- Specs/plans: see `refs` in the feature contract

## Post-Execution Review

- All capabilities from the contract are implemented
- Design token system covers colors, backgrounds, borders, text, and form elements
- Theme toggle button uses emoji icons (☀️/🌙) for clear visual feedback
