# Documentation Formatting

Comparison of documentation formatting/linting tools and rationale for the choice reflected in this repo’s configuration.

## Comparison (summary)

| Tool                        | Scope         | Automation                | Ecosystem/maturity | Notes                          |
| --------------------------- | ------------- | ------------------------- | ------------------ | ------------------------------ |
| **mdformat + markdownlint** | Format + lint | High (autoformat + rules) | Mature             | Current choice in this repo.   |
| Prettier (markdown)         | Format        | High                      | Mature             | Popular; broader JS toolchain. |
| None/manual                 | N/A           | None                      | N/A                | Inconsistent, not recommended. |

## Our choice

- **mdformat + markdownlint** for consistent Markdown formatting and rule enforcement.

## When to consider alternatives

- Prettier: teams standardizing on JS toolchains or already using Prettier elsewhere.
- Manual: only for trivial, one-off docs (accept inconsistency).

## Practices

- Keep docs lintable and autoformat-friendly (blank lines around headings/lists, consistent bullets).
- Centralize rules; avoid per-file overrides unless necessary.
- Prefer tables for comparisons; keep them narrow and readable.
