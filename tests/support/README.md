# Test Support Assets

<details>
<summary>Related documentation</summary>

- [.dev-guidelines/project-specific/TESTING.md](../../.dev-guidelines/project-specific/TESTING.md) – Fixture policies, shared asset expectations, and gating rules.
- [.dev-guidelines/project-specific/DOCUMENTATION.md](../../.dev-guidelines/project-specific/DOCUMENTATION.md) – README structure requirements and folder tree standards.

</details>

## Purpose

Shared configuration builders and fixture factories imported by unit, property, integration, and regression test suites. The `conftest.py` `metadata_config_factory` fixture delegates to this module.

## Exports

- **`create_basic_configs(tmp_path)`** — returns `(PreparerConfig, TrainerConfig, SamplerConfig, MetadataConfig)` for full prepare/train/sample flows.
- **`create_metadata_config(base_dir, *, experiment, mkdir, train_out_dir, sample_out_dir)`** — flexible `MetadataConfig` builder used across suites; creates subdirectories and a stub `config.toml` by default.

## Guidelines

- Treat files as read-only during tests. If a test needs writable temp data, create it under `tmp_path` or another temporary directory.
- Keep assets minimal to avoid slowing down the repository checkout or test execution.
- Prefer utility functions over large sample datasets; generate data programmatically when feasible.

## Folder Structure

```bash
tests/support/
├── README.md          # this file
└── config_builders.py # shared factories for experiment/test configurations
```

## Notes

- Add new modules only when multiple suites require the helper; otherwise keep logic inside the nearest test package.
- When adding binary assets, explain their provenance and size rationale in the pull request description.
- Remember that pre-commit hooks exercise the full quality gate; keep support helpers deterministic and dependency-light.
