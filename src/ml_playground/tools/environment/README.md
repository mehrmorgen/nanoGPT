# Environment Tools

<details>
<summary>Related documentation</summary>

- [Documentation Guidelines](../../../../.dev-guidelines/DOCUMENTATION.md) – Folder-level README blueprint and abstraction policy.

</details>

## Purpose

Tools for managing the development environment, including setup, verification, and cleanup.

## Usage

```bash
# Setup environment
uv run tools env setup

# Verify environment
uv run tools env verify

# Clean caches
uv run tools env clean
```

## Structure

```bash
src/ml_playground/tools/environment/
├── README.md        # package documentation (this file)
├── clean.py         # cache cleaning logic
├── environment.py   # main EnvironmentTools class
├── services.py      # environment services
├── setup.py         # environment setup logic
└── verify.py        # environment verification logic
```
