# Dev Container

<details>
<summary>Related documentation</summary>

- [Setup Guide](../.dev-guidelines/project-specific/SETUP.md) – Canonical environment setup options and troubleshooting.
- [Developer Guidelines](../.dev-guidelines/README.md) – Quality gate and workflow policy.

</details>

## Purpose

Provide a reproducible Linux development environment for macOS, Linux, and Windows contributors with consistent `uv`,
hooks, and test behavior.

## Included Defaults

- Base image: `mcr.microsoft.com/devcontainers/python:1-3.13-bookworm`
- `uv` installed for the `vscode` user
- Forwarded ports: `8050` (analyze), `5000` (MLflow), `6006` (TensorBoard)
- Repo-local cache/env variables under `.cache/`
- Persistent named volumes for `.venv` and `.cache`

## Post-Create Bootstrap

Executed automatically when the container is created:

```bash
bash .devcontainer/post-create.sh
```

## Usage

1. Install Docker Desktop (or another OCI runtime).
2. Install the VS Code Dev Containers extension.
3. Open the repository in VS Code.
4. Run `Dev Containers: Reopen in Container`.
