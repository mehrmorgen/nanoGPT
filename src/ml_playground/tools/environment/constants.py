"""Shared constants for environment tooling."""

from __future__ import annotations

from pathlib import Path

DEFAULT_SYNC_GROUP = "all"
PRE_COMMIT_CONFIG_PATH = ".githooks/.pre-commit-config.yaml"
PRE_COMMIT_TEMPLATE_PATH = Path(".githooks/pre-commit")
REQUIRED_ENV_TOOLS = ("pre-commit", "yamlfix", "basedpyright", "mypy", "vulture")

FALLBACK_PRE_COMMIT_HOOK_TEMPLATE = """#!/usr/bin/env bash
# Pre-commit hook using pre-commit framework with uv

set -euo pipefail

if ! command -v uv >/dev/null 2>&1; then
  echo "[pre-commit] Error: uv is required but not found on PATH." >&2
  echo "[pre-commit] Remediation: install uv, then run 'uv run tools env setup --clear'." >&2
  exit 1
fi

if [ ! -x ".venv/bin/pre-commit" ]; then
  echo "[pre-commit] Error: .venv/bin/pre-commit is missing." >&2
  echo "[pre-commit] Remediation: run 'uv sync --all-groups' then 'uv run tools env setup --clear'." >&2
  exit 1
fi

echo '[pre-commit] Starting full quality gate (verbose output enabled).'
echo '[pre-commit] This can take several minutes on first run.'
UV_NO_SYNC="${UV_NO_SYNC:-1}" uv run pre-commit run -v --config .githooks/.pre-commit-config.yaml

# Note: Mutation testing is excluded from pre-commit. Run manually via `make quality-ext` when needed.
"""


def resolve_pre_commit_hook_template(root_path: Path) -> str:
    """Resolve hook template content from repo source of truth.

    Args:
        root_path: Repository root path.

    Returns:
        Template content used to write `.git/hooks/pre-commit`.
    """
    template_path = root_path / PRE_COMMIT_TEMPLATE_PATH
    try:
        template = template_path.read_text(encoding="utf-8")
    except OSError:
        return FALLBACK_PRE_COMMIT_HOOK_TEMPLATE
    return template if template.strip() else FALLBACK_PRE_COMMIT_HOOK_TEMPLATE
