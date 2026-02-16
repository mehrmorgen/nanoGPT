"""Shared constants for environment tooling."""

from __future__ import annotations

from pathlib import Path

DEFAULT_SYNC_GROUP = "all"
PRE_COMMIT_CONFIG_PATH = ".githooks/.pre-commit-config.yaml"
PRE_COMMIT_TEMPLATE_PATH = Path(".githooks/pre-commit")
REQUIRED_ENV_TOOLS = ("pre-commit", "yamlfix", "basedpyright", "mypy", "vulture")

FALLBACK_PRE_COMMIT_HOOK_TEMPLATE = """#!/usr/bin/env bash
echo 'Error: .githooks/pre-commit is missing or unreadable.' >&2
echo 'Please restore it from version control and rerun setup.' >&2
exit 1
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
