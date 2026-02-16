from __future__ import annotations

import json
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
DEVCONTAINER_DIR = REPO_ROOT / ".devcontainer"
DEVCONTAINER_CONFIG = DEVCONTAINER_DIR / "devcontainer.json"
DEVCONTAINER_DOCKERFILE = DEVCONTAINER_DIR / "Dockerfile"


def _load_devcontainer_json() -> dict[str, Any]:
    with DEVCONTAINER_CONFIG.open(encoding="utf-8") as handle:
        return json.load(handle)


def test_devcontainer_required_files_exist() -> None:
    assert DEVCONTAINER_DIR.exists(), ".devcontainer directory must exist"
    assert DEVCONTAINER_CONFIG.exists(), "devcontainer.json must exist"
    assert DEVCONTAINER_DOCKERFILE.exists(), "Dev Container Dockerfile must exist"


def test_devcontainer_forwards_expected_ports() -> None:
    config = _load_devcontainer_json()
    ports = config.get("forwardPorts")
    assert isinstance(ports, list), "forwardPorts must be a list"
    expected_ports = {8050, 5000, 6006}
    actual_ports = {port for port in ports if isinstance(port, int)}
    assert expected_ports.issubset(actual_ports), (
        "devcontainer must forward analyze/MLflow/TensorBoard ports (8050/5000/6006)"
    )


def test_devcontainer_post_create_bootstrap_contract() -> None:
    config = _load_devcontainer_json()
    command = config.get("postCreateCommand")
    assert isinstance(command, str), "postCreateCommand must be a string"
    assert command == "bash .devcontainer/post-create.sh", (
        "postCreateCommand must delegate to the devcontainer bootstrap script"
    )

    script_path = DEVCONTAINER_DIR / "post-create.sh"
    assert script_path.exists(), "post-create.sh must exist"
    script = script_path.read_text(encoding="utf-8")
    for snippet in (
        "uv run tools env setup",
        "uv pip install -e .",
        "uv run tools env verify",
    ):
        assert snippet in script, f"post-create.sh must include `{snippet}`"


def test_devcontainer_environment_contract() -> None:
    config = _load_devcontainer_json()
    env = config.get("containerEnv")
    assert isinstance(env, dict), "containerEnv must be configured"

    expected_keys = {
        "PYTHONPATH",
        "UV_CACHE_DIR",
        "PRE_COMMIT_HOME",
        "RUFF_CACHE_DIR",
        "HYPOTHESIS_DATABASE_DIRECTORY",
        "HYPOTHESIS_STORAGE_DIRECTORY",
    }
    missing = sorted(expected_keys - set(env))
    assert not missing, "containerEnv missing expected keys: " + ", ".join(missing)
