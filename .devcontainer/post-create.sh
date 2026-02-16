#!/usr/bin/env bash
set -euo pipefail

sudo mkdir -p .venv .cache
sudo chown -R vscode:vscode .venv .cache

uv run tools env setup
uv pip install -e .
uv run tools env verify
