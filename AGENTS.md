.dev-guidelines/README.md

Additional agent rule:
- Always verify the current runtime environment before running project commands or tests.
- Virtual environment selection must support platform-specific variants using the naming pattern `.venv-<target>`.
- If one or more `.venv-<target>` directories exist, choose the one whose `<target>` matches the current runtime environment (for example: `.venv-wsl` for WSL, `.venv-linux` for Linux, `.venv-win` for Windows).
- If no matching `.venv-<target>` exists, fall back to `.venv`.
- Confirm the active Python executable and virtual environment path match the selected environment before running commands (for example via `python -V`, `which python`, and `$VIRTUAL_ENV` on Linux/WSL).
