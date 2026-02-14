# Dev Tools

Development workflow utilities for ml_playground, focused on PR reviews, local repository hygiene, and AI guideline setup.

## Overview

The Dev package provides tools to streamline common development tasks. It includes helpers for managing GitHub PR review threads, cleaning up repository state, and ensuring AI coding assistants are properly configured with project guidelines.

## Key Components

### DevTools (`dev.py`)

- **Review Utilities**:
  - `review_list(pr_number)`: Fetches and displays review threads for a GitHub PR.
  - `review_bulk_reply(pr_number, replies_file)`: Sends multiple replies to PR threads from a JSON file.
  - `review_delete(pr_number, comments_file)`: Deletes specified comments from a PR.
- **Repository Hygiene**:
  - `cleanup_ignored_tracked()`: Removes files from git tracking that are matched by `.gitignore`.
  - `kill_port(port)`: Identifies and terminates processes running on a specific local port.
- **AI Guideline Setup**:
  - `setup_ai_guidelines(tool)`: Syncs project guidelines from `.dev-guidelines/` into tool-specific configuration directories (e.g., `.windsurf/rules/`, `.cursor/rules/`).

### AI Guidelines (`ai_guidelines.py`)

Core utilities for AI tool configuration:

- **Path Management**: `relative_tool_path()`, `project_path()` for path normalization
- **Pattern Matching**: `gitignore_match()`, `is_listed_in_aiignore()` for ignore file handling
- **Tree Mirroring**: `mirror_tree()` for recursive directory synchronization
- **Link Creation**: Cross-platform symlink/junction/hardlink creation with fallbacks

## Usage

These tools are typically invoked via the `uv run tools dev` command family.

```bash
uv run tools dev review-list 123
uv run tools dev cleanup-ignored-tracked
uv run tools dev kill-port 8080
uv run tools dev setup-ai-guidelines windsurf
```

## Testing

The dev tools module includes comprehensive test coverage:

- **Unit Tests**: Located in `tests/unit/tools/dev/`

  - Test individual functions and methods with mocked dependencies
  - Cover edge cases and error handling paths
  - Current coverage: ~96.65% (lines/branches)

- **Property Tests**: Located in `tests/property/tools/dev/`

  - Use Hypothesis for generative testing of core functions
  - Verify invariants across wide input spaces
  - Test path manipulation, pattern matching, and tree operations

### Running Tests

```bash
# Run all dev tools tests
uv run pytest tests/unit/tools/dev/ tests/property/tools/dev/ -q

# Run with coverage
uv run pytest tests/unit/tools/dev/ tests/property/tools/dev/ --cov=ml_playground.tools.dev --cov-report=term-missing
```

## Invariants

- **GitHub CLI Integration**: Review utilities require the `gh` CLI to be installed and authenticated.
- **Structured Results**: All commands return `ToolResult` for consistent CLI reporting and error handling.
- **Dependency Injection**: Subprocess execution and process management are handled via injectable runners and seams to ensure high testability.
- **Cross-Platform Compatibility**: File operations work across Windows, macOS, and Linux with appropriate fallbacks.
- **Idempotent Operations**: Most operations can be run multiple times safely (e.g., setup-ai-guidelines).

## Hardening Status

✅ **Completed**:

- Removed from coverage exclusions
- Full unit test suite with >96% coverage
- Property-based tests for core utilities
- All quality gates passing

📋 **Notes**:

- Remaining uncovered lines are primarily Windows-specific code paths
- Edge cases for invalid gitignore patterns are handled gracefully
- Error paths are tested and return appropriate ToolResult failures
