# tools.core contracts

- `LearningInfo` and `ToolResult` are the canonical learning-mode payloads for tools. Use `ToolResult.create(...)` to build results; it validates `OperationId` and supplies an empty `LearningInfo` when none is provided.
- `OperationId` validation:
  - `namespace`: only `"tools"` or `"ml"`.
  - `category` (tools): one of `ci`, `quality`, `test`, `env`, `agentic`, `dev`, `learn`, `analysis`.
  - `category` (ml): one of `prepare`, `train`, `sample`, `analyze`.
  - `command`: alphanumeric with optional `-` or `_`.
- Learning mode:
  - Verbosity: `MINIMAL` (no best practices/related concepts, no context line), `STANDARD` (balanced), `COMPREHENSIVE` (adds category best practices).
  - `LearningModeEngine.explain_command(...)` preserves `executed_commands` and appends context for non-minimal verbosity.
  - `LearningModeEngine.format_output(...)` always prints operation success/failure and exit code, optional stdout/stderr blocks, and learning info only when `learning_enabled=True`.
- Dependency guidance:
  - Keep `LearningModeEngine` pure (no IO, no subprocess). Accept data, return data/strings.
  - Do not inject runtime-only categories; the validator is strict to avoid fallback code paths.
  - If you add a new tool category, update `OperationId.validate_category` and extend educational content to match.
- Tests:
  - Unit: `tests/unit/tools/core/test_learning_mode.py`
  - Property: `tests/property/tools/core/test_learning_mode_property.py`
