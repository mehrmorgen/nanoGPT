# Specialized Utility Scripts

This directory contains specialized utility scripts that provide complex functionality not easily integrated into the main tools system.

## Scripts

### AI Development Guidelines
- **`setup_ai_guidelines.py`** - Set up AI development guidelines for various tools
  - Complex tool-specific configuration management
  - Used by: `uv run tools env ai-guidelines <tool_name>`
  - Direct usage: `uv run python scripts/setup_ai_guidelines.py <tool_name>`

### GGUF Model Conversion
- **`llama_cpp/`** - GGUF model conversion utilities
  - Specialized model format conversion tools
  - See `llama_cpp/README.md` for detailed usage

## Integration Status

Most development tools have been integrated into the main `uv run tools` system:

- **Review management**: `uv run tools dev review-*` (was `scripts/review.py`)
- **Mutation testing**: `uv run tools test mutation *` (was `scripts/mutation_*.py`)
- **Port management**: `uv run tools dev kill-port` (was `scripts/port_kill.py`)
- **Cleanup utilities**: `uv run tools dev cleanup-ignored-tracked` (was `scripts/cleanup_ignored_tracked.py`)
- **Coverage badges**: `uv run tools ci coverage-badge` (integrated directly)

Only complex, specialized functionality remains as standalone scripts.