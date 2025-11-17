# PBT-First Cleanup Strategy - Next Tasks for Tools & Runtime CLI

**Status**: Cleanup complete. Focus on integration testing for CLI modules with realistic unit coverage (70-85%).

## Current Progress

| Module | Status | Tests Removed | Coverage Maintained |
|--------|---------|---------------|-------------------|
| **Tokenizer** | ✅ Complete | 44 → 16 tests | 94.04% → 90.12% |
| **Tools/Core/Runtime** | ✅ Complete | 4 → 3 tests | 92.31% maintained |
| **Configuration** | ✅ Complete | 99 → 90 tests | 95.31% → 94.13% |
| **CLI** | ✅ Complete | 6 patterns eliminated + exit code fixes | Refactored for better coverage |
| **Tokenization** | ⚠️ Skipped | Minimal redundancy | 87.50% essential error tests |
| **Runtime/Bootstrap** | ✅ Coverage Assessed | 81.40% coverage | Only pragma: no cover lines |

## Next Tasks: Comprehensive CLI Restructuring & Code Quality

### Current Status
- **Unit Coverage Enhancement**: CLI modules refactored to eliminate branching complexity
- **Branch Reduction**: 6 repeated None-check patterns eliminated via common helper
- **Exit Code Consistency**: Replaced sys.exit() calls with typer.Exit() for proper CLI error handling
- **Architecture Ready**: Foundation laid for comprehensive restructuring
- **Property Tests**: Comprehensive PBT coverage for happy-path scenarios

### Completed Refactoring Tasks

#### 1. CLI Branch Complexity Reduction ✅
**Target**: Eliminate repeated None-check patterns in `ml_playground/tools/cli/main.py`
```python
# BEFORE: 6 identical None-check patterns creating 12+ branches
def get_quality_tools() -> QualityTools:
    if state.config is None:
        load_config_with_error_handling(state.project_root)
    assert state.config is not None
    # ... repeated in all tool getters

# AFTER: Common helper eliminates duplication
def _ensure_config_loaded() -> None:
    if state.config is None:
        load_config_with_error_handling(state.project_root)

def get_quality_tools() -> QualityTools:
    _ensure_config_loaded()
    assert state.config is not None
    deps = get_tools_dependencies()
    return deps.quality_factory(state.config, state.project_root or Path.cwd())
```

**Branches Eliminated**: Removed 6 repeated None-check patterns (12+ conditional branches)
**Files Affected**: `src/ml_playground/tools/cli/main.py` (728 lines → simplified)
**Test Compatibility**: Maintained by keeping Optional types for lazy initialization

#### 2. CLI Exit Code Consistency ✅
**Target**: Replace inconsistent sys.exit() calls with proper Typer exit handling
```python
# BEFORE: Inconsistent exit handling bypassing Typer's system
def main_entry() -> None:
    try:
        app()
    except KeyboardInterrupt:
        typer.echo("\nOperation cancelled by user", err=True)
        sys.exit(1)  # Bypasses Typer exit system
    except Exception as e:
        typer.echo(f"Unexpected error: {e}", err=True)
        sys.exit(1)  # Bypasses Typer exit system

# AFTER: Consistent Typer exit handling
def main_entry() -> None:
    try:
        app()
    except KeyboardInterrupt:
        typer.echo("\nOperation cancelled by user", err=True)
        raise typer.Exit(1)  # Uses Typer exit system
    except typer.Exit:
        # Let Typer exit codes propagate properly
        raise
    except Exception as e:
        typer.echo(f"Unexpected error: {e}", err=True)
        raise typer.Exit(1)  # Uses Typer exit system
```

**Benefits Achieved**: 
- Consistent CLI error handling using Typer's exit system
- Improved testability (typer.Exit exceptions easier to test than sys.exit)
- Proper error propagation through CLI system
- Clean codebase (removed obsolete test files)

**Files Affected**: `src/ml_playground/tools/cli/main.py` (main_entry function)
**Tests Cleaned**: Removed obsolete `test_main_helpers.py` that tested old CLI structure

### Priority Tasks: Comprehensive Restructuring

#### 1. Structural Reorganization
**Target**: Fix architectural issues and eliminate duplicate files
```bash
# Remove duplicate CLI entry point
DELETE: src/ml_playground/tools/cli.py
# Keep: src/ml_playground/tools/cli/main.py (proper CLI implementation)

# Create missing test directories
CREATE: tests/unit/runtime/cli/
CREATE: tests/property/runtime/cli/

# Split mixed CLI tests:
MOVE: tests/unit/cli/test_cli.py → tests/unit/runtime/cli/test_cli.py
MOVE: tests/property/cli/test_cli_property.py → tests/property/runtime/cli/test_cli_property.py
```

**Issues Fixed**:
- Duplicate CLI entry points (`tools/cli.py` vs `tools/cli/main.py`)
- Misorganized test structure (mixed runtime/tools CLI tests)
- Missing test directories that mirror source structure

#### 2. File Splitting for Code Reduction
**Target**: Break down large files into focused modules
```bash
# Split large tools/cli/main.py (728 lines) into focused modules:
src/ml_playground/tools/cli/
├── main.py              # Entry point and app setup (~100 lines)
├── commands/
│   ├── quality.py       # Quality commands (~100 lines)
│   ├── testing.py       # Testing commands (~100 lines)
│   ├── environment.py   # Environment commands (~100 lines)
│   ├── ci.py           # CI commands (~100 lines)
│   └── dev.py          # Development commands (~100 lines)
├── state.py             # Global state management (~50 lines)
└── dependencies.py      # Factory and dependency logic (~100 lines)

# Split large runtime/cli/runners.py (12,182 lines) into focused modules:
src/ml_playground/runtime/cli/
├── runners/
│   ├── train.py         # Training runner logic
│   ├── sample.py        # Sampling runner logic
│   ├── device.py        # Device management
│   └── config.py        # Configuration handling
```

**Benefits**:
- **Reduced File Size**: 728-line main.py → ~100-line focused modules
- **Focused Testing**: Each module can achieve 100% coverage independently
- **Better Organization**: Commands grouped by functionality
- **Easier Maintenance**: Changes isolated to specific command areas

#### 3. Legacy Indirection Removal
**Target**: Eliminate unnecessary abstraction layers and fallback code
```bash
# Patterns to eliminate:
1. Optional types with repeated None-checks
2. Factory pattern indirections where direct instantiation works
3. Configuration fallback chains
4. Legacy compatibility flags and duplicate state
5. Over-engineered dependency injection

# Examples:
BEFORE:
_dependency_factory: Callable[[], ToolsDependencies] = default_tools_dependencies
_cached_dependencies: Optional[ToolsDependencies] = None

def get_tools_dependencies() -> ToolsDependencies:
    global _cached_dependencies
    if _cached_dependencies is None:
        _cached_dependencies = _dependency_factory()
    return _cached_dependencies

AFTER:
_cached_dependencies: ToolsDependencies = default_tools_dependencies()

def get_tools_dependencies() -> ToolsDependencies:
    return _cached_dependencies
```

#### 4. Better Types to Reduce Branching
**Target**: Use advanced typing to eliminate conditional logic
```bash
# Type improvements to eliminate conditional logic:
1. Replace Optional[T] + None-checks with Union types + type guards
2. Use Literal types for command dispatch instead of dict lookups
3. Use specific validated types (PositiveInt, UnitIntervalFloat)
4. Replace isinstance checks with Union narrowing
5. Use dataclasses with defaults instead of manual initialization

# Examples:
BEFORE:
if command == "lint":
    return run_lint()
elif command == "format":
    return run_format()
elif command == "typecheck":
    return run_typecheck()

AFTER:
from typing import Literal

CommandType = Literal["lint", "format", "typecheck"]

def run_command(command: CommandType) -> ToolResult:
    if command == "lint":
        return run_lint()
    elif command == "format":
        return run_format()
    else:  # typecheck - compiler knows this is the only remaining option
        return run_typecheck()
```

### Implementation Strategy

#### Phase 1: Basic Reorganization
1. **Verify Dependencies**: Check if `tools/cli.py` is imported anywhere
2. **Create Directories**: Create missing test directories
3. **Move Tests**: Split and move CLI tests to appropriate locations
4. **Remove Duplicates**: Delete redundant `tools/cli.py`

#### Phase 2: File Splitting
1. **Analyze Large Files**: Identify logical groupings in 728-line main.py
2. **Extract Commands**: Split command handlers into separate modules
3. **Split State Management**: Extract state logic into dedicated module
4. **Break Down Runners**: Split 12,182-line runners.py by functionality

#### Phase 3: Indirection Removal
1. **Identify Patterns**: Find Optional types with repeated None-checks
2. **Replace Factories**: Use direct instantiation where safe
3. **Remove Legacy Code**: Eliminate backward-compatibility flags
4. **Consolidate State**: Remove duplicate state management

#### Phase 4: Type System Enhancement
1. **Replace Optional Types**: Use Union + type guards
2. **Add Literal Types**: Use for command dispatch
3. **Apply Validated Types**: Use PositiveInt, UnitIntervalFloat, etc.
4. **Eliminate isinstance**: Use Union narrowing instead

### Validation Requirements

#### Code Quality Metrics
```bash
# File size reduction targets
find src/ml_playground/tools/cli -name "*.py" -exec wc -l {} + | sort -n
find src/ml_playground/runtime/cli -name "*.py" -exec wc -l {} + | sort -n

# Branch count analysis
rg "if.*:" src/ml_playground/tools/cli/main.py | wc -l
rg "elif.*:" src/ml_playground/tools/cli/main.py | wc -l

# Indirection pattern detection
rg "Optional\[" src/ml_playground/tools/cli/main.py
rg "factory" src/ml_playground/tools/cli/main.py
```

#### Test Coverage Validation
```bash
# CLI module coverage check
uv run pytest tests/unit/tools/cli/test_main.py tests/property/tools/cli/test_tools_cli_property.py --cov=ml_playground.tools.cli --cov-report=term-missing

# Runtime CLI coverage check
uv run pytest tests/unit/runtime/cli/test_cli.py tests/property/runtime/cli/test_cli_property.py --cov=ml_playground.runtime.cli --cov-report=term-missing
```

### Success Criteria

✅ **Structural Organization**: Clear separation of runtime vs tools CLI
✅ **File Size Reduction**: Large files split into focused ~100-line modules
✅ **Branch Elimination**: 20+ conditional branches removed through better typing
✅ **Indirection Removal**: Factory patterns replaced with direct instantiation
✅ **Type Safety**: Union types and Literal types reduce runtime checks
✅ **Test Coverage**: Each module independently achieves 100% coverage
✅ **Maintainability**: Changes isolated to specific command domains

---

*This comprehensive strategy addresses both test coverage optimization and code quality improvements through systematic restructuring, file splitting, indirection removal, and advanced type usage.*
