# PBT-First Refactoring Cleanup Strategy

## Overview

This document outlines the current state of the property-based testing (PBT) implementation and the strategy for cleaning up redundant unit tests following the PBT-first guidelines from `.dev-guidelines/TESTING.md`.

## Current State Analysis

### Property Test Suite Status

**Coverage**: 40.52% (Required: 86.0%)
**Status**: 28 failed, 60 passed, 1 skipped, 4 errors

#### Property Test Files (16 total)
- `tests/property/cli/test_cli_property.py` - Runtime CLI property tests
- `tests/property/configuration/test_configuration_property.py` - Configuration package tests
- `tests/property/configuration/test_loading_property.py` - Configuration loading tests
- `tests/property/data_pipeline/test_preparer_property.py` - Data preparer tests
- `tests/property/data_pipeline/test_tokenization_property.py` - Tokenization tests
- `tests/property/runtime/test_runtime_cli_property.py` - Runtime CLI tests
- `tests/property/tools/ci/test_ci_tools_property.py` - CI tools tests
- `tests/property/tools/cli/test_tools_cli_property.py` - Tools CLI tests
- `tests/property/tools/core/test_runtime_state_property.py` - Runtime state tests
- `tests/property/tools/dev/test_dev_tools_property.py` - Dev tools tests
- `tests/property/tools/environment/test_environment_tools_property.py` - Environment tools tests
- `tests/property/tools/quality/test_quality_tools_property.py` - Quality tools tests
- `tests/property/tools/testing/test_coverage_helpers_property.py` - Coverage helpers tests
- `tests/property/tools/testing/test_testing_tools_property.py` - Testing tools tests
- `tests/property/tools/utils/test_subprocess_utils_property.py` - Subprocess utils tests
- `tests/property/training/test_trainer_loop_property.py` - Trainer loop tests

### Unit Test Suite Status

**Total Files**: 45+ unit test files
**Major Issues**: Extensive API drift from tools/runtime refactoring

#### Critical Failure Patterns

1. **Import Location Changes**
   - `ml_playground.runtime.cli.run_or_exit` → `ml_playground.runtime.cli.result.run_or_exit`
   - `ml_playground.runtime.cli.run_analyze` → `ml_playground.runtime.cli.runners.run_analyze`
   - Various functions moved to different CLI submodules

2. **API Changes**
   - `DevTools` constructor no longer accepts `pids_by_port` parameter
   - Typer CLI structure changes affecting command resolution
   - Validation errors in configuration models

3. **Missing Dependencies**
   - Pydantic validation errors in subprocess utilities
   - Missing torch module references in device setup
   - Broken dependency injection patterns

## Cleanup Strategy

### Phase 1: Fix Property Test Suite (Prerequisite)

**Priority**: HIGH - Cannot proceed without working PBT baseline

#### 1.1 Import Fixes
- Update all import statements to match new module structure
- Fix `ml_playground.runtime.cli` imports to use specific submodules
- Resolve torch module dependency issues

#### 1.2 API Compatibility
- Fix DevTools dependency injection patterns
- Update Typer CLI command resolution tests
- Resolve Pydantic validation errors

#### 1.3 Validation Fixes
- Fix hypothesis strategy validation errors
- Resolve subprocess utility parameter validation
- Update configuration model tests

**Expected Outcome**: Property tests achieve ≥86% coverage with 100% pass rate

### Phase 2: Coverage Comparison Analysis

**Priority**: HIGH - Identify redundant unit tests systematically

#### 2.1 Baseline Coverage
```bash
# Property-only coverage
uv run pytest --cov=ml_playground --cov-report=json:.cache/coverage-property.json tests/property/

# Unit + property coverage  
uv run pytest --cov=ml_playground --cov-report=json:.cache/coverage-combined.json tests/unit/ tests/property/
```

#### 2.2 Redundancy Identification
- Compare coverage reports to identify unit tests adding zero incremental coverage
- Focus on branch coverage (primary metric per guidelines)
- Preserve unit tests documenting explicit business rules or regressions

### Phase 3: Strategic Unit Test Removal

**Priority**: MEDIUM - Remove redundant tests while preserving business value

#### 3.1 Removal Criteria
- Unit tests completely covered by property tests (zero delta coverage)
- Tests of removed/deprecated functions (confirmed via source code analysis)
- Duplicate test logic already covered by property-based approaches

#### 3.2 Preservation Criteria
- Tests documenting specific business rules (per TESTING.md §4.1)
- Regression tests for specific bug fixes
- Tests requiring explicit scenario names for clarity
- Tests of opaque collaborators where property tests would duplicate implementation

#### 3.3 Naming Compliance
- Ensure remaining unit tests follow `test_<behavior>_<condition>_<expected>()` pattern
- Verify property tests use `test_<subject>_property.py` naming convention
- Update any non-compliant test file names

### Phase 4: Quality Gate Validation

**Priority**: HIGH - Ensure cleanup doesn't break coverage requirements

#### 4.1 Coverage Verification
```bash
uv run tools ci quality-gate
```

#### 4.2 Requirements
- Global line coverage: 100% (NO EXCEPTIONS)
- Per-module line coverage: 100% for ALL `ml_playground/*` modules
- Branch coverage: 100% for ALL modules (NO COMPROMISES)

## Immediate Actions Required

### 1. Fix Critical Property Test Issues
- `tests/property/cli/test_cli_property.py::test_global_device_setup_*` - ✅ FIXED
- `tests/property/cli/test_cli_property.py::test_log_command_status_*` - ✅ FIXED
- `tests/property/tools/*/test_*_property.py` - Hypothesis health check failures
- `tests/property/tools/utils/test_subprocess_utils_property.py` - Pydantic validation errors

## ✅ COMPLETED: CLI Property Test Fixes

### Fix Patterns Discovered

#### Pattern 1: Module Import Updates
**Issue**: Functions moved to different CLI submodules during refactoring
**Solution**: Update imports to use `ml_playground.runtime.cli.main` directly
```python
# Old pattern (broken)
import ml_playground.runtime.cli as cli

# New pattern (working)
import ml_playground.runtime.cli.main as cli
from ml_playground.runtime.cli.main import get_command, global_options, run_train_cmd, run_sample_cmd, main
```

#### Pattern 2: Parameter Injection over Monkeypatching
**Issue**: Tests trying to override torch module with monkeypatching
**Solution**: Use existing dependency injection parameters
```python
# Old pattern (broken)
with override_attr(cli_main, "torch", BadTorch()):
    global_device_setup("cpu", "float32", 123)

# New pattern (working)
global_device_setup("cpu", "float32", 123, torch_module=BadTorch())
```

#### Pattern 3: Package-Level Override Targets
**Issue**: Functions use package-level attribute lookup for test overrides
**Solution**: Override on the package where the function is actually looked up
```python
# Old pattern (wrong target)
with override_attr(cli_runners, "log_directory", boom):

# New pattern (correct target)
import ml_playground.runtime.cli as cli_pkg
with override_attr(cli_pkg, "log_directory", boom):
```

### CLI Test Results
- **Before**: 2 failed, 7 passed
- **After**: 9 passed, 0 failed
- **Coverage**: Now provides working baseline for CLI module

### 2. Update Import Patterns
```python
# Old pattern (broken)
from ml_playground.runtime.cli import run_or_exit, run_analyze

# New pattern (working)  
from ml_playground.runtime.cli.result import run_or_exit
from ml_playground.runtime.cli.runners import run_analyze
```

### 3. Fix Dependency Injection
```python
# Old pattern (broken)
tools = dev.DevTools(pids_by_port=fake_pids, kill_pid=fake_kill)

# New pattern (working)
tools = dev.DevTools(config=ToolsConfig(), subprocess_runner=runner, root_path=tmp_path)
```

## ✅ CURRENT STATUS: Phase 1 Partially Complete

### Completed Work
- **CLI Property Tests**: 9/9 tests passing (100% success rate)
- **Fix Patterns Documented**: 3 key patterns identified and documented
- **Coverage Baseline**: CLI property tests achieve 36.74% coverage
- **Import Fixes**: Resolved all CLI import errors from refactoring

### Remaining Work
- **Tools Property Tests**: 9 failed, 1 error due to Typer API changes and fixture issues
- **Coverage Gap**: Need additional 49.26% coverage to reach 86% threshold
- **Systematic Cleanup**: Cannot proceed until sufficient PBT coverage exists

## Updated Success Metrics

1. **Property Test Suite**: CLI 100% pass rate, Tools need fixes
2. **Coverage Baseline**: CLI 36.74%, need ≥86% for analysis
3. **Quality Gate**: Coverage threshold prevents validation
4. **Test Reduction**: Requires expanded PBT coverage first

## Revised Timeline Estimate

- **Phase 1 (Property Fixes)**: ✅ CLI complete (2 hours), Tools remaining (2-4 hours)
- **Phase 2 (Coverage Analysis)**: ⏸️ Blocked until 86% coverage achieved
- **Phase 3 (Unit Test Cleanup)**: ⏸️ Dependent on Phase 2 completion
- **Phase 4 (Validation)**: ⏸️ Dependent on Phase 3 completion

**Current Investment**: 2 hours completed
**Remaining Estimated**: 4-6 hours (primarily tools property test fixes)

## Dependencies

- Must have working property test suite before systematic cleanup
- Requires understanding of new tools/runtime module structure
- May need updates to test utilities and helpers

## Notes

- Manual editing of test files proved error-prone (see test_cli.py line 525 bug)
- Prefer systematic pytest-based identification over manual string matching
- Focus on branch coverage as primary metric per PBT-first guidelines
- Preserve narrative tests for business rules and regressions per TESTING.md §4.1

---

*This strategy document should be updated as the property test suite is fixed and cleanup progresses.*
