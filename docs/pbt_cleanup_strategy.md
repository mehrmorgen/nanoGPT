# PBT-First Cleanup Strategy - Next Tasks for Tools & Runtime CLI

**Status**: Cleanup complete. Focus on integration testing for CLI modules with realistic unit coverage (70-85%).

## Current Progress

| Module | Status | Tests Removed | Coverage Maintained |
|--------|---------|---------------|-------------------|
| **Tokenizer** | ✅ Complete | 44 → 16 tests | 94.04% → 90.12% |
| **Tools/Core/Runtime** | ✅ Complete | 4 → 3 tests | 92.31% maintained |
| **Configuration** | ✅ Complete | 99 → 90 tests | 95.31% → 94.13% |
| **Tokenization** | ⚠️ Skipped | Minimal redundancy | 87.50% essential error tests |
| **CLI** | ✅ Coverage Assessed | 69.64% coverage | Realistic for CLI architecture |
| **Runtime/Bootstrap** | ✅ Coverage Assessed | 81.40% coverage | Only pragma: no cover lines |

## Next Tasks: Tools & Runtime CLI Integration Testing

### Current Status
- **Unit Coverage Complete**: CLI modules at realistic limits (70-85% acceptable for command handlers)
- **Bootstrap Module**: 81.40% with only defensive pragma: no cover lines
- **Property Tests**: Comprehensive PBT coverage for happy-path scenarios

### Priority Tasks

#### 1. Enhance CLI Integration Tests
**Target**: Add end-to-end tests for tools CLI commands
```bash
# Focus areas for integration testing
tests/acceptance/features/tools_cli.feature
tests/acceptance/steps/test_tools_cli_steps.py
```

**Commands needing coverage**:
- Quality tools (lint, format, typecheck)
- Testing tools (unit, integration, e2e, coverage)
- Environment tools (ai-guidelines, tensorboard)
- CI tools (quality-gate)
- Development tools (review-rendering)

#### 2. Runtime CLI Acceptance Tests
**Target**: Complete runtime CLI command coverage
```bash
# Existing acceptance tests
tests/acceptance/features/runtime_cli.feature
```

#### 3. Coverage Standards Documentation
**Accept CLI unit coverage standards**:
- 70-85% acceptable for CLI/command handler modules
- Compensate with strong integration/E2E test coverage
- Document architectural limitations for CLI unit testing

### Implementation Strategy

#### Integration Test Enhancement
1. **Command Execution**: Test actual CLI commands through CliRunner
2. **Configuration Loading**: Test config file handling and defaults
3. **Error Propagation**: Verify CLI error handling and exit codes
4. **Learning Mode**: Test verbosity and learning mode functionality
5. **Project Root Handling**: Test project detection and configuration

#### Acceptance Test Coverage
1. **Happy Paths**: Verify successful command execution
2. **Error Conditions**: Test failure scenarios and error messages
3. **Configuration**: Test various config file scenarios
4. **Integration**: Test interaction with actual tools and file systems

### Validation Requirements

#### Integration Test Coverage
```bash
# Run integration tests with coverage
uv run pytest tests/acceptance/ --cov=ml_playground.tools.cli --cov-report=term-missing
```

#### Combined Coverage Metrics
```bash
# Full test suite coverage (unit + integration + e2e)
uv run pytest tests/ --cov=ml_playground.tools.cli --cov-report=term-missing
```

### Success Criteria

✅ **Integration Coverage**: CLI commands tested end-to-end through CliRunner
✅ **Error Handling**: All error paths tested in integration scenarios  
✅ **Configuration**: Config loading and defaults thoroughly tested
✅ **Documentation**: CLI coverage standards documented and accepted
✅ **Quality Gates**: All tests passing with comprehensive coverage

---

*This strategy focuses on integration testing for CLI modules where unit testing has reached realistic architectural limits.*
