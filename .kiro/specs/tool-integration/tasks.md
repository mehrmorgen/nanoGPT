# Implementation Plan

## Important Constraint: BasedPyright Strict Mode

The base branch is currently working on removing all BasedPyright warnings and errors in strict mode. During this tool integration work:

- **DO NOT fix existing codebase for strict mode** - this is being handled in the base branch
- **DO write all new code in `ml_playground/tools/` to strict mode standards** - this will help when we rebase
- **DELAY any modifications outside the `tools/` folder** until later phases to avoid conflicts
- **Focus on the `tools/` module first** - it can be developed independently with strict typing

## Task Workflow Requirements

**BEFORE starting each task**: Check if the base branch has updates and rebase if necessary to stay current with the strict mode work being done in parallel.

**AFTER completing each task**: Create a commit with the completed work. Each task should result in exactly one commit following the project's commit standards (granular commits, TDD pairing, quality gates passing).

## Critical Testing Guidelines

**NO MOCKING POLICY (STRICTLY ENFORCED)**:

- Absolutely NO `unittest.mock`, `pytest_mock`, `MagicMock`, `patch(`, or `monkeypatch` anywhere in tests
- Use dependency injection and lightweight in-memory fakes exclusively
- Design production code with DI seams for external boundaries (subprocess, filesystem, time, network)
- Create seamable adapters for external dependencies following project patterns
- Tests must exercise the same public API and code paths used in production
- Pre-commit hooks will reject any code containing forbidden mocking tokens

- [x] 1. Set up core infrastructure and interfaces
  - Create the basic module structure under `src/ml_playground/tools/`
  - Implement core interfaces (ToolInterface, ToolResult, OperationId) with Pydantic validation
  - Set up error handling system extending MLPlaygroundError hierarchy
  - Create configuration management with explicit timeout philosophy
  - _Requirements: 1.1, 2.1, 3.1, 3.4, 3.6_

- [x] 1.1 Create module structure and package initialization
  - Create `src/ml_playground/tools/__init__.py` with package exports
  - Create subdirectories: `core/`, `categories/`, `utils/`
  - Set up proper PEP 420 namespace structure
  - _Requirements: 1.1, 2.4_

- [x] 1.2 Implement core data models with Pydantic validation
  - Create `src/ml_playground/tools/core/interfaces.py` with OperationId, ToolResult, and LearningInfo models
  - Implement ToolInterface protocol with category and command properties
  - Add validation for operation ID generation (namespace.category.command format)
  - Use explicit type annotations compatible with BasedPyright strict mode
  - _Requirements: 1.1, 3.4, 3.6_

- [x] 1.3 Set up tool-specific error handling
  - Create `src/ml_playground/tools/core/errors.py` with tool-specific error types
  - Implement ToolExecutionError, ToolConfigurationError, EnvironmentSetupError, DependencyError
  - Ensure all errors follow MLPlaygroundError pattern with reason and rationale
  - Use strict type annotations for all error classes and methods
  - _Requirements: 2.1, 3.4, 3.6_

- [x] 1.4 Create configuration management system
  - Create `src/ml_playground/tools/core/config.py` with ToolConfig, QualityToolsConfig, TestToolsConfig models
  - Implement explicit timeout configuration with reasonable defaults
  - Add configuration loading from pyproject.toml
  - _Requirements: 6.1, 6.2, 6.3, 6.4_

- [x] 1.5 Implement basic CLI structure
  - Create `src/ml_playground/tools/cli.py` with main Typer app
  - Set up command routing and help system
  - Implement global options (learning mode, verbosity, dry-run)
  - _Requirements: 1.1, 1.4, 7.1, 7.2_

- [x] 2. Implement testing and quality tools for self-validation
  - Migrate and enhance testing functionality from tools/test_tasks.py
  - Migrate and enhance quality functionality from tools/lint_tasks.py and tools/ci_tasks.py
  - Implement these tools using the new infrastructure
  - Use these tools to validate subsequent development phases
  - _Requirements: 5.1, 5.2, 5.3, 5.4_

- [x] 2.1 Implement testing tools category
  - Create `src/ml_playground/tools/categories/testing.py` with unit, integration, e2e, property test commands
  - Implement coverage reporting and threshold enforcement
  - Add support for pytest arguments and parallel execution
  - _Requirements: 5.4, 8.1, 8.2_

- [x] 2.2 Implement quality tools category
  - Create `src/ml_playground/tools/categories/quality.py` with lint, format, typecheck commands
  - Integrate ruff, mypy, and basedpyright execution with strict mode support
  - Add support for configuration file paths and tool-specific options
  - Ensure all code is compatible with BasedPyright strict mode for future rebase
  - _Requirements: 5.1, 8.1, 8.2_

- [x] 2.3 Create subprocess execution utilities
  - Create `src/ml_playground/tools/utils/subprocess_utils.py` with safe process execution
  - Implement timeout handling and error reporting
  - Add support for environment variable injection and working directory control
  - _Requirements: 3.4, 8.1, 8.2, 12.1, 12.2_

- [x] 2.4 Add CLI commands for testing and quality
  - Register testing commands under `uv run tools test` subcommand group
  - Register quality commands under `uv run tools quality` subcommand group
  - Implement help text and argument validation
  - _Requirements: 1.1, 1.4, 7.1, 7.2_

- [x] 2.5 REFACTOR: Remove mocks from existing tests and implement dependency injection
  - **CRITICAL: Current tests use mocks - MUST be refactored to comply with testing guidelines**
  - **NO MOCKING ALLOWED**: Remove all `unittest.mock`, `pytest_mock`, `MagicMock`, `patch(`, `monkeypatch`
  - Refactor production code to use dependency injection for external boundaries (subprocess, filesystem)
  - Create lightweight in-memory fakes for subprocess execution and filesystem operations
  - Implement seamable adapters for external boundaries following project patterns
  - Ensure tests exercise the same public API and code paths used in production
  - Implement property-based tests for argument validation and execution
  - Achieve 100% line and branch coverage for implemented functionality
  - Use explicit type annotations in all test code for strict mode compatibility
  - _Requirements: 2.1, 2.2_

- [x] 3. Implement environment and CI tools
  - Migrate environment management from tools/env_tasks.py
  - Migrate CI functionality from tools/ci_tasks.py
  - Implement these using the established patterns from Phase 2
  - _Requirements: 5.2, 5.3, 11.1, 11.2_

- [x] 3.1 Implement environment tools category
  - Create `src/ml_playground/tools/categories/environment.py` with setup, sync, verify, clean commands
  - Add support for dependency group selection and environment validation
  - Implement cache management and cleanup operations
  - _Requirements: 5.2, 8.1, 8.2_

- [x] 3.2 Implement CI tools category
  - Create `src/ml_playground/tools/categories/ci.py` with quality-gate, mutation, badges commands
  - Add support for coverage reporting and badge generation
  - Implement mutation testing integration with Cosmic Ray
  - _Requirements: 5.1, 11.1, 11.2, 11.4_

- [x] 3.3 Add CLI commands for environment and CI
  - Register environment commands under `uv run tools env` subcommand group
  - Register CI commands under `uv run tools ci` subcommand group
  - Implement comprehensive help and usage examples
  - _Requirements: 1.1, 1.4, 7.1, 7.2_

- [x] 3.4 Write unit tests for environment and CI tools
  - **NO MOCKING ALLOWED**: Absolutely no `unittest.mock`, `pytest_mock`, `MagicMock`, `patch(`, `monkeypatch`
  - Create comprehensive test coverage for environment.py and ci.py
  - Use dependency injection and lightweight in-memory fakes exclusively
  - Design production code with DI seams for external boundaries (subprocess, filesystem)
  - Validate configuration loading and error handling
  - _Requirements: 2.1, 2.2_

- [x] 4. Implement learning mode infrastructure
  - Create learning mode engine with verbosity level support
  - Add educational content for existing tool categories
  - Implement command explanation and best practices system
  - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5_

- [x] 4.1 Create learning mode engine
  - Create `src/ml_playground/tools/core/learning_mode.py` with LearningModeEngine class
  - Implement VerbosityLevel enum with MINIMAL, STANDARD, COMPREHENSIVE levels
  - Add command explanation generation and output formatting
  - _Requirements: 10.1, 10.2, 10.5_

- [x] 4.2 Add educational content for testing and quality tools
  - Create explanations for each testing command (unit, integration, e2e, coverage)
  - Create explanations for each quality command (lint, format, typecheck)
  - Add best practices information and related concepts
  - _Requirements: 10.3, 10.4_

- [x] 4.3 Integrate learning mode with existing tools
  - Update testing and quality tool implementations to support learning mode
  - Add command explanation output when learning mode is enabled
  - Implement different verbosity levels for educational content
  - _Requirements: 10.1, 10.2, 10.5_

- [x] 4.4 Write unit tests for learning mode
  - **NO MOCKING ALLOWED**: Absolutely no `unittest.mock`, `pytest_mock`, `MagicMock`, `patch(`, `monkeypatch`
  - Test learning mode engine functionality and output generation
  - Validate educational content accuracy and formatting
  - Test verbosity level behavior and command explanations
  - Use dependency injection and lightweight fakes for any external dependencies
  - _Requirements: 2.1, 2.2_

- [x] 5. Implement agentic tools for AI-assisted development
  - Create specialized commands for AI workflow support
  - Add batch operation capabilities and structured output formats
  - Implement workflow helpers for common AI development patterns
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

- [x] 5.1 Implement agentic tools category
  - Create `src/ml_playground/tools/categories/agentic.py` with AI workflow commands
  - Add guidelines setup, batch review, and workflow helper commands
  - Implement structured output formats (JSON, YAML) for AI consumption
  - _Requirements: 4.1, 4.2, 4.3_

- [x] 5.2 Add batch operation capabilities
  - Implement batch processing for common development tasks
  - Add support for automated quality checks triggered by AI agents
  - Create workflow templates for AI-assisted development patterns
  - _Requirements: 4.2, 4.4_

- [x] 5.3 Add CLI commands for agentic tools
  - Register agentic commands under `uv run tools agentic` subcommand group
  - Implement extensible command structure for future AI use cases
  - Add comprehensive help and usage examples
  - _Requirements: 1.1, 4.6, 7.1, 7.2_

- [x] 5.4 Write unit tests for agentic tools
  - **NO MOCKING ALLOWED**: Absolutely no `unittest.mock`, `pytest_mock`, `MagicMock`, `patch(`, `monkeypatch`
  - Test AI workflow command functionality and batch operations
  - Validate structured output generation and format compliance
  - Test extensibility mechanisms for future tool additions
  - Use dependency injection and lightweight fakes for any external dependencies
  - _Requirements: 2.1, 2.2_

- [ ] 6. Complete tools module with comprehensive learning mode
  - Finalize all tool categories within the `ml_playground/tools/` module
  - Add comprehensive educational content for all tools
  - Ensure the tools module is fully functional and self-contained
  - _Requirements: 10.3, 10.4_

- [ ] 6.1 Add educational content for environment and CI tools
  - Create explanations for environment management commands
  - Add best practices for CI/CD operations and quality gates
  - Implement cross-references to related development concepts
  - _Requirements: 10.3, 10.4_

- [ ] 6.2 Create comprehensive best practices guides
  - Implement comprehensive best practices documentation within tools module
  - Add learning paths for different user experience levels
  - Create command discovery and help system
  - _Requirements: 10.3, 10.4, 7.1, 7.2_

- [ ] 6.3 Write unit tests for complete tools module
  - **NO MOCKING ALLOWED**: Absolutely no `unittest.mock`, `pytest_mock`, `MagicMock`, `patch(`, `monkeypatch`
  - Test comprehensive educational content accuracy and completeness
  - Validate all tool categories and their interactions
  - Ensure 100% coverage for the entire tools module
  - Use dependency injection and lightweight fakes for any external dependencies
  - _Requirements: 2.1, 2.2_

- [ ] 7. Update CI workflows and documentation (tools module only)
  - Update GitHub Actions workflows to use `uv run tools` commands
  - Update documentation references to new tool commands
  - Keep changes focused on tooling references only
  - _Requirements: 9.1, 9.2, 9.3, 9.4, 11.1, 11.2, 11.4_

- [ ] 7.1 Update CI workflows to use integrated tools
  - Update GitHub Actions workflows to use `uv run tools` commands
  - Replace direct tool invocations with integrated tool system
  - Document exceptions where external tools are still used for speed/features
  - _Requirements: 11.1, 11.2, 11.3, 11.4_

- [ ] 7.2 Update documentation references to tools
  - Update README.md to reflect new `uv run tools` command structure
  - Update all `.dev-guidelines/` files to use new tool commands
  - Update `tools/README.md` to reflect integration into main module
  - _Requirements: 9.1, 9.2, 9.3, 9.4_

- [ ] 7.3 Add timeout philosophy to development guidelines
  - Update `.dev-guidelines/DEVELOPMENT.md` with explicit timeout philosophy
  - Document timeout selection principles and environmental assumptions
  - Add guidelines for timeout configuration and monitoring
  - _Requirements: Timeout philosophy from design_

- [ ] 8. FUTURE: Integrate with ML workflow CLI (DELAYED until after base branch merge)
  - **NOTE: This phase should be delayed until the base branch strict mode work is complete**
  - Refactor existing `cli.py` to use shared ToolResult infrastructure
  - Add learning mode support to prepare, train, sample, analyze commands
  - Integrate shared configuration management patterns
  - _Requirements: 8.1, 8.2, 8.3, 10.6_

- [ ] 8.1 FUTURE: Refactor ML CLI to use ToolResult
  - Update `src/ml_playground/cli.py` to use ToolResult for all operations
  - Implement operation_id generation for ML workflow commands
  - Add consistent error handling using shared error types
  - _Requirements: 8.1, 8.2_

- [ ] 8.2 FUTURE: Add learning mode to ML workflow commands
  - Integrate LearningModeEngine with prepare, train, sample, analyze commands
  - Add educational explanations for ML workflow operations
  - Implement verbosity level support for ML command explanations
  - _Requirements: 10.6_

- [ ] 8.3 FUTURE: Add comprehensive ML workflow explanations
  - Create detailed explanations for prepare, train, sample, analyze operations
  - Add machine learning best practices and software engineering principles
  - Include operations and security considerations for ML workflows
  - _Requirements: 10.3, 10.4_

- [ ] 8.4 FUTURE: Write unit tests for ML CLI integration
  - **NO MOCKING ALLOWED**: Absolutely no `unittest.mock`, `pytest_mock`, `MagicMock`, `patch(`, `monkeypatch`
  - Test ToolResult integration with existing ML workflow commands
  - Validate learning mode functionality for ML operations
  - Test shared configuration management and error handling
  - Use dependency injection and lightweight fakes for any external dependencies
  - _Requirements: 2.1, 2.2_
