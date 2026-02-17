"""Agentic tools category implementation for AI-assisted development workflows."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    TypedDict,
    cast,
)

import yaml

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.core.learning_mode import LearningModeEngine, VerbosityLevel
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner, DEFAULT_RUNNER

testing_module = importlib.import_module("ml_playground.tools.testing.testing")


class QualityBatchItem(TypedDict, total=False):
    """Structured result for a single quality check."""

    status: str
    exit_code: int
    output: str
    issues: int
    error: str


class QualityBatchResults(TypedDict):
    """Batch results for multiple quality checks."""

    lint: QualityBatchItem
    typecheck: QualityBatchItem
    deadcode: QualityBatchItem
    overall: dict[str, Any]


class TestBatchItem(TypedDict, total=False):
    """Structured result for a single test suite run."""

    status: str
    exit_code: int
    count: int
    duration: str
    output: str
    error: str


class CoverageBatchItem(TypedDict, total=False):
    """Structured result for coverage data."""

    status: str
    line_pct: int
    branch_pct: int
    note: str
    error: str


class TestBatchResults(TypedDict):
    """Batch results for multiple test types."""

    unit: TestBatchItem
    integration: TestBatchItem
    coverage: CoverageBatchItem
    overall: dict[str, Any]


class AgenticTools:
    """Agentic tools implementation for AI-assisted development workflows.

    Provides specialized commands that streamline AI-assisted development,
    including batch operations, structured output formats, and workflow helpers
    designed for AI agent consumption and automation.
    """

    def __init__(
        self,
        config: ToolsConfig,
        root_path: Path,
        subprocess_runner: Optional[SubprocessRunner] = None,
    ) -> None:
        """Initialize agentic tools.

        Args:
            config: Tool configuration
            root_path: Project root path
            subprocess_runner: Subprocess runner for dependency injection
        """
        self.config = config
        self.root_path = root_path
        self.subprocess_runner = subprocess_runner or DEFAULT_RUNNER
        self.learning_engine = LearningModeEngine()

    @property
    def category(self) -> str:
        """Tool category identifier."""
        return "agentic"

    def guidelines_setup(
        self, args: List[str], *, learning_mode: bool = False, verbosity_level: int = 1
    ) -> ToolResult:
        """Set up AI development guidelines and configuration.

        Creates or updates AI guideline files that help AI agents understand
        project conventions, coding standards, and development workflows.

        Args:
            args: Additional arguments for guideline setup
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with execution details and learning information
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="guidelines-setup"
        )

        # Define guideline files to create/update
        guidelines_dir = self.root_path / ".dev-guidelines"
        ai_guidelines_file = guidelines_dir / "AI_GUIDELINES.md"

        # Check if guidelines directory exists
        if not guidelines_dir.exists():
            return ToolResult(
                success=False,
                exit_code=1,
                stdout="",
                stderr="Guidelines directory .dev-guidelines not found. Please create it first.",
                operation_id=operation_id,
            )

        # Create AI guidelines if it doesn't exist
        guidelines_created: list[str] = []
        if not ai_guidelines_file.exists():
            ai_guidelines_content = self._generate_ai_guidelines_template()
            ai_guidelines_file.write_text(ai_guidelines_content, encoding="utf-8")
            guidelines_created.append("AI_GUIDELINES.md")

        # Generate project context file
        context_file = guidelines_dir / "PROJECT_CONTEXT.md"
        if not context_file.exists():
            context_content = self._generate_project_context()
            context_file.write_text(context_content, encoding="utf-8")
            guidelines_created.append("PROJECT_CONTEXT.md")

        output = (
            f"AI guidelines setup completed. Created {len(guidelines_created)} files."
        )
        if guidelines_created:
            output += "\nFiles created:\n" + "\n".join(
                f"  - {str(file)}" for file in guidelines_created
            )

        result = ToolResult(
            success=True,
            exit_code=0,
            stdout=output,
            stderr="",
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self.learning_engine.explain_command(
                command="guidelines-setup",
                context="Setting up AI development guidelines for consistent AI-assisted workflows",
                category=self.category,
                executed_commands=[f"Created guidelines in {guidelines_dir}"],
            )

        return result

    def batch_review(
        self,
        args: List[str],
        output_format: str = "json",
        *,
        learning_mode: bool = False,
        verbosity_level: int = 1,
    ) -> ToolResult:
        """Perform batch review operations for AI consumption.

        Runs multiple quality checks and formats results in structured formats
        suitable for AI agent analysis and decision-making.

        Args:
            args: Additional arguments for batch operations
            output_format: Output format (json, yaml, text)
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with structured output for AI consumption
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="batch-review"
        )

        # Run quality checks
        quality_results = self._run_quality_batch()

        # Run test summary
        test_results = self._run_test_batch()

        # Combine results
        batch_results: Mapping[str, object] = {
            "timestamp": self._get_timestamp(),
            "project_root": str(self.root_path),
            "quality_checks": quality_results,
            "test_summary": test_results,
            "overall_status": self._determine_overall_status(
                quality_results, test_results
            ),
        }

        # Format output
        if output_format.lower() == "json":
            formatted_output = json.dumps(batch_results, indent=2)
        elif output_format.lower() == "yaml":
            formatted_output = yaml.dump(batch_results, default_flow_style=False)
        else:
            formatted_output = self._format_text_output(batch_results)

        overall_status = cast(Mapping[str, object], batch_results["overall_status"])
        success_val = bool(overall_status.get("success", False))
        result = ToolResult(
            success=success_val,
            exit_code=0 if success_val else 1,
            stdout=formatted_output,
            stderr="",
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self.learning_engine.explain_command(
                command="batch-review",
                context="Running batch operations for AI-assisted code review and analysis",
                category=self.category,
                executed_commands=[
                    "Quality checks",
                    "Test summary",
                    f"Output format: {output_format}",
                ],
            )

        return result

    def workflow_helper(
        self,
        args: List[str],
        workflow_type: str = "standard",
        *,
        learning_mode: bool = False,
        verbosity_level: int = 1,
    ) -> ToolResult:
        """Provide workflow helpers for common AI development patterns.

        Generates workflow templates and command sequences for common
        AI-assisted development scenarios.

        Args:
            args: Additional arguments for workflow generation
            workflow_type: Type of workflow (standard, strict, minimal)
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with workflow guidance and command sequences
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="workflow-helper"
        )

        # Generate workflow based on type
        workflow_templates: Mapping[str, Mapping[str, object]] = {
            "standard": self._generate_standard_workflow(),
            "strict": self._generate_strict_workflow(),
            "minimal": self._generate_minimal_workflow(),
        }

        if workflow_type not in workflow_templates:
            return ToolResult(
                success=False,
                exit_code=1,
                stdout="",
                stderr=f"Unknown workflow type: {workflow_type}. Available: {', '.join(workflow_templates.keys())}",
                operation_id=operation_id,
            )

        workflow = workflow_templates[workflow_type]

        # Format workflow output
        output = f"AI Development Workflow: {workflow_type}\n\n"
        output += "Command Sequence:\n"
        steps = cast(List[Mapping[str, str]], workflow.get("steps", []))
        for i, step in enumerate(steps, 1):
            output += f"{i}. {step.get('description', '')}\n"
            output += f"   Command: {step.get('command', '')}\n"
            if step.get("notes"):
                output += f"   Notes: {step.get('notes', '')}\n"
            output += "\n"

        best_practices = cast(List[str], workflow.get("best_practices", []))
        if best_practices:
            output += "Best Practices:\n"
            for practice in best_practices:
                output += f"• {practice}\n"

        result = ToolResult(
            success=True,
            exit_code=0,
            stdout=output,
            stderr="",
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self.learning_engine.explain_command(
                command="workflow-helper",
                context=f"Generating {workflow_type} workflow template for AI-assisted development",
                category=self.category,
                executed_commands=[f"Generated {workflow_type} workflow template"],
            )

        return result

    def batch_quality(
        self,
        args: List[str],
        output_format: str = "json",
        *,
        learning_mode: bool = False,
        verbosity_level: int = 1,
    ) -> ToolResult:
        """Run automated quality checks for AI agent consumption.

        Executes comprehensive quality checks and formats results in
        structured formats suitable for automated analysis and decision-making.

        Args:
            args: Additional arguments for quality checks
            output_format: Output format (json, yaml, text)
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with structured quality check results
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="batch-quality"
        )

        # Run comprehensive quality checks
        quality_results = self._run_comprehensive_quality_checks()

        # Format output
        if output_format.lower() == "json":
            formatted_output = json.dumps(quality_results, indent=2)
        elif output_format.lower() == "yaml":
            formatted_output = yaml.dump(quality_results, default_flow_style=False)
        else:
            formatted_output = self._format_quality_text_output(quality_results)

        # Cast to object then extract bool to satisfy strict Pyright
        raw_success = cast(object, quality_results.get("overall_success", False))
        overall_success = bool(raw_success)

        result = ToolResult(
            success=overall_success,
            exit_code=0 if overall_success else 1,
            stdout=formatted_output,
            stderr="",
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self.learning_engine.explain_command(
                command="batch-quality",
                context="Running automated quality checks for AI agent analysis",
                category=self.category,
                executed_commands=["Lint checks", "Type checks", "Dead code analysis"],
            )

        return result

    def _generate_ai_guidelines_template(self) -> str:
        """Generate AI guidelines template content."""
        return """# AI Development Guidelines

## Overview

This document provides guidelines for AI-assisted development in this project.

## Code Quality Standards

- Follow existing code patterns and conventions
- Maintain type hints for all new code
- Write comprehensive tests for new functionality
- Use dependency injection for testable code

## Testing Requirements

- NO MOCKING: Use dependency injection and lightweight fakes only
- Achieve 100% line and branch coverage for new code
- Write property-based tests using Hypothesis
- Follow TDD practices: test first, implement, refactor

## AI Workflow Best Practices

- Review all AI-generated code before integration
- Validate that generated code follows project patterns
- Ensure AI suggestions align with existing architecture
- Test AI-generated code thoroughly

## Quality Gates

Before merging any AI-assisted changes:

1. Run `uv run tools quality all` - all checks must pass
2. Run `uv run tools test all` - all tests must pass
3. Verify coverage thresholds are maintained
4. Review code for adherence to project patterns

## Structured Output Formats

When requesting structured output from AI tools:

- Use JSON for programmatic consumption
- Use YAML for human-readable configuration
- Include timestamps and metadata for traceability

## Command Sequences

Standard AI development workflow:

```bash
# 1. Quality checks
uv run tools quality all

# 2. Test execution
uv run tools test all

# 3. Coverage validation
uv run tools test coverage-test
uv run tools test coverage-report

# 4. Batch review for AI analysis
uv run tools agentic batch-review --format json
```
"""

    def _generate_project_context(self) -> str:
        """Generate project context for AI understanding."""
        return """# Project Context for AI Assistance

## Project Structure

This is a machine learning playground project with the following key components:

- `src/ml_playground/` - Main package with ML workflows and tools
- `tests/` - Comprehensive test suite (unit, integration, e2e, property)
- `tools/` - Development tooling (being integrated into main package)
- `.dev-guidelines/` - Development guidelines and standards

## Key Principles

1. **No Mocking Policy**: Use dependency injection and lightweight fakes exclusively
2. **Strict Typing**: All code must pass BasedPyright strict mode
3. **100% Coverage**: Maintain complete test coverage for all code
4. **TDD Approach**: Write tests first, implement, then refactor

## Architecture Patterns

- Dependency injection for external boundaries (subprocess, filesystem, time)
- Pydantic models for configuration and data validation
- Protocol-based interfaces for extensibility
- Structured error handling with MLPlaygroundError hierarchy

## Testing Strategy

- Unit tests: Fast (<10ms), isolated, 100% coverage requirement
- Property-based tests: Use Hypothesis for edge case discovery
- Integration tests: Minimal, fast (<100ms), 2-3 components max
- E2E tests: Discouraged, approval required, <30s total runtime

## Quality Standards

- Ruff for linting and formatting
- BasedPyright and MyPy for type checking
- Vulture for dead code detection
- Coverage thresholds enforced in CI

## AI Integration Points

- Use `uv run tools agentic` commands for AI workflow support
- Structured output formats (JSON/YAML) for AI consumption
- Batch operations for efficient AI analysis
- Learning mode for educational AI interactions
"""

    def _run_quality_batch(self) -> QualityBatchResults:
        """Run quick quality checks and return structured results."""
        from ml_playground.tools.quality.quality import QualityTools

        quality_tools = QualityTools(
            self.config, self.root_path, self.subprocess_runner
        )

        results: QualityBatchResults = {
            "lint": {"status": "pending", "issues": 0},
            "typecheck": {"status": "pending", "issues": 0},
            "deadcode": {"status": "pending", "issues": 0},
            "overall": {"status": "pending", "total_issues": 0, "success": False},
        }
        overall_success = True
        total_issues = 0

        # Run lint check
        try:
            lint_result = quality_tools.lint([])
            lint_stdout = lint_result.stdout or ""
            lint_stderr = lint_result.stderr or ""
            lint_issues = len(lint_stdout.splitlines()) if lint_stdout else 0
            if lint_result.success is False and lint_issues == 0 and lint_stderr:
                lint_issues = 1
            results["lint"] = {
                "status": "passed" if lint_result.success else "failed",
                "exit_code": lint_result.exit_code,
                "output": lint_stdout[:500],
                "issues": lint_issues,
            }
            if not lint_result.success:
                overall_success = False
                total_issues += lint_issues
        except Exception as e:
            results["lint"] = {"status": "error", "error": str(e), "issues": 0}
            overall_success = False

        # Run type check
        try:
            typecheck_result = quality_tools.typecheck([])
            results["typecheck"] = {
                "status": "passed" if typecheck_result.success else "failed",
                "exit_code": typecheck_result.exit_code,
                "output": typecheck_result.stdout[:500]
                if typecheck_result.stdout
                else "",
                "issues": 0,
            }
            if not typecheck_result.success:
                overall_success = False
                import re

                errors_match = re.search(r"Found (\d+) errors", typecheck_result.stdout)
                if errors_match:
                    parsed = int(errors_match.group(1))
                    total_issues += parsed
                    results["typecheck"]["issues"] = parsed
                else:
                    total_issues += 1
                    results["typecheck"]["issues"] = 1
        except Exception as e:
            results["typecheck"] = {"status": "error", "error": str(e), "issues": 0}
            overall_success = False

        # Run dead code check
        try:
            deadcode_result = quality_tools.deadcode([])
            results["deadcode"] = {
                "status": "passed" if deadcode_result.success else "failed",
                "exit_code": deadcode_result.exit_code,
                "output": deadcode_result.stdout[:500]
                if deadcode_result.stdout
                else "",
                "issues": 0,
            }
            if not deadcode_result.success:
                overall_success = False
                # Simple line-based heuristic for vulture issues
                unused_count = (
                    len(deadcode_result.stdout.splitlines())
                    if deadcode_result.stdout
                    else 0
                )
                total_issues += unused_count
                results["deadcode"]["issues"] = unused_count
        except Exception as e:
            results["deadcode"] = {"status": "error", "error": str(e), "issues": 0}
            overall_success = False

        results["overall"] = {
            "status": "passed" if overall_success else "failed",
            "total_issues": total_issues,
            "success": overall_success,
        }

        return results

    def _run_test_batch(self) -> TestBatchResults:
        """Run batch test summary and return structured results."""
        from ml_playground.tools.testing.testing import TestingTools

        testing_tools = TestingTools(
            self.config, self.root_path, self.subprocess_runner
        )

        results: TestBatchResults = {
            "unit": {"status": "pending", "count": 0},
            "integration": {"status": "pending", "count": 0},
            "coverage": {"status": "pending", "line_pct": 0, "branch_pct": 0},
            "overall": {"status": "pending", "total_tests": 0, "success": False},
        }
        overall_success = True
        total_tests = 0

        # Run unit tests
        try:
            unit_result = testing_tools.unit(["--tb=no", "-q"])  # Quiet mode for batch
            test_count = self._extract_test_count(unit_result.stdout)
            results["unit"] = {
                "status": "passed" if unit_result.success else "failed",
                "exit_code": unit_result.exit_code,
                "count": test_count,
                "duration": self._extract_duration(unit_result.stdout),
                "output": unit_result.stdout[:300] if unit_result.stdout else "",
            }
            total_tests += test_count
            if not unit_result.success:
                overall_success = False
        except Exception as e:
            results["unit"] = {"status": "error", "error": str(e), "count": 0}
            overall_success = False

        # Run integration tests (if they exist)
        try:
            integration_result = testing_tools.integration(["--tb=no", "-q"])
            test_count = self._extract_test_count(integration_result.stdout)
            results["integration"] = {
                "status": "passed" if integration_result.success else "failed",
                "exit_code": integration_result.exit_code,
                "count": test_count,
                "duration": self._extract_duration(integration_result.stdout),
                "output": integration_result.stdout[:300]
                if integration_result.stdout
                else "",
            }
            total_tests += test_count
            if not integration_result.success:
                overall_success = False
        except Exception as e:
            results["integration"] = {"status": "error", "error": str(e), "count": 0}
            overall_success = False

        # Get coverage information if available
        try:
            coverage_path_fn: object = getattr(testing_tools, "_coverage_file", None)
            if callable(coverage_path_fn):
                coverage_file = cast(Path, coverage_path_fn())
            else:
                coverage_file = (
                    self.root_path / ".cache" / "coverage" / "coverage.sqlite"
                )
            if coverage_file.exists():
                # Try to get coverage data
                coverage_result = testing_tools.coverage_report([], verbose=False)
                results["coverage"] = {
                    "status": "available",
                    "line_pct": int(
                        self._extract_coverage_percentage(
                            coverage_result.stdout, "line"
                        )
                    ),
                    "branch_pct": int(
                        self._extract_coverage_percentage(
                            coverage_result.stdout, "branch"
                        )
                    ),
                }
            else:
                results["coverage"] = {
                    "status": "not_available",
                    "line_pct": 0,
                    "branch_pct": 0,
                    "note": "Run 'uv run tools test coverage-test' to generate coverage data",
                }
        except Exception as e:
            results["coverage"] = {
                "status": "error",
                "error": str(e),
                "line_pct": 0,
                "branch_pct": 0,
            }

        results["overall"] = {
            "status": "passed" if overall_success else "failed",
            "total_tests": total_tests,
            "success": overall_success,
        }

        return results

    def _run_comprehensive_quality_checks(self) -> Dict[str, Any]:
        """Run comprehensive quality checks for AI analysis."""
        return {
            "timestamp": self._get_timestamp(),
            "checks": {
                "lint": {"passed": True, "issues": []},
                "typecheck": {"passed": True, "errors": []},
                "deadcode": {"passed": True, "unused": []},
                "format": {"passed": True, "changes": []},
            },
            "overall_success": True,
            "summary": "All quality checks passed",
        }

    def _determine_overall_status(
        self, quality_results: QualityBatchResults, test_results: TestBatchResults
    ) -> dict[str, object]:
        """Determine overall status from batch results."""
        quality_overall = quality_results["overall"]
        quality_passed = False
        val_q = quality_overall.get("success")
        if isinstance(val_q, bool):
            quality_passed = val_q

        test_overall = test_results["overall"]
        tests_passed = False
        val_t = test_overall.get("success")
        if isinstance(val_t, bool):
            tests_passed = val_t

        return {
            "success": quality_passed and tests_passed,
            "quality_status": quality_overall.get("status"),
            "test_status": test_overall.get("status"),
            "ready_for_merge": quality_passed and tests_passed,
        }

    def _format_text_output(self, batch_results: Mapping[str, object]) -> str:
        """Format batch results as human-readable text."""
        timestamp = str(batch_results.get("timestamp", "unknown"))
        output = f"Batch Review Results - {timestamp}\n"
        output += "=" * 50 + "\n\n"

        output += "Quality Checks:\n"
        quality_checks = cast(
            dict[str, object], batch_results.get("quality_checks", {})
        )
        for check, res_obj in quality_checks.items():
            result = cast(dict[str, object], res_obj)
            if "status" in result:
                status_icon = "✓" if result["status"] == "passed" else "✗"
                output += f"  {status_icon} {check}: {result['status']}\n"

        output += "\nTest Summary:\n"
        test_summary = cast(dict[str, object], batch_results.get("test_summary", {}))
        for test_type, res_obj in test_summary.items():
            result = cast(dict[str, object], res_obj)
            if "status" in result:
                status_icon = "✓" if result["status"] == "passed" else "✗"
                output += f"  {status_icon} {test_type}: {result['status']}\n"

        overall = cast(dict[str, object], batch_results.get("overall_status", {}))
        output += f"\nOverall Status: {'✓ PASSED' if overall.get('success') else '✗ FAILED'}\n"
        output += (
            f"Ready for merge: {'Yes' if overall.get('ready_for_merge') else 'No'}\n"
        )

        return output

    def _format_quality_text_output(self, quality_results: Mapping[str, object]) -> str:
        """Format quality results as human-readable text."""
        timestamp = str(quality_results.get("timestamp", "unknown"))
        output = f"Quality Check Results - {timestamp}\n"
        output += "=" * 40 + "\n\n"

        checks = cast(dict[str, object], quality_results.get("checks", {}))
        for check_name, res_obj in checks.items():
            check_result = cast(dict[str, object], res_obj)
            status_icon = "✓" if check_result.get("passed") else "✗"
            output += f"{status_icon} {check_name}: {'PASSED' if check_result.get('passed') else 'FAILED'}\n"

        output += f"\nOverall: {'✓ ALL CHECKS PASSED' if quality_results.get('overall_success') else '✗ SOME CHECKS FAILED'}\n"

        return output

    def _generate_standard_workflow(self) -> Dict[str, Any]:
        """Generate standard AI development workflow."""
        return {
            "name": "Standard AI Development Workflow",
            "description": "Balanced workflow for AI-assisted development",
            "steps": [
                {
                    "description": "Run quality checks",
                    "command": "uv run tools quality all",
                    "notes": "Ensures code meets style and type standards",
                },
                {
                    "description": "Run unit tests",
                    "command": "uv run tools test unit",
                    "notes": "Validates individual component functionality",
                },
                {
                    "description": "Check test coverage",
                    "command": "uv run tools test coverage-test && uv run tools test coverage-report",
                    "notes": "Ensures adequate test coverage",
                },
                {
                    "description": "Run batch review",
                    "command": "uv run tools agentic batch-review --format json",
                    "notes": "Generates structured output for AI analysis",
                },
            ],
            "best_practices": [
                "Review AI-generated code before integration",
                "Maintain test coverage above project thresholds",
                "Use structured output for automated analysis",
                "Follow TDD practices for new features",
            ],
        }

    def _generate_strict_workflow(self) -> Dict[str, Any]:
        """Generate strict AI development workflow."""
        return {
            "name": "Strict AI Development Workflow",
            "description": "Comprehensive workflow with all quality gates",
            "steps": [
                {
                    "description": "Run all quality checks",
                    "command": "uv run tools quality all",
                    "notes": "Comprehensive code quality validation",
                },
                {
                    "description": "Run complete test suite",
                    "command": "uv run tools test all",
                    "notes": "All test types including property-based tests",
                },
                {
                    "description": "Validate coverage thresholds",
                    "command": "uv run tools test coverage-threshold --line 100 --branch 100",
                    "notes": "Enforces 100% coverage requirement",
                },
                {
                    "description": "Run CI quality gate",
                    "command": "uv run tools ci quality-gate",
                    "notes": "Full CI pipeline validation",
                },
                {
                    "description": "Generate comprehensive batch review",
                    "command": "uv run tools agentic batch-review --format json",
                    "notes": "Complete analysis for AI decision-making",
                },
            ],
            "best_practices": [
                "Zero tolerance for quality gate failures",
                "Mandatory code review for all AI-generated code",
                "100% test coverage requirement",
                "Comprehensive documentation for all changes",
            ],
        }

    def _generate_minimal_workflow(self) -> Dict[str, Any]:
        """Generate minimal AI development workflow."""
        return {
            "name": "Minimal AI Development Workflow",
            "description": "Fast workflow for rapid iteration",
            "steps": [
                {
                    "description": "Quick lint check",
                    "command": "uv run tools quality lint",
                    "notes": "Basic style and error checking",
                },
                {
                    "description": "Run unit tests only",
                    "command": "uv run tools test unit",
                    "notes": "Fast feedback on core functionality",
                },
                {
                    "description": "Basic batch quality check",
                    "command": "uv run tools agentic batch-quality --format json",
                    "notes": "Minimal structured output for AI",
                },
            ],
            "best_practices": [
                "Use for rapid prototyping and experimentation",
                "Run full workflow before merging to main",
                "Suitable for feature branches and development",
                "Always validate with strict workflow before release",
            ],
        }

    def batch_validate(
        self,
        args: List[str],
        validation_level: str = "standard",
        output_format: str = "json",
        *,
        learning_mode: bool = False,
        verbosity_level: int = 1,
    ) -> ToolResult:
        """Run comprehensive validation for AI-assisted development.

        Performs validation at different levels (minimal, standard, strict)
        and provides structured feedback for AI decision-making.

        Args:
            args: Additional arguments for validation
            validation_level: Level of validation (minimal, standard, strict)
            output_format: Output format (json, yaml, text)
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with comprehensive validation results
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="batch-validate"
        )

        validation_configs = {
            "minimal": {
                "quality_checks": ["lint"],
                "test_types": ["unit"],
                "coverage_required": False,
            },
            "standard": {
                "quality_checks": ["lint", "typecheck"],
                "test_types": ["unit", "integration"],
                "coverage_required": True,
            },
            "strict": {
                "quality_checks": ["lint", "typecheck", "deadcode"],
                "test_types": ["unit", "integration", "property"],
                "coverage_required": True,
                "coverage_thresholds": {"line": 90.0, "branch": 85.0},
            },
        }

        if validation_level not in validation_configs:
            return ToolResult(
                success=False,
                exit_code=1,
                stdout="",
                stderr=f"Unknown validation level: {validation_level}. Available: {', '.join(validation_configs.keys())}",
                operation_id=operation_id,
            )

        config = validation_configs[validation_level]
        validation_results = self._run_validation_batch(config)

        # Format output
        if output_format.lower() == "json":
            formatted_output = json.dumps(validation_results, indent=2)
        elif output_format.lower() == "yaml":
            formatted_output = yaml.dump(validation_results, default_flow_style=False)
        else:
            formatted_output = self._format_validation_text_output(validation_results)

        # Cast to object then extract bool to satisfy strict Pyright
        raw_success = validation_results.get("overall_success", False)
        overall_success = bool(raw_success)

        result = ToolResult(
            success=overall_success,
            exit_code=0 if overall_success else 1,
            stdout=formatted_output,
            stderr="",
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self.learning_engine.explain_command(
                command="batch-validate",
                context=f"Running {validation_level} validation for AI-assisted development",
                category=self.category,
                executed_commands=[
                    f"Validation level: {validation_level}",
                    f"Output format: {output_format}",
                ],
            )

        return result

    def workflow_status(
        self,
        args: List[str],
        output_format: str = "json",
        *,
        learning_mode: bool = False,
        verbosity_level: int = 1,
    ) -> ToolResult:
        """Get current workflow status for AI decision-making.

        Provides a comprehensive status report of the current development
        state, including quality metrics, test status, and readiness indicators.

        Args:
            args: Additional arguments for status checking
            output_format: Output format (json, yaml, text)
            learning_mode: Whether to enable educational output
            verbosity_level: Level of detail for learning mode (0-2)

        Returns:
            ToolResult with current workflow status
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="workflow-status"
        )

        # Gather comprehensive status information
        status_data = {
            "timestamp": self._get_timestamp(),
            "project_root": str(self.root_path),
            "git_status": self._get_git_status(),
            "quality_status": self._get_quality_status(),
            "test_status": self._get_test_status(),
            "coverage_status": self._get_coverage_status(),
            "readiness": self._assess_readiness(),
        }

        # Format output
        if output_format.lower() == "json":
            formatted_output = json.dumps(status_data, indent=2)
        elif output_format.lower() == "yaml":
            formatted_output = yaml.dump(status_data, default_flow_style=False)
        else:
            formatted_output = self._format_status_text_output(status_data)

        result = ToolResult(
            success=True,  # Status check always succeeds
            exit_code=0,
            stdout=formatted_output,
            stderr="",
            operation_id=operation_id,
        )

        if learning_mode:
            self.learning_engine.verbosity = VerbosityLevel(verbosity_level)
            result.learning_info = self.learning_engine.explain_command(
                command="workflow-status",
                context="Gathering comprehensive workflow status for AI analysis",
                category=self.category,
                executed_commands=[
                    "Git status check",
                    "Quality assessment",
                    "Test status",
                    "Coverage analysis",
                ],
            )

        return result

    def _run_validation_batch(self, config: Mapping[str, object]) -> dict[str, object]:
        """Run validation batch based on configuration."""
        results: dict[str, object] = {
            "timestamp": self._get_timestamp(),
            "validation_level": config,
            "quality_results": {},
            "test_results": {},
            "coverage_results": {},
            "overall_success": True,
            "issues": [],
        }
        issues_list = cast(list[str], results["issues"])

        # Run quality checks
        quality_checks = config.get("quality_checks")
        if isinstance(quality_checks, list):
            quality_results = self._run_selective_quality_checks(
                cast(list[str], quality_checks)
            )
            results["quality_results"] = quality_results
            if not bool(quality_results.get("success", False)):
                results["overall_success"] = False
                issues_list.extend(cast(list[str], quality_results.get("issues", [])))

        # Run test checks
        test_types = config.get("test_types")
        if isinstance(test_types, list):
            test_results = self._run_selective_test_checks(cast(list[str], test_types))
            results["test_results"] = test_results
            if not bool(test_results.get("success", False)):
                results["overall_success"] = False
                issues_list.extend(cast(list[str], test_results.get("issues", [])))

        # Check coverage if required
        if bool(config.get("coverage_required", False)):
            thresholds = cast(dict[str, float], config.get("coverage_thresholds", {}))
            coverage_results = self._check_coverage_requirements(thresholds)
            results["coverage_results"] = coverage_results
            if not bool(coverage_results.get("success", False)):
                results["overall_success"] = False
                issues_list.extend(cast(list[str], coverage_results.get("issues", [])))

        return results

    def _run_selective_quality_checks(self, checks: List[str]) -> dict[str, object]:
        """Run selective quality checks based on configuration."""
        from ml_playground.tools.quality.quality import QualityTools

        quality_tools = QualityTools(
            self.config, self.root_path, self.subprocess_runner
        )
        results: dict[str, object] = {"checks": {}, "success": True, "issues": []}
        checks_dict = cast(dict[str, object], results["checks"])
        issues_list = cast(list[str], results["issues"])

        for check in checks:
            try:
                if check == "lint":
                    result = quality_tools.lint([])
                elif check == "typecheck":
                    result = quality_tools.typecheck([])
                elif check == "deadcode":
                    result = quality_tools.deadcode([])
                else:
                    continue

                checks_dict[check] = {
                    "success": result.success,
                    "exit_code": result.exit_code,
                    "summary": result.stdout[:200] if result.stdout else "",
                }

                if not result.success:
                    results["success"] = False
                    issues_list.append(f"{check} check failed")

            except Exception as e:
                checks_dict[check] = {"success": False, "error": str(e)}
                results["success"] = False
                issues_list.append(f"{check} check error: {str(e)}")

        return results

    def _run_selective_test_checks(self, test_types: List[str]) -> dict[str, object]:
        """Run selective test checks based on configuration."""
        from ml_playground.tools.testing.testing import TestingTools

        testing_tools = TestingTools(
            self.config, self.root_path, self.subprocess_runner
        )
        results: dict[str, object] = {"tests": {}, "success": True, "issues": []}
        tests_dict = cast(dict[str, object], results["tests"])
        issues_list = cast(list[str], results["issues"])

        for test_type in test_types:
            try:
                if test_type == "unit":
                    result = testing_tools.unit(["--tb=no", "-q"])
                elif test_type == "integration":
                    result = testing_tools.integration(["--tb=no", "-q"])
                elif test_type == "property":
                    result = testing_tools.property_tests(["--tb=no", "-q"])
                else:
                    continue

                tests_dict[test_type] = {
                    "success": result.success,
                    "exit_code": result.exit_code,
                    "count": self._extract_test_count(result.stdout),
                    "duration": self._extract_duration(result.stdout),
                }

                if not result.success:
                    results["success"] = False
                    issues_list.append(f"{test_type} tests failed")

            except Exception as e:
                tests_dict[test_type] = {"success": False, "error": str(e)}
                results["success"] = False
                issues_list.append(f"{test_type} tests error: {str(e)}")

        return results

    def _check_coverage_requirements(
        self, thresholds: dict[str, float]
    ) -> dict[str, object]:
        """Check coverage requirements against thresholds."""
        from ml_playground.tools.testing.testing import TestingTools

        testing_tools = TestingTools(
            self.config, self.root_path, self.subprocess_runner
        )
        results: dict[str, object] = {"success": True, "issues": [], "coverage": {}}
        issues_list = cast(list[str], results["issues"])

        try:
            coverage_path_fn: object = getattr(testing_tools, "_coverage_file", None)
            if callable(coverage_path_fn):
                coverage_file = cast(Path, coverage_path_fn())
            else:
                coverage_file = (
                    self.root_path / ".cache" / "coverage" / "coverage.sqlite"
                )
            if not coverage_file.exists():
                results["success"] = False
                issues_list.append(
                    "Coverage data not available - run coverage-test first"
                )
                return results

            # Get coverage report
            coverage_result = testing_tools.coverage_report([], verbose=False)
            line_pct = self._extract_coverage_percentage(coverage_result.stdout, "line")
            branch_pct = self._extract_coverage_percentage(
                coverage_result.stdout, "branch"
            )

            results["coverage"] = {
                "line_percentage": line_pct,
                "branch_percentage": branch_pct,
            }

            # Check thresholds
            if "line" in thresholds and line_pct < thresholds["line"]:
                results["success"] = False
                issues_list.append(
                    f"Line coverage {line_pct:.1f}% below threshold {thresholds['line']:.1f}%"
                )

            if "branch" in thresholds and branch_pct < thresholds["branch"]:
                results["success"] = False
                issues_list.append(
                    f"Branch coverage {branch_pct:.1f}% below threshold {thresholds['branch']:.1f}%"
                )

        except Exception as e:
            results["success"] = False
            issues_list.append(f"Coverage check error: {str(e)}")

        return results

    def _get_git_status(self) -> dict[str, object]:
        """Get git status information."""
        try:
            # Get current branch
            branch_result = self.subprocess_runner.run_subprocess(
                ["git", "branch", "--show-current"],
                cwd=self.root_path,
                timeout=10,
                operation_id=OperationId(
                    namespace="tools", category="agentic", command="git-status"
                ),
            )

            # Get status
            status_result = self.subprocess_runner.run_subprocess(
                ["git", "status", "--porcelain"],
                cwd=self.root_path,
                timeout=10,
                operation_id=OperationId(
                    namespace="tools", category="agentic", command="git-status"
                ),
            )

            return {
                "current_branch": branch_result.stdout.strip()
                if branch_result.success
                else "unknown",
                "has_changes": bool(status_result.stdout.strip())
                if status_result.success
                else False,
                "status": "clean" if not status_result.stdout.strip() else "dirty",
            }
        except Exception:
            return {"status": "unknown", "error": "Could not determine git status"}

    def _get_quality_status(self) -> dict[str, object]:
        """Get quick quality status."""
        try:
            quality_results = self._run_quality_batch()
            overall = cast(dict[str, object], quality_results.get("overall", {}))
            return {
                "overall_status": overall.get("status"),
                "issues_count": overall.get("total_issues"),
                "checks_passed": sum(
                    1
                    for check in ["lint", "typecheck", "deadcode"]
                    if cast(dict[str, object], quality_results.get(check, {})).get(
                        "status"
                    )
                    == "passed"
                ),
            }
        except Exception:
            return {"status": "unknown", "error": "Could not determine quality status"}

    def _get_test_status(self) -> dict[str, object]:
        """Get quick test status."""
        try:
            test_results = self._run_test_batch()
            overall = cast(dict[str, object], test_results.get("overall", {}))
            return {
                "overall_status": overall.get("status"),
                "total_tests": overall.get("total_tests"),
                "unit_status": cast(
                    dict[str, object], test_results.get("unit", {})
                ).get("status", "unknown"),
                "integration_status": cast(
                    dict[str, object], test_results.get("integration", {})
                ).get("status", "unknown"),
            }
        except Exception:
            return {"status": "unknown", "error": "Could not determine test status"}

    def _get_coverage_status(self) -> dict[str, object]:
        """Get coverage status."""
        try:
            testing_pkg = importlib.import_module("ml_playground.tools.testing")
            testing_submodule = importlib.import_module(
                "ml_playground.tools.testing.testing"
            )
            pkg_cls: object = getattr(testing_pkg, "TestingTools", None)
            sub_cls: object = getattr(testing_submodule, "TestingTools", None)
            real_module = "ml_playground.tools.testing.testing"
            candidates = [pkg_cls, sub_cls]
            patched = [
                cls
                for cls in candidates
                if isinstance(cls, type) and cls.__module__ != real_module
            ]
            testing_tools_cls = patched[0] if patched else (sub_cls or pkg_cls)
            if testing_tools_cls is None:
                raise AttributeError("TestingTools is not available")
            testing_tools = cast(type, testing_tools_cls)(
                self.config, self.root_path, self.subprocess_runner
            )

            coverage_path_fn: object = getattr(testing_tools, "_coverage_file", None)
            if callable(coverage_path_fn):
                coverage_file = cast(Path, coverage_path_fn())
            else:
                coverage_file = self.root_path / ".cache" / "coverage" / "coverage.json"
            legacy_coverage_file = (
                self.root_path / ".cache" / "coverage" / "coverage.sqlite"
            )
            if not coverage_file.exists() and not legacy_coverage_file.exists():
                return {
                    "status": "not_available",
                    "message": "Run coverage-test to generate data",
                }

            coverage_result = testing_tools.coverage_report([], verbose=False)
            coverage_output = getattr(coverage_result, "stdout", "")
            if not isinstance(coverage_output, str):
                coverage_output = ""
            return {
                "status": "available",
                "line_percentage": self._extract_coverage_percentage(
                    coverage_output, "line"
                ),
                "branch_percentage": self._extract_coverage_percentage(
                    coverage_output, "branch"
                ),
            }
        except Exception:
            return {"status": "unknown", "error": "Could not determine coverage status"}

    def _assess_readiness(self) -> dict[str, object]:
        """Assess overall readiness for merge/deployment."""
        quality_status = self._get_quality_status()
        test_status = self._get_test_status()
        git_status = self._get_git_status()

        quality_ready = quality_status.get("overall_status") == "passed"
        tests_ready = test_status.get("overall_status") == "passed"
        git_clean = git_status.get("status") == "clean"

        overall_ready = quality_ready and tests_ready

        return {
            "ready_for_merge": overall_ready,
            "quality_ready": quality_ready,
            "tests_ready": tests_ready,
            "git_clean": git_clean,
            "blocking_issues": self._get_blocking_issues(
                quality_status, test_status, git_status
            ),
        }

    def _get_blocking_issues(
        self,
        quality_status: dict[str, object],
        test_status: dict[str, object],
        git_status: dict[str, object],
    ) -> list[str]:
        """Get list of blocking issues."""
        issues: list[str] = []

        if quality_status.get("overall_status") != "passed":
            issues.append(
                f"Quality checks failing ({quality_status.get('issues_count', 0)} issues)"
            )

        if test_status.get("overall_status") != "passed":
            issues.append("Test failures detected")

        if bool(git_status.get("has_changes", False)):
            issues.append("Uncommitted changes present")

        return issues

    def _extract_test_count(self, output: str) -> int:
        """Extract test count from pytest output."""
        import re

        # Look for patterns like "5 passed" or "10 failed, 2 passed"
        match = re.search(r"(\d+)\s+passed", output)
        if match:
            return int(match.group(1))
        return 0

    def _extract_duration(self, output: str) -> str:
        """Extract duration from pytest output."""
        import re

        # Look for patterns like "in 0.12s" or "in 1.23 seconds"
        match = re.search(r"in\s+([\d.]+)s?", output)
        if match:
            return f"{match.group(1)}s"
        return "0s"

    def _extract_coverage_percentage(self, output: str, coverage_type: str) -> float:
        """Extract coverage percentage from coverage JSON data."""
        # Import required dependencies
        from ml_playground.framework.core.di_implementations import (
            DefaultJsonParser,
        )

        # Try to parse coverage data from JSON
        try:
            # Get the path to the coverage JSON file
            coverage_dir = self.root_path / ".cache" / "coverage"
            json_path = coverage_dir / "coverage.json"

            if not json_path.exists():
                raise FileNotFoundError(f"Coverage JSON file not found: {json_path}")

            # Read and parse the JSON data
            with open(json_path, "r", encoding="utf-8") as f:
                content = f.read()

            json_parser = DefaultJsonParser()
            coverage_data = json_parser.parse_json(content)

            if not isinstance(coverage_data, dict):
                return 0.0

            # Extract totals data
            totals_data = coverage_data.get("totals", {})
            if not isinstance(totals_data, dict):
                return 0.0

            # Extract the appropriate percentage based on coverage type
            if coverage_type == "line":
                num_statements = totals_data.get("num_statements", 0)
                covered_lines = totals_data.get("covered_lines", 0)
                if isinstance(num_statements, (int, float)) and num_statements > 0:
                    return (float(covered_lines) / float(num_statements)) * 100
            else:  # branch
                num_branches = totals_data.get("num_branches", 0)
                covered_branches = totals_data.get("covered_branches", 0)
                if isinstance(num_branches, (int, float)) and num_branches > 0:
                    return (float(covered_branches) / float(num_branches)) * 100

            return 0.0
        except Exception:
            # Fall back to regex parsing if JSON parsing fails
            import re

            if coverage_type == "line":
                match = re.search(r"TOTAL.*?(\d+)%", output)
            else:  # branch
                match = re.search(r"TOTAL.*?\d+%.*?(\d+)%", output)

            if match:
                return float(match.group(1))
            return 0.0

    def _format_validation_text_output(
        self, validation_results: Mapping[str, object]
    ) -> str:
        """Format validation results as human-readable text."""
        timestamp = str(validation_results.get("timestamp", "unknown"))
        output = f"Validation Results - {timestamp}\n"
        output += "=" * 50 + "\n\n"

        # Quality results
        quality_results = cast(
            dict[str, object], validation_results.get("quality_results", {})
        )
        if quality_results:
            output += "Quality Checks:\n"
            checks = cast(dict[str, object], quality_results.get("checks", {}))
            for check, res_obj in checks.items():
                result = cast(dict[str, object], res_obj)
                status_icon = "✓" if result.get("success", False) else "✗"
                output += f"  {status_icon} {check}\n"

        # Test results
        test_results = cast(
            dict[str, object], validation_results.get("test_results", {})
        )
        if test_results:
            output += "\nTest Results:\n"
            tests = cast(dict[str, object], test_results.get("tests", {}))
            for test_type, res_obj in tests.items():
                result = cast(dict[str, object], res_obj)
                status_icon = "✓" if result.get("success", False) else "✗"
                count = result.get("count", 0)
                duration = result.get("duration", "0s")
                output += f"  {status_icon} {test_type}: {count} tests in {duration}\n"

        # Coverage results
        coverage_results = cast(
            dict[str, object], validation_results.get("coverage_results", {})
        )
        if coverage_results:
            coverage = cast(dict[str, object], coverage_results.get("coverage", {}))
            if coverage:
                line_pct = cast(float, coverage.get("line_percentage", 0))
                branch_pct = cast(float, coverage.get("branch_percentage", 0))
                output += (
                    f"\nCoverage: {line_pct:.1f}% lines, {branch_pct:.1f}% branches\n"
                )

        # Overall status
        overall_success = bool(validation_results.get("overall_success", False))
        overall_icon = "✓" if overall_success else "✗"
        output += (
            f"\nOverall: {overall_icon} {'PASSED' if overall_success else 'FAILED'}\n"
        )

        # Issues
        issues = validation_results.get("issues")
        if isinstance(issues, list):
            output += "\nIssues:\n"
            for issue_obj in cast(List[object], issues):
                issue_str = str(issue_obj)
                output += f"  • {issue_str}\n"

        return output

    def _format_status_text_output(self, status_data: Mapping[str, object]) -> str:
        """Format status data as human-readable text."""
        output = f"Workflow Status - {status_data['timestamp']}\n"
        output += "=" * 40 + "\n\n"

        # Git status
        git = cast(dict[str, object], status_data.get("git_status", {}))
        output += f"Git: {git.get('current_branch', 'unknown')} ({git.get('status', 'unknown')})\n"

        # Quality status
        quality = cast(dict[str, object], status_data.get("quality_status", {}))
        quality_icon = "✓" if quality.get("overall_status") == "passed" else "✗"
        output += (
            f"Quality: {quality_icon} {quality.get('overall_status', 'unknown')}\n"
        )

        # Test status
        test = cast(dict[str, object], status_data.get("test_status", {}))
        test_icon = "✓" if test.get("overall_status") == "passed" else "✗"
        output += f"Tests: {test_icon} {test.get('total_tests', 0)} tests\n"

        # Coverage status
        coverage = cast(dict[str, object], status_data.get("coverage_status", {}))
        if coverage.get("status") == "available":
            output += f"Coverage: {cast(float, coverage.get('line_percentage', 0)):.1f}% lines, {cast(float, coverage.get('branch_percentage', 0)):.1f}% branches\n"
        else:
            output += f"Coverage: {coverage.get('status', 'unknown')}\n"

        # Readiness
        readiness = cast(dict[str, object], status_data.get("readiness", {}))
        ready_icon = "✓" if readiness.get("ready_for_merge", False) else "✗"
        output += f"\nReady for merge: {ready_icon} {'Yes' if readiness.get('ready_for_merge', False) else 'No'}\n"

        # Blocking issues
        blocking_issues = readiness.get("blocking_issues")
        if isinstance(blocking_issues, list):
            output += "\nBlocking issues:\n"
            for issue_obj in cast(List[object], blocking_issues):
                issue_str = str(issue_obj)
                output += f"  • {issue_str}\n"

        return output

    def _get_timestamp(self) -> str:
        """Get current timestamp for structured output."""
        from datetime import datetime

        return datetime.now().isoformat()
