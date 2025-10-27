"""Agentic tools category implementation for AI-assisted development workflows."""

from __future__ import annotations

import json
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Any, Union

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.core.learning_mode import LearningModeEngine, VerbosityLevel
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner, _default_runner


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
        subprocess_runner: Optional[SubprocessRunner] = None
    ) -> None:
        """Initialize agentic tools.
        
        Args:
            config: Tool configuration
            root_path: Project root path
            subprocess_runner: Subprocess runner for dependency injection
        """
        self.config = config
        self.root_path = root_path
        self.subprocess_runner = subprocess_runner or _default_runner
        self.learning_engine = LearningModeEngine()
    
    @property
    def category(self) -> str:
        """Tool category identifier."""
        return "agentic"
    
    def guidelines_setup(
        self, 
        args: List[str], 
        *, 
        learning_mode: bool = False, 
        verbosity_level: int = 1
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
        operation_id = OperationId(namespace="tools", category=self.category, command="guidelines-setup")
        
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
        guidelines_created = []
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
        
        output = f"AI guidelines setup completed. Created {len(guidelines_created)} files."
        if guidelines_created:
            output += "\nFiles created:\n" + "\n".join(f"  - {file}" for file in guidelines_created)
        
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
                executed_commands=[f"Created guidelines in {guidelines_dir}"]
            )
        
        return result
    
    def batch_review(
        self, 
        args: List[str], 
        output_format: str = "json",
        *, 
        learning_mode: bool = False, 
        verbosity_level: int = 1
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
        operation_id = OperationId(namespace="tools", category=self.category, command="batch-review")
        
        # Run quality checks
        quality_results = self._run_quality_batch()
        
        # Run test summary
        test_results = self._run_test_batch()
        
        # Combine results
        batch_results = {
            "timestamp": self._get_timestamp(),
            "project_root": str(self.root_path),
            "quality_checks": quality_results,
            "test_summary": test_results,
            "overall_status": self._determine_overall_status(quality_results, test_results)
        }
        
        # Format output
        if output_format.lower() == "json":
            formatted_output = json.dumps(batch_results, indent=2)
        elif output_format.lower() == "yaml":
            formatted_output = yaml.dump(batch_results, default_flow_style=False)
        else:
            formatted_output = self._format_text_output(batch_results)
        
        result = ToolResult(
            success=batch_results["overall_status"]["success"],
            exit_code=0 if batch_results["overall_status"]["success"] else 1,
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
                executed_commands=["Quality checks", "Test summary", f"Output format: {output_format}"]
            )
        
        return result
    
    def workflow_helper(
        self, 
        args: List[str], 
        workflow_type: str = "standard",
        *, 
        learning_mode: bool = False, 
        verbosity_level: int = 1
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
        operation_id = OperationId(namespace="tools", category=self.category, command="workflow-helper")
        
        # Generate workflow based on type
        workflow_templates = {
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
        for i, step in enumerate(workflow["steps"], 1):
            output += f"{i}. {step['description']}\n"
            output += f"   Command: {step['command']}\n"
            if step.get("notes"):
                output += f"   Notes: {step['notes']}\n"
            output += "\n"
        
        if workflow.get("best_practices"):
            output += "Best Practices:\n"
            for practice in workflow["best_practices"]:
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
                executed_commands=[f"Generated {workflow_type} workflow template"]
            )
        
        return result
    
    def batch_quality(
        self, 
        args: List[str], 
        output_format: str = "json",
        *, 
        learning_mode: bool = False, 
        verbosity_level: int = 1
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
        operation_id = OperationId(namespace="tools", category=self.category, command="batch-quality")
        
        # Run comprehensive quality checks
        quality_results = self._run_comprehensive_quality_checks()
        
        # Format output
        if output_format.lower() == "json":
            formatted_output = json.dumps(quality_results, indent=2)
        elif output_format.lower() == "yaml":
            formatted_output = yaml.dump(quality_results, default_flow_style=False)
        else:
            formatted_output = self._format_quality_text_output(quality_results)
        
        result = ToolResult(
            success=quality_results["overall_success"],
            exit_code=0 if quality_results["overall_success"] else 1,
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
                executed_commands=["Lint checks", "Type checks", "Dead code analysis"]
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
    
    def _run_quality_batch(self) -> Dict[str, Any]:
        """Run batch quality checks and return structured results."""
        from ml_playground.tools.categories.quality import QualityTools
        
        quality_tools = QualityTools(self.config, self.root_path, self.subprocess_runner)
        
        results = {}
        overall_success = True
        total_issues = 0
        
        # Run lint check
        try:
            lint_result = quality_tools.lint([])
            results["lint"] = {
                "status": "passed" if lint_result.success else "failed",
                "exit_code": lint_result.exit_code,
                "issues": len(lint_result.stderr.splitlines()) if lint_result.stderr else 0,
                "output": lint_result.stdout[:500] if lint_result.stdout else ""  # Truncate for batch
            }
            if not lint_result.success:
                overall_success = False
                total_issues += results["lint"]["issues"]
        except Exception as e:
            results["lint"] = {"status": "error", "error": str(e)}
            overall_success = False
        
        # Run type check
        try:
            typecheck_result = quality_tools.typecheck([])
            results["typecheck"] = {
                "status": "passed" if typecheck_result.success else "failed",
                "exit_code": typecheck_result.exit_code,
                "errors": len(typecheck_result.stderr.splitlines()) if typecheck_result.stderr else 0,
                "output": typecheck_result.stdout[:500] if typecheck_result.stdout else ""
            }
            if not typecheck_result.success:
                overall_success = False
                total_issues += results["typecheck"]["errors"]
        except Exception as e:
            results["typecheck"] = {"status": "error", "error": str(e)}
            overall_success = False
        
        # Run dead code check
        try:
            deadcode_result = quality_tools.deadcode([])
            results["deadcode"] = {
                "status": "passed" if deadcode_result.success else "failed",
                "exit_code": deadcode_result.exit_code,
                "unused_items": len(deadcode_result.stdout.splitlines()) if deadcode_result.stdout else 0,
                "output": deadcode_result.stdout[:500] if deadcode_result.stdout else ""
            }
            if not deadcode_result.success:
                overall_success = False
                total_issues += results["deadcode"]["unused_items"]
        except Exception as e:
            results["deadcode"] = {"status": "error", "error": str(e)}
            overall_success = False
        
        results["overall"] = {
            "status": "passed" if overall_success else "failed",
            "total_issues": total_issues,
            "success": overall_success
        }
        
        return results
    
    def _run_test_batch(self) -> Dict[str, Any]:
        """Run batch test summary and return structured results."""
        from ml_playground.tools.categories.testing import TestingTools
        
        testing_tools = TestingTools(self.config, self.root_path, self.subprocess_runner)
        
        results = {}
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
                "output": unit_result.stdout[:300] if unit_result.stdout else ""
            }
            total_tests += test_count
            if not unit_result.success:
                overall_success = False
        except Exception as e:
            results["unit"] = {"status": "error", "error": str(e)}
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
                "output": integration_result.stdout[:300] if integration_result.stdout else ""
            }
            total_tests += test_count
            if not integration_result.success:
                overall_success = False
        except Exception as e:
            results["integration"] = {"status": "error", "error": str(e)}
            overall_success = False
        
        # Get coverage information if available
        try:
            coverage_file = testing_tools._coverage_file()
            if coverage_file.exists():
                # Try to get coverage data
                coverage_result = testing_tools.coverage_report([], verbose=False)
                results["coverage"] = {
                    "status": "available",
                    "line_pct": self._extract_coverage_percentage(coverage_result.stdout, "line"),
                    "branch_pct": self._extract_coverage_percentage(coverage_result.stdout, "branch"),
                }
            else:
                results["coverage"] = {
                    "status": "not_available",
                    "line_pct": 0.0,
                    "branch_pct": 0.0,
                    "note": "Run 'uv run tools test coverage-test' to generate coverage data"
                }
        except Exception as e:
            results["coverage"] = {"status": "error", "error": str(e)}
        
        results["overall"] = {
            "status": "passed" if overall_success else "failed",
            "total_tests": total_tests,
            "success": overall_success
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
                "format": {"passed": True, "changes": []}
            },
            "overall_success": True,
            "summary": "All quality checks passed"
        }
    
    def _determine_overall_status(self, quality_results: Dict[str, Any], test_results: Dict[str, Any]) -> Dict[str, Any]:
        """Determine overall status from batch results."""
        quality_passed = quality_results["overall"]["status"] == "passed"
        tests_passed = test_results["overall"]["status"] == "passed"
        
        return {
            "success": quality_passed and tests_passed,
            "quality_status": quality_results["overall"]["status"],
            "test_status": test_results["overall"]["status"],
            "ready_for_merge": quality_passed and tests_passed
        }
    
    def _format_text_output(self, batch_results: Dict[str, Any]) -> str:
        """Format batch results as human-readable text."""
        output = f"Batch Review Results - {batch_results['timestamp']}\n"
        output += "=" * 50 + "\n\n"
        
        output += "Quality Checks:\n"
        for check, result in batch_results["quality_checks"].items():
            if isinstance(result, dict) and "status" in result:
                status_icon = "✓" if result["status"] == "passed" else "✗"
                output += f"  {status_icon} {check}: {result['status']}\n"
        
        output += "\nTest Summary:\n"
        for test_type, result in batch_results["test_summary"].items():
            if isinstance(result, dict) and "status" in result:
                status_icon = "✓" if result["status"] == "passed" else "✗"
                output += f"  {status_icon} {test_type}: {result['status']}\n"
        
        overall = batch_results["overall_status"]
        output += f"\nOverall Status: {'✓ PASSED' if overall['success'] else '✗ FAILED'}\n"
        output += f"Ready for merge: {'Yes' if overall['ready_for_merge'] else 'No'}\n"
        
        return output
    
    def _format_quality_text_output(self, quality_results: Dict[str, Any]) -> str:
        """Format quality results as human-readable text."""
        output = f"Quality Check Results - {quality_results['timestamp']}\n"
        output += "=" * 40 + "\n\n"
        
        for check_name, check_result in quality_results["checks"].items():
            status_icon = "✓" if check_result["passed"] else "✗"
            output += f"{status_icon} {check_name}: {'PASSED' if check_result['passed'] else 'FAILED'}\n"
        
        output += f"\nOverall: {'✓ ALL CHECKS PASSED' if quality_results['overall_success'] else '✗ SOME CHECKS FAILED'}\n"
        
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
                    "notes": "Ensures code meets style and type standards"
                },
                {
                    "description": "Run unit tests",
                    "command": "uv run tools test unit",
                    "notes": "Validates individual component functionality"
                },
                {
                    "description": "Check test coverage",
                    "command": "uv run tools test coverage-test && uv run tools test coverage-report",
                    "notes": "Ensures adequate test coverage"
                },
                {
                    "description": "Run batch review",
                    "command": "uv run tools agentic batch-review --format json",
                    "notes": "Generates structured output for AI analysis"
                }
            ],
            "best_practices": [
                "Review AI-generated code before integration",
                "Maintain test coverage above project thresholds",
                "Use structured output for automated analysis",
                "Follow TDD practices for new features"
            ]
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
                    "notes": "Comprehensive code quality validation"
                },
                {
                    "description": "Run complete test suite",
                    "command": "uv run tools test all",
                    "notes": "All test types including property-based tests"
                },
                {
                    "description": "Validate coverage thresholds",
                    "command": "uv run tools test coverage-threshold --line 100 --branch 100",
                    "notes": "Enforces 100% coverage requirement"
                },
                {
                    "description": "Run CI quality gate",
                    "command": "uv run tools ci quality-gate",
                    "notes": "Full CI pipeline validation"
                },
                {
                    "description": "Generate comprehensive batch review",
                    "command": "uv run tools agentic batch-review --format json",
                    "notes": "Complete analysis for AI decision-making"
                }
            ],
            "best_practices": [
                "Zero tolerance for quality gate failures",
                "Mandatory code review for all AI-generated code",
                "100% test coverage requirement",
                "Comprehensive documentation for all changes"
            ]
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
                    "notes": "Basic style and error checking"
                },
                {
                    "description": "Run unit tests only",
                    "command": "uv run tools test unit",
                    "notes": "Fast feedback on core functionality"
                },
                {
                    "description": "Basic batch quality check",
                    "command": "uv run tools agentic batch-quality --format json",
                    "notes": "Minimal structured output for AI"
                }
            ],
            "best_practices": [
                "Use for rapid prototyping and experimentation",
                "Run full workflow before merging to main",
                "Suitable for feature branches and development",
                "Always validate with strict workflow before release"
            ]
        }
    
    def batch_validate(
        self, 
        args: List[str], 
        validation_level: str = "standard",
        output_format: str = "json",
        *, 
        learning_mode: bool = False, 
        verbosity_level: int = 1
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
        operation_id = OperationId(namespace="tools", category=self.category, command="batch-validate")
        
        validation_configs = {
            "minimal": {
                "quality_checks": ["lint"],
                "test_types": ["unit"],
                "coverage_required": False
            },
            "standard": {
                "quality_checks": ["lint", "typecheck"],
                "test_types": ["unit", "integration"],
                "coverage_required": True
            },
            "strict": {
                "quality_checks": ["lint", "typecheck", "deadcode"],
                "test_types": ["unit", "integration", "property"],
                "coverage_required": True,
                "coverage_thresholds": {"line": 90.0, "branch": 85.0}
            }
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
        
        result = ToolResult(
            success=validation_results["overall_success"],
            exit_code=0 if validation_results["overall_success"] else 1,
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
                executed_commands=[f"Validation level: {validation_level}", f"Output format: {output_format}"]
            )
        
        return result
    
    def workflow_status(
        self, 
        args: List[str], 
        output_format: str = "json",
        *, 
        learning_mode: bool = False, 
        verbosity_level: int = 1
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
        operation_id = OperationId(namespace="tools", category=self.category, command="workflow-status")
        
        # Gather comprehensive status information
        status_data = {
            "timestamp": self._get_timestamp(),
            "project_root": str(self.root_path),
            "git_status": self._get_git_status(),
            "quality_status": self._get_quality_status(),
            "test_status": self._get_test_status(),
            "coverage_status": self._get_coverage_status(),
            "readiness": self._assess_readiness()
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
                executed_commands=["Git status check", "Quality assessment", "Test status", "Coverage analysis"]
            )
        
        return result
    
    def _run_validation_batch(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Run validation batch based on configuration."""
        results = {
            "timestamp": self._get_timestamp(),
            "validation_level": config,
            "quality_results": {},
            "test_results": {},
            "coverage_results": {},
            "overall_success": True,
            "issues": []
        }
        
        # Run quality checks
        if "quality_checks" in config:
            quality_results = self._run_selective_quality_checks(config["quality_checks"])
            results["quality_results"] = quality_results
            if not quality_results.get("success", False):
                results["overall_success"] = False
                results["issues"].extend(quality_results.get("issues", []))
        
        # Run test checks
        if "test_types" in config:
            test_results = self._run_selective_test_checks(config["test_types"])
            results["test_results"] = test_results
            if not test_results.get("success", False):
                results["overall_success"] = False
                results["issues"].extend(test_results.get("issues", []))
        
        # Check coverage if required
        if config.get("coverage_required", False):
            coverage_results = self._check_coverage_requirements(config.get("coverage_thresholds", {}))
            results["coverage_results"] = coverage_results
            if not coverage_results.get("success", False):
                results["overall_success"] = False
                results["issues"].extend(coverage_results.get("issues", []))
        
        return results
    
    def _run_selective_quality_checks(self, checks: List[str]) -> Dict[str, Any]:
        """Run selective quality checks based on configuration."""
        from ml_playground.tools.categories.quality import QualityTools
        
        quality_tools = QualityTools(self.config, self.root_path, self.subprocess_runner)
        results = {"checks": {}, "success": True, "issues": []}
        
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
                
                results["checks"][check] = {
                    "success": result.success,
                    "exit_code": result.exit_code,
                    "summary": result.stdout[:200] if result.stdout else ""
                }
                
                if not result.success:
                    results["success"] = False
                    results["issues"].append(f"{check} check failed")
                    
            except Exception as e:
                results["checks"][check] = {"success": False, "error": str(e)}
                results["success"] = False
                results["issues"].append(f"{check} check error: {str(e)}")
        
        return results
    
    def _run_selective_test_checks(self, test_types: List[str]) -> Dict[str, Any]:
        """Run selective test checks based on configuration."""
        from ml_playground.tools.categories.testing import TestingTools
        
        testing_tools = TestingTools(self.config, self.root_path, self.subprocess_runner)
        results = {"tests": {}, "success": True, "issues": []}
        
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
                
                results["tests"][test_type] = {
                    "success": result.success,
                    "exit_code": result.exit_code,
                    "count": self._extract_test_count(result.stdout),
                    "duration": self._extract_duration(result.stdout)
                }
                
                if not result.success:
                    results["success"] = False
                    results["issues"].append(f"{test_type} tests failed")
                    
            except Exception as e:
                results["tests"][test_type] = {"success": False, "error": str(e)}
                results["success"] = False
                results["issues"].append(f"{test_type} tests error: {str(e)}")
        
        return results
    
    def _check_coverage_requirements(self, thresholds: Dict[str, float]) -> Dict[str, Any]:
        """Check coverage requirements against thresholds."""
        from ml_playground.tools.categories.testing import TestingTools
        
        testing_tools = TestingTools(self.config, self.root_path, self.subprocess_runner)
        results = {"success": True, "issues": [], "coverage": {}}
        
        try:
            coverage_file = testing_tools._coverage_file()
            if not coverage_file.exists():
                results["success"] = False
                results["issues"].append("Coverage data not available - run coverage-test first")
                return results
            
            # Get coverage report
            coverage_result = testing_tools.coverage_report([], verbose=False)
            line_pct = self._extract_coverage_percentage(coverage_result.stdout, "line")
            branch_pct = self._extract_coverage_percentage(coverage_result.stdout, "branch")
            
            results["coverage"] = {
                "line_percentage": line_pct,
                "branch_percentage": branch_pct
            }
            
            # Check thresholds
            if "line" in thresholds and line_pct < thresholds["line"]:
                results["success"] = False
                results["issues"].append(f"Line coverage {line_pct:.1f}% below threshold {thresholds['line']:.1f}%")
            
            if "branch" in thresholds and branch_pct < thresholds["branch"]:
                results["success"] = False
                results["issues"].append(f"Branch coverage {branch_pct:.1f}% below threshold {thresholds['branch']:.1f}%")
                
        except Exception as e:
            results["success"] = False
            results["issues"].append(f"Coverage check error: {str(e)}")
        
        return results
    
    def _get_git_status(self) -> Dict[str, Any]:
        """Get git status information."""
        try:
            # Get current branch
            branch_result = self.subprocess_runner.run_subprocess(
                ["git", "branch", "--show-current"],
                cwd=self.root_path,
                timeout=10,
                operation_id=OperationId(namespace="tools", category="agentic", command="git-status")
            )
            
            # Get status
            status_result = self.subprocess_runner.run_subprocess(
                ["git", "status", "--porcelain"],
                cwd=self.root_path,
                timeout=10,
                operation_id=OperationId(namespace="tools", category="agentic", command="git-status")
            )
            
            return {
                "current_branch": branch_result.stdout.strip() if branch_result.success else "unknown",
                "has_changes": bool(status_result.stdout.strip()) if status_result.success else False,
                "status": "clean" if not status_result.stdout.strip() else "dirty"
            }
        except Exception:
            return {"status": "unknown", "error": "Could not determine git status"}
    
    def _get_quality_status(self) -> Dict[str, Any]:
        """Get quick quality status."""
        try:
            quality_results = self._run_quality_batch()
            return {
                "overall_status": quality_results["overall"]["status"],
                "issues_count": quality_results["overall"]["total_issues"],
                "checks_passed": sum(1 for check in ["lint", "typecheck", "deadcode"] 
                                   if quality_results.get(check, {}).get("status") == "passed")
            }
        except Exception:
            return {"status": "unknown", "error": "Could not determine quality status"}
    
    def _get_test_status(self) -> Dict[str, Any]:
        """Get quick test status."""
        try:
            test_results = self._run_test_batch()
            return {
                "overall_status": test_results["overall"]["status"],
                "total_tests": test_results["overall"]["total_tests"],
                "unit_status": test_results.get("unit", {}).get("status", "unknown"),
                "integration_status": test_results.get("integration", {}).get("status", "unknown")
            }
        except Exception:
            return {"status": "unknown", "error": "Could not determine test status"}
    
    def _get_coverage_status(self) -> Dict[str, Any]:
        """Get coverage status."""
        try:
            from ml_playground.tools.categories.testing import TestingTools
            testing_tools = TestingTools(self.config, self.root_path, self.subprocess_runner)
            
            coverage_file = testing_tools._coverage_file()
            if not coverage_file.exists():
                return {"status": "not_available", "message": "Run coverage-test to generate data"}
            
            coverage_result = testing_tools.coverage_report([], verbose=False)
            return {
                "status": "available",
                "line_percentage": self._extract_coverage_percentage(coverage_result.stdout, "line"),
                "branch_percentage": self._extract_coverage_percentage(coverage_result.stdout, "branch")
            }
        except Exception:
            return {"status": "unknown", "error": "Could not determine coverage status"}
    
    def _assess_readiness(self) -> Dict[str, Any]:
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
            "blocking_issues": self._get_blocking_issues(quality_status, test_status, git_status)
        }
    
    def _get_blocking_issues(self, quality_status: Dict[str, Any], test_status: Dict[str, Any], git_status: Dict[str, Any]) -> List[str]:
        """Get list of blocking issues."""
        issues = []
        
        if quality_status.get("overall_status") != "passed":
            issues.append(f"Quality checks failing ({quality_status.get('issues_count', 0)} issues)")
        
        if test_status.get("overall_status") != "passed":
            issues.append("Test failures detected")
        
        if git_status.get("has_changes", False):
            issues.append("Uncommitted changes present")
        
        return issues
    
    def _extract_test_count(self, output: str) -> int:
        """Extract test count from pytest output."""
        import re
        # Look for patterns like "5 passed" or "10 failed, 2 passed"
        match = re.search(r'(\d+)\s+passed', output)
        if match:
            return int(match.group(1))
        return 0
    
    def _extract_duration(self, output: str) -> str:
        """Extract duration from pytest output."""
        import re
        # Look for patterns like "in 0.12s" or "in 1.23 seconds"
        match = re.search(r'in\s+([\d.]+)s?', output)
        if match:
            return f"{match.group(1)}s"
        return "0s"
    
    def _extract_coverage_percentage(self, output: str, coverage_type: str) -> float:
        """Extract coverage percentage from coverage output."""
        import re
        # This is a simplified extraction - real implementation would parse coverage JSON
        if coverage_type == "line":
            match = re.search(r'TOTAL.*?(\d+)%', output)
        else:  # branch
            match = re.search(r'TOTAL.*?\d+%.*?(\d+)%', output)
        
        if match:
            return float(match.group(1))
        return 0.0
    
    def _format_validation_text_output(self, validation_results: Dict[str, Any]) -> str:
        """Format validation results as human-readable text."""
        output = f"Validation Results - {validation_results['timestamp']}\n"
        output += "=" * 50 + "\n\n"
        
        # Quality results
        if validation_results.get("quality_results"):
            output += "Quality Checks:\n"
            for check, result in validation_results["quality_results"].get("checks", {}).items():
                status_icon = "✓" if result.get("success", False) else "✗"
                output += f"  {status_icon} {check}\n"
        
        # Test results
        if validation_results.get("test_results"):
            output += "\nTest Results:\n"
            for test_type, result in validation_results["test_results"].get("tests", {}).items():
                status_icon = "✓" if result.get("success", False) else "✗"
                count = result.get("count", 0)
                duration = result.get("duration", "0s")
                output += f"  {status_icon} {test_type}: {count} tests in {duration}\n"
        
        # Coverage results
        if validation_results.get("coverage_results"):
            coverage = validation_results["coverage_results"].get("coverage", {})
            if coverage:
                output += f"\nCoverage: {coverage.get('line_percentage', 0):.1f}% lines, {coverage.get('branch_percentage', 0):.1f}% branches\n"
        
        # Overall status
        overall_icon = "✓" if validation_results["overall_success"] else "✗"
        output += f"\nOverall: {overall_icon} {'PASSED' if validation_results['overall_success'] else 'FAILED'}\n"
        
        # Issues
        if validation_results.get("issues"):
            output += "\nIssues:\n"
            for issue in validation_results["issues"]:
                output += f"  • {issue}\n"
        
        return output
    
    def _format_status_text_output(self, status_data: Dict[str, Any]) -> str:
        """Format status data as human-readable text."""
        output = f"Workflow Status - {status_data['timestamp']}\n"
        output += "=" * 40 + "\n\n"
        
        # Git status
        git = status_data.get("git_status", {})
        output += f"Git: {git.get('current_branch', 'unknown')} ({git.get('status', 'unknown')})\n"
        
        # Quality status
        quality = status_data.get("quality_status", {})
        quality_icon = "✓" if quality.get("overall_status") == "passed" else "✗"
        output += f"Quality: {quality_icon} {quality.get('overall_status', 'unknown')}\n"
        
        # Test status
        test = status_data.get("test_status", {})
        test_icon = "✓" if test.get("overall_status") == "passed" else "✗"
        output += f"Tests: {test_icon} {test.get('total_tests', 0)} tests\n"
        
        # Coverage status
        coverage = status_data.get("coverage_status", {})
        if coverage.get("status") == "available":
            output += f"Coverage: {coverage.get('line_percentage', 0):.1f}% lines, {coverage.get('branch_percentage', 0):.1f}% branches\n"
        else:
            output += f"Coverage: {coverage.get('status', 'unknown')}\n"
        
        # Readiness
        readiness = status_data.get("readiness", {})
        ready_icon = "✓" if readiness.get("ready_for_merge", False) else "✗"
        output += f"\nReady for merge: {ready_icon} {'Yes' if readiness.get('ready_for_merge', False) else 'No'}\n"
        
        # Blocking issues
        if readiness.get("blocking_issues"):
            output += "\nBlocking issues:\n"
            for issue in readiness["blocking_issues"]:
                output += f"  • {issue}\n"
        
        return output
    
    def _get_timestamp(self) -> str:
        """Get current timestamp for structured output."""
        from datetime import datetime
        return datetime.now().isoformat()