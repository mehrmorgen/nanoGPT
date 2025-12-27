from __future__ import annotations

from typing import Any, Dict, Optional

import typer
from typing_extensions import Annotated

from ml_playground.tools.core.learning_mode import LearningModeEngine, VerbosityLevel

app = typer.Typer(
    name="learn",
    help="Learning mode utilities and educational content",
    no_args_is_help=True,
)


@app.command("commands")
def learn_commands(
    category: Annotated[
        Optional[str],
        typer.Option("--category", help="Show commands for specific category"),
    ] = None,
    detailed: Annotated[
        bool, typer.Option("--detailed", help="Show detailed command descriptions")
    ] = False,
) -> None:
    """Show available commands with descriptions and usage examples."""
    command_catalog: Dict[str, Dict[str, Any]] = {
        "quality": {
            "description": "Code quality tools (lint, format, typecheck)",
            "commands": {
                "lint": "Run Ruff lint checks for code style and potential issues",
                "format": "Auto-fix and format code with Ruff",
                "lint-check": "Run Ruff in check-only mode (alias for lint)",
                "deadcode": "Scan for unused code using vulture",
                "basedpyright": "Run BasedPyright type checks",
                "pyright": "Run BasedPyright type checks (Pyright CLI alias)",
                "mypy": "Run Mypy type checks",
                "typecheck": "Run both BasedPyright and Mypy type checks",
                "all": "Run all quality checks (lint, typecheck, deadcode)",
            },
            "examples": [
                "uv run tools quality lint",
                "uv run tools quality format",
                "uv run tools quality all --learning-mode",
            ],
        },
        "test": {
            "description": "Testing tools (unit, integration, e2e, coverage)",
            "commands": {
                "unit": "Run unit tests",
                "integration": "Run integration tests",
                "e2e": "Run end-to-end tests",
                "acceptance": "Run acceptance tests",
                "property": "Run property-based tests",
                "all": "Run all tests",
                "coverage-test": "Run tests with coverage collection",
                "coverage-report": "Generate coverage reports",
                "coverage-threshold": "Check coverage thresholds",
                "clean": "Clean test artifacts and caches",
            },
            "examples": [
                "uv run tools test unit",
                "uv run tools test coverage-test",
                "uv run tools test all --learning-mode",
            ],
        },
        "env": {
            "description": "Environment management (setup, sync, clean)",
            "commands": {
                "setup": "Create fresh virtual environment and install dependencies",
                "sync": "Sync project dependencies using uv",
                "verify": "Ensure the project package imports correctly",
                "clean": "Remove caches and temporary build artifacts",
                "info": "Show environment information",
                "ai-guidelines": "Set up AI guideline symlinks",
                "tensorboard": "Launch TensorBoard for log visualization",
                "gguf-help": "Show llama.cpp GGUF conversion help",
            },
            "examples": [
                "uv run tools env setup",
                "uv run tools env sync --all-groups",
                "uv run tools env clean",
            ],
        },
        "ci": {
            "description": "CI/CD operations (quality gates, mutation testing)",
            "commands": {
                "quality-gate": "Run full pre-commit quality gate",
                "quality-fast": "Run fast quality checks (lint/format focused)",
                "quality-ext": "Run extended quality validation with mutation testing",
                "quality-ci-local": "Run GitHub quality workflow locally using act",
                "coverage-badge": "Regenerate SVG coverage badges",
                "mutation reset": "Remove cached Cosmic Ray session",
                "mutation summary": "Show summary of previous mutation testing",
                "mutation init": "Initialize Cosmic Ray session database",
                "mutation exec": "Execute mutation tests",
                "mutation report": "Render mutation testing report",
                "mutation run": "Run full mutation testing pipeline",
            },
            "examples": [
                "uv run tools ci quality-gate",
                "uv run tools ci quality-fast",
                "uv run tools ci mutation run",
            ],
        },
        "agentic": {
            "description": "AI-assisted development tools",
            "commands": {
                "guidelines-setup": "Set up AI development guidelines",
                "batch-review": "Perform batch review operations for AI consumption",
                "workflow-helper": "Provide workflow helpers for AI development patterns",
                "batch-quality": "Run automated quality checks for AI agents",
                "batch-validate": "Run comprehensive validation for AI decision-making",
                "workflow-status": "Get current workflow status for AI analysis",
            },
            "examples": [
                "uv run tools agentic guidelines-setup",
                "uv run tools agentic batch-review --format json",
                "uv run tools agentic workflow-status",
            ],
        },
        "learn": {
            "description": "Learning mode utilities and educational content",
            "commands": {
                "commands": "Show available commands with descriptions",
                "explain": "Explain a specific command with educational content",
                "best-practices": "Show comprehensive best practices guide",
            },
            "examples": [
                "uv run tools learn commands --category quality",
                "uv run tools learn explain quality.lint",
                "uv run tools learn best-practices --verbosity 2",
            ],
        },
    }

    if category:
        if category not in command_catalog:
            typer.echo(
                f"Error: Unknown category '{category}'. Valid categories: {', '.join(command_catalog.keys())}",
                err=True,
            )
            raise typer.Exit(1)

        cat_info = command_catalog[category]
        typer.echo(f"[bold]{category.title()} Tools[/bold]")
        typer.echo(f"{cat_info['description']}")
        typer.echo("")

        typer.echo("[bold]Available Commands:[/bold]")
        for cmd, desc in cat_info["commands"].items():
            typer.echo(f"  [bold]{cmd}[/bold] - {desc}")
        typer.echo("")

        if detailed or len(cat_info["examples"]) <= 3:
            typer.echo("[bold]Usage Examples:[/bold]")
            for example in cat_info["examples"]:
                typer.echo(f"  {example}")
            typer.echo("")

        typer.echo(
            f"Use [bold]uv run tools {category} --help[/bold] for detailed command options."
        )
        typer.echo(
            f"Use [bold]uv run tools learn explain {category}.<command>[/bold] for educational content."
        )
    else:
        typer.echo("[bold]ML Playground Tools - Command Discovery[/bold]")
        typer.echo("")
        typer.echo("Available tool categories:")
        typer.echo("")

        for cat, info in command_catalog.items():
            typer.echo(f"  [bold]{cat}[/bold] - {info['description']}")
            if detailed:
                teaser = ", ".join(list(info["commands"].keys())[:5])
                suffix = "..." if len(info["commands"]) > 5 else ""
                typer.echo(f"    Commands: {teaser}{suffix}")
        typer.echo("")

        typer.echo("[bold]Quick Start:[/bold]")
        typer.echo(
            "  uv run tools env setup              # Set up development environment"
        )
        typer.echo("  uv run tools quality all            # Run all quality checks")
        typer.echo("  uv run tools test unit              # Run unit tests")
        typer.echo("  uv run tools ci quality-gate        # Run full quality gate")
        typer.echo("")

        typer.echo("[bold]Learning and Discovery:[/bold]")
        typer.echo(
            "  uv run tools learn commands --category <name>    # Show commands for category"
        )
        typer.echo(
            "  uv run tools learn explain <category>.<command>  # Get detailed explanations"
        )
        typer.echo(
            "  uv run tools learn best-practices               # Show best practices guide"
        )
        typer.echo(
            "  uv run tools <category> <command> --learning-mode # See explanations while running"
        )
        typer.echo("")

        typer.echo(
            "Use [bold]--detailed[/bold] for more information or [bold]--category <name>[/bold] for specific categories."
        )


@app.command("explain")
def learn_explain(
    command: Annotated[
        str,
        typer.Argument(help="Command to explain (e.g., 'quality.lint', 'test.unit')"),
    ],
    verbosity: Annotated[
        int,
        typer.Option(
            "--verbosity",
            "-v",
            min=0,
            max=2,
            help="Explanation verbosity: 0=minimal, 1=standard, 2=comprehensive",
        ),
    ] = 1,
) -> None:
    """Explain a specific command with educational content."""
    if "." not in command:
        typer.echo(
            "Error: Command must be in format 'category.command' (e.g., 'quality.lint')",
            err=True,
        )
        raise typer.Exit(1)

    try:
        category, cmd = command.split(".", 1)
    except ValueError:
        typer.echo(
            f"Error: Invalid command format '{command}'. Use 'category.command'",
            err=True,
        )
        raise typer.Exit(1)

    valid_categories = {"quality", "test", "env", "ci", "agentic"}
    if category not in valid_categories:
        typer.echo(
            f"Error: Unknown category '{category}'. Valid categories: {', '.join(valid_categories)}",
            err=True,
        )
        raise typer.Exit(1)

    verbosity_level = VerbosityLevel(verbosity)
    engine = LearningModeEngine(verbosity_level)

    learning_info = engine.explain_command(
        command=cmd, context=f"Explaining {category}.{cmd} command", category=category
    )

    typer.echo(f"[bold]Command: {command}[/bold]")
    typer.echo("")

    if learning_info.explanations:
        typer.echo("[bold]📖 Explanation:[/bold]")
        for explanation in learning_info.explanations:
            typer.echo(f"  {explanation}")
        typer.echo("")

    if learning_info.best_practices:
        typer.echo("[bold]✨ Best Practices:[/bold]")
        for practice in learning_info.best_practices:
            typer.echo(f"  • {practice}")
        typer.echo("")

    if learning_info.related_concepts:
        typer.echo("[bold]🔗 Related Concepts:[/bold]")
        for concept in learning_info.related_concepts:
            typer.echo(f"  • {concept}")
        typer.echo("")

    if not any(
        [
            learning_info.explanations,
            learning_info.best_practices,
            learning_info.related_concepts,
        ]
    ):
        typer.echo(f"No educational content available for '{command}'")
        typer.echo(
            "This command may not exist or educational content may not be implemented yet."
        )


@app.command("best-practices")
def learn_best_practices(
    category: Annotated[
        Optional[str],
        typer.Option("--category", help="Show best practices for specific category"),
    ] = None,
    verbosity: Annotated[
        int,
        typer.Option(
            "--verbosity",
            "-v",
            min=0,
            max=2,
            help="Best practices verbosity: 0=minimal, 1=standard, 2=comprehensive",
        ),
    ] = 1,
) -> None:
    """Show comprehensive best practices guide for development workflows."""
    verbosity_level = VerbosityLevel(verbosity)
    engine = LearningModeEngine(verbosity_level)

    if category:
        valid_categories = {"quality", "test", "env", "ci", "agentic"}
        if category not in valid_categories:
            typer.echo(
                f"Error: Unknown category '{category}'. Valid categories: {', '.join(valid_categories)}",
                err=True,
            )
            raise typer.Exit(1)

        typer.echo(f"[bold]Best Practices for {category.title()} Tools[/bold]")
        typer.echo("")

        category_practices = engine.get_category_best_practices(category)
        for practice in category_practices:
            typer.echo(f"  • {practice}")
        typer.echo("")

        _show_learning_paths(category, verbosity_level)
    else:
        typer.echo("[bold]ML Playground Development Best Practices[/bold]")
        typer.echo("")

        typer.echo("[bold]🚀 Development Workflow[/bold]")
        workflow_practices = [
            "Start with environment setup: `uv run tools env setup`",
            "Run fast quality checks frequently: `uv run tools ci quality-fast`",
            "Write tests before implementing features (TDD approach)",
            "Use learning mode to understand tools: `--learning-mode`",
            "Run full quality gates before merging: `uv run tools ci quality-gate`",
        ]
        for practice in workflow_practices:
            typer.echo(f"  • {practice}")
        typer.echo("")

        categories = ["quality", "test", "env", "ci", "agentic"]
        for cat in categories:
            typer.echo(f"[bold]{cat.title()} Tools:[/bold]")
            cat_practices = engine.get_category_best_practices(cat)
            for practice in cat_practices[:2]:
                typer.echo(f"  • {practice}")
            typer.echo("")

        typer.echo("[bold]📚 Learning Paths[/bold]")
        typer.echo("")
        _show_learning_paths_overview(verbosity_level)


def _show_learning_paths(category: str, verbosity_level: VerbosityLevel) -> None:
    learning_paths = {
        "quality": {
            "beginner": [
                "Start with `uv run tools quality lint` to check code style",
                "Use `uv run tools quality format` to auto-fix formatting issues",
                "Learn about type checking with `uv run tools quality typecheck`",
                "Run all quality checks with `uv run tools quality all`",
            ],
            "intermediate": [
                "Integrate quality checks into your editor for real-time feedback",
                "Set up pre-commit hooks to run quality checks automatically",
                "Understand different type checkers: BasedPyright vs MyPy",
                "Use dead code detection to maintain clean codebase",
            ],
            "advanced": [
                "Configure custom linting rules for your team's standards",
                "Optimize quality check performance for large codebases",
                "Integrate quality metrics into CI/CD pipelines",
                "Use quality tools for code review automation",
            ],
        },
        "test": {
            "beginner": [
                "Start with unit tests: `uv run tools test unit`",
                "Learn about test coverage: `uv run tools test coverage-test`",
                "Understand different test types: unit, integration, e2e",
                "Use property-based testing for robust validation",
            ],
            "intermediate": [
                "Write effective integration tests for component interactions",
                "Use coverage reports to identify untested code paths",
                "Implement acceptance tests for business requirements",
                "Balance test types in the testing pyramid",
            ],
            "advanced": [
                "Use mutation testing to validate test effectiveness",
                "Optimize test suite performance and parallelization",
                "Implement comprehensive test strategies for ML workflows",
                "Design testable architectures with dependency injection",
            ],
        },
        "env": {
            "beginner": [
                "Set up development environment: `uv run tools env setup`",
                "Sync dependencies regularly: `uv run tools env sync`",
                "Verify environment health: `uv run tools env verify`",
                "Clean caches when issues arise: `uv run tools env clean`",
            ],
            "intermediate": [
                "Understand dependency groups and selective syncing",
                "Use environment info for troubleshooting issues",
                "Set up AI guidelines for consistent workflows",
                "Manage multiple environments for different purposes",
            ],
            "advanced": [
                "Optimize environment setup for team onboarding",
                "Implement environment validation in CI/CD",
                "Use containerized environments for consistency",
                "Automate environment management workflows",
            ],
        },
        "ci": {
            "beginner": [
                "Run quality gates: `uv run tools ci quality-gate`",
                "Use fast quality checks for quick feedback",
                "Generate coverage badges for documentation",
                "Understand the importance of quality gates",
            ],
            "intermediate": [
                "Set up local CI testing with act",
                "Use mutation testing to improve test quality",
                "Implement comprehensive quality validation",
                "Monitor quality metrics over time",
            ],
            "advanced": [
                "Design robust CI/CD pipelines with quality gates",
                "Optimize CI performance with caching and parallelization",
                "Implement progressive quality standards",
                "Use CI metrics for continuous improvement",
            ],
        },
        "agentic": {
            "beginner": [
                "Set up AI guidelines for consistent workflows",
                "Use batch operations for AI-assisted development",
                "Understand AI workflow helpers and templates",
                "Learn about structured output formats for AI",
            ],
            "intermediate": [
                "Implement AI-driven code review processes",
                "Use workflow status for AI decision-making",
                "Combine AI tools with human oversight",
                "Optimize AI workflows for your team's needs",
            ],
            "advanced": [
                "Design comprehensive AI-assisted development pipelines",
                "Implement custom AI workflow patterns",
                "Use AI tools for automated quality assurance",
                "Build AI-driven development decision systems",
            ],
        },
    }

    if category not in learning_paths:
        return

    paths = learning_paths[category]

    if verbosity_level.value >= 1:
        typer.echo("[bold]🎯 Beginner Path:[/bold]")
        for step in paths["beginner"]:
            typer.echo(f"  1. {step}")
        typer.echo("")

        typer.echo("[bold]🚀 Intermediate Path:[/bold]")
        for step in paths["intermediate"]:
            typer.echo(f"  2. {step}")
        typer.echo("")

    if verbosity_level.value >= 2:
        typer.echo("[bold]🏆 Advanced Path:[/bold]")
        for step in paths["advanced"]:
            typer.echo(f"  3. {step}")
        typer.echo("")


def _show_learning_paths_overview(verbosity_level: VerbosityLevel) -> None:
    if verbosity_level.value == 0:
        typer.echo(
            "Use `uv run tools learn best-practices --category <name>` for specific guidance"
        )
        return

    typer.echo("[bold]🎯 For Beginners:[/bold]")
    typer.echo("  1. Start with environment setup and basic quality checks")
    typer.echo("  2. Learn unit testing and coverage measurement")
    typer.echo("  3. Use learning mode to understand each tool")
    typer.echo("")

    typer.echo("[bold]🚀 For Intermediate Users:[/bold]")
    typer.echo("  1. Integrate tools into your development workflow")
    typer.echo("  2. Set up comprehensive testing strategies")
    typer.echo("  3. Use CI tools for automated quality assurance")
    typer.echo("")

    if verbosity_level.value >= 2:
        typer.echo("[bold]🏆 For Advanced Users:[/bold]")
        typer.echo("  1. Design custom workflows and automation")
        typer.echo("  2. Implement AI-assisted development patterns")
        typer.echo("  3. Optimize tools for team and organizational needs")
        typer.echo("")

    typer.echo(
        "Use `--category <name>` to see detailed paths for specific tool categories."
    )


def build_app() -> typer.Typer:
    return app
