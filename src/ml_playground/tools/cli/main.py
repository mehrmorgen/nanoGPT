"""Main CLI entry point for the ML Playground tools system.

This module provides the unified CLI accessible via `uv run tools`, organizing
all development tools under logical subcommands with learning mode support.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TypedDict, Annotated, Optional

import typer

from ml_playground.tools.cli.commands.quality import quality_app
from ml_playground.tools.cli.commands.testing import test_app
from ml_playground.tools.cli.commands.environment import env_app
from ml_playground.tools.cli.commands.ci import ci_app
from ml_playground.tools.cli.commands.dev import dev_app
from ml_playground.tools.cli.state import state
from ml_playground.tools.cli.dependencies import (
    get_tools_dependencies,
)
from ml_playground.tools.cli.config_loader import (
    load_config_with_error_handling,
    ensure_config_loaded,
)

from ml_playground.tools.core.errors import (
    ToolExecutionError,
    ToolConfigurationError,
)


class CategoryInfo(TypedDict):
    """Type definition for category information in get_command_info()."""

    name: str
    description: str
    commands: dict[str, str]


# Main Typer app
app = typer.Typer(
    name="tools",
    help="ML Playground unified development tools",
    no_args_is_help=False,
    rich_markup_mode="rich",
)

learn_app = typer.Typer(
    name="learn",
    help="Learning mode utilities and educational content",
    no_args_is_help=True,
)


def get_command_info() -> dict[str, CategoryInfo]:
    """Get information about all available commands by category."""
    return {
        "quality": {
            "name": "Quality Tools",
            "description": "Code quality tools (lint, format, typecheck)",
            "commands": {
                "lint": "Run Ruff lint checks",
                "format": "Run code formatting tools",
                "typecheck": "Run type checking tools",
                "all": "Run all quality checks (lint, typecheck, deadcode)",
            },
        },
        "test": {
            "name": "Testing Tools",
            "description": "Testing tools (unit, integration, e2e, coverage)",
            "commands": {
                "unit": "Run unit tests",
                "integration": "Run integration tests",
                "e2e": "Run end-to-end tests",
                "coverage": "Run coverage analysis",
                "all": "Run all tests",
                "clean": "Clean test artifacts and caches",
            },
        },
        "env": {
            "name": "Environment Tools",
            "description": "Environment setup and management",
            "commands": {
                "setup": "Create fresh uv-managed virtual environment",
                "sync": "Sync dependencies using uv",
                "verify": "Ensure project imports correctly",
                "clean": "Remove caches and temporary artifacts",
                "info": "Show environment information",
            },
        },
        "ci": {
            "name": "CI Tools",
            "description": "CI/CD operations and quality gates",
            "commands": {
                "quality-fast": "Run lint/format focused pre-commit hooks",
                "quality-ext": "Run extended quality gates",
                "quality-ci-local": "Run GitHub Actions workflow locally",
                "coverage-badge": "Regenerate SVG coverage badges",
            },
        },
        "dev": {
            "name": "Development Tools",
            "description": "Development workflow tools (PR management, cleanup)",
            "commands": {
                "review-list": "List review threads for a pull request",
                "review-bulk-reply": "Bulk reply to review threads from JSON",
                "review-delete": "Delete review comments from JSON",
                "batch-review": "Perform batch review operations for AI",
                "workflow-status": "Get current workflow status for AI",
                "setup-ai-guidelines": "Set up AI development guidelines",
            },
        },
    }


@learn_app.command("commands")
def learn_commands(
    category: Annotated[
        Optional[str],
        typer.Option("--category", "-c", help="Show commands for specific category"),
    ] = None,
    detailed: Annotated[
        bool,
        typer.Option("--detailed", "-d", help="Show detailed command information"),
    ] = False,
) -> None:
    """Show overview of available commands."""
    try:
        command_info = get_command_info()

        if category:
            if category not in command_info:
                typer.echo(f"❌ Unknown category '{category}'", err=True)
                typer.echo(f"Available categories: {', '.join(command_info.keys())}")
                raise typer.Exit(1)

            cat_info = command_info[category]
            typer.echo(f"\n📚 {cat_info['name']}")
            typer.echo(f"   {cat_info['description']}")
            typer.echo("\n🔧 Commands:")

            for cmd, desc in cat_info["commands"].items():
                if detailed:
                    typer.echo(f"   • {category}.{cmd:<15} - {desc}")
                else:
                    typer.echo(f"   • {cmd:<15} - {desc}")
        else:
            typer.echo("\n📚 ML Playground Tools Overview")
            typer.echo("=" * 50)

            for cat_name, cat_info in command_info.items():
                typer.echo(f"\n🔧 {cat_info['name']}")
                typer.echo(f"   {cat_info['description']}")

                if detailed:
                    typer.echo("\n   Commands:")
                    for cmd, desc in cat_info["commands"].items():
                        typer.echo(f"   • {cat_name}.{cmd:<15} - {desc}")
                else:
                    commands = list(cat_info["commands"].keys())
                    typer.echo(
                        f"   Commands: {', '.join(commands[:4])}"
                        + (f", {commands[4]}..." if len(commands) > 4 else "")
                    )

        if not detailed:
            typer.echo("\n💡 Use --detailed for full command descriptions")
            typer.echo("💡 Use --category <name> to focus on specific tools")

    except Exception as e:
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(1)


@learn_app.command("explain")
def learn_explain(
    command: Annotated[
        str, typer.Argument(help="Command in format 'category.command'")
    ],
) -> None:
    """Explain a specific command with best practices."""
    try:
        if "." not in command:
            typer.echo("❌ Command must be in format 'category.command'", err=True)
            typer.echo("Example: tools learn explain quality.lint")
            raise typer.Exit(1)

        category, cmd_name = command.split(".", 1)
        command_info = get_command_info()

        if category not in command_info:
            typer.echo(f"❌ Unknown category '{category}'", err=True)
            raise typer.Exit(1)

        cat_commands = command_info[category]["commands"]
        if cmd_name not in cat_commands:
            typer.echo(
                f"❌ Unknown command '{cmd_name}' in category '{category}'", err=True
            )
            typer.echo(f"Available commands: {', '.join(cat_commands.keys())}")
            raise typer.Exit(1)

        description = cat_commands[cmd_name]

        typer.echo(f"\n🔧 Command: {command}")
        typer.echo("=" * 50)
        typer.echo(f"\n📝 Description: {description}")

        # Generate best practices based on command type
        typer.echo("\n💡 Best Practices:")

        if category == "quality":
            if "lint" in cmd_name:
                typer.echo("   • Run lint before committing changes")
                typer.echo("   • Use --fix to auto-fix fixable issues")
                typer.echo("   • Configure .ruff.toml for project-specific rules")
            elif "format" in cmd_name:
                typer.echo("   • Format before committing changes")
                typer.echo("   • Use pre-commit hooks to enforce formatting")
                typer.echo("   • Configure formatters in pyproject.toml")
            elif "typecheck" in cmd_name:
                typer.echo("   • Run typecheck before CI/CD")
                typer.echo("   • Use strict type annotations for better safety")
                typer.echo("   • Configure mypy/basedpyright settings")

        elif category == "test":
            if "coverage" in cmd_name:
                typer.echo("   • Aim for 80%+ line coverage")
                typer.echo("   • Focus on business logic, not implementation details")
                typer.echo("   • Use coverage to find untested code paths")
            elif "unit" in cmd_name:
                typer.echo("   • Test one thing per test")
                typer.echo("   • Use descriptive test names")
                typer.echo("   • Mock external dependencies")

        elif category == "env":
            if "setup" in cmd_name:
                typer.echo("   • Use fresh environments for each project")
                typer.echo("   • Pin dependency versions for reproducibility")
                typer.echo("   • Use uv for fast dependency management")

        typer.echo("\n🔗 Related Concepts:")
        if category == "quality":
            typer.echo(
                "   • Code quality standards • Pre-commit hooks • CI/CD integration"
            )
        elif category == "test":
            typer.echo("   • Test-driven development • Mocking • Coverage analysis")
        elif category == "env":
            typer.echo(
                "   • Virtual environments • Dependency management • Reproducibility"
            )
        else:
            typer.echo("   • Development workflow • Automation • Best practices")

        typer.echo(f"\n💡 Usage: tools {category} {cmd_name}")

    except Exception as e:
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(1)


@learn_app.command("best-practices")
def learn_best_practices(
    category: Annotated[
        Optional[str],
        typer.Option(
            "--category", "-c", help="Show best practices for specific category"
        ),
    ] = None,
) -> None:
    """Show best practices for development workflow."""
    try:
        command_info = get_command_info()

        if category:
            if category not in command_info:
                typer.echo(f"❌ Unknown category '{category}'", err=True)
                typer.echo(f"Available categories: {', '.join(command_info.keys())}")
                raise typer.Exit(1)

            cat_name = command_info[category]["name"]
            typer.echo(f"\n📚 {cat_name} Best Practices")
            typer.echo("=" * 50)

            # Category-specific best practices
            if category == "quality":
                typer.echo("\n🔧 Code Quality Best Practices:")
                typer.echo("   • Run quality checks before every commit")
                typer.echo("   • Use pre-commit hooks to enforce standards")
                typer.echo("   • Configure tools for project-specific needs")
                typer.echo("   • Focus on consistency and readability")
                typer.echo("   • Address warnings and errors promptly")

            elif category == "test":
                typer.echo("\n🧪 Testing Best Practices:")
                typer.echo("   • Write tests before fixing bugs (TDD)")
                typer.echo("   • Test behavior, not implementation")
                typer.echo("   • Use descriptive test names and docstrings")
                typer.echo("   • Mock external dependencies")
                typer.echo("   • Aim for high coverage of business logic")

            elif category == "env":
                typer.echo("\n🌍 Environment Best Practices:")
                typer.echo("   • Use isolated virtual environments")
                typer.echo("   • Pin dependency versions")
                typer.echo("   • Keep environments clean and minimal")
                typer.echo("   • Use uv for fast dependency management")
                typer.echo("   • Document environment setup steps")

            elif category == "ci":
                typer.echo("\n🚀 CI/CD Best Practices:")
                typer.echo("   • Run quality gates in CI")
                typer.echo("   • Fail fast on quality issues")
                typer.echo("   • Use coverage thresholds")
                typer.echo("   • Test on multiple Python versions")
                typer.echo("   • Keep CI pipelines fast and reliable")

            elif category == "dev":
                typer.echo("\n👨‍💻 Development Best Practices:")
                typer.echo("   • Review code changes thoroughly")
                typer.echo("   • Use descriptive commit messages")
                typer.echo("   • Keep PRs focused and small")
                typer.echo("   • Document decisions and trade-offs")
                typer.echo("   • Use AI tools to assist, not replace thinking")

        else:
            typer.echo("\n📚 ML Playground Development Best Practices")
            typer.echo("=" * 55)

            typer.echo("\n🔧 General Workflow:")
            typer.echo("   1. Set up environment: tools env setup")
            typer.echo("   2. Make changes with quality checks: tools quality lint")
            typer.echo("   3. Run tests: tools test unit")
            typer.echo("   4. Check coverage: tools test coverage")
            typer.echo("   5. Run full quality gate: tools ci quality-fast")

            typer.echo("\n💡 Core Principles:")
            typer.echo("   • Quality first - lint, format, typecheck everything")
            typer.echo("   • Test thoroughly - unit, integration, coverage")
            typer.echo("   • Automate workflows - CI/CD, pre-commit hooks")
            typer.echo(
                "   • Document decisions - clear commit messages, PR descriptions"
            )
            typer.echo("   • Keep environments clean - isolated, reproducible setups")

            typer.echo("\n🔗 Tool Categories:")
            for cat_name, cat_info in command_info.items():
                typer.echo(f"   • {cat_name:<8} - {cat_info['description']}")

        typer.echo("\n💡 Use --category <name> for category-specific practices")

    except Exception as e:
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(1)


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add subcommands to main app
app.add_typer(quality_app, name="quality")
app.add_typer(test_app, name="test")
app.add_typer(env_app, name="env")
app.add_typer(ci_app, name="ci")
app.add_typer(dev_app, name="dev")
app.add_typer(learn_app, name="learn")


@app.callback()
def main(
    learning_mode: Annotated[
        Optional[bool],
        typer.Option(
            "--learning-mode/--no-learning-mode",
            help="Enable learning mode to show underlying commands and explanations",
        ),
    ] = None,
    verbosity: Annotated[
        Optional[int],
        typer.Option(
            "--verbosity",
            "-v",
            min=0,
            max=2,
            help="Learning mode verbosity: 0=minimal, 1=standard, 2=comprehensive",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run", help="Show what would be done without executing commands"
        ),
    ] = False,
    project_root: Annotated[
        Optional[Path],
        typer.Option(
            "--project-root",
            help="Path to project root (auto-detected if not specified)",
        ),
    ] = None,
) -> None:
    """ML Playground unified development tools.

    Provides a single entry point for all development tooling including
    quality checks, testing, environment management, CI operations, and
    AI-assisted development workflows.

    Use --learning-mode to see underlying commands and educational explanations.
    """
    # Load configuration first (unit tests expect this to happen at entry)
    deps = get_tools_dependencies()
    load_config_with_error_handling(project_root, deps=deps)

    # Set global options, preferring explicit CLI args over config defaults
    if learning_mode is not None:
        state.learning_mode = learning_mode
        state.mark_learning_mode_explicit(True)

    if verbosity is not None:
        state.verbosity = verbosity

    state.dry_run = dry_run
    if state.dry_run:
        os.environ["ML_PLAYGROUND_TOOLS_DRY_RUN"] = "1"
    else:
        os.environ.pop("ML_PLAYGROUND_TOOLS_DRY_RUN", None)


@app.command("version")
def version() -> None:
    """Show version information."""
    typer.echo("ML Playground Tools v0.1.0")
    typer.echo("Unified development tooling for ML Playground")


@app.command("config")
def show_config() -> None:
    """Show current configuration."""
    ensure_config_loaded()
    assert state.config is not None, (
        "Config should be loaded after _ensure_config_loaded"
    )

    typer.echo("Current tools configuration:")
    typer.echo(f"  Learning mode default: {state.config.learning_mode_default}")
    typer.echo(f"  Default verbosity: {state.config.default_verbosity}")
    typer.echo(f"  Project root: {state.project_root or 'auto-detected'}")
    typer.echo("")
    typer.echo("Tool categories:")
    typer.echo(
        f"  Quality tools: {'enabled' if state.config.quality.enabled else 'disabled'}"
    )
    typer.echo(
        f"  Testing tools: {'enabled' if state.config.testing.enabled else 'disabled'}"
    )
    typer.echo(
        f"  Environment tools: {'enabled' if state.config.environment.enabled else 'disabled'}"
    )
    typer.echo(f"  CI tools: {'enabled' if state.config.ci.enabled else 'disabled'}")


def main_entry() -> None:
    """Main entry point for the tools CLI."""
    try:
        app()
    except KeyboardInterrupt:
        typer.echo("\nOperation cancelled by user", err=True)
        raise typer.Exit(1)
    except typer.Exit:
        # Let Typer exit codes propagate properly
        raise
    except (ToolExecutionError, ToolConfigurationError) as e:
        typer.echo(f"Tool error: {e}", err=True)
        raise typer.Exit(1)
    except Exception as e:
        # Fallback for truly unexpected errors
        typer.echo(f"Unexpected error: {e}", err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    main_entry()
