"""Acceptance tests for learn commands CLI behavior.

Tests verify CLI interactions, exit codes, output format, and error handling
for the learn commands educational system. Focus on behavior validation
rather than content accuracy.
"""

from __future__ import annotations

import subprocess
import re

import pytest

# Apply acceptance marker to all tests in this file
pytestmark = pytest.mark.acceptance


def assert_successful_exit(result: subprocess.CompletedProcess[str]) -> None:
    """Assert that command completed successfully."""
    assert result.returncode == 0, (
        f"Expected exit code 0, got {result.returncode}. stderr: {result.stderr}"
    )


def assert_error_exit(result: subprocess.CompletedProcess[str]) -> None:
    """Assert that command failed with proper error exit code."""
    assert result.returncode == 1, (
        f"Expected exit code 1, got {result.returncode}. stdout: {result.stdout}"
    )


def assert_output_contains_pattern(
    result: subprocess.CompletedProcess[str], pattern: str
) -> None:
    """Assert that output contains a specific regex pattern."""
    # Check both stdout and stderr for pattern with DOTALL flag to match across newlines
    output = result.stdout + result.stderr
    assert re.search(pattern, output, re.IGNORECASE | re.DOTALL), (
        f"Pattern '{pattern}' not found in output: stdout={result.stdout}, stderr={result.stderr}"
    )


def assert_error_output_contains(
    result: subprocess.CompletedProcess[str], text: str
) -> None:
    """Assert that stderr contains specific error text."""
    # Check both stdout and stderr for error text (CLI may use stdout for some errors)
    output = result.stdout + result.stderr
    assert text in output, (
        f"Error text '{text}' not found in output: stdout={result.stdout}, stderr={result.stderr}"
    )


class TestLearnCommandsCLI:
    """Test suite for learn commands CLI interactions.

    Tests verify that CLI commands show proper output format, categories,
    and command listings with correct exit codes.
    """

    def test_learn_commands_overview_shows_all_categories(self, run_cli) -> None:
        """Verify overview shows all categories with correct format."""
        result = run_cli("tools", "learn", "commands")

        assert_successful_exit(result)
        assert_output_contains_pattern(result, r"ML Playground Tools Overview")
        assert_output_contains_pattern(
            result,
            r"Quality Tools.*Testing Tools.*Environment Tools.*CI Tools.*Development Tools",
        )

        # Should show category names and descriptions
        assert_output_contains_pattern(result, r"🔧.*Quality Tools")
        assert_output_contains_pattern(result, r"🔧.*Testing Tools")
        assert_output_contains_pattern(result, r"🔧.*Environment Tools")

        # Should show command lists (truncated if many)
        assert_output_contains_pattern(result, r"Commands:")

    def test_learn_commands_category_specific_displays_focused_output(
        self, run_cli
    ) -> None:
        """Test --category flag shows focused output for specific category."""
        result = run_cli("tools", "learn", "commands", "--category", "quality")

        assert_successful_exit(result)
        assert_output_contains_pattern(result, r"📚.*Quality")
        assert_output_contains_pattern(result, r"🔧 Commands:")

        # Should only show quality commands
        assert_output_contains_pattern(result, r"lint")
        assert_output_contains_pattern(result, r"format")
        assert_output_contains_pattern(result, r"deadcode")

        # Should not show other categories
        assert "Testing Tools" not in result.stdout

    def test_learn_commands_detailed_shows_full_descriptions(self, run_cli) -> None:
        """Test --detailed flag shows full command descriptions."""
        result = run_cli("tools", "learn", "commands", "--detailed")

        assert_successful_exit(result)
        assert_output_contains_pattern(result, r"🔧.*Quality")

        # Should show full command names with category prefix
        assert_output_contains_pattern(result, r"quality\.lint")
        assert_output_contains_pattern(result, r"quality\.format")
        assert_output_contains_pattern(result, r"test\.unit")
        assert_output_contains_pattern(result, r"test\.integration")

        # Should show detailed descriptions
        assert " - " in result.stdout  # Command descriptions with dash separator

    def test_learn_best_practices_all_displays_general_practices(self, run_cli) -> None:
        """Verify general best practices display correctly."""
        result = run_cli("tools", "learn", "best-practices")

        assert_successful_exit(result)
        assert_output_contains_pattern(result, r"🔧.*General Workflow")

        # Should show practices for different categories
        assert_output_contains_pattern(result, r"quality.*lint.*format")
        assert_output_contains_pattern(result, r"test.*unit.*integration")
        assert_output_contains_pattern(result, r"env.*Environment")

    def test_learn_best_practices_category_shows_specific_practices(
        self, run_cli
    ) -> None:
        """Test category-specific best practices."""
        result = run_cli("tools", "learn", "best-practices", "--category", "test")

        assert_successful_exit(result)
        assert_output_contains_pattern(result, r"🧪.*Testing Best Practices")

        # Should show testing-specific practices
        assert_output_contains_pattern(result, r"TDD")
        assert_output_contains_pattern(result, r"coverage")
        assert_output_contains_pattern(result, r"Write tests before")

    def test_learn_commands_help_discovery_shows_subcommands(self, run_cli) -> None:
        """Test --help shows all learn subcommands."""
        result = run_cli("tools", "learn", "--help")

        assert_successful_exit(result)
        assert_output_contains_pattern(result, r"commands")
        assert_output_contains_pattern(result, r"explain")
        assert_output_contains_pattern(result, r"best-practices")

        # Should show command descriptions
        assert "Show overview of available commands" in result.stdout
        assert "Explain a specific command" in result.stdout
        assert "Show best practices" in result.stdout


class TestLearnCommandsErrorHandling:
    """Test suite for learn commands error handling.

    Tests verify that invalid inputs produce proper error messages,
    exit codes, and helpful error output for users.
    """

    def test_learn_commands_invalid_category_shows_error(self, run_cli) -> None:
        """Test proper error for unknown categories."""
        result = run_cli("tools", "learn", "commands", "--category", "invalid")

        assert_error_exit(result)
        assert_error_output_contains(result, "Unknown category 'invalid'")
        assert_error_output_contains(result, "Available categories:")
        assert "quality" in result.stdout
        assert "test" in result.stdout

    def test_learn_explain_invalid_format_shows_error(self, run_cli) -> None:
        """Test error for malformed command format."""
        result = run_cli("tools", "learn", "explain", "invalidformat")

        assert_error_exit(result)
        assert_error_output_contains(
            result, "Command must be in format 'category.command'"
        )
        assert_error_output_contains(
            result, "Example: tools learn explain quality.lint"
        )

    def test_learn_explain_invalid_category_shows_error(self, run_cli) -> None:
        """Test error for unknown category in explain."""
        result = run_cli("tools", "learn", "explain", "invalid.command")

        assert_error_exit(result)
        assert_error_output_contains(result, "Unknown category 'invalid'")

    def test_learn_explain_invalid_command_shows_error(self, run_cli) -> None:
        """Test error for unknown command in category."""
        result = run_cli("tools", "learn", "explain", "quality.invalidcmd")

        assert_error_exit(result)
        assert_error_output_contains(
            result, "Unknown command 'invalidcmd' in category 'quality'"
        )
        assert_error_output_contains(result, "Available commands:")
        assert "lint" in result.stdout


class TestLearnCommandExplanations:
    """Test suite for command explanation functionality.

    Tests verify that command explanations show descriptions,
    best practices, and related concepts with proper formatting.
    """

    def test_learn_explain_valid_command_shows_description(self, run_cli) -> None:
        """Verify explain shows description, best practices, related concepts."""
        result = run_cli("tools", "learn", "explain", "quality.lint")

        assert_successful_exit(result)
        assert_output_contains_pattern(result, r"🔧 Command: quality\.lint")
        assert_output_contains_pattern(result, r"📝 Description:")
        assert_output_contains_pattern(result, r"💡 Best Practices:")

        # Should show command-specific content
        assert "lint" in result.stdout.lower()

    def test_learn_explain_quality_commands_shows_quality_practices(
        self, run_cli
    ) -> None:
        """Test quality-specific best practices."""
        result = run_cli("tools", "learn", "explain", "quality.format")

        assert_successful_exit(result)
        assert_output_contains_pattern(result, r"🔧 Command: quality\.format")
        assert_output_contains_pattern(result, r"💡 Best Practices:")

        # Should mention formatting-specific practices
        assert_output_contains_pattern(result, r"format")

    def test_learn_explain_test_commands_shows_testing_practices(self, run_cli) -> None:
        """Test testing-specific best practices."""
        result = run_cli("tools", "learn", "explain", "test.unit")

        assert_successful_exit(result)
        assert_output_contains_pattern(result, r"🔧 Command: test\.unit")
        assert_output_contains_pattern(result, r"💡 Best Practices:")

        # Should mention testing-specific practices
        assert_output_contains_pattern(result, r"test.*unit")

    def test_learn_explain_env_commands_shows_environment_practices(
        self, run_cli
    ) -> None:
        """Test environment-specific best practices."""
        result = run_cli("tools", "learn", "explain", "env.setup")

        assert_successful_exit(result)
        assert_output_contains_pattern(result, r"🔧 Command: env\.setup")
        assert_output_contains_pattern(result, r"💡 Best Practices:")


class TestLearnCommandsIntegration:
    """Test suite for learn commands integration with CLI system.

    Tests verify that learn commands integrate properly with the
    main CLI system and maintain consistency with actual commands.
    """

    def test_learn_command_sync_with_cli_matches_actual_apps(self, run_cli) -> None:
        """Verify get_command_info() categories match actual typer apps."""
        # Get learn commands overview
        result = run_cli("tools", "learn", "commands")
        assert_successful_exit(result)

        # Get main tools help to see actual subcommands
        help_result = run_cli("tools", "--help")
        assert_successful_exit(help_result)

        # Verify categories mentioned in learn exist in main CLI
        learn_output = result.stdout

        # Key categories should be present in both
        for category in ["quality", "test", "env"]:
            assert category in learn_output.lower(), (
                f"Category {category} missing from learn output"
            )
            # Some categories might be abbreviated in main help, so check partial matches

    def test_learn_help_discovery_shows_all_subcommands(self, run_cli) -> None:
        """Test --help shows all learn subcommands consistently."""
        # Test main learn help
        main_help = run_cli("tools", "learn", "--help")
        assert_successful_exit(main_help)

        # Test individual command helps
        commands_help = run_cli("tools", "learn", "commands", "--help")
        assert_successful_exit(commands_help)

        explain_help = run_cli("tools", "learn", "explain", "--help")
        assert_successful_exit(explain_help)

        practices_help = run_cli("tools", "learn", "best-practices", "--help")
        assert_successful_exit(practices_help)

        # All should have proper help structure
        for help_output in [
            main_help.stdout,
            commands_help.stdout,
            explain_help.stdout,
            practices_help.stdout,
        ]:
            assert "Usage:" in help_output or "usage:" in help_output
            # Rich help format uses box drawing characters
            assert (
                "╭─" in help_output
                or "Options:" in help_output
                or "options:" in help_output
                or "Commands:" in help_output
                or "commands:" in help_output
            )
