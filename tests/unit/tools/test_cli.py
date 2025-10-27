"""Unit tests for tools CLI functionality.

Tests the CLI entry points, command routing, and learning mode integration
without using mocks, following the project's testing guidelines.
"""

from pathlib import Path
from typer.testing import CliRunner

from ml_playground.tools.cli import app, GlobalState, state


class TestGlobalState:
    """Test GlobalState functionality."""

    def test_global_state_initialization(self):
        """Test that global state initializes with correct defaults."""
        test_state = GlobalState()

        assert test_state.learning_mode is False
        assert test_state.verbosity == 1
        assert test_state.dry_run is False
        assert test_state.project_root is None
        assert test_state.config is None

    def test_global_state_modification(self):
        """Test that global state can be modified."""
        test_state = GlobalState()

        test_state.learning_mode = True
        test_state.verbosity = 2
        test_state.dry_run = True
        test_state.project_root = Path("/test/path")

        assert test_state.learning_mode is True
        assert test_state.verbosity == 2
        assert test_state.dry_run is True
        assert test_state.project_root == Path("/test/path")


class TestCLIBasics:
    """Test basic CLI functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()
        # Reset global state before each test
        state.learning_mode = False
        state.verbosity = 1
        state.dry_run = False
        state.project_root = None
        state.config = None

    def test_cli_help(self):
        """Test that CLI shows help when no arguments provided."""
        result = self.runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "ML Playground unified development tools" in result.stdout
        assert "quality" in result.stdout
        assert "test" in result.stdout
        assert "env" in result.stdout
        assert "ci" in result.stdout
        assert "agentic" in result.stdout
        assert "learn" in result.stdout

    def test_cli_version(self):
        """Test version command."""
        result = self.runner.invoke(app, ["version"])

        assert result.exit_code == 0
        assert "ML Playground Tools" in result.stdout
        assert "v0.1.0" in result.stdout

    def test_cli_config_without_config_file(self):
        """Test config command when no config is loaded."""
        result = self.runner.invoke(app, ["config"])

        # Should load default config and show it
        assert result.exit_code == 0
        assert "Current tools configuration:" in result.stdout
        assert "Learning mode default:" in result.stdout
        assert "Tool categories:" in result.stdout

    def test_cli_global_options(self):
        """Test global CLI options parsing."""
        # Test learning mode option
        result = self.runner.invoke(app, ["--learning-mode", "version"])
        assert result.exit_code == 0

        # Test verbosity option
        result = self.runner.invoke(app, ["--verbosity", "2", "version"])
        assert result.exit_code == 0

        # Test dry run option
        result = self.runner.invoke(app, ["--dry-run", "version"])
        assert result.exit_code == 0


class TestLearnCommands:
    """Test learn subcommands."""

    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()

    def test_learn_commands_overview(self):
        """Test learn commands shows overview."""
        result = self.runner.invoke(app, ["learn", "commands"])

        assert result.exit_code == 0
        assert "Command Discovery" in result.stdout
        assert "quality" in result.stdout
        assert "test" in result.stdout
        assert "env" in result.stdout
        assert "ci" in result.stdout
        assert "agentic" in result.stdout
        assert "Quick Start:" in result.stdout

    def test_learn_commands_category_specific(self):
        """Test learn commands for specific category."""
        result = self.runner.invoke(app, ["learn", "commands", "--category", "quality"])

        assert result.exit_code == 0
        assert "Quality Tools" in result.stdout
        assert "lint" in result.stdout
        assert "format" in result.stdout
        assert "typecheck" in result.stdout
        assert "Usage Examples:" in result.stdout

    def test_learn_commands_invalid_category(self):
        """Test learn commands with invalid category."""
        result = self.runner.invoke(app, ["learn", "commands", "--category", "invalid"])

        assert result.exit_code == 1
        # Error messages go to stderr in Typer
        assert (
            "Unknown category 'invalid'" in result.stderr
            or "Unknown category 'invalid'" in result.stdout
        )

    def test_learn_commands_detailed(self):
        """Test learn commands with detailed flag."""
        result = self.runner.invoke(app, ["learn", "commands", "--detailed"])

        assert result.exit_code == 0
        assert "Commands:" in result.stdout

    def test_learn_explain_valid_command(self):
        """Test learn explain for valid command."""
        result = self.runner.invoke(app, ["learn", "explain", "quality.lint"])

        assert result.exit_code == 0
        assert "Command: quality.lint" in result.stdout
        assert "Explanation:" in result.stdout
        assert "Best Practices:" in result.stdout
        assert "Related Concepts:" in result.stdout

    def test_learn_explain_invalid_format(self):
        """Test learn explain with invalid command format."""
        result = self.runner.invoke(app, ["learn", "explain", "invalid"])

        assert result.exit_code == 1
        # Error messages go to stderr in Typer
        assert (
            "Command must be in format 'category.command'" in result.stderr
            or "Command must be in format 'category.command'" in result.stdout
        )

    def test_learn_explain_invalid_category(self):
        """Test learn explain with invalid category."""
        result = self.runner.invoke(app, ["learn", "explain", "invalid.command"])

        assert result.exit_code == 1
        # Error messages go to stderr in Typer
        assert (
            "Unknown category 'invalid'" in result.stderr
            or "Unknown category 'invalid'" in result.stdout
        )

    def test_learn_explain_verbosity_levels(self):
        """Test learn explain with different verbosity levels."""
        # Test minimal verbosity
        result = self.runner.invoke(
            app, ["learn", "explain", "test.unit", "--verbosity", "0"]
        )
        assert result.exit_code == 0

        # Test standard verbosity
        result = self.runner.invoke(
            app, ["learn", "explain", "test.unit", "--verbosity", "1"]
        )
        assert result.exit_code == 0

        # Test comprehensive verbosity
        result = self.runner.invoke(
            app, ["learn", "explain", "test.unit", "--verbosity", "2"]
        )
        assert result.exit_code == 0

    def test_learn_best_practices_overview(self):
        """Test learn best-practices shows overview."""
        result = self.runner.invoke(app, ["learn", "best-practices"])

        assert result.exit_code == 0
        assert "ML Playground Development Best Practices" in result.stdout
        assert "Development Workflow" in result.stdout
        assert "Learning Paths" in result.stdout

    def test_learn_best_practices_category_specific(self):
        """Test learn best-practices for specific category."""
        result = self.runner.invoke(
            app, ["learn", "best-practices", "--category", "quality"]
        )

        assert result.exit_code == 0
        assert "Best Practices for Quality Tools" in result.stdout
        assert "Beginner Path:" in result.stdout
        assert "Intermediate Path:" in result.stdout

    def test_learn_best_practices_invalid_category(self):
        """Test learn best-practices with invalid category."""
        result = self.runner.invoke(
            app, ["learn", "best-practices", "--category", "invalid"]
        )

        assert result.exit_code == 1
        # Error messages go to stderr in Typer
        assert (
            "Unknown category 'invalid'" in result.stderr
            or "Unknown category 'invalid'" in result.stdout
        )

    def test_learn_best_practices_verbosity_levels(self):
        """Test learn best-practices with different verbosity levels."""
        # Test minimal verbosity
        result = self.runner.invoke(
            app, ["learn", "best-practices", "--verbosity", "0"]
        )
        assert result.exit_code == 0

        # Test comprehensive verbosity
        result = self.runner.invoke(
            app, ["learn", "best-practices", "--verbosity", "2"]
        )
        assert result.exit_code == 0
        assert "Advanced Path:" in result.stdout or "Advanced Users:" in result.stdout


class TestCLISubcommands:
    """Test CLI subcommand structure."""

    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()

    def test_quality_subcommand_help(self):
        """Test quality subcommand shows help."""
        result = self.runner.invoke(app, ["quality", "--help"])

        assert result.exit_code == 0
        assert "Code quality tools" in result.stdout
        assert "lint" in result.stdout
        assert "format" in result.stdout
        assert "typecheck" in result.stdout

    def test_test_subcommand_help(self):
        """Test test subcommand shows help."""
        result = self.runner.invoke(app, ["test", "--help"])

        assert result.exit_code == 0
        assert "Testing tools" in result.stdout
        assert "unit" in result.stdout
        assert "integration" in result.stdout
        assert "coverage" in result.stdout

    def test_env_subcommand_help(self):
        """Test env subcommand shows help."""
        result = self.runner.invoke(app, ["env", "--help"])

        assert result.exit_code == 0
        assert "Environment management" in result.stdout
        assert "setup" in result.stdout
        assert "sync" in result.stdout
        assert "clean" in result.stdout

    def test_ci_subcommand_help(self):
        """Test ci subcommand shows help."""
        result = self.runner.invoke(app, ["ci", "--help"])

        assert result.exit_code == 0
        assert "CI/CD operations" in result.stdout
        assert "quality-gate" in result.stdout
        assert "mutation" in result.stdout

    def test_agentic_subcommand_help(self):
        """Test agentic subcommand shows help."""
        result = self.runner.invoke(app, ["agentic", "--help"])

        assert result.exit_code == 0
        assert "AI-assisted development" in result.stdout
        assert "guidelines-setup" in result.stdout
        assert "batch-review" in result.stdout

    def test_learn_subcommand_help(self):
        """Test learn subcommand shows help."""
        result = self.runner.invoke(app, ["learn", "--help"])

        assert result.exit_code == 0
        assert "Learning mode utilities" in result.stdout
        assert "commands" in result.stdout
        assert "explain" in result.stdout
        assert "best-practices" in result.stdout


class TestCLIErrorHandling:
    """Test CLI error handling."""

    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()

    def test_invalid_subcommand(self):
        """Test CLI handles invalid subcommands gracefully."""
        result = self.runner.invoke(app, ["invalid-command"])

        # Typer should show help for invalid commands
        assert result.exit_code != 0

    def test_invalid_global_option_values(self):
        """Test CLI handles invalid global option values."""
        # Test invalid verbosity
        result = self.runner.invoke(app, ["--verbosity", "5", "version"])
        assert result.exit_code != 0

        # Test invalid verbosity (negative)
        result = self.runner.invoke(app, ["--verbosity", "-1", "version"])
        assert result.exit_code != 0


class TestCLIIntegration:
    """Test CLI integration with tool categories."""

    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()

    def test_cli_loads_configuration(self):
        """Test that CLI loads configuration properly."""
        # This test verifies that the CLI can load configuration without errors
        result = self.runner.invoke(app, ["config"])

        assert result.exit_code == 0
        assert "Current tools configuration:" in result.stdout

    def test_cli_help_integration(self):
        """Test that CLI help integrates properly with all subcommands."""
        # Test main help
        result = self.runner.invoke(app, ["--help"])
        assert result.exit_code == 0

        # Test that all expected subcommands are present
        subcommands = ["quality", "test", "env", "ci", "agentic", "learn"]
        for subcommand in subcommands:
            assert subcommand in result.stdout

    def test_cli_learning_mode_integration(self):
        """Test that learning mode flag is properly integrated."""
        # Test that learning mode flag is accepted
        result = self.runner.invoke(app, ["--learning-mode", "--help"])
        assert result.exit_code == 0

        # Test that no-learning-mode flag is accepted
        result = self.runner.invoke(app, ["--no-learning-mode", "--help"])
        assert result.exit_code == 0
