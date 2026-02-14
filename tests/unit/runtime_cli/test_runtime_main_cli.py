from pathlib import Path

from typer.testing import CliRunner

from ml_playground.runtime_cli.main import app


def test_global_options_help() -> None:
    # Set COLUMNS to a fixed value and use a dummy TERM to ensure consistent rendering.
    # We also use NO_COLOR=1 to avoid ANSI codes in the output for easier assertion.
    env = {"COLUMNS": "100", "TERM": "dumb", "NO_COLOR": "1"}
    runner = CliRunner(env=env)
    result = runner.invoke(app, ["--help"], standalone_mode=False)

    assert result.exit_code == 0
    assert "ML Playground CLI" in result.output
    assert "--exp-config" in result.output
    assert "--learning-mode" in result.output


def test_prepare_help() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["prepare", "--help"])
    assert result.exit_code == 0
    assert "prepare" in result.output.lower()


def test_train_help() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["train", "--help"])
    assert result.exit_code == 0
    assert "train" in result.output.lower()


def test_sample_help() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["sample", "--help"])
    assert result.exit_code == 0
    assert "sample" in result.output.lower()


def test_analyze_help() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["analyze", "--help"])
    assert result.exit_code == 0
    assert "analyze" in result.output.lower()


def test_global_options_invalid_config(tmp_path: Path) -> None:
    runner = CliRunner()
    invalid_path = tmp_path / "non_existent.toml"
    result = runner.invoke(app, ["--exp-config", str(invalid_path), "prepare", "demo"])
    assert result.exit_code == 2
    assert "Config file not found" in result.output
