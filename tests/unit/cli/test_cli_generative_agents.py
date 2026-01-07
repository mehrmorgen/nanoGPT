from typer.testing import CliRunner
from src.ml_playground.cli import app

runner = CliRunner()


def test_generative_agents_command():
    """Tests the generative-agents CLI command."""
    result = runner.invoke(app, ["generative-agents", "--num-steps", "1"])
    assert result.exit_code == 0
    assert "--- Step 1 ---" in result.stdout
    assert "Simulation step..." in result.stdout
    assert "Processing agent: John Doe" in result.stdout
