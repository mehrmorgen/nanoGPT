from __future__ import annotations

import types
from pathlib import Path

import pytest

from ml_playground.cli import main


def test_main_prepare_shakespeare_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test prepare command with shakespeare dataset succeeds."""
    called = []

    def mock_run_prepare(*args, **kwargs):
        called.append(True)

    monkeypatch.setattr("ml_playground.cli._run_prepare", mock_run_prepare)
    with pytest.raises(SystemExit, match="0"):
        main(["prepare", "shakespeare"])
    assert len(called) == 1


def test_main_prepare_bundestag_char_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test prepare command with bundestag_char dataset succeeds."""
    called = []

    def mock_run_prepare(*args, **kwargs):
        called.append(True)

    monkeypatch.setattr("ml_playground.cli._run_prepare", mock_run_prepare)
    with pytest.raises(SystemExit, match="0"):
        main(["prepare", "bundestag_char"])
    assert len(called) == 1


def test_main_prepare_unknown_dataset_fails() -> None:
    """Test prepare command with an unknown experiment raises SystemExit."""
    with pytest.raises(SystemExit):
        main(["prepare", "unknown"])


def test_main_train_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test train command auto-resolves config for experiment and calls _run_train."""
    called = []

    def mock_run_train(*args, **kwargs):
        called.append(True)

    monkeypatch.setattr("ml_playground.cli._run_train", mock_run_train)
    with pytest.raises(SystemExit, match="0"):
        main(["train", "shakespeare"])
    assert len(called) == 1


def test_main_train_no_train_block_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test train command fails when train block is missing (single-load path)."""
    from ml_playground.config import AppConfig
    from ml_playground.prepare import PreparerConfig
    from pathlib import Path as _P

    def mock_load_app_config(experiment, exp_config):
        cfg_path = _P("/fake/config.toml")
        return (cfg_path, AppConfig(train=None, sample=None), PreparerConfig())

    monkeypatch.setattr("ml_playground.cli.load_app_config", mock_load_app_config)
    with pytest.raises(SystemExit, match="2"):
        main(["train", "shakespeare"])


def test_main_sample_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test sample command auto-resolves config and calls _run_sample."""
    called = []

    def mock_run_sample(*args, **kwargs):
        called.append(True)

    monkeypatch.setattr("ml_playground.cli._run_sample", mock_run_sample)
    with pytest.raises(SystemExit, match="0"):
        main(["sample", "shakespeare"])
    assert len(called) == 1


def test_main_sample_no_sample_block_fails(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test sample command fails when sample block is missing (single-load path)."""
    from ml_playground.config import AppConfig
    from ml_playground.prepare import PreparerConfig
    from pathlib import Path as _P

    def mock_load_app_config(experiment, exp_config):
        cfg_path = _P("/fake/config.toml")
        return (cfg_path, AppConfig(train=None, sample=None), PreparerConfig())

    monkeypatch.setattr("ml_playground.cli.load_app_config", mock_load_app_config)
    with pytest.raises(SystemExit, match="2"):
        main(["sample", "shakespeare"])


def test_main_sample_speakger_delegation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test sample command delegates to speakger module for speakger experiment."""
    called = []

    def mock_sample_from_toml(cfg_path):
        called.append(cfg_path)

    # Create a fake module with the sample_from_toml function
    fake_module = types.ModuleType("fake_speakger_sampler")
    fake_module.sample_from_toml = mock_sample_from_toml

    def mock_import_module(name):
        if name == "ml_playground.experiments.speakger.sampler":
            return fake_module
        raise ImportError(f"No module named '{name}'")

    monkeypatch.setattr("importlib.import_module", mock_import_module)

    with pytest.raises(SystemExit, match="0"):
        main(["sample", "speakger"])

    assert len(called) == 1
    # Check that the config path was passed
    assert isinstance(called[0], Path)


def test_main_loop_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test loop command executes successfully."""
    called = []

    def mock_run_loop(*args, **kwargs):
        called.append(True)

    monkeypatch.setattr("ml_playground.cli._run_loop", mock_run_loop)
    with pytest.raises(SystemExit, match="0"):
        main(["loop", "shakespeare"])
    assert len(called) == 1


def test_main_loop_missing_train_block_fails(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test loop command fails when train block is missing (single-load path)."""
    from ml_playground.config import AppConfig
    from ml_playground.prepare import PreparerConfig
    from pathlib import Path as _P

    def mock_load_app_config(experiment, exp_config):
        cfg_path = _P("/fake/config.toml")
        return (cfg_path, AppConfig(train=None, sample=None), PreparerConfig())

    monkeypatch.setattr("ml_playground.cli.load_app_config", mock_load_app_config)
    with pytest.raises(SystemExit, match="2"):
        main(["loop", "shakespeare"])


def test_main_loop_missing_sample_block_fails(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test loop command fails when sample block is missing (single-load path)."""
    from ml_playground.config import AppConfig, TrainerConfig
    from ml_playground.prepare import PreparerConfig
    from pathlib import Path as _P

    trainer = TrainerConfig(
        model={"n_layer": 2, "n_head": 2, "n_embd": 64, "block_size": 64, "dropout": 0.0, "bias": False},
        data={"dataset_dir": "/fake", "batch_size": 8, "block_size": 64, "grad_accum_steps": 2},
        optim={"learning_rate": 0.0006, "weight_decay": 0.1, "beta1": 0.9, "beta2": 0.95, "grad_clip": 1.0},
        schedule={"decay_lr": True, "warmup_iters": 100, "lr_decay_iters": 2000, "min_lr": 6e-05},
        runtime={"out_dir": "/fake/out", "device": "cpu", "dtype": "float32", "compile": False,
                 "max_iters": 10000, "eval_interval": 20, "eval_iters": 10, "log_interval": 10, "seed": 1337}
    )

    def mock_load_app_config(experiment, exp_config):
        cfg_path = _P("/fake/config.toml")
        return (cfg_path, AppConfig(train=trainer, sample=None), PreparerConfig())

    monkeypatch.setattr("ml_playground.cli.load_app_config", mock_load_app_config)

    with pytest.raises(SystemExit, match="2"):
        main(["loop", "shakespeare"])

# Tests for removed functionality (meta.pkl copying, manual dispatcher) have been removed
# as they are no longer relevant after the refactoring
