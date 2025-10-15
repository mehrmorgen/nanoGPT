"""Tests for the Connect Four trainer integration."""

from __future__ import annotations

from pathlib import Path

import pytest

from ml_playground.experiments.connect_four.trainer import ConnectFourTrainer
from tests.support.config_builders import create_basic_configs


def test_connect_four_trainer_uses_shared(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _, trainer_cfg, _, shared = create_basic_configs(tmp_path)
    captured: dict[str, object] = {}

    class _FakeTrainer:
        def __init__(self, cfg, shared_cfg) -> None:  # noqa: D401
            captured["cfg"] = cfg
            captured["shared"] = shared_cfg

        def run(self) -> None:  # noqa: D401
            captured["ran"] = True

    monkeypatch.setattr(
        "ml_playground.experiments.connect_four.trainer._CoreTrainer",
        _FakeTrainer,
    )

    trainer = ConnectFourTrainer()
    report = trainer.train(trainer_cfg, shared)

    assert captured["cfg"] is trainer_cfg
    assert captured["shared"] is shared
    assert captured.get("ran") is True
    assert "[connect_four]" in " ".join(report.messages)


def test_connect_four_trainer_builds_default_shared(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _, trainer_cfg, _, _ = create_basic_configs(tmp_path)
    new_out = tmp_path / "alt_out"
    runtime = trainer_cfg.runtime.model_copy(update={"out_dir": new_out})
    trainer_cfg = trainer_cfg.model_copy(update={"runtime": runtime})

    captured: dict[str, object] = {}

    class _FakeTrainer:
        def __init__(self, cfg, shared_cfg) -> None:  # noqa: D401
            captured["shared"] = shared_cfg

        def run(self) -> None:  # noqa: D401
            captured["ran"] = True

    monkeypatch.setattr(
        "ml_playground.experiments.connect_four.trainer._CoreTrainer",
        _FakeTrainer,
    )

    trainer = ConnectFourTrainer()
    trainer.train(trainer_cfg)

    shared_cfg = captured["shared"]
    assert shared_cfg.train_out_dir == new_out
    exp_dir = (
        Path(__file__).resolve().parents[4]
        / "ml_playground"
        / "experiments"
        / "connect_four"
    )
    assert shared_cfg.dataset_dir == exp_dir / "datasets"
    assert captured.get("ran") is True
