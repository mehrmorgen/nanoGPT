from __future__ import annotations

from pathlib import Path
from typing import Any

from ml_playground.framework.runtime import runners as runtime_runners
from tests.support.config_builders import create_basic_configs


def _noop(*_: Any, **__: Any) -> None:
    return


class _FakeRunner:
    def __init__(self, calls: list[str], label: str) -> None:
        self._calls = calls
        self._label = label

    def run(self) -> None:
        self._calls.append(self._label)


def test_run_prepare_impl_invokes_pipeline(tmp_path: Path) -> None:
    prep_cfg, _, _, metadata = create_basic_configs(tmp_path)
    calls: list[str] = []

    hooks = runtime_runners.RuntimeRunHooks(
        pipeline_factory=lambda *_: _FakeRunner(calls, "prepare"),
        trainer_factory=lambda *_: _FakeRunner(calls, "train"),
        sampler_factory=lambda *_: _FakeRunner(calls, "sample"),
        device_setup=_noop,
        log_status=_noop,
    )

    result = runtime_runners.run_prepare_impl(
        "demo", prep_cfg, tmp_path / "config.toml", metadata, hooks=hooks
    )

    assert result.success is True
    assert calls == ["prepare"]


def test_run_train_impl_uses_resolved_seed(tmp_path: Path) -> None:
    _, train_cfg, _, metadata = create_basic_configs(tmp_path)
    calls: list[str] = []
    seeds: list[int] = []

    def _device_setup(device: str, dtype: str, seed: int, **_kwargs: Any) -> None:
        seeds.append(seed)

    def _resolve_seed(_phase: str, _metadata: object, seed: int) -> int:
        return seed + 1

    hooks = runtime_runners.RuntimeRunHooks(
        pipeline_factory=lambda *_: _FakeRunner(calls, "prepare"),
        trainer_factory=lambda *_: _FakeRunner(calls, "train"),
        sampler_factory=lambda *_: _FakeRunner(calls, "sample"),
        device_setup=_device_setup,
        log_status=_noop,
        resolve_seed=_resolve_seed,
    )

    result = runtime_runners.run_train_impl(
        "demo", train_cfg, tmp_path / "config.toml", metadata, hooks=hooks
    )

    assert result.success is True
    assert calls == ["train"]
    assert seeds == [train_cfg.runtime.seed + 1]


def test_run_sample_impl_invokes_sampler(tmp_path: Path) -> None:
    _, _, sampler_cfg, metadata = create_basic_configs(tmp_path)
    calls: list[str] = []

    hooks = runtime_runners.RuntimeRunHooks(
        pipeline_factory=lambda *_: _FakeRunner(calls, "prepare"),
        trainer_factory=lambda *_: _FakeRunner(calls, "train"),
        sampler_factory=lambda *_: _FakeRunner(calls, "sample"),
        device_setup=_noop,
        log_status=_noop,
    )

    result = runtime_runners.run_sample_impl(
        "demo", sampler_cfg, tmp_path / "config.toml", metadata, hooks=hooks
    )

    assert result.success is True
    assert calls == ["sample"]
