from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping
from contextlib import contextmanager

from ml_playground.framework.configuration.models import (
    ExperimentConfig,
    PreparerConfig,
    RuntimeConfig,
    SamplerConfig,
)
from ml_playground.experiments.shakespeare.preparer import ShakespearePreparer
from ml_playground.experiments.shakespeare.sampler import ShakespeareSampler
from ml_playground.experiments.shakespeare.trainer import ShakespeareTrainer
from tests.support.config_builders import create_basic_configs


class _DummyTokenizer:
    def __init__(self) -> None:
        self._vocab = {"a": 0, "b": 1}

    @property
    def name(self) -> str:
        return "char"

    @property
    def vocab_size(self) -> int:
        return len(self._vocab)

    @property
    def vocab(self) -> Mapping[str, int]:
        return self._vocab

    def encode(self, text: str) -> list[int]:
        return [self._vocab.get(ch, 0) for ch in text]

    def decode(self, token_ids: list[int]) -> str:
        reverse = {val: key for key, val in self._vocab.items()}
        return "".join(reverse.get(token, "a") for token in token_ids)


def test_preparer_contract_respects_overrides_and_writes_artifacts(
    tmp_path: Path,
) -> None:
    base_dir = tmp_path / "experiments" / "shakespeare"
    base_dir.mkdir(parents=True, exist_ok=True)

    def fake_get(_url: str, timeout: int = 30) -> Any:  # noqa: ARG001
        return SimpleNamespace(text="hello", raise_for_status=lambda: None)

    extras = {
        "base_dir": base_dir,
        "tokenizer_factory": _DummyTokenizer,
        "http_get": fake_get,
    }

    cfg = PreparerConfig.model_validate({"extras": extras})
    report = ShakespearePreparer().prepare(cfg)

    ds_dir = base_dir / "datasets"
    assert (ds_dir / "train.bin").exists()
    assert (ds_dir / "val.bin").exists()
    assert (ds_dir / "meta.pkl").exists()
    assert "shakespeare" in " ".join(report.messages)


def test_experiment_config_contract_accepts_tiny_configs(tmp_path: Path) -> None:
    prep_cfg, train_cfg, sample_cfg, shared_cfg = create_basic_configs(tmp_path)
    config = ExperimentConfig(
        prepare=prep_cfg,
        training=train_cfg,
        sampling=sample_cfg,
        metadata=shared_cfg,
    )
    assert config.prepare is prep_cfg
    assert config.training is train_cfg
    assert config.sampling is sample_cfg


def test_trainer_contract_invokes_core_runner(tmp_path: Path) -> None:
    _, train_cfg, _, _ = create_basic_configs(tmp_path)

    called: dict[str, object] = {}

    class _FakeCoreTrainer:
        def __init__(self, cfg: object, shared: object) -> None:
            called["cfg"] = cfg
            called["shared"] = shared

        def run(self) -> None:
            called["ran"] = True

    with override_attr(
        "ml_playground.experiments.shakespeare.trainer",
        "_CoreTrainer",
        _FakeCoreTrainer,
    ):
        report = ShakespeareTrainer().train(train_cfg)

    assert called.get("ran") is True
    assert report.messages


def test_sampler_contract_passes_runtime_seed(tmp_path: Path) -> None:
    _, _, sampler_cfg, _ = create_basic_configs(tmp_path)
    runtime = RuntimeConfig.model_validate(
        {**sampler_cfg.runtime.model_dump(), "seed": 123}
    )
    cfg = SamplerConfig(runtime=runtime, sample=sampler_cfg.sample)

    captured: dict[str, object] = {}

    class _FakeCoreSampler:
        def __init__(self, cfg: SamplerConfig, shared: object) -> None:
            captured["seed"] = cfg.runtime.seed
            captured["shared"] = shared

        def run(self) -> None:
            return

    with override_attr(
        "ml_playground.experiments.shakespeare.sampler",
        "_CoreSampler",
        _FakeCoreSampler,
    ):
        report = ShakespeareSampler().sample(cfg)

    assert captured["seed"] == 123
    assert report.messages


@contextmanager
def override_attr(module_path: str, attr: str, value: object):
    module = __import__(module_path, fromlist=[attr])
    original = getattr(module, attr)
    object.__setattr__(module, attr, value)
    try:
        yield
    finally:
        object.__setattr__(module, attr, original)
