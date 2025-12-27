from __future__ import annotations

from pathlib import Path

from ml_playground.runtime.protocols import (
    DeviceSetup,
    PrepareConfigLike,
    SampleConfigLike,
    SharedConfigLike,
    TrainConfigLike,
)


def test_device_setup_protocol_accepts_required_signature() -> None:
    def setup(
        device: str,
        dtype: str,
        seed: int,
        *,
        cuda_is_available: object | None = None,
        torch_module: object | None = None,
    ) -> None:
        assert device and dtype and isinstance(seed, int)

    assert isinstance(setup, DeviceSetup)  # type: ignore[arg-type]


def test_prepare_config_like_requires_logger() -> None:
    class PrepareCfg:
        def __init__(self) -> None:
            self.logger = object()

    assert isinstance(PrepareCfg(), PrepareConfigLike)  # type: ignore[arg-type]


def test_train_config_like_requires_logger_and_runtime() -> None:
    class TrainCfg:
        def __init__(self) -> None:
            self.logger = object()
            self.runtime = None
            self.data = None
            self.model = None
            self.optim = None
            self.schedule = None

    assert isinstance(TrainCfg(), TrainConfigLike)  # type: ignore[arg-type]


def test_sample_config_like_requires_logger_and_runtime() -> None:
    class SampleCfg:
        def __init__(self) -> None:
            self.logger = object()
            self.runtime = None

    assert isinstance(SampleCfg(), SampleConfigLike)  # type: ignore[arg-type]


def test_shared_config_like_requires_paths(tmp_path: Path) -> None:
    class SharedCfg:
        def __init__(self) -> None:
            self.config_path = tmp_path / "config.toml"
            self.dataset_dir = tmp_path / "dataset"
            self.train_out_dir = tmp_path / "train"
            self.sample_out_dir = tmp_path / "sample"

    assert isinstance(SharedCfg(), SharedConfigLike)  # type: ignore[arg-type]


def test_runtime_checkable_rejects_non_conforming() -> None:
    class NotShared:
        pass

    assert not isinstance(NotShared(), SharedConfigLike)
