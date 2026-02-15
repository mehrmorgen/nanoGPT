from __future__ import annotations

import tomllib
from pathlib import Path

from ml_playground.runtime_cli.main import main


def _write_exp_config(tmp_dir: Path, out_dir: Path, dataset_dir: Path) -> Path:
    import tomli_w

    base_config_path = (
        Path(__file__).resolve().parents[4]
        / "src"
        / "ml_playground"
        / "experiments"
        / "copy_stage1"
        / "test_config.toml"
    )
    with base_config_path.open("rb") as file:
        config = tomllib.load(file)

    dataset_dir_str = str(dataset_dir)
    out_dir_str = str(out_dir)

    prepare = config.setdefault("prepare", {})
    training = config.setdefault("training", {})
    sampling = config.setdefault("sampling", {})
    metadata = config.setdefault("metadata", {})

    if not isinstance(prepare, dict):
        prepare = {}
        config["prepare"] = prepare
    if not isinstance(training, dict):
        training = {}
        config["training"] = training
    if not isinstance(sampling, dict):
        sampling = {}
        config["sampling"] = sampling
    if not isinstance(metadata, dict):
        metadata = {}
        config["metadata"] = metadata

    prepare["dataset_dir"] = dataset_dir_str
    prepare["raw_dir"] = dataset_dir_str
    prepare["raw_text_path"] = str(dataset_dir / "input.txt")

    training_runtime = training.setdefault("runtime", {})
    if not isinstance(training_runtime, dict):
        training_runtime = {}
        training["runtime"] = training_runtime
    training_runtime["out_dir"] = out_dir_str

    sampling_runtime = sampling.setdefault("runtime", {})
    if not isinstance(sampling_runtime, dict):
        sampling_runtime = {}
        sampling["runtime"] = sampling_runtime
    sampling_runtime["out_dir"] = out_dir_str

    metadata["dataset_dir"] = dataset_dir_str
    metadata["train_out_dir"] = out_dir_str
    metadata["sample_out_dir"] = out_dir_str

    path = tmp_dir / "copy_stage1_test_config.toml"
    with path.open("wb") as file:
        tomli_w.dump(config, file)
    return path


def test_e2e_copy_stage1_prepare_train_sample(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    dataset_dir = tmp_path / "datasets"
    cfg = _write_exp_config(tmp_path, out_dir, dataset_dir)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    (dataset_dir / "input.txt").write_text("AB" * 320, encoding="utf-8")

    main(["--exp-config", str(cfg), "prepare", "copy_stage1"])

    assert (dataset_dir / "train.bin").exists()
    assert (dataset_dir / "val.bin").exists()
    assert (dataset_dir / "meta.pkl").exists()

    main(["--exp-config", str(cfg), "train", "copy_stage1"])

    assert any(out_dir.glob("ckpt_last_*.pt"))
    assert any(out_dir.glob("*.pt"))
    assert (out_dir / "meta.pkl").exists()

    main(["--exp-config", str(cfg), "sample", "copy_stage1"])
