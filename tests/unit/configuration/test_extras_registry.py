from __future__ import annotations


from ml_playground.configuration import loading as config_loading
from ml_playground.experiments.extras_registry import (
    get_extras_model,
    load_extras_models,
)


def test_load_extras_models_registers_known_experiment() -> None:
    load_extras_models("speakger")
    assert get_extras_model("speakger", "prepare") is not None
    assert get_extras_model("speakger", "train") is not None
    assert get_extras_model("speakger", "sample") is not None


def test_load_extras_models_ignores_missing_module() -> None:
    load_extras_models("unknown_experiment")
    assert get_extras_model("unknown_experiment", "prepare") is None


def test_load_experiment_toml_validates_extras_models() -> None:
    cfg_path = (
        config_loading._package_root()
        / "experiments"
        / "bundestag_qwen15b_lora_mps"
        / "config.toml"
    )
    cfg = config_loading.load_experiment_toml(cfg_path)
    assert cfg.train.extras["save_merged_on_best"] is True
    assert cfg.sample.extras["require_adapters"] is True
