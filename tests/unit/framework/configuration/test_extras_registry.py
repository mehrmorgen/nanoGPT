from __future__ import annotations


from importlib import import_module
from ml_playground.framework.configuration import loading as config_loading


def test_load_extras_models_registers_known_experiment() -> None:
    extras_registry = import_module(
        "ml_playground.framework.experiment_registry.extras_registry"
    )
    extras_registry.load_extras_models("speakger")
    assert extras_registry.get_extras_model("speakger", "prepare") is not None
    assert extras_registry.get_extras_model("speakger", "training") is not None
    assert extras_registry.get_extras_model("speakger", "sampling") is not None


def test_load_extras_models_ignores_missing_module() -> None:
    extras_registry = import_module(
        "ml_playground.framework.experiment_registry.extras_registry"
    )
    extras_registry.load_extras_models("unknown_experiment")
    assert extras_registry.get_extras_model("unknown_experiment", "prepare") is None


def test_load_experiment_toml_validates_extras_models() -> None:
    cfg_path = (
        config_loading._package_root()
        / "experiments"
        / "bundestag_qwen15b_lora_mps"
        / "config.toml"
    )
    cfg = config_loading.load_full_experiment_config(
        cfg_path,
        config_loading._package_root().parent,
        "bundestag_qwen15b_lora_mps",
    )
    assert cfg.training.extras["save_merged_on_best"] is True
    assert cfg.sampling.extras["require_adapters"] is True
