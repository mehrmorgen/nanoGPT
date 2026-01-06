from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import BaseModel, ConfigDict, ValidationError

from ml_playground.configuration import models as config_models
from ml_playground.configuration.models import (
    DataConfig,
    ExperimentConfig,
    LRSchedule,
    ModelConfig,
    OptimConfig,
    PreparerConfig,
    RuntimeConfig,
    SampleConfig,
    TrainerConfig,
)
from ml_playground.configuration import cli as config_cli
from ml_playground.configuration import loading as config_loading
from ml_playground.configuration.merge_utils import merge_mappings
from ml_playground.experiments.extras_registry import register_extras_model
from tests.conftest import minimal_full_experiment_toml


def test_full_loader_when_valid_config_then_roundtrips(tmp_path: Path) -> None:
    """Full loader when valid config then roundtrips."""
    toml_text = minimal_full_experiment_toml(
        dataset_dir=Path("data/shakespeare"),
        out_dir=Path("out/test_next"),
        extra_optim="learning_rate = 0.001",
        extra_train="max_iters = 1",
        extra_sample="",
        extra_sample_sample="",
    )
    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text(toml_text)
    project_home = tmp_path.parent if tmp_path.parent.name else tmp_path
    experiment_name = cfg_path.parent.name
    exp: ExperimentConfig = config_loading.load_full_experiment_config(
        cfg_path, project_home, experiment_name
    )
    assert exp.train is not None
    assert exp.sample is not None
    assert isinstance(exp.train.runtime.out_dir, Path)
    assert isinstance(exp.shared.dataset_dir, Path)


def test_read_toml_dict_when_missing_then_raises(tmp_path: Path) -> None:
    """Read toml dict when missing then raises."""
    missing_path = tmp_path / "missing.toml"
    with pytest.raises(FileNotFoundError):
        config_loading.read_toml_dict(missing_path)


def test_get_default_config_path_when_root_none_then_uses_package_root() -> None:
    """get_default_config_path with None should use package root."""
    path = config_loading.get_default_config_path(None)
    assert path.name == "default_config.toml"
    assert (
        str(path)
        .replace("\\", "/")
        .endswith("src/ml_playground/experiments/default_config.toml")
    )


def test_get_default_config_path_when_root_provided_then_uses_root(
    tmp_path: Path,
) -> None:
    """get_default_config_path with explicit root should use that root."""
    path = config_loading.get_default_config_path(tmp_path)
    assert (
        path
        == tmp_path / "src" / "ml_playground" / "experiments" / "default_config.toml"
    )


def test_default_config_path_when_root_is_src_then_uses_src_layout(
    tmp_path: Path,
) -> None:
    """_default_config_path_from_root should handle roots named 'src'."""
    src_root = tmp_path / "src"
    src_root.mkdir()
    resolved = config_loading.get_default_config_path(src_root)
    assert (
        resolved == src_root / "ml_playground" / "experiments" / "default_config.toml"
    )


def test_get_cfg_path_when_no_override_then_returns_default(tmp_path: Path) -> None:
    """Get cfg path when no override then returns default."""
    expected = config_loading._package_root() / "experiments" / "demo" / "config.toml"
    result = config_loading.get_cfg_path("demo", None)
    assert result == expected


def test_list_experiments_with_config_when_configs_exist_then_returns_sorted_names(
    tmp_path: Path,
) -> None:
    """list_experiments_with_config should return sorted experiment names with config.toml."""
    # Create fake experiments directory structure
    experiments_root = tmp_path / "src" / "ml_playground" / "experiments"
    experiments_root.mkdir(parents=True)

    # Create experiments with config.toml
    (experiments_root / "exp_a").mkdir()
    (experiments_root / "exp_a" / "config.toml").write_text("")
    (experiments_root / "exp_c").mkdir()
    (experiments_root / "exp_c" / "config.toml").write_text("")
    (experiments_root / "exp_b").mkdir()
    (experiments_root / "exp_b" / "config.toml").write_text("")

    # Create experiment without config.toml (should be excluded)
    (experiments_root / "exp_no_config").mkdir()

    # Mock the package root
    result = config_loading.list_experiments_with_config(
        experiments_root=experiments_root
    )
    assert result == ["exp_a", "exp_b", "exp_c"]


def test_list_experiments_with_config_when_prefix_given_then_filters_names(
    tmp_path: Path,
) -> None:
    """list_experiments_with_config should filter by prefix."""
    experiments_root = tmp_path / "src" / "ml_playground" / "experiments"
    experiments_root.mkdir(parents=True)

    (experiments_root / "bundestag_char").mkdir()
    (experiments_root / "bundestag_char" / "config.toml").write_text("")
    (experiments_root / "bundestag_tiktoken").mkdir()
    (experiments_root / "bundestag_tiktoken" / "config.toml").write_text("")
    (experiments_root / "shakespeare").mkdir()
    (experiments_root / "shakespeare" / "config.toml").write_text("")

    result = config_loading.list_experiments_with_config(
        "bundestag", experiments_root=experiments_root
    )
    assert result == ["bundestag_char", "bundestag_tiktoken"]


def test_list_experiments_with_config_when_root_missing_then_returns_empty() -> None:
    """list_experiments_with_config should return empty list if experiments root doesn't exist."""
    missing_root = Path("/nonexistent/path/loading")
    result = config_loading.list_experiments_with_config(experiments_root=missing_root)
    assert result == []


def test_list_experiments_with_config_when_iterdir_fails_then_returns_empty(
    tmp_path: Path,
) -> None:
    """list_experiments_with_config should return empty list on OSError."""
    experiments_root = tmp_path / "src" / "ml_playground" / "experiments"
    experiments_root.mkdir(parents=True)

    class BrokenPath(type(experiments_root)):  # type: ignore[misc]
        def iterdir(self):  # type: ignore[override]
            raise OSError("Simulated error")

    broken_root = BrokenPath(experiments_root)

    result = config_loading.list_experiments_with_config(experiments_root=broken_root)
    assert result == []


def test_load_and_merge_configs_when_missing_then_raises(tmp_path: Path) -> None:
    """_load_and_merge_configs should raise FileNotFoundError for missing config."""
    missing_path = tmp_path / "missing.toml"
    with pytest.raises(FileNotFoundError, match="Config file not found"):
        config_loading._load_and_merge_configs(missing_path, tmp_path, "test")


def test_load_prepare_config_when_valid_then_returns_config(tmp_path: Path) -> None:
    """load_prepare_config should load and validate prepare config."""
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text("""
[prepare]
tokenizer_type = "char"
""")

    # Create default config
    default_path = (
        tmp_path / "src" / "ml_playground" / "experiments" / "default_config.toml"
    )
    default_path.parent.mkdir(parents=True)
    default_path.write_text("")

    cfg = config_loading.load_prepare_config(cfg_path, default_config_path=default_path)
    assert isinstance(cfg, PreparerConfig)
    assert cfg.tokenizer_type == "char"
    assert "provenance" in cfg.extras


def test_load_prepare_config_when_missing_section_then_raises(tmp_path: Path) -> None:
    """load_prepare_config should raise ValueError if [prepare] section is missing."""
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text("[train]\n")

    default_path = (
        tmp_path / "src" / "ml_playground" / "experiments" / "default_config.toml"
    )
    default_path.parent.mkdir(parents=True)
    default_path.write_text("")

    with pytest.raises(ValueError, match="must contain a \\[prepare\\] section"):
        config_loading.load_prepare_config(cfg_path, default_config_path=default_path)


def test_load_train_config_when_loaded_then_sets_provenance(tmp_path: Path) -> None:
    """Load train config when loaded then sets provenance."""
    config = tmp_path / "train.toml"
    config.write_text(
        """
[train]
[train.runtime]
out_dir = "./out"
[train.model]
[train.data]
[train.optim]
[train.schedule]
"""
    )

    default_config = tmp_path / "default.toml"
    default_config.write_text("")

    cfg = config_loading.load_train_config(config, default_config_path=default_config)

    provenance = cfg.extras.get("provenance", {})
    assert provenance.get("raw") is not None
    assert provenance.get("context", {}).get("config_path") == str(config)


def test_load_sample_config_when_loaded_then_sets_provenance(tmp_path: Path) -> None:
    """Load sample config when loaded then sets provenance."""
    config = tmp_path / "sample.toml"
    config.write_text(
        """
[sample]
[sample.runtime]
out_dir = "./out"
[sample.sample]

[train]
[train.runtime]
out_dir = "./train"
[train.model]
[train.data]
[train.optim]
[train.schedule]
"""
    )

    default_config = tmp_path / "default.toml"
    default_config.write_text("")

    cfg = config_loading.load_sample_config(config, default_config_path=default_config)

    provenance = cfg.extras.get("provenance", {})
    assert provenance.get("raw") is not None
    assert provenance.get("context", {}).get("config_path") == str(config)


def test_load_train_config_when_section_not_mapping_then_raises(tmp_path: Path) -> None:
    """Load train config when section not mapping then raises."""
    config = tmp_path / "train_invalid.toml"
    config.write_text("train = 'value'\n")

    default_config = tmp_path / "default.toml"
    default_config.write_text("")

    with pytest.raises(TypeError, match="\\[train\\] section"):
        config_loading.load_train_config(config, default_config_path=default_config)


def test_load_sample_config_when_sample_missing_then_raises(tmp_path: Path) -> None:
    """Load sample config when sample missing then raises."""
    config = tmp_path / "sample_invalid.toml"
    config.write_text("[train]\n[train.runtime]\nout_dir='.'\n")

    default_config = tmp_path / "default.toml"
    default_config.write_text("")

    with pytest.raises(ValueError, match=r"must contain a \[sample\] section"):
        config_loading.load_sample_config(config, default_config_path=default_config)


def test_read_toml_dict_when_file_exists_then_returns_mapping(tmp_path: Path) -> None:
    """Read toml dict when file exists then returns mapping."""
    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text("key = 'value'", encoding="utf-8")
    data = config_loading.read_toml_dict(cfg_path)
    assert data == {"key": "value"}


def test_read_toml_dict_when_root_not_mapping_then_raises(tmp_path: Path) -> None:
    """Read toml dict when root not mapping then raises."""
    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text("key = 'value'", encoding="utf-8")

    def fake_loads(_: str) -> list[int]:
        return [1, 2, 3]

    with pytest.raises(TypeError, match="must be a mapping"):
        config_loading.read_toml_dict(cfg_path, toml_loader=fake_loads)


def test_read_toml_dict_when_toml_invalid_then_raises(tmp_path: Path) -> None:
    """Read toml dict when toml invalid then raises."""
    cfg_path = tmp_path / "broken.toml"
    cfg_path.write_text("not = [", encoding="utf-8")

    with pytest.raises(Exception, match="broken.toml"):
        config_loading.read_toml_dict(cfg_path)


def test_full_loader_when_empty_config_then_raises(tmp_path: Path) -> None:
    """Full loader when empty config then raises."""
    toml_text = ""
    cfg_path = tmp_path / "empty.toml"
    cfg_path.write_text(toml_text)
    project_home = tmp_path.parent if tmp_path.parent.name else tmp_path
    experiment_name = cfg_path.parent.name
    with pytest.raises(Exception):
        config_loading.load_full_experiment_config(
            cfg_path, project_home, experiment_name
        )


def test_full_loader_when_root_not_mapping_then_raises(tmp_path: Path) -> None:
    """Full loader when root not mapping then raises."""
    bad_text = """
arr = [1,2,3]
"""
    cfg_path = tmp_path / "bad.toml"
    cfg_path.write_text(bad_text)
    project_home = tmp_path.parent if tmp_path.parent.name else tmp_path
    experiment_name = cfg_path.parent.name
    with pytest.raises(Exception):
        config_loading.load_full_experiment_config(
            cfg_path, project_home, experiment_name
        )


def test_full_loader_when_sample_has_unknown_keys_then_raises(tmp_path: Path) -> None:
    """Full loader when sample has unknown keys then raises."""
    cfg_path = tmp_path / "cfg_bad_sample_nested.toml"
    text = minimal_full_experiment_toml(
        dataset_dir=Path("./data"),
        out_dir=Path("./out"),
        extra_sample_sample="unknown_leaf = 42",
    )
    cfg_path.write_text(text)
    with pytest.raises(ValidationError):
        config_loading.load_experiment_toml(cfg_path)


def test_full_loader_when_train_incomplete_then_raises(tmp_path: Path) -> None:
    """Full loader when train incomplete then raises."""
    toml_text = """
[prepare]

[train.model]
n_layer=1

# Missing other required sections
"""
    cfg_path = tmp_path / "incomplete.toml"
    cfg_path.write_text(toml_text)

    with pytest.raises(ValidationError):
        config_loading.load_experiment_toml(cfg_path)


def test_full_loader_when_unknown_top_level_then_raises(tmp_path: Path) -> None:
    """Full loader when unknown top level then raises."""
    cfg_path = tmp_path / "cfg.toml"
    base = minimal_full_experiment_toml(
        dataset_dir=Path("./data"),
        out_dir=Path("./out"),
        include_sample=True,
        extra_sample_sample='start = "\\n"',
    )
    cfg_path.write_text(base + "\n[export]\nfoo = 1\n")
    with pytest.raises(ValidationError):
        config_loading.load_experiment_toml(cfg_path)


def test_full_loader_when_train_has_unknown_keys_then_raises(tmp_path: Path) -> None:
    """Full loader when train has unknown keys then raises."""
    cfg_path = tmp_path / "cfg_bad_nested.toml"
    text = minimal_full_experiment_toml(
        dataset_dir=Path("./data"),
        out_dir=Path("./out"),
    )
    text = text.replace("[train.model]", "[train.model]\nunknown_key = 123")
    cfg_path.write_text(text)
    with pytest.raises(ValidationError):
        config_loading.load_experiment_toml(cfg_path)


def test_load_experiment_toml_when_valid_then_returns_config(tmp_path: Path) -> None:
    """Load experiment toml when valid then returns config."""
    cfg_path = tmp_path / "exp.toml"
    text = minimal_full_experiment_toml(
        dataset_dir=Path("./data"),
        out_dir=Path("./out"),
        extra_train="log_interval = 2",
        extra_sample="log_interval = 2",
        extra_sample_sample='start = "\\n"',
    )
    cfg_path.write_text(text)
    exp = config_loading.load_experiment_toml(cfg_path)
    assert isinstance(exp, ExperimentConfig)
    assert exp.sample.runtime is not None
    assert str(exp.sample.runtime.out_dir).endswith("out")
    assert exp.sample.runtime.log_interval == 2


def test_sample_runtime_when_explicit_then_overrides_defaults(tmp_path: Path) -> None:
    """Sample runtime when explicit then overrides defaults."""
    cfg_path = tmp_path / "exp2.toml"
    text = minimal_full_experiment_toml(
        dataset_dir=Path("./data"),
        out_dir=Path("./out"),
        extra_train="""
eval_interval = 100
eval_iters = 20
""",
        extra_sample="""
eval_interval = 200
""",
        extra_sample_sample='start = "\\n"',
    )
    cfg_path.write_text(text)
    exp = config_loading.load_experiment_toml(cfg_path)
    runtime = exp.sample.runtime
    assert runtime is not None
    assert str(runtime.out_dir).endswith("out")
    assert runtime.eval_interval == 200
    assert runtime.eval_iters == 200


def test_data_config_when_tokenizer_choices_then_accepts() -> None:
    """Data config when tokenizer choices then accepts."""
    DataConfig(tokenizer="char")
    DataConfig(tokenizer="word")
    DataConfig(tokenizer="tiktoken")


def test_data_config_when_positive_ints_then_accepts() -> None:
    """Data config when positive ints then accepts."""
    with pytest.raises(ValidationError):
        DataConfig(batch_size=0)
    with pytest.raises(ValidationError):
        DataConfig(block_size=-1)
    with pytest.raises(ValidationError):
        DataConfig(grad_accum_steps=0)
    with pytest.raises(ValidationError):
        DataConfig(ngram_size=0)
    cfg = DataConfig(batch_size=1, block_size=1, grad_accum_steps=1, ngram_size=1)
    assert cfg.batch_size == 1


def test_resolve_path_strict_when_missing_then_raises(tmp_path: Path) -> None:
    from ml_playground.configuration.models import _resolve_path_strict

    with pytest.raises(ValueError, match="Invalid path"):
        _resolve_path_strict(tmp_path / "does_not_exist")


def test_trainer_config_inject_providers_does_not_override_explicit() -> None:
    from ml_playground.configuration.models import TrainerConfig

    class _Telemetry:
        def log_metric(self, name: str, value: float, step: int | None = None) -> None:
            _ = name
            _ = value
            _ = step

        def time_block(self, name: str) -> Any:
            _ = name

            class _Ctx:
                def __enter__(self) -> None:
                    return None

                def __exit__(
                    self,
                    exc_type: type[BaseException] | None,
                    exc: BaseException | None,
                    tb: Any,
                ) -> None:
                    _ = exc_type
                    _ = exc
                    _ = tb
                    return None

            return _Ctx()

    sentinel_telemetry = _Telemetry()

    def explicit_save(*_a: object, **_k: object) -> None:
        return None

    def provided_save(*_a: object, **_k: object) -> None:
        return None

    cfg = TrainerConfig.model_validate(
        {
            "model": {
                "n_layer": 1,
                "n_head": 1,
                "n_embd": 8,
                "block_size": 8,
                "vocab_size": 32,
            },
            "data": {
                "batch_size": 1,
                "block_size": 8,
                "grad_accum_steps": 1,
                "tokenizer": "char",
                "ngram_size": 1,
            },
            "optim": {"learning_rate": 1e-4},
            "schedule": {"warmup_iters": 0},
            "runtime": {"out_dir": Path(".")},
            "checkpoint_save_fn": explicit_save,
            "telemetry": sentinel_telemetry,
        },
        context={
            "providers": {
                "checkpoint_save_fn": provided_save,
                "telemetry": _Telemetry(),
            }
        },
    )

    assert cfg.checkpoint_save_fn is explicit_save
    assert cfg.telemetry is sentinel_telemetry


def test_experiment_config_resolve_paths_handles_path_values(tmp_path: Path) -> None:
    from ml_playground.configuration.models import ExperimentConfig

    exp = ExperimentConfig.model_validate(
        {
            "prepare": {
                "raw_dir": "./raw",
                "dataset_dir": tmp_path / "data",
            },
            "train": {
                "model": {
                    "n_layer": 1,
                    "n_head": 1,
                    "n_embd": 8,
                    "block_size": 8,
                    "vocab_size": 32,
                },
                "data": {
                    "batch_size": 1,
                    "block_size": 8,
                    "grad_accum_steps": 1,
                    "tokenizer": "char",
                    "ngram_size": 1,
                },
                "optim": {"learning_rate": 1e-4},
                "schedule": {"warmup_iters": 0},
                "runtime": {
                    "out_dir": tmp_path / "train_out",
                    "eval_interval": 1,
                    "log_interval": 1,
                    "max_iters": 0,
                    "eval_only": True,
                },
            },
            "sample": {
                "runtime": {
                    "out_dir": tmp_path / "sample_out",
                    "max_iters": 0,
                    "eval_only": True,
                },
                "sample": {"start": "\n"},
            },
            "shared": {
                "experiment": "exp",
                "config_path": tmp_path / "cfg.toml",
                "project_home": tmp_path,
                "dataset_dir": tmp_path / "data",
                "train_out_dir": "./train_out",
                "sample_out_dir": "./sample_out",
            },
        }
    )

    assert exp.shared.train_out_dir.is_absolute()
    assert exp.shared.sample_out_dir.is_absolute()


def test_frozen_models_allow_setting_logger(tmp_path: Path) -> None:
    cfg = DataConfig(batch_size=1, block_size=1, grad_accum_steps=1, ngram_size=1)
    cfg.logger = logging.getLogger("test")
    assert cfg.logger.name == "test"


def test_experience_storage_resolve_path_ignores_invalid_context(
    tmp_path: Path,
) -> None:
    from ml_playground.configuration.models import ExperienceStorageConfig

    cfg = ExperienceStorageConfig.model_validate(
        {"strategy": "json_file", "path": Path("./store.json")},
        context={"config_path": "not-a-path"},
    )
    assert cfg.path == Path("./store.json")


def test_experiment_config_resolve_paths_runtime_not_dict_branches(
    tmp_path: Path,
) -> None:
    from ml_playground.configuration.models import ExperimentConfig

    with pytest.raises(ValidationError):
        ExperimentConfig.model_validate(
            {
                "prepare": {"raw_dir": "./raw"},
                "train": {
                    "model": {
                        "n_layer": 1,
                        "n_head": 1,
                        "n_embd": 8,
                        "block_size": 8,
                        "vocab_size": 32,
                    },
                    "data": {
                        "batch_size": 1,
                        "block_size": 8,
                        "grad_accum_steps": 1,
                        "tokenizer": "char",
                        "ngram_size": 1,
                    },
                    "optim": {"learning_rate": 1e-4},
                    "schedule": {"warmup_iters": 0},
                    "runtime": "not-a-dict",
                },
                "sample": {
                    "runtime": "not-a-dict",
                    "sample": {"start": "\n"},
                },
                "shared": {
                    "experiment": "exp",
                    "config_path": tmp_path / "cfg.toml",
                    "project_home": tmp_path,
                    "dataset_dir": tmp_path / "data",
                    "train_out_dir": "./train_out",
                    "sample_out_dir": "./sample_out",
                },
            }
        )


def test_sample_config_when_out_of_range_then_raises() -> None:
    """Sample config when out of range then raises."""
    with pytest.raises(ValidationError):
        SampleConfig(temperature=0.0)
    with pytest.raises(ValidationError):
        SampleConfig(top_k=-1)
    with pytest.raises(ValidationError):
        SampleConfig(top_p=0.0)
    with pytest.raises(ValidationError):
        SampleConfig(top_p=1.5)
    SampleConfig(temperature=0.1, top_k=0, top_p=0.5)


def test_lr_schedule_when_invalid_then_raises() -> None:
    """Lr schedule when invalid then raises."""
    with pytest.raises(ValidationError):
        LRSchedule(warmup_iters=-1)
    with pytest.raises(ValidationError):
        LRSchedule(lr_decay_iters=-5)
    with pytest.raises(ValidationError):
        LRSchedule(warmup_iters=10, lr_decay_iters=5)
    with pytest.raises(ValidationError):
        LRSchedule(min_lr=-1e-5)
    LRSchedule(warmup_iters=1, lr_decay_iters=2, min_lr=0)
    LRSchedule(warmup_iters=2, lr_decay_iters=2, min_lr=0)


def test_optim_config_when_negative_then_raises() -> None:
    """Optim config when negative then raises."""
    with pytest.raises(ValidationError):
        OptimConfig(learning_rate=-1e-3)
    with pytest.raises(ValidationError):
        OptimConfig(weight_decay=-1e-1)
    with pytest.raises(ValidationError):
        OptimConfig(beta1=-0.1)
    with pytest.raises(ValidationError):
        OptimConfig(beta2=-0.1)
    with pytest.raises(ValidationError):
        OptimConfig(grad_clip=-1)
    OptimConfig()


def test_model_config_when_invalid_then_raises() -> None:
    """Model config when invalid then raises."""
    with pytest.raises(ValidationError):
        ModelConfig(n_layer=0)
    with pytest.raises(ValidationError):
        ModelConfig(n_head=0)
    with pytest.raises(ValidationError):
        ModelConfig(n_embd=0)
    with pytest.raises(ValidationError):
        ModelConfig(block_size=0)
    with pytest.raises(ValidationError):
        ModelConfig(dropout=1.5)
    with pytest.raises(ValidationError):
        ModelConfig(vocab_size=0)
    ModelConfig()


def test_config_defaults_when_initialized_then_match_expected() -> None:
    """Config defaults when initialized then match expected."""
    schedule = LRSchedule()
    assert schedule.decay_lr is True
    assert schedule.warmup_iters == 2_000
    assert schedule.lr_decay_iters == 600_000
    assert schedule.min_lr == 6e-5

    optim = OptimConfig()
    assert optim.learning_rate == pytest.approx(6e-4)
    assert optim.weight_decay == pytest.approx(1e-1)
    assert optim.beta1 == pytest.approx(0.9)
    assert optim.beta2 == pytest.approx(0.95)
    assert optim.grad_clip == pytest.approx(1.0)

    model = ModelConfig()
    assert model.n_layer == 12
    assert model.n_head == 12
    assert model.n_embd == 767
    assert model.block_size == 1024

    sample = SampleConfig()
    assert sample.start == "\n"
    assert sample.num_samples == 3
    assert sample.max_new_tokens == 200
    assert sample.temperature == pytest.approx(0.8)
    assert sample.top_k == 200
    assert sample.top_p is None


def test_runtime_checkpointing_when_negative_keep_then_raises(tmp_path: Path) -> None:
    """Runtime checkpointing when negative keep then raises."""
    with pytest.raises(ValidationError):
        RuntimeConfig(
            out_dir=tmp_path,
            checkpointing=RuntimeConfig.Checkpointing(
                keep=RuntimeConfig.Checkpointing.Keep(last=-1)
            ),
        )
    with pytest.raises(ValidationError):
        RuntimeConfig(
            out_dir=tmp_path,
            checkpointing=RuntimeConfig.Checkpointing(
                keep=RuntimeConfig.Checkpointing.Keep(best=-2)
            ),
        )
    RuntimeConfig(out_dir=tmp_path)


def test_runtime_config_when_initialized_then_defaults_match() -> None:
    """Runtime config when initialized then defaults match."""
    runtime = RuntimeConfig(out_dir=Path("./out"))
    assert runtime.max_iters == 600_000
    assert runtime.max_games is None
    assert runtime.eval_interval == 2_000
    assert runtime.eval_iters == 200
    assert runtime.eval_interval_games is None
    assert runtime.eval_games is None
    assert runtime.log_interval == 1
    assert runtime.log_interval_games is None
    assert runtime.eval_only is False
    assert runtime.seed == 1337
    assert runtime.device == "cpu"
    assert runtime.dtype == "float32"
    assert runtime.compile is False
    assert runtime.games_per_epoch is None

    checkpoint = runtime.checkpointing
    assert checkpoint.read_policy in ("latest", "best")
    assert checkpoint.keep.last == 1
    assert checkpoint.keep.best == 1
    assert runtime.ckpt_metric in ("val_loss", "perplexity")
    assert runtime.ckpt_greater_is_better is False
    assert runtime.ckpt_atomic is True
    assert runtime.ckpt_write_metadata is True
    assert runtime.ckpt_naming_policy == "steps"
    assert runtime.ckpt_domain_label is None
    assert runtime.ckpt_naming_strict is False
    assert runtime.ckpt_time_interval_minutes == 0


def test_merge_mappings_when_nested_then_merges_and_overrides() -> None:
    """Merge mappings when nested then merges and overrides."""
    base = {"a": 1, "b": {"x": 1, "y": 2}, "c": {"k": 1}, "d": 4}
    override = {"b": {"y": 20, "z": 3}, "c": 5, "e": 6}
    out = merge_mappings(base, override)
    assert out["b"] == {"x": 1, "y": 20, "z": 3}
    assert out["c"] == 5
    assert out["a"] == 1 and out["d"] == 4
    assert out["e"] == 6


def test_merge_mappings_when_override_numeric_then_replaces() -> None:
    """Merge mappings when override numeric then replaces."""
    base = {"a": {"x": 1, "y": -2}, "b": 10}
    override = {"a": {"x": 3}, "b": 0}
    out = merge_mappings(base, override)
    assert out["a"]["x"] == 3
    assert out["a"]["y"] == -2
    assert out["b"] == 0


def test_merge_mappings_when_override_type_then_replaces() -> None:
    """Merge mappings when override type then replaces."""
    base = {"a": {"x": 1}, "b": {"y": 2}}
    override = {"b": 7}
    out = merge_mappings(base, override)
    assert out["a"] == {"x": 1}
    assert out["b"] == 7


def test_trainer_config_when_relative_out_dir_then_resolves(tmp_path: Path) -> None:
    """Trainer config when relative out dir then resolves."""
    cfg_text = minimal_full_experiment_toml(
        dataset_dir=Path("./data"),
        out_dir=Path("out/rel_train"),
        include_sample=True,
    )
    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text(cfg_text, encoding="utf-8")
    exp = config_loading.load_full_experiment_config(cfg_path, tmp_path, "exp")
    assert isinstance(exp.train.runtime.out_dir, Path)
    assert str(exp.train.runtime.out_dir).endswith("out/rel_train")


def test_sampler_config_when_relative_out_dir_then_resolves(tmp_path: Path) -> None:
    """Sampler config when relative out dir then resolves."""
    cfg_text = minimal_full_experiment_toml(
        dataset_dir=Path("./data"),
        out_dir=Path("out/rel_sample"),
        include_sample=True,
    )
    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text(cfg_text, encoding="utf-8")
    exp = config_loading.load_full_experiment_config(cfg_path, tmp_path, "exp")
    assert isinstance(exp.sample.runtime.out_dir, Path)
    assert str(exp.sample.runtime.out_dir).endswith("out/rel_sample")


def test_experiment_config_when_shared_paths_then_coerces(tmp_path: Path) -> None:
    """Experiment config when shared paths then coerces."""
    ds_dir = Path("./data/shared")
    out_dir = Path("out/shared")
    cfg_text = minimal_full_experiment_toml(dataset_dir=ds_dir, out_dir=out_dir)
    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text(cfg_text, encoding="utf-8")

    exp: ExperimentConfig = config_loading.load_full_experiment_config(
        cfg_path, tmp_path, "exp"
    )
    assert isinstance(exp.shared.dataset_dir, Path)
    assert isinstance(exp.shared.train_out_dir, Path)
    assert isinstance(exp.shared.sample_out_dir, Path)
    assert exp.shared.train_out_dir.is_absolute()
    assert exp.shared.sample_out_dir.is_absolute()
    assert str(exp.shared.train_out_dir).endswith(str(out_dir))
    assert str(exp.shared.sample_out_dir).endswith(str(out_dir))


def test_trainer_config_when_cross_field_invalid_then_raises(tmp_path: Path) -> None:
    """Trainer config when cross field invalid then raises."""
    with pytest.raises(ValueError):
        TrainerConfig(
            model=ModelConfig(block_size=4),
            data=DataConfig(block_size=8, batch_size=1, grad_accum_steps=1),
            optim=OptimConfig(),
            schedule=LRSchedule(
                decay_lr=True, warmup_iters=1, lr_decay_iters=2, min_lr=0
            ),
            runtime=RuntimeConfig(out_dir=tmp_path),
        )

    with pytest.raises(ValueError):
        LRSchedule(decay_lr=True, warmup_iters=10, lr_decay_iters=5, min_lr=0)

    with pytest.raises(ValueError):
        TrainerConfig(
            model=ModelConfig(block_size=4),
            data=DataConfig(block_size=4, batch_size=1, grad_accum_steps=1),
            optim=OptimConfig(learning_rate=1e-3),
            schedule=LRSchedule(
                decay_lr=True, warmup_iters=0, lr_decay_iters=2, min_lr=2e-3
            ),
            runtime=RuntimeConfig(out_dir=tmp_path),
        )

    with pytest.raises(ValueError):
        RuntimeConfig(out_dir=tmp_path, log_interval=10, eval_interval=1)


def test_data_config_when_initialized_then_defaults_match() -> None:
    """Data config when initialized then defaults match."""
    config = DataConfig()
    assert config.train_bin == "train.bin"
    assert config.val_bin == "val.bin"
    assert config.meta_pkl == "meta.pkl"
    assert config.batch_size == 12
    assert config.block_size == 1024
    assert config.grad_accum_steps == 40
    assert config.tokenizer in ("char", "word", "tiktoken")
    assert config.ngram_size == 1
    assert config.sampler in ("random", "sequential")


def test_data_config_when_meta_none_then_raises() -> None:
    """Data config when meta none then raises."""
    with pytest.raises(ValidationError):
        DataConfig(meta_pkl=cast(Any, None))


def test_preparer_config_when_paths_then_resolves(tmp_path: Path) -> None:
    """Preparer config when paths then resolves."""
    config = PreparerConfig(raw_dir=tmp_path / "raw")
    assert isinstance(config.raw_dir, Path)
    _ = config.raw_dir


def test_sample_config_when_bounds_then_accepts() -> None:
    """Sample config when bounds then accepts."""
    with pytest.raises(ValidationError):
        SampleConfig(num_samples=0)
    with pytest.raises(ValidationError):
        SampleConfig(max_new_tokens=0)
    with pytest.raises(ValidationError):
        SampleConfig(temperature=-0.1)
    SampleConfig(temperature=1e-6, top_k=0, top_p=1.0)


def test_configuration_module_when_imported_then_exports_expected() -> None:
    """Test that canonical configuration modules export expected APIs."""
    from ml_playground.configuration import models
    from ml_playground.configuration import loading

    assert hasattr(models, "TrainerConfig")
    assert hasattr(models, "SamplerConfig")
    assert hasattr(models, "DataConfig")
    assert hasattr(models, "RuntimeConfig")
    assert hasattr(loading, "load_full_experiment_config")


def test_full_loader_when_sample_incomplete_then_raises(tmp_path: Path) -> None:
    """Full loader when sample incomplete then raises."""
    toml_text = minimal_full_experiment_toml(
        dataset_dir=Path("./data"),
        out_dir=Path("out/test"),
        include_sample=True,
    )
    toml_text = toml_text.replace("[sample.sample]", "# Missing sample.sample")
    cfg_path = tmp_path / "incomplete_sample.toml"
    cfg_path.write_text(toml_text)

    with pytest.raises(ValidationError):
        config_loading.load_experiment_toml(cfg_path)


def test_full_loader_when_train_missing_then_raises(tmp_path: Path) -> None:
    """Full loader when train missing then raises."""
    toml_text = """
[prepare]

[sample.runtime]
out_dir = "out/test"

[sample.sample]
"""
    cfg_path = tmp_path / "no_train.toml"
    cfg_path.write_text(toml_text)
    with pytest.raises(ValidationError):
        config_loading.load_experiment_toml(cfg_path)


def test_full_loader_when_sample_missing_then_raises(tmp_path: Path) -> None:
    """Full loader when sample missing then raises."""
    toml_text = minimal_full_experiment_toml(
        dataset_dir=Path("data/shakespeare"),
        out_dir=Path("out/test"),
        include_sample=False,
    )
    cfg_path = tmp_path / "no_sample.toml"
    cfg_path.write_text(toml_text)
    with pytest.raises(ValidationError):
        config_loading.load_experiment_toml(cfg_path)


def test_full_loader_when_train_data_missing_then_raises(tmp_path: Path) -> None:
    """Full loader when train data missing then raises."""
    toml_text = minimal_full_experiment_toml(
        dataset_dir=Path("data/shakespeare"),
        out_dir=Path("out/test"),
        include_train_data=False,
    )
    cfg_path = tmp_path / "missing_data.toml"
    cfg_path.write_text(toml_text)

    with pytest.raises(ValidationError):
        config_loading.load_experiment_toml(cfg_path)


def test_full_loader_when_train_runtime_missing_then_raises(tmp_path: Path) -> None:
    """Full loader when train runtime missing then raises."""
    toml_text = minimal_full_experiment_toml(
        dataset_dir=Path("data/shakespeare"),
        out_dir=Path("out/test"),
        include_train_runtime=False,
    )
    cfg_path = tmp_path / "missing_runtime.toml"
    cfg_path.write_text(toml_text)

    with pytest.raises(ValidationError):
        config_loading.load_experiment_toml(cfg_path)


def test_full_loader_when_sample_runtime_missing_then_raises(tmp_path: Path) -> None:
    """Full loader when sample runtime missing then raises."""
    toml_text = minimal_full_experiment_toml(
        dataset_dir=Path("./data"),
        out_dir=Path("out/test"),
        include_sample=True,
    )
    toml_text = toml_text.replace("[sample.runtime]", "# Missing [sample.runtime]")
    cfg_path = tmp_path / "sample_missing_runtime.toml"
    cfg_path.write_text(toml_text)

    with pytest.raises(ValidationError):
        config_loading.load_experiment_toml(cfg_path)


def test_cli_adapters_when_loaded_then_validates(tmp_path: Path) -> None:
    """Cli adapters when loaded then validates."""
    cfg_path = tmp_path / "exp.toml"
    cfg_path.write_text(
        minimal_full_experiment_toml(
            dataset_dir=Path("./data"),
            out_dir=Path("out/exp"),
            include_sample=True,
        ),
        encoding="utf-8",
    )
    exp = config_cli.load_experiment("exp", cfg_path)
    assert exp.shared.experiment == "exp"
    assert exp.shared.dataset_dir.is_absolute()


def test_cli_adapters_when_prereqs_present_then_passes(tmp_path: Path) -> None:
    """Cli adapters when prereqs present then passes."""
    cfg_path = tmp_path / "exp.toml"
    cfg_path.write_text(
        minimal_full_experiment_toml(
            dataset_dir=Path("./data"),
            out_dir=Path("out/exp"),
            include_sample=True,
        ),
        encoding="utf-8",
    )
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    train_meta = data_dir / "meta.pkl"
    train_meta.write_bytes(b"meta")

    exp = config_cli.load_experiment("exp", cfg_path)

    found_train_meta = config_cli.ensure_train_prerequisites(exp)
    assert found_train_meta == train_meta

    runtime_meta_dir = exp.shared.sample_out_dir / exp.shared.experiment
    runtime_meta_dir.mkdir(parents=True, exist_ok=True)
    (runtime_meta_dir / "meta.pkl").write_bytes(b"meta")
    config_cli.ensure_sample_prerequisites(exp)


def test_cli_train_prereqs_when_meta_missing_then_raises(tmp_path: Path) -> None:
    """Cli train prereqs when meta missing then raises."""
    cfg_path = tmp_path / "exp.toml"
    cfg_path.write_text(
        minimal_full_experiment_toml(
            dataset_dir=Path("./data"),
            out_dir=Path("out/exp"),
            include_sample=True,
        ),
        encoding="utf-8",
    )
    exp = config_cli.load_experiment("exp", cfg_path)

    with pytest.raises(ValueError) as exc:
        config_cli.ensure_train_prerequisites(exp)

    msg = str(exc.value)
    assert "Missing required meta file" in msg
    assert "Run 'prepare' first" in msg


def test_cli_sample_prereqs_when_meta_missing_then_raises(tmp_path: Path) -> None:
    """Cli sample prereqs when meta missing then raises."""
    cfg_path = tmp_path / "exp.toml"
    cfg_path.write_text(
        minimal_full_experiment_toml(
            dataset_dir=Path("./data"),
            out_dir=Path("out/exp"),
            include_sample=True,
        ),
        encoding="utf-8",
    )
    exp = config_cli.load_experiment("exp", cfg_path)

    with pytest.raises(ValueError) as exc:
        config_cli.ensure_sample_prerequisites(exp)

    msg = str(exc.value)
    assert "Missing required meta file for sampling" in msg
    assert "Run 'prepare' and 'train' first" in msg


def test_path_helpers_when_invalid_then_raises(tmp_path: Path) -> None:
    """Path helpers when invalid then raises."""

    class _BadPath:
        def __init__(self, value: Path) -> None:
            self._value = value

        def resolve(self) -> Path:
            raise OSError("cannot resolve")

        def __str__(self) -> str:
            return str(self._value)

    bad_path = cast(Path, _BadPath(tmp_path / "bad"))

    with pytest.raises(ValueError, match="Invalid path"):
        config_models._resolve_path_strict(bad_path)

    relative = config_models._resolve_if_relative("rel", tmp_path)
    assert isinstance(relative, Path) and relative.is_absolute()

    absolute_path = tmp_path / "abs"
    assert config_models._resolve_if_relative(absolute_path, tmp_path) == absolute_path


def test_optim_config_when_nan_then_raises() -> None:
    """Optim config when nan then raises."""
    with pytest.raises(ValidationError):
        config_models.OptimConfig(learning_rate=float("nan"))


def test_preparer_config_when_context_path_then_resolves(tmp_path: Path) -> None:
    """Preparer config when context path then resolves."""
    cfg_path = tmp_path / "exp.toml"
    context = {"config_path": cfg_path}
    cfg = config_models.PreparerConfig.model_validate(
        {"raw_dir": "data", "raw_text_path": Path("text.txt")},
        context=context,
    )
    assert cfg.raw_dir.is_absolute()
    assert cfg.raw_text_path and cfg.raw_text_path.is_absolute()

    # Non-path context should leave values unchanged
    cfg2 = config_models.PreparerConfig.model_validate(
        {"raw_dir": Path("data")},
        context={"config_path": "not-a-path"},
    )
    assert not cfg2.raw_dir.is_absolute()


def test_prepare_extras_when_model_missing_then_raises(tmp_path: Path) -> None:
    """Prepare extras when model missing then raises."""
    exp_dir = tmp_path / "missing_extras_model"
    exp_dir.mkdir()
    cfg_path = exp_dir / "config.toml"
    cfg_path.write_text(
        "[prepare]\n[prepare.extras]\nunknown = 1\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="missing_extras_model.prepare"):
        config_loading.load_prepare_config(cfg_path, default_config_path=cfg_path)


def test_prepare_extras_when_fields_valid_then_allows(tmp_path: Path) -> None:
    """Prepare extras when fields valid then allows."""

    class StrictExtras(BaseModel):
        model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

        allowed: int | None = None

    register_extras_model("extras_test", "prepare", StrictExtras)

    exp_dir = tmp_path / "extras_test"
    exp_dir.mkdir()
    cfg_path = exp_dir / "config.toml"
    cfg_path.write_text(
        "[prepare]\n[prepare.extras]\nallowed = 1\n",
        encoding="utf-8",
    )

    cfg = config_loading.load_prepare_config(cfg_path, default_config_path=cfg_path)
    assert cfg.extras["allowed"] == 1


def test_prepare_extras_when_unknown_field_then_raises(tmp_path: Path) -> None:
    """Prepare extras when unknown field then raises."""

    class StrictExtras(BaseModel):
        model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

        allowed: int | None = None

    register_extras_model("extras_strict", "prepare", StrictExtras)

    exp_dir = tmp_path / "extras_strict"
    exp_dir.mkdir()
    cfg_path = exp_dir / "config.toml"
    cfg_path.write_text(
        "[prepare]\n[prepare.extras]\nunknown = 1\n",
        encoding="utf-8",
    )

    with pytest.raises(ValidationError):
        config_loading.load_prepare_config(cfg_path, default_config_path=cfg_path)


def test_peft_config_when_targets_list_then_coerces_tuple() -> None:
    """Peft config when targets list then coerces tuple."""
    peft = config_models.TrainerConfig.PeftConfig.model_validate(
        {"target_modules": ["a", "b"], "enabled": True}
    )
    assert isinstance(peft.target_modules, tuple)
    assert peft.target_modules == ("a", "b")


def test_experiment_config_when_resolving_dict_then_converts_paths(
    tmp_path: Path,
) -> None:
    """Experiment config when resolving dict then converts paths."""
    base_dir = tmp_path / "project"
    base_dir.mkdir()
    cfg_path = base_dir / "cfg.toml"

    data = {
        "prepare": {"raw_dir": "raw_rel", "dataset_dir": "prep_dataset"},
        "train": {"runtime": {"out_dir": "train_rel"}},
        "sample": {"runtime": {"out_dir": "sample_rel"}},
        "shared": {
            "config_path": cfg_path,
            "project_home": "proj_rel",
            "dataset_dir": "data_rel",
            "train_out_dir": "train_rel",
            "sample_out_dir": "sample_rel",
        },
    }

    result = config_models.ExperimentConfig._resolve_paths(data)

    shared = result["shared"]
    assert isinstance(shared["project_home"], Path)
    assert isinstance(shared["dataset_dir"], Path)
    assert isinstance(shared["train_out_dir"], Path)
    assert isinstance(shared["sample_out_dir"], Path)
    # prepare paths are resolved relative to config directory
    assert result["prepare"]["raw_dir"].is_absolute()
    assert "dataset_dir" not in result["prepare"]
    assert isinstance(shared["dataset_dir"], Path)
    # runtime paths are converted to Path instances
    assert isinstance(result["train"]["runtime"]["out_dir"], Path)
    assert isinstance(result["sample"]["runtime"]["out_dir"], Path)


def test_experiment_config_when_shared_namespace_then_preserves(tmp_path: Path) -> None:
    """Experiment config when shared namespace then preserves."""
    cfg_path = tmp_path / "cfg.toml"
    shared_ns = SimpleNamespace(config_path=cfg_path)
    data = {"shared": shared_ns}
    # Should not raise or mutate namespace for non-dict shared data
    assert config_models.ExperimentConfig._resolve_paths(data)["shared"] is shared_ns


def test_shared_config_when_relative_then_resolves(tmp_path: Path) -> None:
    """Shared config when relative then resolves."""
    cfg_path = tmp_path / "cfg.toml"
    data = {
        "config_path": cfg_path,
        "project_home": "proj",
        "dataset_dir": "data",
        "train_out_dir": "train",
        "sample_out_dir": "sample",
    }
    resolved = config_models.SharedConfig._resolve_shared_paths(data.copy())
    assert resolved["project_home"].is_absolute()
    assert resolved["dataset_dir"].is_absolute()


def _trainer_dict(tmp_path: Path) -> dict[str, Any]:
    model = ModelConfig().model_dump()
    data = DataConfig().model_dump()
    optim = OptimConfig().model_dump()
    schedule = LRSchedule().model_dump()
    runtime = config_models.RuntimeConfig(out_dir=tmp_path / "out").model_dump(
        exclude={"total_eval_steps", "total_eval_games"}
    )
    runtime["out_dir"] = "rel_out"
    return {
        "model": model,
        "data": data,
        "optim": optim,
        "schedule": schedule,
        "runtime": runtime,
    }


def test_trainer_config_when_context_path_then_resolves(tmp_path: Path) -> None:
    """Trainer config when context path then resolves."""
    trainer_dict = _trainer_dict(tmp_path)
    cfg_path = tmp_path / "cfg.toml"
    trainer = TrainerConfig.model_validate(
        trainer_dict,
        context={"config_path": cfg_path},
    )
    assert trainer.runtime.out_dir.is_absolute()


def test_trainer_config_when_context_missing_then_keeps_relative(
    tmp_path: Path,
) -> None:
    """Trainer config when context missing then keeps relative."""
    trainer_dict = _trainer_dict(tmp_path)
    trainer_dict["runtime"]["out_dir"] = Path("rel_out")
    trainer = TrainerConfig.model_validate(
        trainer_dict,
        context={"config_path": "not-a-path"},
    )
    assert str(trainer.runtime.out_dir) == "rel_out"


def _sampler_dict(tmp_path: Path) -> dict[str, Any]:
    runtime = config_models.RuntimeConfig(out_dir=tmp_path / "out").model_dump(
        exclude={"total_eval_steps", "total_eval_games"}
    )
    runtime["out_dir"] = "rel_out"
    sample = SampleConfig().model_dump()
    return {"runtime": runtime, "sample": sample}


def test_sampler_config_when_context_path_then_resolves(tmp_path: Path) -> None:
    """Sampler config when context path then resolves."""
    sampler_dict = _sampler_dict(tmp_path)
    cfg_path = tmp_path / "cfg.toml"
    sampler = config_models.SamplerConfig.model_validate(
        sampler_dict,
        context={"config_path": cfg_path},
    )
    assert sampler.runtime.out_dir.is_absolute()


def test_sampler_config_when_context_missing_then_keeps_relative(
    tmp_path: Path,
) -> None:
    """Sampler config when context missing then keeps relative."""
    sampler_dict = _sampler_dict(tmp_path)
    sampler_dict["runtime"]["out_dir"] = Path("rel_out")
    sampler = config_models.SamplerConfig.model_validate(
        sampler_dict,
        context={"config_path": "not-a-path"},
    )
    assert str(sampler.runtime.out_dir) == "rel_out"


def test_experiment_config_when_non_dict_then_returns_input() -> None:
    """Experiment config when non dict then returns input."""
    assert config_models.ExperimentConfig._resolve_paths(123) == 123


def test_experiment_config_when_config_missing_then_returns_input(
    tmp_path: Path,
) -> None:
    """Experiment config when config missing then returns input."""
    data = {
        "shared": {"experiment": "unit"},
        "prepare": {},
    }
    assert config_models.ExperimentConfig._resolve_paths(data) is data


def test_shared_config_when_config_invalid_then_returns_input() -> None:
    """Shared config when config invalid then returns input."""
    data = {"config_path": object(), "project_home": "rel", "dataset_dir": 123}
    resolved = config_models.SharedConfig._resolve_shared_paths(data.copy())
    assert resolved == data


def test_runtime_config_when_log_interval_invalid_then_raises(tmp_path: Path) -> None:
    """Runtime config when log interval invalid then raises."""
    with pytest.raises(ValidationError):
        RuntimeConfig(
            out_dir=tmp_path / "out",
            max_iters=1,
            eval_interval=1,
            eval_iters=1,
            log_interval=2,
            eval_only=False,
            seed=0,
            device="cpu",
            dtype="float32",
            compile=False,
        )


def _base_trainer_kwargs(tmp_path: Path) -> dict[str, Any]:
    return {
        "model": ModelConfig(
            n_layer=1,
            n_head=1,
            n_embd=8,
            block_size=4,
            dropout=0.0,
            bias=True,
        ),
        "data": DataConfig(
            batch_size=2,
            block_size=4,
            grad_accum_steps=1,
            tokenizer="char",
        ),
        "optim": OptimConfig(
            learning_rate=0.1,
            weight_decay=0.0,
            beta1=0.9,
            beta2=0.95,
            grad_clip=0.0,
        ),
        "schedule": LRSchedule(
            decay_lr=True,
            warmup_iters=1,
            lr_decay_iters=10,
            min_lr=0.01,
        ),
        "runtime": RuntimeConfig(
            out_dir=tmp_path / "out",
            max_iters=1,
            eval_interval=1,
            eval_iters=1,
            log_interval=1,
            eval_only=False,
            seed=42,
            device="cpu",
            dtype="float32",
            compile=False,
        ),
    }


def test_trainer_config_when_data_block_too_large_then_raises(tmp_path: Path) -> None:
    """Trainer config when data block too large then raises."""
    kwargs = _base_trainer_kwargs(tmp_path)
    kwargs["data"] = DataConfig(
        batch_size=2,
        block_size=8,
        grad_accum_steps=1,
        tokenizer="char",
    )
    with pytest.raises(ValidationError):
        TrainerConfig(**kwargs)


def test_trainer_config_when_min_lr_too_high_then_raises(tmp_path: Path) -> None:
    """Trainer config when min lr too high then raises."""
    kwargs = _base_trainer_kwargs(tmp_path)
    kwargs["optim"] = OptimConfig(
        learning_rate=0.05,
        weight_decay=0.0,
        beta1=0.9,
        beta2=0.95,
        grad_clip=0.0,
    )
    kwargs["schedule"] = LRSchedule(
        decay_lr=True,
        warmup_iters=1,
        lr_decay_iters=10,
        min_lr=0.1,
    )
    with pytest.raises(ValidationError):
        TrainerConfig(**kwargs)


def test_trainer_config_when_decay_off_then_requires_zero_warmup(
    tmp_path: Path,
) -> None:
    """Trainer config when decay off then requires zero warmup."""
    kwargs = _base_trainer_kwargs(tmp_path)
    kwargs["schedule"] = LRSchedule(
        decay_lr=False,
        warmup_iters=1,
        lr_decay_iters=10,
        min_lr=0.01,
    )
    with pytest.raises(ValidationError):
        TrainerConfig(**kwargs)


def test_lr_schedule_when_warmup_gt_decay_then_raises() -> None:
    """Lr schedule when warmup gt decay then raises."""
    with pytest.raises(ValidationError):
        LRSchedule(
            decay_lr=True,
            warmup_iters=5,
            lr_decay_iters=4,
            min_lr=0.01,
        )


def test_data_config_when_non_tiktoken_ngram_then_raises() -> None:
    """Data config when non tiktoken ngram then raises."""
    with pytest.raises(ValidationError):
        DataConfig(
            batch_size=2,
            block_size=4,
            grad_accum_steps=1,
            tokenizer="word",
            ngram_size=2,
        )
