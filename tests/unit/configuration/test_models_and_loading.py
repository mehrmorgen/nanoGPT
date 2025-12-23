from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping, cast
import math

import pytest
from pydantic import ValidationError

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
from tests.conftest import minimal_full_experiment_toml


class ExperimentConfigTestHarness(config_models.ExperimentConfig):
    """Test harness exposing ExperimentConfig protected validators."""

    @classmethod
    def resolve_paths(
        cls, data: Any, *, context: Mapping[str, Any] | None = None
    ) -> Any:
        del context
        descriptor = getattr(config_models.ExperimentConfig, "_resolve_paths")
        validator = descriptor.__get__(  # pyright: ignore[reportPrivateUsage]
            None, config_models.ExperimentConfig
        )
        return validator(data)  # pyright: ignore[reportGeneralTypeIssues]


class SharedConfigTestHarness(config_models.SharedConfig):
    """Test harness exposing SharedConfig protected validators."""

    @classmethod
    def resolve_paths(
        cls, data: Any, *, context: Mapping[str, Any] | None = None
    ) -> Any:
        del context
        descriptor = getattr(config_models.SharedConfig, "_resolve_shared_paths")
        validator = descriptor.__get__(  # pyright: ignore[reportPrivateUsage]
            None, config_models.SharedConfig
        )
        return validator(data)  # pyright: ignore[reportGeneralTypeIssues]


def test_full_loader_roundtrip(tmp_path: Path) -> None:
    """Test full loader roundtrip."""
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


def test_read_toml_dict_missing_file_raises(tmp_path: Path) -> None:
    """Test read toml dict missing file raises."""
    missing_path = tmp_path / "missing.toml"
    with pytest.raises(FileNotFoundError):
        config_loading.read_toml_dict(missing_path)


def test_get_default_config_path_with_none_uses_package_root() -> None:
    """get_default_config_path with None should use package root."""
    path = config_loading.get_default_config_path(None)
    assert path.name == "default_config.toml"
    assert (
        str(path)
        .replace("\\", "/")
        .endswith("src/ml_playground/experiments/default_config.toml")
    )


def test_get_default_config_path_with_explicit_root(tmp_path: Path) -> None:
    """get_default_config_path with explicit root should use that root."""
    path = config_loading.get_default_config_path(tmp_path)
    assert (
        path
        == tmp_path / "src" / "ml_playground" / "experiments" / "default_config.toml"
    )


def test_default_config_path_when_root_is_src(tmp_path: Path) -> None:
    """_default_config_path_from_root should handle roots named 'src'."""
    src_root = tmp_path / "src"
    src_root.mkdir()
    resolved = config_loading.get_default_config_path(src_root)
    assert (
        resolved == src_root / "ml_playground" / "experiments" / "default_config.toml"
    )


def test_get_cfg_path_without_override(tmp_path: Path) -> None:
    """Test get cfg path without override."""
    expected = config_loading._package_root() / "experiments" / "demo" / "config.toml"  # pyright: ignore[reportPrivateUsage]
    result = config_loading.get_cfg_path("demo", None)
    assert result == expected


def test_list_experiments_with_config_returns_sorted_names(tmp_path: Path) -> None:
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


def test_list_experiments_with_config_filters_by_prefix(tmp_path: Path) -> None:
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


def test_list_experiments_with_config_handles_missing_root() -> None:
    """list_experiments_with_config should return empty list if experiments root doesn't exist."""
    missing_root = Path("/nonexistent/path/loading")
    result = config_loading.list_experiments_with_config(experiments_root=missing_root)
    assert result == []


def test_list_experiments_with_config_handles_os_error(tmp_path: Path) -> None:
    """list_experiments_with_config should return empty list on OSError."""
    experiments_root = tmp_path / "src" / "ml_playground" / "experiments"
    experiments_root.mkdir(parents=True)

    class BrokenPath(type(experiments_root)):  # type: ignore[misc]
        def iterdir(self):  # type: ignore[override]
            raise OSError("Simulated error")

    broken_root = BrokenPath(experiments_root)

    result = config_loading.list_experiments_with_config(experiments_root=broken_root)
    assert result == []


def test_load_and_merge_configs_missing_file_raises(tmp_path: Path) -> None:
    """_load_and_merge_configs should raise FileNotFoundError for missing config."""
    missing_path = tmp_path / "missing.toml"
    with pytest.raises(FileNotFoundError, match="Config file not found"):
        config_loading._load_and_merge_configs(missing_path, tmp_path, "test")  # pyright: ignore[reportPrivateUsage]


def test_load_prepare_config_success(tmp_path: Path) -> None:
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


def test_load_prepare_config_missing_section_raises(tmp_path: Path) -> None:
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


def test_load_train_config_sets_provenance(tmp_path: Path) -> None:
    """Test load train config sets provenance."""
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


def test_load_sample_config_sets_provenance(tmp_path: Path) -> None:
    """Test load sample config sets provenance."""
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


def test_load_train_config_requires_mapping(tmp_path: Path) -> None:
    """Test load train config requires mapping."""
    config = tmp_path / "train_invalid.toml"
    config.write_text("train = 'value'\n")

    default_config = tmp_path / "default.toml"
    default_config.write_text("")

    with pytest.raises(TypeError, match="\\[train\\] section"):
        config_loading.load_train_config(config, default_config_path=default_config)


def test_load_sample_config_requires_sample_block(tmp_path: Path) -> None:
    """Test load sample config requires sample block."""
    config = tmp_path / "sample_invalid.toml"
    config.write_text("[train]\n[train.runtime]\nout_dir='.'\n")

    default_config = tmp_path / "default.toml"
    default_config.write_text("")

    with pytest.raises(ValueError, match=r"must contain a \[sample\] section"):
        config_loading.load_sample_config(config, default_config_path=default_config)


def test_read_toml_dict_reads_existing_file(tmp_path: Path) -> None:
    """Test read toml dict reads existing file."""
    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text("key = 'value'", encoding="utf-8")
    data = config_loading.read_toml_dict(cfg_path)
    assert data == {"key": "value"}


def test_read_toml_dict_rejects_non_mapping_root(tmp_path: Path) -> None:
    """Test read toml dict rejects non mapping root."""
    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text("key = 'value'", encoding="utf-8")

    def fake_loads(_: str) -> Mapping[str, Any]:
        return cast(Mapping[str, Any], [1, 2, 3])

    with pytest.raises(TypeError, match="must be a mapping"):
        config_loading.read_toml_dict(cfg_path, toml_loader=fake_loads)


def test_read_toml_dict_invalid_toml_raises(tmp_path: Path) -> None:
    """Test read toml dict invalid toml raises."""
    cfg_path = tmp_path / "broken.toml"
    cfg_path.write_text("not = [", encoding="utf-8")

    with pytest.raises(Exception, match="broken.toml"):
        config_loading.read_toml_dict(cfg_path)


def test_full_loader_empty_config_raises(tmp_path: Path) -> None:
    """Test full loader empty config raises."""
    toml_text = ""
    cfg_path = tmp_path / "empty.toml"
    cfg_path.write_text(toml_text)
    project_home = tmp_path.parent if tmp_path.parent.name else tmp_path
    experiment_name = cfg_path.parent.name
    with pytest.raises(Exception):
        config_loading.load_full_experiment_config(
            cfg_path, project_home, experiment_name
        )


def test_full_loader_bad_root_type(tmp_path: Path) -> None:
    """Test full loader bad root type."""
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


def test_full_loader_nested_unknown_keys_in_sample_raise(tmp_path: Path) -> None:
    """Test full loader nested unknown keys in sample raise."""
    cfg_path = tmp_path / "cfg_bad_sample_nested.toml"
    text = minimal_full_experiment_toml(
        dataset_dir=Path("./data"),
        out_dir=Path("./out"),
        extra_sample_sample="unknown_leaf = 42",
    )
    cfg_path.write_text(text)
    with pytest.raises(ValidationError):
        config_loading.load_experiment_toml(cfg_path)


def test_full_loader_incomplete_train_config(tmp_path: Path) -> None:
    """Test full loader incomplete train config."""
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


def test_full_loader_unknown_top_level_sections_raise(tmp_path: Path) -> None:
    """Test full loader unknown top level sections raise."""
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


def test_full_loader_nested_unknown_keys_raise(tmp_path: Path) -> None:
    """Test full loader nested unknown keys raise."""
    cfg_path = tmp_path / "cfg_bad_nested.toml"
    text = minimal_full_experiment_toml(
        dataset_dir=Path("./data"),
        out_dir=Path("./out"),
    )
    text = text.replace("[train.model]", "[train.model]\nunknown_key = 123")
    cfg_path.write_text(text)
    with pytest.raises(ValidationError):
        config_loading.load_experiment_toml(cfg_path)


def test_load_experiment_toml_strict_sections(tmp_path: Path) -> None:
    """Test load experiment toml strict sections."""
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


def test_experiment_config_resolves_shared_and_section_paths(tmp_path: Path) -> None:
    """Test experiment config resolves shared and section paths."""
    config_dir = tmp_path / "configs"
    config_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = config_dir / "exp.toml"
    cfg_path.write_text("")

    data = {
        "shared": {
            "experiment": "demo",
            "config_path": str(cfg_path),
            "project_home": "..",
            "dataset_dir": "dataset_rel",
            "train_out_dir": "train_rel",
            "sample_out_dir": "sample_rel",
        },
        "prepare": {
            "raw_dir": "raw",
            "raw_text_path": "texts/input.txt",
            "dataset_dir": "prep_dataset",
        },
        "train": {
            "model": {},
            "data": {},
            "optim": {},
            "schedule": {},
            "runtime": {
                "out_dir": "train_out",
                "log_interval": 1,
                "eval_interval": 2,
            },
        },
        "sample": {
            "runtime": {
                "out_dir": "sample_out",
                "log_interval": 1,
                "eval_interval": 2,
            },
            "sample": {},
        },
    }

    exp = ExperimentConfig.model_validate(data)

    assert exp.shared.config_path == cfg_path.resolve()
    assert exp.shared.project_home == config_dir.parent.resolve()
    assert exp.shared.dataset_dir == (config_dir / "prep_dataset").resolve()
    assert exp.shared.train_out_dir == (config_dir / "train_out").resolve()
    assert exp.shared.sample_out_dir == (config_dir / "sample_out").resolve()
    assert exp.prepare.raw_dir == (config_dir / "raw").resolve()
    assert exp.prepare.raw_text_path == (config_dir / "texts" / "input.txt").resolve()
    assert exp.train.runtime.out_dir == (config_dir / "train_out").resolve()
    assert exp.sample.runtime.out_dir == (config_dir / "sample_out").resolve()
    assert exp.shared.dataset_dir == (config_dir / "prep_dataset").resolve()


def test_explicit_sample_runtime_overrides(tmp_path: Path) -> None:
    """Test explicit sample runtime overrides."""
    cfg_path = tmp_path / "exp2.toml"
    text = minimal_full_experiment_toml(
        dataset_dir=Path("./data"),
        out_dir=Path("./out"),
        extra_train="""
eval_interval = 100
eval_iters = 20
tensorboard_enabled = true
""",
        extra_sample="""
eval_interval = 200
tensorboard_enabled = false
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
    assert runtime.tensorboard_enabled is False


def test_data_config_tokenizer_choices() -> None:
    """Test data config tokenizer choices."""
    DataConfig(tokenizer="char")
    DataConfig(tokenizer="word")
    DataConfig(tokenizer="tiktoken")


def test_dataconfig_positive_ints() -> None:
    """Test dataconfig positive ints."""
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


def test_sample_config_validation_rejects_out_of_range_params() -> None:
    """Test sample config validation rejects out-of-range parameters."""
    with pytest.raises(ValidationError):
        SampleConfig(temperature=0.0)
    with pytest.raises(ValidationError):
        SampleConfig(top_k=-1)
    with pytest.raises(ValidationError):
        SampleConfig(top_p=0.0)
    with pytest.raises(ValidationError):
        SampleConfig(top_p=1.5)
    SampleConfig(temperature=0.1, top_k=0, top_p=0.5)


def test_lr_schedule_validation_rejects_invalid_inputs() -> None:
    """Test lr schedule validation rejects invalid inputs."""
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


def test_optimconfig_non_negative() -> None:
    """Test optimconfig non negative."""
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


def test_model_config_validation_rejects_invalid_ranges() -> None:
    """Test model config validation rejects invalid ranges."""
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


def test_default_constants_across_configs() -> None:
    """Test default constants across configs."""
    schedule = LRSchedule()
    assert schedule.decay_lr is True
    assert schedule.warmup_iters == 2_000
    assert schedule.lr_decay_iters == 600_000
    assert schedule.min_lr == 6e-5

    optim = OptimConfig()
    assert math.isclose(optim.learning_rate, 6e-4)
    assert math.isclose(optim.weight_decay, 1e-1)
    assert math.isclose(optim.beta1, 0.9)
    assert math.isclose(optim.beta2, 0.95)
    assert math.isclose(optim.grad_clip, 1.0)

    model = ModelConfig()
    assert model.n_layer == 12
    assert model.n_head == 12
    assert model.n_embd == 767
    assert model.block_size == 1024

    sample = SampleConfig()
    assert sample.start == "\n"
    assert sample.num_samples == 3
    assert sample.max_new_tokens == 200
    assert math.isclose(sample.temperature, 0.8)
    assert sample.top_k == 200
    assert sample.top_p is None


def test_runtime_checkpointing_keep_non_negative(tmp_path: Path) -> None:
    """Test runtime checkpointing keep non negative."""
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


def test_runtimeconfig_defaults_and_checkpointing() -> None:
    """Test runtimeconfig defaults and checkpointing."""
    runtime = RuntimeConfig(out_dir=Path("./out"))
    assert runtime.max_iters == 600_000
    assert runtime.eval_interval == 2_000
    assert runtime.eval_iters == 200
    assert runtime.log_interval == 1
    assert runtime.eval_only is False
    assert runtime.seed == 1337
    assert runtime.device == "cpu"
    assert runtime.dtype == "float32"
    assert runtime.compile is False
    assert runtime.tensorboard_enabled is True

    checkpoint = runtime.checkpointing
    assert checkpoint.read_policy in ("latest", "best")
    assert checkpoint.keep.last == 1
    assert checkpoint.keep.best == 1
    assert runtime.ckpt_metric in ("val_loss", "perplexity")
    assert runtime.ckpt_greater_is_better is False
    assert runtime.ckpt_atomic is True
    assert runtime.ckpt_write_metadata is True
    assert runtime.ckpt_time_interval_minutes == 0


def test_merge_mappings_nested_and_replace() -> None:
    """Test merge mappings nested and replace."""
    base = {"a": 1, "b": {"x": 1, "y": 2}, "c": {"k": 1}, "d": 4}
    override = {"b": {"y": 20, "z": 3}, "c": 5, "e": 6}
    out = merge_mappings(base, override)
    assert out["b"] == {"x": 1, "y": 20, "z": 3}
    assert out["c"] == 5
    assert out["a"] == 1 and out["d"] == 4
    assert out["e"] == 6


def test_merge_mappings_numeric_replacements() -> None:
    """Test merge mappings numeric replacements."""
    base = {"a": {"x": 1, "y": -2}, "b": 10}
    override = {"a": {"x": 3}, "b": 0}
    out = merge_mappings(base, override)
    assert out["a"]["x"] == 3
    assert out["a"]["y"] == -2
    assert out["b"] == 0


def test_merge_mappings_type_replacement() -> None:
    """Test merge mappings type replacement."""
    base = {"a": {"x": 1}, "b": {"y": 2}}
    override = {"b": 7}
    out = merge_mappings(base, override)
    assert out["a"] == {"x": 1}
    assert out["b"] == 7


def test_trainer_resolves_relative_runtime_out_dir(tmp_path: Path) -> None:
    """Test trainer resolves relative runtime out dir."""
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


def test_sampler_resolves_relative_runtime_out_dir(tmp_path: Path) -> None:
    """Test sampler resolves relative runtime out dir."""
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


def test_experiment_config_shared_path_coercions(tmp_path: Path) -> None:
    """Test experiment config shared path coercions."""
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


def test_cross_field_validations(tmp_path: Path) -> None:
    """Test cross field validations."""
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


def test_dataconfig_paths_and_defaults() -> None:
    """Test dataconfig paths and defaults."""
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


def test_dataconfig_meta_none_rejected() -> None:
    """Test dataconfig meta none rejected."""
    with pytest.raises(ValidationError):
        DataConfig(meta_pkl=cast(Any, None))


def test_preparerconfig_path_coercion_and_resolve(tmp_path: Path) -> None:
    """Test preparerconfig path coercion and resolve."""
    config = PreparerConfig(raw_dir=tmp_path / "raw")
    assert isinstance(config.raw_dir, Path)
    _ = config.raw_dir


def test_sampleconfig_more_ranges() -> None:
    """Test sampleconfig more ranges."""
    with pytest.raises(ValidationError):
        SampleConfig(num_samples=0)
    with pytest.raises(ValidationError):
        SampleConfig(max_new_tokens=0)
    with pytest.raises(ValidationError):
        SampleConfig(temperature=-0.1)
    SampleConfig(temperature=1e-6, top_k=0, top_p=1.0)


def test_config_canonical_exports() -> None:
    """Test that canonical configuration modules export expected APIs."""
    from ml_playground.configuration import models
    from ml_playground.configuration import loading

    assert hasattr(models, "TrainerConfig")
    assert hasattr(models, "SamplerConfig")
    assert hasattr(models, "DataConfig")
    assert hasattr(models, "RuntimeConfig")
    assert hasattr(loading, "load_full_experiment_config")


def test_full_loader_incomplete_sample_config(tmp_path: Path) -> None:
    """Test full loader incomplete sample config."""
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


def test_full_loader_no_train_section_raises(tmp_path: Path) -> None:
    """Test full loader no train section raises."""
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


def test_full_loader_no_sample_section_raises(tmp_path: Path) -> None:
    """Test full loader no sample section raises."""
    toml_text = minimal_full_experiment_toml(
        dataset_dir=Path("data/shakespeare"),
        out_dir=Path("out/test"),
        include_sample=False,
    )
    cfg_path = tmp_path / "no_sample.toml"
    cfg_path.write_text(toml_text)
    with pytest.raises(ValidationError):
        config_loading.load_experiment_toml(cfg_path)


def test_full_loader_train_missing_data_section(tmp_path: Path) -> None:
    """Test full loader train missing data section."""
    toml_text = minimal_full_experiment_toml(
        dataset_dir=Path("data/shakespeare"),
        out_dir=Path("out/test"),
        include_train_data=False,
    )
    cfg_path = tmp_path / "missing_data.toml"
    cfg_path.write_text(toml_text)

    with pytest.raises(ValidationError):
        config_loading.load_experiment_toml(cfg_path)


def test_full_loader_train_missing_runtime_section(tmp_path: Path) -> None:
    """Test full loader train missing runtime section."""
    toml_text = minimal_full_experiment_toml(
        dataset_dir=Path("data/shakespeare"),
        out_dir=Path("out/test"),
        include_train_runtime=False,
    )
    cfg_path = tmp_path / "missing_runtime.toml"
    cfg_path.write_text(toml_text)

    with pytest.raises(ValidationError):
        config_loading.load_experiment_toml(cfg_path)


def test_full_loader_sample_missing_runtime_section(tmp_path: Path) -> None:
    """Test full loader sample missing runtime section."""
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


def test_cli_adapters_load_and_validate(tmp_path: Path) -> None:
    """Test cli adapters load and validate."""
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


def test_cli_adapters_prerequisites(tmp_path: Path) -> None:
    """Test cli adapters prerequisites."""
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


def test_cli_ensure_train_prerequisites_missing_meta(tmp_path: Path) -> None:
    """Test cli ensure train prerequisites missing meta."""
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


def test_cli_ensure_sample_prerequisites_missing_meta(tmp_path: Path) -> None:
    """Test cli ensure sample prerequisites missing meta."""
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


def test_internal_path_helpers(tmp_path: Path) -> None:
    """Test internal path helpers."""
    bad_path = tmp_path / "bad"

    def fake_resolve(path: Path) -> Path:
        if path == bad_path:
            raise OSError("cannot resolve")
        return path

    with pytest.raises(ValueError, match="Invalid path"):
        config_models._resolve_path_strict(bad_path, resolve=fake_resolve)  # pyright: ignore[reportPrivateUsage]

    relative = config_models._resolve_if_relative("rel", tmp_path, resolve=Path.resolve)  # pyright: ignore[reportPrivateUsage]
    assert isinstance(relative, Path) and relative.is_absolute()

    absolute_path = tmp_path / "abs"
    assert (
        config_models._resolve_if_relative(  # pyright: ignore[reportPrivateUsage]
            absolute_path, tmp_path, resolve=Path.resolve
        )
        == absolute_path
    )
    assert config_models._resolve_if_relative(absolute_path, tmp_path) == absolute_path  # pyright: ignore[reportPrivateUsage]


def test_no_nan_validator_raises() -> None:
    """Test no nan validator raises."""
    with pytest.raises(ValidationError):
        config_models.OptimConfig(learning_rate=float("nan"))


def test_preparer_config_context_path_resolution(tmp_path: Path) -> None:
    """Test preparer config context path resolution."""
    cfg = config_models.PreparerConfig.model_validate(
        {"raw_dir": "data", "raw_text_path": "texts/in.txt"},
        context={"config_path": tmp_path / "cfg.toml"},
    )
    assert cfg.raw_dir.is_absolute()
    assert cfg.raw_text_path and cfg.raw_text_path.is_absolute()
    # Non-path context should leave values unchanged aside from Path coercion,
    # which produces absolute paths relative to the current working directory.
    cfg2 = config_models.PreparerConfig.model_validate(
        {"raw_dir": Path("data")},
        context={"config_path": "not-a-path"},
    )
    assert cfg2.raw_dir.is_absolute()
    assert cfg2.raw_dir.name == "data"


def test_peft_config_coerces_target_modules() -> None:
    """Test peft config coerces target modules."""
    peft = config_models.TrainerConfig.PeftConfig.model_validate(
        {"target_modules": ["a", "b"], "enabled": True}
    )
    assert isinstance(peft.target_modules, tuple)
    assert peft.target_modules == ("a", "b")


def test_experiment_config_resolve_paths_from_dict(tmp_path: Path) -> None:
    """Test experiment config resolve paths from dict."""
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

    result = cast(Mapping[str, Any], ExperimentConfigTestHarness.resolve_paths(data))

    shared = cast(Mapping[str, Any], result["shared"])
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


def test_experiment_config_resolve_paths_with_namespace(tmp_path: Path) -> None:
    """Test experiment config resolve paths with namespace."""
    cfg_path = tmp_path / "cfg.toml"
    shared_ns = SimpleNamespace(config_path=cfg_path)
    data = {"shared": shared_ns}
    # Should not raise or mutate namespace for non-dict shared data
    result = cast(Mapping[str, Any], ExperimentConfigTestHarness.resolve_paths(data))
    assert result["shared"] is shared_ns


def test_shared_config_resolves_relative_paths(tmp_path: Path) -> None:
    """Test shared config resolves relative paths."""
    cfg_path = tmp_path / "cfg.toml"
    data = {
        "config_path": cfg_path,
        "project_home": "proj",
        "dataset_dir": "data",
        "train_out_dir": "train",
        "sample_out_dir": "sample",
    }
    resolved = SharedConfigTestHarness.resolve_paths(data.copy())
    assert resolved["project_home"].is_absolute()
    assert resolved["dataset_dir"].is_absolute()


def _trainer_dict(tmp_path: Path) -> dict[str, Any]:
    model = ModelConfig().model_dump()
    data = DataConfig().model_dump()
    optim = OptimConfig().model_dump()
    schedule = LRSchedule().model_dump()
    runtime = config_models.RuntimeConfig(out_dir=tmp_path / "out").model_dump(
        exclude={"total_eval_steps"}
    )
    runtime["out_dir"] = "rel_out"
    return {
        "model": model,
        "data": data,
        "optim": optim,
        "schedule": schedule,
        "runtime": runtime,
    }


def test_trainer_config_resolve_paths_with_context(tmp_path: Path) -> None:
    """Test trainer config resolve paths with context."""
    trainer_dict = _trainer_dict(tmp_path)
    cfg_path = tmp_path / "cfg.toml"
    trainer = TrainerConfig.model_validate(
        trainer_dict,
        context={"config_path": cfg_path},
    )
    assert trainer.runtime.out_dir.is_absolute()


def test_trainer_config_resolve_paths_without_context(tmp_path: Path) -> None:
    """Test trainer config resolve paths without context."""
    trainer_dict = _trainer_dict(tmp_path)
    trainer_dict["runtime"]["out_dir"] = Path("rel_out")
    trainer = TrainerConfig.model_validate(
        trainer_dict,
        context={"config_path": "not-a-path"},
    )
    assert trainer.runtime.out_dir.is_absolute()
    assert trainer.runtime.out_dir.name == "rel_out"


def _sampler_dict(tmp_path: Path) -> dict[str, Any]:
    runtime = config_models.RuntimeConfig(out_dir=tmp_path / "out").model_dump(
        exclude={"total_eval_steps"}
    )
    runtime["out_dir"] = "rel_out"
    sample = SampleConfig().model_dump()
    return {"runtime": runtime, "sample": sample}


def test_sampler_config_resolve_paths_with_context(tmp_path: Path) -> None:
    """Test sampler config resolve paths with context."""
    sampler_dict = _sampler_dict(tmp_path)
    cfg_path = tmp_path / "cfg.toml"
    sampler = config_models.SamplerConfig.model_validate(
        sampler_dict,
        context={"config_path": cfg_path},
    )
    assert sampler.runtime.out_dir.is_absolute()


def test_sampler_config_resolve_paths_without_context(tmp_path: Path) -> None:
    """Test sampler config resolve paths without context."""
    sampler_dict = _sampler_dict(tmp_path)
    sampler_dict["runtime"]["out_dir"] = Path("rel_out")
    sampler = config_models.SamplerConfig.model_validate(
        sampler_dict,
        context={"config_path": "not-a-path"},
    )
    assert sampler.runtime.out_dir.is_absolute()
    assert sampler.runtime.out_dir.name == "rel_out"


def test_experiment_config_resolve_paths_non_dict() -> None:
    """Test experiment config resolve paths non dict."""
    assert ExperimentConfigTestHarness.resolve_paths(123) == 123


def test_experiment_config_resolve_paths_missing_config_path(tmp_path: Path) -> None:
    """Test experiment config resolve paths missing config path."""
    data = {
        "shared": {"experiment": "unit"},
        "prepare": {},
    }
    assert ExperimentConfigTestHarness.resolve_paths(data) is data


def test_shared_config_resolve_paths_invalid_config_path() -> None:
    """Test shared config resolve paths invalid config path."""
    data = {"config_path": object(), "project_home": "rel", "dataset_dir": 123}
    resolved = cast(
        Mapping[str, Any], SharedConfigTestHarness.resolve_paths(data.copy())
    )
    assert dict(resolved) == data


def test_runtime_config_validates_log_interval(tmp_path: Path) -> None:
    """Test runtime config validates log interval."""
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
            tensorboard_enabled=False,
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
            tensorboard_enabled=False,
        ),
    }


def test_trainer_config_rejects_large_data_block(tmp_path: Path) -> None:
    """Test trainer config rejects large data block."""
    kwargs = _base_trainer_kwargs(tmp_path)
    kwargs["data"] = DataConfig(
        batch_size=2,
        block_size=8,
        grad_accum_steps=1,
        tokenizer="char",
    )
    with pytest.raises(ValidationError):
        TrainerConfig(**kwargs)


def test_trainer_config_rejects_min_lr_above_learning_rate(tmp_path: Path) -> None:
    """Test trainer config rejects min lr above learning rate."""
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


def test_trainer_config_requires_zero_warmup_without_decay(tmp_path: Path) -> None:
    """Test trainer config requires zero warmup without decay."""
    kwargs = _base_trainer_kwargs(tmp_path)
    kwargs["schedule"] = LRSchedule(
        decay_lr=False,
        warmup_iters=1,
        lr_decay_iters=10,
        min_lr=0.01,
    )
    with pytest.raises(ValidationError):
        TrainerConfig(**kwargs)


def test_lr_schedule_requires_warmup_le_decay_iters() -> None:
    """Test lr schedule requires warmup le decay iters."""
    with pytest.raises(ValidationError):
        LRSchedule(
            decay_lr=True,
            warmup_iters=5,
            lr_decay_iters=4,
            min_lr=0.01,
        )


def test_data_config_requires_ngram_one_for_non_tiktoken() -> None:
    """Test data config requires ngram one for non tiktoken."""
    with pytest.raises(ValidationError):
        DataConfig(
            batch_size=2,
            block_size=4,
            grad_accum_steps=1,
            tokenizer="word",
            ngram_size=2,
        )


def test_resolve_if_relative_edge_cases(tmp_path: Path) -> None:
    """Test edge cases in _resolve_if_relative function to cover missing lines."""
    from ml_playground.configuration.utils import resolve_if_relative

    # Test with non-string, non-Path value (should return as-is) - covers line 60
    result = resolve_if_relative(123, tmp_path)
    assert result == 123

    # Test with None value
    result = resolve_if_relative(None, tmp_path)
    assert result is None


def test_type_checking_branch_coverage() -> None:
    """Test TYPE_CHECKING branch to cover line 25."""
    # This line is only executed at import time, but we can verify the constants exist
    from ml_playground.configuration.models import (
        READ_POLICY_LATEST,
        READ_POLICY_BEST,
        DEFAULT_READ_POLICY,
    )

    assert READ_POLICY_LATEST == "latest"
    assert READ_POLICY_BEST == "best"
    assert DEFAULT_READ_POLICY == "best"


def test_coerce_path_edge_cases() -> None:
    """Test _coerce_path function edge cases to cover missing lines."""
    from ml_playground.configuration.utils import coerce_path

    # Test with invalid string that can't be converted to Path - covers lines 100-101
    result = coerce_path("")
    assert result == Path("")  # Empty string is valid Path

    # Test with object that raises exception during Path conversion
    class BadPathLike:
        def __str__(self) -> str:
            raise ValueError("Cannot convert to string")

    result = coerce_path(BadPathLike())
    assert result is None


def test_experiment_config_path_resolution_edge_cases(tmp_path: Path) -> None:
    """Test edge cases in ExperimentConfig path resolution to cover missing lines."""

    cfg_path = tmp_path / "cfg.toml"

    # Test with shared data that has config_path but no other path fields - covers line 271
    data: Mapping[str, Any] = {
        "shared": {"config_path": cfg_path, "experiment": "test"},
        "prepare": {"raw_dir": "data"},
        "train": {
            "runtime": {"out_dir": "train_out"},
            "model": {},
            "data": {},
            "optim": {},
            "schedule": {},
        },
        "sample": {"runtime": {"out_dir": "sample_out"}, "sample": {}},
    }

    # This should not raise and should process the paths
    result = cast(Mapping[str, Any], ExperimentConfigTestHarness.resolve_paths(data))
    assert "shared" in result


def test_cross_field_validator_coverage() -> None:
    """Test cross-field validators to cover missing lines."""
    from ml_playground.configuration.models import RuntimeConfig, LRSchedule

    # Instantiating should trigger validators without exceptions
    RuntimeConfig(
        out_dir=Path("out"),
        log_interval=1,
        eval_interval=2,
    )

    LRSchedule(
        decay_lr=True,
        warmup_iters=100,
        lr_decay_iters=1000,
        min_lr=0.01,
    )


def test_normalize_runtime_out_dir_edge_cases(tmp_path: Path) -> None:
    """Test _normalize_runtime_out_dir function edge cases."""

    cfg_path = tmp_path / "cfg.toml"

    # Test with section that doesn't have runtime - covers lines 525-528, 530-533
    data: Mapping[str, Any] = {
        "shared": {"config_path": cfg_path},
        "train": {
            "model": {},
            "data": {},
            "optim": {},
            "schedule": {},
        },  # No runtime section
        "sample": {"sample": {}},  # No runtime section
    }

    result = cast(Mapping[str, Any], ExperimentConfigTestHarness.resolve_paths(data))
    assert "train" in result
    assert "sample" in result


def test_shared_config_path_coercion_edge_cases(tmp_path: Path) -> None:
    """Test SharedConfig path coercion edge cases."""
    cfg_path = tmp_path / "cfg.toml"

    # Test with data that has config_path but other fields are not paths - covers lines 548, 563
    data: Mapping[str, Any] = {
        "config_path": cfg_path,
        "experiment": "test",
        "project_home": 123,  # Not a path-like object
        "dataset_dir": None,  # None value
        "train_out_dir": cfg_path,  # Valid path
        "sample_out_dir": "relative_path",  # String path
    }

    result = cast(Mapping[str, Any], SharedConfigTestHarness.resolve_paths(data))
    assert result["project_home"] == 123  # Should remain unchanged
    assert result["dataset_dir"] is None  # Should remain unchanged
    assert isinstance(result["train_out_dir"], Path)
    assert isinstance(result["sample_out_dir"], Path)


def test_peft_config_target_modules_edge_case() -> None:
    """Test PeftConfig target_modules coercion edge case - covers line 501."""
    from ml_playground.configuration.models import TrainerConfig

    # Test with data that doesn't have target_modules key
    data = {"enabled": True, "r": 8}
    peft = TrainerConfig.PeftConfig.model_validate(data)
    assert peft.target_modules == ()  # Should use default empty tuple


def test_runtime_validator_log_interval_exceeds_eval_interval() -> None:
    """RuntimeConfig validator should raise when log_interval > eval_interval."""
    from ml_playground.configuration.models import RuntimeConfig

    with pytest.raises(ValidationError) as exc:
        RuntimeConfig(
            out_dir=Path("out"),
            log_interval=100,
            eval_interval=50,
        )

    assert "log_interval must be <=" in str(exc.value)


def test_trainer_validator_data_block_size_exceeds_model() -> None:
    """TrainerConfig validator should raise when data.block_size > model.block_size."""
    from ml_playground.configuration.models import (
        TrainerConfig,
        ModelConfig,
        DataConfig,
        OptimConfig,
        LRSchedule,
        RuntimeConfig,
    )

    with pytest.raises(ValidationError) as exc:
        TrainerConfig(
            model=ModelConfig(block_size=512),
            data=DataConfig(block_size=1024),  # Exceeds model block_size
            optim=OptimConfig(),
            schedule=LRSchedule(),
            runtime=RuntimeConfig(out_dir=Path("out")),
        )

    assert "block_size must be <=" in str(exc.value)


def test_trainer_validator_min_lr_exceeds_learning_rate_with_decay() -> None:
    """TrainerConfig validator should raise when min_lr > learning_rate with decay_lr=true."""
    from ml_playground.configuration.models import (
        TrainerConfig,
        ModelConfig,
        DataConfig,
        OptimConfig,
        LRSchedule,
        RuntimeConfig,
    )

    with pytest.raises(ValidationError) as exc:
        TrainerConfig(
            model=ModelConfig(),
            data=DataConfig(),
            optim=OptimConfig(learning_rate=0.001),
            schedule=LRSchedule(decay_lr=True, min_lr=0.01),  # min_lr > learning_rate
            runtime=RuntimeConfig(out_dir=Path("out")),
        )

    assert "min_lr must be <=" in str(exc.value)


def test_trainer_validator_warmup_iters_nonzero_without_decay() -> None:
    """TrainerConfig validator should raise when warmup_iters != 0 but decay_lr=false."""
    from ml_playground.configuration.models import (
        TrainerConfig,
        ModelConfig,
        DataConfig,
        OptimConfig,
        LRSchedule,
        RuntimeConfig,
    )

    with pytest.raises(ValidationError) as exc:
        TrainerConfig(
            model=ModelConfig(),
            data=DataConfig(),
            optim=OptimConfig(),
            schedule=LRSchedule(
                decay_lr=False, warmup_iters=100
            ),  # warmup without decay
            runtime=RuntimeConfig(out_dir=Path("out")),
        )

    assert "warmup_iters must be 0" in str(exc.value)


def test_lr_schedule_validator_warmup_exceeds_decay() -> None:
    """LRSchedule validator should raise when warmup_iters > lr_decay_iters."""
    from ml_playground.configuration.models import LRSchedule

    with pytest.raises(ValidationError) as exc:
        LRSchedule(
            warmup_iters=1000,
            lr_decay_iters=500,  # warmup > decay
        )

    assert "warmup_iters must be <=" in str(exc.value)


def test_data_config_validator_ngram_with_non_tiktoken() -> None:
    """DataConfig validator should raise when ngram_size != 1 with non-tiktoken tokenizer."""
    from ml_playground.configuration.models import DataConfig

    with pytest.raises(ValidationError) as exc:
        DataConfig(
            tokenizer="char",
            ngram_size=2,  # ngram_size must be 1 for non-tiktoken
        )

    assert "ngram_size must be 1" in str(exc.value)


def test_data_config_validator_ngram_with_tiktoken_passes() -> None:
    """DataConfig validator should allow ngram_size > 1 with tiktoken tokenizer."""
    from ml_playground.configuration.models import DataConfig

    # Should not raise
    cfg = DataConfig(
        tokenizer="tiktoken",
        ngram_size=2,
    )
    assert cfg.ngram_size == 2


def test_peft_config_target_modules_from_set() -> None:
    """PeftConfig should coerce target_modules from set to tuple."""
    from ml_playground.configuration.models import TrainerConfig

    data = {
        "enabled": True,
        "r": 8,
        "target_modules": {"module1", "module2"},  # Set input
    }
    peft = TrainerConfig.PeftConfig.model_validate(data)
    assert isinstance(peft.target_modules, tuple)
    assert set(peft.target_modules) == {"module1", "module2"}


def test_peft_config_target_modules_from_sequence() -> None:
    """PeftConfig should coerce target_modules from sequence to tuple."""
    from ml_playground.configuration.models import TrainerConfig

    data = {
        "enabled": True,
        "r": 8,
        "target_modules": ["module1", "module2"],  # List input
    }
    peft = TrainerConfig.PeftConfig.model_validate(data)
    assert isinstance(peft.target_modules, tuple)
    assert peft.target_modules == ("module1", "module2")
