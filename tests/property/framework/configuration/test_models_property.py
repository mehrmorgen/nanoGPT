from typing import Any, cast
from pathlib import Path
from tempfile import TemporaryDirectory
import string

import pytest
from hypothesis import assume, given, settings, strategies as st
from pydantic import ValidationError

from ml_playground.framework.configuration import models as config_models


def _make_runtime(**overrides: Any) -> config_models.RuntimeConfig:
    params: dict[str, Any] = {
        "out_dir": Path("."),
        "eval_interval": 2,
        "log_interval": 1,
        "eval_iters": 1,
        "max_iters": 100,
    }
    params.update(overrides)
    return config_models.RuntimeConfig(**params)


def _build_trainer_config(
    *, model_block: int, data_block: int, schedule: config_models.LRSchedule
) -> config_models.TrainerConfig:
    return config_models.TrainerConfig(
        model=config_models.ModelConfig(block_size=model_block),
        data=config_models.DataConfig(
            batch_size=1,
            block_size=data_block,
            grad_accum_steps=1,
            ngram_size=1,
            tokenizer="char",
        ),
        optim=config_models.OptimConfig(),
        schedule=schedule,
        runtime=_make_runtime(),
    )


@settings(max_examples=25, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    log_interval=st.integers(min_value=2, max_value=2000),
    eval_interval=st.integers(min_value=1, max_value=1999),
)
def test_runtime_log_interval_validator(log_interval: int, eval_interval: int) -> None:
    assume(log_interval > eval_interval)
    with pytest.raises(ValueError, match="log_interval must be <="):
        _make_runtime(eval_interval=eval_interval, log_interval=log_interval)


@settings(max_examples=25, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    log_games=st.integers(min_value=1, max_value=20),
    eval_games=st.integers(min_value=1, max_value=19),
)
def test_runtime_log_interval_games_validator(log_games: int, eval_games: int) -> None:
    assume(log_games > eval_games)
    with pytest.raises(ValueError, match="log_interval_games must be <="):
        _make_runtime(eval_interval_games=eval_games, log_interval_games=log_games + 1)


def test_runtime_device_mps_compile_and_dtype_guard() -> None:
    with pytest.raises(ValueError, match="runtime.compile must be false"):
        _make_runtime(device="mps", compile=True)
    with pytest.raises(
        ValueError, match="runtime.dtype float16 is not supported on mps"
    ):
        _make_runtime(device="mps", dtype="float16")


def test_runtime_ckpt_domain_label_validations() -> None:
    with pytest.raises((ValueError, ValidationError), match="ckpt_domain_label"):
        _make_runtime(ckpt_naming_policy="domain", ckpt_domain_label=None)
    with pytest.raises((ValueError, ValidationError), match="must match"):
        _make_runtime(ckpt_naming_policy="domain", ckpt_domain_label="BAD-LABEL!")


def test_runtime_mlflow_disable_clears_uri() -> None:
    cfg = _make_runtime(mlflow_enabled=False, mlflow_tracking_uri="sqlite:///mlruns.db")
    assert cfg.mlflow_tracking_uri is None


def test_runtime_total_eval_helpers() -> None:
    cfg = _make_runtime()
    object.__setattr__(cfg, "eval_interval", 0)
    assert cfg.total_eval_steps == 0
    object.__setattr__(cfg, "eval_interval", 5)
    object.__setattr__(cfg, "max_iters", 27)
    assert cfg.total_eval_steps == 27 // 5
    object.__setattr__(cfg, "eval_interval_games", 0)
    object.__setattr__(cfg, "max_games", 10)
    assert cfg.total_eval_games == 0


@settings(max_examples=25, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    max_iters=st.integers(min_value=1, max_value=10000),
    eval_interval=st.integers(min_value=1, max_value=1000),
    max_games=st.integers(min_value=0, max_value=1000),
    eval_interval_games=st.integers(min_value=1, max_value=100),
)
def test_runtime_computed_fields_invariants(
    max_iters: int, eval_interval: int, max_games: int, eval_interval_games: int
) -> None:
    """Computed fields should equal integer division of max by interval."""
    cfg = _make_runtime(
        max_iters=max_iters,
        eval_interval=eval_interval,
        max_games=max_games,
        eval_interval_games=eval_interval_games,
    )
    assert cfg.total_eval_steps == max_iters // eval_interval
    assert cfg.total_eval_games == max_games // eval_interval_games


@settings(max_examples=25, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    log_games=st.integers(min_value=2, max_value=100),
    eval_games=st.integers(min_value=1, max_value=99),
)
def test_runtime_domain_counters_validator(log_games: int, eval_games: int) -> None:
    """Domain game counters enforce log_interval_games <= eval_interval_games."""
    assume(log_games > eval_games)
    with pytest.raises(ValueError, match="runtime.log_interval_games must be <="):
        _make_runtime(
            eval_interval_games=eval_games,
            log_interval_games=log_games,
        )


@settings(max_examples=25, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    model_block=st.integers(min_value=1, max_value=10),
    data_block=st.integers(min_value=2, max_value=20),
)
def test_trainer_data_block_size_validator(model_block: int, data_block: int) -> None:
    assume(data_block > model_block)
    with pytest.raises(ValueError, match="training\\.data\\.block_size"):
        _build_trainer_config(
            model_block=model_block,
            data_block=data_block,
            schedule=config_models.LRSchedule(),
        )


def test_trainer_schedule_min_lr_validator() -> None:
    schedule = config_models.LRSchedule(min_lr=1.0)
    with pytest.raises(ValueError, match="training\\.schedule\\.min_lr"):
        _build_trainer_config(model_block=4, data_block=4, schedule=schedule)


def test_trainer_warmup_validator() -> None:
    schedule = config_models.LRSchedule(decay_lr=False, warmup_iters=1)
    with pytest.raises(ValueError, match="training\\.schedule\\.warmup_iters"):
        _build_trainer_config(model_block=4, data_block=4, schedule=schedule)


def _safe_name() -> st.SearchStrategy[str]:
    return st.text(min_size=1, max_size=2).filter(lambda s: "/" not in s)


@settings(max_examples=25, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    names=st.tuples(_safe_name(), _safe_name(), _safe_name())
)
def test_peft_adapter_output_policy_names(names: tuple[str, str, str]) -> None:
    assume(len(set(names)) == len(names))
    policy = config_models.TrainerConfig.PeftConfig.AdapterOutputPolicy(
        base_dir="adapters", best_name=names[0], last_name=names[1], final_name=names[2]
    )
    cast(Any, policy)._validate_names()
    resolved = policy.resolve(Path("/tmp"))
    assert resolved["best"].name == policy.best_name
    assert resolved["last"].name == policy.last_name
    assert resolved["final"].name == policy.final_name


@settings(max_examples=15, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    names=st.tuples(_safe_name(), st.just("dup"), st.just("dup"))
)
def test_peft_adapter_output_policy_duplicate_names(
    names: tuple[str, str, str],
) -> None:
    with pytest.raises(ValidationError, match="unique"):
        config_models.TrainerConfig.PeftConfig.AdapterOutputPolicy(
            base_dir="adapters",
            best_name=names[0],
            last_name=names[1],
            final_name=names[2],
        )


def test_peft_adapter_target_modules_coerce_tuple() -> None:
    peft_dict = {"target_modules": ["a", "b"]}
    truncated = cast(
        Any, config_models.TrainerConfig.PeftConfig
    )._coerce_target_modules(peft_dict)
    assert isinstance(truncated["target_modules"], tuple)


def test_pool_size_policy_derives_size() -> None:
    policy = config_models.PoolSizePolicy(
        target_labeled_positions=10, avg_positions_per_game=2, oversample_factor=1.5
    )
    assert policy.pool_size == config_models.derive_pool_size(
        10, 2, oversample_factor=1.5
    )


def test_resolve_mlflow_tracking_uri_relative(tmp_path: Path) -> None:
    uri = f"sqlite:///{tmp_path.name}/mlflow.db"
    resolved = config_models._resolve_mlflow_tracking_uri(uri, tmp_path)
    assert resolved is not None
    assert resolved.startswith("sqlite:///")
    assert tmp_path.name in resolved


def test_metadata_config_resolves_paths(tmp_path: Path) -> None:
    data = {
        "experiment": "demo",
        "config_path": str(tmp_path / "config.toml"),
        "project_home": "home",
        "dataset_dir": "dataset",
        "train_out_dir": "train",
        "sample_out_dir": "sample",
    }
    resolved = cast(Any, config_models.MetadataConfig)._resolve_metadata_paths(
        data.copy()
    )
    assert isinstance(resolved["project_home"], Path)
    assert resolved["project_home"].is_absolute()


def test_experiment_config_resolves_prepare_paths(tmp_path: Path) -> None:
    cfg = {
        "prepare": {"raw_dir": "raw", "dataset_dir": "data"},
        "training": {"runtime": {"out_dir": "train"}},
        "sampling": {"runtime": {"out_dir": "sample"}},
        "metadata": {
            "experiment": "demo",
            "config_path": str(tmp_path / "config.toml"),
            "project_home": str(tmp_path / "project"),
            "dataset_dir": str(tmp_path / "data"),
            "train_out_dir": str(tmp_path / "train"),
            "sample_out_dir": str(tmp_path / "sample"),
        },
    }
    resolved = cast(Any, config_models.ExperimentConfig)._resolve_paths(cfg)
    project_home = resolved["metadata"]["project_home"]
    assert Path(project_home).is_absolute()


@settings(max_examples=20, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    relative=st.text(
        alphabet=string.ascii_letters + string.digits, min_size=1, max_size=12
    )
)
def test_resolve_if_relative_converts_relative_strings(relative: str) -> None:
    """Resolve relative string inputs against the supplied base directory."""
    with TemporaryDirectory() as tmp_dir:
        base_dir = Path(tmp_dir) / "base"
        resolved = config_models._resolve_if_relative(relative, base_dir)
        assert resolved == (base_dir / relative).resolve()


@settings(max_examples=15, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    relative=st.text(alphabet=string.ascii_lowercase, min_size=1, max_size=6)
)
def test_resolve_if_relative_converts_rel_paths(relative: str) -> None:
    """Resolve relative Path inputs by anchoring them to the base directory."""
    with TemporaryDirectory() as tmp_dir:
        base_dir = Path(tmp_dir) / "base"
        candidate = Path(relative)
        resolved = config_models._resolve_if_relative(candidate, base_dir)
        assert resolved == (base_dir / relative).resolve()


def test_resolve_if_relative_leaves_absolute_alone(tmp_path: Path) -> None:
    """Do not rewrite already-absolute paths."""
    absolute = (tmp_path / "already").resolve()
    assert config_models._resolve_if_relative(absolute, tmp_path) == absolute


def test_resolve_mlflow_tracking_uri_resolves_sqlite_relative(tmp_path: Path) -> None:
    """Relative sqlite URIs should be remapped under the config base directory."""
    uri = "sqlite:///mlruns/artifacts.sqlite"
    resolved = config_models._resolve_mlflow_tracking_uri(uri, tmp_path)
    assert resolved is not None
    assert resolved.startswith("sqlite:///")
    assert str((tmp_path / "mlruns/artifacts.sqlite").resolve()) in resolved


def test_resolve_mlflow_tracking_uri_preserves_absolutes(tmp_path: Path) -> None:
    """Absolute sqlite URIs or four-slash prefixes stay untouched."""
    absolute = f"sqlite:////{tmp_path / 'db.sqlite'}"
    assert config_models._resolve_mlflow_tracking_uri(absolute, tmp_path) == absolute
    assert config_models._resolve_mlflow_tracking_uri(None, tmp_path) is None


@settings(max_examples=25, deadline=None, derandomize=True)
@given(  # type: ignore[reportAny]
    warmup=st.integers(min_value=1, max_value=1000),
    decay=st.integers(min_value=0, max_value=999),
)
def test_lr_schedule_rejects_warmup_longer_than_decay(warmup: int, decay: int) -> None:
    """Ensure warmup iterations do not exceed decay iterations."""
    assume(warmup > decay)
    with pytest.raises(ValueError, match="warmup_iters must be <= lr_decay_iters"):
        config_models.LRSchedule(warmup_iters=warmup, lr_decay_iters=decay)


def test_data_config_declines_ngram_with_char_tokenizer() -> None:
    """DataConfig enforces ngram_size=1 when tokenizer is not tiktoken."""
    with pytest.raises(ValueError, match="training\\.data\\.ngram_size must be 1"):
        config_models.DataConfig(tokenizer="char", ngram_size=2)


def test_optimizer_rejects_nan_learning_rate() -> None:
    """OptimConfig ensures NaN values are invalid for learning_rate."""
    with pytest.raises(ValidationError, match="must not be NaN"):
        config_models.OptimConfig(learning_rate=float("nan"))


def test_experience_storage_json_file_requires_path(tmp_path: Path) -> None:
    """json_file strategy must declare a storage path."""
    with pytest.raises(ValueError, match="experience storage path is required"):
        config_models.ExperienceStorageConfig(strategy="json_file")


def test_experience_storage_rejects_directory_paths(tmp_path: Path) -> None:
    """json_file strategy refuses directories as storage targets."""
    directory = tmp_path / "store"
    directory.mkdir()
    with pytest.raises(
        ValueError, match="experience storage path must point to a file"
    ):
        config_models.ExperienceStorageConfig(strategy="json_file", path=directory)


def test_experience_storage_resolves_relative_paths(tmp_path: Path) -> None:
    """Relative storage paths are anchored to the config location."""
    config_path = tmp_path / "config" / "exp.toml"
    config_path.parent.mkdir(parents=True)
    cfg = config_models.ExperienceStorageConfig.model_validate(
        {"strategy": "json_file", "path": "storage.db"},
        context={"config_path": config_path},
    )
    assert cfg.path == (config_path.parent / "storage.db").resolve()


def test_preparer_config_resolves_relative_paths(tmp_path: Path) -> None:
    """PreparerConfig resolves raw_dir and raw_text_path relative to the config."""
    config_path = tmp_path / "config/exp.toml"
    cfg = config_models.PreparerConfig.model_validate(
        {"raw_dir": "raw", "raw_text_path": "raw/data.txt"},
        context={"config_path": config_path},
    )
    assert cfg.raw_dir.is_absolute()
    assert cfg.raw_text_path is not None and cfg.raw_text_path.is_absolute()


def test_trainer_config_runtime_paths_resolve(tmp_path: Path) -> None:
    """Trainer runtime values are normalized relative to the experiment config."""
    config_path = tmp_path / "config/exp.toml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    trainer = config_models.TrainerConfig.model_validate(
        {
            "model": {},
            "data": {},
            "optim": {},
            "schedule": {},
            "runtime": {
                "out_dir": "train/out",
                "mlflow_enabled": True,
                "mlflow_tracking_uri": "sqlite:///mlruns.db",
            },
        },
        context={"config_path": config_path},
    )
    assert trainer.runtime.out_dir == (config_path.parent / "train/out").resolve()
    assert trainer.runtime.mlflow_tracking_uri is not None
    assert "mlruns.db" in cast(str, trainer.runtime.mlflow_tracking_uri)


def test_sampler_config_runtime_outdir_resolves(tmp_path: Path) -> None:
    """Sampler runtime out_dir paths are normalized relative to the config."""
    config_path = tmp_path / "config/sample.toml"
    sampler = config_models.SamplerConfig.model_validate(
        {
            "runtime": {"out_dir": "sample/out"},
            "sample": {},
        },
        context={"config_path": config_path},
    )
    assert sampler.runtime.out_dir == (config_path.parent / "sample/out").resolve()


def test_experiment_config_metadata_paths_gain_prepare_dataset(tmp_path: Path) -> None:
    """ExperimentConfig hoists prepare dataset paths into metadata config."""
    config_path = tmp_path / "exp/run.toml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    metadata = {
        "experiment": "demo",
        "config_path": config_path,
        "project_home": tmp_path / "home",
        "dataset_dir": tmp_path / "data",
        "train_out_dir": tmp_path / "train",
        "sample_out_dir": tmp_path / "sample",
    }
    cfg = config_models.ExperimentConfig.model_validate(
        {
            "prepare": {"raw_dir": "raw", "dataset_dir": "data"},
            "training": {
                "model": {},
                "data": {},
                "optim": {},
                "schedule": {},
                "runtime": {"out_dir": "train"},
            },
            "sampling": {"runtime": {"out_dir": "sample"}, "sample": {}},
            "metadata": metadata,
        }
    )
    assert cfg.metadata.dataset_dir.is_absolute()
    assert cfg.prepare.raw_dir.is_absolute()


def test_metadata_config_resolves_relative_fields(tmp_path: Path) -> None:
    """MetadataConfig resolves members relative to the config location."""
    config_path = tmp_path / "experiment/config.toml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    data: dict[str, object] = {
        "experiment": "demo",
        "config_path": str(config_path),
        "project_home": "home",
        "dataset_dir": "data",
        "train_out_dir": "train",
        "sample_out_dir": "sample",
    }
    resolved = cast(Any, config_models.MetadataConfig)._resolve_metadata_paths(data)
    assert (
        isinstance(resolved["project_home"], Path)
        and resolved["project_home"].is_absolute()
    )
    assert (
        isinstance(resolved["dataset_dir"], Path)
        and resolved["dataset_dir"].is_absolute()
    )


def test_peft_adapter_output_policy_resolves_paths(tmp_path: Path) -> None:
    """AdapterOutputPolicy resolves each named adapter artifact location."""
    policy = config_models.TrainerConfig.PeftConfig.AdapterOutputPolicy()
    resolved = policy.resolve(tmp_path)
    assert resolved["best"].parent == tmp_path / policy.base_dir
    assert resolved["final"].name == policy.final_name


def test_peft_adapter_output_policy_rejects_empty_names() -> None:
    """Adapter output names must all be non-empty."""
    with pytest.raises(ValueError, match="adapter output names must be non-empty"):
        config_models.TrainerConfig.PeftConfig.AdapterOutputPolicy(
            base_dir="adapters", best_name="", last_name="a", final_name="b"
        )


def test_peft_adapter_output_policy_rejects_empty_base_dir() -> None:
    """Adapter base_dir cannot be empty."""
    with pytest.raises(ValueError, match="base_dir must be non-empty"):
        config_models.TrainerConfig.PeftConfig.AdapterOutputPolicy(base_dir="")


def test_peft_target_modules_list_coerces_to_tuple() -> None:
    """List target modules are coerced into tuples before validation."""
    policy = config_models.TrainerConfig.PeftConfig.model_validate(
        {"target_modules": ["a", "b"]}
    )
    assert isinstance(policy.target_modules, tuple)


def test_data_config_path_helpers(tmp_path: Path) -> None:
    """DataConfig exposes helpers for dataset role paths."""
    cfg = config_models.DataConfig()
    assert cfg.train_path(tmp_path) == tmp_path / cfg.train_bin
    assert cfg.val_path(tmp_path) == tmp_path / cfg.val_bin
    assert cfg.meta_path(tmp_path) == tmp_path / cfg.meta_pkl
