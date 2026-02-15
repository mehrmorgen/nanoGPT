from __future__ import annotations

import logging
import math
import tomllib
from collections.abc import Iterable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Callable, Literal, Optional, TYPE_CHECKING, cast
import typing as _t

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    AfterValidator,
    NonNegativeInt,
    PositiveInt,
    ValidationInfo,
    computed_field,
    field_validator,
    model_validator,
)

from ml_playground.framework.core.logging_protocol import LoggerLike

if TYPE_CHECKING:  # import for type checking only to avoid runtime cycles
    pass
READ_POLICY_LATEST: Literal["latest"] = "latest"
READ_POLICY_BEST: Literal["best"] = "best"


def _load_default_read_policy_from_toml() -> Literal["latest", "best"]:
    config_path = (
        Path(__file__).resolve().parents[2] / "experiments" / "default_config.toml"
    )
    data = tomllib.loads(config_path.read_text(encoding="utf-8"))
    try:
        read_policy = data["training"]["runtime"]["checkpointing"]["read_policy"]
    except KeyError as e:
        raise ValueError(
            f"Could not find default read_policy in {config_path}. Missing key: {e}"
        ) from e
    if read_policy == READ_POLICY_LATEST:
        return READ_POLICY_LATEST
    if read_policy == READ_POLICY_BEST:
        return READ_POLICY_BEST
    raise ValueError(
        f"Unsupported default read_policy in {config_path}: {read_policy!r}"
    )


DEFAULT_READ_POLICY: Literal["latest", "best"] = _load_default_read_policy_from_toml()

# ----- DI type aliases (kept generic to avoid import cycles) -----
# Read raw text from a filesystem path
ReadTextFn = _t.Callable[[Path], str]
# Construct a tokenizer object from a generic discriminator (e.g., kind/enum)
TokenizerFactoryFn = _t.Callable[[object], Any]
# Trainer hooks around a single training step
BeforeStepHook = _t.Callable[..., None]
AfterStepHook = _t.Callable[..., None]
# Checkpoint IO indirections
CheckpointLoadFn = _t.Callable[..., Any]
CheckpointSaveFn = _t.Callable[..., None]
# Sampler model factory (uses local ModelConfig without importing runtime symbols)
ModelFactoryFn = _t.Callable[[Any, object], Any]
# Optional model compile indirection for sampling
CompileModelFn = _t.Callable[[Any], Any]


ResolveFn = Callable[[Path], Path]


def _resolve_if_relative(
    value: object, base_dir: Path, *, resolve: ResolveFn | None = None
) -> Path | str | object:
    resolver = resolve or Path.resolve
    candidate = _coerce_path(value)
    if candidate is None:
        return value

    if candidate.is_absolute():
        return resolver(candidate)
    return resolver(base_dir / candidate)


def _resolve_path_strict(value: Path, *, resolve: ResolveFn | None = None) -> Path:
    """Resolve a path, raising ValueError on resolution errors.

    Used by tests to validate error handling of strict resolution.
    """
    resolver = resolve or Path.resolve
    try:
        return resolver(value)
    except OSError as e:
        raise ValueError(f"Invalid path: {value}") from e


def _resolve_fields_relative(
    data: _MutableConfig,
    keys: Iterable[str],
    base_dir: Path,
    *,
    resolve: ResolveFn | None = None,
) -> None:
    for key in keys:
        if key in data:
            data[key] = _resolve_if_relative(data[key], base_dir, resolve=resolve)


def _clone_config(mapping: Mapping[str, object]) -> _ConfigDict:
    cloned: _ConfigDict = {}
    for key, value in mapping.items():
        cloned[str(key)] = value
    return cloned


def _coerce_path(value: object) -> Path | None:
    if isinstance(value, Path):
        return value
    if isinstance(value, str):
        try:
            return Path(value)
        except (TypeError, ValueError, OSError):
            return None
    return None


def _get_context(info: ValidationInfo) -> Mapping[str, object]:
    if info.context is None:
        return cast(Mapping[str, object], {})
    return cast(Mapping[str, object], info.context)


def _get_resolve_fn(info: ValidationInfo | None) -> ResolveFn | None:
    if info is None:
        return None
    context = _get_context(info)
    for key in ("resolve_path", "resolve_fn"):
        candidate = context.get(key)
        if callable(candidate):
            return cast(ResolveFn, candidate)
    return None


def resolve_if_relative(
    value: object,
    base_dir: Path,
    *,
    resolve: ResolveFn | None = None,
) -> Path | str | object:
    return _resolve_if_relative(value, base_dir, resolve=resolve)


def coerce_path(value: object) -> Path | None:
    return _coerce_path(value)


def _resolve_mlflow_tracking_uri(uri: str | None, base_dir: Path) -> str | None:  # pyright: ignore[reportUnusedFunction]
    """Normalize sqlite URIs so they resolve relative to the experiment config.

    Args:
        uri: The MLflow tracking URI to normalize.
        base_dir: The directory to resolve relative paths against.
    """
    if uri is None:
        return None
    if uri.startswith("sqlite:////"):
        return uri
    prefix = "sqlite:///"
    if uri.startswith(prefix) and len(uri) > len(prefix):
        relative = uri[len(prefix) :]
        if relative.startswith("/"):
            return uri
        resolved = (base_dir / relative).resolve()
        return f"{prefix}{resolved}"
    return uri


SECTION_PREPARE = "prepare"
SECTION_TRAIN = "training"
SECTION_SAMPLE = "sampling"
SECTION_METADATA = "metadata"
KEY_EXTRAS = "extras"

DeviceKind = Literal["cpu", "mps", "cuda"]
DTypeKind = Literal["float32", "bfloat16", "float16"]

_ConfigDict = dict[str, object]
_MutableConfig = MutableMapping[str, object]
_ReadOnlyConfig = Mapping[str, object]


class _ConfigCrossFieldValidator:
    """Centralized cross-field validation helpers for configuration models."""

    @staticmethod
    def runtime(runtime: "RuntimeConfig") -> None:
        if runtime.log_interval > runtime.eval_interval:
            raise ValueError(
                "training.runtime.log_interval must be <= training.runtime.eval_interval"
            )

    @staticmethod
    def trainer(trainer: "TrainerConfig") -> None:
        if trainer.data.block_size > trainer.model.block_size:
            raise ValueError(
                "training.data.block_size must be <= training.model.block_size"
            )

        if (
            trainer.schedule.decay_lr
            and trainer.schedule.min_lr > trainer.optim.learning_rate
        ):
            raise ValueError(
                "training.schedule.min_lr must be <= training.optim.learning_rate when decay_lr=true"
            )

        if not trainer.schedule.decay_lr and trainer.schedule.warmup_iters != 0:
            raise ValueError(
                "training.schedule.warmup_iters must be 0 when decay_lr=false"
            )

    @staticmethod
    def lr_schedule(schedule: "LRSchedule") -> None:
        if schedule.warmup_iters > schedule.lr_decay_iters:
            raise ValueError("warmup_iters must be <= lr_decay_iters")

    @staticmethod
    def data(data: "DataConfig") -> None:
        if data.tokenizer != "tiktoken" and data.ngram_size != 1:
            raise ValueError(
                "training.data.ngram_size must be 1 when tokenizer='tiktoken'"
            )


class ConfigCrossFieldValidator:
    """Public façade over internal configuration validators."""

    runtime: Callable[[Any], None] = staticmethod(_ConfigCrossFieldValidator.runtime)
    trainer: Callable[[Any], None] = staticmethod(_ConfigCrossFieldValidator.trainer)
    lr_schedule: Callable[[Any], None] = staticmethod(
        _ConfigCrossFieldValidator.lr_schedule
    )
    data: Callable[[Any], None] = staticmethod(_ConfigCrossFieldValidator.data)


class _FrozenStrictModel(BaseModel):
    """Base model that is immutable, strict, and forbids extra fields."""

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_default=True,
        strict=True,
        arbitrary_types_allowed=True,
    )

    logger: LoggerLike = Field(default_factory=lambda: logging.getLogger(__name__))

    @model_validator(mode="before")
    @classmethod
    def _coerce_logger(cls, data: object) -> object:
        if not isinstance(data, Mapping):
            return data
        logger_value = data.get("logger")
        if logger_value is None:
            data = dict(cast(Mapping[str, object], data))
            data["logger"] = logging.getLogger(__name__)
        return data


def _no_nan(v: float) -> float:
    if v != v:  # NaN check
        raise ValueError("must not be NaN")
    return v


NonNaNNonNegativeStrictFloat = Annotated[float, AfterValidator(_no_nan), Field(ge=0)]
UnitIntervalStrictFloat = Annotated[float, Field(ge=0, le=1)]
PosUnitIntervalStrictFloat = Annotated[float, Field(gt=0, le=1)]
PositiveStrictFloat = Annotated[float, Field(gt=0)]

NonNegativeStrictInt = Annotated[int, Field(ge=0)]
PositiveStrictInt = Annotated[int, Field(gt=0)]
AtLeastOneInt = Annotated[int, Field(ge=1)]
SeedInt = Annotated[int, Field(ge=0)]
MinutesNonNegative = Annotated[int, Field(ge=0)]
EpochCount = AtLeastOneInt


@dataclass(frozen=True)
class BinRefreshPolicy:
    min_new_tokens: NonNegativeStrictInt = 0
    min_new_ratio: UnitIntervalStrictFloat = 0.0


def derive_pool_size(
    target_labeled_positions: int,
    avg_positions_per_game: int,
    *,
    oversample_factor: float = 1.0,
) -> int:
    if target_labeled_positions < 0:
        raise ValueError("target_labeled_positions must be >= 0")
    if avg_positions_per_game <= 0:
        raise ValueError("avg_positions_per_game must be > 0")
    if oversample_factor <= 0:
        raise ValueError("oversample_factor must be > 0")
    if target_labeled_positions == 0:
        return 0
    return int(
        math.ceil(
            (target_labeled_positions / avg_positions_per_game) * oversample_factor
        )
    )


@dataclass(frozen=True)
class PoolSizePolicy:
    target_labeled_positions: int = 0
    avg_positions_per_game: int = 1
    oversample_factor: float = 1.0

    @property
    def pool_size(self) -> int:
        return derive_pool_size(
            self.target_labeled_positions,
            self.avg_positions_per_game,
            oversample_factor=self.oversample_factor,
        )


class PreparerConfig(_FrozenStrictModel):
    raw_dir: Path = Path("./raw")
    raw_text_path: Path | None = None
    tokenizer_type: Literal["char", "word", "tiktoken"] = "char"
    add_structure_tokens: bool = False
    doc_separator: str = ""
    extras: dict[str, Any] = Field(default_factory=dict)
    # Optional DI hooks (keep generic to avoid import cycles)
    # Function to read text from a path (e.g., Path -> str)
    read_text_fn: Optional[ReadTextFn] = None
    # Factory to create a tokenizer from a kind
    tokenizer_factory: Optional[TokenizerFactoryFn] = None

    @model_validator(mode="before")
    @classmethod
    def _resolve_paths(cls, data: object, info: ValidationInfo) -> _ConfigDict | object:
        if not isinstance(data, Mapping):
            return data
        context = _get_context(info)
        config_path = _coerce_path(context.get("config_path"))
        if config_path is None:
            return cast(object, data)
        base_dir = config_path.parent
        resolve = _get_resolve_fn(info)
        typed_data = cast(_ReadOnlyConfig, data)
        mutable_data: _ConfigDict = _clone_config(typed_data)
        _resolve_fields_relative(
            mutable_data,
            ["raw_dir", "raw_text_path"],
            base_dir,
            resolve=resolve,
        )
        return mutable_data


class RuntimeConfig(_FrozenStrictModel):
    out_dir: Path
    max_iters: NonNegativeStrictInt = 600_000
    eval_interval: AtLeastOneInt = 2_000
    eval_iters: AtLeastOneInt = 200
    log_interval: AtLeastOneInt = 1
    max_games: NonNegativeStrictInt = 0
    eval_interval_games: NonNegativeStrictInt = 0
    eval_games: NonNegativeStrictInt = 0
    log_interval_games: NonNegativeStrictInt = 0
    eval_only: bool = False
    seed: SeedInt = 1337
    device: DeviceKind = "cpu"
    dtype: DTypeKind = "float32"
    compile: bool = False
    tensorboard_enabled: bool = True
    tensorboard_update_mode: Literal["eval", "log"] = "eval"
    always_save_checkpoint: bool = False
    mlflow_enabled: bool = False
    mlflow_tracking_uri: str | None = None
    mlflow_experiment_name: str | None = None
    mlflow_run_name: str | None = None
    mlflow_log_system_metrics: bool = False
    iters_per_epoch: Optional[EpochCount] = None
    max_epochs: Optional[EpochCount] = None
    ckpt_metric: Literal["val_loss", "perplexity"] = "val_loss"
    ckpt_greater_is_better: bool = False
    ckpt_atomic: bool = True
    ckpt_write_metadata: bool = True
    ckpt_last_filename: str = "ckpt_last.pt"
    ckpt_best_filename: str = "ckpt_best.pt"
    ckpt_top_k: NonNegativeStrictInt = 0
    ckpt_time_interval_minutes: MinutesNonNegative = 0
    initial_best_val_loss: NonNaNNonNegativeStrictFloat = 1e9
    ckpt_naming_policy: Literal["standard", "domain"] = "standard"
    ckpt_domain_label: str | None = None

    class Checkpointing(_FrozenStrictModel):
        class Keep(_FrozenStrictModel):
            last: NonNegativeInt = 1
            best: NonNegativeInt = 1

        keep: Keep = Keep()
        read_policy: Literal["latest", "best"] = DEFAULT_READ_POLICY

    checkpointing: Checkpointing = Checkpointing()
    best_smoothing_alpha: UnitIntervalStrictFloat = 1.0
    early_stop_patience: NonNegativeStrictInt = 0
    ema_decay: UnitIntervalStrictFloat = 0.0

    @field_validator("out_dir", mode="after")
    @classmethod
    def _resolve_out_dir(cls, v: Path) -> Path:
        return v

    @model_validator(mode="after")
    def _check_logging_intervals(self) -> "RuntimeConfig":
        _ConfigCrossFieldValidator.runtime(self)
        return self

    @model_validator(mode="after")
    def _clear_mlflow_when_disabled(self) -> "RuntimeConfig":
        if not self.mlflow_enabled:
            object.__setattr__(self, "mlflow_tracking_uri", None)
        return self

    @model_validator(mode="after")
    def _validate_checkpoint_naming(self) -> "RuntimeConfig":
        if self.ckpt_naming_policy == "domain":
            if not self.ckpt_domain_label:
                raise ValueError(
                    "ckpt_domain_label must be set when ckpt_naming_policy='domain'"
                )
            label = self.ckpt_domain_label
            allowed = "abcdefghijklmnopqrstuvwxyz0123456789_"
            if not all(ch in allowed for ch in label):
                raise ValueError(
                    "ckpt_domain_label must match [a-z0-9_]+ (lowercase ASCII letters, digits, or underscores)"
                )
        return self

    @model_validator(mode="after")
    def _validate_domain_counters(self) -> "RuntimeConfig":
        if self.eval_interval_games and self.log_interval_games:
            if self.log_interval_games > self.eval_interval_games:
                raise ValueError(
                    "runtime.log_interval_games must be <= runtime.eval_interval_games"
                )
        return self

    @model_validator(mode="after")
    def _validate_mps_constraints(self) -> "RuntimeConfig":
        if self.device == "mps":
            if self.compile:
                raise ValueError("runtime.compile must be false when device is mps")
            if self.dtype == "float16":
                raise ValueError("runtime.dtype float16 is not supported on mps")
        return self

    @model_validator(mode="before")
    @classmethod
    def _strip_computed_fields(cls, data: object) -> object:
        if not isinstance(data, Mapping):
            return data
        mutable = dict(cast(Mapping[str, object], data))
        # Drop computed fields that may be present from model_dump
        mutable.pop("total_eval_steps", None)
        mutable.pop("total_eval_games", None)
        return mutable

    @computed_field(return_type=int)
    def total_eval_steps(self) -> int:
        if self.eval_interval <= 0:
            return 0
        return int(self.max_iters // self.eval_interval)

    @computed_field(return_type=int)
    def total_eval_games(self) -> int:
        if self.eval_interval_games <= 0:
            return 0
        return int(self.max_games // self.eval_interval_games)


class ExperienceStorageConfig(_FrozenStrictModel):
    strategy: Literal["memory", "json_file"] = "memory"
    path: Path | str | None = None
    flush_on_store: bool = False
    extras: dict[str, Any] = Field(default_factory=dict)
    logger: LoggerLike = Field(default_factory=lambda: logging.getLogger(__name__))

    @model_validator(mode="before")
    @classmethod
    def _resolve_paths(cls, data: object, info: ValidationInfo) -> _ConfigDict | object:
        if not isinstance(data, Mapping):
            return data
        context = _get_context(info)
        config_path = _coerce_path(context.get("config_path"))
        if config_path is None:
            return cast(object, data)
        base_dir = config_path.parent
        mutable_data: _ConfigDict = _clone_config(cast(_ReadOnlyConfig, data))
        if "path" in mutable_data and mutable_data["path"] is not None:
            resolved = _resolve_if_relative(mutable_data["path"], base_dir)
            coerced = _coerce_path(resolved)
            mutable_data["path"] = coerced if coerced is not None else resolved
        return mutable_data

    @model_validator(mode="after")
    def _validate_json_file(self) -> "ExperienceStorageConfig":
        if self.strategy == "json_file":
            if self.path is None:
                raise ValueError(
                    "experience storage path is required for strategy json_file"
                )
            if Path(self.path).is_dir():
                raise ValueError("experience storage path must point to a file")
        return self


class TrainerConfig(_FrozenStrictModel):
    @model_validator(mode="before")
    @classmethod
    def _resolve_paths(cls, data: object, info: ValidationInfo) -> _ConfigDict | object:
        if not isinstance(data, Mapping):
            return data
        context = _get_context(info)
        config_path = _coerce_path(context.get("config_path"))
        if config_path is None:
            return cast(_ConfigDict, data)
        base_dir = config_path.parent
        resolve = _get_resolve_fn(info)
        typed_data = cast(_ReadOnlyConfig, data)
        mutable_data: _ConfigDict = _clone_config(typed_data)
        runtime_obj = typed_data.get("runtime")
        if isinstance(runtime_obj, Mapping):
            runtime_data = _clone_config(cast(_ReadOnlyConfig, runtime_obj))
            _resolve_fields_relative(
                runtime_data, ["out_dir"], base_dir, resolve=resolve
            )
            if "mlflow_tracking_uri" in runtime_data:
                runtime_data["mlflow_tracking_uri"] = _resolve_mlflow_tracking_uri(
                    cast(str | None, runtime_data.get("mlflow_tracking_uri")), base_dir
                )
            mutable_data["runtime"] = runtime_data
        return mutable_data

    class HFModelConfig(_FrozenStrictModel):
        model_name: str
        gradient_checkpointing: bool = False
        block_size: AtLeastOneInt = 1024

    class PeftConfig(_FrozenStrictModel):
        enabled: bool = False
        r: PositiveStrictInt = 8
        lora_alpha: PositiveStrictFloat = 16.0
        lora_dropout: UnitIntervalStrictFloat = 0.0
        bias: Literal["none", "all", "lora_only"] = "none"
        target_modules: tuple[str, ...] = ()
        extend_mlp_targets: bool = False

        class AdapterOutputPolicy(_FrozenStrictModel):
            """Naming policy for adapter output artifacts."""

            base_dir: str = "adapters"
            best_name: str = "best"
            last_name: str = "last"
            final_name: str = "final"

            @model_validator(mode="after")
            def _validate_names(self) -> "TrainerConfig.PeftConfig.AdapterOutputPolicy":
                names = [self.best_name, self.last_name, self.final_name]
                if any(name == "" for name in names):
                    raise ValueError("adapter output names must be non-empty")
                if len(set(names)) != len(names):
                    raise ValueError("adapter output names must be unique")
                if not self.base_dir:
                    raise ValueError("adapter output base_dir must be non-empty")
                return self

            def resolve(self, out_dir: Path) -> dict[str, Path]:
                base = out_dir / self.base_dir
                return {
                    "best": base / self.best_name,
                    "last": base / self.last_name,
                    "final": base / self.final_name,
                }

        @model_validator(mode="before")
        @classmethod
        def _coerce_target_modules(cls, data: object) -> object:
            if not isinstance(data, Mapping):
                return data
            data_dict: dict[str, object] = cast(dict[str, object], data)
            modules_obj = data_dict.get("target_modules")
            if isinstance(modules_obj, set):
                modules_iter = cast(Iterable[object], modules_obj)
                data_dict["target_modules"] = tuple(
                    str(module) for module in modules_iter
                )
                return data_dict
            if isinstance(modules_obj, Sequence) and not isinstance(
                modules_obj, (str, bytes)
            ):
                modules_seq = cast(Sequence[object], modules_obj)
                data_dict["target_modules"] = tuple(
                    str(module) for module in modules_seq
                )
            return data_dict

    model: "ModelConfig"
    data: "DataConfig"
    optim: "OptimConfig"
    schedule: "LRSchedule"
    runtime: RuntimeConfig
    extras: dict[str, Any] = Field(default_factory=dict)
    hf_model: HFModelConfig | None = None
    peft: PeftConfig | None = None
    checkpointing: RuntimeConfig.Checkpointing = RuntimeConfig.Checkpointing()
    # Optional DI callables (kept generic to avoid import cycles)
    # Hooks around a training step
    before_step_hook: Optional[BeforeStepHook] = None
    after_step_hook: Optional[AfterStepHook] = None
    # Checkpoint save/load indirections
    checkpoint_save_fn: Optional[CheckpointSaveFn] = None
    checkpoint_load_fn: Optional[CheckpointLoadFn] = None

    @model_validator(mode="after")
    def _cross_field_checks(self) -> "TrainerConfig":
        _ConfigCrossFieldValidator.trainer(self)
        return self


class SamplerConfig(_FrozenStrictModel):
    @model_validator(mode="before")
    @classmethod
    def _resolve_paths(cls, data: object, info: ValidationInfo) -> _ConfigDict | object:
        if not isinstance(data, Mapping):
            return data
        context = _get_context(info)
        config_path = _coerce_path(context.get("config_path"))
        if config_path is None:
            return cast(_ConfigDict, data)
        base_dir = config_path.parent
        resolve = _get_resolve_fn(info)
        typed_data = cast(_ReadOnlyConfig, data)
        mutable_data: _ConfigDict = _clone_config(typed_data)
        runtime_obj = typed_data.get("runtime")
        if isinstance(runtime_obj, Mapping):
            runtime_data = _clone_config(cast(_ReadOnlyConfig, runtime_obj))
            _resolve_fields_relative(
                runtime_data, ["out_dir"], base_dir, resolve=resolve
            )
            if "mlflow_tracking_uri" in runtime_data:
                runtime_data["mlflow_tracking_uri"] = _resolve_mlflow_tracking_uri(
                    cast(str | None, runtime_data.get("mlflow_tracking_uri")), base_dir
                )
            mutable_data["runtime"] = runtime_data
        return mutable_data

    runtime: RuntimeConfig
    sample: "SampleConfig"
    extras: dict[str, Any] = Field(default_factory=dict)
    # Optional DI callables for sampling
    checkpoint_load_fn: Optional[CheckpointLoadFn] = None
    model_factory: Optional[ModelFactoryFn] = None
    # Optional compile hook; if provided and runtime.compile=True, this will be used
    # to compile/wrap the model. Defaults to torch.compile when available.
    compile_model_fn: Optional[CompileModelFn] = None


class OptimConfig(_FrozenStrictModel):
    learning_rate: NonNaNNonNegativeStrictFloat = 6e-4
    weight_decay: NonNaNNonNegativeStrictFloat = 1e-1
    beta1: NonNaNNonNegativeStrictFloat = 0.9
    beta2: NonNaNNonNegativeStrictFloat = 0.95
    grad_clip: NonNaNNonNegativeStrictFloat = 1.0


class LRSchedule(_FrozenStrictModel):
    decay_lr: bool = True
    warmup_iters: NonNegativeStrictInt = 2000
    lr_decay_iters: NonNegativeStrictInt = 600_000
    min_lr: NonNaNNonNegativeStrictFloat = 6e-5

    @model_validator(mode="after")
    def _check_warmup_le_decay(self) -> "LRSchedule":
        _ConfigCrossFieldValidator.lr_schedule(self)
        return self


class ModelConfig(_FrozenStrictModel):
    n_layer: PositiveStrictInt = 12
    n_head: PositiveStrictInt = 12
    n_embd: PositiveStrictInt = 767
    block_size: AtLeastOneInt = 1024
    dropout: UnitIntervalStrictFloat = 0.0
    bias: bool = True
    vocab_size: Optional[PositiveInt] = None
    init_std: NonNaNNonNegativeStrictFloat = 0.02
    init_std_c_proj_scale: NonNaNNonNegativeStrictFloat = 2.0


class DataConfig(_FrozenStrictModel):
    train_bin: str = "train.bin"
    val_bin: str = "val.bin"
    meta_pkl: str = "meta.pkl"
    batch_size: AtLeastOneInt = 12
    block_size: AtLeastOneInt = 1024
    grad_accum_steps: AtLeastOneInt = 40
    tokenizer: Literal["char", "word", "tiktoken"] = "char"
    ngram_size: PositiveInt = 1
    sampler: Literal["random", "sequential"] = "random"

    @model_validator(mode="after")
    def _check_tokenizer_compat(self) -> "DataConfig":
        _ConfigCrossFieldValidator.data(self)
        return self

    def train_path(self, dataset_dir: Path) -> Path:
        return dataset_dir / self.train_bin

    def val_path(self, dataset_dir: Path) -> Path:
        return dataset_dir / self.val_bin

    def meta_path(self, dataset_dir: Path) -> Path:
        return dataset_dir / self.meta_pkl


class SampleConfig(_FrozenStrictModel):
    start: str = "\n"
    num_samples: AtLeastOneInt = 3
    max_new_tokens: AtLeastOneInt = 200
    temperature: PositiveStrictFloat = 0.8
    top_k: NonNegativeStrictInt = 200
    top_p: Optional[PosUnitIntervalStrictFloat] = None


class ExperimentConfig(_FrozenStrictModel):
    prepare: PreparerConfig
    training: TrainerConfig
    sampling: SamplerConfig
    metadata: "MetadataConfig"

    @model_validator(mode="before")
    @classmethod
    def _resolve_paths(cls, data: object) -> _ConfigDict | object:
        if not isinstance(data, Mapping):
            return data

        typed_data = cast(_ReadOnlyConfig, data)

        metadata_obj = typed_data.get("metadata")
        metadata_mapping: _ReadOnlyConfig | None = None
        config_path: Path | None = None
        if isinstance(metadata_obj, Mapping):
            metadata_mapping = cast(_ReadOnlyConfig, metadata_obj)
            config_path = _coerce_path(metadata_mapping.get("config_path"))
        elif hasattr(metadata_obj, "config_path"):
            config_path = _coerce_path(getattr(metadata_obj, "config_path", None))

        if config_path is None:
            # Preserve original mapping identity when no config_path is provided
            return cast(object, data)

        resolved_config_path = _resolve_path_strict(config_path)
        base_dir = resolved_config_path.parent

        mutable_data: _ConfigDict = _clone_config(typed_data)

        metadata_data: _ConfigDict | None = None
        if metadata_mapping is not None:
            metadata_data = _clone_config(metadata_mapping)
            metadata_data["config_path"] = resolved_config_path
            mutable_data["metadata"] = metadata_data

        def _normalize_runtime(section: str, shared_key: str) -> None:
            section_obj = typed_data.get(section)
            if not isinstance(section_obj, Mapping):
                return
            section_data = _clone_config(cast(_ReadOnlyConfig, section_obj))
            mutable_data[section] = section_data
            runtime_obj = section_data.get("runtime")
            if not isinstance(runtime_obj, Mapping):
                return
            runtime_data = _clone_config(cast(_ReadOnlyConfig, runtime_obj))
            _resolve_fields_relative(runtime_data, ["out_dir"], base_dir)
            section_data["runtime"] = runtime_data
            if metadata_data is not None:
                out_dir_path = _coerce_path(runtime_data.get("out_dir"))
                if out_dir_path is not None:
                    metadata_data[shared_key] = out_dir_path

        _normalize_runtime(SECTION_TRAIN, "train_out_dir")
        _normalize_runtime(SECTION_SAMPLE, "sample_out_dir")

        prepare_obj = typed_data.get(SECTION_PREPARE)
        if isinstance(prepare_obj, Mapping):
            prepare_data = _clone_config(cast(_ReadOnlyConfig, prepare_obj))
            mutable_data[SECTION_PREPARE] = prepare_data
            _resolve_fields_relative(
                prepare_data,
                ["raw_dir", "raw_text_path"],
                base_dir,
            )
            dataset_dir_raw = prepare_data.pop("dataset_dir", None)
            dataset_dir_path = _coerce_path(dataset_dir_raw)
            if dataset_dir_path is not None and metadata_data is not None:
                metadata_data["dataset_dir"] = _resolve_if_relative(
                    dataset_dir_path, base_dir
                )

        if metadata_data is not None:
            for key in (
                "project_home",
                "dataset_dir",
                "train_out_dir",
                "sample_out_dir",
            ):
                value = metadata_data.get(key)
                path_value = _coerce_path(value)
                if path_value is not None:
                    metadata_data[key] = _resolve_if_relative(path_value, base_dir)

        return mutable_data


class MetadataConfig(_FrozenStrictModel):
    experiment: str
    config_path: Path
    project_home: Path
    dataset_dir: Path
    train_out_dir: Path
    sample_out_dir: Path

    @model_validator(mode="before")
    @classmethod
    def _resolve_metadata_paths(cls, data: object) -> _ConfigDict | object:
        if not isinstance(data, Mapping):
            return data
        typed_data = cast(_ReadOnlyConfig, data)
        mutable_data: _ConfigDict = _clone_config(typed_data)

        cfg_path_value = typed_data.get("config_path")
        cfg_path = _coerce_path(cfg_path_value)
        if cfg_path is None:
            return mutable_data

        resolved_cfg_path = _resolve_path_strict(cfg_path)
        base_dir = resolved_cfg_path.parent
        mutable_data["config_path"] = resolved_cfg_path

        for key in ("project_home", "dataset_dir", "train_out_dir", "sample_out_dir"):
            value = mutable_data.get(key)
            path_value = _coerce_path(value)
            if path_value is not None:
                mutable_data[key] = _resolve_if_relative(path_value, base_dir)
        return mutable_data

    @field_validator(
        "config_path",
        "project_home",
        "dataset_dir",
        "train_out_dir",
        "sample_out_dir",
        mode="after",
    )
    @classmethod
    def _as_is(cls, v: Path) -> Path:
        return v


__all__ = [
    "READ_POLICY_LATEST",
    "READ_POLICY_BEST",
    "DEFAULT_READ_POLICY",
    # DI type aliases
    "ReadTextFn",
    "TokenizerFactoryFn",
    "BeforeStepHook",
    "AfterStepHook",
    "CheckpointLoadFn",
    "CheckpointSaveFn",
    "ModelFactoryFn",
    "CompileModelFn",
    "resolve_if_relative",
    "coerce_path",
    "ConfigCrossFieldValidator",
    "SECTION_PREPARE",
    "SECTION_TRAIN",
    "SECTION_SAMPLE",
    "SECTION_METADATA",
    "KEY_EXTRAS",
    "DeviceKind",
    "DTypeKind",
    "PreparerConfig",
    "RuntimeConfig",
    "TrainerConfig",
    "SamplerConfig",
    "OptimConfig",
    "LRSchedule",
    "ModelConfig",
    "DataConfig",
    "SampleConfig",
    "ExperimentConfig",
    "MetadataConfig",
]

# Ensure forward references across configuration models are resolved at import time
# This is safe/idempotent and avoids requiring callers to rebuild explicitly.
PreparerConfig.model_rebuild()
RuntimeConfig.model_rebuild()
TrainerConfig.model_rebuild()
SamplerConfig.model_rebuild()
ModelConfig.model_rebuild()
DataConfig.model_rebuild()
SampleConfig.model_rebuild()
ExperimentConfig.model_rebuild()
MetadataConfig.model_rebuild()
