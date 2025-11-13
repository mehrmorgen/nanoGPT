from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping, MutableMapping, Sequence
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

from ml_playground.core.logging_protocol import LoggerLike

if TYPE_CHECKING:  # import for type checking only to avoid runtime cycles
    pass
READ_POLICY_LATEST: Literal["latest"] = "latest"
READ_POLICY_BEST: Literal["best"] = "best"
DEFAULT_READ_POLICY: Literal["best"] = READ_POLICY_BEST

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
    value: Any, base_dir: Path, *, resolve: ResolveFn | None = None
) -> Any:
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


def _clone_config(mapping: Mapping[str, Any]) -> _ConfigDict:
    cloned: _ConfigDict = {}
    for key, value in mapping.items():
        cloned[str(key)] = value
    return cloned


def _coerce_path(value: Any) -> Path | None:
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
    value: Any,
    base_dir: Path,
    *,
    resolve: ResolveFn | None = None,
) -> Any:
    return _resolve_if_relative(value, base_dir, resolve=resolve)


def coerce_path(value: Any) -> Path | None:
    return _coerce_path(value)


SECTION_PREPARE = "prepare"
SECTION_TRAIN = "train"
SECTION_SAMPLE = "sample"
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
                "train.runtime.log_interval must be <= train.runtime.eval_interval"
            )

    @staticmethod
    def trainer(trainer: "TrainerConfig") -> None:
        if trainer.data.block_size > trainer.model.block_size:
            raise ValueError("train.data.block_size must be <= train.model.block_size")

        if (
            trainer.schedule.decay_lr
            and trainer.schedule.min_lr > trainer.optim.learning_rate
        ):
            raise ValueError(
                "train.schedule.min_lr must be <= train.optim.learning_rate when decay_lr=true"
            )

        if not trainer.schedule.decay_lr and trainer.schedule.warmup_iters != 0:
            raise ValueError(
                "train.schedule.warmup_iters must be 0 when decay_lr=false"
            )

    @staticmethod
    def lr_schedule(schedule: "LRSchedule") -> None:
        if schedule.warmup_iters > schedule.lr_decay_iters:
            raise ValueError("warmup_iters must be <= lr_decay_iters")

    @staticmethod
    def data(data: "DataConfig") -> None:
        if data.tokenizer != "tiktoken" and data.ngram_size != 1:
            raise ValueError(
                "train.data.ngram_size must be 1 when tokenizer='tiktoken'"
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
    def _resolve_paths(cls, data: Any, info: ValidationInfo) -> Any:
        if not isinstance(data, Mapping):
            return data
        context = _get_context(info)
        config_path = _coerce_path(context.get("config_path"))
        if config_path is None:
            return cast(Any, data)
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
    eval_only: bool = False
    seed: SeedInt = 1337
    device: DeviceKind = "cpu"
    dtype: DTypeKind = "float32"
    compile: bool = False
    tensorboard_enabled: bool = True
    tensorboard_update_mode: Literal["eval", "log"] = "eval"
    always_save_checkpoint: bool = False
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

    @computed_field(return_type=int)
    def total_eval_steps(self) -> int:
        if self.eval_interval <= 0:
            return 0
        return int(self.max_iters // self.eval_interval)


class TrainerConfig(_FrozenStrictModel):
    @model_validator(mode="before")
    @classmethod
    def _resolve_paths(cls, data: Any, info: ValidationInfo) -> Any:
        if not isinstance(data, Mapping):
            return data
        context = _get_context(info)
        config_path = _coerce_path(context.get("config_path"))
        if config_path is None:
            return cast(Any, data)
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

        @model_validator(mode="before")
        @classmethod
        def _coerce_target_modules(cls, data: Any) -> Any:
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
    def _resolve_paths(cls, data: Any, info: ValidationInfo) -> Any:
        if not isinstance(data, Mapping):
            return data
        context = _get_context(info)
        config_path = _coerce_path(context.get("config_path"))
        if config_path is None:
            return cast(Any, data)
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
    train: TrainerConfig
    sample: SamplerConfig
    shared: "SharedConfig"

    @model_validator(mode="before")
    @classmethod
    def _resolve_paths(cls, data: Any) -> Any:
        if not isinstance(data, Mapping):
            return data

        typed_data = cast(_ReadOnlyConfig, data)

        shared_obj = typed_data.get("shared")
        shared_mapping: _ReadOnlyConfig | None = None
        config_path: Path | None = None
        if isinstance(shared_obj, Mapping):
            shared_mapping = cast(_ReadOnlyConfig, shared_obj)
            config_path = _coerce_path(shared_mapping.get("config_path"))
        elif hasattr(shared_obj, "config_path"):
            config_path = _coerce_path(getattr(shared_obj, "config_path", None))

        if config_path is None:
            # Preserve original mapping identity when no config_path is provided
            return cast(Any, data)

        resolved_config_path = _resolve_path_strict(config_path)
        base_dir = resolved_config_path.parent

        mutable_data: _ConfigDict = _clone_config(typed_data)

        shared_data: _ConfigDict | None = None
        if shared_mapping is not None:
            shared_data = _clone_config(shared_mapping)
            shared_data["config_path"] = resolved_config_path
            mutable_data["shared"] = shared_data

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
            if shared_data is not None:
                out_dir_path = _coerce_path(runtime_data.get("out_dir"))
                if out_dir_path is not None:
                    shared_data[shared_key] = out_dir_path

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
            if dataset_dir_path is not None and shared_data is not None:
                shared_data["dataset_dir"] = _resolve_if_relative(
                    dataset_dir_path, base_dir
                )

        if shared_data is not None:
            for key in (
                "project_home",
                "dataset_dir",
                "train_out_dir",
                "sample_out_dir",
            ):
                value = shared_data.get(key)
                path_value = _coerce_path(value)
                if path_value is not None:
                    shared_data[key] = _resolve_if_relative(path_value, base_dir)

        return mutable_data


class SharedConfig(_FrozenStrictModel):
    experiment: str
    config_path: Path
    project_home: Path
    dataset_dir: Path
    train_out_dir: Path
    sample_out_dir: Path

    @model_validator(mode="before")
    @classmethod
    def _resolve_shared_paths(cls, data: Any) -> Any:
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
    "SharedConfig",
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
SharedConfig.model_rebuild()
