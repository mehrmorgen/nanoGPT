from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, Mapping, TypedDict, cast

import tomllib

from ml_playground.framework.configuration.models import (
    ExperimentConfig,
    PreparerConfig,
    SamplerConfig,
    TrainerConfig,
)
from ml_playground.framework.configuration.merge_utils import merge_mappings
from importlib import import_module
from pydantic import BaseModel

logger = logging.getLogger(__name__)

TomlMapping = dict[str, object]


def _package_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _project_root() -> Path:
    return _package_root().parent


class ExperimentPayload(TypedDict, total=False):
    metadata: TomlMapping
    prepare: TomlMapping
    training: TomlMapping
    sampling: TomlMapping


def get_cfg_path(experiment: str, exp_config: Path | None) -> Path:
    if exp_config:
        return exp_config
    return _package_root() / "experiments" / experiment / "config.toml"


def get_default_config_path(project_root: Path | None = None) -> Path:
    if project_root is None:
        project_root = _project_root()
    return _default_config_path_from_root(project_root)


def list_experiments_with_config(
    prefix: str = "",
    *,
    experiments_root: Path | None = None,
) -> list[str]:
    root = experiments_root or (_package_root() / "experiments")
    if not root.exists():
        return []
    try:
        return sorted(
            [
                p.name
                for p in root.iterdir()
                if p.is_dir()
                and (p / "config.toml").exists()
                and p.name.startswith(prefix)
            ]
        )
    except OSError:
        return []


def _ensure_mapping(value: object, context: str) -> TomlMapping:
    if not isinstance(value, Mapping):
        raise TypeError(f"Expected mapping for {context}")
    mapping_value = cast(Mapping[object, object], value)
    typed_mapping: TomlMapping = {}
    for key, item in mapping_value.items():
        typed_mapping[str(key)] = item
    return typed_mapping


def _validate_extras(experiment_name: str, section: str, data: TomlMapping) -> None:
    extras_registry = import_module(
        "ml_playground.framework.experiment_registry.extras_registry"
    )
    load_fn: object = getattr(extras_registry, "load_extras_models", None)
    get_fn: object = getattr(extras_registry, "get_extras_model", None)
    if callable(load_fn):
        cast(Callable[[str], object], load_fn)(experiment_name)
    model = (
        cast(Callable[[str, str], type[BaseModel] | None], get_fn)(
            experiment_name, section
        )
        if callable(get_fn)
        else None
    )
    extras = data.get("extras", {})
    if extras is None:
        extras = {}
    if not isinstance(extras, Mapping):
        raise TypeError("extras must be a mapping")
    extras_payload = dict(cast(Mapping[str, object], extras))

    def _validate_budget(payload: object) -> dict[str, object]:
        if not isinstance(payload, Mapping):
            raise ValueError("budget extras must be a mapping")
        budget_mapping = cast(Mapping[str, object], payload)
        allowed_keys = {"max_hours", "max_games"}
        unknown = set(budget_mapping) - allowed_keys
        if unknown:
            raise ValueError("budget extras contain unknown keys")
        budget: dict[str, object] = {}
        if "max_hours" in budget_mapping:
            value = budget_mapping["max_hours"]
            if not isinstance(value, (int, float)):
                raise ValueError("budget.max_hours must be a number")
            if value < 0:
                raise ValueError("budget.max_hours must be >= 0")
            budget["max_hours"] = float(value)
        if "max_games" in budget_mapping:
            value = budget_mapping["max_games"]
            if not isinstance(value, int) or isinstance(value, bool):
                raise ValueError("budget.max_games must be an integer")
            if value < 0:
                raise ValueError("budget.max_games must be >= 0")
            budget["max_games"] = value
        return budget

    budget_payload: dict[str, object] | None = None
    if "budget" in extras_payload:
        budget_payload = _validate_budget(extras_payload["budget"])
        extras_payload.pop("budget", None)

    if model is None:
        if extras_payload:
            raise ValueError(
                f"Missing extras model registration for '{experiment_name}.{section}'"
            )
        if budget_payload is not None:
            data["extras"] = {"budget": budget_payload}
        return

    validated = model.model_validate(extras_payload)
    merged: dict[str, object] = dict(validated.model_dump())
    if budget_payload is not None:
        merged["budget"] = budget_payload
    data["extras"] = merged


def read_toml_dict(
    path: Path,
    *,
    toml_loader: Callable[[str], Mapping[str, object]] | None = None,
) -> TomlMapping:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    text = path.read_text(encoding="utf-8")
    try:
        loader = toml_loader if toml_loader is not None else tomllib.loads
        data = loader(text)
    except tomllib.TOMLDecodeError as exc:
        raise Exception(f"{path.name}: {exc}")
    if not isinstance(data, dict):
        raise TypeError(f"TOML root in {path} must be a mapping")
    return data  # type: ignore[assignment]


def _default_config_path_from_root(project_root: Path) -> Path:
    # If we're running from an installed package, _project_root() equals the
    # parent of the package directory. In that case, defaults live inside the
    # package itself.
    if _package_root().parent == project_root:
        return _package_root() / "experiments" / "default_config.toml"

    # Otherwise, compute a path based on common project layouts. Do not check
    # for existence here; callers like tests may pass a synthetic root and
    # assert the constructed path shape.
    if project_root.name == "ml_playground":
        base = project_root
    elif project_root.name == "src":
        base = project_root / "ml_playground"
    else:
        base = project_root / "src" / "ml_playground"
    return base / "experiments" / "default_config.toml"


def _load_and_merge_configs(
    config_path: Path, project_home: Path, experiment_name: str
) -> ExperimentPayload:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    raw_exp: TomlMapping = read_toml_dict(config_path)

    defaults_path = _default_config_path_from_root(project_home)
    defaults_raw: TomlMapping
    if defaults_path.exists():
        defaults_raw = read_toml_dict(defaults_path)
    else:
        defaults_raw = {}

    ldres_config = (
        project_home
        / ".ldres"
        / "etc"
        / "ml_playground"
        / "experiments"
        / experiment_name
        / "config.toml"
    )
    ldres_raw: TomlMapping
    if ldres_config.exists():
        ldres_raw = read_toml_dict(ldres_config)
    else:
        ldres_raw = {}

    merged = merge_mappings(defaults_raw, raw_exp, override_only=True)

    merged_payload = merge_mappings(merged, ldres_raw)
    return cast(ExperimentPayload, merged_payload)


def load_full_experiment_config(
    config_path: Path, project_home: Path, experiment_name: str
) -> ExperimentConfig:
    effective_config = _load_and_merge_configs(
        config_path, project_home, experiment_name
    )

    deprecated_sections = {"shared", "train", "sample"}
    unknown_deprecated = deprecated_sections & set(effective_config.keys())
    if unknown_deprecated:
        raise ValueError(
            "Deprecated config sections are no longer supported: "
            f"{sorted(unknown_deprecated)}"
        )

    for section in ("prepare", "training", "sampling"):
        section_data = effective_config.get(section)
        if isinstance(section_data, Mapping):
            _validate_extras(experiment_name, section, cast(TomlMapping, section_data))

    metadata = _ensure_mapping(
        effective_config.setdefault("metadata", {}), "[metadata]"
    )
    metadata["config_path"] = config_path
    metadata["project_home"] = project_home
    metadata["experiment"] = experiment_name
    effective_config["metadata"] = metadata

    return ExperimentConfig.model_validate(
        effective_config,
        context={"config_path": config_path},
    )


def load_train_config(
    config_path: Path, *, default_config_path: Path | None = None
) -> TrainerConfig:
    raw_exp: TomlMapping = read_toml_dict(config_path)
    project_root = _project_root()
    defaults_path = (
        default_config_path
        if default_config_path is not None
        else _default_config_path_from_root(project_root)
    )
    defaults_raw: TomlMapping
    if defaults_path.exists():
        defaults_raw = read_toml_dict(defaults_path)
    else:
        defaults_raw = {}

    raw_merged = merge_mappings(defaults_raw, raw_exp)

    train_data = _ensure_mapping(raw_merged.get("training", {}), "[training] section")
    _validate_extras(config_path.parent.name, "training", train_data)

    context = {"config_path": config_path}
    cfg = TrainerConfig.model_validate(train_data, context=context)

    info = {"raw": raw_merged, "context": {"config_path": str(config_path)}}
    cfg.extras["provenance"] = info
    return cfg


def load_sample_config(
    config_path: Path, *, default_config_path: Path | None = None
) -> SamplerConfig:
    raw_exp: TomlMapping = read_toml_dict(config_path)
    project_root = _project_root()
    defaults_path = (
        default_config_path
        if default_config_path is not None
        else _default_config_path_from_root(project_root)
    )
    defaults_raw: TomlMapping
    if defaults_path.exists():
        defaults_raw = read_toml_dict(defaults_path)
    else:
        defaults_raw = {}

    raw_merged = merge_mappings(defaults_raw, raw_exp)

    if "sampling" not in raw_exp:
        raise ValueError("Config must contain a [sampling] section")

    sample_data = _ensure_mapping(raw_merged.get("sampling", {}), "[sampling] section")
    _validate_extras(config_path.parent.name, "sampling", sample_data)

    context = {"config_path": config_path}
    cfg = SamplerConfig.model_validate(sample_data, context=context)

    info = {"raw": raw_merged, "context": {"config_path": str(config_path)}}
    cfg.extras["provenance"] = info
    return cfg


def load_prepare_config(
    config_path: Path, *, default_config_path: Path | None = None
) -> PreparerConfig:
    raw_exp: TomlMapping = read_toml_dict(config_path)
    project_root = _project_root()
    defaults_path = (
        default_config_path
        if default_config_path is not None
        else _default_config_path_from_root(project_root)
    )
    defaults_raw: TomlMapping
    if defaults_path.exists():
        defaults_raw = read_toml_dict(defaults_path)
    else:
        defaults_raw = {}

    raw_merged = merge_mappings(defaults_raw, raw_exp)

    if "prepare" not in raw_merged:
        raise ValueError("Config must contain a [prepare] section")

    prepare_data = _ensure_mapping(raw_merged.get("prepare", {}), "[prepare] section")
    _validate_extras(config_path.parent.name, "prepare", prepare_data)

    context = {"config_path": config_path}
    cfg = PreparerConfig.model_validate(prepare_data, context=context)

    info = {"raw": raw_merged, "context": {"config_path": str(config_path)}}
    cfg.extras["provenance"] = info
    return cfg


__all__ = [
    "ExperimentPayload",
    "TomlMapping",
    "get_cfg_path",
    "get_default_config_path",
    "list_experiments_with_config",
    "read_toml_dict",
    "merge_mappings",
    "load_full_experiment_config",
    "load_train_config",
    "load_sample_config",
    "load_prepare_config",
]
