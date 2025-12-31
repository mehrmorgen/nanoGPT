from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, TypedDict, cast

import tomllib

from ml_playground.configuration.models import (
    ExperimentConfig,
    PreparerConfig,
    SamplerConfig,
    TrainerConfig,
)
from ml_playground.configuration.merge_utils import merge_mappings
from ml_playground.experiments.extras_registry import (
    get_extras_model,
    load_extras_models,
)

logger = logging.getLogger(__name__)

TomlMapping = Dict[str, Any]


def _package_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _project_root() -> Path:
    return _package_root().parent


class ExperimentPayload(TypedDict, total=False):
    shared: TomlMapping
    prepare: TomlMapping
    train: TomlMapping
    sample: TomlMapping


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


def _ensure_mapping(value: Any, context: str) -> TomlMapping:
    if not isinstance(value, Mapping):
        raise TypeError(f"Expected mapping for {context}")
    return dict(value)


def _validate_budget(extras: Mapping[str, Any]) -> dict[str, Any] | None:
    if "budget" not in extras:
        return None
    budget = extras.get("budget")
    if budget is None:
        return None
    if not isinstance(budget, Mapping):
        raise ValueError("extras.budget must be a mapping")

    allowed_keys = {"max_hours", "max_games"}
    unknown_keys = set(budget) - allowed_keys
    if unknown_keys:
        raise ValueError(f"extras.budget has unknown keys: {sorted(unknown_keys)}")

    cleaned: dict[str, Any] = {}
    if "max_hours" in budget:
        value = budget["max_hours"]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("extras.budget.max_hours must be a number")
        if value < 0:
            raise ValueError("extras.budget.max_hours must be >= 0")
        cleaned["max_hours"] = float(value)
    if "max_games" in budget:
        value = budget["max_games"]
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("extras.budget.max_games must be an integer")
        if value < 0:
            raise ValueError("extras.budget.max_games must be >= 0")
        cleaned["max_games"] = value
    return cleaned


def _validate_extras(experiment_name: str, section: str, data: TomlMapping) -> None:
    load_extras_models(experiment_name)
    model = get_extras_model(experiment_name, section)
    extras = data.get("extras", {})
    if extras is None:
        extras = {}
    if not isinstance(extras, Mapping):
        raise TypeError("extras must be a mapping")
    extras_payload = dict(extras)
    budget = _validate_budget(extras_payload)
    extras_payload.pop("budget", None)
    if model is None:
        if extras_payload:
            raise ValueError(
                f"Missing extras model registration for '{experiment_name}.{section}'"
            )
        if budget is not None:
            data["extras"] = {"budget": budget}
        return
    validated = model.model_validate(extras_payload)
    merged = validated.model_dump()
    if budget is not None:
        merged["budget"] = budget
    data["extras"] = merged


def read_toml_dict(
    path: Path,
    *,
    toml_loader: Callable[[str], Mapping[str, Any]] | None = None,
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

    raw_exp = read_toml_dict(config_path)

    defaults_path = _default_config_path_from_root(project_home)
    defaults_raw = read_toml_dict(defaults_path) if defaults_path.exists() else {}

    ldres_config = (
        project_home
        / ".ldres"
        / "etc"
        / "ml_playground"
        / "experiments"
        / experiment_name
        / "config.toml"
    )
    ldres_raw = read_toml_dict(ldres_config) if ldres_config.exists() else {}

    merged = merge_mappings(defaults_raw, raw_exp, override_only=True)

    merged_payload = merge_mappings(merged, ldres_raw)
    return cast(ExperimentPayload, merged_payload)


def load_full_experiment_config(
    config_path: Path, project_home: Path, experiment_name: str
) -> ExperimentConfig:
    effective_config = _load_and_merge_configs(
        config_path, project_home, experiment_name
    )

    for section in ("prepare", "train", "sample"):
        section_data = effective_config.get(section)
        if isinstance(section_data, Mapping):
            _validate_extras(experiment_name, section, cast(TomlMapping, section_data))

    shared = _ensure_mapping(effective_config.setdefault("shared", {}), "[shared]")
    shared["config_path"] = config_path
    shared["project_home"] = project_home
    shared["experiment"] = experiment_name
    effective_config["shared"] = shared

    return ExperimentConfig.model_validate(
        effective_config,
        context={"config_path": config_path},
    )


def load_train_config(
    config_path: Path, *, default_config_path: Path | None = None
) -> TrainerConfig:
    raw_exp = read_toml_dict(config_path)
    project_root = _project_root()
    defaults_path = (
        default_config_path
        if default_config_path is not None
        else _default_config_path_from_root(project_root)
    )
    defaults_raw = read_toml_dict(defaults_path) if defaults_path.exists() else {}

    raw_merged = merge_mappings(defaults_raw, raw_exp)

    train_data = _ensure_mapping(raw_merged.get("train", {}), "[train] section")
    _validate_extras(config_path.parent.name, "train", train_data)

    context = {"config_path": config_path}
    cfg = TrainerConfig.model_validate(train_data, context=context)

    info = {"raw": raw_merged, "context": {"config_path": str(config_path)}}
    cfg.extras["provenance"] = info
    return cfg


def load_sample_config(
    config_path: Path, *, default_config_path: Path | None = None
) -> SamplerConfig:
    raw_exp = read_toml_dict(config_path)
    project_root = _project_root()
    defaults_path = (
        default_config_path
        if default_config_path is not None
        else _default_config_path_from_root(project_root)
    )
    defaults_raw = read_toml_dict(defaults_path) if defaults_path.exists() else {}

    raw_merged = merge_mappings(defaults_raw, raw_exp)

    if "sample" not in raw_exp:
        raise ValueError("Config must contain a [sample] section")

    sample_data = _ensure_mapping(raw_merged.get("sample", {}), "[sample] section")
    _validate_extras(config_path.parent.name, "sample", sample_data)

    context = {"config_path": config_path}
    cfg = SamplerConfig.model_validate(sample_data, context=context)

    info = {"raw": raw_merged, "context": {"config_path": str(config_path)}}
    cfg.extras["provenance"] = info
    return cfg


def load_prepare_config(
    config_path: Path, *, default_config_path: Path | None = None
) -> PreparerConfig:
    raw_exp = read_toml_dict(config_path)
    project_root = _project_root()
    defaults_path = (
        default_config_path
        if default_config_path is not None
        else _default_config_path_from_root(project_root)
    )
    defaults_raw = read_toml_dict(defaults_path) if defaults_path.exists() else {}

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


def load_experiment_toml(path: Path) -> ExperimentConfig:
    project_home = Path(__file__).resolve().parent.parent
    experiment_name = path.parent.name
    return load_full_experiment_config(path, project_home, experiment_name)


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
    "load_experiment_toml",
]
