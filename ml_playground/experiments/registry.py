from __future__ import annotations
from typing import TYPE_CHECKING, Any, Callable, Dict

if TYPE_CHECKING:
    from ml_playground.configuration.models import PreparerConfig
else:  # pragma: no cover - runtime fallback avoids import cycles
    PreparerConfig = Any  # type: ignore[assignment]
if TYPE_CHECKING:
    from ml_playground.configuration.models import TrainerConfig, SamplerConfig, SharedConfig
else:  # pragma: no cover - runtime fallback avoids import cycles
    TrainerConfig = Any  # type: ignore[assignment]
    SamplerConfig = Any  # type: ignore[assignment]
    SharedConfig = Any  # type: ignore[assignment]
from importlib import import_module
from importlib import resources

# Registry for experiment-level dataset preparers (mirrors previous datasets.PREPARERS)
PREPARERS: Dict[str, Callable[[PreparerConfig | None], Any]] = {}
TRAINERS: Dict[str, Callable[[TrainerConfig | None, SharedConfig | None], Any]] = {}
SAMPLERS: Dict[str, Callable[[SamplerConfig | None, SharedConfig | None], Any]] = {}


def load_preparers(
    *,
    resources_mod: Any | None = None,
    import_mod: Callable[[str], Any] | None = None,
) -> None:
    """Plugin loader: import experiment preparers to populate PREPARERS.

    Strict mode: only class-based API is supported. An experiment must expose
    a preparer.py with a class that has a .prepare method. A callable is
    registered that instantiates the class and invokes ``.prepare`` with the
    provided :class:`PreparerConfig` when supplied (falling back to a no-arg
    call for legacy preparers).
    """
    if PREPARERS:
        # Already populated (or tests monkeypatched it)
        return

    pkg = "ml_playground.experiments"
    _resources = resources_mod if resources_mod is not None else resources
    _import = import_mod if import_mod is not None else import_module
    try:
        root = _resources.files(pkg)
    except (ImportError, FileNotFoundError, OSError, RuntimeError):
        # If discovery fails (e.g., frozen environments), do nothing; callers may
        # have injected PREPARERS via tests or alternative mechanisms.
        return

    for entry in root.iterdir():
        try:
            if not entry.is_dir():
                continue
            exp_name = entry.name

            # Strict API: preparer.py with a class exposing .prepare
            prep_file = entry / "preparer.py"
            if not prep_file.is_file():
                continue
            try:
                mod = _import(f"{pkg}.{exp_name}.preparer")
                # Find first class with a 'prepare' attribute
                cls = None
                for attr_name in dir(mod):
                    attr = getattr(mod, attr_name)
                    if isinstance(attr, type) and hasattr(attr, "prepare"):
                        cls = attr
                        break
                if cls is None:
                    continue
                from ml_playground.configuration.models import (
                    PreparerConfig,
                )  # local import

                def _make_runner(_cls=cls) -> Callable[[PreparerConfig | None], Any]:
                    def _runner(cfg: PreparerConfig | None = None) -> Any:
                        inst = _cls()
                        if cfg is None:
                            try:
                                return inst.prepare()  # type: ignore[attr-defined]
                            except TypeError:
                                return inst.prepare(PreparerConfig())  # type: ignore[attr-defined]
                        try:
                            return inst.prepare(cfg)  # type: ignore[attr-defined]
                        except TypeError:
                            return inst.prepare()  # type: ignore[attr-defined]

                    return _runner

                PREPARERS.setdefault(exp_name, _make_runner())
            except (ImportError, AttributeError, RuntimeError) as e:
                raise SystemExit(f"Failed to load experiment '{exp_name}': {e}")
        except (OSError, RuntimeError):
            # Defensive: ignore any unexpected filesystem/resource issues per entry
            continue


def load_trainers(
    *,
    resources_mod: Any | None = None,
    import_mod: Callable[[str], Any] | None = None,
) -> None:
    if TRAINERS:
        return

    pkg = "ml_playground.experiments"
    _resources = resources_mod if resources_mod is not None else resources
    _import = import_mod if import_mod is not None else import_module
    try:
        root = _resources.files(pkg)
    except (ImportError, FileNotFoundError, OSError, RuntimeError):
        return

    for entry in root.iterdir():
        try:
            if not entry.is_dir():
                continue
            exp_name = entry.name
            trainer_file = entry / "trainer.py"
            if not trainer_file.is_file():
                continue
            try:
                mod = _import(f"{pkg}.{exp_name}.trainer")
                cls = None
                for attr_name in dir(mod):
                    attr = getattr(mod, attr_name)
                    if isinstance(attr, type) and hasattr(attr, "train"):
                        cls = attr
                        break
                if cls is None:
                    continue
                from ml_playground.configuration.models import (
                    TrainerConfig,
                    SharedConfig,
                )  # local import

                def _make_runner(
                    _cls=cls,
                ) -> Callable[[TrainerConfig | None, SharedConfig | None], Any]:
                    def _runner(
                        cfg: TrainerConfig | None = None,
                        shared: SharedConfig | None = None,
                    ) -> Any:
                        inst = _cls()
                        if cfg is not None and shared is not None:
                            try:
                                return inst.train(cfg, shared)  # type: ignore[attr-defined]
                            except TypeError:
                                pass
                        if cfg is not None:
                            try:
                                return inst.train(cfg)  # type: ignore[attr-defined]
                            except TypeError:
                                pass
                        if shared is not None:
                            try:
                                return inst.train(shared)  # type: ignore[attr-defined]
                            except TypeError:
                                pass
                        return inst.train()  # type: ignore[attr-defined]

                    return _runner

                TRAINERS.setdefault(exp_name, _make_runner())
            except (ImportError, AttributeError, RuntimeError) as e:
                raise SystemExit(f"Failed to load experiment '{exp_name}': {e}")
        except (OSError, RuntimeError):
            continue


def load_samplers(
    *,
    resources_mod: Any | None = None,
    import_mod: Callable[[str], Any] | None = None,
) -> None:
    if SAMPLERS:
        return

    pkg = "ml_playground.experiments"
    _resources = resources_mod if resources_mod is not None else resources
    _import = import_mod if import_mod is not None else import_module
    try:
        root = _resources.files(pkg)
    except (ImportError, FileNotFoundError, OSError, RuntimeError):
        return

    for entry in root.iterdir():
        try:
            if not entry.is_dir():
                continue
            exp_name = entry.name
            sampler_file = entry / "sampler.py"
            if not sampler_file.is_file():
                continue
            try:
                mod = _import(f"{pkg}.{exp_name}.sampler")
                cls = None
                for attr_name in dir(mod):
                    attr = getattr(mod, attr_name)
                    if isinstance(attr, type) and hasattr(attr, "sample"):
                        cls = attr
                        break
                if cls is None:
                    continue
                from ml_playground.configuration.models import (
                    SamplerConfig,
                    SharedConfig,
                )  # local import

                def _make_runner(
                    _cls=cls,
                ) -> Callable[[SamplerConfig | None, SharedConfig | None], Any]:
                    def _runner(
                        cfg: SamplerConfig | None = None,
                        shared: SharedConfig | None = None,
                    ) -> Any:
                        inst = _cls()
                        if cfg is not None and shared is not None:
                            try:
                                return inst.sample(cfg, shared)  # type: ignore[attr-defined]
                            except TypeError:
                                pass
                        if cfg is not None:
                            try:
                                return inst.sample(cfg)  # type: ignore[attr-defined]
                            except TypeError:
                                pass
                        if shared is not None:
                            try:
                                return inst.sample(shared)  # type: ignore[attr-defined]
                            except TypeError:
                                pass
                        return inst.sample()  # type: ignore[attr-defined]

                    return _runner

                SAMPLERS.setdefault(exp_name, _make_runner())
            except (ImportError, AttributeError, RuntimeError) as e:
                raise SystemExit(f"Failed to load experiment '{exp_name}': {e}")
        except (OSError, RuntimeError):
            continue
