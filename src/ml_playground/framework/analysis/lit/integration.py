from __future__ import annotations

import importlib
from collections.abc import Callable, Iterable, Mapping, Sequence
import argparse
from pathlib import Path
from types import ModuleType
from typing import Protocol, cast

from ml_playground.framework.core.di_implementations import DefaultModuleImporter
from ml_playground.framework.core.logging_protocol import LoggerLike
from ml_playground.framework.core.project_config import get_default_host

WSGIApp = Callable[..., Iterable[bytes]]


class LitDataset(Protocol):
    def spec(self) -> dict[str, object]: ...

    def __len__(self) -> int: ...

    def __iter__(self) -> Iterable[Mapping[str, object]]: ...


class LitDatasetModule(Protocol):
    Dataset: type[LitDataset]


class LitModel(Protocol):
    def input_spec(self) -> dict[str, object]: ...

    def output_spec(self) -> dict[str, object]: ...

    def predict(
        self, _inputs: Iterable[Mapping[str, object]], **kwargs: object
    ) -> list[Mapping[str, object]]: ...


class LitModelModule(Protocol):
    Model: type[LitModel]


class LitTypesModule(Protocol):
    def TextSegment(self) -> object: ...


def _load_lit_components() -> tuple[LitDatasetModule, LitModelModule, LitTypesModule]:
    try:
        importlib.import_module("lit_nlp.api")
    except ImportError as exc:
        message = (
            "LIT dependencies are unavailable. Install lit-nlp in an isolated environment "
            + "or add it as an extra before using this integration. Try `uv sync --extra lit` "
            + "or `uv add lit-nlp`."
        )
        raise RuntimeError(message) from exc

    module_importer = DefaultModuleImporter()
    dataset_mod = cast(LitDatasetModule, module_importer.import_dataset_module())
    model_mod = cast(LitModelModule, module_importer.import_model_module())
    types_mod = cast(LitTypesModule, module_importer.import_types_module())
    return dataset_mod, model_mod, types_mod


class LitServerFactory(Protocol):
    def __call__(
        self,
        models: dict[str, LitModel],
        datasets: dict[str, LitDataset],
    ) -> object: ...


def _import_lit_server() -> ModuleType:
    paths = (
        "lit_nlp.server",
        "lit_nlp.dev_server",
        "lit_nlp.runtime.server",
        "lit_nlp.lib.server",
    )
    last_err: Exception | None = None
    for candidate in paths:
        try:
            return importlib.import_module(candidate)
        except (ImportError, ModuleNotFoundError) as err:
            last_err = err

    try:
        lit_pkg = importlib.import_module("lit_nlp")
        lit_ver = getattr(lit_pkg, "__version__", "<unknown>")
        ver_msg = f"(detected lit-nlp version: {lit_ver})"
    except (ImportError, AttributeError):
        ver_msg = "(lit-nlp not importable)"

    message = (
        "Unable to import LIT server module. Tried: lit_nlp.server, "
        + "lit_nlp.dev_server, lit_nlp.runtime.server, lit_nlp.lib.server.\n"
        + f"{ver_msg}. Last error: {last_err}"
    )
    raise RuntimeError(message)


def _resolve_experiment_lit_runner(
    experiment: str, *, import_fn: Callable[[str], object] = importlib.import_module
) -> Callable[..., None]:
    module_name = f"ml_playground.experiments.{experiment}.lit_integration"
    try:
        module_obj = import_fn(module_name)
    except ImportError as exc:
        raise RuntimeError(
            f"No LIT integration module registered for experiment: {experiment}"
        ) from exc

    module = cast(ModuleType, module_obj)
    run_server = getattr(module, "run_server", None)
    if callable(run_server):
        return cast(Callable[..., None], run_server)

    legacy_name = f"run_server_{experiment}"
    legacy_runner = getattr(module, legacy_name, None)
    if callable(legacy_runner):
        return cast(Callable[..., None], legacy_runner)

    raise RuntimeError(
        f"LIT integration module for '{experiment}' does not expose run_server(...)"
    )


def run_server_experiment(
    experiment: str,
    host: str | None = None,
    port: int = 5432,
    open_browser: bool = False,
    logger: LoggerLike | None = None,
    _loader_override: Callable[
        [], tuple[LitDatasetModule, LitModelModule, LitTypesModule]
    ]
    | None = None,
    _server_importer_override: Callable[[], ModuleType] | None = None,
    _path_resolver_override: Callable[[Path], Path] | None = None,
) -> None:
    """Run experiment-owned LIT wiring for a given experiment."""
    run_experiment_lit_server = _resolve_experiment_lit_runner(experiment)

    loader = _loader_override or _load_lit_components
    server_importer = _server_importer_override or _import_lit_server

    def _wrapped_loader() -> tuple[LitDatasetModule, LitModelModule, LitTypesModule]:
        try:
            return loader()
        except RuntimeError as exc:
            message = (
                "LIT dependencies not available. Install lit-nlp in an isolated environment "
                + "or add it as an extra before using this integration. Try `uv sync --extra lit` "
                + "or `uv add lit-nlp`."
            )
            raise RuntimeError(message) from exc

    def _wrapped_server_importer() -> ModuleType:
        try:
            return server_importer()
        except RuntimeError as exc:
            message = (
                "LIT server import failed. Ensure a supported lit-nlp version is installed. "
                + "Try `uv sync --extra lit` or `uv add lit-nlp`."
            )
            raise RuntimeError(message) from exc

    run_experiment_lit_server(
        host=host,
        port=port,
        open_browser=open_browser,
        logger=logger,
        _loader_override=_wrapped_loader,
        _server_importer_override=_wrapped_server_importer,
        _path_resolver_override=_path_resolver_override,
    )


def run_server_bundestag_char(
    host: str | None = None,
    port: int = 5432,
    open_browser: bool = False,
    logger: LoggerLike | None = None,
    _loader_override: Callable[
        [], tuple[LitDatasetModule, LitModelModule, LitTypesModule]
    ]
    | None = None,
    _server_importer_override: Callable[[], ModuleType] | None = None,
    _path_resolver_override: Callable[[Path], Path] | None = None,
) -> None:
    """Backward-compatible alias for the bundestag_char experiment."""
    run_server_experiment(
        "bundestag_char",
        host=host,
        port=port,
        open_browser=open_browser,
        logger=logger,
        _loader_override=_loader_override,
        _server_importer_override=_server_importer_override,
        _path_resolver_override=_path_resolver_override,
    )


def _parse_cli_args(argv: Sequence[str] | None = None) -> tuple[str, int, bool]:
    parser = argparse.ArgumentParser(description="Run experiment LIT server")
    try:
        default_host = get_default_host()
    except (ValueError, TypeError):
        default_host = "127.0.0.1"
    _host_arg = parser.add_argument(
        "--host", type=str, default=default_host, help="Host to bind"
    )
    _port_arg = parser.add_argument(
        "--port", type=int, default=5432, help="Port to bind (0 for auto)"
    )
    _open_browser_arg = parser.add_argument(
        "--open-browser", action="store_true", help="Open browser on start"
    )
    namespace = parser.parse_args(argv)

    host_attr = cast(object, namespace.host)
    if not isinstance(host_attr, str):
        raise TypeError("--host must be a string")
    host_value: str = host_attr

    port_attr = cast(object, namespace.port)
    if not isinstance(port_attr, int):
        raise TypeError("--port must be parsed as an integer")
    port_value: int = port_attr

    open_browser_attr = cast(object, namespace.open_browser)
    open_browser_value: bool
    if isinstance(open_browser_attr, bool):
        open_browser_value = open_browser_attr
    else:
        open_browser_value = bool(open_browser_attr)

    return host_value, port_value, open_browser_value


# Public wrappers (non-underscored) for external callers/tests
def load_lit_components() -> tuple[LitDatasetModule, LitModelModule, LitTypesModule]:
    """Public entry to load lit components."""
    return _load_lit_components()


def import_lit_server() -> ModuleType:
    """Public entry to import lit server module."""
    return _import_lit_server()


def parse_cli_args(argv: Sequence[str] | None = None) -> tuple[str, int, bool]:
    """Public entry to parse CLI args for the lit server."""
    return _parse_cli_args(argv)


__all__ = [
    "LitDataset",
    "LitDatasetModule",
    "LitModel",
    "LitModelModule",
    "LitTypesModule",
    "load_lit_components",
    "import_lit_server",
    "parse_cli_args",
    "run_server_experiment",
    "run_server_bundestag_char",
]


if __name__ == "__main__":
    host_arg, port_arg, open_browser_arg = _parse_cli_args()
    run_server_bundestag_char(
        host=host_arg,
        port=port_arg,
        open_browser=open_browser_arg,
    )
