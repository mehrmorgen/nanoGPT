from __future__ import annotations

import sys
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import ContextManager, Iterator, Mapping, cast
from collections.abc import Callable

import pytest

from ml_playground.tools.analysis import lit_integration


OverrideAttr = Callable[[object, str, object], ContextManager[None]]


class _CapturingLogger:
    def __init__(self) -> None:
        self.infos: list[str] = []

    def debug(self, msg: str, *_args: object, **_kwargs: object) -> None:
        return

    def info(self, msg: str, *_args: object, **_kwargs: object) -> None:
        self.infos.append(msg)

    def warning(self, msg: str, *_args: object, **_kwargs: object) -> None:
        return

    def error(self, msg: str, *_args: object, **_kwargs: object) -> None:
        return


@contextmanager
def _install_modules(modules: Mapping[str, ModuleType]) -> Iterator[None]:
    originals: dict[str, ModuleType | None] = {}
    try:
        for name, module in modules.items():
            originals[name] = sys.modules.get(name)
            sys.modules[name] = module
        yield
    finally:
        for name, original in originals.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original


def _make_fake_lit_modules(*, server: ModuleType) -> dict[str, ModuleType]:
    dataset_module = ModuleType("lit_nlp.api.dataset")

    class DatasetBase:
        pass

    dataset_module.Dataset = DatasetBase  # type: ignore[attr-defined]

    model_module = ModuleType("lit_nlp.api.model")

    class ModelBase:
        pass

    model_module.Model = ModelBase  # type: ignore[attr-defined]

    types_module = ModuleType("lit_nlp.api.types")

    def TextSegment() -> str:  # noqa: N802 - external API name
        return "TextSegment"

    types_module.TextSegment = TextSegment  # type: ignore[attr-defined]

    lit_api_module = ModuleType("lit_nlp.api")
    lit_api_module.__path__ = []  # type: ignore[attr-defined]

    lit_module = ModuleType("lit_nlp")
    lit_module.__path__ = []  # type: ignore[attr-defined]
    lit_module.server = server  # type: ignore[attr-defined]

    return {
        "lit_nlp": lit_module,
        "lit_nlp.api": lit_api_module,
        "lit_nlp.api.dataset": dataset_module,
        "lit_nlp.api.model": model_module,
        "lit_nlp.api.types": types_module,
        "lit_nlp.server": server,
    }


def test_run_server_bundestag_char_happy_path_embedded_samples(
    override_attr: OverrideAttr,
) -> None:
    state: dict[str, object] = {}

    class App:
        def __init__(
            self, models: Mapping[str, object], datasets: Mapping[str, object]
        ):
            state["models"] = models
            state["datasets"] = datasets

        def serve(self, *, port: int, host: str, open_browser: bool) -> None:
            state["serve_kwargs"] = {
                "port": port,
                "host": host,
                "open_browser": open_browser,
            }

    server_module = ModuleType("lit_nlp.server")

    def Server(models: Mapping[str, object], datasets: Mapping[str, object]) -> App:  # noqa: N802
        return App(models, datasets)

    server_module.Server = Server  # type: ignore[attr-defined]

    modules = _make_fake_lit_modules(server=server_module)

    logger = _CapturingLogger()

    with _install_modules(modules):
        lit_integration.run_server_bundestag_char(
            host="127.0.0.1",
            port=0,
            open_browser=False,
            logger=logger,
        )

    assert state["serve_kwargs"] == {
        "port": 0,
        "host": "127.0.0.1",
        "open_browser": False,
    }

    datasets = cast(Mapping[str, object], state["datasets"])
    models = cast(Mapping[str, object], state["models"])

    dataset = datasets["bundestag_char_sample"]
    model = models["echo_model"]

    spec = cast(object, getattr(dataset, "spec"))
    assert callable(spec)
    spec_result = cast(dict[str, object], spec())
    assert "text" in spec_result

    length = cast(object, getattr(dataset, "__len__"))
    assert callable(length)
    assert cast(int, length()) >= 0

    iterator = cast(object, getattr(dataset, "__iter__"))
    assert callable(iterator)

    predict = cast(object, getattr(model, "predict"))
    assert callable(predict)

    input_spec = cast(object, getattr(model, "input_spec"))
    output_spec = cast(object, getattr(model, "output_spec"))
    assert callable(input_spec)
    assert callable(output_spec)

    examples = list(cast(Iterator[Mapping[str, object]], iterator()))
    assert examples

    in_spec = cast(dict[str, object], input_spec())
    out_spec = cast(dict[str, object], output_spec())
    assert "text" in in_spec
    assert "generated" in out_spec

    out = cast(list[Mapping[str, object]], predict(examples[:2]))
    assert out
    assert "generated" in out[0]

    assert any("Registered models" in msg for msg in logger.infos)
    assert any("Registered datasets" in msg for msg in logger.infos)
    assert any("Starting server" in msg for msg in logger.infos)


def test_run_server_bundestag_char_uses_input_file_lines(
    override_attr: OverrideAttr,
) -> None:
    state: dict[str, object] = {}

    class App:
        def __init__(
            self, _models: Mapping[str, object], datasets: Mapping[str, object]
        ):
            state["datasets"] = datasets

        def serve(self, *, port: int, host: str, open_browser: bool) -> None:  # noqa: ARG002
            return

    server_module = ModuleType("lit_nlp.server")

    def Server(models: Mapping[str, object], datasets: Mapping[str, object]) -> App:  # noqa: N802,ARG001
        return App(models, datasets)

    server_module.Server = Server  # type: ignore[attr-defined]

    modules = _make_fake_lit_modules(server=server_module)

    def fake_exists(self: Path) -> bool:
        if str(self).endswith("experiments/bundestag_char/datasets/input.txt"):
            return True
        return Path.exists(self)

    def fake_read_text(
        self: Path,
        *,
        encoding: str | None = None,
        errors: str | None = None,
    ) -> str:
        if str(self).endswith("experiments/bundestag_char/datasets/input.txt"):
            return "first\n\nsecond\n"
        return Path.read_text(self, encoding=encoding, errors=errors)

    with _install_modules(modules):
        with override_attr(lit_integration.Path, "exists", fake_exists):
            with override_attr(lit_integration.Path, "read_text", fake_read_text):
                lit_integration.run_server_bundestag_char(
                    host="127.0.0.1",
                    port=0,
                    open_browser=False,
                    logger=_CapturingLogger(),
                )

    datasets = cast(Mapping[str, object], state["datasets"])
    dataset = datasets["bundestag_char_sample"]
    examples = cast(list[Mapping[str, object]], getattr(dataset, "_examples"))
    assert [ex["text"] for ex in examples] == ["first", "second"]


def test_run_server_bundestag_char_ignores_empty_input_file(
    override_attr: OverrideAttr,
) -> None:
    state: dict[str, object] = {}

    class App:
        def __init__(
            self, _models: Mapping[str, object], datasets: Mapping[str, object]
        ):
            state["datasets"] = datasets

        def serve(self, *, port: int, host: str, open_browser: bool) -> None:  # noqa: ARG002
            return

    server_module = ModuleType("lit_nlp.server")

    def Server(models: Mapping[str, object], datasets: Mapping[str, object]) -> App:  # noqa: N802,ARG001
        return App(models, datasets)

    server_module.Server = Server  # type: ignore[attr-defined]

    modules = _make_fake_lit_modules(server=server_module)

    def fake_exists(self: Path) -> bool:
        if str(self).endswith("experiments/bundestag_char/datasets/input.txt"):
            return True
        return Path.exists(self)

    def fake_read_text(
        self: Path,
        *,
        encoding: str | None = None,
        errors: str | None = None,
    ) -> str:
        if str(self).endswith("experiments/bundestag_char/datasets/input.txt"):
            return "\n\n"
        return Path.read_text(self, encoding=encoding, errors=errors)

    with _install_modules(modules):
        with override_attr(lit_integration.Path, "exists", fake_exists):
            with override_attr(lit_integration.Path, "read_text", fake_read_text):
                lit_integration.run_server_bundestag_char(
                    host="127.0.0.1",
                    port=0,
                    open_browser=False,
                    logger=_CapturingLogger(),
                )

    datasets = cast(Mapping[str, object], state["datasets"])
    dataset = datasets["bundestag_char_sample"]
    examples = cast(list[Mapping[str, object]], getattr(dataset, "_examples"))
    assert "Nächste Rednerin" in str(examples[0]["text"])


def test_run_server_bundestag_char_ignores_input_file_unicode_error(
    override_attr: OverrideAttr,
) -> None:
    state: dict[str, object] = {}

    class App:
        def __init__(
            self, _models: Mapping[str, object], datasets: Mapping[str, object]
        ):
            state["datasets"] = datasets

        def serve(self, *, port: int, host: str, open_browser: bool) -> None:  # noqa: ARG002
            return

    server_module = ModuleType("lit_nlp.server")

    def Server(models: Mapping[str, object], datasets: Mapping[str, object]) -> App:  # noqa: N802,ARG001
        return App(models, datasets)

    server_module.Server = Server  # type: ignore[attr-defined]

    modules = _make_fake_lit_modules(server=server_module)

    def fake_exists(self: Path) -> bool:
        if str(self).endswith("experiments/bundestag_char/datasets/input.txt"):
            return True
        return Path.exists(self)

    def fake_read_text(
        self: Path,
        *,
        encoding: str | None = None,
        errors: str | None = None,
    ) -> str:
        if str(self).endswith("experiments/bundestag_char/datasets/input.txt"):
            raise UnicodeError("boom")
        return Path.read_text(self, encoding=encoding, errors=errors)

    with _install_modules(modules):
        with override_attr(lit_integration.Path, "exists", fake_exists):
            with override_attr(lit_integration.Path, "read_text", fake_read_text):
                lit_integration.run_server_bundestag_char(
                    host="127.0.0.1",
                    port=0,
                    open_browser=False,
                    logger=_CapturingLogger(),
                )

    datasets = cast(Mapping[str, object], state["datasets"])
    dataset = datasets["bundestag_char_sample"]
    examples = cast(list[Mapping[str, object]], getattr(dataset, "_examples"))
    assert "Nächste Rednerin" in str(examples[0]["text"])


def test_run_server_bundestag_char_raises_when_lit_deps_missing() -> None:
    for name in [
        "lit_nlp",
        "lit_nlp.api",
        "lit_nlp.api.dataset",
        "lit_nlp.api.model",
        "lit_nlp.api.types",
        "lit_nlp.server",
    ]:
        sys.modules.pop(name, None)

    with pytest.raises(RuntimeError, match=r"LIT dependencies not available"):
        lit_integration.run_server_bundestag_char(
            host="127.0.0.1",
            port=0,
            open_browser=False,
            logger=_CapturingLogger(),
        )


def test_run_server_bundestag_char_raises_when_server_import_fails() -> None:
    server_module = ModuleType("lit_nlp.server")

    modules = _make_fake_lit_modules(server=server_module)
    lit_module = modules["lit_nlp"]
    delattr(lit_module, "server")  # type: ignore[arg-type]

    # Ensure the import statement inside the implementation (`from lit_nlp import server`)
    # does not fall back to an already-loaded `lit_nlp.server` submodule.
    modules.pop("lit_nlp.server", None)

    with _install_modules(modules):
        sys.modules.pop("lit_nlp.server", None)
        with pytest.raises(RuntimeError, match=r"LIT server import failed"):
            lit_integration.run_server_bundestag_char(
                host="127.0.0.1",
                port=0,
                open_browser=False,
                logger=_CapturingLogger(),
            )


def test_run_server_bundestag_char_wraps_app_construction_error() -> None:
    server_module = ModuleType("lit_nlp.server")

    def Server(*_args: object, **_kwargs: object) -> object:  # noqa: N802
        raise ValueError("boom")

    server_module.Server = Server  # type: ignore[attr-defined]

    modules = _make_fake_lit_modules(server=server_module)

    with _install_modules(modules):
        with pytest.raises(RuntimeError, match=r"Failed to build LIT app"):
            lit_integration.run_server_bundestag_char(
                host="127.0.0.1",
                port=0,
                open_browser=False,
                logger=_CapturingLogger(),
            )


def test_run_server_bundestag_char_wraps_missing_serve_method() -> None:
    server_module = ModuleType("lit_nlp.server")

    class App:
        pass

    def Server(*_args: object, **_kwargs: object) -> App:  # noqa: N802
        return App()

    server_module.Server = Server  # type: ignore[attr-defined]

    modules = _make_fake_lit_modules(server=server_module)

    with _install_modules(modules):
        with pytest.raises(
            RuntimeError,
            match=r"does not expose a serve\(\.\.\.\) method",
        ):
            lit_integration.run_server_bundestag_char(
                host="127.0.0.1",
                port=0,
                open_browser=False,
                logger=_CapturingLogger(),
            )
