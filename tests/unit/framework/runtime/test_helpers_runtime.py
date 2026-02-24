from __future__ import annotations

from pathlib import Path
from typing import cast

import typer
from pytest import LogCaptureFixture

from ml_playground.framework.runtime import helpers
from ml_playground.framework.runtime.core.bootstrap import (
    CLIDependencies,
    override_cli_dependencies,
)


class _ListLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, msg: object, *args: object, **kwargs: object) -> None:
        self.messages.append(str(msg))

    def debug(
        self, msg: object, *args: object, **kwargs: object
    ) -> None:  # pragma: no cover - unused
        self.messages.append(str(msg))

    def warning(
        self, msg: object, *args: object, **kwargs: object
    ) -> None:  # pragma: no cover - unused
        self.messages.append(str(msg))

    def error(
        self, msg: object, *args: object, **kwargs: object
    ) -> None:  # pragma: no cover - unused
        self.messages.append(str(msg))


def test_complete_experiments_delegates_to_deps() -> None:
    captured: list[str] = []

    def fake_list(incomplete: str) -> list[str]:
        captured.append(incomplete)
        return ["a", "b"]

    deps = CLIDependencies(list_experiments=fake_list)
    with override_cli_dependencies(deps):
        ctx = cast(typer.Context, object())
        assert helpers.complete_experiments(ctx=ctx, incomplete="demo") == [
            "a",
            "b",
        ]

    assert captured == ["demo"]


def test_extract_exp_config_returns_none_for_non_dict_context() -> None:
    class Ctx:
        obj = object()

    ctx = cast(typer.Context, Ctx())
    assert helpers.extract_exp_config(ctx) is None


def test_extract_exp_config_ignores_non_path_value() -> None:
    class Ctx:
        obj = {"exp_config": "nope"}

    ctx = cast(typer.Context, Ctx())
    assert helpers.extract_exp_config(ctx) is None


def test_log_directory_returns_for_non_path() -> None:
    logger = _ListLogger()

    helpers.log_directory("tag", "dir", "not-a-path", logger)  # type: ignore[arg-type]

    assert logger.messages == []


def test_log_directory_logs_exists_even_when_iterdir_raises(tmp_path: Path) -> None:
    logger = _ListLogger()

    file_path = tmp_path / "not_a_dir.txt"
    file_path.write_text("x", encoding="utf-8")

    helpers.log_directory("tag", "dir", file_path, logger)

    assert any("(exists)" in msg for msg in logger.messages)
    assert not any("Contents" in msg for msg in logger.messages)


def test_log_command_status_returns_when_dataset_dir_access_raises() -> None:
    logger = _ListLogger()

    class BadShared:
        @property
        def dataset_dir(self) -> Path:
            raise TypeError("boom")

    helpers.log_command_status("tag", BadShared(), out_dir=None, logger=logger)  # type: ignore[arg-type]
    assert logger.messages == []


def test_log_command_status_swallows_errors_from_log_directory() -> None:
    import dataclasses

    @dataclasses.dataclass
    class Shared:
        dataset_dir: Path
        config_path: Path = Path("/tmp/config.toml")
        train_out_dir: Path = Path("/tmp/train")
        sample_out_dir: Path = Path("/tmp/sample")

    metadata = Shared(dataset_dir=Path("/tmp"))

    class FailingLogger:
        def debug(
            self, msg: object, *args: object, **kwargs: object
        ) -> None:  # pragma: no cover - unused
            raise TypeError(str(msg))

        def info(self, msg: object, *args: object, **kwargs: object) -> None:
            raise TypeError(str(msg))

        def warning(
            self, msg: object, *args: object, **kwargs: object
        ) -> None:  # pragma: no cover - unused
            raise TypeError(str(msg))

        def error(
            self, msg: object, *args: object, **kwargs: object
        ) -> None:  # pragma: no cover - unused
            raise TypeError(str(msg))

    helpers.log_command_status("tag", metadata, out_dir=None, logger=FailingLogger())


def test_run_or_exit_keyboard_interrupt_logs_and_returns(
    caplog: LogCaptureFixture,
) -> None:
    with caplog.at_level("INFO"):
        helpers.run_or_exit(
            lambda: (_ for _ in ()).throw(KeyboardInterrupt),
            keyboard_interrupt_msg="stop",
        )

    assert "stop" in caplog.text
