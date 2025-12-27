from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, cast

import typer
from pytest import LogCaptureFixture

from ml_playground.runtime import helpers


@contextmanager
def override_attr(obj: object, name: str, value: Any) -> Iterator[None]:
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


class _ListLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, msg: str) -> None:
        self.messages.append(msg)

    def debug(self, msg: str) -> None:  # pragma: no cover - unused
        self.messages.append(msg)

    def warning(self, msg: str) -> None:  # pragma: no cover - unused
        self.messages.append(msg)

    def error(self, msg: str) -> None:  # pragma: no cover - unused
        self.messages.append(msg)


def test_complete_experiments_delegates_to_loader() -> None:
    captured: list[str] = []

    def fake_list(incomplete: str) -> list[str]:
        captured.append(incomplete)
        return ["a", "b"]

    with override_attr(
        helpers.config_loading, "list_experiments_with_config", fake_list
    ):
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

    shared = Shared(dataset_dir=Path("/tmp"))

    class FailingLogger:
        def debug(self, msg: str) -> None:  # pragma: no cover - unused
            raise TypeError(msg)

        def info(self, msg: str) -> None:
            raise TypeError(msg)

        def warning(self, msg: str) -> None:  # pragma: no cover - unused
            raise TypeError(msg)

        def error(self, msg: str) -> None:  # pragma: no cover - unused
            raise TypeError(msg)

    helpers.log_command_status("tag", shared, out_dir=None, logger=FailingLogger())


def test_run_or_exit_keyboard_interrupt_logs_and_returns(
    caplog: LogCaptureFixture,
) -> None:
    with caplog.at_level("INFO"):
        helpers.run_or_exit(
            lambda: (_ for _ in ()).throw(KeyboardInterrupt),
            keyboard_interrupt_msg="stop",
        )

    assert "stop" in caplog.text
