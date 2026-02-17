from __future__ import annotations

from typing import Any
from ml_playground.tools.analysis.lit_integration import run_server_bundestag_char


class FakeLogger:
    def info(
        self,
        msg: object,
        *args: object,
        exc_info: Any = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Any = None,
    ) -> None:
        pass

    def error(
        self,
        msg: object,
        *args: object,
        exc_info: Any = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Any = None,
    ) -> None:
        pass

    def warning(
        self,
        msg: object,
        *args: object,
        exc_info: Any = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Any = None,
    ) -> None:
        pass

    def debug(
        self,
        msg: object,
        *args: object,
        exc_info: Any = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Any = None,
    ) -> None:
        pass


def test_run_server_bundestag_char() -> None:
    called_with = {}

    def fake_run_server(**kwargs):
        nonlocal called_with
        called_with = kwargs

    logger = FakeLogger()
    run_server_bundestag_char(
        "localhost", 1234, True, logger, _run_server_override=fake_run_server
    )  # type: ignore
    assert called_with == {"host": "localhost", "port": 1234, "open_browser": True}


def test_main_block() -> None:
    """Test the if __name__ == "__main__": block content via main()."""
    import ml_playground.tools.analysis.lit_integration as mod
    import sys

    called_with = {}

    def fake_run_server(**kwargs):
        nonlocal called_with
        called_with = kwargs

    orig_argv = sys.argv
    sys.argv = ["prog", "--host", "1.2.3.4", "--port", "8888", "--open-browser"]
    try:
        mod.main(default_host="localhost", _run_server_override=fake_run_server)  # type: ignore
        assert called_with == {
            "experiment": "bundestag_char",
            "host": "1.2.3.4",
            "port": 8888,
            "open_browser": True,
        }
    finally:
        sys.argv = orig_argv
