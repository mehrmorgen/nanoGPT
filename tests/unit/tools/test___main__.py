from __future__ import annotations

import runpy

import ml_playground.tools.cli.main as tools_cli_main


def test_tools_module_main_invokes_main_entry() -> None:
    called: list[bool] = []

    def fake_main_entry() -> None:
        called.append(True)

    original = tools_cli_main.main_entry
    tools_cli_main.main_entry = fake_main_entry
    try:
        runpy.run_module("ml_playground.tools.__main__", run_name="__main__")
    finally:
        tools_cli_main.main_entry = original

    assert called == [True]
