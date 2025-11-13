from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Iterable, Iterator, Protocol

import pytest

import ml_playground.experiments.registry as registry


def test_load_preparers_returns_if_already_populated():
    """Test load preparers returns if already populated."""
    # Pre-populate
    registry.PREPARERS.clear()
    registry.PREPARERS["foo"] = lambda: None

    # If resources.files is called, fail the test
    def bad_files(_: str) -> None:  # noqa: D401
        raise AssertionError(
            "resources.files should not be called when PREPARERS present"
        )

    registry.load_preparers(resources_mod=SimpleNamespace(files=bad_files))
    assert "foo" in registry.PREPARERS


def test_load_preparers_handles_resources_error():
    """Test load preparers handles resources error."""
    registry.PREPARERS.clear()

    def raise_files(_: str) -> None:  # noqa: D401
        raise RuntimeError("boom")

    registry.load_preparers(resources_mod=SimpleNamespace(files=raise_files))
    assert registry.PREPARERS == {}


class _EntryLike(Protocol):
    name: str

    def is_dir(self) -> bool: ...


class _FakePath:
    def __init__(self, is_file: bool):
        self._is_file = is_file

    def is_file(self) -> bool:  # noqa: D401
        return self._is_file


class _FakeEntry:
    def __init__(self, name: str, is_dir: bool, has_preparer: bool):
        self.name = name
        self._is_dir = is_dir
        self._has_preparer = has_preparer

    def is_dir(self) -> bool:  # noqa: D401
        return self._is_dir

    def __truediv__(self, other: str):  # noqa: D401
        if other == "preparer.py":
            return _FakePath(self._has_preparer)
        raise AssertionError("unexpected path component")


class _FakeRoot:
    def __init__(self, entries: Iterable[_EntryLike]):
        self._entries: list[_EntryLike] = list(entries)

    def iterdir(self) -> Iterator[_EntryLike]:  # noqa: D401
        for entry in self._entries:
            yield entry


def test_load_preparers_registers_class():
    """Test load preparers registers class."""
    registry.PREPARERS.clear()
    root = _FakeRoot([_FakeEntry("expA", True, True)])

    def files(_: str) -> _FakeRoot:
        return root

    resources_ns = SimpleNamespace(files=files)

    class Prep:
        def prepare(self, *_args: Any, **_kwargs: Any) -> None:  # noqa: D401
            # no-op
            return None

    # import_module should return module with class Prep
    fake_mod = SimpleNamespace(Prep=Prep)

    def import_mod(name: str) -> SimpleNamespace:
        del name
        return fake_mod

    registry.load_preparers(resources_mod=resources_ns, import_mod=import_mod)
    assert "expA" in registry.PREPARERS
    # Calling the registered function shouldn't raise
    registry.PREPARERS["expA"]()


def test_load_preparers_raises_on_import_failure():
    """Test load preparers raises on import failure."""
    registry.PREPARERS.clear()
    root = _FakeRoot([_FakeEntry("bad", True, True)])

    def files(_: str) -> _FakeRoot:
        return root

    resources_ns = SimpleNamespace(files=files)

    def bad_import(name: str) -> None:  # noqa: D401
        return (_ for _ in ()).throw(RuntimeError("nope"))

    with pytest.raises(SystemExit) as ei:
        registry.load_preparers(resources_mod=resources_ns, import_mod=bad_import)
    assert "Failed to load experiment 'bad':" in str(ei.value)


def test_load_preparers_skips_non_dir_and_missing_preparer():
    """Test load preparers skips non dir and missing preparer."""
    registry.PREPARERS.clear()
    # One non-dir, one dir without preparer.py
    root = _FakeRoot(
        [
            _FakeEntry("file.txt", False, False),
            _FakeEntry("expNoPrep", True, False),
        ]
    )

    def files(_: str) -> _FakeRoot:
        return root

    registry.load_preparers(resources_mod=SimpleNamespace(files=files))
    assert registry.PREPARERS == {}


def test_load_preparers_module_without_prepare_class():
    """Test load preparers module without prepare class."""
    registry.PREPARERS.clear()
    root = _FakeRoot([_FakeEntry("expNoClass", True, True)])

    def files(_: str) -> _FakeRoot:
        return root

    resources_ns = SimpleNamespace(files=files)

    # Module has classes but none with 'prepare'
    class X:  # noqa: D401
        pass

    fake_mod = SimpleNamespace(X=X)

    def import_mod(name: str) -> SimpleNamespace:
        del name
        return fake_mod

    registry.load_preparers(resources_mod=resources_ns, import_mod=import_mod)
    # Should not register anything
    assert registry.PREPARERS == {}


def test_load_preparers_noarg_prepare_calls_without_args():
    """Test load preparers noarg prepare calls without args."""
    registry.PREPARERS.clear()
    root = _FakeRoot([_FakeEntry("expNoArg", True, True)])

    def files(_: str) -> _FakeRoot:
        return root

    resources_ns = SimpleNamespace(files=files)

    prepared = {"called": False}

    class Prep:
        def prepare(self) -> None:  # noqa: D401
            prepared["called"] = True

    fake_mod = SimpleNamespace(Prep=Prep)

    def import_mod(name: str) -> SimpleNamespace:
        del name
        return fake_mod

    registry.load_preparers(resources_mod=resources_ns, import_mod=import_mod)
    assert "expNoArg" in registry.PREPARERS
    registry.PREPARERS["expNoArg"]()
    assert prepared["called"] is True


def test_load_preparers_catches_per_entry_exception():
    """Test load preparers catches per entry exception."""
    registry.PREPARERS.clear()

    class _BoomEntry:
        def __init__(self):
            self.name = "boom"

        def is_dir(self):  # noqa: D401
            raise RuntimeError("boom")

    root = _FakeRoot([_BoomEntry(), _FakeEntry("ok", True, False)])

    def files(_: str) -> _FakeRoot:
        return root

    resources_ns = SimpleNamespace(files=files)
    # Should not raise
    registry.load_preparers(resources_mod=resources_ns)
    # No registrations since second entry had no preparer
    assert registry.PREPARERS == {}
