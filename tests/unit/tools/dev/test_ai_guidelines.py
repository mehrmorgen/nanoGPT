import os
from pathlib import Path
from typing import Any, List
from contextlib import contextmanager

import pytest

from ml_playground.tools.core.interfaces import ToolResult
from ml_playground.tools.dev.dev import DevTools
from ml_playground.tools.dev.ai_guidelines import (
    mirror_tree,
    ensure_dir,
    create_or_update_link,
    gitignore_match,
    is_listed_in_aiignore,
    windows_create_junction,
    project_path,
    log_gitignore_status,
    log_aiignore_status,
)
from tests.unit.tools.fakes import FakeSubprocessRunner
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner


@contextmanager
def override_attr(target: object, name: str, value: object):
    missing = object()
    original = getattr(target, name, missing)
    object.__setattr__(target, name, value)
    try:
        yield
    finally:
        if original is not missing:
            object.__setattr__(target, name, original)
        else:
            delattr(target, name)


def test_project_path_valid(tmp_path: Path) -> None:
    """Test project_path with valid project-relative paths."""
    assert project_path(tmp_path, ".") == tmp_path
    assert project_path(tmp_path, "rel") == tmp_path / "rel"
    assert project_path(tmp_path, "a/b/c") == tmp_path / "a" / "b" / "c"


def test_project_path_invalid(tmp_path: Path) -> None:
    """Test project_path with invalid paths (83, 87)."""
    # 83: Absolute path
    with pytest.raises(ValueError, match="must be project-relative"):
        project_path(tmp_path, str(Path("/abs").absolute()))

    # 87: Parent directory references
    with pytest.raises(ValueError, match="parent directory references"):
        project_path(tmp_path, "../outside")
    with pytest.raises(ValueError, match="parent directory references"):
        project_path(tmp_path, "a/../../b")


def test_gitignore_match_public(tmp_path: Path) -> None:
    """Cover gitignore_match branches using public API (111->114, 121, 126, 129, 133)."""
    gitignore = tmp_path / ".gitignore"

    # 111-114: base logic
    # Trigger 111: directory=True, relative_path="foo" -> base="foo" -> candidates={"foo", "foo/"}
    # Pattern "foo/" matches "foo/"
    gitignore.write_text("foo/\n", encoding="utf-8")
    ignored, _ = gitignore_match(tmp_path, "foo", directory=True)
    assert ignored

    # Trigger 111-112: directory=True, relative_path="/" -> base=""
    # rstrip("/") on "/" returns "" -> base=""
    # base and not base.endswith("/") -> line 111: False
    # elif not base: -> line 112: True -> candidates.add("/")
    class DummyPattern:
        def __init__(self) -> None:
            self.include = True

        def match_file(self, path: str) -> bool:
            calls.append(path)
            return True

    calls: list[str] = []

    class DummyFactory:
        def __call__(self, pattern: str) -> DummyPattern:
            return DummyPattern()

    factory = DummyFactory()
    gitignore.write_text("dummy\n", encoding="utf-8")
    # Use "/" to ensure we get base="" after rstrip
    gitignore_match(
        tmp_path, "/", directory=True, git_wild_match_pattern_factory=factory
    )
    # candidates should contain "/" and possibly others depending on normalization
    assert "/" in calls or "" in calls

    # 122: skip comments and empty lines
    gitignore.write_text("\n# comment\nfoo\n", encoding="utf-8")
    ignored, _ = gitignore_match(tmp_path, "foo", directory=False)
    assert ignored

    # 121-122: Empty line and comment in .gitignore
    gitignore.write_text("\n# comment\n  # indented\nfoo\n", encoding="utf-8")
    ignored, matched = gitignore_match(tmp_path, "foo", directory=False)
    assert ignored
    assert matched == "foo"

    # 126: custom factory (Public API parameter)
    class DummyPattern2:
        def __init__(self) -> None:
            self.include = True

        def match_file(self, path: str) -> bool:
            calls2.append(path)
            return True

    class DummyFactory2:
        def __call__(self, pattern: str) -> DummyPattern2:
            return DummyPattern2()

    calls2: list[str] = []
    gitignore_match(
        tmp_path, "bar", directory=False, git_wild_match_pattern_factory=DummyFactory2()
    )
    assert calls2

    # 129-130: ValueError branch (malformed pattern)
    gitignore.write_text("[\n", encoding="utf-8")
    ignored, _ = gitignore_match(tmp_path, "any", directory=False)
    assert not ignored

    # Custom factory raising ValueError should be swallowed (129-130)
    def raising_factory(_: str) -> Any:
        raise ValueError("bad pattern")

    ignored, _ = gitignore_match(
        tmp_path, "any", directory=False, git_wild_match_pattern_factory=raising_factory
    )
    assert not ignored

    # 133: include branch (negated pattern)
    gitignore.write_text("!keep\n", encoding="utf-8")
    ignored, pattern = gitignore_match(tmp_path, "keep", directory=False)
    assert not ignored
    assert pattern == "!keep"


def test_is_listed_in_aiignore_public(tmp_path: Path) -> None:
    """Cover is_listed_in_aiignore branches via public API."""
    # 141: not exists
    assert not is_listed_in_aiignore(tmp_path, tmp_path / "any")

    # 153: no patterns after filtering
    aiignore = tmp_path / ".aiignore"
    aiignore.write_text("\n# comment\n", encoding="utf-8")
    assert not is_listed_in_aiignore(tmp_path, tmp_path / "any")

    # success
    aiignore.write_text("foo/\n", encoding="utf-8")
    assert is_listed_in_aiignore(tmp_path, tmp_path / "foo")


def test_windows_create_junction_logic(tmp_path: Path) -> None:
    """Cover windows_create_junction logic (175-176)."""
    runner = FakeSubprocessRunner()
    # success (175->exit)
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                namespace="tools",
                category="dev",
                command="setup-ai-guidelines-junction",
                stdout="",
            )
        ]
    )
    windows_create_junction(tmp_path / "link", tmp_path / "target", runner)
    assert len(runner.calls) == 1

    # failure (176)
    runner.set_results(
        [
            ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="dev",
                command="setup-ai-guidelines-junction",
                stderr="err",
            )
        ]
    )
    with pytest.raises(RuntimeError, match="failed to create junction"):
        windows_create_junction(tmp_path / "link2", tmp_path / "target2", runner)


def test_ensure_dir_public(tmp_path: Path) -> None:
    """Cover ensure_dir branches (185, 196)."""
    logs_ensure_base: List[str] = []

    # 185-186: OSError on path.exists()
    # We use a mock Path to trigger the branch through public entry point
    class DenyPath(Path):
        # Need to satisfy Path construction
        _flavour = Path(".")._flavour  # type: ignore[attr-defined]

        def exists(self) -> bool:  # type: ignore[override]
            raise OSError("denied")

        @property
        def suffix(self) -> str:  # type: ignore[override]
            return ""

        def mkdir(self, parents: bool = False, exist_ok: bool = False):  # type: ignore[override]
            logs_ensure_base.append("mkdir called")

    deny_path = DenyPath(tmp_path / "deny")
    ensure_dir(deny_path, False, logs=logs_ensure_base)
    assert any("mkdir" in log for log in logs_ensure_base)

    # 196->exit: touch if not exists
    p_touch = tmp_path / "ensure_dir_touch.txt"
    if p_touch.exists():
        p_touch.unlink()
    logs_touch: List[str] = []
    # Force exists() to return False then True after touch to hit line 196->exit
    # ensure_dir calls path.exists() at 183 and 196
    original_exists = getattr(Path, "exists")
    try:
        Path.exists = lambda self: False  # type: ignore[assignment]
        ensure_dir(p_touch, False, logs=logs_touch)
    finally:
        Path.exists = original_exists  # type: ignore[assignment]
    assert any("create" in str(log_item_final) for log_item_final in logs_touch)

    # 190-191: dry_run logs
    logs_dry: List[str] = []
    ensure_dir(tmp_path / "dry.txt", True, logs=logs_dry)
    assert any("[dry-run] touch" in log for log in logs_dry)
    logs_dry_dir: List[str] = []
    ensure_dir(tmp_path / "dry_dir", True, logs=logs_dry_dir)
    assert any("[dry-run] mkdir -p" in log for log in logs_dry_dir)


def test_create_or_update_link_os_errors(tmp_path: Path) -> None:
    """Cover OSError branches in create_or_update_link (232, 239, 261)."""
    target = tmp_path / "target"
    target.touch()
    logs_err: List[str] = []
    runner = FakeSubprocessRunner()

    # 232-234: OSError on link_path check
    link_err = tmp_path / "link_err"
    original_exists = getattr(Path, "exists")
    original_is_symlink = getattr(Path, "is_symlink")
    try:
        Path.exists = lambda self: (_ for _ in ()).throw(OSError("err"))  # type: ignore[assignment]
        Path.is_symlink = lambda self: (_ for _ in ()).throw(OSError("err"))  # type: ignore[assignment]
        create_or_update_link(
            link_err, target, False, logs=logs_err, subprocess_runner=runner
        )
    finally:
        Path.exists = original_exists  # type: ignore[assignment]
        Path.is_symlink = original_is_symlink  # type: ignore[assignment]
    assert link_err.is_symlink()

    # 239-240: OSError on target_is_dir
    logs_dir: List[str] = []
    original_is_dir = getattr(Path, "is_dir")
    try:
        Path.is_dir = lambda self: (_ for _ in ()).throw(OSError("err"))  # type: ignore[assignment]
        create_or_update_link(
            tmp_path / "link2_err",
            target,
            False,
            logs=logs_dir,
            subprocess_runner=runner,
        )
    finally:
        Path.is_dir = original_is_dir  # type: ignore[assignment]
    assert (tmp_path / "link2_err").is_symlink()

    # 261-262: OSError on readlink
    logs_rl: List[str] = []
    link_f_rl = tmp_path / "link_f"
    link_f_rl.symlink_to(target)
    original_readlink = getattr(Path, "readlink")
    original_is_symlink2 = getattr(Path, "is_symlink")
    try:
        Path.readlink = lambda self: (_ for _ in ()).throw(OSError("err"))  # type: ignore[assignment]
        Path.is_symlink = lambda self: True  # type: ignore[assignment]
        create_or_update_link(
            link_f_rl, target, False, logs=logs_rl, subprocess_runner=runner
        )
    finally:
        Path.readlink = original_readlink  # type: ignore[assignment]
        Path.is_symlink = original_is_symlink2  # type: ignore[assignment]
    assert (tmp_path / "link_f").is_symlink()

    # 268, 274: custom samefile/resolve
    def mock_same(a: Path | str, b: Path | str) -> bool:
        return True

    link_custom = tmp_path / "link_custom_u"
    link_custom.touch()
    logs_custom: List[str] = []
    # Using public parameters os_path_samefile and path_resolve
    create_or_update_link(
        link_custom,
        target,
        False,
        logs=logs_custom,
        subprocess_runner=runner,
        os_path_samefile=mock_same,
        path_resolve=lambda p: (_ for _ in ()).throw(OSError("err")),
    )

    # 282: same but not link_exists
    link_ne = tmp_path / "not_here_u"
    os.symlink(str(target), str(link_ne))

    logs_hard: List[str] = []
    create_or_update_link(
        tmp_path / "hardlink_u",
        target,
        False,
        logs=logs_hard,
        subprocess_runner=runner,
        os_link_op=os.link,
    )
    assert any("hardlink" in log for log in logs_hard)

    logs_re: List[str] = []
    create_or_update_link(
        tmp_path / "link_re_sym_u",
        target,
        False,
        logs=logs_re,
        subprocess_runner=runner,
        os_path_relpath=lambda p, start=None: (_ for _ in ()).throw(ValueError("err")),
    )
    assert (tmp_path / "link_re_sym_u").is_symlink()

    # 347: POSIX symlink to dir
    target_pd_u = tmp_path / "target_pd_u"
    target_pd_u.mkdir()
    create_or_update_link(
        tmp_path / "link_pd_u", target_pd_u, False, logs=[], subprocess_runner=runner
    )
    assert (tmp_path / "link_pd_u").is_symlink()

    # 351-352: POSIX symlink failure
    class BadSymlink(Path):
        _flavour = Path(".")._flavour  # type: ignore[attr-defined]

        def __new__(cls, p: Path) -> "BadSymlink":  # type: ignore[override]
            return Path.__new__(cls, p)

        def symlink_to(self, target: Any, target_is_directory: bool = False) -> None:  # type: ignore[override]
            _ = target_is_directory
            raise OSError("err")

        def exists(self) -> bool:  # type: ignore[override]
            return False

        def is_symlink(self) -> bool:  # type: ignore[override]
            return False

        def is_dir(self) -> bool:  # type: ignore[override]
            return False

    mock_bad_s = BadSymlink(tmp_path / "mock_bad_s_u")
    logs_bad_s: List[str] = []
    create_or_update_link(
        mock_bad_s, target, False, logs=logs_bad_s, subprocess_runner=runner
    )
    assert any("ERROR: failed to create symlink" in str(log) for log in logs_bad_s)


def test_create_or_update_link_integrated_public(tmp_path: Path) -> None:
    """Cover remaining create_or_update_link branches using public API parameters."""
    target = tmp_path / "target_u"
    target.touch()
    logs_u: List[str] = []
    runner = FakeSubprocessRunner()

    # 252: is_windows and target_is_dir
    target_dir = tmp_path / "target_dir_u"
    target_dir.mkdir()
    create_or_update_link(
        tmp_path / "link_win_dir_u",
        target_dir,
        True,
        logs=logs_u,
        subprocess_runner=runner,
        os_name="nt",
    )
    assert any("junction" in log for log in logs_u)

    # 268, 274: custom samefile/resolve
    def mock_same(a: Path | str, b: Path | str) -> bool:
        return True

    link_custom = tmp_path / "link_custom_u"
    link_custom.touch()

    # Using public parameters os_path_samefile and path_resolve
    def mock_resolve_fail(p: Path) -> Path:
        raise OSError("err")

    logs_custom: List[str] = []
    create_or_update_link(
        link_custom,
        target,
        False,
        logs=logs_custom,
        subprocess_runner=runner,
        os_path_samefile=mock_same,
        path_resolve=mock_resolve_fail,
    )

    # 282: same but not link_exists
    link_ne = tmp_path / "not_here_u"
    os.symlink(str(target), str(link_ne))

    logs_hard: List[str] = []
    create_or_update_link(
        tmp_path / "hardlink_u",
        target,
        False,
        logs=logs_hard,
        subprocess_runner=runner,
        os_link_op=os.link,
    )
    assert any("hardlink" in log for log in logs_hard)

    logs_re: List[str] = []
    create_or_update_link(
        tmp_path / "link_re_sym_u",
        target,
        False,
        logs=logs_re,
        subprocess_runner=runner,
        os_path_relpath=lambda p, start=None: (_ for _ in ()).throw(ValueError("err")),
    )
    assert (tmp_path / "link_re_sym_u").is_symlink()

    # 347: POSIX symlink to dir
    target_pd_u = tmp_path / "target_pd_u"
    target_pd_u.mkdir()
    create_or_update_link(
        tmp_path / "link_pd_u", target_pd_u, False, logs=[], subprocess_runner=runner
    )
    assert (tmp_path / "link_pd_u").is_symlink()

    # 351-352: POSIX symlink failure
    class BadSymlink(Path):
        _flavour = Path(".")._flavour  # type: ignore[attr-defined]

        def __new__(cls, p: Path) -> "BadSymlink":  # type: ignore[override]
            return Path.__new__(cls, p)

        def symlink_to(self, target: Any, target_is_directory: bool = False) -> None:  # type: ignore[override]
            _ = target_is_directory
            raise OSError("err")

        def exists(self) -> bool:  # type: ignore[override]
            return False

        def is_symlink(self) -> bool:  # type: ignore[override]
            return False

        def is_dir(self) -> bool:  # type: ignore[override]
            return False

    mock_bad_s = BadSymlink(tmp_path / "mock_bad_s_u")
    logs_bad_s: List[str] = []
    create_or_update_link(
        mock_bad_s, target, False, logs=logs_bad_s, subprocess_runner=runner
    )
    assert any("ERROR: failed to create symlink" in str(log) for log in logs_bad_s)


def test_setup_ai_guidelines_missing_branches(tmp_path: Path) -> None:
    """Cover remaining ai_guidelines.py branches (302, 316, 325, 330, 334, 416, 464, 494)."""
    runner = FakeSubprocessRunner()

    # 302: rmdir non-empty failure
    link_dir = tmp_path / "non_empty_dir"
    link_dir.mkdir()
    (link_dir / "file").touch()
    target = tmp_path / "target"
    target.touch()
    with pytest.raises(RuntimeError, match="Cannot replace non-empty directory"):
        create_or_update_link(
            link_dir, target, False, logs=[], subprocess_runner=runner
        )

    # 316: dry_run hardlink (windows)
    logs: list[str] = []
    create_or_update_link(
        tmp_path / "h", target, True, logs=logs, subprocess_runner=runner, os_name="nt"
    )
    assert any("hardlink" in log for log in logs)

    # 325: junction (windows)
    target_dir = tmp_path / "td"
    target_dir.mkdir()
    logs = []
    runner.set_results(
        [
            ToolResult.create(
                success=True,
                exit_code=0,
                namespace="tools",
                category="dev",
                command="setup-ai-guidelines-junction",
                stdout="",
            )
        ]
    )
    create_or_update_link(
        tmp_path / "j",
        target_dir,
        False,
        logs=logs,
        subprocess_runner=runner,
        os_name="nt",
    )
    assert any("junction" in log for log in logs)

    # 330, 334: os_link_op and OSError
    def failing_link_op(src: Path | str, dst: Path | str) -> None:
        raise OSError("link fail")

    with pytest.raises(RuntimeError, match="failed to create hardlink"):
        create_or_update_link(
            tmp_path / "h2",
            target,
            False,
            logs=logs,
            subprocess_runner=runner,
            os_link_op=failing_link_op,
        )

    # 416: log_gitignore_status ignored
    logs = []
    gitignore = tmp_path / ".gitignore"
    gitignore.write_text("ignored/\n")
    log_gitignore_status(tmp_path, tmp_path / "ignored", directory=True, logs=logs)
    assert any("ignored by pattern" in log for log in logs)

    # 464: unknown tool
    tools = DevTools(root_path=tmp_path)
    result = tools.setup_ai_guidelines(tool="unknown")
    assert not result.success
    assert "Unknown tool" in result.stdout

    # 494: single_file_root primary_path
    result_gemini = tools.setup_ai_guidelines(tool="gemini")
    assert result_gemini.success
    gemini_logs = (result_gemini.stdout or "").splitlines()
    assert any("single-file root" in str(log_item_gem) for log_item_gem in gemini_logs)

    # 607-608: exception catch in setup_ai_guidelines
    # We trigger an exception in setup_ai_guidelines by passing a non-existent tool
    # but that's handled at 464. To reach 607, we need something to fail inside the try block.
    # We'll mock ensure_base_and_empty_readme_op to raise an exception.
    def fail_op(path: Path, dry_run: bool, logs: list[str]) -> Path:
        raise RuntimeError("critical fail")

    tools_fail = DevTools(root_path=tmp_path, ensure_base_and_empty_readme_op=fail_op)
    result_crit = tools_fail.setup_ai_guidelines(tool="windsurf")
    assert result_crit.success is False
    assert "critical fail" in result_crit.stderr


def test_setup_ai_guidelines_cleanup_public(tmp_path: Path) -> None:
    """Cover cleanup logic branches (569, 574, 583)."""
    base = tmp_path / ".dev-guidelines"
    base.mkdir(exist_ok=True)
    (base / "README.md").touch()
    tool_dir = tmp_path / ".windsurf" / "rules"
    tool_dir.mkdir(parents=True, exist_ok=True)

    # 569: not a symlink check
    (tool_dir / "file").touch()

    # 574-576: OSError on readlink in cleanup
    link = tool_dir / "bad_read"
    link.symlink_to(base / "README.md")

    # 574: Catch OSError during readlink in cleanup
    original_readlink = getattr(Path, "readlink")
    try:
        Path.readlink = lambda self: (_ for _ in ()).throw(OSError("err"))  # type: ignore[assignment]
        tools = DevTools(root_path=tmp_path)
        # setup_ai_guidelines should swallow the OSError and continue
        tools.setup_ai_guidelines(tool="windsurf")
    finally:
        Path.readlink = original_readlink  # type: ignore[assignment]
    assert (tool_dir / "file").exists()
    assert link.is_symlink()

    # 585-586: actual cleanup (unlink)
    link_broken = tool_dir / "broken_unlink"
    # Create a symlink to something that doesn't exist within base_dir
    os.symlink(str(base / "nonexistent"), str(link_broken))
    # Confirm it's broken
    assert not link_broken.exists()
    assert link_broken.is_symlink()

    tools = DevTools(root_path=tmp_path)
    result = tools.setup_ai_guidelines(tool="windsurf")
    # Line 586: logs.append(f"clean  removed broken symlink {path}")
    assert any(
        "clean  removed broken symlink" in log for log in result.stdout.splitlines()
    )
    assert not link_broken.exists()

    # 583: dry_run cleanup log
    link2_cleanup = tool_dir / "broken"
    os.symlink(str(base / "missing"), str(link2_cleanup))
    tools_cleanup = DevTools(root_path=tmp_path)
    result_cleanup = tools_cleanup.setup_ai_guidelines(tool="windsurf", dry_run=True)
    logs_lines = (result_cleanup.stdout or "").splitlines()
    assert any("rm broken symlink" in str(log_item_cln) for log_item_cln in logs_lines)


def test_log_status_public(tmp_path: Path) -> None:
    """Cover log_gitignore_status and log_aiignore_status branches."""
    # log_gitignore_status negated pattern
    logs: list[str] = []
    gitignore = tmp_path / ".gitignore"
    gitignore.write_text("!foo\n")
    log_gitignore_status(tmp_path, tmp_path / "foo", directory=False, logs=logs)
    assert any("kept by negated pattern" in log for log in logs)

    # log_aiignore_status warning
    logs = []
    aiignore = tmp_path / ".aiignore"
    aiignore.write_text("foo/\n")
    log_aiignore_status(tmp_path, tmp_path / "foo", logs=logs)
    assert any("WARNING" in log for log in logs)


def test_mirror_tree_public(tmp_path: Path) -> None:
    """Cover mirror_tree logic."""
    src = tmp_path / "src_u"
    src.mkdir()
    (src / "f1_u").touch()
    dst = tmp_path / "dst_u"
    dst.mkdir()
    mirror_tree(
        tmp_path,
        src,
        dst,
        None,
        False,
        logs=[],
        subprocess_runner=FakeSubprocessRunner(),
    )
    assert (dst / "f1_u").exists()


def test_setup_ai_guidelines_integrated_public(tmp_path: Path) -> None:
    """Cover setup_ai_guidelines success path."""
    runner = FakeSubprocessRunner()
    tools = DevTools(subprocess_runner=runner, root_path=tmp_path)
    result = tools.setup_ai_guidelines(tool="windsurf", dry_run=True)
    assert result.success is True
    assert "[dry-run]" in result.stdout


def test_setup_ai_guidelines_custom_ops_public(tmp_path: Path) -> None:
    """Exercise custom operation parameters in setup_ai_guidelines."""

    def readme_op(path: Path, dry_run: bool, logs: list[str]) -> Path:
        logs.append("readme_op")
        return tmp_path / "README.md"

    def link_op(
        link_path: Path,
        target: Path,
        dry_run: bool,
        logs: list[str],
        subprocess_runner: SubprocessRunner,
    ) -> None:
        logs.append("link_op")

    # Passing custom operation callbacks is public API
    tools = DevTools(
        root_path=tmp_path,
        ensure_base_and_empty_readme_op=readme_op,
        ensure_aiignore_symlink_op=link_op,
    )
    tools.setup_ai_guidelines(tool="windsurf")
    assert "readme_op" in tools.setup_ai_guidelines(tool="windsurf").stdout
    assert "link_op" in tools.setup_ai_guidelines(tool="windsurf").stdout
