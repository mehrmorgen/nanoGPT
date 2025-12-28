import os
from pathlib import Path
from typing import Any, List
from unittest.mock import MagicMock, patch

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
    factory = MagicMock()
    factory.return_value.match_file.return_value = True
    factory.return_value.include = True
    gitignore.write_text("dummy\n", encoding="utf-8")
    # Use "/" to ensure we get base="" after rstrip
    gitignore_match(
        tmp_path, "/", directory=True, git_wild_match_pattern_factory=factory
    )
    calls = [call.args[0] for call in factory.return_value.match_file.call_args_list]
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
    factory = MagicMock()
    factory.return_value.match_file.return_value = True
    factory.return_value.include = True
    gitignore_match(
        tmp_path, "bar", directory=False, git_wild_match_pattern_factory=factory
    )
    factory.assert_called()

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
    mock_p = MagicMock(spec=Path)
    mock_p.exists.side_effect = OSError("denied")
    mock_p.suffix = ""
    ensure_dir(mock_p, False, logs=logs_ensure_base)
    mock_p.mkdir.assert_called()

    # 196->exit: touch if not exists
    p_touch = tmp_path / "ensure_dir_touch.txt"
    if p_touch.exists():
        p_touch.unlink()
    logs_touch: List[str] = []
    # Force exists() to return False then True after touch to hit line 196->exit
    # ensure_dir calls path.exists() at 183 and 196
    with patch.object(Path, "exists", side_effect=[False, False, True]):
        ensure_dir(p_touch, False, logs=logs_touch)
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
    with patch.object(Path, "exists", side_effect=OSError("err")):
        with patch.object(Path, "is_symlink", side_effect=OSError("err")):
            create_or_update_link(
                link_err, target, False, logs=logs_err, subprocess_runner=runner
            )
    assert link_err.is_symlink()

    # 239-240: OSError on target_is_dir
    logs_dir: List[str] = []
    with patch.object(Path, "is_dir", side_effect=OSError("err")):
        create_or_update_link(
            tmp_path / "link2_err",
            target,
            False,
            logs=logs_dir,
            subprocess_runner=runner,
        )
    assert (tmp_path / "link2_err").is_symlink()

    # 261-262: OSError on readlink
    logs_rl: List[str] = []
    link_f_rl = tmp_path / "link_f"
    link_f_rl.symlink_to(target)
    with patch.object(Path, "readlink", side_effect=OSError("err")):
        with patch.object(Path, "is_symlink", return_value=True):
            create_or_update_link(
                link_f_rl, target, False, logs=logs_rl, subprocess_runner=runner
            )


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
    mock_same = MagicMock(return_value=True)
    link_custom = tmp_path / "link_custom_u"
    link_custom.touch()
    # Using public parameters os_path_samefile and path_resolve
    logs_custom: list[str] = []
    create_or_update_link(
        link_custom,
        target,
        False,
        logs=logs_custom,
        subprocess_runner=runner,
        os_path_samefile=mock_same,
        path_resolve=lambda p: target.absolute(),
    )
    assert mock_same.called
    assert any("same path" in log for log in logs_custom)

    # 277-278: OSError on resolve
    logs_res: List[str] = []

    def mock_resolve_fail(p: Path) -> Path:
        raise OSError("err")

    with patch("os.path.samefile", side_effect=OSError("err")):
        create_or_update_link(
            link_custom,
            target,
            False,
            logs=logs_res,
            subprocess_runner=runner,
            path_resolve=mock_resolve_fail,
        )

    # 282: same but not link_exists
    link_ne = tmp_path / "not_here_u"
    # Manual symlink to trigger the branch
    os.symlink(str(target), str(link_ne))
    logs_ne: List[str] = []
    with patch.object(Path, "exists", return_value=False):
        with patch.object(Path, "is_symlink", return_value=True):
            with patch("os.path.samefile", return_value=True):
                create_or_update_link(
                    link_ne, target, False, logs=logs_ne, subprocess_runner=runner
                )
    assert any("link" in str(log_item) for log_item in logs_ne)

    # 287: same = True but mismatched representation
    logs_mis: List[str] = []
    link_mis = tmp_path / "link_mis_u"
    link_mis.symlink_to(tmp_path / "other")
    # force same=True to reach representation check (283-287)
    with patch("os.path.samefile", return_value=True):
        # Line 283-286: current_link_repr is compared with desired_link_repr
        # current_link_repr comes from readlink().as_posix()
        # desired_link_repr comes from relpath() (since it's not windows)
        with patch.object(Path, "readlink", return_value=Path("mismatch")):
            with patch("os.path.relpath", return_value="desired"):
                # We also need link_path.exists() to be True at 281
                with patch.object(Path, "exists", return_value=True):
                    create_or_update_link(
                        link_mis, target, False, logs=logs_mis, subprocess_runner=runner
                    )
    assert any("link" in str(log_it) for log_it in logs_mis)

    # 295: dry_run rm
    logs_dr: List[str] = []
    link_rm = tmp_path / "link_rm_u"
    link_rm.touch()
    create_or_update_link(link_rm, target, True, logs=logs_dr, subprocess_runner=runner)
    assert any("[dry-run] rm" in str(log) for log in logs_dr)

    # 305: remove empty dir
    link_dir = tmp_path / "empty_dir_u"
    link_dir.mkdir()
    create_or_update_link(link_dir, target, False, logs=[], subprocess_runner=runner)
    assert link_dir.is_symlink()

    # 307: missing_ok
    mock_rare = MagicMock(spec=Path)
    mock_rare.exists.return_value = True
    mock_rare.is_symlink.return_value = False
    mock_rare.is_file.return_value = False
    mock_rare.is_dir.return_value = False
    mock_rare.parent = tmp_path
    mock_rare.__str__ = MagicMock(return_value=str(tmp_path / "mock_rare_u"))
    logs_rare: List[str] = []
    create_or_update_link(
        mock_rare, target, False, logs=logs_rare, subprocess_runner=runner
    )
    getattr(mock_rare, "unlink").assert_called()

    # 309: OSError on removal
    mock_unrem = MagicMock(spec=Path)
    mock_unrem.exists.return_value = True
    mock_unrem.is_file.return_value = True
    mock_unrem.is_symlink.return_value = False
    mock_unrem.is_dir.return_value = False
    mock_unrem.parent = tmp_path
    getattr(mock_unrem, "unlink").side_effect = OSError("perm")
    mock_unrem.__str__ = MagicMock(return_value=str(tmp_path / "mock_unrem_u"))
    with pytest.raises(RuntimeError, match="failed to remove existing path"):
        create_or_update_link(
            mock_unrem, target, False, logs=[], subprocess_runner=runner
        )

    # 332: os.link success
    with patch("os.link") as mock_link:
        logs_hard: List[str] = []
        create_or_update_link(
            tmp_path / "hardlink_u",
            target,
            False,
            logs=logs_hard,
            subprocess_runner=runner,
            os_name="nt",
        )
        mock_link.assert_called()

    # 343-344: non-windows relpath ValueError during symlink
    logs_re: List[str] = []
    with patch("os.path.relpath", side_effect=ValueError("err")):
        create_or_update_link(
            tmp_path / "link_re_sym_u",
            target,
            False,
            logs=logs_re,
            subprocess_runner=runner,
            os_name="posix",
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
    mock_bad_s = MagicMock(spec=Path)
    mock_bad_s.symlink_to.side_effect = OSError("err")
    mock_bad_s.exists.return_value = False
    mock_bad_s.is_symlink.return_value = False
    mock_bad_s.parent = tmp_path
    mock_bad_s.__str__ = MagicMock(return_value=str(tmp_path / "mock_bad_s_u"))
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
    mock_link_op = MagicMock(side_effect=OSError("link fail"))
    with pytest.raises(RuntimeError, match="failed to create hardlink"):
        create_or_update_link(
            tmp_path / "h2",
            target,
            False,
            logs=[],
            subprocess_runner=runner,
            os_name="nt",
            os_link_op=mock_link_op,
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
    mock_fail_op = MagicMock(side_effect=RuntimeError("critical fail"))
    tools_fail = DevTools(
        root_path=tmp_path, ensure_base_and_empty_readme_op=mock_fail_op
    )
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
    with patch.object(Path, "readlink", side_effect=OSError("err")):
        tools = DevTools(root_path=tmp_path)
        # setup_ai_guidelines should swallow the OSError and continue
        tools.setup_ai_guidelines(tool="windsurf")
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
    mock_readme_op = MagicMock(return_value=tmp_path / "README.md")
    mock_link_op = MagicMock()

    # Passing custom operation callbacks is public API
    tools = DevTools(
        root_path=tmp_path,
        ensure_base_and_empty_readme_op=mock_readme_op,
        create_or_update_link_op=mock_link_op,
    )
    tools.setup_ai_guidelines(tool="windsurf")
    assert mock_readme_op.called
    assert mock_link_op.called
