from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable, List, Optional

from pydantic import BaseModel, ConfigDict
from pathspec import PathSpec
from pathspec.patterns.gitwildmatch import GitWildMatchPattern

from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.utils.subprocess_utils import (
    RealSubprocessRunner,
    SubprocessRunner,
)

README_NAME = "README.md"


class ToolSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    primary_link: str
    root: str
    single_file_root: bool = False


class SetupResult(BaseModel):
    """Structured result for setup_ai_guidelines."""

    model_config = ConfigDict(frozen=True)

    success: bool
    logs: List[str]
    error: Optional[str] = None


TOOL_MAP: dict[str, ToolSpec] = {
    "copilot": ToolSpec(
        primary_link=".github/copilot-instructions.md",
        root=".github",
    ),
    "aiassistant": ToolSpec(
        primary_link=".aiassistant/rules/00-README.md",
        root=".aiassistant/rules",
    ),
    "junie": ToolSpec(
        primary_link=".junie/guidelines.md",
        root=".junie",
    ),
    "kiro": ToolSpec(
        primary_link=".kiro/steering/product.md",
        root=".kiro/steering",
    ),
    "windsurf": ToolSpec(
        primary_link=".windsurf/rules/rule.md",
        root=".windsurf/rules",
    ),
    "cursor": ToolSpec(
        primary_link=".cursor/rules/00-readme.mdc",
        root=".cursor/rules",
    ),
    "gemini": ToolSpec(
        primary_link="GEMINI.md",
        root=".",
        single_file_root=True,
    ),
    "codex": ToolSpec(
        primary_link="AGENTS.md",
        root=".",
        single_file_root=True,
    ),
}


def relative_tool_path(project_dir: Path, tool_dir: Path) -> str:
    return os.path.relpath(tool_dir, start=project_dir).replace(os.sep, "/")


def project_path(project_dir: Path, relative_path: str) -> Path:
    path = Path(relative_path)
    if path.is_absolute():
        raise ValueError(
            f"ToolSpec paths must be project-relative; got absolute '{relative_path}'."
        )
    if any(part == ".." for part in path.parts):
        raise ValueError(
            f"ToolSpec paths must not contain parent directory references: '{relative_path}'."
        )
    if path == Path("."):
        return project_dir
    return project_dir / path


def gitignore_match(
    project_dir: Path,
    relative_path: str,
    *,
    directory: bool,
    git_wild_match_pattern_factory: Callable[[str], Any] | None = None,
) -> tuple[bool, str | None]:
    gitignore = project_dir / ".gitignore"
    if not gitignore.exists():
        return False, None

    candidates = {relative_path.replace(os.sep, "/")}
    if directory:
        base = relative_path.rstrip("/")
        if base and not base.endswith("/"):
            candidates.add(f"{base}/")
        elif not base:
            candidates.add("/")

    ignored = False
    matched_pattern: str | None = None

    with gitignore.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            stripped = line.lstrip()
            if not stripped or stripped.startswith("#"):
                continue

            try:
                if git_wild_match_pattern_factory:
                    pattern = git_wild_match_pattern_factory(line)
                else:
                    pattern = GitWildMatchPattern(line)
            except ValueError:
                continue

            if any(pattern.match_file(candidate) for candidate in candidates):
                ignored = bool(pattern.include)
                matched_pattern = line.strip()

    return ignored, matched_pattern


def is_listed_in_aiignore(project_dir: Path, tool_dir: Path) -> bool:
    ignore_path = project_dir / ".aiignore"
    if not ignore_path.exists():
        return False

    with ignore_path.open("r", encoding="utf-8") as handle:
        lines = [line.rstrip("\n") for line in handle]

    patterns = [
        line.strip()
        for line in lines
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not patterns:
        return False

    spec = PathSpec.from_lines("gitwildmatch", patterns)
    rel_path = relative_tool_path(project_dir, tool_dir).rstrip("/")
    return bool(spec.match_file(rel_path) or spec.match_file(rel_path + "/"))


def windows_create_junction(
    link_path: Path, target_path: Path, subprocess_runner: SubprocessRunner
) -> None:
    cmd = [
        "cmd",
        "/c",
        "mklink",
        "/J",
        str(link_path),
        str(target_path),
    ]
    operation_id = OperationId(
        namespace="tools", category="dev", command="setup-ai-guidelines-junction"
    )
    result = subprocess_runner.run_subprocess(cmd, operation_id=operation_id)
    if not result.success:
        raise RuntimeError(
            f"failed to create junction {link_path} -> {target_path}: {result.stderr.strip()}"
        )


def ensure_dir(path: Path, dry_run: bool, *, logs: list[str]) -> None:
    try:
        if path.exists():
            return
    except OSError:
        pass

    is_file_like = path.suffix != ""
    if dry_run:
        action = "touch" if is_file_like else "mkdir -p"
        logs.append(f"[dry-run] {action} {path}")
        return

    if is_file_like:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.touch()
            logs.append(f"create {path} (empty)")
    else:
        path.mkdir(parents=True, exist_ok=True)


def ensure_base_and_empty_readme(
    project_dir: Path, dry_run: bool, *, logs: list[str]
) -> Path:
    base_dir = project_dir / ".dev-guidelines"
    ensure_dir(base_dir, dry_run, logs=logs)
    readme = base_dir / README_NAME
    ensure_dir(readme, dry_run, logs=logs)
    return readme


def create_or_update_link(
    link_path: Path,
    target_path: Path,
    dry_run: bool,
    *,
    logs: list[str],
    subprocess_runner: SubprocessRunner,
    os_path_samefile: Callable[[Path | str, Path | str], bool] | None = None,
    path_resolve: Callable[[Path], Path] | None = None,
    path_readlink: Callable[[Path], Path] | None = None,
    os_relpath: Callable[[Path | str, Path | str], str] | None = None,
    os_name: str | None = None,
    os_link_op: Callable[[Path | str, Path | str], None] | None = None,
) -> None:
    link_exists = False
    link_is_symlink = False
    try:
        link_exists = link_path.exists()
        link_is_symlink = link_path.is_symlink()
    except OSError:
        link_exists = False
        link_is_symlink = False

    is_windows = (os_name or os.name) == "nt"
    try:
        target_is_dir = target_path.is_dir()
    except OSError:
        target_is_dir = False

    desired_link_repr: str | None = None
    if not is_windows:
        try:
            if os_relpath:
                desired_link_repr = os_relpath(target_path, link_path.parent)
            else:
                desired_link_repr = os.path.relpath(target_path, start=link_path.parent)
        except ValueError:
            desired_link_repr = str(target_path)
    elif target_is_dir:
        desired_link_repr = str(target_path)

    current_link_repr: str | None = None
    if link_is_symlink:
        try:
            if path_readlink:
                current_link_repr = path_readlink(link_path).as_posix()
            else:
                current_link_repr = link_path.readlink().as_posix()
        except OSError:
            current_link_repr = None

    same = False
    if link_exists or (link_is_symlink and current_link_repr is not None):
        try:
            if os_path_samefile:
                same = os_path_samefile(link_path, target_path)
            else:
                same = os.path.samefile(link_path, target_path)
        except OSError:
            try:
                if path_resolve:
                    same = path_resolve(link_path) == path_resolve(target_path)
                else:
                    same = link_path.resolve() == target_path.resolve()
            except OSError:
                same = False

    if same and link_is_symlink:
        if not link_exists:
            same = False
        elif desired_link_repr is not None and current_link_repr is not None:
            if current_link_repr.replace("\\", "/") != desired_link_repr.replace(
                "\\", "/"
            ):
                same = False

    if same:
        logs.append(f"ok     {link_path} == {target_path} (same path)")
        return

    if link_exists or link_is_symlink:
        if dry_run:
            logs.append(f"[dry-run] rm {link_path}")
        else:
            try:
                if link_path.is_symlink() or link_path.is_file():
                    link_path.unlink()
                elif link_path.is_dir():
                    if any(link_path.iterdir()):
                        raise RuntimeError(
                            f"Cannot replace non-empty directory at {link_path}. Remove it first."
                        )
                    link_path.rmdir()
                else:
                    link_path.unlink(missing_ok=True)  # type: ignore[arg-type]
            except OSError as e:
                raise RuntimeError(
                    f"failed to remove existing path {link_path}: {e}"
                ) from e

    ensure_dir(link_path.parent, dry_run, logs=logs)
    if dry_run:
        if is_windows and not target_is_dir:
            logs.append(f"[dry-run] hardlink {link_path} -> {target_path}")
        elif is_windows and target_is_dir:
            logs.append(f"[dry-run] junction {link_path} -> {target_path}")
        else:
            logs.append(f"[dry-run] ln -s {target_path} {link_path}")
        return

    if is_windows:
        if target_is_dir:
            windows_create_junction(link_path, target_path, subprocess_runner)
            logs.append(f"link   {link_path} => {target_path} (junction)")
        else:
            try:
                if os_link_op:
                    os_link_op(target_path, link_path)
                else:
                    os.link(target_path, link_path)
                logs.append(f"link   {link_path} == {target_path} (hardlink)")
            except OSError as e:
                message = (
                    f"failed to create hardlink {link_path} -> {target_path}: {e}. "
                    "Ensure both paths are on the same volume."
                )
                raise RuntimeError(message) from e
    else:
        try:
            rel = os.path.relpath(target_path, start=link_path.parent)
        except ValueError:
            rel = str(target_path)
        try:
            if target_is_dir:
                link_path.symlink_to(rel, target_is_directory=True)
            else:
                link_path.symlink_to(rel)
            logs.append(f"link   {link_path} -> {target_path}")
        except OSError as e:
            logs.append(
                f"ERROR: failed to create symlink {link_path} -> {target_path}: {e}"
            )


def mirror_tree(
    project_dir: Path,
    src_dir: Path,
    dest_dir: Path,
    exclude: set[Path] | None,
    dry_run: bool,
    *,
    logs: list[str],
    subprocess_runner: SubprocessRunner,
    os_path_samefile: Callable[[Path | str, Path | str], bool] | None = None,
    path_resolve: Callable[[Path], Path] | None = None,
    path_readlink: Callable[[Path], Path] | None = None,
    os_relpath: Callable[[Path | str, Path | str], str] | None = None,
    os_name: str | None = None,
    os_link_op: Callable[[Path | str, Path | str], None] | None = None,
) -> None:
    if not src_dir.exists():
        return
    for entry in src_dir.iterdir():
        target = entry.resolve()
        if exclude and target in exclude:
            continue
        dest_path = (dest_dir / entry.name).resolve()
        create_or_update_link(
            dest_path,
            target,
            dry_run,
            logs=logs,
            subprocess_runner=subprocess_runner,
            os_path_samefile=os_path_samefile,
            path_resolve=path_resolve,
            path_readlink=path_readlink,
            os_relpath=os_relpath,
            os_name=os_name,
            os_link_op=os_link_op,
        )


def log_gitignore_status(
    project_dir: Path,
    tool_dir: Path,
    *,
    directory: bool,
    logs: list[str],
    git_wild_match_pattern_factory: Callable[[str], Any] | None = None,
) -> None:
    rel_path = relative_tool_path(project_dir, tool_dir).replace(os.sep, "/")
    display_path = rel_path.rstrip("/")
    if directory:
        display_path = (display_path + "/") if display_path else "/"

    ignored, matched_pattern = gitignore_match(
        project_dir,
        rel_path,
        directory=directory,
        git_wild_match_pattern_factory=git_wild_match_pattern_factory,
    )

    if ignored:
        logs.append(
            f"git    '{display_path}' ignored by pattern '{matched_pattern or '<unknown>'}'."
        )
        return

    if matched_pattern and matched_pattern.startswith("!"):
        logs.append(
            f"git    '{display_path}' kept by negated pattern '{matched_pattern}'."
        )
        return

    logs.append(
        f"WARNING: git    '{display_path}' is not ignored by .gitignore. Add an entry if you want Git to skip committing these files."
    )


def log_aiignore_status(project_dir: Path, tool_dir: Path, *, logs: list[str]) -> None:
    display_path = relative_tool_path(project_dir, tool_dir).rstrip("/") + "/"
    if is_listed_in_aiignore(project_dir, tool_dir):
        logs.append(
            f"WARNING: ai     '{display_path}' is excluded by .aiignore. Remove this entry so AI tools can access their guidelines."
        )
    else:
        logs.append(f"ai     '{display_path}' accessible to AI tools")


def setup_ai_guidelines(
    *,
    tool: str,
    project_dir: Path,
    dry_run: bool = False,
    subprocess_runner: SubprocessRunner | None = None,
    os_path_samefile: Callable[[Path | str, Path | str], bool] | None = None,
    path_resolve: Callable[[Path], Path] | None = None,
    path_readlink: Callable[[Path], Path] | None = None,
    os_relpath: Callable[[Path | str, Path | str], str] | None = None,
    os_name: str | None = None,
    git_wild_match_pattern_factory: Callable[[str], Any] | None = None,
    create_or_update_link_op: Callable[..., None] | None = None,
    ensure_base_and_empty_readme_op: Callable[..., Path] | None = None,
    os_link_op: Callable[[Path | str, Path | str], None] | None = None,
) -> ToolResult:
    logs: list[str] = []
    runner = subprocess_runner or RealSubprocessRunner()

    try:
        tool_key = tool.lower()
        if tool_key not in TOOL_MAP:
            logs.append(f"ERROR: Unknown tool '{tool}'")
            return ToolResult.create(
                success=False,
                exit_code=1,
                namespace="tools",
                category="dev",
                command="setup-ai-guidelines",
                stderr=logs[-1],
                stdout="\n".join(logs),
            )

        spec = TOOL_MAP[tool_key]
        base_dir = project_dir / ".dev-guidelines"

        # 1) Ensure base + empty README
        if ensure_base_and_empty_readme_op:
            readme = ensure_base_and_empty_readme_op(project_dir, dry_run, logs=logs)
        else:
            readme = ensure_base_and_empty_readme(project_dir, dry_run, logs=logs)

        # 2) Ensure tool directory exists
        tool_dir = (
            project_dir
            if spec.single_file_root
            else project_path(project_dir, spec.root)
        )
        ensure_dir(tool_dir, dry_run, logs=logs)

        # 3) Create primary link from tool map path to README
        if spec.single_file_root:
            primary_path = project_dir / Path(spec.primary_link).name
        else:
            primary_path = project_path(project_dir, spec.primary_link)
        ensure_dir(primary_path.parent, dry_run, logs=logs)

        if create_or_update_link_op:
            create_or_update_link_op(
                primary_path,
                readme,
                dry_run,
                logs=logs,
                subprocess_runner=runner,
                os_path_samefile=os_path_samefile,
                path_resolve=path_resolve,
                path_readlink=path_readlink,
                os_relpath=os_relpath,
                os_name=os_name,
            )
        else:
            create_or_update_link(
                primary_path,
                readme,
                dry_run,
                logs=logs,
                subprocess_runner=runner,
                os_path_samefile=os_path_samefile,
                path_resolve=path_resolve,
                path_readlink=path_readlink,
                os_relpath=os_relpath,
                os_name=os_name,
            )

        if spec.single_file_root:
            log_gitignore_status(
                project_dir,
                primary_path,
                directory=False,
                logs=logs,
                git_wild_match_pattern_factory=git_wild_match_pattern_factory,
            )
            logs.append(
                f"note   {tool_key} configured as single-file root; skipping tree mirroring."
            )
            logs.append("done.")
            return ToolResult.create(
                success=True,
                exit_code=0,
                namespace="tools",
                category="dev",
                command="setup-ai-guidelines",
                stdout="\n".join(logs),
            )

        # 4) Mirror entire BASE_DIR contents into tool_dir (exclude README)
        exclude: set[Path] = {readme.resolve()}
        mirror_tree(
            project_dir,
            base_dir.resolve(),
            tool_dir,
            exclude,
            dry_run,
            logs=logs,
            subprocess_runner=runner,
            os_path_samefile=os_path_samefile,
            path_resolve=path_resolve,
            path_readlink=path_readlink,
            os_relpath=os_relpath,
            os_name=os_name,
            os_link_op=os_link_op,
        )

        # 5) Clean broken symlinks pointing into BASE_DIR within tool's directory
        if tool_dir.exists():
            for path in tool_dir.rglob("*"):
                if not path.is_symlink():
                    continue
                try:
                    if path_readlink:
                        target = (path.parent / path_readlink(path)).resolve()
                    else:
                        target = (path.parent / path.readlink()).resolve()
                except OSError:
                    target = None
                if (
                    target
                    and str(target).startswith(str(base_dir.resolve()))
                    and not target.exists()
                ):
                    if dry_run:
                        logs.append(f"[dry-run] rm broken symlink {path} (-> {target})")
                    else:
                        path.unlink()
                        logs.append(f"clean  removed broken symlink {path}")

        # 6) Report ignore status for the tool's directory
        log_gitignore_status(
            project_dir,
            tool_dir,
            directory=True,
            logs=logs,
            git_wild_match_pattern_factory=git_wild_match_pattern_factory,
        )
        log_aiignore_status(project_dir, tool_dir, logs=logs)

        logs.append("done.")
        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace="tools",
            category="dev",
            command="setup-ai-guidelines",
            stdout="\n".join(logs),
        )
    except Exception as exc:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace="tools",
            category="dev",
            command="setup-ai-guidelines",
            stderr=f"Failed to setup AI guidelines: {exc}",
        )
