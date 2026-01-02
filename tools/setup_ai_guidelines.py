#!/usr/bin/env python3
from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from pathspec import PathSpec
from pathspec.patterns.gitwildmatch import GitWildMatchPattern
import typer

app = typer.Typer(add_completion=False)

# ---- Constants ----
README_NAME = "README.md"
PROJECT_DIR = Path(__file__).resolve().parent.parent
SCRIPT_DIR = PROJECT_DIR / "tools"
BASE_DIR = PROJECT_DIR / ".dev-guidelines"


# Tool configuration:
#   maps tool name -> ToolSpec describing the primary README.md link target and
#   whether the target should mirror the full guidelines directory tree.
#   All paths are relative to PROJECT_DIR to keep locations anchored at repo root.
@dataclass(frozen=True)
class ToolSpec:
    """Description of the filesystem layout for an AI tool."""

    primary_link: str
    root: str
    single_file_root: bool = False


TOOL_MAP: dict[str, ToolSpec] = {
    "copilot": ToolSpec(".github/copilot-instructions.md", ".github"),
    "aiassistant": ToolSpec(".aiassistant/rules/00-README.md", ".aiassistant/rules"),
    "junie": ToolSpec(".junie/guidelines.md", ".junie"),
    "kiro": ToolSpec(".kiro/steering/product.md", ".kiro/steering"),
    "windsurf": ToolSpec(".windsurf/rules/rule.md", ".windsurf/rules"),
    "cursor": ToolSpec(".cursor/rules/00-readme.mdc", ".cursor/rules"),
    "gemini": ToolSpec("GEMINI.md", ".", True),
    "codex": ToolSpec("AGENTS.md", ".", True),  # https://agents.md
}


# ---- Helpers ----
def info(msg: str) -> None:
    typer.echo(msg)


def warn(msg: str) -> None:
    typer.echo(typer.style(f"WARNING: {msg}", fg=typer.colors.YELLOW))


def err(msg: str) -> None:
    typer.echo(typer.style(f"ERROR: {msg}", fg=typer.colors.RED))


def ensure_dir(path: Path, dry_run: bool) -> None:
    """Ensure a path exists, creating it if necessary.

    Automatically detects if the path is a file or directory based on whether it has a file extension.

    Args:
        path: The path to ensure.
        dry_run: If True, only print the action without executing.
    """
    # If it already exists, nothing to do
    try:
        if path.exists():
            return
    except OSError:
        # On some platforms calling exists() can raise for problematic paths; fall through to create logic
        pass

    # Heuristic: treat as file if it has a suffix (e.g., "README.md"); directories like ".github" have no suffix
    is_file_like = path.suffix != ""

    if dry_run:
        action = "touch" if is_file_like else "mkdir -p"
        info(f"[dry-run] {action} {path}")
        return

    if is_file_like:
        # Ensure parent directory first, then create an empty file
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.touch()
            info(f"create {path} (empty)")
    else:
        path.mkdir(parents=True, exist_ok=True)


def ensure_base_and_empty_readme(dry_run: bool) -> Path:
    """Ensure the base directory and empty README file exist.

    Args:
        dry_run: If True, only print actions without executing.

    Returns:
        Path to the README file.
    """
    ensure_dir(BASE_DIR, dry_run)
    readme = BASE_DIR / README_NAME
    ensure_dir(readme, dry_run)
    return readme


def _windows_create_junction(link_path: Path, target_path: Path) -> None:
    """Create a directory junction on Windows using mklink /J.

    link_path must not already exist.
    """
    # Use absolute paths for mklink
    cmd = [
        "cmd",
        "/c",
        "mklink",
        "/J",
        str(link_path),
        str(target_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"failed to create junction {link_path} -> {target_path}: {result.stderr.strip()}"
        )


def create_or_update_link(link_path: Path, target_path: Path, dry_run: bool) -> None:
    """
    Create/refresh a link from link_path to target_path.

    Policy:
    - Windows: files -> hardlinks; directories -> junctions.
    - Unix-like: symlinks for both files and directories (relative path when feasible).
    - Strict: if an incompatible existing path is present, raise instead of silently copying.
    """
    link_exists = False
    link_is_symlink = False
    try:
        link_exists = link_path.exists()
        link_is_symlink = link_path.is_symlink()
    except OSError:
        # On Windows, certain paths can raise when probing; assume non-existent
        link_exists = False
        link_is_symlink = False

    is_windows = os.name == "nt"
    try:
        target_is_dir = target_path.is_dir()
    except OSError:
        target_is_dir = False

    desired_link_repr: str | None = None
    if not is_windows:
        try:
            desired_link_repr = os.path.relpath(target_path, start=link_path.parent)
        except ValueError:
            desired_link_repr = str(target_path)
    elif target_is_dir:
        desired_link_repr = str(target_path)

    current_link_repr: str | None = None
    if link_is_symlink:
        try:
            current_link_repr = link_path.readlink().as_posix()
        except OSError:
            current_link_repr = None

    same = False
    if link_exists or (link_is_symlink and current_link_repr):
        try:
            same = os.path.samefile(link_path, target_path)
        except OSError:
            try:
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
        info(f"ok     {link_path} == {target_path} (same path)")
        return

    # If something exists at link_path, handle per OS policy
    if link_exists or link_is_symlink:
        if dry_run:
            info(f"[dry-run] rm {link_path}")
            # Continue to create link below in dry-run mode
        else:
            # Remove existing link/symlink/file; do not recursively delete directories
            try:
                if link_path.is_symlink() or link_path.is_file():
                    link_path.unlink()
                elif link_path.is_dir():
                    # Only allow removing empty dir to avoid destructive behavior
                    if any(link_path.iterdir()):
                        raise RuntimeError(
                            f"Cannot replace non-empty directory at {link_path}. Remove it first."
                        )
                    link_path.rmdir()
                else:
                    # Unknown type; attempt unlink
                    link_path.unlink(missing_ok=True)  # type: ignore[arg-type]
            except OSError as e:
                raise RuntimeError(
                    f"failed to remove existing path {link_path}: {e}"
                ) from e

    ensure_dir(link_path.parent, dry_run)
    if dry_run:
        if is_windows and not target_is_dir:
            info(f"[dry-run] hardlink {link_path} -> {target_path}")
        elif is_windows and target_is_dir:
            info(f"[dry-run] junction {link_path} -> {target_path}")
        else:
            info(f"[dry-run] ln -s {target_path} {link_path}")
        return

    # Create per-OS
    if is_windows:
        if target_is_dir:
            # Directory junction
            try:
                _windows_create_junction(link_path, target_path)
                info(f"link   {link_path} => {target_path} (junction)")
            except RuntimeError:
                raise
        else:
            # File hardlink (requires same volume)
            try:
                os.link(target_path, link_path)
                info(f"link   {link_path} == {target_path} (hardlink)")
            except OSError as e:
                raise RuntimeError(
                    f"failed to create hardlink {link_path} -> {target_path}: {e}. "
                    "Ensure both paths are on the same volume."
                ) from e
    else:
        # Unix-like: symlink for both files and directories (use relative path)
        rel = os.path.relpath(target_path, start=link_path.parent)
        try:
            if target_is_dir:
                link_path.symlink_to(rel, target_is_directory=True)
            else:
                link_path.symlink_to(rel)
            info(f"link   {link_path} -> {target_path}")
        except OSError as e:
            err(f"failed to create symlink {link_path} -> {target_path}: {e}")


def mirror_tree(
    src_dir: Path, dest_dir: Path, exclude: set[Path] | None, dry_run: bool
) -> None:
    """Mirror src_dir into dest_dir by linking each top-level entry.

    - Windows: files -> hardlinks, directories -> junctions.
    - Unix: symlinks for both.
    - Exclude: set of absolute paths under src_dir to skip.

    Note: We do NOT recurse into directories. A directory link replaces recursion and avoids
    creating links inside linked directories (which could mutate the source via junctions).
    """
    if not src_dir.exists():
        return
    for entry in src_dir.iterdir():
        target = entry.resolve()
        if exclude and target in exclude:
            continue
        dest_path = (dest_dir / entry.name).resolve()
        create_or_update_link(dest_path, target, dry_run)


def clean_broken_symlinks_pointing_into_base(scan_dir: Path, dry_run: bool) -> None:
    """
    Remove any symlink under scan_dir that points into BASE_DIR but whose target is missing.
    """
    if not scan_dir.exists():
        return
    for path in scan_dir.rglob("*"):
        if not path.is_symlink():
            continue
        try:
            target = (path.parent / path.readlink()).resolve()
        except OSError:
            target = None
        if (
            target
            and str(target).startswith(str(BASE_DIR.resolve()))
            and not target.exists()
        ):
            if dry_run:
                info(f"[dry-run] rm broken symlink {path} (-> {target})")
            else:
                path.unlink()
                info(f"clean  removed broken symlink {path}")


def _relative_tool_path(tool_dir: Path) -> str:
    """Return the project-relative path for the given tool directory."""

    return os.path.relpath(tool_dir, start=PROJECT_DIR).replace(os.sep, "/")


def _project_path(relative_path: str) -> Path:
    """Join a project-relative path to PROJECT_DIR, forbidding escapes."""

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
        return PROJECT_DIR
    return PROJECT_DIR / path


def _gitignore_match(relative_path: str, *, directory: bool) -> tuple[bool, str | None]:
    """Return whether a path is ignored and the last matching pattern."""

    gitignore = PROJECT_DIR / ".gitignore"
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
                pattern = GitWildMatchPattern(line)
            except ValueError:
                continue

            if any(pattern.match_file(candidate) for candidate in candidates):
                ignored = pattern.include
                matched_pattern = line.strip()

    return ignored, matched_pattern


def log_gitignore_status(path: Path, *, directory: bool) -> None:
    """Log when a path is not ignored by Git, accounting for negated patterns."""

    relative_path = _relative_tool_path(path).replace(os.sep, "/")
    display_path = relative_path.rstrip("/")
    if directory:
        display_path = (display_path + "/") if display_path else "/"

    ignored, matched_pattern = _gitignore_match(relative_path, directory=directory)

    if ignored:
        info(
            "git    '%s' ignored by pattern '%s'."
            % (display_path, matched_pattern or "<unknown>")
        )
        return

    if matched_pattern and matched_pattern.startswith("!"):
        info(
            "git    '%s' kept by negated pattern '%s'."
            % (display_path, matched_pattern)
        )
        return

    warn(
        "git    '%s' is not ignored by .gitignore. Add an entry if you want Git to "
        "skip committing these files." % display_path
    )


def is_listed_in_aiignore(tool_dir: Path) -> bool:
    """Check if the given directory is matched by patterns in .aiignore."""

    ignore_path = PROJECT_DIR / ".aiignore"
    if not ignore_path.exists():
        return False

    with ignore_path.open("r", encoding="utf-8") as f:
        lines = [line.rstrip("\n") for line in f]

    patterns = [
        line.strip()
        for line in lines
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not patterns:
        return False

    spec = PathSpec.from_lines("gitwildmatch", patterns)
    relative_path = _relative_tool_path(tool_dir).rstrip("/")
    return bool(spec.match_file(relative_path) or spec.match_file(relative_path + "/"))


def log_aiignore_status(tool_dir: Path) -> None:
    """Warn when the tool directory is excluded via .aiignore."""

    relative_path = _relative_tool_path(tool_dir).rstrip("/") + "/"
    if is_listed_in_aiignore(tool_dir):
        warn(
            "ai     '%s' is excluded by .aiignore. Remove this entry so AI tools can "
            "access their guidelines." % relative_path
        )
    else:
        info(f"ai     '{relative_path}' accessible to AI tools")


# ---- Command ----
@app.command()
def setup(
    tool: str = typer.Argument(
        ..., help=f"One of: {', '.join(sorted(TOOL_MAP.keys()))}"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run/--no-dry-run", help="Show actions without making changes."
    ),
):
    """
    Create symlinks for all files in the base directory to the tool's directory.
    Paths are resolved from the project (tools) directory.
    """
    tool = tool.lower()
    if tool not in TOOL_MAP:
        err(f"Unknown tool '{tool}'. Supported: {', '.join(sorted(TOOL_MAP))}")
        raise typer.Exit(code=1)

    spec = TOOL_MAP[tool]

    # 1) Ensure base + empty README
    readme = ensure_base_and_empty_readme(dry_run)

    # 2) Ensure tool's directory exists (under PROJECT_DIR)
    tool_dir = PROJECT_DIR if spec.single_file_root else _project_path(spec.root)
    ensure_dir(tool_dir, dry_run)

    # 3) Create primary link from tool map path to README
    if spec.single_file_root:
        primary_path = PROJECT_DIR / Path(spec.primary_link).name
    else:
        primary_path = _project_path(spec.primary_link)
    ensure_dir(primary_path.parent, dry_run)
    create_or_update_link(primary_path, readme, dry_run=dry_run)

    if spec.single_file_root:
        log_gitignore_status(primary_path, directory=False)
        info(f"note   {tool} configured as single-file root; skipping tree mirroring.")
        info("done.")
        return

    # 4) Mirror entire BASE_DIR contents into tool_dir (recursive)
    # Exclude the README file to avoid duplication, since it already has a primary mapping above
    exclude: set[Path] = {readme.resolve()}
    mirror_tree(BASE_DIR.resolve(), tool_dir, exclude, dry_run)

    # 5) Clean broken symlinks pointing into BASE_DIR within tool's directory
    clean_broken_symlinks_pointing_into_base(tool_dir, dry_run)

    # 6) Report ignore status for the tool's directory
    log_gitignore_status(tool_dir, directory=True)
    log_aiignore_status(tool_dir)

    info("done.")


if __name__ == "__main__":
    app()
