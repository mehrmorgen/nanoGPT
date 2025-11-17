"""Shared helpers for managing AI guideline materialization."""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

from pathspec import PathSpec
from pathspec.patterns.gitwildmatch import GitWildMatchPattern


@dataclass(frozen=True)
class ToolSpec:
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
    "codex": ToolSpec("AGENTS.md", ".", True),
}


@dataclass(frozen=True)
class SetupResult:
    """Outcome of running the AI guideline setup workflow."""

    success: bool
    logs: list[str]
    error: str | None = None


def run_setup_ai_guidelines(
    tool: str, project_dir: Path, *, dry_run: bool = False
) -> SetupResult:
    """Materialize .dev-guidelines links for the requested tool."""

    logs: list[str] = []

    def info(msg: str) -> None:
        logs.append(msg)

    def warn(msg: str) -> None:
        logs.append(f"WARNING: {msg}")

    def err(msg: str) -> None:
        logs.append(f"ERROR: {msg}")

    try:
        base_dir = project_dir / ".dev-guidelines"
        readme_name = "README.md"

        def ensure_dir(path: Path, dry: bool) -> None:
            try:
                if path.exists():
                    return
            except OSError:
                pass

            is_file_like = path.suffix != ""
            if dry:
                action = "touch" if is_file_like else "mkdir -p"
                info(f"[dry-run] {action} {path}")
                return
            if is_file_like:
                path.parent.mkdir(parents=True, exist_ok=True)
                if not path.exists():
                    path.touch()
                    info(f"create {path} (empty)")
            else:
                path.mkdir(parents=True, exist_ok=True)

        def ensure_base_and_empty_readme(dry: bool) -> Path:
            ensure_dir(base_dir, dry)
            readme = base_dir / readme_name
            ensure_dir(readme, dry)
            return readme

        def _windows_create_junction(link_path: Path, target_path: Path) -> None:
            cmd = ["cmd", "/c", "mklink", "/J", str(link_path), str(target_path)]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise RuntimeError(
                    "failed to create junction"
                    f" {link_path} -> {target_path}: {result.stderr.strip()}"
                )

        def create_or_update_link(link_path: Path, target_path: Path, dry: bool) -> None:
            link_exists = False
            link_is_symlink = False
            try:
                link_exists = link_path.exists()
                link_is_symlink = link_path.is_symlink()
            except OSError:
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

            if same and link_is_symlink and desired_link_repr is not None and current_link_repr:
                if current_link_repr.replace("\\", "/") != desired_link_repr.replace(
                    "\\", "/"
                ):
                    same = False

            if same:
                info(f"ok     {link_path} == {target_path} (same path)")
                return

            if link_exists or link_is_symlink:
                if dry:
                    info(f"[dry-run] rm {link_path}")
                else:
                    try:
                        if link_path.is_symlink() or link_path.is_file():
                            link_path.unlink()
                        elif link_path.is_dir():
                            if any(link_path.iterdir()):
                                raise RuntimeError(
                                    "Cannot replace non-empty directory"
                                    f" at {link_path}. Remove it first."
                                )
                            link_path.rmdir()
                        else:
                            link_path.unlink(missing_ok=True)  # type: ignore[arg-type]
                    except OSError as exc:
                        raise RuntimeError(
                            f"failed to remove existing path {link_path}: {exc}"
                        ) from exc

            ensure_dir(link_path.parent, dry)
            if dry:
                if is_windows and not target_is_dir:
                    info(f"[dry-run] hardlink {link_path} -> {target_path}")
                elif is_windows and target_is_dir:
                    info(f"[dry-run] junction {link_path} -> {target_path}")
                else:
                    info(f"[dry-run] ln -s {target_path} {link_path}")
                return

            if is_windows:
                if target_is_dir:
                    _windows_create_junction(link_path, target_path)
                    info(f"link   {link_path} => {target_path} (junction)")
                else:
                    try:
                        os.link(target_path, link_path)
                        info(f"link   {link_path} == {target_path} (hardlink)")
                    except OSError as exc:
                        message = (
                            f"failed to create hardlink {link_path} -> {target_path}: {exc}. "
                            "Ensure both paths are on the same volume."
                        )
                        raise RuntimeError(message) from exc
            else:
                rel = os.path.relpath(target_path, start=link_path.parent)
                try:
                    if target_is_dir:
                        link_path.symlink_to(rel, target_is_directory=True)
                    else:
                        link_path.symlink_to(rel)
                    info(f"link   {link_path} -> {target_path}")
                except OSError as exc:
                    err(f"failed to create symlink {link_path} -> {target_path}: {exc}")

        def mirror_tree(src_dir: Path, dest_dir: Path, exclude: set[Path] | None, dry: bool) -> None:
            if not src_dir.exists():
                return
            for entry in src_dir.iterdir():
                target = entry.resolve()
                if exclude and target in exclude:
                    continue
                dest_path = (dest_dir / entry.name).resolve()
                create_or_update_link(dest_path, target, dry)

        def _relative_tool_path(tool_dir: Path) -> str:
            return os.path.relpath(tool_dir, start=project_dir).replace(os.sep, "/")

        def _project_path(relative_path: str) -> Path:
            path = Path(relative_path)
            if path.is_absolute():
                raise ValueError(
                    "ToolSpec paths must be project-relative; "
                    f"got absolute '{relative_path}'."
                )
            if any(part == ".." for part in path.parts):
                raise ValueError(
                    "ToolSpec paths must not contain parent directory references: "
                    f"'{relative_path}'."
                )
            if path == Path("."):
                return project_dir
            return project_dir / path

        def _gitignore_match(relative_path: str, *, directory: bool) -> tuple[bool, str | None]:
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
                        pattern = GitWildMatchPattern(line)
                    except ValueError:
                        continue
                    if any(pattern.match_file(candidate) for candidate in candidates):
                        ignored = bool(pattern.include)
                        matched_pattern = line.strip()
            return ignored, matched_pattern

        def log_gitignore_status(path: Path, *, directory: bool) -> None:
            relative_path = _relative_tool_path(path).replace(os.sep, "/")
            display_path = relative_path.rstrip("/")
            if directory:
                display_path = (display_path + "/") if display_path else "/"
            ignored, matched_pattern = _gitignore_match(relative_path, directory=directory)
            if ignored:
                info(
                    "git    '"
                    f"{display_path}' ignored by pattern '"
                    f"{matched_pattern or '<unknown>'}'."
                )
                return
            if matched_pattern and matched_pattern.startswith("!"):
                info(
                    "git    '"
                    f"{display_path}' kept by negated pattern '"
                    f"{matched_pattern}'."
                )
                return
            warn(
                "git    '"
                f"{display_path}' is not ignored by .gitignore. Add an entry if you want"
                " Git to skip committing these files."
            )

        def is_listed_in_aiignore(tool_dir: Path) -> bool:
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
            relative_path = _relative_tool_path(tool_dir).rstrip("/")
            return bool(
                spec.match_file(relative_path)
                or spec.match_file(relative_path + "/")
            )

        def log_aiignore_status(tool_dir: Path) -> None:
            relative_path = _relative_tool_path(tool_dir).rstrip("/") + "/"
            if is_listed_in_aiignore(tool_dir):
                warn(
                    "ai     '"
                    f"{relative_path}' is excluded by .aiignore. Remove this entry so AI"
                    " tools can access their guidelines."
                )
            else:
                info(f"ai     '{relative_path}' accessible to AI tools")

        tool_key = tool.lower()
        if tool_key not in TOOL_MAP:
            message = f"Unknown tool '{tool}'. Supported: {', '.join(sorted(TOOL_MAP))}"
            err(message)
            return SetupResult(success=False, logs=logs, error=message)

        spec = TOOL_MAP[tool_key]
        readme = ensure_base_and_empty_readme(dry_run)
        tool_dir = project_dir if spec.single_file_root else _project_path(spec.root)
        ensure_dir(tool_dir, dry_run)

        if spec.single_file_root:
            primary_path = project_dir / Path(spec.primary_link).name
        else:
            primary_path = _project_path(spec.primary_link)
        ensure_dir(primary_path.parent, dry_run)
        create_or_update_link(primary_path, readme, dry=dry_run)

        if spec.single_file_root:
            log_gitignore_status(primary_path, directory=False)
            info(
                f"note   {tool_key} configured as single-file root; skipping tree mirroring."
            )
            info("done.")
            return SetupResult(success=True, logs=logs, error=None)

        exclude: set[Path] = {readme.resolve()}
        mirror_tree(base_dir.resolve(), tool_dir, exclude, dry_run)

        if tool_dir.exists():
            for path in tool_dir.rglob("*"):
                if not path.is_symlink():
                    continue
                try:
                    target = (path.parent / path.readlink()).resolve()
                except OSError:
                    target = None
                if (
                    target
                    and str(target).startswith(str(base_dir.resolve()))
                    and not target.exists()
                ):
                    if dry_run:
                        info(f"[dry-run] rm broken symlink {path} (-> {target})")
                    else:
                        path.unlink()
                        info(f"clean  removed broken symlink {path}")

        log_gitignore_status(tool_dir, directory=True)
        log_aiignore_status(tool_dir)
        info("done.")
        return SetupResult(success=True, logs=logs, error=None)
    except Exception as exc:  # pragma: no cover - caught by callers for ToolResult
        message = f"Failed to setup AI guidelines: {exc}"
        err(message)
        return SetupResult(success=False, logs=logs, error=message)
