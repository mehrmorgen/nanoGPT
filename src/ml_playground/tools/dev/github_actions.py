from __future__ import annotations

from pathlib import Path

from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner


def run_github_actions(
    *,
    root_path: Path,
    subprocess_runner: SubprocessRunner,
    limit: int,
    run_id: int | None,
    latest: bool,
    log_failed: bool,
    remote: str,
    repo: str | None,
) -> ToolResult:
    operation_id = OperationId(namespace="tools", category="dev", command="gha")

    try:
        full_repo = repo or _infer_repo(
            root_path=root_path, subprocess_runner=subprocess_runner, remote=remote
        )

        if limit <= 0:
            limit = 10

        list_result = subprocess_runner.run_subprocess(
            [
                "gh",
                "run",
                "list",
                "--repo",
                full_repo,
                "-L",
                str(limit),
            ],
            cwd=root_path,
            operation_id=operation_id,
        )
        if not list_result.success:
            return list_result

        output_parts: list[str] = ["== gh run list ==", list_result.stdout.rstrip()]  # type: ignore[arg-type]

        resolved_run_id = run_id
        if resolved_run_id is None and latest:
            latest_result = subprocess_runner.run_subprocess(
                [
                    "gh",
                    "run",
                    "list",
                    "--repo",
                    full_repo,
                    "-L",
                    "1",
                    "--json",
                    "databaseId",
                    "-q",
                    ".[0].databaseId",
                ],
                cwd=root_path,
                operation_id=operation_id,
            )
            if not latest_result.success:
                return latest_result
            try:
                resolved_run_id = int(latest_result.stdout.strip())
            except ValueError:
                return ToolResult.create(
                    success=False,
                    exit_code=1,
                    namespace=operation_id.namespace,
                    category=operation_id.category,
                    command=operation_id.command,
                    stderr=(
                        "Failed to parse latest workflow run id from gh output: "
                        + latest_result.stdout.strip()
                    ),
                )

        if resolved_run_id is not None:
            view_cmd: list[str] = [
                "gh",
                "run",
                "view",
                str(resolved_run_id),
                "--repo",
                full_repo,
            ]
            if log_failed:
                view_cmd.append("--log-failed")

            view_result = subprocess_runner.run_subprocess(
                view_cmd,
                cwd=root_path,
                operation_id=operation_id,
            )
            if not view_result.success:
                return view_result

            output_parts.extend(["", "== gh run view ==", view_result.stdout.rstrip()])

        return ToolResult.create(
            success=True,
            exit_code=0,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stdout="\n".join([part for part in output_parts if part]),
        )
    except ToolExecutionError:
        raise
    except (OSError, ValueError) as exc:
        return ToolResult.create(
            success=False,
            exit_code=1,
            namespace=operation_id.namespace,
            category=operation_id.category,
            command=operation_id.command,
            stderr=f"Failed to query GitHub Actions via gh: {exc}",
        )


def _infer_repo(
    *, root_path: Path, subprocess_runner: SubprocessRunner, remote: str
) -> str:
    op_id = OperationId(namespace="tools", category="dev", command="gha-infer-repo")

    res = subprocess_runner.run_subprocess(
        ["git", "remote", "get-url", remote],
        cwd=root_path,
        operation_id=op_id,
    )
    if res.success and res.stdout.strip():
        url = res.stdout.strip()
        if url.startswith("git@"):  # git@github.com:owner/repo.git
            path = url.split(":", 1)[1]
        else:
            parts = url.split("github.com/")
            path = parts[1] if len(parts) > 1 else url
        path = path.rstrip(".git").strip("/")
        owner, name = path.split("/", 1)
        return f"{owner}/{name}"

    gh = subprocess_runner.run_subprocess(
        [
            "gh",
            "repo",
            "view",
            "--json",
            "owner,name",
            "-q",
            ".owner.login + '/' + .name",
        ],
        cwd=root_path,
        operation_id=op_id,
    )
    if not gh.success:
        raise ToolExecutionError(
            "Failed to infer repository",
            reason="git remote and gh repo view unavailable",
            rationale="Ensure GitHub CLI is installed and authenticated.",
        )
    return gh.stdout.strip()
