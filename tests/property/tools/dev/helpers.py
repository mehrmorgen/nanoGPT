from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator

from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.cli.dependencies import (
    default_tools_dependencies,
    ToolsDependencies,
    override_tools_dependencies,
)


@dataclass
class RecordedCall:
    kind: str
    args: list[str]
    cwd: str | Path | None
    env: dict[str, str]
    timeout: int | None
    operation_id: OperationId
    extra: dict[str, object]


class DeterministicRunner:
    """Subprocess runner stub satisfying the SubprocessRunner protocol."""

    def __init__(
        self,
        *,
        result_factory: Callable[[OperationId], ToolResult] | None = None,
    ) -> None:
        self._default_factory = result_factory or self._default_result
        self._result_queue: list[Callable[[OperationId], ToolResult]] = []
        self.calls: list[RecordedCall] = []

    # ------------------------------------------------------------------
    # SubprocessRunner protocol methods
    # ------------------------------------------------------------------
    def run_subprocess(
        self,
        command: list[str],
        *,
        cwd: str | Path | None = None,
        env: dict[str, str] | None = None,
        timeout: int | None = None,
        operation_id: OperationId,
        capture_output: bool = True,
    ) -> ToolResult:
        result = self._resolve_result(operation_id)
        self._record(
            kind="subprocess",
            args=command,
            cwd=cwd,
            env=env,
            timeout=timeout,
            operation_id=operation_id,
            extra={"capture_output": capture_output},
        )
        return result

    def run_uv_command(
        self,
        args: list[str],
        *,
        cwd: str | Path | None = None,
        env: dict[str, str] | None = None,
        timeout: int | None = None,
        operation_id: OperationId,
        python: str | None = None,
        no_project: bool = False,
    ) -> ToolResult:
        result = self._resolve_result(operation_id)
        self._record(
            kind="uv",
            args=args,
            cwd=cwd,
            env=env,
            timeout=timeout,
            operation_id=operation_id,
            extra={"python": python, "no_project": no_project},
        )
        return result

    def run_pytest_command(
        self,
        args: list[str],
        *,
        cwd: str | Path | None = None,
        env: dict[str, str] | None = None,
        timeout: int | None = None,
        operation_id: OperationId,
    ) -> ToolResult:
        result = self._resolve_result(operation_id)
        self._record(
            kind="pytest",
            args=args,
            cwd=cwd,
            env=env,
            timeout=timeout,
            operation_id=operation_id,
            extra={},
        )
        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def queue_result(self, factory: Callable[[OperationId], ToolResult]) -> None:
        """Queue a custom result factory for the next invocation."""

        self._result_queue.append(factory)

    def queue_success(
        self,
        *,
        success: bool = True,
        exit_code: int = 0,
        stdout: str = "",
        stderr: str = "",
    ) -> None:
        """Queue a simple ToolResult with the provided values."""

        def _factory(operation_id: OperationId) -> ToolResult:
            return ToolResult(
                success=success,
                exit_code=exit_code,
                stdout=stdout,
                stderr=stderr,
                operation_id=operation_id,
            )

        self.queue_result(_factory)

    def _record(
        self,
        *,
        kind: str,
        args: list[str],
        cwd: str | Path | None,
        env: dict[str, str] | None,
        timeout: int | None,
        operation_id: OperationId,
        extra: dict[str, object],
    ) -> None:
        self.calls.append(
            RecordedCall(
                kind=kind,
                args=list(args),
                cwd=cwd,
                env=dict(env or {}),
                timeout=timeout,
                operation_id=operation_id,
                extra=extra,
            )
        )

    def _resolve_result(self, operation_id: OperationId) -> ToolResult:
        if self._result_queue:
            factory = self._result_queue.pop(0)
            return factory(operation_id)
        return self._default_factory(operation_id)

    @staticmethod
    def _default_result(operation_id: OperationId) -> ToolResult:
        return ToolResult(
            success=True,
            exit_code=0,
            stdout="ok",
            stderr="",
            operation_id=operation_id,
        )


@contextmanager
def override_tools_with_deterministic_runner(
    *,
    load_config: Callable[[Path | None], ToolsConfig] | None = None,
) -> Iterator[DeterministicRunner]:
    """Install a DeterministicRunner across all tool factories via dependency overrides."""

    runner = DeterministicRunner()
    base_deps = default_tools_dependencies()
    active_load_config = (
        load_config if load_config is not None else base_deps.load_config
    )

    missing = object()

    def _attach_runner(tool: Any) -> Any:
        if getattr(tool, "subprocess_runner", missing) is not missing:
            tool.subprocess_runner = runner
        if getattr(tool, "_subprocess_runner", missing) is not missing:
            tool._subprocess_runner = runner  # type: ignore[attr-defined]
        return tool

    def _wrap_factory(factory: Callable[..., Any]) -> Callable[..., Any]:
        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            tool = factory(*args, **kwargs)
            return _attach_runner(tool)

        return _wrapped

    overridden = ToolsDependencies(
        load_config=active_load_config,
        quality_factory=_wrap_factory(base_deps.quality_factory),
        testing_factory=_wrap_factory(base_deps.testing_factory),
        environment_factory=_wrap_factory(base_deps.environment_factory),
        ci_factory=_wrap_factory(base_deps.ci_factory),
        agentic_factory=_wrap_factory(base_deps.agentic_factory),
        dev_factory=_wrap_factory(base_deps.dev_factory),
    )

    with override_tools_dependencies(overridden):
        yield runner
