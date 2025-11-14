"""CI tools category implementation."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import List, Tuple

from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.utils.subprocess_utils import SubprocessRunner, _default_runner


class CITools:
    """CI/CD tools implementation."""

    def __init__(
        self,
        config: ToolsConfig,
        root_path: Path,
        subprocess_runner: SubprocessRunner | None = None,
    ) -> None:
        """Initialize CI tools.

        Args:
            config: Tool configuration
            root_path: Project root path
            subprocess_runner: Subprocess runner for dependency injection
        """
        self.config = config
        self.root_path = root_path
        self.cache_dir = root_path / ".cache"
        # Use the project-local githooks pre-commit configuration
        self.pre_commit_config = root_path / ".githooks" / ".pre-commit-config.yaml"
        self._subprocess_runner = subprocess_runner or _default_runner

    @property
    def category(self) -> str:
        """Tool category identifier."""
        return "ci"

    def _coverage_file(self) -> Path:
        """Get the coverage data file path."""
        return self.cache_dir / "coverage" / "coverage.sqlite"

    def _ensure_cache_dirs(self, *subdirs: str) -> None:
        """Ensure cache directories exist."""
        for subdir in subdirs:
            (self.cache_dir / subdir).mkdir(parents=True, exist_ok=True)

    def quality_gate(self, args: List[str]) -> ToolResult:
        """Run the pre-commit quality gate only.

        Pre-commit now includes regression, integration, acceptance and e2e tests,
        as well as coverage threshold. Therefore, this gate defers entirely to
        pre-commit and surfaces a clear, structured summary.

        Args:
            args: Additional pre-commit arguments

        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="quality-gate"
        )

        # Run pre-commit on all files (authoritative gate)
        precommit_cmd = [
            "pre-commit",
            "run",
            "-v",
            "--config",
            str(self.pre_commit_config),
            "--all-files",
            *args,
        ]
        precommit_result = self._subprocess_runner.run_uv_command(
            precommit_cmd,
            cwd=self.root_path,
            timeout=self.config.ci.timeout,
            operation_id=operation_id,
        )

        status = "PASS" if precommit_result.success else "FAIL"
        summary_lines = [
            "Quality Gate Summary:",
            f"- pre-commit: {status}",
            "",
            "Command executed:",
            "  • " + " ".join(precommit_cmd),
        ]
        stdout = "\n".join(summary_lines)
        if precommit_result.stdout:
            stdout += f"\n\nPre-commit output:\n{precommit_result.stdout}"

        return ToolResult(
            success=precommit_result.success,
            exit_code=precommit_result.exit_code,
            stdout=stdout,
            stderr=precommit_result.stderr or "",
            operation_id=operation_id,
        )

    def quality_fast(self, args: List[str]) -> ToolResult:
        """Run lint/format focused pre-commit hooks.

        Args:
            args: Additional pre-commit arguments

        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="quality-fast"
        )

        # Run specific pre-commit hooks for fast feedback
        hooks = ["ruff", "ruff-format", "mdformat"]
        results = []

        for hook in hooks:
            result = self._subprocess_runner.run_uv_command(
                [
                    "pre-commit",
                    "run",
                    "--config",
                    str(self.pre_commit_config),
                    "--all-files",
                    hook,
                    *args,
                ],
                cwd=self.root_path,
                timeout=self.config.ci.timeout,
                operation_id=operation_id,
            )

            if not result.success:
                return result  # Return first failure

            results.append((hook, result))

        # Combine successful results
        combined_stdout = ""
        combined_stderr = ""

        for hook, result in results:
            if result.stdout:
                combined_stdout += f"{hook}:\n{result.stdout}\n"
            if result.stderr:
                combined_stderr += f"{hook} warnings:\n{result.stderr}\n"

        return ToolResult(
            success=True,
            exit_code=0,
            stdout=combined_stdout,
            stderr=combined_stderr,
            operation_id=operation_id,
        )

    def quality_ext(self, args: List[str]) -> ToolResult:
        """Run extended quality gates (mutation testing moved to testing tools).

        Args:
            args: Additional arguments (ignored)

        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="quality-ext"
        )

        # Run quality gate (mutation testing moved to testing tools)
        quality_result = self.quality_gate([])

        return ToolResult(
            success=quality_result.success,
            exit_code=quality_result.exit_code,
            stdout=quality_result.stdout,
            stderr=quality_result.stderr,
            operation_id=operation_id,
        )

    def quality_ci_local(self, args: List[str], bind_caches: bool = True) -> ToolResult:
        """Run the GitHub quality workflow locally using act.

        Args:
            args: Additional act arguments
            bind_caches: Whether to bind local caches into the container

        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="quality-ci-local"
        )

        # Ensure cache directories exist
        self._ensure_cache_dirs("uv", "pre-commit", "ruff")
        (self.root_path / ".venv").mkdir(parents=True, exist_ok=True)

        # Build act command
        command = [
            "act",
            "--container-architecture",
            "linux/amd64",
            "-P",
            "ubuntu-latest=catthehacker/ubuntu:act-latest",
            "-W",
            ".github/workflows/quality.yml",
            "--job",
            "quality",
        ]

        # Add cache binds if requested
        if bind_caches:
            binds: List[Tuple[Path, str]] = [
                (self.cache_dir / "uv", "/root/.cache/uv"),
                (self.cache_dir / "pre-commit", "/root/.cache/pre-commit"),
                (self.cache_dir / "ruff", "/root/.cache/ruff"),
                (self.root_path / ".venv", "/root/project/.venv"),
            ]

            for host_path, container_path in binds:
                host_path.mkdir(parents=True, exist_ok=True)
                command.extend(["--bind", f"{host_path}:{container_path}"])

        # Add additional arguments
        command.extend(args)

        # Run act directly with subprocess since it's not a uv command
        try:
            result = subprocess.run(
                command,
                cwd=self.root_path,
                capture_output=True,
                text=True,
                timeout=self.config.ci.timeout,
            )

            return ToolResult(
                success=result.returncode == 0,
                exit_code=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
                operation_id=operation_id,
            )
        except subprocess.TimeoutExpired as exc:
            raise ToolExecutionError(
                f"Act command timed out after {self.config.ci.timeout} seconds",
                reason="Command execution exceeded configured timeout",
                rationale="CI operations must complete within reasonable time bounds",
            ) from exc
        except Exception as exc:
            raise ToolExecutionError(
                f"Failed to execute act command: {exc}",
                reason="Subprocess execution failed",
                rationale="Act must be available and executable for local CI testing",
            ) from exc

    def coverage_badge(self, args: List[str]) -> ToolResult:
        """Regenerate the SVG coverage badges.

        Args:
            args: Additional arguments (ignored)

        Returns:
            ToolResult with execution details
        """
        operation_id = OperationId(
            namespace="tools", category=self.category, command="coverage-badge"
        )

        # Ensure coverage JSON exists
        json_path = self.cache_dir / "coverage" / "coverage.json"
        if not json_path.exists():
            # Try to generate coverage report first
            coverage_result = self._subprocess_runner.run_uv_command(
                ["coverage", "json", "-o", str(json_path)],
                cwd=self.root_path,
                env={"COVERAGE_FILE": str(self._coverage_file())},
                timeout=self.config.ci.timeout,
                operation_id=operation_id,
            )

            if not coverage_result.success:
                raise ToolExecutionError(
                    "Failed to generate coverage JSON for badge creation",
                    reason="Coverage report generation failed",
                    rationale="Badge generation requires valid coverage data",
                )

        # Generate badges directly
        try:
            import json

            # Read coverage data
            with open(json_path) as f:
                coverage_data = json.load(f)

            total_coverage = coverage_data.get("totals", {}).get("percent_covered", 0)

            # Create simple SVG badge in configured directory (relative to root path)
            badge_dir = (self.root_path / self.config.ci.badge_output_dir).resolve()
            badge_dir.mkdir(parents=True, exist_ok=True)

            # Determine color based on coverage
            if total_coverage >= 90:
                color = "brightgreen"
            elif total_coverage >= 80:
                color = "green"
            elif total_coverage >= 70:
                color = "yellowgreen"
            elif total_coverage >= 60:
                color = "yellow"
            else:
                color = "red"

            # Simple SVG badge content
            svg_content = f'''<svg xmlns="http://www.w3.org/2000/svg" width="104" height="20">
<linearGradient id="b" x2="0" y2="100%">
<stop offset="0" stop-color="#bbb" stop-opacity=".1"/>
<stop offset="1" stop-opacity=".1"/>
</linearGradient>
<mask id="a">
<rect width="104" height="20" rx="3" fill="#fff"/>
</mask>
<g mask="url(#a)">
<path fill="#555" d="M0 0h63v20H0z"/>
<path fill="{color}" d="M63 0h41v20H63z"/>
<path fill="url(#b)" d="M0 0h104v20H0z"/>
</g>
<g fill="#fff" text-anchor="middle" font-family="DejaVu Sans,Verdana,Geneva,sans-serif" font-size="110">
<text x="325" y="150" fill="#010101" fill-opacity=".3" transform="scale(.1)" textLength="530">coverage</text>
<text x="325" y="140" transform="scale(.1)" textLength="530">coverage</text>
<text x="825" y="150" fill="#010101" fill-opacity=".3" transform="scale(.1)" textLength="310">{total_coverage:.0f}%</text>
<text x="825" y="140" transform="scale(.1)" textLength="310">{total_coverage:.0f}%</text>
</g>
</svg>'''

            # Write badge file
            badge_file = badge_dir / "coverage.svg"
            badge_file.write_text(svg_content)

            return ToolResult(
                success=True,
                exit_code=0,
                stdout=f"Generated coverage badge: {badge_file} ({total_coverage:.1f}% coverage)",
                stderr="",
                operation_id=operation_id,
            )

        except Exception as e:
            return ToolResult(
                success=False,
                exit_code=1,
                stdout="",
                stderr=f"Failed to generate coverage badge: {e}",
                operation_id=operation_id,
            )
