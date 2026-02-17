"""CI tools category implementation."""

from __future__ import annotations

from pathlib import Path
from typing import List, Tuple, cast

from ml_playground.framework.core.di_implementations import (
    DefaultCoverageDataExtractor,
    DefaultJsonParser,
)
from ml_playground.tools.core.config import ToolsConfig
from ml_playground.tools.core.errors import ToolExecutionError
from ml_playground.tools.core.interfaces import OperationId, ToolResult
from ml_playground.tools.environment.environment import EnvironmentTools
from ml_playground.tools.utils.subprocess_utils import (
    RealSubprocessRunner,
    SubprocessRunner,
)


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
        self.subprocess_runner = subprocess_runner or RealSubprocessRunner()
        self.cache_dir = root_path / ".cache"
        self._json_parser = DefaultJsonParser()
        self._coverage_extractor = DefaultCoverageDataExtractor()

    @property
    def _subprocess_runner(self) -> SubprocessRunner:
        return self.subprocess_runner

    @_subprocess_runner.setter
    def _subprocess_runner(self, value: SubprocessRunner) -> None:
        self.subprocess_runner = value

    @property
    def pre_commit_config(self) -> Path:
        return self.root_path / ".githooks" / ".pre-commit-config.yaml"

    @property
    def category(self) -> str:
        """Tool category identifier."""
        return "ci"

    def _coverage_file(self) -> Path:
        """Get the coverage data file path."""
        return self.cache_dir / "coverage" / "coverage.json"

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

        env_tools = EnvironmentTools(
            self.config,
            self.root_path,
            subprocess_runner=self.subprocess_runner,
        )
        env_steps: list[tuple[str, ToolResult]] = []
        env_status = "PASS"
        precommit_status = "SKIPPED"

        verify_result = env_tools.verify([])
        env_steps.append(("verify", verify_result))
        if not verify_result.success:
            setup_result = env_tools.setup([], clear=True)
            env_steps.append(("setup", setup_result))
            if not setup_result.success:
                env_status = "FAIL"
                stdout = self._build_quality_summary(
                    env_status=env_status,
                    precommit_status=precommit_status,
                    precommit_cmd=None,
                    env_steps=env_steps,
                )
                stdout = self._append_env_outputs(stdout, env_steps)
                return ToolResult(
                    success=False,
                    exit_code=setup_result.exit_code,
                    stdout=stdout,
                    stderr=setup_result.stderr or "",
                    operation_id=operation_id,
                )

            reverify_result = env_tools.verify([])
            env_steps.append(("verify", reverify_result))
            if not reverify_result.success:
                env_status = "FAIL"
                stdout = self._build_quality_summary(
                    env_status=env_status,
                    precommit_status=precommit_status,
                    precommit_cmd=None,
                    env_steps=env_steps,
                )
                stdout = self._append_env_outputs(stdout, env_steps)
                return ToolResult(
                    success=False,
                    exit_code=reverify_result.exit_code,
                    stdout=stdout,
                    stderr=reverify_result.stderr or "",
                    operation_id=operation_id,
                )

            env_status = "RECOVERED"

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
        # Run in streaming mode to provide immediate feedback for long gates.
        precommit_result = self.subprocess_runner.run_subprocess(
            [
                "uv",
                "run",
                "--no-sync",
                "--project",
                str(self.root_path),
                *precommit_cmd,
            ],
            cwd=self.root_path,
            timeout=self.config.ci.timeout,
            operation_id=operation_id,
            capture_output=False,
        )

        precommit_status = "PASS" if precommit_result.success else "FAIL"
        stdout = self._build_quality_summary(
            env_status=env_status,
            precommit_status=precommit_status,
            precommit_cmd=precommit_cmd,
            env_steps=env_steps,
        )
        stdout = self._append_env_outputs(stdout, env_steps)
        return ToolResult(
            success=precommit_result.success,
            exit_code=precommit_result.exit_code,
            stdout=stdout,
            stderr=precommit_result.stderr or "",
            operation_id=operation_id,
        )

    @staticmethod
    def _build_quality_summary(
        *,
        env_status: str,
        precommit_status: str,
        precommit_cmd: list[str] | None,
        env_steps: list[tuple[str, ToolResult]],
    ) -> str:
        summary_lines = [
            "Quality Gate Summary:",
            f"- environment: {env_status}",
            f"- pre-commit: {precommit_status}",
        ]

        if env_steps:
            summary_lines.append("")
            summary_lines.append("Environment steps:")
            for step, result in env_steps:
                status = "PASS" if result.success else "FAIL"
                summary_lines.append(f"  • {step}: {status}")

        if precommit_cmd:
            summary_lines.extend(
                [
                    "",
                    "Command executed:",
                    "  • " + " ".join(precommit_cmd),
                ]
            )
        else:
            summary_lines.extend(
                [
                    "",
                    "Pre-commit skipped due to environment failure.",
                ]
            )

        return "\n".join(summary_lines)

    @staticmethod
    def _append_env_outputs(
        stdout: str, env_steps: list[tuple[str, ToolResult]]
    ) -> str:
        output_lines = [stdout]
        for step, result in env_steps:
            if result.stdout:
                output_lines.append(f"\n{step.capitalize()} output:\n{result.stdout}")
            if result.stderr:
                output_lines.append(f"\n{step.capitalize()} errors:\n{result.stderr}")
        return "\n".join(output_lines)

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
        hooks = ["ruff", "ruff-format"]
        results: List[tuple[str, ToolResult]] = []

        for hook in hooks:
            result = self.subprocess_runner.run_uv_command(
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

        for hook_name, hook_result in results:
            if hook_result.stdout:
                combined_stdout += f"{hook_name}:\n{hook_result.stdout}\n"
            if hook_result.stderr:
                combined_stderr += f"{hook_name} warnings:\n{hook_result.stderr}\n"

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

        # Use SubprocessRunner for DI and consistent execution
        return self.subprocess_runner.run_subprocess(
            command,
            cwd=self.root_path,
            timeout=self.config.ci.timeout,
            operation_id=operation_id,
        )

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
            # Generate coverage JSON directly with coverage.
            coverage_result = self.subprocess_runner.run_uv_command(
                [
                    "python",
                    "-m",
                    "coverage",
                    "run",
                    "-m",
                    "pytest",
                    "-n",
                    "auto",
                    "tests/unit",
                    "tests/property",
                ],
                cwd=self.root_path,
                timeout=self.config.ci.timeout,
                operation_id=operation_id,
            )

            if not coverage_result.success:
                raise ToolExecutionError(
                    "Failed to generate coverage JSON for badge creation",
                    reason="Coverage report generation failed",
                    rationale="Badge generation requires valid coverage data",
                )

            combine_result = self.subprocess_runner.run_uv_command(
                ["python", "-m", "coverage", "combine"],
                cwd=self.root_path,
                timeout=self.config.ci.timeout,
                operation_id=operation_id,
            )
            if not combine_result.success:
                raise ToolExecutionError(
                    "Failed to generate coverage JSON for badge creation",
                    reason="Coverage report merging failed",
                    rationale="Badge generation requires valid coverage data",
                )

            json_result = self.subprocess_runner.run_uv_command(
                ["python", "-m", "coverage", "json", "-o", str(json_path)],
                cwd=self.root_path,
                timeout=self.config.ci.timeout,
                operation_id=operation_id,
            )
            if not json_result.success or not json_path.exists():
                raise ToolExecutionError(
                    "Failed to generate coverage JSON for badge creation",
                    reason="Coverage JSON file was not created by coverage json",
                    rationale="Badge generation requires valid coverage data",
                )

        # Generate badges directly
        try:
            # Read coverage data
            with open(json_path) as f:
                content = f.read()
                coverage_data = self._json_parser.parse_json(content)

            totals = self._coverage_extractor.extract_totals(
                cast(dict[str, object], coverage_data)
            )
            total_coverage = self._coverage_extractor.get_coverage_percent(totals)

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
                stderr=f"Failed to generate coverage badge: {e}\nRationale: Resource access or SVG formatting failed. Ensure the output directory is writable and coverage data is valid.",
                operation_id=operation_id,
            )
