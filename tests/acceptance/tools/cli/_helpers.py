from __future__ import annotations

import re
import subprocess


def assert_successful_exit(result: subprocess.CompletedProcess[str]) -> None:
    assert result.returncode == 0, (
        f"Expected exit code 0, got {result.returncode}. stderr: {result.stderr}"
    )


def assert_error_exit(result: subprocess.CompletedProcess[str]) -> None:
    assert result.returncode == 1, (
        f"Expected exit code 1, got {result.returncode}. stdout: {result.stdout}"
    )


def assert_output_contains_pattern(
    result: subprocess.CompletedProcess[str], pattern: str
) -> None:
    output = result.stdout + result.stderr
    assert re.search(pattern, output, re.IGNORECASE | re.DOTALL), (
        f"Pattern '{pattern}' not found in output: stdout={result.stdout}, stderr={result.stderr}"
    )


def assert_error_output_contains(
    result: subprocess.CompletedProcess[str], text: str
) -> None:
    output = result.stdout + result.stderr
    assert text in output, (
        f"Error text '{text}' not found in output: stdout={result.stdout}, stderr={result.stderr}"
    )
