"""Default implementations of core protocols for dependency injection.

This module provides protocol-compliant wrappers for standard library modules
and null object implementations for optional dependencies, enabling testability
and eliminating runtime casts.
"""

from __future__ import annotations

import importlib
import json
from typing import Iterator, Mapping, cast

from ml_playground.framework.core.coverage_data import extract_coverage_totals
from ml_playground.framework.core.protocols import (
    ConfigSectionExtractor,
    CoverageDataExtractor,
    JsonParser,
    MLflowClient,
    MLflowRun,
    ModuleImporter,
    OSModule,
    PlatformModule,
    SysModule,
    TestResultExtractor,
)


class StdOSModule(OSModule):
    """Protocol-compliant wrapper for the os module."""

    def getcwd(self) -> str:
        os = __import__("os")  # type: ignore[assignment]
        return cast(str, os.getcwd())  # type: ignore[reportAny]

    def getlogin(self) -> str:
        os = __import__("os")  # type: ignore[assignment]
        try:
            return cast(str, os.getlogin())  # type: ignore[reportAny]
        except OSError:
            # Fall back to environment variable when no controlling terminal
            return os.environ.get("USER", "unknown")


class StdPlatformModule(PlatformModule):
    """Protocol-compliant wrapper for the platform module."""

    def platform(self) -> str:
        platform = __import__("platform")  # type: ignore[assignment]
        return cast(str, platform.platform())  # type: ignore[reportAny]

    def processor(self) -> str:
        platform = __import__("platform")  # type: ignore[assignment]
        return cast(str, platform.processor())  # type: ignore[reportAny]


class StdSysModule(SysModule):
    """Protocol-compliant wrapper for the sys module."""

    sys = __import__("sys")  # type: ignore[assignment]
    version: str = cast(str, sys.version)
    argv: list[str] = cast(list[str], sys.argv)


class NullMLflowClient(MLflowClient):
    """Null object pattern implementation for optional MLflow dependency.

    All methods are no-ops, allowing code to run without MLflow installed.
    """

    def set_tracking_uri(self, _uri: str, /) -> None:
        pass

    def get_experiment_by_name(self, _name: str, /) -> object:
        return None

    def set_experiment(self, _experiment_name: str, /) -> object:
        return None

    def create_experiment(self, _name: str, /, **kwargs: object) -> str:
        return ""

    def start_run(self, **kwargs: object) -> MLflowRun:
        return NullMLflowRun()

    def end_run(self) -> None:
        pass

    def log_params(self, _params: Mapping[str, object], /) -> None:
        pass

    def log_metrics(
        self, _metrics: dict[str, float], /, *, step: int | None = None
    ) -> None:
        pass

    def log_artifact(
        self, _local_path: str, /, *, artifact_path: str | None = None
    ) -> None:
        pass

    def log_artifacts(
        self, _local_dir: str, /, *, artifact_path: str | None = None
    ) -> None:
        pass

    def set_tag(self, _key: str, _value: object, /) -> None:
        pass


class DefaultJsonParser(JsonParser):
    """Default JSON parser with typed return values."""

    def parse_json(self, content: str) -> Mapping[str, object]:
        return cast(Mapping[str, object], json.loads(content))

    def parse_gate_snapshot(self, content: str) -> object:
        return json.loads(content)  # type: ignore[no-any-return]

    def parse_github_response(self, content: str) -> dict[str, object]:
        return cast(dict[str, object], json.loads(content))


class DefaultConfigSectionExtractor(ConfigSectionExtractor):
    """Default implementation for extracting nested config sections."""

    def extract_section(
        self, config: Mapping[str, object], section: str
    ) -> Mapping[str, object]:
        section_data = config.get(section)
        if section_data is None:
            return {}
        if not isinstance(section_data, Mapping):
            return {}
        return cast(Mapping[str, object], section_data)

    def get_string(self, mapping: Mapping[str, object], key: str, default: str) -> str:
        value = mapping.get(key, default)
        if value is None:
            return default
        if isinstance(value, str):
            return value
        return default


class DefaultModuleImporter(ModuleImporter):
    """Default implementation for importing LIT modules."""

    def import_dataset_module(self) -> object:
        return importlib.import_module("lit_nlp.api.dataset")

    def import_model_module(self) -> object:
        return importlib.import_module("lit_nlp.api.model")

    def import_types_module(self) -> object:
        return importlib.import_module("lit_nlp.api.types")


class DefaultTestResultExtractor(TestResultExtractor):
    """Default implementation for extracting test results."""

    def extract_overall(self, results: dict[str, object]) -> dict[str, object]:
        overall = results.get("overall", {})
        if isinstance(overall, dict):
            return cast(dict[str, object], overall)
        return {}

    def extract_status(self, section: dict[str, object]) -> str:
        status = section.get("status")
        if isinstance(status, str):
            return status
        return "unknown"


class DefaultCoverageDataExtractor(CoverageDataExtractor):
    """Default implementation for extracting coverage data."""

    def extract_totals(self, coverage_data: dict[str, object]) -> dict[str, object]:
        return cast(dict[str, object], extract_coverage_totals(coverage_data))

    def get_coverage_percent(self, totals: dict[str, object]) -> float:
        percent = totals.get("percent_covered", 0.0)
        if isinstance(percent, (int, float)):
            return float(percent)
        return 0.0


class NullMLflowRun(MLflowRun):
    """No-op MLflow run handle used by NullMLflowClient."""

    def __enter__(self) -> object:
        return self

    def __exit__(self, *exc: object) -> bool | None:
        return None

    def __iter__(self) -> Iterator[object]:
        return iter(())


__all__ = [
    "StdOSModule",
    "StdPlatformModule",
    "StdSysModule",
    "NullMLflowRun",
    "NullMLflowClient",
    "DefaultJsonParser",
    "DefaultConfigSectionExtractor",
    "DefaultModuleImporter",
    "DefaultTestResultExtractor",
    "DefaultCoverageDataExtractor",
]
