from __future__ import annotations
import time
from contextlib import contextmanager
from typing import Any, Iterator, Protocol, runtime_checkable


@runtime_checkable
class Telemetry(Protocol):
    """Protocol for experiment telemetry and performance hooks."""

    def log_metric(self, name: str, value: float, step: int | None = None) -> None: ...
    @contextmanager
    def time_block(self, name: str) -> Iterator[None]: ...


class NoOpTelemetry:
    """Default telemetry implementation that does nothing."""

    def log_metric(self, name: str, value: float, step: int | None = None) -> None:
        """Log a metric (no-op)."""
        pass

    @contextmanager
    def time_block(self, name: str) -> Iterator[None]:
        """Time a block of code (no-op)."""
        yield


class ConsoleTelemetry:
    """Telemetry implementation that logs to console/logger."""

    def __init__(self, logger: Any = None) -> None:
        self.logger = logger

    def log_metric(self, name: str, value: float, step: int | None = None) -> None:
        msg = f"[Metric] {name}: {value}"
        if step is not None:
            msg += f" (step: {step})"
        if self.logger:
            self.logger.info(msg)
        else:
            print(msg)

    @contextmanager
    def time_block(self, name: str) -> Iterator[None]:
        start = time.perf_counter()
        yield
        duration = time.perf_counter() - start
        msg = f"[Perf] {name} took {duration:.4f}s"
        if self.logger:
            self.logger.info(msg)
        else:
            print(msg)
