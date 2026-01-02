"""Metrics registry and naming rules.

Naming convention:
- Use lowercase segments separated by dots.
- Each segment starts with a letter and contains [a-z0-9_].
- Avoid leading/trailing dots or empty segments.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
from typing import Iterable

_SEGMENT_RE = re.compile(r"^[a-z][a-z0-9_]*$")


class MetricKind(str, Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    RATE = "rate"


@dataclass(frozen=True)
class MetricSpec:
    name: str
    description: str
    kind: MetricKind = MetricKind.GAUGE
    unit: str | None = None


def is_valid_metric_name(name: str) -> bool:
    if not name or name.startswith(".") or name.endswith("."):
        return False
    parts = name.split(".")
    return all(_SEGMENT_RE.match(part) for part in parts)


def validate_metric_name(name: str) -> None:
    if not is_valid_metric_name(name):
        raise ValueError(
            "Metric names must be lowercase segments separated by dots; "
            "each segment starts with a letter and uses [a-z0-9_]."
        )


class MetricsRegistry:
    def __init__(self) -> None:
        self._metrics: dict[str, MetricSpec] = {}

    def register(self, spec: MetricSpec) -> MetricSpec:
        validate_metric_name(spec.name)
        if spec.name in self._metrics:
            raise ValueError(f"Metric '{spec.name}' is already registered.")
        self._metrics[spec.name] = spec
        return spec

    def register_metric(
        self,
        name: str,
        description: str,
        kind: MetricKind = MetricKind.GAUGE,
        unit: str | None = None,
    ) -> MetricSpec:
        return self.register(
            MetricSpec(name=name, description=description, kind=kind, unit=unit)
        )

    def get(self, name: str) -> MetricSpec:
        return self._metrics[name]

    def list(self) -> tuple[MetricSpec, ...]:
        return tuple(self._metrics[name] for name in sorted(self._metrics))

    def to_dashboard_spec(self) -> dict[str, list[dict[str, str]]]:
        return {
            "metrics": [
                {
                    "name": spec.name,
                    "kind": spec.kind.value,
                    "unit": "" if spec.unit is None else spec.unit,
                    "description": spec.description,
                }
                for spec in self.list()
            ]
        }

    def to_markdown(self) -> str:
        lines = [
            "# Metrics Registry",
            "",
            "| Name | Kind | Unit | Description |",
            "| --- | --- | --- | --- |",
        ]
        for spec in self.list():
            unit = "" if spec.unit is None else spec.unit
            lines.append(
                f"| {spec.name} | {spec.kind.value} | {unit} | {spec.description} |"
            )
        lines.append("")
        return "\n".join(lines)


def register_all(registry: MetricsRegistry, specs: Iterable[MetricSpec]) -> None:
    for spec in specs:
        registry.register(spec)
