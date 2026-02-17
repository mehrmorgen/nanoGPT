from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

_FALLBACK_SAMPLES: tuple[str, ...] = (
    "Nächste Rednerin ist die Vorsitzende der AfD-Fraktion, Dr. Alice Weidel.",
    "Herr Präsident, liebe Kolleginnen und Kollegen, wir beraten heute wichtige Vorlagen.",
    "(Beifall bei der SPD)",
    "Die Bundesregierung handelt entschlossen.",
    "Applaus bei der CDU/CSU.",
    "Vielen Dank. — Zur Geschäftsordnung hat der Abgeordnete das Wort.",
    "Wir müssen die Inflation bekämpfen und Familien entlasten.",
    "Das Wort hat nun die Bundeskanzlerin.",
    "Meine Damen und Herren, die Lage ist ernst, aber beherrschbar.",
    "(Heiterkeit) Der nächste Redner folgt.",
)


def load_lit_samples(
    *,
    path_resolver: Callable[[Path], Path] | None = None,
    max_lines: int = 10,
) -> list[str]:
    """Return tiny text samples for LIT demo from experiment data if available."""
    samples = list(_FALLBACK_SAMPLES)
    if max_lines <= 0:
        return []

    try:
        resolved = (
            path_resolver(Path(__file__)) if path_resolver else Path(__file__).resolve()
        )
        exp_dir = resolved.parent
        input_path = exp_dir / "datasets" / "input.txt"
        if not input_path.exists():
            return samples[:max_lines]

        lines: list[str] = []
        with input_path.open("r", encoding="utf-8", errors="ignore") as input_file:
            for raw_line in input_file:
                line = raw_line.strip()
                if not line:
                    continue
                lines.append(line)
                if len(lines) >= max_lines:
                    break
        if lines:
            return lines
    except (OSError, UnicodeError):
        return samples[:max_lines]

    return samples[:max_lines]
