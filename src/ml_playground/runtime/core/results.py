"""Runtime-specific result and learning abstractions."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable, Literal

__all__ = [
    "LearningInfo",
    "LearningModeEngine",
    "OperationId",
    "ToolResult",
    "VerbosityLevel",
]


class VerbosityLevel(Enum):
    """Learning verbosity levels."""

    MINIMAL = 0
    STANDARD = 1
    COMPREHENSIVE = 2


@dataclass
class LearningInfo:
    """Educational metadata captured during command execution."""

    commands_executed: list[str] = field(default_factory=list)
    explanations: list[str] = field(default_factory=list)
    best_practices: list[str] = field(default_factory=list)
    related_concepts: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class OperationId:
    """Structured identifier for runtime/tooling commands."""

    namespace: Literal["ml", "tools"]
    category: str
    command: str

    def __post_init__(self) -> None:
        if not self.category:
            raise ValueError("category must be provided")
        if not self.command:
            raise ValueError("command must be provided")

    def __str__(self) -> str:
        return f"{self.namespace}.{self.category}.{self.command}"


@dataclass
class ToolResult:
    """Outcome container for runtime operations."""

    success: bool
    exit_code: int
    operation_id: OperationId
    stdout: str = ""
    stderr: str = ""
    learning_info: LearningInfo = field(default_factory=LearningInfo)

    @classmethod
    def create(
        cls,
        *,
        success: bool,
        exit_code: int,
        namespace: Literal["ml", "tools"],
        category: str,
        command: str,
        stdout: str = "",
        stderr: str = "",
        learning_info: LearningInfo | None = None,
    ) -> ToolResult:
        return cls(
            success=success,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            learning_info=learning_info or LearningInfo(),
            operation_id=OperationId(
                namespace=namespace, category=category, command=command
            ),
        )


_CATEGORY_TEMPLATES: dict[str, dict[str, list[str]]] = {
    "prepare": {
        "minimal": ["Prepares data before training."],
        "standard": [
            "Converts raw datasets into curated artifacts for experiments.",
            "Validates splits and ensures deterministic preprocessing.",
        ],
        "comprehensive": [
            "Preparation includes validation, tokenization, and artifact staging.",
            "Consistent preprocessing is critical for reproducible training runs.",
        ],
        "practices": [
            "Document preprocessing parameters alongside experiment configs.",
            "Verify dataset integrity after every pipeline change.",
        ],
        "concepts": [
            "Data lineage",
            "Deterministic preprocessing",
        ],
    },
    "train": {
        "minimal": ["Optimises model parameters for the experiment."],
        "standard": [
            "Runs the configured training loop and logs intermediate metrics.",
            "Applies configured optimizers, schedulers, and callbacks.",
        ],
        "comprehensive": [
            "Training monitors convergence, validates checkpoints, and records metrics.",
            "Carefully chosen seeds and device configuration ensure deterministic behaviour.",
        ],
        "practices": [
            "Track loss curves to detect divergence early.",
            "Persist checkpoints with associated configuration snapshots.",
        ],
        "concepts": [
            "Gradient descent",
            "Checkpointing",
        ],
    },
    "sample": {
        "minimal": ["Generates outputs using the trained experiment."],
        "standard": [
            "Loads the trained model and produces samples for evaluation.",
            "Applies decoding strategies and logs qualitative artefacts.",
        ],
        "comprehensive": [
            "Sampling evaluates inference quality under configured decoding parameters.",
            "Review generated content to validate alignment with dataset expectations.",
        ],
        "practices": [
            "Capture prompts and seeds alongside generated outputs.",
            "Compare samples across checkpoints to track quality drift.",
        ],
        "concepts": [
            "Decoding strategies",
            "Inference reproducibility",
        ],
    },
    "analyze": {
        "minimal": ["Inspects experiment results for insights."],
        "standard": [
            "Aggregates evaluation metrics and renders diagnostic views.",
            "Surfaces regressions relative to baseline checkpoints.",
        ],
        "comprehensive": [
            "Analysis correlates quantitative metrics with qualitative findings.",
            "Use analysis reports to drive iteration on data and model configuration.",
        ],
        "practices": [
            "Automate report generation after each training run.",
            "Track historical metrics to detect long-term regressions.",
        ],
        "concepts": [
            "Model evaluation",
            "Regression tracking",
        ],
    },
}


_COMMAND_OVERRIDES: dict[tuple[str, str], list[str]] = {
    ("prepare", "bundestag_char"): [
        "Converts raw text files into character-level tokens for the bundestag dataset.",
    ],
}


class LearningModeEngine:
    """Lightweight educational helper for runtime commands."""

    def __init__(self, verbosity: VerbosityLevel = VerbosityLevel.STANDARD) -> None:
        self.verbosity = verbosity

    def explain_command(
        self,
        command: str,
        context: str,
        category: str,
        executed_commands: Iterable[str] | None = None,
    ) -> LearningInfo:
        info = LearningInfo()
        info.commands_executed.extend(executed_commands or [])

        templates = _CATEGORY_TEMPLATES.get(category, {})
        if self.verbosity is VerbosityLevel.MINIMAL:
            info.explanations.extend(templates.get("minimal", []))
        else:
            info.explanations.extend(templates.get("standard", []))
            info.explanations.append(f"Context: {context}")
            if self.verbosity is VerbosityLevel.COMPREHENSIVE:
                info.explanations.extend(templates.get("comprehensive", []))

        # Command-specific enrichment
        info.explanations.extend(_COMMAND_OVERRIDES.get((category, command), []))

        if self.verbosity is not VerbosityLevel.MINIMAL:
            info.best_practices.extend(templates.get("practices", []))
            info.related_concepts.extend(templates.get("concepts", []))

        return info
