from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, TypedDict, TYPE_CHECKING

if TYPE_CHECKING:
    from ml_playground.core.protocols import Telemetry

from ml_playground.self_play.pool_size import derive_pool_size

PoolSizeProvider = Callable[[int, int, float], int]


class ProviderBundle(TypedDict, total=False):
    pool_size_provider: PoolSizeProvider
    read_text_fn: Callable[[Path], str]
    tokenizer_factory: Callable[[Any], Any]
    checkpoint_load_fn: Callable[..., Any]
    checkpoint_save_fn: Callable[..., None]
    model_factory: Callable[..., Any]
    compile_model_fn: Callable[[Any], Any]
    cuda_is_available_fn: Callable[[], bool]
    cuda_manual_seed_fn: Callable[[int], None]
    telemetry: Telemetry


def get_default_providers() -> ProviderBundle:
    """Return the default provider bundle for configuration-bound indirections."""
    from ml_playground.core.tokenizer import create_tokenizer
    from ml_playground.core.telemetry import NoOpTelemetry

    def _read_text_default(p: Path) -> str:
        return p.read_text(encoding="utf-8")

    return {
        "pool_size_provider": derive_pool_size,
        "read_text_fn": _read_text_default,
        "tokenizer_factory": create_tokenizer,
        "telemetry": NoOpTelemetry(),
    }
