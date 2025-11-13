from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from numbers import Integral, Real
from types import MappingProxyType
from typing import Any, Literal, cast

import numpy as np
import numpy.typing as npt

from ml_playground.core.tokenizer_protocol import Tokenizer


__all__ = ["Tokenizer", "create_tokenizer"]


class CharTokenizer:
    """Character-level tokenizer that maps single characters to integer ids."""

    def __init__(self, vocab: dict[str, int] | None = None) -> None:
        self._name = "char"
        if vocab is not None:
            self.stoi: dict[str, int] = dict(vocab)
            self.itos: dict[int, str] = {i: s for s, i in vocab.items()}
            self._itos_array: npt.NDArray[np.object_] = self._build_lookup_array()
        else:
            # Default character-level vocabulary will be built during training
            self.stoi = {}
            self.itos = {}
            self._itos_array = np.empty(0, dtype=object)

    @property
    def name(self) -> str:
        return self._name

    @property
    def vocab_size(self) -> int:
        return len(self.stoi)

    @property
    def vocab(self) -> Mapping[str, int]:
        return MappingProxyType(self.stoi)

    def encode(self, text: str) -> list[int]:
        return [self.stoi.get(ch, 0) for ch in text]

    def decode(self, token_ids: Sequence[int]) -> str:
        if not self.itos:
            return ""
        lookup: npt.NDArray[np.object_] = self._ensure_lookup_array()
        arr = np.atleast_1d(np.asarray(token_ids, dtype=np.int64))
        mask = (arr >= 0) & (arr < lookup.shape[0])
        if not np.any(mask):
            return ""
        return "".join(lookup[arr[mask]])

    def _build_lookup_array(self) -> npt.NDArray[np.object_]:
        if not self.itos:
            return np.empty(0, dtype=object)
        size = max(self.itos.keys()) + 1
        lookup: npt.NDArray[np.object_] = np.full(size, "", dtype=object)
        for idx, token in self.itos.items():
            if 0 <= idx < size:
                lookup[idx] = token
        return lookup

    def _ensure_lookup_array(self) -> npt.NDArray[np.object_]:
        if getattr(self, "_itos_array", None) is None or (
            self._itos_array.shape[0] < (max(self.itos.keys()) + 1 if self.itos else 0)
        ):
            self._itos_array = self._build_lookup_array()
        return self._itos_array


class WordTokenizer:
    """Word-level tokenizer that segments text via a simple regex pattern."""

    def __init__(self, vocab: dict[str, int] | None = None) -> None:
        self._name = "word"
        if vocab is not None:
            self.stoi: dict[str, int] = dict(vocab)
            self.itos: dict[int, str] = {i: s for s, i in vocab.items()}
            self._itos_array: npt.NDArray[np.object_] = self._build_lookup_array()
        else:
            # Default word-level vocabulary will be built during training
            self.stoi = {}
            self.itos = {}
            self._itos_array = np.empty(0, dtype=object)

    @property
    def name(self) -> str:
        return self._name

    def encode(self, text: str) -> list[int]:
        import re

        # Simple word tokenization
        words = re.findall(r"\w+|[^\w\s]", text)
        return [self.stoi.get(word, 0) for word in words]

    def decode(self, token_ids: Sequence[int]) -> str:
        if not self.itos:
            return ""
        lookup: npt.NDArray[np.object_] = self._ensure_lookup_array()
        arr = np.atleast_1d(np.asarray(token_ids, dtype=np.int64))
        mask = (arr >= 0) & (arr < lookup.shape[0])
        if not np.any(mask):
            return ""
        tokens = lookup[arr[mask]]
        return " ".join(tokens)

    def _build_lookup_array(self) -> npt.NDArray[np.object_]:
        if not self.itos:
            return np.empty(0, dtype=object)
        size = max(self.itos.keys()) + 1
        lookup: npt.NDArray[np.object_] = np.full(size, "", dtype=object)
        for idx, token in self.itos.items():
            if 0 <= idx < size:
                lookup[idx] = token
        return lookup

    def _ensure_lookup_array(self) -> npt.NDArray[np.object_]:
        if getattr(self, "_itos_array", None) is None or (
            self._itos_array.shape[0] < (max(self.itos.keys()) + 1 if self.itos else 0)
        ):
            self._itos_array = self._build_lookup_array()
        return self._itos_array

    @property
    def vocab_size(self) -> int:
        return len(self.stoi)

    @property
    def vocab(self) -> Mapping[str, int]:
        return MappingProxyType(self.stoi)


class TiktokenTokenizer:
    """`tiktoken`-based BPE tokenizer supporting GPT-style byte pair encoding."""

    def __init__(
        self,
        encoding_name: str = "cl100k_base",
        *,
        loader: Callable[[], Any] | None = None,
    ):
        module_loader = loader if loader is not None else lambda: __import__("tiktoken")
        try:
            tiktoken_module = module_loader()
        except ImportError as exc:
            raise ImportError(
                "tiktoken is required for TiktokenTokenizer but is not installed. "
                "Please install it with `pip install tiktoken`."
            ) from exc

        self.encoding_name = encoding_name
        self.encoder = tiktoken_module.get_encoding(encoding_name)
        self._name = "tiktoken"

    @property
    def name(self) -> str:
        return self._name

    def encode(self, text: str) -> list[int]:
        return self.encoder.encode(text, allowed_special={"<|endoftext|>"})

    def decode(self, token_ids: Sequence[int]) -> str:
        return self.encoder.decode(token_ids)

    @property
    def vocab_size(self) -> int:
        return self.encoder.n_vocab

    @property
    def vocab(self) -> Mapping[str, int]:
        # tiktoken exposes mergeable ranks as a dict[str, int]; use it when available
        ranks_obj = getattr(self.encoder, "_mergeable_ranks", None)
        if isinstance(ranks_obj, Mapping):
            ranks_map = cast(Mapping[str, int | float | bool], ranks_obj)
            typed_ranks = {str(token): int(rank) for token, rank in ranks_map.items()}
            return MappingProxyType(typed_ranks)
        # Fallback to empty mapping if ranks is not available or not a dict
        return MappingProxyType({})


def _coerce_vocab_mapping(value: Mapping[object, object]) -> dict[str, int]:
    typed: dict[str, int] = {}
    for key_obj, val_obj in value.items():
        key = str(key_obj)
        if isinstance(val_obj, bool):
            typed[key] = int(val_obj)
            continue
        if isinstance(val_obj, Integral):
            typed[key] = int(val_obj)
            continue
        if isinstance(val_obj, Real):
            typed[key] = int(float(val_obj))
            continue
        raise TypeError("vocab values must be numeric or boolean")
    return typed


def create_tokenizer(
    tokenizer_type: Literal["char", "word", "tiktoken"], **kwargs: Any
) -> Tokenizer:
    """Factory for known tokenizer implementations."""

    tokenizer_kwargs: dict[str, Any] = dict(kwargs)
    if tokenizer_type == "char":
        vocab_obj = tokenizer_kwargs.pop("vocab", None)
        if vocab_obj is None:
            vocab_mapping: dict[str, int] | None = None
        elif isinstance(vocab_obj, Mapping):
            mapping_obj = cast(Mapping[object, object], vocab_obj)
            vocab_mapping = _coerce_vocab_mapping(mapping_obj)
        else:
            raise TypeError("vocab must be a mapping when provided")  # pragma: no cover

        if tokenizer_kwargs:
            raise ValueError(  # pragma: no cover
                "Unsupported keyword arguments for char tokenizer: "
                f"{sorted(tokenizer_kwargs.keys())}"
            )
        return CharTokenizer(vocab=vocab_mapping)

    if tokenizer_type == "word":
        vocab_obj = tokenizer_kwargs.pop("vocab", None)
        if vocab_obj is None:
            vocab_mapping = None
        elif isinstance(vocab_obj, Mapping):
            mapping_obj = cast(Mapping[object, object], vocab_obj)
            vocab_mapping = _coerce_vocab_mapping(mapping_obj)
        else:
            raise TypeError("vocab must be a mapping when provided")

        if tokenizer_kwargs:
            raise ValueError(  # pragma: no cover
                "Unsupported keyword arguments for word tokenizer: "
                f"{sorted(tokenizer_kwargs.keys())}"
            )
        return WordTokenizer(vocab=vocab_mapping)

    if tokenizer_type == "tiktoken":
        encoding_name_obj = tokenizer_kwargs.pop("encoding_name", "cl100k_base")
        if not isinstance(encoding_name_obj, str):
            raise TypeError("encoding_name must be a string")  # pragma: no cover
        loader_obj = tokenizer_kwargs.pop("loader", None)
        if loader_obj is None:
            loader: Callable[[], Any] | None = None
        elif callable(loader_obj):
            loader = cast(Callable[[], Any], loader_obj)
        else:
            raise TypeError("loader must be callable when provided")  # pragma: no cover

        if tokenizer_kwargs:
            raise ValueError(  # pragma: no cover
                "Unsupported keyword arguments for tiktoken tokenizer: "
                f"{sorted(tokenizer_kwargs.keys())}"
            )
        return TiktokenTokenizer(encoding_name=encoding_name_obj, loader=loader)
    raise ValueError(f"Unknown tokenizer type: {tokenizer_type}")
