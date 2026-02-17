from __future__ import annotations

import re

from collections.abc import Callable, Mapping, Sequence
from numbers import Integral, Real
from types import MappingProxyType
from typing import Protocol, cast

import numpy as np
import numpy.typing as npt

from ml_playground.framework.core.tokenizer_protocol import Tokenizer


class _TiktokenEncoding(Protocol):
    """Minimal interface used by *tiktoken* encodings."""

    n_vocab: int
    _mergeable_ranks: Mapping[str, int | float | bool] | None

    def encode(self, text: str, *, allowed_special: set[str]) -> list[int]:
        """Encode text with explicit allowed special tokens."""
        del allowed_special
        raise NotImplementedError

    def decode(self, token_ids: Sequence[int]) -> str: ...


class _TiktokenModule(Protocol):
    def get_encoding(self, encoding_name: str) -> _TiktokenEncoding: ...


def _default_tiktoken_loader() -> _TiktokenModule:
    return cast(_TiktokenModule, __import__("tiktoken"))


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
        arr: npt.NDArray[np.int64] = np.atleast_1d(
            np.asarray(token_ids, dtype=np.int64)
        )
        mask: npt.NDArray[np.bool_] = (arr >= 0) & (arr < lookup.shape[0])
        if not np.any(mask):
            return ""
        return "".join(lookup[arr[mask]])

    def _build_lookup_array(self) -> npt.NDArray[np.object_]:
        if not self.itos:
            return np.empty(0, dtype=object)
        size = int(max(self.itos.keys()) + 1)
        lookup: npt.NDArray[np.object_] = np.full(size, "", dtype=object)
        for idx, token in self.itos.items():
            if 0 <= idx < size:
                lookup[idx] = token
        return lookup

    def _ensure_lookup_array(self) -> npt.NDArray[np.object_]:
        if getattr(self, "_itos_array", None) is None or (
            self._itos_array.shape[0]
            < (int(max(self.itos.keys()) + 1) if self.itos else 0)
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
        # Simple word tokenization
        raw_words: list[str] = re.findall(r"\w+|[^\w\s]", text)
        words: list[str] = [word for word in raw_words]
        return [self.stoi.get(word, 0) for word in words]

    def decode(self, token_ids: Sequence[int]) -> str:
        if not self.itos:
            return ""
        lookup: npt.NDArray[np.object_] = self._ensure_lookup_array()
        arr: npt.NDArray[np.int64] = np.atleast_1d(
            np.asarray(token_ids, dtype=np.int64)
        )
        mask: npt.NDArray[np.bool_] = (arr >= 0) & (arr < lookup.shape[0])
        if not np.any(mask):
            return ""
        tokens: npt.NDArray[np.object_] = lookup[arr[mask]]
        return " ".join(tokens)

    def _build_lookup_array(self) -> npt.NDArray[np.object_]:
        if not self.itos:
            return np.empty(0, dtype=object)
        size = int(max(self.itos.keys()) + 1)
        lookup: npt.NDArray[np.object_] = np.full(size, "", dtype=object)
        for idx, token in self.itos.items():
            if 0 <= idx < size:
                lookup[idx] = token
        return lookup

    def _ensure_lookup_array(self) -> npt.NDArray[np.object_]:
        max_idx = int(max(self.itos.keys()) + 1) if self.itos else 0
        if getattr(self, "_itos_array", None) is None or (
            self._itos_array.shape[0] < max_idx
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
        loader: Callable[[], _TiktokenModule] | None = None,
    ):
        module_loader = loader if loader is not None else _default_tiktoken_loader
        try:
            tiktoken_module = module_loader()
        except ImportError as exc:
            raise ImportError(
                "tiktoken is required for TiktokenTokenizer but is not installed. "
                "Please install it with `pip install tiktoken`."
            ) from exc

        self.encoding_name = encoding_name
        self.encoder: _TiktokenEncoding = tiktoken_module.get_encoding(encoding_name)
        self._name = "tiktoken"

    @property
    def name(self) -> str:
        return self._name

    @property
    def vocab_size(self) -> int:
        return self.encoder.n_vocab

    @property
    def vocab(self) -> Mapping[str, int]:
        ranks_obj: object = getattr(self.encoder, "_mergeable_ranks", None)
        if not isinstance(ranks_obj, Mapping):
            return MappingProxyType({})
        ranks: Mapping[object, object] = cast(Mapping[object, object], ranks_obj)
        vocab: dict[str, int] = {}
        for token_obj, rank_obj in ranks.items():
            if not isinstance(rank_obj, Integral):
                continue
            if isinstance(token_obj, bytes):
                token_str = token_obj.decode("utf-8", errors="replace")
            else:
                token_str = str(token_obj)
            vocab[token_str] = int(rank_obj)
        return MappingProxyType(vocab)

    def encode(self, text: str) -> list[int]:
        allowed_special_tokens = {"<|reserved_special_token_0|>", "<|endoftext|>"}
        return self.encoder.encode(text, allowed_special=allowed_special_tokens)

    def decode(self, token_ids: Sequence[int]) -> str:
        return self.encoder.decode(token_ids)


def create_tokenizer(tokenizer_type: str, **kwargs: object) -> Tokenizer:
    """Factory for known tokenizer implementations."""

    tokenizer_kwargs: dict[str, object] = dict(kwargs)
    if tokenizer_type == "char":
        vocab_obj: object | None = tokenizer_kwargs.pop("vocab", None)
        if vocab_obj is None:
            vocab_mapping: dict[str, int] | None = None
        elif isinstance(vocab_obj, Mapping):
            mapping_obj = cast(Mapping[object, object], vocab_obj)
            vocab_mapping = _coerce_vocab_mapping(mapping_obj)
        else:
            raise TypeError("vocab must be a mapping when provided")

        if tokenizer_kwargs:
            raise ValueError(
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
            raise ValueError(
                "Unsupported keyword arguments for word tokenizer: "
                f"{sorted(tokenizer_kwargs.keys())}"
            )
        return WordTokenizer(vocab=vocab_mapping)

    if tokenizer_type == "tiktoken":
        encoding_name_obj: object = tokenizer_kwargs.pop("encoding_name", "cl100k_base")
        if not isinstance(encoding_name_obj, str):
            raise TypeError("encoding_name must be a string")
        loader_obj: object | None = tokenizer_kwargs.pop("loader", None)
        if loader_obj is None:
            loader: Callable[[], _TiktokenModule] | None = None
        elif callable(loader_obj):
            loader = cast(Callable[[], _TiktokenModule], loader_obj)
        else:
            raise TypeError("loader must be callable when provided")

        if tokenizer_kwargs:
            raise ValueError(
                "Unsupported keyword arguments for tiktoken tokenizer: "
                f"{sorted(tokenizer_kwargs.keys())}"
            )
        return TiktokenTokenizer(encoding_name=encoding_name_obj, loader=loader)

    raise ValueError(f"Unknown tokenizer type: {tokenizer_type}")


def _coerce_vocab_mapping(vocab: Mapping[object, object]) -> dict[str, int]:
    normalized: dict[str, int] = {}
    for key_obj, val_obj in vocab.items():
        key = str(key_obj)
        if isinstance(val_obj, bool):
            normalized[key] = int(val_obj)
            continue
        if isinstance(val_obj, Integral):
            normalized[key] = int(val_obj)
            continue
        if isinstance(val_obj, Real):
            normalized[key] = int(float(val_obj))
            continue
        raise TypeError("Tokenizer vocab values must be numeric or boolean")
    return normalized
