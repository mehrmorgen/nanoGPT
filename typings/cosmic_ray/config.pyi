"""Type stubs for cosmic_ray.config used in tooling."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Iterator, override

class ConfigDict(Mapping[str, object]):
    @override
    def __getitem__(self, __key: str, /) -> object: ...
    @override
    def __iter__(self) -> Iterator[str]: ...
    @override
    def __len__(self) -> int: ...
    @override
    def __contains__(self, __key: object, /) -> bool: ...

def load_config(filename: str | None = ...) -> ConfigDict: ...
