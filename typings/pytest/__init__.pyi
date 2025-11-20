from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any, ParamSpec, TypeVar, overload

_P = ParamSpec("_P")
_T = TypeVar("_T")

class _RaisesContext(AbstractContextManager[BaseException]):
    def __enter__(self) -> BaseException: ...
    def __exit__(self, exc_type, exc, tb) -> bool: ...  # type: ignore[override]

def fixture(func: Callable[_P, _T]) -> Callable[_P, _T]: ...
def mark() -> None: ...
@overload
def raises(
    expected_exception: type[BaseException] | tuple[type[BaseException], ...],
    match: str | None = ...,
) -> _RaisesContext: ...
def approx(value: Any, *args: Any, **kwargs: Any) -> Any: ...
