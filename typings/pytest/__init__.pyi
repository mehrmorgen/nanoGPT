from __future__ import annotations

from collections.abc import Callable, Iterable
from contextlib import AbstractContextManager
from typing import Any, ParamSpec, TypeVar, overload, Generic

_P = ParamSpec("_P")
_T = TypeVar("_T")
_E = TypeVar("_E", bound=BaseException)

class ExceptionInfo(Generic[_E]):
    @property
    def value(self) -> _E: ...
    @property
    def type(self) -> type[_E]: ...

class _RaisesContext(AbstractContextManager[ExceptionInfo[_E]], Generic[_E]):
    def __enter__(self) -> ExceptionInfo[_E]: ...
    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool: ...  # type: ignore[override]

@overload
def fixture(__function: Callable[_P, _T]) -> Callable[_P, _T]: ...
@overload
def fixture(
    __function: None = ...,
    *,
    scope: str | None = ...,
    autouse: bool | None = ...,
    params: Iterable[Any] | None = ...,
    ids: Iterable[str] | None = ...,
    name: str | None = ...,
) -> Callable[[Callable[_P, _T]], Callable[_P, _T]]: ...

class CaptureFixture(Generic[_T]):
    def readouterr(self) -> Any: ...

class LogCaptureFixture:
    @property
    def records(self) -> list[Any]: ...
    @property
    def text(self) -> str: ...
    def clear(self) -> None: ...
    def set_level(self, level: int | str, logger: str | None = ...) -> None: ...
    def at_level(
        self, level: int | str, logger: str | None = ...
    ) -> AbstractContextManager[None]: ...

class _MarkDecorator:
    def parametrize(
        self,
        argnames: str | Iterable[str],
        argvalues: Iterable[Any],
        *args: Any,
        **kwargs: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]: ...

mark: _MarkDecorator

def raises(
    expected_exception: type[_E] | tuple[type[_E], ...],
    match: str | None = ...,
) -> _RaisesContext[_E]: ...
def approx(value: Any, *args: Any, **kwargs: Any) -> Any: ...
def skip(reason: str | None = ...) -> None: ...
