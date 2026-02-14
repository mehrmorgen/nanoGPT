from __future__ import annotations

from types import TracebackType
from typing import Mapping, Protocol, TypeAlias, runtime_checkable


__all__ = ["LoggerLike"]

_ExcInfoType: TypeAlias = (
    bool
    | BaseException
    | tuple[type[BaseException], BaseException, TracebackType | None]
    | None
)


@runtime_checkable
class LoggerLike(Protocol):
    """Structural protocol for loggers used in this project.

    Allows using standard logging.Logger as well as lightweight test doubles
    that implement these methods.
    """

    def debug(
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None: ...

    def info(
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None: ...

    def warning(
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None: ...

    def error(
        self,
        msg: object,
        *args: object,
        exc_info: _ExcInfoType = None,
        stack_info: bool = False,
        stacklevel: int = 1,
        extra: Mapping[str, object] | None = None,
    ) -> None: ...
