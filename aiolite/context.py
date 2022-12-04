from functools import wraps
from types import TracebackType
from typing import (
    Generator,
    Any,
    Optional,
    Type,
    Coroutine
)

from .cursor import Cursor


class ContextManager:

    __slots__ = ('_coro', '_obj')

    def __init__(self, coro: Coroutine[Any, Any, Any]):
        self._coro = coro

    def __await__(self) -> Generator[Any, None, "ContextManager"]:
        return self._coro.__await__()

    async def __aenter__(self) -> Cursor:
        self._obj = await self._coro
        return self._obj

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType]
    ) -> None:
        await self._obj.close()


def contextmanager(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        return ContextManager(method(self, *args, **kwargs))
    return wrapper
