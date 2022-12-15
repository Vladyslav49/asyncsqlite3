import sqlite3

from typing import (
    AsyncIterator,
    Any,
    Iterable,
    Optional,
    Tuple,
    TYPE_CHECKING,
    Type
)

if TYPE_CHECKING:
    from .core import Connection

from .factory import Record


class Cursor:

    __slots__ = ('_prefetch', '_conn', '_cursor', '_closed', '_put')

    def __init__(
            self,
            conn: "Connection",
            cursor: sqlite3.Cursor,
            prefetch: Optional[int] = None
    ) -> None:
        if prefetch is None:
            self._prefetch = conn.prefetch
        else:
            self._prefetch = prefetch
        self._conn = conn
        self._cursor = cursor
        self._closed = False
        self._put = self._conn._put

    async def __aiter__(self) -> AsyncIterator[Record]:
        """Async iterator."""
        while True:
            rows = await self.fetchmany(self._prefetch)
            if not rows:
                return
            for row in rows:
                yield row

    async def execute(
            self,
            sql: str,
            parameters: Optional[Iterable[Any]] = None,
            *,
            timeout: Optional[float] = None
    ) -> "Cursor":
        """Execute the given query."""
        if parameters is None:
            parameters = []
        await self._put(self._cursor.execute, sql, parameters, timeout=timeout)
        return self

    async def executemany(
            self,
            sql: str,
            parameters: Iterable[Iterable[Any]],
            *,
            timeout: Optional[float] = None
    ) -> "Cursor":
        """Execute the given multiquery."""
        await self._put(self._cursor.executemany, sql, parameters, timeout=timeout)
        return self

    async def executescript(
            self,
            sql_script: str,
            *,
            timeout: Optional[float] = None
    ) -> "Cursor":
        """Execute a user script."""
        await self._put(self._cursor.executescript, sql_script, timeout=timeout)
        return self

    async def fetchone(
            self,
            *,
            timeout: Optional[float] = None,
            row_factory: Optional[Type] = Record
    ) -> Optional[Record]:
        """Fetch a single row."""
        self.row_factory = row_factory
        return await self._put(self._cursor.fetchone, timeout=timeout)

    async def fetchmany(
            self,
            size: Optional[int] = None,
            *,
            timeout: Optional[float] = None,
            row_factory: Optional[Type] = Record
    ) -> Iterable[Record]:
        """Fetch up to `cursor.arraysize` number of rows."""
        if size is None:
            size = self.arraysize
        self.row_factory = row_factory
        return await self._put(self._cursor.fetchmany, size, timeout=timeout)

    async def fetchall(
            self,
            *,
            timeout: Optional[float] = None,
            row_factory: Optional[Type] = Record
    ) -> Iterable[Record]:
        """Fetch all remaining rows."""
        self.row_factory = row_factory
        return await self._put(self._cursor.fetchall, timeout=timeout)

    async def close(self) -> None:
        """Close the cursor."""
        await self._put(self._cursor.close)
        self._closed = True

    @property
    def prefetch(self) -> int:
        return self._prefetch

    @prefetch.setter
    def prefetch(self, value: int) -> None:
        self._prefetch = value

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    @property
    def lastrowid(self) -> int:
        return self._cursor.lastrowid

    @property
    def arraysize(self) -> int:
        return self._cursor.arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._cursor.arraysize = value

    @property
    def description(self) -> Tuple[Tuple]:
        return self._cursor.description

    @property
    def connection(self) -> sqlite3.Connection:
        return self._cursor.connection

    @property
    def row_factory(self) -> Optional[Type]:
        return self._cursor.row_factory

    @row_factory.setter
    def row_factory(self, factory: Optional[Type]) -> None:
        self._cursor.row_factory = factory

    def __repr__(self) -> str:
        return f'<Cursor at {id(self):#x} {self._format()}>'

    def __str__(self) -> str:
        return f'<Cursor {self._format()}>'

    def _format(self) -> str:
        return f'connection={self._conn._name!r} closed={self._closed}'
