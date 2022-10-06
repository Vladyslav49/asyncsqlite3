import sqlite3

from typing import AsyncIterator, Any, Iterable, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .core import Connection

from .factory import Record


class Cursor:

    __slots__ = ("_iter_chunk_size", "_conn", "_cursor")

    def __init__(self, conn: "Connection", cursor: sqlite3.Cursor) -> None:
        self._iter_chunk_size = conn._iter_chunk_size
        self._conn = conn
        self._cursor = cursor

    async def __aiter__(self) -> AsyncIterator[Record]:
        """Async iterator."""
        while True:
            rows = await self.fetchmany(self._iter_chunk_size)
            if not rows:
                return
            for row in rows:
                yield row

    async def execute(self, sql: str, parameters: Iterable[Any] = None) -> "Cursor":
        """Execute the given query."""
        if parameters is None:
            parameters = []
        await self._conn._put(self._cursor.execute, sql, parameters)
        return self

    async def executemany(self, sql: str, parameters: Iterable[Iterable[Any]]) -> "Cursor":
        """Execute the given multiquery."""
        await self._conn._put(self._cursor.executemany, sql, parameters)
        return self

    async def executescript(self, sql_script: str) -> "Cursor":
        """Execute a user script."""
        await self._conn._put(self._cursor.executescript, sql_script)
        return self

    async def fetchone(self) -> Optional[Record]:
        """Fetch a single row."""
        return await self._conn._put(self._cursor.fetchone)

    async def fetchmany(self, size: int = None) -> Iterable[Record]:
        """Fetch up to `cursor.arraysize` number of rows."""
        if size is None:
            size = self.arraysize
        return await self._conn._put(self._cursor.fetchmany, size)

    async def fetchall(self) -> Iterable[Record]:
        """Fetch all remaining rows."""
        return await self._conn._put(self._cursor.fetchall)

    async def close(self) -> None:
        """Close the cursor."""
        await self._conn._put(self._cursor.close)

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
