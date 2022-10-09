import asyncio

from queue import Queue, Empty
from threading import Event
from types import TracebackType
from typing import Union, Optional, Type, Generator, Any, Iterable
from pathlib import Path

from . import Connection, Cursor, connect
from .exceptions import PoolError


class PoolAcquireContext:

    __slots__ = ('_pool', '_timeout', '_conn')

    def __init__(self, pool: "Pool", timeout: float) -> None:
        self._pool = pool
        self._timeout = timeout
        self._conn = None

    async def __aenter__(self) -> Connection:
        if self._conn is not None:
            raise PoolError("a connection is already acquired")
        self._conn = await self._pool._acquire(timeout=self._timeout)
        return self._conn

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType]
    ) -> None:
        conn = self._conn
        self._conn = None
        await self._pool.release(conn)

    def __await__(self):
        return self._pool._acquire(timeout=self._timeout).__await__()


class Pool:
    """A connection pool.

    Connection pool can be used to manage a set of connections to the database.
    Connections are first acquired from the pool, then used, and then released
    back to the pool. Once a connection is released, it's reset to close all
    open cursors and other resources *except* prepared statements.

    Pools are created by calling :func: `aiolite.create_pool`.
    """

    __slots__ = (
        '_database', '_min_size', '_max_size', '_loop',
        '_row_factory', '_iter_chunk_size', '_connect_kwargs',
        '_all_connections', '_waiters', '_event'
    )

    def __init__(
            self,
            database: Union[str, Path],
            min_size: int,
            max_size: int,
            row_factory: bool,
            iter_chunk_size: int,
            loop: Optional[asyncio.AbstractEventLoop],
            **kwargs
    ) -> None:

        if max_size <= 0:
            raise ValueError('max_size is expected to be greater than zero')

        if min_size < 0:
            raise ValueError(
                'min_size is expected to be greater or equal to zero')

        if min_size > max_size:
            raise ValueError('min_size is greater than max_size')

        self._database = database
        self._min_size = min_size
        self._max_size = max_size
        self._loop = loop
        self._row_factory = row_factory
        self._iter_chunk_size = iter_chunk_size
        self._connect_kwargs = kwargs
        self._all_connections = []
        self._waiters = Queue(maxsize=self._max_size)
        self._event = Event()

    async def _acquire(self, *, timeout: float) -> Connection:
        if len(self._all_connections) < self._max_size:
            conn = await connect(
                self._database,
                **self._connect_kwargs,
                row_factory=self._row_factory,
                iter_chunk_size=self._iter_chunk_size,
                loop=self._loop
            )
            self._waiters.put_nowait(conn)

            self._all_connections.append(conn)

        if self._event.is_set():
            raise PoolError("pool is closed")
        else:
            try:
                return self._waiters.get(timeout=timeout)
            except Empty:
                raise PoolError("There are no free connections in the pool.")

    def acquire(self, *, timeout: float = 10.0) -> PoolAcquireContext:
        """Acquire a database connection from the pool."""
        return PoolAcquireContext(self, timeout)

    async def release(self, conn: Connection) -> None:
        """Release a database connection back to the pool."""
        if conn not in self._all_connections:
            raise PoolError("Connection not found.")
        elif str(conn) not in str(self._waiters):
            self._waiters.put_nowait(conn)
        elif str(conn) in str(self._waiters):
            raise PoolError("The connection is already in the pool.")

    async def close(self) -> None:
        """Attempt to gracefully close all connections in the pool."""
        if self._pool_is_full():
            for conn in self._all_connections:
                await conn.close()
        if not self._event.is_set():
            self._event.set()
            self._all_connections.clear()
        else:
            raise PoolError("Pool already is closed.")

    async def terminate(self) -> None:
        """Terminate all connections in the pool."""
        for conn in self._all_connections:
            await conn.close()
        if not self._event.is_set():
            self._event.set()
            self._all_connections.clear()
        else:
            raise PoolError("Pool already is closed.")

    async def execute(self, sql: str, parameters: Iterable[Any] = None) -> Cursor:
        """Pool performs this operation using one of its connections and Connection.transaction() to auto-commit.

        Other than that, it behaves identically to Connection.execute().
        """
        async with self.acquire() as conn:
            async with conn.transaction():
                return await conn.execute(sql, parameters)

    async def executemany(self, sql: str, parameters: Iterable[Iterable[Any]]) -> Cursor:
        """Pool performs this operation using one of its connections and Connection.transaction() to auto-commit.

        Other than that, it behaves identically to Connection.executemany().
        """
        async with self.acquire() as conn:
            async with conn.transaction():
                return await conn.executemany(sql, parameters)

    async def executescript(self, sql_script: str) -> Cursor:
        """Pool performs this operation using one of its connections and Connection.transaction() to auto-commit.

        Other than that, it behaves identically to Connection.executescript().
        """
        async with self.acquire() as conn:
            async with conn.transaction():
                return await conn.executescript(sql_script)

    def get_max_size(self) -> int:
        """Return the maximum allowed number of connections in this pool."""
        return self._max_size

    def get_min_size(self) -> int:
        """Return the maximum allowed number of connections in this pool."""
        return self._min_size

    def get_size(self) -> int:
        """Return the current number of idle connections in this pool."""
        return self._waiters.qsize()

    def _pool_is_full(self) -> bool:
        while not self._event.is_set():
            if len(self._all_connections) == self._waiters.qsize():
                return True
        else:
            return False

    async def connector(self) -> "Pool":
        """Connect to the sqlite database and put connection in pool."""
        if self._waiters.empty():
            for _ in range(self._min_size):
                conn = await connect(
                    self._database,
                    **self._connect_kwargs,
                    row_factory=self._row_factory,
                    iter_chunk_size=self._iter_chunk_size,
                    loop=self._loop
                )
                self._waiters.put_nowait(conn)

                self._all_connections.append(conn)

        return self

    def __repr__(self):
        return f'<{type(self).__name__} at {id(self):#x} {self._format()}>'

    def __str__(self):
        return f'<{type(self).__name__} {self._format()}>'

    def _format(self):
        return f'size={self.get_size()!r} min_size={self.get_min_size()!r} max_size={self.get_max_size()!r}'

    def __await__(self) -> Generator[Any, None, "Pool"]:
        return self.connector().__await__()

    async def __aenter__(self) -> "Pool":
        return await self

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType]
    ) -> None:
        await self.close()


def create_pool(
        database: Union[str, Path],
        *,
        min_size=10,
        max_size=10,
        row_factory: bool = True,
        iter_chunk_size: int = 64,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        **kwargs
) -> Pool:
    """Create and return a connection pool.

    :param database:
        Path to the database file.

    :param min_size:
        Number of connection the pool will be initialized with.

    :param max_size:
        Max number of connections in the pool.

    :param row_factory:
        aiolite.Record factory to all connections of the pool.
    """

    return Pool(database, min_size, max_size, row_factory,
                iter_chunk_size, loop, **kwargs)
