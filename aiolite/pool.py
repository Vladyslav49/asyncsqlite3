import asyncio
import sqlite3

from types import TracebackType
from typing import (
    Union,
    Optional,
    Type,
    Generator,
    Any,
    Iterable
)
from pathlib import Path

import async_timeout

from .core import Connection
from .cursor import Cursor
from .exceptions import PoolError
from .transaction import IsolationLevel
from .factory import Record


class ConnectionProxy(Connection):

    __slots__ = '_in_use'

    def __init__(self, *args: Any) -> None:
        super().__init__(*args)
        self._in_use: Optional[asyncio.Future] = None

    async def _wait_until_released(self) -> None:
        if self._in_use is not None:
            await self._in_use


def connect(
        database: Union[bytes, str, Path],
        *,
        timeout: float = 5.0,
        detect_types: int = 0,
        isolation_level: IsolationLevel = 'DEFERRED',
        check_same_thread: bool = False,
        factory: Type[Connection] = sqlite3.Connection,
        cached_statements: int = 128,
        uri: bool = False,
        prefetch: int = 64
) -> ConnectionProxy:
    """Create and return a connection to the sqlite database."""

    def _connector() -> sqlite3.Connection:
        if isinstance(database, str):
            loc = database
        elif isinstance(database, bytes):
            loc = database.decode('utf-8')
        else:
            loc = str(database)

        return sqlite3.connect(
            database=loc,
            timeout=timeout,
            detect_types=detect_types,
            isolation_level=None,
            check_same_thread=check_same_thread,
            factory=factory,
            cached_statements=cached_statements,
            uri=uri
        )

    return ConnectionProxy(_connector, isolation_level, prefetch)


class PoolAcquireContext:

    __slots__ = ('_pool', '_timeout', '_conn')

    def __init__(self, pool: "Pool", timeout: Optional[float]) -> None:
        self._pool = pool
        self._timeout = timeout
        self._conn = None

    async def __aenter__(self) -> ConnectionProxy:
        if self._conn is not None:
            raise PoolError('A connection is already acquired.')
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
        self._pool.release(conn)

    def __await__(self) -> ConnectionProxy:
        if self._conn is not None:
            raise PoolError('A connection is already acquired.')
        self._conn = yield from self._pool._acquire(timeout=self._timeout).__await__()
        return self._conn


class Pool:
    """A connection pool.

    Connection pool can be used to manage a set of connections to the database.
    Connections are first acquired from the pool, then used, and then released
    back to the pool. Once a connection is released, it's reset to close all
    open cursors and other resources *except* prepared statements.

    Pools are created by calling :func: `aiolite.create_pool`.
    """

    __slots__ = (
        '_database', '_min_size', '_max_size',
        '_connect_kwargs', '_initialized',
        '_initializing', '_all_connections',
        '_queue', '_end'
    )

    def __init__(
            self,
            database: Union[bytes, str, Path],
            min_size: int,
            max_size: int,
            **kwargs: Any
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
        self._connect_kwargs = kwargs
        self._initialized = False
        self._initializing = False
        self._all_connections = []
        self._queue = asyncio.Queue(maxsize=self.get_max_size())
        self._end = False

    def _create_new_connection(self) -> ConnectionProxy:
        conn = connect(
            self._database,
            **self._connect_kwargs
        )
        self._queue.put_nowait(conn)

        self._all_connections.append(conn)

        return conn

    async def _acquire(self, *, timeout: Optional[float]) -> ConnectionProxy:
        if self.is_closed():
            raise PoolError('Pool is closed.')

        if len(self._all_connections) < self.get_max_size():
            await self._create_new_connection()

        try:
            if timeout is not None:
                async with async_timeout.timeout(timeout):
                    conn = await self._queue.get()
            else:
                conn = await self._queue.get()
        except asyncio.TimeoutError:
            raise PoolError('There are no free connections in the pool.') from None
        else:
            if conn.is_closed():
                self._all_connections.remove(conn)
                return await self._acquire(timeout=timeout)
            conn._in_use = asyncio.get_event_loop().create_future()
            return conn

    def acquire(self, *, timeout: Optional[float] = None) -> PoolAcquireContext:
        """Acquire a database connection from the pool."""
        return PoolAcquireContext(self, timeout)

    def release(self, conn: ConnectionProxy) -> None:
        """Release a database connection back to the pool."""
        if self.is_closed():
            raise PoolError('Pool is closed.')
        if conn not in self._all_connections:
            raise PoolError('Connection not found in the pool.')
        if conn in self._queue._queue:
            raise PoolError('The connection is already in the pool.')
        if conn._in_use is None:
            raise PoolError('The connection is not currently used by the pool.')

        if not conn._in_use.done():
            conn._in_use.set_result(None)
        conn._in_use = None

        self._queue.put_nowait(conn)

    async def close(self) -> None:
        """Attempt to gracefully close all connections in the pool."""
        if self.is_closed():
            raise PoolError('Pool is closed.')

        await asyncio.gather(*[conn._wait_until_released() for conn in self._all_connections])
        await self.terminate()

    async def terminate(self) -> None:
        """Terminate all connections in the pool."""
        if self.is_closed():
            raise PoolError('Pool is closed.')

        try:
            await asyncio.gather(*[conn.close() for conn in self._all_connections])
        finally:
            self._end = True
            self._all_connections.clear()

    async def execute(
            self,
            sql: str,
            parameters: Optional[Iterable[Any]] = None,
            *,
            timeout: Optional[float] = None
    ) -> Cursor:
        """Pool performs this operation using one of its connections and Connection.transaction().
        Other than that, it behaves identically to Connection.execute().
        """
        async with self.acquire() as conn:
            async with conn.transaction():
                async with conn.execute(sql, parameters, timeout=timeout) as cursor:
                    return cursor

    async def executemany(
            self,
            sql: str,
            parameters: Iterable[Iterable[Any]],
            *,
            timeout: Optional[float] = None
    ) -> Cursor:
        """Pool performs this operation using one of its connections and Connection.transaction().
        Other than that, it behaves identically to Connection.executemany().
        """
        async with self.acquire() as conn:
            async with conn.transaction():
                async with conn.executemany(sql, parameters, timeout=timeout) as cursor:
                    return cursor

    async def executescript(
            self,
            sql_script: str,
            *,
            timeout: Optional[float] = None
    ) -> Cursor:
        """Pool performs this operation using one of its connections and Connection.transaction().
        Other than that, it behaves identically to Connection.executescript().
        """
        async with self.acquire() as conn:
            async with conn.transaction():
                async with conn.executescript(sql_script, timeout=timeout) as cursor:
                    return cursor

    async def fetchone(
            self,
            sql: str,
            parameters: Optional[Iterable[Any]] = None,
            *,
            timeout: Optional[float] = None,
            row_factory: Optional[Type] = Record
    ) -> Optional[Record]:
        """Pool performs this operation using one of its connections.
        Other than that, it behaves identically to Connection.fetchone().
        """
        async with self.acquire() as conn:
            async with conn.execute(sql, parameters, timeout=timeout) as cur:
                cur.row_factory = row_factory
                return await cur.fetchone()

    async def fetchmany(
            self,
            sql: str,
            parameters: Optional[Iterable[Any]] = None,
            *,
            size: Optional[int] = None,
            timeout: Optional[float] = None,
            row_factory: Optional[Type] = Record
    ) -> Iterable[Record]:
        """Pool performs this operation using one of its connections.
        Other than that, it behaves identically to Connection.fetchmany().
        """
        async with self.acquire() as conn:
            async with conn.execute(sql, parameters, timeout=timeout) as cur:
                cur.row_factory = row_factory
                return await cur.fetchmany(size)

    async def fetchall(
            self,
            sql: str,
            parameters: Optional[Iterable[Any]] = None,
            *,
            timeout: Optional[float] = None,
            row_factory: Optional[Type] = Record
    ) -> Iterable[Record]:
        """Pool performs this operation using one of its connections.
        Other than that, it behaves identically to Connection.fetchall().
        """
        async with self.acquire() as conn:
            async with conn.execute(sql, parameters, timeout=timeout) as cur:
                cur.row_factory = row_factory
                return await cur.fetchall()

    def get_max_size(self) -> int:
        """Return the maximum allowed number of connections in this pool."""
        return self._max_size

    def get_min_size(self) -> int:
        """Return the maximum allowed number of connections in this pool."""
        return self._min_size

    def get_size(self) -> int:
        """Return the current number of idle connections in this pool."""
        return self._queue.qsize()

    def is_closed(self) -> bool:
        return not self._all_connections and self._end

    async def _initialization(self) -> Optional["Pool"]:
        """Connect to the sqlite database and put connection in pool."""
        if self._initialized:
            return
        if self._initializing:
            raise PoolError('Pool initialization is already in progress.')
        if self.is_closed():
            raise PoolError('Pool is closed.')
        self._initializing = True
        try:
            for _ in range(self.get_min_size()):
                self._create_new_connection()

            await asyncio.gather(*self._all_connections)

            return self
        finally:
            self._initializing = False
            self._initialized = True

    def __repr__(self) -> str:
        return f'<Pool at {id(self):#x} {self._format()}>'

    def __str__(self) -> str:
        return f'<Pool {self._format()}>'

    def _format(self) -> str:
        return f'size={self.get_size()} min_size={self.get_min_size()} max_size={self.get_max_size()} closed={self.is_closed()}'

    def __await__(self) -> Generator[Any, None, "Pool"]:
        return self._initialization().__await__()

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
        database: Union[bytes, str, Path],
        *,
        min_size: int = 10,
        max_size: int = 10,
        **kwargs: Any
) -> Pool:
    """Create and return a connection pool."""
    return Pool(database, min_size, max_size, **kwargs)
