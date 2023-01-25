import asyncio
import sqlite3

from itertools import repeat
from types import TracebackType
from typing import (
    Optional,
    Type,
    Generator,
    Any,
    Iterable
)

import async_timeout

from .core import Connection, DatabasePath
from .cursor import Cursor
from .exceptions import PoolError
from .transaction import IsolationLevel
from .factory import Record
from .context import contextmanager


class ConnectionProxy(Connection):

    __slots__ = '_in_use'

    def __init__(self, *args: Any) -> None:
        super().__init__(*args)
        self._in_use: Optional[asyncio.Future] = None

    def _acquire(self) -> None:
        self._in_use = asyncio.get_running_loop().create_future()

    def _release(self) -> None:
        if not self._in_use.done():
            self._in_use.set_result(None)
        self._in_use = None

    async def _wait_until_released(self) -> None:
        if self._in_use is not None:
            await self._in_use


def connect(
        database: DatabasePath,
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

    __slots__ = ('_pool', '_timeout', '_conn', '_done')

    def __init__(self, pool: "Pool", timeout: Optional[float]) -> None:
        self._pool = pool
        self._timeout = timeout
        self._conn = None
        self._done = False

    async def _acquire(self) -> ConnectionProxy:
        if self._conn is not None or self._done:
            raise PoolError('A connection is already acquired.')
        self._done = True
        self._conn = await self._pool._acquire(timeout=self._timeout)
        return self._conn

    def _release(self) -> None:
        conn = self._conn
        self._conn = None
        self._pool.release(conn)

    async def __aenter__(self) -> ConnectionProxy:
        return await self

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType]
    ) -> None:
        self._release()

    def __await__(self) -> Generator[Any, None, "ConnectionProxy"]:
        return self._acquire().__await__()


class Pool:
    """A connection pool.

    Connection pool can be used to manage a set of connections to the database.
    Connections are first acquired from the pool, then used, and then released
    back to the pool. Once a connection is released, it's reset to close all
    open cursors and other resources *except* prepared statements.

    Pools are created by calling :func: `asyncsqlite3.create_pool`.
    """

    __slots__ = (
        '_database', '_min_size', '_max_size',
        '_close_timeout', '_connect_kwargs',
        '_initialized', '_initializing',
        '_all_connections', '_queue', '_closed'
    )

    def __init__(
            self,
            database: DatabasePath,
            min_size: int,
            max_size: int,
            close_timeout: Optional[float],
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
        self._close_timeout = close_timeout
        self._connect_kwargs = kwargs
        self._initialized = False
        self._initializing = False
        self._all_connections = []
        self._queue = asyncio.LifoQueue(maxsize=self.get_max_size())
        self._closed = False

    def _create_new_connection(self) -> ConnectionProxy:
        conn = connect(self._database, **self._connect_kwargs)

        self._all_connections.append(conn)

        return conn

    async def _get_new_connection_if_current_closed(self, conn: ConnectionProxy) -> ConnectionProxy:
        if conn.is_closed():
            self._all_connections.remove(conn)
            conn = await self._create_new_connection()
        return conn

    async def _acquire(self, *, timeout: Optional[float]) -> ConnectionProxy:
        if self.is_closed():
            raise PoolError('Pool is closed.')

        if len(self._all_connections) < self.get_max_size():
            conn = await self._create_new_connection()
            conn._acquire()
            return conn

        try:
            if timeout is not None:
                async with async_timeout.timeout(timeout):
                    conn = await self._queue.get()
            else:
                conn = await self._queue.get()
        except asyncio.TimeoutError:
            raise PoolError('There are no free connections in the pool.') from None
        else:
            conn = await self._get_new_connection_if_current_closed(conn)
            conn._acquire()
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

        conn._release()

        self._queue.put_nowait(conn)

    async def close(self) -> None:
        """Attempt to gracefully close all connections in the pool."""
        if self.is_closed():
            raise PoolError('Pool is closed.')

        try:
            if self._close_timeout is not None:
                async with async_timeout.timeout(self._close_timeout):
                    await self._wait_all_connections()
            else:
                await self._wait_all_connections()
            await self.terminate()
        except asyncio.TimeoutError:
            await self.terminate()
            raise

    async def terminate(self) -> None:
        """Terminate all connections in the pool."""
        if self.is_closed():
            raise PoolError('Pool is closed.')

        try:
            await asyncio.gather(*[conn.close() for conn in self._all_connections])
        finally:
            self._closed = True
            self._all_connections.clear()

    @contextmanager
    async def execute(
            self,
            sql: str,
            parameters: Optional[Iterable[Any]] = None,
            *,
            timeout: Optional[float] = None
    ) -> Cursor:
        """Pool performs this operation using one of its connections.
        Other than that, it behaves identically to Connection.execute().
        """
        async with self.acquire() as conn:
            return await conn.execute(sql, parameters, timeout=timeout)

    @contextmanager
    async def executemany(
            self,
            sql: str,
            parameters: Iterable[Iterable[Any]],
            *,
            timeout: Optional[float] = None
    ) -> Cursor:
        """Pool performs this operation using one of its connections.
        Other than that, it behaves identically to Connection.executemany().
        """
        async with self.acquire() as conn:
            return await conn.executemany(sql, parameters, timeout=timeout)

    @contextmanager
    async def executescript(
            self,
            sql_script: str,
            *,
            timeout: Optional[float] = None
    ) -> Cursor:
        """Pool performs this operation using one of its connections.
        Other than that, it behaves identically to Connection.executescript().
        """
        async with self.acquire() as conn:
            return await conn.executescript(sql_script, timeout=timeout)

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
            return await conn.fetchone(sql, parameters, timeout=timeout, row_factory=row_factory)

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
            return await conn.fetchmany(sql, parameters, size=size, timeout=timeout, row_factory=row_factory)

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
            return await conn.fetchall(sql, parameters, timeout=timeout, row_factory=row_factory)

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
        return not self._all_connections and self._closed

    async def _wait_all_connections(self) -> None:
        await asyncio.gather(*[conn._wait_until_released() for conn in self._all_connections])

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
            for _ in repeat(None, self.get_min_size()):
                self._queue.put_nowait(self._create_new_connection())

            await asyncio.gather(*self._all_connections)

            return self
        finally:
            self._initializing = False
            self._initialized = True

    def _format(self) -> str:
        return 'size={} min_size={} max_size={} closed={}'.format(
            self.get_size(), self.get_min_size(), self.get_max_size(), self.is_closed())

    def __repr__(self) -> str:
        return f'<Pool at {id(self):#x} {self._format()}>'

    def __str__(self) -> str:
        return f'<Pool {self._format()}>'

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
        database: DatabasePath,
        *,
        min_size: int = 10,
        max_size: int = 10,
        close_timeout: Optional[float] = None,
        **kwargs: Any
) -> Pool:
    """Create and return a connection pool."""
    return Pool(database, min_size, max_size,
                close_timeout, **kwargs)
