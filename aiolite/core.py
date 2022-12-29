import asyncio
import sqlite3
import sys

from logging import getLogger
from functools import partial
from pathlib import Path
from types import TracebackType
from typing import (
    Union,
    Optional,
    Iterable,
    Any,
    Callable,
    Type,
    AsyncIterator,
    Generator
)
from warnings import warn
from threading import Thread
from queue import SimpleQueue, Empty

import async_timeout

from .cursor import Cursor
from .context import contextmanager
from .factory import Record
from .transaction import Transaction, IsolationLevel
from .exceptions import (
    Warning,
    Error,
    DatabaseError,
    DataError,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError
)

logger = getLogger(__name__)

_count = 0


def _new_connection() -> int:
    global _count
    _count += 1
    return _count


class Connection:

    __slots__ = (
        '_name', '_conn', '_connector',
        '_isolation_level', '_prefetch',
        '_queue', '_end'
    )

    def __init__(
            self,
            connector: Callable[[], sqlite3.Connection],
            isolation_level: IsolationLevel,
            prefetch: int
    ) -> None:
        self._name = f'aiolite-connection-{_new_connection()}'

        self._conn: Optional[sqlite3.Connection] = None
        self._connector = connector

        self._isolation_level = isolation_level
        self._prefetch = prefetch

        self._queue = SimpleQueue()
        self._end = False

    def _handler(self) -> None:
        """Execute function calls on a separate thread."""
        while not self.is_closed():
            # Continues running until all queue items are processed,
            # even after connection is closed (so we can finalize all futures)
            try:
                future, function = self._queue.get(timeout=0.1)
            except Empty:
                pass
            else:
                try:
                    logger.debug('executing: %s', function)

                    try:
                        result = function()
                    except sqlite3.IntegrityError as error:
                        raise IntegrityError(error) from None
                    except sqlite3.NotSupportedError as error:
                        raise NotSupportedError(error) from None
                    except sqlite3.DataError as error:
                        raise DataError(error) from None
                    except sqlite3.InterfaceError as error:
                        raise InterfaceError(error) from None
                    except sqlite3.InternalError as error:
                        raise InternalError(error) from None
                    except sqlite3.ProgrammingError as error:
                        raise ProgrammingError(error) from None
                    except sqlite3.OperationalError as error:
                        raise OperationalError(error) from None
                    except sqlite3.DatabaseError as error:
                        raise DatabaseError(error) from None
                    except sqlite3.Error as error:
                        raise Error(error) from None
                    except sqlite3.Warning as error:
                        raise Warning(error) from None

                    logger.debug('operation %s completed', function)

                    future.get_loop().call_soon_threadsafe(future.set_result, result)
                except BaseException as error:
                    logger.debug('returning exception: %s', error)

                    future.get_loop().call_soon_threadsafe(future.set_exception, error)

    async def _put(self, func, *args, timeout=None, **kwargs):
        """Queue a function with the given arguments for execution."""
        function = partial(func, *args, **kwargs)

        future = asyncio.get_event_loop().create_future()

        self._queue.put((future, function))

        if timeout is not None:
            async with async_timeout.timeout(timeout):
                result = await future
        else:
            result = await future

        return result

    @contextmanager
    async def cursor(
            self,
            *,
            row_factory: Optional[Type] = Record,
            prefetch: Optional[int] = None
    ) -> Cursor:
        """Create an aiolite cursor wrapping a sqlite3 cursor object."""
        cursor = Cursor(self, await self._put(self._conn.cursor), prefetch)
        cursor.row_factory = row_factory
        return cursor

    @contextmanager
    async def execute(
            self,
            sql: str,
            parameters: Optional[Iterable[Any]] = None,
            *,
            timeout: Optional[float] = None
    ) -> Cursor:
        """Helper to create a cursor and execute the given query."""
        if parameters is None:
            parameters = []
        cursor = await self._put(self._conn.execute, sql, parameters, timeout=timeout)
        return Cursor(self, cursor)

    @contextmanager
    async def executemany(
            self,
            sql: str,
            parameters: Iterable[Iterable[Any]],
            *,
            timeout: Optional[float] = None
    ) -> Cursor:
        """Helper to create a cursor and execute the given multiquery."""
        cursor = await self._put(self._conn.executemany, sql, parameters, timeout=timeout)
        return Cursor(self, cursor)

    @contextmanager
    async def executescript(
            self,
            sql_script: str,
            *,
            timeout: Optional[float] = None
    ) -> Cursor:
        """Helper to create a cursor and execute a user script."""
        cursor = await self._put(self._conn.executescript, sql_script, timeout=timeout)
        return Cursor(self, cursor)

    async def fetchone(
            self,
            sql: str,
            parameters: Optional[Iterable[Any]] = None,
            *,
            timeout: Optional[float] = None,
            row_factory: Optional[Type] = Record
    ) -> Optional[Record]:
        """Shortcut version of aiolite.Cursor.fetchone."""
        async with self.execute(sql, parameters, timeout=timeout) as cur:
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
        """Shortcut version of aiolite.Cursor.fetchmany."""
        async with self.execute(sql, parameters, timeout=timeout) as cur:
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
        """Shortcut version of aiolite.Cursor.fetchall."""
        async with self.execute(sql, parameters, timeout=timeout) as cur:
            cur.row_factory = row_factory
            return await cur.fetchall()

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self._put(self._conn.commit)

    async def rollback(self) -> None:
        """Roll back the current transaction."""
        await self._put(self._conn.rollback)

    async def close(self) -> None:
        """Complete queued queries/cursors and close the connection."""
        if not self.is_closed():
            try:
                await self._put(self._conn.close)
            finally:
                self._end = True
                self._conn = None

    async def interrupt(self) -> None:
        """Interrupt pending queries."""
        return self._conn.interrupt()

    async def enable_load_extension(self, value: bool) -> None:
        await self._put(self._conn.enable_load_extension, value)  # type: ignore

    async def load_extension(self, path: str) -> None:
        await self._put(self._conn.load_extension, path)  # type: ignore

    async def set_progress_handler(
            self, handler: Callable[[], Optional[int]], n: int
    ) -> None:
        await self._put(self._conn.set_progress_handler, handler, n)

    async def set_trace_callback(self, handler: Callable) -> None:
        await self._put(self._conn.set_trace_callback, handler)

    async def create_function(
            self, name: str, num_params: int, func: Callable, deterministic: bool = False
    ) -> None:
        """
        Create user-defined function that can be later used
        within SQL statements. Must be run within the same thread
        that query executions take place so instead of executing directly
        against the connection, we defer this to `run` function.
        In Python 3.8 and above, if *deterministic* is true, the created
        function is marked as deterministic, which allows SQLite to perform
        additional optimizations. This flag is supported by SQLite 3.8.3 or
        higher, ``NotSupportedError`` will be raised if used with older
        versions.
        """
        if sys.version_info >= (3, 8):
            await self._put(
                self._conn.create_function,
                name,
                num_params,
                func,
                deterministic=deterministic,
            )
        else:
            if deterministic:
                warn(
                    "Deterministic function support is only available on "
                    'Python 3.8+. Function "{}" will be registered as '
                    "non-deterministic as per SQLite defaults.".format(name)
                )

            await self._put(self._conn.create_function, name, num_params, func)

    async def iterdump(self) -> AsyncIterator[str]:
        """
        Return an async iterator to dump the database in SQL text format.
        Example::
            async for line in db.iterdump():
                ...
        """
        for line in await self._put(self._conn.iterdump):
            yield line

    async def backup(
            self,
            target: Union["Connection", sqlite3.Connection],
            *,
            pages: int = 0,
            progress: Optional[Callable[[int, int, int], None]] = None,
            name: str = "main",
            sleep: float = 0.250
    ) -> None:
        """
        Make a backup of the current database to the target database.

        Takes either a standard sqlite3 or aiolite Connection object as the target.
        """
        if isinstance(target, Connection):
            target = target._conn

        await self._put(
            self._conn.backup,
            target,
            pages=pages,
            progress=progress,
            name=name,
            sleep=sleep,
        )

    def transaction(
            self,
            isolation_level: IsolationLevel = None,
            *,
            timeout: Optional[float] = None
    ) -> Transaction:
        """Gets a transaction object."""
        if isolation_level is None:
            isolation_level = self._isolation_level
        return Transaction(self, isolation_level, timeout)

    def is_closed(self) -> bool:
        return self._end and self._conn is None

    @property
    def prefetch(self) -> int:
        return self._prefetch

    @prefetch.setter
    def prefetch(self, value: int) -> None:
        self._prefetch = value

    @property
    def in_transaction(self) -> bool:
        return self._conn.in_transaction

    @property
    def isolation_level(self) -> IsolationLevel:
        return self._isolation_level

    @isolation_level.setter
    def isolation_level(self, value: IsolationLevel) -> None:
        self._isolation_level = value

    @property
    def row_factory(self) -> Optional[Type]:
        return self._conn.row_factory

    @row_factory.setter
    def row_factory(self, factory: Optional[Type]) -> None:
        self._conn.row_factory = factory

    @property
    def text_factory(self) -> Type:
        return self._conn.text_factory

    @text_factory.setter
    def text_factory(self, factory: Type) -> None:
        self._conn.text_factory = factory

    @property
    def total_changes(self) -> int:
        return self._conn.total_changes

    async def _initialization(self) -> "Connection":
        """Connect to the sqlite database."""
        if self._conn is None:
            try:
                Thread(
                    target=self._handler,
                    name=self._name,
                    daemon=True
                ).start()

                self._conn = await self._put(self._connector)
            except BaseException:
                self._end = True
                self._conn = None
                raise

        return self

    def __repr__(self) -> str:
        return f'<Connection at {id(self):#x} {self._format()}>'

    def __str__(self) -> str:
        return f'<Connection {self._format()}>'

    def _format(self) -> str:
        return f'name={self._name!r} closed={self.is_closed()}'

    def __await__(self) -> Generator[Any, None, "Connection"]:
        return self._initialization().__await__()

    async def __aenter__(self) -> "Connection":
        return await self

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType]
    ) -> None:
        await self.close()


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
) -> Connection:
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

    return Connection(_connector, isolation_level, prefetch)
