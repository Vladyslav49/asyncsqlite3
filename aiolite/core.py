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
from threading import Thread, Event
from queue import Queue, Empty

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

num = 0


def get_loop(future: asyncio.Future) -> asyncio.AbstractEventLoop:
    if sys.version_info >= (3, 7):
        return future.get_loop()
    else:
        return future._loop


def get_thread_number() -> int:
    global num
    num += 1
    return num


class Connection(Thread):

    __slots__ = (
        "_loop", "_conn", "_connector",
        "_row_factory",  "_isolation_level",
        "_iter_chunk_size", "_queue", "_event"
    )

    def __init__(
            self,
            connector: Callable[[], sqlite3.Connection],
            loop: asyncio.AbstractEventLoop,
            row_factory: bool,
            isolation_level: IsolationLevel,
            iter_chunk_size: int
    ) -> None:
        super().__init__(name=f"aiolite-{get_thread_number()}", daemon=True)
        self._loop = loop

        self._conn: Optional[sqlite3.Connection] = None
        self._connector = connector

        self._row_factory = row_factory
        self._isolation_level = isolation_level
        self._iter_chunk_size = iter_chunk_size

        self._queue = Queue()
        self._event = Event()

    def run(self) -> None:
        """Execute function calls on a separate thread."""
        while not self._event.is_set():
            # Continues running until all queue items are processed,
            # even after connection is closed (so we can finalize all futures)
            try:
                future, function = self._queue.get(timeout=0.1)
            except Empty:
                pass
            else:
                try:
                    logger.debug("executing: %s", function)

                    try:
                        result = function()
                    except sqlite3.IntegrityError as error:
                        raise IntegrityError(error)
                    except sqlite3.NotSupportedError as error:
                        raise NotSupportedError(error)
                    except sqlite3.DataError as error:
                        raise DataError(error)
                    except sqlite3.InterfaceError as error:
                        raise InterfaceError(error)
                    except sqlite3.InternalError as error:
                        raise InternalError(error)
                    except sqlite3.ProgrammingError as error:
                        raise ProgrammingError(error)
                    except sqlite3.OperationalError as error:
                        raise OperationalError(error)
                    except sqlite3.DatabaseError as error:
                        raise DatabaseError(error)
                    except sqlite3.Error as error:
                        raise Error(error)
                    except sqlite3.Warning as error:
                        raise Warning(error)

                    logger.debug("operation %s completed", function)

                    get_loop(future).call_soon_threadsafe(future.set_result, result)
                except BaseException as error:
                    logger.debug("returning exception: %s", error)

                    get_loop(future).call_soon_threadsafe(future.set_exception, error)

    async def _put(self, func, *args, **kwargs):
        """Queue a function with the given arguments for execution."""
        function = partial(func, *args, **kwargs)

        future = self._loop.create_future()

        self._queue.put_nowait((future, function))

        return await future

    @contextmanager
    async def cursor(self) -> Cursor:
        """Create an aiolite cursor wrapping a sqlite3 cursor object."""
        return Cursor(self, await self._put(self._conn.cursor))

    @contextmanager
    async def execute(self, sql: str, parameters: Iterable[Any] = None) -> Cursor:
        """Helper to create a cursor and execute the given query."""
        if parameters is None:
            parameters = []
        cursor = await self._put(self._conn.execute, sql, parameters)
        return Cursor(self, cursor)

    @contextmanager
    async def executemany(self, sql: str, parameters: Iterable[Iterable[Any]]) -> Cursor:
        """Helper to create a cursor and execute the given multiquery."""
        cursor = await self._put(self._conn.executemany, sql, parameters)
        return Cursor(self, cursor)

    @contextmanager
    async def executescript(self, sql_script: str) -> Cursor:
        """Helper to create a cursor and execute a user script."""
        cursor = await self._put(self._conn.executescript, sql_script)
        return Cursor(self, cursor)

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self._put(self._conn.commit)

    async def rollback(self) -> None:
        """Roll back the current transaction."""
        await self._put(self._conn.rollback)

    async def close(self) -> None:
        """Complete queued queries/cursors and close the connection."""
        if not self._event.is_set() and self._conn is not None:
            try:
                await self._put(self._conn.close)
            finally:
                self._event.set()
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
        queue = Queue()

        def dumper():
            try:
                for line in self._conn.iterdump():
                    queue.put_nowait(line)
                queue.put_nowait(None)

            except Exception:
                logger.exception("exception while dumping db")
                queue.put_nowait(None)
                raise

        fut = self._put(dumper)
        task = asyncio.ensure_future(fut)

        while True:
            try:
                line: Optional[str] = queue.get_nowait()
                if line is None:
                    break
                yield line

            except Empty:
                if task.done():
                    logger.warning("iterdump completed unexpectedly")
                    break

                await asyncio.sleep(0.01)

        await task

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
        if sys.version_info < (3, 7):
            raise NotSupportedError("backup() method is only available on Python 3.7+")

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

    def transaction(self) -> Transaction:
        """Gets a transaction object."""
        return Transaction(self, self.isolation_level)

    def is_closed(self) -> bool:
        return self._event.is_set() and self._conn is None

    @property
    def iter_chunk_size(self) -> int:
        return self._iter_chunk_size

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

    async def connector(self) -> "Connection":
        """Connect to the sqlite database."""
        if self._conn is None:
            try:
                self.start()

                self._conn = await self._put(self._connector)

                if self._row_factory is True:
                    self._conn.row_factory = Record
            except Exception:
                self._conn = None
                raise

        return self

    def __await__(self) -> Generator[Any, None, "Connection"]:
        return self.connector().__await__()

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
        isolation_level: IsolationLevel = "DEFERRED",
        check_same_thread: bool = True,
        factory: Type[Connection] = sqlite3.Connection,
        cached_statements: int = 128,
        uri: bool = False,
        row_factory: bool = True,
        iter_chunk_size: int = 64,
        loop: Optional[asyncio.AbstractEventLoop] = None
) -> Connection:
    """Create and return a connection to the sqlite database."""

    if loop is None:
        loop = asyncio.get_event_loop()

    def connector() -> sqlite3.Connection:
        if isinstance(database, str):
            loc = database
        elif isinstance(database, bytes):
            loc = database.decode("utf-8")
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

    return Connection(connector, loop, row_factory,
                      isolation_level, iter_chunk_size)
