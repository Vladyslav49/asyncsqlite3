import sys

from enum import Enum
from types import TracebackType
from typing import Optional, Type, TYPE_CHECKING, NoReturn

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

import async_timeout

if TYPE_CHECKING:
    from .core import Connection

from .exceptions import TransactionError

IsolationLevel = Optional[Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"]]


class TransactionState(Enum):
    NEW = 0
    STARTED = 1
    COMMITTED = 2
    ROLLEDBACK = 3
    FAILED = 4


class Transaction:
    """
    Asyncio Transaction for sqlite3.
    """

    __slots__ = (
        '_conn', '_isolation_level', '_managed',
        '_state', '_timeout_handler'
    )

    def __init__(
            self,
            conn: "Connection",
            isolation_level: IsolationLevel,
            timeout: Optional[float]
    ) -> None:

        if isolation_level is None:
            isolation_level = 'DEFERRED'

        if timeout is not None:
            timeout = async_timeout.timeout(timeout)

        self._conn = conn
        self._isolation_level = isolation_level
        self._timeout_handler = timeout
        self._managed = False
        self._state = TransactionState.NEW

    def _check_state(self, operation: str) -> Optional[NoReturn]:
        if self._state is not TransactionState.STARTED:
            if self._state is TransactionState.NEW:
                raise TransactionError(
                    f'cannot {operation}; the transaction is not yet started')
            if self._state is TransactionState.COMMITTED:
                raise TransactionError(
                    f'cannot {operation}; the transaction is already committed')
            if self._state is TransactionState.ROLLEDBACK:
                raise TransactionError(
                    f'cannot {operation}; the transaction is already rolled back')
            if self._state is TransactionState.FAILED:
                raise TransactionError(
                    f'cannot {operation}; the transaction is in error state')

    async def _start(self) -> None:
        """Enter the transaction or savepoint block."""
        if self._state is TransactionState.STARTED:
            raise TransactionError(
                'cannot start; the transaction is already started')

        try:
            async with self._conn.cursor() as cursor:
                await cursor.execute(f'BEGIN {self._isolation_level} TRANSACTION;')
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.STARTED

    async def _commit(self) -> None:
        """Exit the transaction or savepoint block and commit changes."""
        self._check_state('commit')

        try:
            await self._conn.commit()
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.COMMITTED

    async def _rollback(self) -> None:
        """Exit the transaction or savepoint block and rollback changes."""
        self._check_state('rollback')

        try:
            await self._conn.rollback()
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.ROLLEDBACK

    def _timeout_available(self) -> bool:
        return isinstance(self._timeout_handler, async_timeout.Timeout)

    @property
    def state(self) -> str:
        return self._state.name

    @property
    def isolation_level(self) -> IsolationLevel:
        return self._isolation_level

    async def __aenter__(self) -> "Transaction":
        if self._managed:
            raise TransactionError(
                'cannot enter context: already in an `async with` block')
        else:
            self._managed = True
        await self._start()

        if self._timeout_available():
            self._timeout_handler._do_enter()

        return self

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType]
    ) -> None:
        try:
            if exc_type is not None:
                await self._rollback()
            else:
                await self._commit()
        finally:
            self._managed = False
            if self._timeout_available():
                self._timeout_handler._do_exit(exc_type)

    def __repr__(self) -> str:
        return f'<Transaction at {id(self):#x} {self._format()}>'

    def __str__(self) -> str:
        return f'<Transaction {self._format()}>'

    def _format(self) -> str:
        return f'state={self._state.name!r} isolation_level={self._isolation_level!r}'
