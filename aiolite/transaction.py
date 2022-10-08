import sys

from enum import Enum
from types import TracebackType
from typing import Optional, Type, TYPE_CHECKING

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

if TYPE_CHECKING:
    from .core import Connection

from .exceptions import TransactionError

from .lock import RLock

lock = RLock()

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

    __slots__ = ("_conn", "_isolation", "_managed", "_state")

    def __init__(self, conn: "Connection", isolation: IsolationLevel) -> None:
        self._conn = conn
        self._isolation = isolation
        self._managed = False
        self._state = TransactionState.NEW

    def _check_state(self, operation: str) -> None:
        if self._state is not TransactionState.STARTED:
            if self._state is TransactionState.NEW:
                raise TransactionError(
                    "cannot {}; the transaction is not yet started".format(
                        operation))
            if self._state is TransactionState.COMMITTED:
                raise TransactionError(
                    "cannot {}; the transaction is already committed".format(
                        operation))
            if self._state is TransactionState.ROLLEDBACK:
                raise TransactionError(
                    "cannot {}; the transaction is already rolled back".format(
                        operation))
            if self._state is TransactionState.FAILED:
                raise TransactionError(
                    "cannot {}; the transaction is in error state".format(
                        operation))

    async def start(self) -> None:
        """Enter the transaction or savepoint block."""
        if self._state is TransactionState.STARTED:
            raise TransactionError(
                "cannot start; the transaction is already started")

        try:
            if self._isolation is None:
                await self._conn.execute("BEGIN TRANSACTION;")
            else:
                await self._conn.execute(f"BEGIN {self._isolation} TRANSACTION;")
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.STARTED

    async def _commit(self) -> None:
        """Exit the transaction or savepoint block and commit changes."""
        self._check_state("commit")

        try:
            await self._conn.commit()
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.COMMITTED

    async def _rollback(self) -> None:
        """Exit the transaction or savepoint block and rollback changes."""
        self._check_state("rollback")

        try:
            await self._conn.rollback()
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.ROLLEDBACK

    async def __aenter__(self) -> None:
        if self._managed:
            raise TransactionError(
                "cannot enter context: already in an `async with` block")
        else:
            self._managed = True
        await lock.acquire()
        await self.start()

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType],
    ) -> None:
        try:
            if exc_type is not None:
                await self._rollback()
            else:
                await self._commit()
        finally:
            self._managed = False
            lock.release()
