import sqlite3

from io import StringIO
from typing import Tuple


class Record:

    __slots__ = ('_records', '_row')

    def __init__(self, cursor: sqlite3.Cursor, row: Tuple) -> None:
        self._records = {column[0]: row[i] for i, column in enumerate(cursor.description)}
        self._row = row

    def get(self, name, default=None, /):
        return self._records.get(name, default)

    def items(self):
        return self._records.items()

    def keys(self):
        return self._records.keys()

    def values(self):
        return self._records.values()

    def __contains__(self, key):
        return key in self

    def __eq__(self, value):
        return self._row == value

    def __getattribute__(self, name):
        return object.__getattribute__(self, name)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._row[key]
        return self._records[key]

    def __ge__(self, value):
        return self._row >= value

    def __gt__(self, value):
        return self._row > value

    def __hash__(self):
        return object.__hash__(self)

    def __len__(self):
        return len(self._row)

    def __le__(self, value):
        return self._row <= value

    def __lt__(self, value):
        return self._row < value

    def __ne__(self, value):
        return self._row != value

    def __repr__(self):
        with StringIO() as f:
            f.write("<Record ")
            count = 0
            for key, value in self._records.items():
                count += 1
                if len(self._records) == count:
                    f.write(f"{key}={value}>")
                else:
                    f.write(f"{key}={value}, ")
            return f.getvalue()
