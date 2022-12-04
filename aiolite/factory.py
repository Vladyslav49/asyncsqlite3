from sqlite3 import Row
from typing import Hashable, Any


class Record(Row):
    def get(self, name: Hashable, default: Any = None, /) -> Any:
        try:
            return self[name]
        except IndexError:
            return default

    def __repr__(self) -> str:
        result = f'<Record'
        for key, value in dict(self).items():
            result += f' {key}={value!r}'
        return f'{result}>'
