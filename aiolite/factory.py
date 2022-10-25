from sqlite3 import Row


class Record(Row):
    def get(self, name, default=None, /):
        try:
            return self[name]
        except IndexError:
            return default

    def __repr__(self) -> str:
        result = f'<{type(self).__name__}'
        for key, value in dict(self).items():
            result += f' {key}={value!r}'
        return f'{result}>'
