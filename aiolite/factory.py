from sqlite3 import Row
from io import StringIO


class Record(Row):
    def get(self, name, default=None, /):
        try:
            return self[name]
        except IndexError:
            return default

    def __repr__(self):
        with StringIO() as f:
            f.write("<Record ")
            count = 0
            for key, value in dict(self).items():
                count += 1
                if len(dict(self)) == count:
                    f.write(f"{key}={value}>")
                else:
                    f.write(f"{key}={value} ")
            return f.getvalue()

