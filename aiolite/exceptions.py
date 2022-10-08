# sqlite3
class Warning(Exception):
    pass


class Error(Exception):
    pass


class DatabaseError(Error):
    pass


class InterfaceError(Error):
    pass


class DataError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


# transaction
class TransactionError(Exception):
    pass


# pools
class PoolError(Exception):
    pass
