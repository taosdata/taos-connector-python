class Error(Exception):
    def __init__(self, msg=None, errno=0xffff):
        self.msg = msg
        self.errno = errno
        self._full_msg = "[0x%04x]: %s" % (self.errno & 0xffff, self.msg)

    def __str__(self):
        return self._full_msg


class ExecutionError(Error):
    """Run sql error"""
    pass


class HTTPError(Exception):
    def __init__(self, status_code, msg=None):
        self.msg = msg
        self.status_code = status_code
        self._full_msg = f"HTTP response code {status_code}"
        if msg:
            self._full_msg += "." + msg

    def __str__(self):
        return self._full_msg


class ConnectError(Error):
    """Exception raised for connection failed"""
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass
