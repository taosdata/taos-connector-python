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


class ConnectionError(Error):
    """Exception raised for connection failed"""
    pass
