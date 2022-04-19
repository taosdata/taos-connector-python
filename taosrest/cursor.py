# For API reference, please refer to: https://peps.python.org/pep-0249/#cursor-objects

from .errors import *


class TaosRestCursor:
    def __init__(self):
        self.description = []
        self.rowcount = -1
        self.arraysize = 1

    def callproc(self, procname, parameters=None):
        raise NotSupportedError()

    def close(self):
        pass

    def execute(self, operation, parameters=None):
        pass

    def executemany(self, operation, parameters=None):
        raise NotSupportedError()

    def fetchone(self):
        pass

    def fetchmany(self):
        pass

    def fetchall(self):
        pass

    def nextset(self):
        raise NotSupportedError()

    def setinputsizes(self):
        raise NotSupportedError()

    def setoutputsize(self, size, column=None):
        raise NotSupportedError()

    def setoutputsize(self, size, column=None):
        raise NotSupportedError()
