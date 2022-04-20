from .errors import *
from .restclient import RestClient


class TaosRestCursor:
    """
    Implement [PEP 249 cursor API](https://peps.python.org/pep-0249/#cursor-objects)
    """

    def __init__(self, client: RestClient):
        self._c = client
        self.arraysize = 1
        self._rowcount = -1
        self._response = None
        self._index = -1

    @property
    def rowcount(self):
        return self._rowcount

    @property
    def description(self):
        if self._response is None:
            return None
        return self._response["column_meta"]

    def callproc(self, procname, parameters=None):
        raise NotSupportedError()

    def close(self):
        pass

    def execute(self, operation, parameters=None):
        self._response = None
        self._index = -1
        self._response = self._c.sql(operation)

    def executemany(self, operation, parameters=None):
        self.execute(operation)

    def fetchone(self):
        if self._response is None:
            raise OperationalError("no result to fetch")
        self._index += 1
        if self._index + 1 > self._response["rows"]:
            return None
        return self._response["data"][self._index]

    def fetchmany(self):
        return self.fetchone()

    def fetchall(self):
        if self._response is None:
            raise OperationalError("no result to fetch")
        start_index = self._index + 1
        self._index = self._response["rows"]
        return self._response["data"][start_index:]

    def nextset(self):
        raise NotSupportedError()

    def setinputsizes(self):
        raise NotSupportedError()

    def setoutputsize(self, size, column=None):
        raise NotSupportedError()

    def setoutputsize(self, size, column=None):
        raise NotSupportedError()
