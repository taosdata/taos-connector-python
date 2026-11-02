# Copyright (c) 2022 TAOS Data, Inc.
#
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from .errors import *
from .restclient import RestClient
from typing import Optional


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
        self._description = None
        self._logfile = ""
        self._affected_rows = None

    @property
    def rowcount(self):
        """the number of rows that the last .execute*() produced (for DQL statements like SELECT) or affected (for DML statements like UPDATE or INSERT)."""
        return self._rowcount

    @property
    def affected_rows(self):
        """Return the rowcount of insertion. For SELECT statement, it will be None"""
        return self._affected_rows

    @property
    def description(self):
        """
        Returns
        -------
        This read-only attribute is a sequence of 3-item sequences.
        Each of these sequences contains information describing one column:
        - column_name
        - type_name
        - internal_size
        """
        if self._response is None:
            return None
        return self._description

    def callproc(self, procname, parameters=None):
        raise NotSupportedError()

    def close(self):
        pass

    def execute(self, operation: str, parameters=None, req_id: Optional[int] = None):
        self._response = None
        self._index = -1
        self._response = self._c.sql(operation, req_id=req_id)

        if self._logfile:
            with open(self._logfile, "a", encoding="utf-8") as logfile:
                logfile.write(f"{operation};\n")

        if self._response["column_meta"][0][0] == "affected_rows":
            # for INSERT
            self._description = self._response["column_meta"]
            self._affected_rows = self._response["data"][0][0]
            self._rowcount = self._affected_rows
            return self._affected_rows
        else:
            # for SELECT, Show, ...
            self._description = self._response["column_meta"]
            self._affected_rows = None
            self._rowcount = self._response["rows"]

    def log(self, logfile):
        self._logfile = logfile

    def executemany(self, operation: str, parameters=None, req_id: Optional[int] = None):
        self.execute(operation, parameters, req_id)

    def istype(self, col, datatype):
        if datatype.upper().strip() == self._description[col][1]:
            return True
        if datatype.upper().strip() == "BINARY" and self._description[col][1] == "VARCHAR":
            return True
        return False

    def get_type(self, col):
        if not self._description:
            return None
        return self._description[col][1]

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
