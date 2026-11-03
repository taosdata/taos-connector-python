# Copyright (c) 2021 TAOS Data, Inc.
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

from taos.cinterface import *
from taos.error import StatementError
from taos.result import TaosResult


class TaosStmt(object):
    """TDengine STMT interface"""

    def __init__(self, stmt, decode_binary=True, timezone=DEFAULT_TIMEZONE):
        self._stmt = stmt
        self._decode_binary = decode_binary
        self._timezone = timezone

    def set_tbname(self, name):
        """Set table name if needed.

        Note that the set_tbname* method should only used in insert statement
        """
        if self._stmt is None:
            raise StatementError("Invalid use of set_tbname")
        taos_stmt_set_tbname(self._stmt, name)

    def prepare(self, sql):
        # type: (str) -> None
        taos_stmt_prepare(self._stmt, sql)

    def set_tbname_tags(self, name, tags):
        # type: (str, Array[TaosBind]) -> None
        """Set table name with tags, tags is array of BindParams"""
        if self._stmt is None:
            raise StatementError("Invalid use of set_tbname")
        taos_stmt_set_tbname_tags(self._stmt, name, tags)

    def bind_param(self, params, add_batch=True):
        # type: (Array[TaosBind], bool) -> None
        if self._stmt is None:
            raise StatementError("Invalid use of stmt")
        taos_stmt_bind_param(self._stmt, params)
        if add_batch:
            taos_stmt_add_batch(self._stmt)

    def bind_param_batch(self, binds, add_batch=True):
        # type: (Array[TaosMultiBind], bool) -> None
        if self._stmt is None:
            raise StatementError("Invalid use of stmt")
        taos_stmt_bind_param_batch(self._stmt, binds)
        if add_batch:
            taos_stmt_add_batch(self._stmt)

    def add_batch(self):
        if self._stmt is None:
            raise StatementError("Invalid use of stmt")
        taos_stmt_add_batch(self._stmt)

    def execute(self):
        if self._stmt is None:
            raise StatementError("Invalid use of execute")
        taos_stmt_execute(self._stmt)

    def use_result(self):
        """NOTE: Don't use a stmt result more than once."""
        result = taos_stmt_use_result(self._stmt)
        return TaosResult(result, close_after=False, decode_binary=self._decode_binary, timezone=self._timezone)

    @property
    def affected_rows(self):
        # type: () -> int
        return taos_stmt_affected_rows(self._stmt)

    def close(self):
        """Close stmt."""
        if self._stmt is None:
            return
        taos_stmt_close(self._stmt)
        self._stmt = None

    def __del__(self):
        self.close()
