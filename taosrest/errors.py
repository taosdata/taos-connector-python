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

class Error(Exception):
    def __init__(self, msg=None, errno=0xFFFF, status=""):
        self.msg = msg
        self.errno = errno
        self.status = status
        if self.status != "":
            self._full_msg = "[%s]: %s" % (self.status, self.msg)
        else:
            self._full_msg = "[0x%04x]: %s" % (self.errno & 0xFFFF, self.msg)

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
