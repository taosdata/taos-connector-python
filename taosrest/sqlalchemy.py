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

import sys
from sqlalchemy import types as sqltypes
from sqlalchemy.engine import default, reflection
from sqlalchemy import text

# taos.sqlalchemy.BaseDialect
import taos

#
# ---------------- taos rest impl -------------
#
import taos.sqlalchemy
import taosrest


# rest connect
class AlchemyRestConnection:
    threadsafety = 0
    paramstyle = "pyformat"
    Error = taosrest.Error

    # connect
    def connect(self, **kwargs):
        host = kwargs["host"] if "host" in kwargs else None
        port = kwargs["port"] if "port" in kwargs else None
        user = kwargs["username"] if "username" in kwargs else "root"
        password = kwargs["password"] if "password" in kwargs else "taosdata"
        database = kwargs["database"] if "database" in kwargs else None
        token = kwargs["token"] if "token" in kwargs else None

        if not host:
            host = "localhost"
        if host == "localhost" and not port:
            port = 6041

        url = f"http://{host}"
        if port:
            url += f":{port}"
        return taosrest.connect(url=url, user=user, password=password, database=database, token=token)


# rest dialect
class TaosRestDialect(taos.sqlalchemy.BaseDialect):
    name = "taosrest"
    driver = "taosrest"

    @classmethod
    def dbapi(cls):
        return AlchemyRestConnection()

    @classmethod
    def import_dbapi(cls):
        return AlchemyRestConnection()
