# Copyright (c) 2019 TAOS Data, Inc.
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

# encoding:UTF-8

import re
from typing import Optional, List
import pytz
from tzlocal.windows_tz import win_tz

from taos.cinterface import *
from taos.constants import TSDB_CONNECTIONS_MODE, TSDB_OPTION_CONNECTION
from taos.result import TaosResult
from taos.cursor import TaosCursor
from taos.subscription import TaosSubscription
from taos.statement import TaosStmt
from taos.statement2 import TaosStmt2, TaosStmt2Option


class TaosConnection(object):
    """TDengine connection object"""

    def __init__(self, *args, **kwargs):
        self._conn = None
        self._host = None
        self._user = "root"
        self._password = "taosdata"
        self._database = None
        self._port = 0
        self._config = None
        self._tz = None
        self._timezone = None
        self.decode_binary = True
        self._charset = None
        self._user_app = None
        self._user_ip = None
        self._bi_mode = None
        self._totp_code = None
        self._bearer_token = None

        self._init_config(**kwargs)
        self._chandle = CTaosInterface(self._config, self._tz)

        if self._bearer_token:
            self._conn = self._chandle.connect_token(self._host, self._bearer_token, self._database, self._port)
        elif self._totp_code:
            self._conn = self._chandle.connect_totp(
                self._host, self._user, self._password, self._totp_code, self._database, self._port
            )
        else:
            self._conn = self._chandle.connect(self._host, self._user, self._password, self._database, self._port)

        if self._charset is not None:
            self.set_option(TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_CHARSET.value, self._charset)

        if self._user_app is not None:
            self.set_option(TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_USER_APP.value, self._user_app)

        if self._user_ip is not None:
            self.set_option(TSDB_OPTION_CONNECTION.TSDB_OPTION_CONNECTION_USER_IP.value, self._user_ip)

        if self._bi_mode is not None:
            if self._bi_mode:
                self.set_mode(TSDB_CONNECTIONS_MODE.TSDB_CONNECTIONS_MODE_BI.value, 1)
            else:
                self.set_mode(TSDB_CONNECTIONS_MODE.TSDB_CONNECTIONS_MODE_BI.value, 0)

        self._sync_python_timezone()

    def _init_config(self, **kwargs):
        # host
        if "host" in kwargs:
            self._host = kwargs["host"]

        # user
        if "user" in kwargs:
            self._user = kwargs["user"]

        # password
        if "password" in kwargs:
            self._password = kwargs["password"]

        # database
        if "database" in kwargs:
            self._database = kwargs["database"]

        # port
        if "port" in kwargs:
            self._port = kwargs["port"]

        # config
        if "config" in kwargs:
            self._config = kwargs["config"]

        # timezone
        if "timezone" in kwargs:
            self._tz = kwargs["timezone"]

        if "decode_binary" in kwargs:
            self.decode_binary = kwargs["decode_binary"]

        if "charset" in kwargs:
            self._charset = kwargs["charset"]

        if "user_app" in kwargs:
            self._user_app = kwargs["user_app"]

        if "user_ip" in kwargs:
            self._user_ip = kwargs["user_ip"]

        if "bi_mode" in kwargs:
            self._bi_mode = kwargs["bi_mode"]

        if "totp_code" in kwargs:
            self._totp_code = kwargs["totp_code"]

        if "bearer_token" in kwargs:
            self._bearer_token = kwargs["bearer_token"]

    def close(self):
        """Close current connection."""
        if self._conn:
            taos_close(self._conn)
            self._conn = None

    @property
    def client_info(self):
        # type: () -> str
        return taos_get_client_info()

    @property
    def server_info(self):
        # type: () -> str
        return taos_get_server_info(self._conn)

    def set_option(self, option, value):
        taos_options_connection(self._conn, option, value)

    def _set_python_timezone(self, tz):
        if tz is None:
            self._timezone = None
            return

        if isinstance(tz, bytes):
            tz = tz.decode("utf-8")
        self._timezone = self._parse_python_timezone(str(tz))

    @staticmethod
    def _parse_python_timezone(value):
        # Preserve Windows names such as "Central Standard Time (Mexico)"
        # while discarding TDengine's trailing display data "(CST, +0800)".
        tz_name = re.sub(r"\s+\([^()]*,\s*[+-]\d{4}\)$", "", value).strip()
        if tz_name in win_tz:
            tz_name = win_tz[tz_name]

        match = re.fullmatch(r"([+-])(\d{2}):?(\d{2})?", tz_name)
        if match is not None:
            sign, hours, minutes = match.groups()
            hours = int(hours)
            minutes = int(minutes or 0)
            if hours > 23 or minutes > 59:
                raise pytz.UnknownTimeZoneError(tz_name)
            offset = hours * 60 + minutes
            return pytz.FixedOffset(offset if sign == "+" else -offset)

        match = re.fullmatch(r"UTC([+-])(\d{1,2})(?::?(\d{2}))?", tz_name, re.IGNORECASE)
        if match is not None:
            sign, hours, minutes = match.groups()
            hours = int(hours)
            minutes = int(minutes or 0)
            if hours > 23 or minutes > 59:
                raise pytz.UnknownTimeZoneError(tz_name)
            offset = hours * 60 + minutes
            # TDengine's UTC offsets use POSIX signs: UTC-8 is UTC+08:00.
            return pytz.FixedOffset(-offset if sign == "+" else offset)

        return pytz.timezone(tz_name)

    @staticmethod
    def _is_set_timezone_statement(sql):
        if not isinstance(sql, str):
            return False

        tokens = []
        index = 0
        length = len(sql)
        while index < length and len(tokens) < 3:
            if sql[index].isspace():
                index += 1
                continue
            if sql.startswith("--", index):
                newline = sql.find("\n", index + 2)
                index = length if newline == -1 else newline + 1
                continue
            if sql.startswith("/*", index):
                comment_end = sql.find("*/", index + 2)
                if comment_end == -1:
                    return False
                index = comment_end + 2
                continue
            if sql[index] in ("'", '"'):
                quote = sql[index]
                index += 1
                start = index
                while index < length and sql[index] != quote:
                    index += 1
                if index == length:
                    return False
                tokens.append(sql[start:index].upper())
                index += 1
                continue
            if not (sql[index].isalpha() or sql[index] == "_"):
                return False
            start = index
            index += 1
            while index < length and (sql[index].isalnum() or sql[index] == "_"):
                index += 1
            tokens.append(sql[start:index].upper())

        return tokens[:2] == ["SET", "TIMEZONE"] or tokens == ["ALTER", "LOCAL", "TIMEZONE"]

    def _sync_python_timezone(self):
        if self._conn is None:
            return

        res = taos_query(self._conn, "select timezone()")
        try:
            errno = taos_errno(res)
            if errno != 0:
                raise ProgrammingError(taos_errstr(res), errno)

            fields = taos_fetch_result_fields(res)
            # Connection state must always use text, independently from how callers
            # request BINARY query results.
            block, num_of_rows = taos_fetch_block(res, fields, decode_binary=True)
            errno = taos_errno(res)
            if errno != 0:
                raise ProgrammingError(taos_errstr(res), errno)
            if num_of_rows == 0:
                return

            timezone_value = block[0][0]
            if timezone_value is not None:
                self._set_python_timezone(timezone_value)
        finally:
            taos_free_result(res)

    def set_mode(self, mode, value):
        taos_set_conn_mode(self._conn, mode, value)

    def select_db(self, database):
        # type: (str) -> None
        taos_select_db(self._conn, database)

    def execute(self, sql, req_id: Optional[int] = None):
        """Simplely execute sql ignoring the results"""
        return self.query(sql, req_id).affected_rows

    def query(self, sql: str, req_id: Optional[int] = None) -> TaosResult:
        should_sync_timezone = self._is_set_timezone_statement(sql)
        if req_id is None:
            res = taos_query(self._conn, sql)
        else:
            res = taos_query_with_reqid(self._conn, sql, req_id)
        if should_sync_timezone and taos_errno(res) == 0:
            self._sync_python_timezone()
        return TaosResult(res, close_after=True, decode_binary=self.decode_binary, timezone=self._timezone)

    def query_a(self, sql: str, callback: async_query_callback_type, param: c_void_p, req_id: Optional[int] = None):
        """Asynchronously query a sql with callback function"""
        if req_id is None:
            taos_query_a(self._conn, sql, callback, param)
        else:
            taos_query_a_with_reqid(self._conn, sql, callback, param, req_id)

    def subscribe(
        self,
        restart: bool,
        topic: str,
        sql: str,
        interval: int,
        callback: Optional[subscribe_callback_type] = None,
        param: Optional[c_void_p] = None,
    ) -> Optional[TaosSubscription]:
        """Create a subscription."""
        if self._conn is None:
            return None
        sub = taos_subscribe(self._conn, restart, topic, sql, c_int(interval), callback, param)
        if not sub:
            errno = taos_errno(c_void_p(None))
            msg = taos_errstr(c_void_p(None))
            raise Error(msg, errno)
        return TaosSubscription(sub, callback is not None, timezone=self._timezone)

    def statement(self, sql=None):
        # type: (str | None) -> TaosStmt|None
        if self._conn is None:
            return None
        stmt = taos_stmt_init(self._conn)
        if sql is not None:
            taos_stmt_prepare(stmt, sql)

        return TaosStmt(stmt, decode_binary=self.decode_binary, timezone=self._timezone)

    def statement2(self, sql=None, option=None):
        # type: (str | None, TaosStmt2Option | None) -> TaosStmt2|None
        if self._conn is None:
            return None
        if option is not None:
            option = option.get_impl()
        _stmt2 = taos_stmt2_init(self._conn, option)
        stmt2 = TaosStmt2(_stmt2, decode_binary=self.decode_binary, timezone=self._timezone)
        if sql is not None:
            stmt2.prepare(sql)

        return stmt2

    def load_table_info(self, tables):
        # type: (str) -> None
        taos_load_table_info(self._conn, tables)

    def schemaless_insert(
        self,
        lines: List[str],
        protocol: SmlProtocol,
        precision: SmlPrecision,
        req_id: Optional[int] = None,
        ttl: Optional[int] = None,
    ) -> int:
        """
        1.Line protocol and schemaless support

        ## Example

        ```python
        import taos
        conn = taos.connect()
        conn.exec("drop database if exists test")
        conn.select_db("test")
        lines = [
            'ste,t2=5,t3=L"ste" c1=true,c2=4,c3="string" 1626056811855516532',
        ]
        conn.schemaless_insert(lines, 0, "ns")
        ```

        2.OpenTSDB telnet style API format support

        ## Example

        ```python
        import taos
        conn = taos.connect()
        conn.exec("drop database if exists test")
        conn.select_db("test")
        lines = [
            'cpu_load 1626056811855516532ns 2.0f32 id="tb1",host="host0",interface="eth0"',
        ]
        conn.schemaless_insert(lines, 1, None)
        ```

        3.OpenTSDB HTTP JSON format support

        ## Example

        ```python
        import taos
        conn = taos.connect()
        conn.exec("drop database if exists test")
        conn.select_db("test")
        payload = ['''
        {
            "metric": "cpu_load_0",
            "timestamp": 1626006833610123,
            "value": 55.5,
            "tags":
                {
                    "host": "ubuntu",
                    "interface": "eth0",
                    "Id": "tb0"
                }
        }
        ''']
        conn.schemaless_insert(lines, 2, None)
        ```
        """
        if ttl is None:
            if req_id is None:
                return taos_schemaless_insert(
                    self._conn,
                    lines,
                    protocol,
                    precision,
                )
            else:
                return taos_schemaless_insert_with_reqid(
                    self._conn,
                    lines,
                    protocol,
                    precision,
                    req_id,
                )
        else:
            if req_id is None:
                return taos_schemaless_insert_ttl(
                    self._conn,
                    lines,
                    protocol,
                    precision,
                    ttl,
                )
            else:
                return taos_schemaless_insert_ttl_with_reqid(
                    self._conn,
                    lines,
                    protocol,
                    precision,
                    ttl,
                    req_id,
                )
                pass

    def schemaless_insert_raw(
        self,
        lines: str,
        protocol: SmlProtocol,
        precision: SmlPrecision,
        req_id: Optional[int] = None,
        ttl: Optional[int] = None,
    ) -> int:
        """
        1.Line protocol and schemaless support

        ## Example

        ```python
        import taos
        conn = taos.connect()
        conn.exec("drop database if exists test")
        conn.select_db("test")
        lines = 'ste,t2=5,t3=L"ste" c1=true,c2=4,c3="string" 1626056811855516532'
        conn.schemaless_insert_raw(lines, 0, "ns")
        ```

        2.OpenTSDB telnet style API format support

        ## Example

        ```python
        import taos
        conn = taos.connect()
        conn.exec("drop database if exists test")
        conn.select_db("test")
        lines = 'cpu_load 1626056811855516532ns 2.0f32 id="tb1",host="host0",interface="eth0"'
        conn.schemaless_insert_raw(lines, 1, None)
        ```

        3.OpenTSDB HTTP JSON format support

        ## Example

        ```python
        import taos
        conn = taos.connect()
        conn.exec("drop database if exists test")
        conn.select_db("test")
        payload = '''
        {
            "metric": "cpu_load_0",
            "timestamp": 1626006833610123,
            "value": 55.5,
            "tags":
                {
                    "host": "ubuntu",
                    "interface": "eth0",
                    "Id": "tb0"
                }
        }
        '''
        conn.schemaless_insert_raw(lines, 2, None)
        ```
        """
        if ttl is None:
            if req_id is None:
                return taos_schemaless_insert_raw(
                    self._conn,
                    lines,
                    protocol,
                    precision,
                )
            else:
                return taos_schemaless_insert_raw_with_reqid(
                    self._conn,
                    lines,
                    protocol,
                    precision,
                    req_id,
                )
        else:
            if req_id is None:
                return taos_schemaless_insert_raw_ttl(
                    self._conn,
                    lines,
                    protocol,
                    precision,
                    ttl,
                )
            else:
                return taos_schemaless_insert_raw_ttl_with_reqid(
                    self._conn,
                    lines,
                    protocol,
                    precision,
                    ttl,
                    req_id,
                )

    def cursor(self):
        # type: () -> TaosCursor
        """Return a new Cursor object using the connection."""
        return TaosCursor(self, decode_binary=self.decode_binary)

    def commit(self):
        """Commit any pending transaction to the database.

        Since TDengine do not support transactions, the implement is void functionality.
        """
        pass

    def rollback(self):
        """Void functionality"""
        pass

    def clear_result_set(self):
        """Clear unused result set on this connection."""
        pass

    def __del__(self):
        self.close()

    def get_table_vgroup_id(self, db, table):
        # type: (str, str) -> int
        """
        get table's vgroup id. It's require db name and table name, and return an int type vgroup id.
        """
        return taos_get_table_vgId(self._conn, db, table)


if __name__ == "__main__":
    conn = TaosConnection()
    conn.close()
