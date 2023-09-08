# encoding:UTF-8
from taos.cinterface import *
from taos.cursor import TaosCursor
from taos.subscription import TaosSubscription
from taos.statement import TaosStmt
from taos.result import TaosResult
from typing import Optional, List


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
        self.decode_binary = True
        self._init_config(**kwargs)
        self._chandle = CTaosInterface(self._config, self._tz)
        self._conn = self._chandle.connect(self._host, self._user, self._password, self._database, self._port)

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

    def select_db(self, database):
        # type: (str) -> None
        taos_select_db(self._conn, database)

    def execute(self, sql, req_id: Optional[int] = None):
        """Simplely execute sql ignoring the results"""
        return self.query(sql, req_id).affected_rows

    def query(self, sql: str, req_id: Optional[int] = None) -> TaosResult:
        if req_id is None:
            res = taos_query(self._conn, sql)
        else:
            res = taos_query_with_reqid(self._conn, sql, req_id)
        return TaosResult(res, close_after=True, decode_binary=self.decode_binary)

    def query_a(self, sql: str, callback: async_query_callback_type, param: c_void_p, req_id: Optional[int] = None):
        """Asynchronously query a sql with callback function"""
        if req_id is None:
            taos_query_a(self._conn, sql, callback, param)
        else:
            taos_query_a_with_reqid(self._conn, sql, callback, param, req_id)

    def subscribe(self, restart: bool, topic: str, sql: str, interval: int,
                  callback: Optional[subscribe_callback_type] = None,
                  param: Optional[c_void_p] = None) -> Optional[TaosSubscription]:

        """Create a subscription."""
        if self._conn is None:
            return None
        sub = taos_subscribe(self._conn, restart, topic, sql, c_int(interval), callback, param)
        if not sub:
            errno = taos_errno(c_void_p(None))
            msg = taos_errstr(c_void_p(None))
            raise Error(msg, errno)
        return TaosSubscription(sub, callback is not None)

    def statement(self, sql=None):
        # type: (str | None) -> TaosStmt|None
        if self._conn is None:
            return None
        stmt = taos_stmt_init(self._conn)
        if sql is not None:
            taos_stmt_prepare(stmt, sql)

        return TaosStmt(stmt, decode_binary=self.decode_binary)

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
