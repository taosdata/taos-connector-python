# cython: profile=True

from libc.stdlib cimport malloc, free
from libc.string cimport memset, strcpy, memcpy
import asyncio
import datetime as dt
import pytz
from typing import Optional, List, Tuple, Dict, Iterator, AsyncIterator, Callable, Union
from taos._cinterface cimport *
from taos._cinterface import SIZED_TYPE, UNSIZED_TYPE, CONVERT_FUNC
from taos._parser cimport _convert_timestamp_to_datetime
from taos._constants import TaosOption, SmlPrecision, SmlProtocol, TmqResultType, PrecisionEnum
from taos.error import ProgrammingError, OperationalError, ConnectionError, DatabaseError, StatementError, InternalError, TmqError, SchemalessError
from taos.constants import FieldType

DB_MAX_LEN = 64
DEFAULT_TZ = None
DEFAULT_DT_EPOCH = dt.datetime.fromtimestamp(0, tz=DEFAULT_TZ)


cdef _check_malloc(void *ptr):
    if ptr is NULL:
        raise MemoryError()


# ---------------------------------------------- TAOS -------------------------------------------------------------- v
cdef void async_result_future_wrapper(void *param, TAOS_RES *res, int code) with gil:
    fut = <object>param
    if code != 0:
        errstr = taos_errstr(res).decode("utf-8")
        e = ProgrammingError(errstr, code)
        fut.get_loop().call_soon_threadsafe(fut.set_exception, e)
    else:
        taos_result = TaosResult(<size_t>res)
        fut.get_loop().call_soon_threadsafe(fut.set_result, taos_result)


cdef void async_rows_future_wrapper(void *param, TAOS_RES *res, int num_of_rows) with gil:
    cdef int i = 0
    taos_result, fut = <tuple>param
    rows = []
    if num_of_rows > 0:
        while True:
            row = taos_result._fetch_row()
            rows.append(row)
            i += 1
            if i >= num_of_rows:
                break

    fut.get_loop().call_soon_threadsafe(fut.set_result, rows)


cdef void async_block_future_wrapper(void *param, TAOS_RES *res, int num_of_rows) with gil:
    cdef int i = 0
    taos_result, fut = <tuple>param
    if num_of_rows > 0:
        block, n = taos_result._fetch_block()
    else:
        block, n = [], 0

    fut.get_loop().call_soon_threadsafe(fut.set_result, (block, n))


cdef class TaosConnection:
    cdef char *_host
    cdef char *_user
    cdef char *_password
    cdef char *_database
    cdef uint16_t _port
    # cdef char *_raw_tz  # can not name as _timezone
    cdef char *_config
    cdef TAOS *_raw_conn
    cdef char *_current_db
    cdef object _tz
    cdef object _dt_epoch

    def __cinit__(self, *args, **kwargs):
        self._current_db = <char*>malloc(DB_MAX_LEN * sizeof(char))
        self._tz = DEFAULT_TZ
        self._dt_epoch = DEFAULT_DT_EPOCH
        _check_malloc(self._current_db)
        self._init_options(**kwargs)
        self._init_conn(**kwargs)
        self._check_conn_error()

    def _init_options(self, **kwargs):
        if "timezone" in kwargs:
            _timezone = kwargs["timezone"].encode("utf-8")
            taos_options(TaosOption.Timezone, <char*>_timezone)
            self.tz = kwargs["timezone"]

        if "config" in kwargs:
            _config = kwargs["config"].encode("utf-8")
            self._config = _config
            taos_options(TaosOption.ConfigDir, self._config)

    def _init_conn(self, **kwargs):
        if "host" in kwargs:
            _host = kwargs["host"].encode("utf-8")
            self._host = _host

        if "user" in kwargs:
            _user = kwargs["user"].encode("utf-8")
            self._user = _user

        if "password" in kwargs:
            _password = kwargs["password"].encode("utf-8")
            self._password = _password

        if "database" in kwargs:
            _database = kwargs["database"].encode("utf-8")
            self._database = _database

        if "port" in kwargs:
            self._port = int(kwargs.get("port", 0))

        self._raw_conn = taos_connect(self._host, self._user, self._password, self._database, self._port)

    def _check_conn_error(self):
        errno = taos_errno(self._raw_conn)
        if errno != 0:
            errstr = taos_errstr(self._raw_conn).decode("utf-8")
            raise ConnectionError(errstr, errno)

    def __dealloc__(self):
        if self._current_db is not NULL:
            free(self._current_db)
        
        self.close()

    @property
    def client_info(self) -> str:
        return taos_get_client_info().decode("utf-8")

    @property
    def server_info(self) -> Optional[str]:
        if self._raw_conn is NULL:
            return

        return taos_get_server_info(self._raw_conn).decode("utf-8")

    @property
    def tz(self) -> Optional[dt.tzinfo]:
        return self._tz

    @tz.setter
    def tz(self, timezone: Optional[Union[str, dt.tzinfo]]):
        if isinstance(timezone, str):
            timezone = pytz.timezone(timezone)
        
        assert timezone is None or isinstance(timezone, dt.tzinfo)

        self._tz = timezone
        self._dt_epoch = dt.datetime.fromtimestamp(0, tz=self._tz)

    @property
    def current_db(self) -> Optional[str]:
        if self._raw_conn is NULL:
            return

        memset(self._current_db, 0, DB_MAX_LEN * sizeof(char))
        cdef int required
        errno = taos_get_current_db(self._raw_conn, self._current_db, DB_MAX_LEN * sizeof(char), &required)
        if errno != 0:
            errstr = taos_errstr(NULL).decode("utf-8")
            raise ProgrammingError(errstr, errno)

        return self._current_db.decode("utf-8")

    def validate_sql(self, str sql) -> bool:
        _sql = sql.encode("utf-8")
        errno = taos_validate_sql(self._raw_conn, _sql)
        return errno == 0

    def select_db(self, str database):
        _database = database.encode("utf-8")
        errno = taos_select_db(self._raw_conn, _database)
        if errno != 0:
            raise DatabaseError("select database error", errno)

    def execute(self, str sql, req_id: Optional[int] = None) -> int:
        return self.query(sql, req_id).affected_rows

    def query(self, str sql, req_id: Optional[int] = None) -> TaosResult:
        _sql = sql.encode("utf-8")
        if req_id is None:
            res = taos_query(self._raw_conn, _sql)
        else:
            res = taos_query_with_reqid(self._raw_conn, _sql, req_id)

        errno = taos_errno(res)
        if errno != 0:
            errstr = taos_errstr(res).decode("utf-8")
            taos_free_result(res)
            raise ProgrammingError(errstr, errno)

        taos_res = TaosResult(<size_t>res)
        taos_res._set_dt_epoch(self._dt_epoch)
        return taos_res

    async def query_a(self, str sql, req_id: Optional[int] = None) -> TaosResult:
        loop = asyncio.get_event_loop()
        _sql = sql.encode("utf-8")
        fut = loop.create_future()
        if req_id is None:
            taos_query_a(self._raw_conn, _sql, async_result_future_wrapper, <void*>fut)
        else:
            taos_query_a_with_reqid(self._raw_conn, _sql, async_result_future_wrapper, <void*>fut, req_id)

        taos_res = await fut
        taos_res._set_dt_epoch(self._dt_epoch)
        taos_res._mark_async()
        return taos_res

    def statement(self, sql: Optional[str]=None) -> Optional[TaosStmt]:
        if self._raw_conn is NULL:
            return None

        stmt = taos_stmt_init(self._raw_conn)
        if stmt is NULL:
            raise StatementError("init taos statement failed!")

        if sql:
            _sql = sql.encode("utf-8")
            errno = taos_stmt_prepare(stmt, _sql, len(_sql))
            if errno != 0:
                stmt_errstr = taos_stmt_errstr(stmt).decode("utf-8")
                raise StatementError(stmt_errstr, errno)

        return TaosStmt(<size_t>stmt)
        
    def load_table_info(self, tables: List[str]):
        _tables = ",".join(tables).encode("utf-8")
        errno = taos_load_table_info(self._raw_conn, _tables)
        if errno != 0:
            errstr = taos_errstr(NULL).decode("utf-8")
            raise OperationalError(errstr, errno)

    def close(self):
        if self._raw_conn is not NULL:
            taos_close(self._raw_conn)
            self._raw_conn = NULL

    def schemaless_insert(
            self,
            lines: List[str],
            protocol: int,
            precision: int,
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
        num_of_lines = len(lines)
        _lines = <char**>malloc(num_of_lines * sizeof(char*))
        if _lines is NULL:
            raise MemoryError()

        try:
            for i in range(num_of_lines):
                _line = lines[i].encode("utf-8")
                _lines[i] = _line

            if ttl is None:
                if req_id is None:
                    res = taos_schemaless_insert(
                        self._raw_conn,
                        _lines,
                        num_of_lines,
                        protocol,
                        precision,
                    )
                else:
                    res = taos_schemaless_insert_with_reqid(
                        self._raw_conn,
                        _lines,
                        num_of_lines,
                        protocol,
                        precision,
                        req_id,
                    )
            else:
                if req_id is None:
                    res = taos_schemaless_insert_ttl(
                        self._raw_conn,
                        _lines,
                        num_of_lines,
                        protocol,
                        precision,
                        ttl,
                    )
                else:
                    res = taos_schemaless_insert_ttl_with_reqid(
                        self._raw_conn,
                        _lines,
                        num_of_lines,
                        protocol,
                        precision,
                        ttl,
                        req_id,
                    )
        finally:
            free(_lines)
        
        errno = taos_errno(res)
        affected_rows = taos_affected_rows(res)
        if errno != 0:
            errstr = taos_errstr(res).decode("utf-8")
            taos_free_result(res)
            raise SchemalessError(errstr, errno, affected_rows)

        return affected_rows

    def schemaless_insert_raw(
            self,
            lines: str,
            protocol: int,
            precision: int,
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
        cdef int32_t total_rows
        _lines = lines.encode("utf-8")
        _length = len(_lines)

        if ttl is None:
            if req_id is None:
                res = taos_schemaless_insert_raw(
                    self._raw_conn,
                    _lines,
                    _length,
                    &total_rows,
                    protocol,
                    precision,
                )
            else:
                res = taos_schemaless_insert_raw_with_reqid(
                    self._raw_conn,
                    _lines,
                    _length,
                    &total_rows,
                    protocol,
                    precision,
                    req_id,
                )
        else:
            if req_id is None:
                res = taos_schemaless_insert_raw_ttl(
                    self._raw_conn,
                    _lines,
                    _length,
                    &total_rows,
                    protocol,
                    precision,
                    ttl,
                )
            else:
                res = taos_schemaless_insert_raw_ttl_with_reqid(
                    self._raw_conn,
                    _lines,
                    _length,
                    &total_rows,
                    protocol,
                    precision,
                    ttl,
                    req_id,
                )

        errno = taos_errno(res)
        affected_rows = taos_affected_rows(res)
        if errno != 0:
            errstr = taos_errstr(res).decode("utf-8")
            taos_free_result(res)
            raise SchemalessError(errstr, errno, affected_rows)
        
        return affected_rows

    def commit(self):
        """
        Commit any pending transaction to the database.
        Since TDengine do not support transactions, the implement is void functionality.
        """
        pass

    def rollback(self):
        """Void functionality"""
        pass

    def cursor(self) -> TaosCursor:
        return TaosCursor(self)

    def clear_result_set(self):
        """Clear unused result set on this connection."""
        pass

    def get_table_vgroup_id(self, str db, str table) -> int:
        """
        get table's vgroup id. It's require db name and table name, and return an int type vgroup id.
        """
        cdef int vg_id
        _db = db.encode("utf-8")
        _table = table.encode("utf-8")
        errno = taos_get_table_vgId(self._raw_conn, _db, _table, &vg_id)
        if errno != 0:
            errstr = taos_errstr(NULL).decode("utf-8")
            raise InternalError(errstr, errno)
        return vg_id


cdef class TaosField:
    cdef str _name
    cdef int8_t _type
    cdef int32_t _bytes

    def __cinit__(self, str name, int8_t type_, int32_t bytes):
        self._name = name
        self._type = type_
        self._bytes = bytes

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type

    @property
    def bytes(self):
        return self._bytes

    @property
    def length(self):
        return self._bytes

    def __str__(self):
        return "TaosField{name: %s, type: %d, bytes: %d}" % (self.name, self.type, self.bytes)

    def __repr__(self):
        return "TaosField{name: %s, type: %d, bytes: %d}" % (self.name, self.type, self.bytes)

    def __getitem__(self, item):
        return getattr(self, item)


cdef class TaosResult:
    """TDengine result interface"""
    cdef TAOS_RES *_res
    cdef TAOS_FIELD *_fields
    cdef list _taos_fields
    cdef int _field_count
    cdef int _precision
    cdef int _row_count
    cdef int _affected_rows
    cdef object _dt_epoch
    cdef bool _is_async

    def __cinit__(self, size_t res):
        self._res = <TAOS_RES*>res
        self._check_result_error()
        self._field_count = taos_field_count(self._res)
        self._fields = taos_fetch_fields(self._res)
        self._taos_fields = [TaosField(f.name.decode("utf-8"), f.type, f.bytes) for f in self._fields[:self._field_count]]
        self._precision = taos_result_precision(self._res)
        self._affected_rows = taos_affected_rows(self._res)
        self._row_count = self._affected_rows if self._field_count == 0 else 0
        self._dt_epoch = DEFAULT_DT_EPOCH
        self._is_async = False

    def __str__(self):
        return "TaosResult(field_count=%d, precision=%d, affected_rows=%d, row_count=%d)" % (self._field_count, self._precision, self._affected_rows, self._row_count)

    def __repr__(self):
        return "TaosResult(field_count=%d, precision=%d, affected_rows=%d, row_count=%d)" % (self._field_count, self._precision, self._affected_rows, self._row_count)

    def __iter__(self):
        return self.rows_iter()

    def __aiter__(self):
        return self.rows_iter_a()

    @property
    def fields(self) -> List[TaosField]:
        """fields definitions of the current result"""
        return self._taos_fields

    @property
    def field_count(self):
        """the fields count of the current result"""
        return self._field_count

    @property
    def precision(self):
        """the precision of the current result"""
        return self._precision

    @property
    def affected_rows(self):
        """the affected_rows of the current result"""
        return self._affected_rows

    @property
    def row_count(self):
        """the row_count of the object"""
        return self._row_count

    def _set_dt_epoch(self, dt_epoch):
        self._dt_epoch = dt_epoch
    
    def _mark_async(self):
        self._is_async = True

    def _check_result_error(self):
        errno = taos_errno(self._res)
        if errno != 0:
            errstr = taos_errstr(self._res).decode("utf-8")
            raise ProgrammingError(errstr, errno)

    def _fetch_block(self) -> Tuple[List, int]:
        cdef TAOS_ROW pblock
        num_of_rows = taos_fetch_block(self._res, &pblock)
        if num_of_rows == 0:
            return [], 0

        block = [None] * self._field_count
        cdef int i
        for i in range(self._field_count):
            data = pblock[i]
            field = self._fields[i]

            if field.type in SIZED_TYPE:
                is_null = taos_get_column_data_is_null(self._res, i, num_of_rows)
                block[i] = CONVERT_FUNC[field.type](<size_t>data, num_of_rows, <size_t>is_null)
                free(is_null)            
            elif field.type in UNSIZED_TYPE:
                offsets = taos_get_column_data_offset(self._res, i)
                block[i] = CONVERT_FUNC[field.type](<size_t>data, num_of_rows, <size_t>offsets)
            else:
                pass

            if field.type == TSDB_DATA_TYPE_TIMESTAMP and self._precision in (PrecisionEnum.Milliseconds, PrecisionEnum.Microseconds):
                block[i] = _convert_timestamp_to_datetime(block[i], self._precision, self._dt_epoch)

        self._row_count += num_of_rows
        return block, abs(num_of_rows)

    def _fetch_row(self) -> Optional[Tuple]:
        cdef TAOS_ROW taos_row
        cdef int i
        cdef int[1] offsets = [-2]
        cdef bool[1] is_null = [False]

        taos_row = taos_fetch_row(self._res)
        if taos_row is NULL:
            return None

        row = [None] * self._field_count
        for i in range(self._field_count):
            data = taos_row[i]
            field = self._fields[i]
            if field.type in SIZED_TYPE:
                row[i] = CONVERT_FUNC[field.type](<size_t>data, 1, <size_t>is_null)[0] if data is not NULL else None
            elif field.type in UNSIZED_TYPE:
                row[i] = CONVERT_FUNC[field.type](<size_t>data, 1, <size_t>offsets)[0] if data is not NULL else None  # FIXME: is it ok to set offsets = [-2] here
            else:
                pass

            if field.type == TSDB_DATA_TYPE_TIMESTAMP and self._precision in (PrecisionEnum.Milliseconds, PrecisionEnum.Microseconds):
                row[i] = _convert_timestamp_to_datetime([row[i]], self._precision, self._dt_epoch)[0]

        self._row_count += 1
        return tuple(row) # TODO: list -> tuple, too much object is create here

    async def _fetch_block_a(self) -> Tuple[List, int]:
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        param = (self, fut)
        taos_fetch_rows_a(self._res, async_block_future_wrapper, <void*>param)
        # taos_fetch_raw_block_a(self._res, async_block_future_wrapper, <void*>param)  # FIXME: have some problem when parsing nchar

        block, num_of_rows = await fut
        return block, num_of_rows

    async def _fetch_rows_a(self) -> List[Tuple]:
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        param = (self, fut)
        taos_fetch_rows_a(self._res, async_rows_future_wrapper, <void*>param)
        rows = await fut
        return rows

    def fetch_block(self) -> Tuple[List, int]:
        if self._res is NULL:
            raise OperationalError("Invalid use of fetch_block")

        if self._is_async:
            raise OperationalError("Invalid use of fetch_block, async result can not use sync func")

        block, num_of_rows = self._fetch_block()
        self._check_result_error()

        return [r for r in map(tuple, zip(*block))], num_of_rows

    async def fetch_block_a(self) -> Tuple[List, int]:
        if self._res is NULL:
            raise OperationalError("Invalid use of fetch_block_a")

        if not self._is_async:
            raise OperationalError("Invalid use of fetch_block_a, sync result can not use async func")

        block, num_of_rows = await self._fetch_block_a()
        self._check_result_error()

        return [r for r in map(tuple, zip(*block))], num_of_rows

    def fetch_all(self) -> List[Tuple]:
        """
        fetch all data from taos result into python list
        using `taos_fetch_block`
        """
        if self._res is NULL:
            raise OperationalError("Invalid use of fetchall")

        if self._is_async:
            raise OperationalError("Invalid use of fetch_all, async result can not use sync func")

        blocks = []
        while True:
            block, num_of_rows = self._fetch_block()

            if num_of_rows == 0:
                break

            blocks.append(block)
        self._check_result_error()

        return [r for b in blocks for r in map(tuple, zip(*b))]

    async def fetch_all_a(self) -> List[Tuple]:
        """
        async fetch all data from taos result into python list
        using `taos_fetch_block`
        """
        if self._res is NULL:
            raise OperationalError("Invalid use of fetch_all_a")

        if not self._is_async:
            raise OperationalError("Invalid use of fetch_all_a, sync result can not use async func")

        blocks = []
        while True:
            block, num_of_rows = await self._fetch_block_a()

            if num_of_rows == 0:
                break

            blocks.append(block)
        self._check_result_error()

        return [r for b in blocks for r in map(tuple, zip(*b))]

    def fetch_all_into_dict(self) -> List[Dict]:
        """
        fetch all data from taos result into python dict
        using `taos_fetch_block`
        """
        if self._res is NULL:
            raise OperationalError("Invalid use of fetch_all_into_dict")

        if self._is_async:
            raise OperationalError("Invalid use of fetch_all_into_dict, async result can not use sync func")

        field_names = [field.name for field in self.fields]
        blocks = []
        while True:
            block, num_of_rows = self._fetch_block()

            if num_of_rows == 0:
                break

            blocks.append(block)
        self._check_result_error()
        return [dict((f, v) for f, v in zip(field_names, r)) for b in blocks for r in map(tuple, zip(*b))]

    async def fetch_all_into_dict_a(self) -> List[Dict]:
        """
        async fetch all data from taos result into python dict
        using `taos_fetch_block`
        """
        if self._res is NULL:
            raise OperationalError("Invalid use of fetch_all_into_dict_a")

        if not self._is_async:
            raise OperationalError("Invalid use of fetch_all_into_dict_a, sync result can not use async func")

        field_names = [field.name for field in self.fields]
        blocks = []
        while True:
            block, num_of_rows = await self._fetch_block_a()

            if num_of_rows == 0:
                break

            blocks.append(block)
        self._check_result_error()

        return [dict((f, v) for f, v in zip(field_names, r)) for b in blocks for r in map(tuple, zip(*b))]

    def rows_iter(self) -> Iterator[Tuple]:
        """
        Iterate row from taos result into python list
        """
        if self._res is NULL:
            raise OperationalError("Invalid use of rows_iter")

        if self._is_async:
            raise OperationalError("Invalid use of rows_iter, async result can not use sync func")

        while True:
            row = self._fetch_row()

            if row is None:
                break

            yield row
        self._check_result_error()

    def blocks_iter(self) -> Iterator[Tuple[List[Tuple], int]]:
        if self._res is NULL:
            raise OperationalError("Invalid use of rows_iter")

        if self._is_async:
            raise OperationalError("Invalid use of rows_iter, async result can not use sync func")

        while True:
            block, num_of_rows = self._fetch_block()

            if num_of_rows == 0:
                break

            yield [r for r in map(tuple, zip(*block))], num_of_rows
        self._check_result_error()

    async def rows_iter_a(self) -> AsyncIterator[Tuple]:
        if self._res is NULL:
            raise OperationalError("Invalid use of rows_iter_a")

        if not self._is_async:
            raise OperationalError("Invalid use of rows_iter_a, sync result can not use async func")

        while True:
            rows = await self._fetch_rows_a()

            if not rows:
                break
            
            for row in rows:
                yield row
        self._check_result_error()

    async def blocks_iter_a(self) -> AsyncIterator[Tuple[List[Tuple], int]]:
        if self._res is NULL:
            raise OperationalError("Invalid use of blocks_iter_a")

        if not self._is_async:
            raise OperationalError("Invalid use of blocks_iter_a, sync result can not use async func")

        while True:
            block, num_of_rows = await self._fetch_block_a()

            if num_of_rows == 0:
                break
            
            yield [r for r in map(tuple, zip(*block))], num_of_rows
        self._check_result_error()

    def stop_query(self):
        if self._res is not NULL:
            taos_stop_query(self._res)

    def close(self):
        if self._res is not NULL:
            taos_free_result(self._res)

        self._res = NULL
        self._field_count
        self._fields = NULL
        self._taos_fields = []

    def __dealloc__(self):
        self.close()


cdef class TaosCursor:
    """Database cursor which is used to manage the context of a fetch operation.

    Attributes:
        .description: Read-only attribute consists of 7-item sequences:

            > name (mandatory)
            > type_code (mandatory)
            > display_size
            > internal_size
            > precision
            > scale
            > null_ok

            This attribute will be None for operations that do not return rows or
            if the cursor has not had an operation invoked via the .execute*() method yet.

        .rowcount:This read-only attribute specifies the number of rows that the last
            .execute*() produced (for DQL statements like SELECT) or affected
    """
    cdef list _description
    cdef TaosConnection _connection
    cdef TaosResult _result

    def __init__(self, connection: Optional[TaosConnection]=None):
        self._description = []
        self._connection = connection
        self._result = None

    def __iter__(self) -> Iterator[Tuple]:
        for block, _ in self._result.blocks_iter():
            for row in block:
                yield row

    @property
    def description(self) -> List[Tuple]:
        return self._description

    @property
    def rowcount(self) -> int:
        """
        For INSERT statement, rowcount is assigned immediately after execute the statement.
        For SELECT statement, rowcount will not get correct value until fetched all data.
        """
        return self._result.row_count

    @property
    def affected_rows(self) -> int:
        """Return the rowcount of insertion"""
        return self._result.affected_rows

    def callproc(self, procname, *args):
        """
        Call a stored database procedure with the given name.
        Void functionality since no stored procedures.
        """
        pass

    def close(self):
        """Close the cursor."""
        if self._connection is None:
            return False

        self._reset_result()
        self._connection = None

        return True

    def execute(self, operation, params=None, req_id: Optional[int] = None):
        """Prepare and execute a database operation (query or command)."""
        if not operation:
            return

        if not self._connection:
            raise ProgrammingError("Cursor is not connected")

        self._reset_result()
        sql = operation
        self._result = self._connection.query(sql, req_id)
        self._description = [(f.name, f.type, None, None, None, None, False) for f in self._result.fields]

        if self._result.field_count == 0:
            return self.affected_rows
        else:
            return self._result

    def executemany(self, operation, data_list, req_id: Optional[int] = None) -> int:
        """
        Prepare a database operation (query or command) and then execute it against all parameter sequences or mappings
        found in the sequence seq_of_parameters.
        """
        sql = operation
        flag = True
        affected_rows = 0
        for line in data_list:
            if isinstance(line, dict):
                flag = False
                affected_rows += self.execute(sql.format(**line), req_id=req_id)
            elif isinstance(line, list):
                sql += f' {tuple(line)} '
            elif isinstance(line, tuple):
                sql += f' {line} '
        if flag:
            affected_rows += self.execute(sql, req_id=req_id)
        return affected_rows

    def fetchone(self) -> Optional[Tuple]:
        try:
            row = next(self)
        except StopIteration:
            row = None
        return row

    def fetchmany(self, size=None) -> List[Tuple]:
        cdef int i = 0
        size = size or 1
        rows = []
        for row in self:
            rows.append(row)
            i += 1
            if i >= size:
                break
        
        return rows

    def istype(self, col: int, data_type: str):
        ft_name = "".join(["C ", data_type.upper()]).replace(" ", "_")
        return self._description[col][1] == getattr(FieldType, ft_name)

    def fetchall(self) -> List[Tuple]:
        return [r for r in self]

    def stop_query(self):
        self._result.stop_query()

    def nextset(self):
        pass

    def setinputsize(self, sizes):
        pass

    def setutputsize(self, size, column=None):
        pass

    def _reset_result(self):
        """Reset the result to unused version."""
        self._description = []
        self._result = None

    def __del__(self):
        self.close()

# ---------------------------------------------- TAOS -------------------------------------------------------------- v


# ---------------------------------------------- TMQ --------------------------------------------------------------- v
cdef void async_commit_future_wrapper(tmq_t *tmq, int32_t code, void *param) with gil:
    fut = <object>param
    fut.get_loop().call_soon_threadsafe(fut.set_result, code)


cdef void tmq_auto_commit_wrapper(tmq_t *tmq, int32_t code, void *param) with gil:
    consumer = <object>param
    if code == 0:
        if consumer.callback is not None:
            consumer.callback(consumer) # TODO: param is consumer itself only, seem pretty weird
    else:
        errstr = tmq_err2str(code).decode("utf-8")
        tmq_err = TmqError(errstr, code)
        if consumer.error_callback is not None:
            consumer.error_callback(tmq_err)


cdef class TopicPartition:
    cdef str _topic
    cdef int32_t _partition
    cdef int64_t _offset
    cdef int64_t _begin
    cdef int64_t _end

    def __cinit__(self, str topic, int32_t partition, int64_t offset, int64_t begin=0, int64_t end=0):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._begin = begin
        self._end = end

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def partition(self) -> int:
        return self._partition

    @property
    def offset(self) -> int:
        return self._offset

    @offset.setter
    def offset(self, int64_t value):
        self._offset = value

    @property
    def begin(self) -> int:
        return self._begin

    @property
    def end(self) -> int:
        return self._end

    def __str__(self):
        return "TopicPartition(topic=%s, partition=%s, offset=%s)" % (self.topic, self.partition, self.offset)

    def __repr__(self):
        return "TopicPartition(topic=%s, partition=%s, offset=%s)" % (self.topic, self.partition, self.offset)


cdef class MessageBlock:
    cdef list _block
    cdef list _fields
    cdef int _nrows
    cdef int _ncols
    cdef str _table

    def __init__(self, 
                block: Optional[List[Optional[List]]]=None, 
                fields: Optional[List[TaosField]]=None, 
                nrows: int=0, ncols: int=0, table: str=""):
        self._block = block or []
        self._fields = fields or []
        self._nrows = nrows
        self._ncols = ncols
        self._table = table

    def fields(self) -> List[TaosField]:
        """
        Get fields in message block
        """
        return self._fields

    def nrows(self) -> int:
        """
        get total count of rows of message block
        """
        return self._nrows

    def ncols(self) -> int:
        """
        get total count of rows of message block
        """
        return self._ncols

    def table(self) -> str:
        """
        get table name of message block
        """
        return self._table

    def fetchall(self) -> List[Tuple]:
        """
        get all data in message block
        """
        return [r for r in self]

    def __iter__(self) -> Iterator[Tuple]:
        return zip(*self._block)

    def __str__(self):
        return "MessageBlock(table=%s, nrows=%s, ncols=%s)" % (self.table, self.nrows, self.ncols)

    def __repr__(self):
        return "MessageBlock(table=%s, nrows=%s, ncols=%s)" % (self.table, self.nrows, self.ncols)

cdef class Message:
    cdef TAOS_RES *_res
    cdef int _err_no
    cdef char *_err_str
    cdef object _dt_epoch

    def __cinit__(self, size_t res):
        self._res = <TAOS_RES*>res
        self._err_no = taos_errno(self._res)
        self._err_str = taos_errstr(self._res)
        self._dt_epoch = DEFAULT_DT_EPOCH

    def __str__(self):
        return "Message(topic=%s, database=%s, vgroup=%s, offset=%s)" % (self.topic(), self.database(), self.vgroup(), self.offset())

    def __repr__(self):
        return "Message(topic=%s, database=%s, vgroup=%s, offset=%s)" % (self.topic(), self.database(), self.vgroup(), self.offset())

    def error(self) -> Optional[TmqError]:
        """
        The message object is also used to propagate errors and events, an application must check error() to determine
        if the Message is a proper message (error() returns None) or an error or event (error() returns a TmqError
         object)

        :rtype: None or :py:class:`TmqError
        """
        return TmqError(self._err_str.decode("utf-8"), self._err_no) if self._err_no else None

    def topic(self) -> Optional[str]:
        """

        :returns: topic name.
        :rtype: str
        """
        _topic = tmq_get_topic_name(self._res)
        topic = None if _topic is NULL else _topic.decode("utf-8")
        return topic

    def database(self) -> Optional[str]:
        """

        :returns: database name.
        :rtype: str
        """
        _db = tmq_get_db_name(self._res)
        db = None if _db is NULL else _db.decode("utf-8")
        return db

    def vgroup(self) -> int:
        return tmq_get_vgroup_id(self._res)

    def offset(self) -> int:
        """
        :returns: message offset.
        :rtype: int
        """
        return tmq_get_vgroup_offset(self._res)

    def _set_dt_epoch(self, dt_epoch):
        self._dt_epoch = dt_epoch
    
    def _fetch_message_block(self) -> Optional[MessageBlock]:
        cdef TAOS_ROW pblock
        num_of_rows = taos_fetch_block(self._res, &pblock)
        if num_of_rows == 0:
            return None

        field_count = taos_field_count(self._res)
        _fields = taos_fetch_fields(self._res)
        fields = [TaosField(f.name.decode("utf-8"), f.type, f.bytes) for f in _fields[:field_count]]
        precision = taos_result_precision(self._res)

        _table = tmq_get_table_name(self._res)
        table = "" if _table is NULL else _table.decode("utf-8")

        block = [None] * field_count
        cdef int i
        for i in range(field_count):
            data = pblock[i]
            field = _fields[i]

            if field.type in SIZED_TYPE:
                is_null = taos_get_column_data_is_null(self._res, i, num_of_rows)
                block[i] = CONVERT_FUNC[field.type](<size_t>data, num_of_rows, <size_t>is_null)
                free(is_null)
            elif field.type in UNSIZED_TYPE:
                offsets = taos_get_column_data_offset(self._res, i)
                block[i] = CONVERT_FUNC[field.type](<size_t>data, num_of_rows, <size_t>offsets)
            else:
                pass

            if field.type == TSDB_DATA_TYPE_TIMESTAMP and precision in (PrecisionEnum.Milliseconds, PrecisionEnum.Microseconds):
                block[i] = _convert_timestamp_to_datetime(block[i], precision, self._dt_epoch)

        return MessageBlock(block, fields, num_of_rows, field_count, table)

    def value(self) -> List[MessageBlock]:
        """

        :returns: message value (payload).
        :rtype: list[MessageBlock]
        """
        res_type = tmq_get_res_type(self._res)
        if res_type in (TmqResultType.TABLE_META, TmqResultType.INVALID):
            return None # TODO: deal with meta data

        message_blocks = []
        while True:
            mb = self._fetch_message_block()
            if not mb:
                break
            
            message_blocks.append(mb)

        return message_blocks

    def __dealloc__(self):
        if self._res is not NULL:
            taos_free_result(self._res)

        self._res = NULL
        self._err_no = 0
        self._err_str = NULL


cdef class TaosConsumer:
    cdef object __cb
    cdef object __err_cb
    cdef dict _configs
    cdef tmq_conf_t *_tmq_conf
    cdef tmq_t *_tmq
    cdef bool _subscribed
    cdef object _tz
    cdef object _dt_epoch
    default_config = {
        'group.id',
        'client.id',
        'msg.with.table.name',
        'enable.auto.commit',
        'auto.commit.interval.ms',
        'auto.offset.reset',
        'experimental.snapshot.enable',
        'enable.heartbeat.background',
        'experimental.snapshot.batch.size',
        'td.connect.ip',
        'td.connect.user',
        'td.connect.pass',
        'td.connect.port',
        'td.connect.db',
    }

    def __cinit__(self, dict configs, **kwargs):
        self.__cb = None
        self.__err_cb = None
        self._init_config(configs)
        self._init_consumer()
        self._subscribed = False
        self._tz = DEFAULT_TZ
        self._dt_epoch = DEFAULT_DT_EPOCH
        if "timezone" in kwargs:
            self.tz = kwargs["timezone"]

    def _init_config(self, dict configs):
        if 'group.id' not in configs:
            raise TmqError('missing group.id in consumer config setting')

        self._configs = configs
        self._tmq_conf = tmq_conf_new()
        if self._tmq_conf is NULL:
            raise TmqError("new tmq conf failed")

        for k, v in self._configs.items():
            _k = k.encode("utf-8")
            _v = v.encode("utf-8")
            tmq_conf_res = tmq_conf_set(self._tmq_conf, _k, _v)
            if tmq_conf_res != tmq_conf_res_t.TMQ_CONF_OK:
                tmq_conf_destroy(self._tmq_conf)
                self._tmq_conf = NULL
                raise TmqError("set tmq conf failed!")

    def _init_consumer(self):
        if self._tmq_conf is NULL:
            raise TmqError('tmq_conf is NULL')

        if self._configs.get("enable.auto.commit", "false") == "true":
            param = self
            tmq_conf_set_auto_commit_cb(self._tmq_conf, tmq_auto_commit_wrapper, <void*>param)
        
        self._tmq = tmq_consumer_new(self._tmq_conf, NULL, 0)
        if self._tmq is NULL:
            raise TmqError("new tmq consumer failed")

    def _check_tmq_error(self, tmq_errno):
        if tmq_errno != 0:
            tmq_errstr = tmq_err2str(tmq_errno).decode("utf-8")
            raise TmqError(tmq_errstr, tmq_errno)

    @property
    def tz(self) -> Optional[dt.tzinfo]:
        return self._tz

    @tz.setter
    def tz(self, timezone: Optional[Union[str, dt.tzinfo]]):
        if isinstance(timezone, str):
            timezone = pytz.timezone(timezone)
        
        assert timezone is None or isinstance(timezone, dt.tzinfo)

        self._tz = timezone
        self._dt_epoch = dt.datetime.fromtimestamp(0, tz=self._tz)

    @property
    def callback(self):
        return self.__cb

    @callback.setter
    def callback(self, cb):
        assert callable(cb)
        self.__cb = cb

    @property
    def error_callback(self):
        return self.__err_cb

    @error_callback.setter
    def error_callback(self, err_cb):
        assert callable(err_cb)
        self.__err_cb = err_cb
    
    def subscribe(self, topics: List[str]):
        """
        Set subscription to supplied list of topics.
        :param list(str) topics: List of topics (strings) to subscribe to.
        """
        tmq_list = tmq_list_new()
        if tmq_list is NULL:
            raise TmqError("new tmq list failed!")
        
        try:
            for tp in topics:
                _tp = tp.encode("utf-8")
                tmq_errno = tmq_list_append(tmq_list, _tp)
                if tmq_errno != 0:
                    tmq_errstr = tmq_err2str(tmq_errno).decode("utf-8")
                    raise TmqError(tmq_errstr, tmq_errno)

            tmq_errno = tmq_subscribe(self._tmq, tmq_list)
            if tmq_errno != 0:
                tmq_errstr = tmq_err2str(tmq_errno).decode("utf-8")
                raise TmqError(tmq_errstr, tmq_errno)
        finally:
            tmq_list_destroy(tmq_list)
        
        self._subscribed = True

    def unsubscribe(self):
        """
        Remove current subscription.
        """
        tmq_errno = tmq_unsubscribe(self._tmq)
        self._check_tmq_error(tmq_errno)
        
        self._subscribed = False

    def close(self):
        """
        Close down and terminate the Kafka Consumer.
        """
        if self._tmq is not NULL:
            tmq_errno = tmq_consumer_close(self._tmq)
            self._check_tmq_error(tmq_errno)
            self._tmq = NULL

    def poll(self, float timeout=1.0) -> Optional[Message]:
        """
        Consumes a single message and returns events.

        The application must check the returned `Message` object's `Message.error()` method to distinguish between
        proper messages (error() returns None).

        :param float timeout: Maximum time to block waiting for message, event or callback (default: 1). (second)
        :returns: A Message object or None on timeout
        :rtype: `Message` or None
        """
        if not self._subscribed:
            raise TmqError("unsubscribe topic")

        timeout_ms = int(timeout * 1000)
        res = tmq_consumer_poll(self._tmq, timeout_ms)
        if res is NULL:
            return None
        
        msg = Message(<size_t>res)
        msg._set_dt_epoch(self._dt_epoch)
        return msg

    def set_auto_commit_cb(self, callback: Callable[[TaosConsumer]]=None, error_callback: Callable[[TmqError]]=None):
        """
        Set callback for auto commit.

        :param callback: a Callable[[TaosConsumer]] object which was called when message is committed
        :param error_callback: a Callable[[TmqError]] object which waw called when something wrong(code != 0)
        """
        self.callback = callback
        self.error_callback = error_callback

    def commit(self, message: Message=None, offsets: List[TopicPartition]=None):
        """
        Commit a message.

        The `message` and `offsets` parameters are mutually exclusive. If neither is set, the current partition
        assignment's offsets are used instead. Use this method to commit offsets if you have 'enable.auto.commit' set
        to False.

        :param Message message: Commit the message's offset+1. Note: By convention, committed offsets reflect the next
            message to be consumed, **not** the last message consumed.
        :param list(TopicPartition) offsets: List of topic+partitions+offsets to commit.
        """
        if message:
            self.message_commit(message)
            return 

        if offsets:
            self.offsets_commit(offsets)
            return

        tmq_errno = tmq_commit_sync(self._tmq, NULL)
        self._check_tmq_error(tmq_errno)

    async def commit_a(self, message: Message=None, offsets: List[TopicPartition]=None):
        """
        Async commit a message.

        The `message` and `offsets` parameters are mutually exclusive. If neither is set, the current partition
        assignment's offsets are used instead. Use this method to commit offsets if you have 'enable.auto.commit' set
        to False.

        :param Message message: Commit the message's offset+1. Note: By convention, committed offsets reflect the next
            message to be consumed, **not** the last message consumed.
        :param list(TopicPartition) offsets: List of topic+partitions+offsets to commit.
        """
        if message:
            await self.message_commit_a(message)
            return

        if offsets:
            await self.offsets_commit_a(offsets)
            return

        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        tmq_commit_async(self._tmq, NULL, async_commit_future_wrapper, <void*>fut)
        tmq_errno = await fut
        self._check_tmq_error(tmq_errno)

    def message_commit(self, message: Message):
        """ Commit with message """
        tmq_errno = tmq_commit_sync(self._tmq, message._res)
        self._check_tmq_error(tmq_errno)

    async def message_commit_a(self, message: Message):
        """ Async commit with message """
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        tmq_commit_async(self._tmq, message._res, async_commit_future_wrapper, <void*>fut)
        tmq_errno = await fut
        self._check_tmq_error(tmq_errno)

    def offsets_commit(self, partitions: List[TopicPartition]):
        """ Commit with topic partitions """
        for tp in partitions:
            _tp = tp._topic.encode("utf-8")
            tmq_errno = tmq_commit_offset_sync(self._tmq, _tp, tp._partition, tp._offset)
            self._check_tmq_error(tmq_errno)
    
    async def offsets_commit_a(self, partitions: List[TopicPartition]):
        """ async commit with topic partitions """
        loop = asyncio.get_event_loop()
        futs = []
        for tp in partitions:
            _tp = tp._topic.encode("utf-8")
            fut = loop.create_future()
            tmq_commit_offset_async(self._tmq, _tp, tp._partition, tp._offset, async_commit_future_wrapper, <void*>fut)
            futs.append(fut)
        
        tmq_errnos = await asyncio.gather(futs, return_exceptions=True)
        for tmq_errno in tmq_errnos:
            self._check_tmq_error(tmq_errno)

    def assignment(self) -> List[TopicPartition]:
        """
        Returns the current partition assignment as a list of TopicPartition tuples.
        """
        cdef int32_t i
        cdef int32_t num_of_assignment
        cdef tmq_topic_assignment *p_assignment = NULL

        topics = self.list_topics()
        topic_partitions = []
        try:
            for topic in topics:
                _topic = topic.encode("utf-8")
                tmq_errno = tmq_get_topic_assignment(self._tmq, _topic, &p_assignment, &num_of_assignment)
                if tmq_errno != 0:
                    tmq_errstr = tmq_err2str(tmq_errno).decode("utf-8")
                    raise TmqError(tmq_errstr, tmq_errno)

                for i in range(num_of_assignment):
                    assignment = p_assignment[i]
                    tp = TopicPartition(topic, assignment.vgId, assignment.currentOffset, assignment.begin, assignment.end)
                    topic_partitions.append(tp)
        finally:
            if p_assignment is not NULL:
                tmq_free_assignment(p_assignment)

        return topic_partitions

    def seek(self, partition: TopicPartition):
        """
        Set consume position for partition to offset.
        """
        _tp = partition._topic.encode("utf-8")
        tmq_errno = tmq_offset_seek(self._tmq, _tp, partition._partition, partition._offset)
        self._check_tmq_error(tmq_errno)

    def committed(self, partitions: List[TopicPartition]) -> List[TopicPartition]:
        """
        Retrieve committed offsets for the specified partitions.

        :param list(TopicPartition) partitions: List of topic+partitions to query for stored offsets.
        :returns: List of topic+partitions with offset and possibly error set.
        :rtype: list(TopicPartition)
        """
        for partition in partitions:
            if not isinstance(partition, TopicPartition):
                raise TmqError("Invalid partition type")
            
            _tp = partition._topic.encode("utf-8")
            tmq_errno = offset = tmq_committed(self._tmq, _tp, partition._partition)
            self._check_tmq_error(tmq_errno)
            
            partition.offset = offset

        return partitions

    def position(self, partitions: List[TopicPartition]) -> List[TopicPartition]:
        """
        Retrieve current positions (offsets) for the specified partitions.

        :param list(TopicPartition) partitions: List of topic+partitions to return current offsets for.
        :returns: List of topic+partitions with offset and possibly error set.
        :rtype: list(TopicPartition)
        """
        for partition in partitions:
            if not isinstance(partition, TopicPartition):
                raise TmqError("Invalid partition type")

            _tp = partition._topic.encode("utf-8")
            tmq_errno = offset = tmq_position(self._tmq, _tp, partition._partition)
            self._check_tmq_error(tmq_errno)

            partition.offset = offset

        return partitions

    def list_topics(self) -> List[str]:
        """
        Request subscription topics from the tmq.

        :rtype: topics list
        """
        cdef int i
        tmq_list = tmq_list_new()
        if tmq_list is NULL:
            raise TmqError("new tmq list failed!")

        try:
            tmq_errno = tmq_subscription(self._tmq, &tmq_list)
            self._check_tmq_error(tmq_errno)

            ca = tmq_list_to_c_array(tmq_list)
            n = tmq_list_get_size(tmq_list)

            tp_list = []
            for i in range(n):
                tp_list.append(ca[i].decode("utf-8"))
        finally:
            tmq_list_destroy(tmq_list)

        return tp_list

    def __iter__(self) -> Iterator[Message]:
        while self._tmq:
            message = self.poll()
            if message:
                yield message

    def __dealloc__(self):
        if self._tmq_conf is not NULL:
            tmq_conf_destroy(self._tmq_conf)
            self._tmq_conf = NULL
        self.close()

# ---------------------------------------------- TMQ --------------------------------------------------------------- ^


# ---------------------------------------- statement --------------------------------------------------------------- v

cdef class TaosStmt:
    cdef TAOS_STMT *_stmt

    def __cinit__(self, size_t stmt):
        self._stmt = <TAOS_STMT*>stmt

    def _check_stmt_error(self, int errno):
        if errno != 0:
            stmt_errstr = taos_stmt_errstr(self._stmt).decode("utf-8")
            raise StatementError(stmt_errstr, errno)

    def set_tbname(self, str name):
        if self._stmt is NULL:
            raise StatementError("Invalid use of set_tbname")

        _name = name.encode("utf-8")
        errno = taos_stmt_set_tbname(self._stmt, _name)
        self._check_stmt_error(errno)

    def prepare(self, str sql):
        if self._stmt is NULL:
            raise StatementError("Invalid use of prepare")

        _sql = sql.encode("utf-8")
        errno = taos_stmt_prepare(self._stmt, _sql, len(_sql))
        self._check_stmt_error(errno)

    def set_tbname_tags(self, str name, TaosMultiBinds tags):
        """Set table name with tags, tags is array of BindParams"""
        if self._stmt is NULL:
            raise StatementError("Invalid use of set_tbname_tags")
        
        _name = name.encode("utf-8")
        errno = taos_stmt_set_tbname_tags(self._stmt, _name, tags._raw_binds)
        self._check_stmt_error(errno)

    def bind_param(self, TaosMultiBinds binds, bool add_batch=True):
        if self._stmt is NULL:
            raise StatementError("Invalid use of bind_param")

        errno = taos_stmt_bind_param(self._stmt, binds._raw_binds)
        self._check_stmt_error(errno)

        if add_batch:
            self.add_batch()

    def bind_param_batch(self, TaosMultiBinds binds, bool add_batch=True):
        if self._stmt is NULL:
            raise StatementError("Invalid use of bind_param_batch")

        errno = taos_stmt_bind_param_batch(self._stmt, binds._raw_binds)
        self._check_stmt_error(errno)

        if add_batch:
            self.add_batch()

    def add_batch(self):
        if self._stmt is NULL:
            raise StatementError("Invalid use of add_batch")
        
        errno = taos_stmt_add_batch(self._stmt)
        self._check_stmt_error(errno)

    def execute(self):
        if self._stmt is NULL:
            raise StatementError("Invalid use of execute")

        errno = taos_stmt_execute(self._stmt)
        self._check_stmt_error(errno)

    def use_result(self) -> TaosResult:
        if self._stmt is NULL:
            raise StatementError("Invalid use of use_result")
        
        res = taos_stmt_use_result(self._stmt)
        if res is NULL:
            raise StatementError(taos_stmt_errstr(self._stmt).decode("utf-8"))

        return TaosResult(<size_t>res)

    @property
    def affected_rows(self) -> int:
        # type: () -> int
        return taos_stmt_affected_rows(self._stmt)

    def close(self):
        """Close stmt."""
        if self._stmt is NULL:
            return

        errno = taos_stmt_close(self._stmt)
        self._check_stmt_error(errno)
        self._stmt = NULL

    def __dealloc__(self):
        self.close()


cdef class TaosMultiBinds:
    cdef size_t _size
    cdef TAOS_MULTI_BIND *_raw_binds
    cdef list _binds

    def __cinit__(self, size_t size):
        self._size = size
        self._init_binds(size)

    def _init_binds(self, size_t size):
        self._raw_binds = <TAOS_MULTI_BIND*>malloc(size * sizeof(TAOS_MULTI_BIND))
        _check_malloc(<void*>self._raw_binds)
        memset(self._raw_binds, 0, size * sizeof(TAOS_MULTI_BIND))
        self._binds = []
        for i in range(size):
            _pbind = <size_t>self._raw_binds + (i * sizeof(TAOS_MULTI_BIND))
            self._binds.append(TaosMultiBind(_pbind))

    def __str__(self):
        return "TaosMultiBinds(size=%d)" % (self._size, )

    def __repr__(self):
        return "TaosMultiBinds(size=%d)" % (self._size, )

    def __getitem__(self, item):
        return self._binds[item]
    
    def __dealloc__(self):
        if self._raw_binds is not NULL:
            self._binds = []
            free(self._raw_binds)
            self._raw_binds = NULL

        self._size = 0


cdef class TaosMultiBind:
    cdef TAOS_MULTI_BIND *_inner

    def __cinit__(self, size_t pbind):
        self._inner = <TAOS_MULTI_BIND*>pbind

    def __dealloc__(self):
        if self._inner.buffer is not NULL:
            free(self._inner.buffer)
            self._inner.buffer = NULL

        if self._inner.is_null is not NULL:
            free(self._inner.is_null)
            self._inner.is_null = NULL

        if self._inner.length is not NULL:
            free(self._inner.length)
            self._inner.length = NULL

    cdef _init_buffer(self, size_t size):
        if self._inner.buffer is not NULL:
            free(self._inner.buffer)
            self._inner.buffer = NULL

        _buffer = malloc(size)
        _check_malloc(_buffer)
        self._inner.buffer = <void*>_buffer

    cdef _init_is_null(self, size_t size):
        if self._inner.is_null is not NULL:
            free(self._inner.is_null)
            self._inner.is_null = NULL

        _is_null = malloc(size)
        _check_malloc(_is_null)
        self._inner.is_null = <char*>_is_null

    cdef _init_length(self, size_t size):
        if self._inner.length is not NULL:
            free(self._inner.length)
            self._inner.length = NULL

        _length = malloc(size)
        _check_malloc(_length)
        self._inner.length = <int32_t*>_length

    def __str__(self):
        return "TaosMultiBind(buffer_type=%s, buffer_length=%d, num=%d)" % (self._inner.buffer_type, self._inner.buffer_length, self._inner.num)

    def __repr__(self):
        return "TaosMultiBind(buffer_type=%s, buffer_length=%d, num=%d)" % (self._inner.buffer_type, self._inner.buffer_length, self._inner.num)

    def bool(self, values):
        self._inner.buffer_type = FieldType.C_BOOL
        self._inner.buffer_length = sizeof(bool)
        self._inner.num = len(values)
        self._init_buffer(self._inner.num * sizeof(bool))
        self._init_is_null(self._inner.num * sizeof(char))
        _buffer = <int8_t*>self._inner.buffer
        _is_null = self._inner.is_null

        for i in range(self._inner.num):
            v = values[i]
            if v is None:
                _buffer[i] = <int8_t>FieldType.C_BOOL_NULL
                _is_null[i] = 1
            else:
                _buffer[i] = <int8_t>v
                _is_null[i] = 0

    def tinyint(self, values):
        self._inner.buffer_type = FieldType.C_TINYINT
        self._inner.buffer_length = sizeof(int8_t)
        self._inner.num = len(values)
        self._init_buffer(self._inner.num * sizeof(int8_t))
        self._init_is_null(self._inner.num * sizeof(char))
        _buffer = <int8_t*>self._inner.buffer
        _is_null = self._inner.is_null

        for i in range(self._inner.num):
            v = values[i]
            if v is None:
                _buffer[i] = <int8_t>FieldType.C_TINYINT_NULL
                _is_null[i] = 1
            else:
                _buffer[i] = <int8_t>v
                _is_null[i] = 0

    def smallint(self, values):
        self._inner.buffer_type = FieldType.C_SMALLINT
        self._inner.buffer_length = sizeof(int16_t)
        self._inner.num = len(values)
        self._init_buffer(self._inner.num * sizeof(int16_t))
        self._init_is_null(self._inner.num * sizeof(char))
        _buffer = <int16_t*>self._inner.buffer
        _is_null = self._inner.is_null

        for i in range(self._inner.num):
            v = values[i]
            if v is None:
                _buffer[i] = <int16_t>FieldType.C_SMALLINT_NULL
                _is_null[i] = 1
            else:
                _buffer[i] = <int16_t>v
                _is_null[i] = 0

    def int(self, values):
        self._inner.buffer_type = FieldType.C_INT
        self._inner.buffer_length = sizeof(int32_t)
        self._inner.num = len(values)
        self._init_buffer(self._inner.num * sizeof(int32_t))
        self._init_is_null(self._inner.num * sizeof(char))
        _buffer = <int32_t*>self._inner.buffer
        _is_null = self._inner.is_null

        for i in range(self._inner.num):
            v = values[i]
            if v is None:
                _buffer[i] = <int32_t>FieldType.C_INT_NULL
                _is_null[i] = 1
            else:
                _buffer[i] = <int32_t>v
                _is_null[i] = 0

    def bigint(self, values):
        self._inner.buffer_type = FieldType.C_BIGINT
        self._inner.buffer_length = sizeof(int64_t)
        self._inner.num = len(values)
        self._init_buffer(self._inner.num * sizeof(int64_t))
        self._init_is_null(self._inner.num * sizeof(char))
        _buffer = <int64_t*>self._inner.buffer
        _is_null = self._inner.is_null

        for i in range(self._inner.num):
            v = values[i]
            if v is None:
                _buffer[i] = <int64_t>FieldType.C_BIGINT_NULL
                _is_null[i] = 1
            else:
                _buffer[i] = <int64_t>v
                _is_null[i] = 0

    def float(self, values):
        self._inner.buffer_type = FieldType.C_FLOAT
        self._inner.buffer_length = sizeof(float)
        self._inner.num = len(values)
        self._init_buffer(self._inner.num * sizeof(float))
        self._init_is_null(self._inner.num * sizeof(char))
        _buffer = <float*>self._inner.buffer
        _is_null = self._inner.is_null

        for i in range(self._inner.num):
            v = values[i]
            if v is None:
                # _buffer[i] = <float>FieldType.C_FLOAT_NULL
                _buffer[i] = <float>float('nan')
                _is_null[i] = 1
            else:
                _buffer[i] = <float>v
                _is_null[i] = 0

    def double(self, values):
        self._inner.buffer_type = FieldType.C_DOUBLE
        self._inner.buffer_length = sizeof(double)
        self._inner.num = len(values)
        self._init_buffer(self._inner.num * sizeof(double))
        self._init_is_null(self._inner.num * sizeof(char))
        _buffer = <double*>self._inner.buffer
        _is_null = self._inner.is_null

        for i in range(self._inner.num):
            v = values[i]
            if v is None:
                # _buffer[i] = <double>FieldType.C_DOUBLE_NULL
                _buffer[i] = <float>float('nan')
                _is_null[i] = 1
            else:
                _buffer[i] = <double>v
                _is_null[i] = 0

    def tinyint_unsigned(self, values):
        self._inner.buffer_type = FieldType.C_TINYINT_UNSIGNED
        self._inner.buffer_length = sizeof(uint8_t)
        self._inner.num = len(values)
        self._init_buffer(self._inner.num * sizeof(uint8_t))
        self._init_is_null(self._inner.num * sizeof(char))
        _buffer = <uint8_t*>self._inner.buffer
        _is_null = self._inner.is_null

        for i in range(self._inner.num):
            v = values[i]
            if v is None:
                _buffer[i] = <uint8_t>FieldType.C_TINYINT_UNSIGNED_NULL
                _is_null[i] = 1
            else:
                _buffer[i] = <uint8_t>v
                _is_null[i] = 0

    def smallint_unsigned(self, values):
        self._inner.buffer_type = FieldType.C_SMALLINT_UNSIGNED
        self._inner.buffer_length = sizeof(uint16_t)
        self._inner.num = len(values)
        self._init_buffer(self._inner.num * sizeof(uint16_t))
        self._init_is_null(self._inner.num * sizeof(char))
        _buffer = <uint16_t*>self._inner.buffer
        _is_null = self._inner.is_null

        for i in range(self._inner.num):
            v = values[i]
            if v is None:
                _buffer[i] = <uint16_t>FieldType.C_SMALLINT_UNSIGNED_NULL
                _is_null[i] = 1
            else:
                _buffer[i] = <uint16_t>v
                _is_null[i] = 0

    def int_unsigned(self, values):
        self._inner.buffer_type = FieldType.C_INT_UNSIGNED
        self._inner.buffer_length = sizeof(uint32_t)
        self._inner.num = len(values)
        self._init_buffer(self._inner.num * sizeof(uint32_t))
        self._init_is_null(self._inner.num * sizeof(char))
        _buffer = <uint32_t*>self._inner.buffer
        _is_null = self._inner.is_null

        for i in range(self._inner.num):
            v = values[i]
            if v is None:
                _buffer[i] = <uint32_t>FieldType.C_INT_UNSIGNED_NULL
                _is_null[i] = 1
            else:
                _buffer[i] = <uint32_t>v
                _is_null[i] = 0

    def bigint_unsigned(self, values):
        self._inner.buffer_type = FieldType.C_BIGINT_UNSIGNED
        self._inner.buffer_length = sizeof(uint64_t)
        self._inner.num = len(values)
        self._init_buffer(self._inner.num * sizeof(uint64_t))
        self._init_is_null(self._inner.num * sizeof(char))
        _buffer = <uint64_t*>self._inner.buffer
        _is_null = self._inner.is_null

        for i in range(self._inner.num):
            v = values[i]
            if v is None:
                _buffer[i] = <uint64_t>FieldType.C_BIGINT_UNSIGNED_NULL
                _is_null[i] = 1
            else:
                _buffer[i] = <uint64_t>v
                _is_null[i] = 0

    def timestamp(self, values, precision=PrecisionEnum.Milliseconds):  # BUG: program just crash if one of the values is None in the first timestamp column
        self._inner.buffer_type = FieldType.C_TIMESTAMP
        self._inner.buffer_length = sizeof(int64_t)
        self._inner.num = len(values)
        self._init_buffer(self._inner.num * sizeof(int64_t))
        self._init_is_null(self._inner.num * sizeof(char))
        _buffer = <int64_t*>self._inner.buffer
        _is_null = self._inner.is_null

        m = 10**(3*(precision+1))
        for i in range(self._inner.num):
            v = values[i]
            if v is None:
                _buffer[i] = <int64_t>FieldType.C_BIGINT_NULL
                _is_null[i] = 1
            else:
                if isinstance(v, dt.datetime):
                    v = int(round(v.timestamp() * m))
                elif isinstance(v, str):
                    v = int(round(dt.datetime.fromisoformat(v).timestamp() * m))
                elif isinstance(v, float):
                    v = int(round(v * m))
                else:
                    pass

                _buffer[i] = <int64_t>v
                _is_null[i] = 0

    def binary(self, values):
        _bytes = [v if v is None else v.encode("utf-8") for v in values]
        self._inner.buffer_type = FieldType.C_BINARY
        self._inner.buffer_length = max(len(b) for b in _bytes if b is not None)
        self._inner.num = len(values)
        self._init_buffer(self._inner.num * self._inner.buffer_length)
        self._init_is_null(self._inner.num * sizeof(char))
        self._init_length(self._inner.num * sizeof(int32_t))
        _buffer = self._inner.buffer
        _is_null = self._inner.is_null
        _length = self._inner.length

        _buf = bytearray(self._inner.num * self._inner.buffer_length)
        for i in range(self._inner.num):
            offset = i * self._inner.buffer_length
            v = _bytes[i]
            if v is None:
                # _buf[offset:offset+self._inner.buffer_length] = b"\x00" * self._inner.buffer_length
                _is_null[i] = 1
                _length[i] = 0
            else:
                _buf[offset:offset+len(v)] = v
                _is_null[i] = 0
                _length[i] = len(v)

        memcpy(_buffer, <char*>_buf, self._inner.num * self._inner.buffer_length)

    def nchar(self, values):
        _bytes = [v if v is None else v.encode("utf-8") for v in values]
        self._inner.buffer_type = FieldType.C_NCHAR
        self._inner.buffer_length = max(len(b) for b in _bytes if b is not None)
        self._inner.num = len(values)
        self._init_buffer(self._inner.num * self._inner.buffer_length)
        self._init_is_null(self._inner.num * sizeof(char))
        self._init_length(self._inner.num * sizeof(int32_t))
        _buffer = self._inner.buffer
        _is_null = self._inner.is_null
        _length = self._inner.length

        _buf = bytearray(self._inner.num * self._inner.buffer_length)
        for i in range(self._inner.num):
            offset = i * self._inner.buffer_length
            v = _bytes[i]
            if v is None:
                # _buf[offset:offset+self._inner.buffer_length] = b"\x00" * self._inner.buffer_length
                _is_null[i] = 1
                _length[i] = 0
            else:
                _buf[offset:offset+len(v)] = v
                _is_null[i] = 0
                _length[i] = len(v)

        memcpy(_buffer, <char*>_buf, self._inner.num * self._inner.buffer_length)

    def json(self, values):
        _bytes = [v if v is None else v.encode("utf-8") for v in values]
        self._inner.buffer_type = FieldType.C_JSON
        self._inner.buffer_length = max(len(b) for b in _bytes if b is not None)
        self._inner.num = len(values)
        self._init_buffer(self._inner.num * self._inner.buffer_length)
        self._init_is_null(self._inner.num * sizeof(char))
        self._init_length(self._inner.num * sizeof(int32_t))
        _buffer = self._inner.buffer
        _is_null = self._inner.is_null
        _length = self._inner.length

        _buf = bytearray(self._inner.num * self._inner.buffer_length)
        for i in range(self._inner.num):
            offset = i * self._inner.buffer_length
            v = _bytes[i]
            if v is None:
                # _buf[offset:offset+self._inner.buffer_length] = b"\x00" * self._inner.buffer_length
                _is_null[i] = 1
                _length[i] = 0
            else:
                _buf[offset:offset+len(v)] = v
                _is_null[i] = 0
                _length[i] = len(v)

        memcpy(_buffer, <char*>_buf, self._inner.num * self._inner.buffer_length)

# ---------------------------------------- statement --------------------------------------------------------------- ^
