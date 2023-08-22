# cython: profile=True

from taos._cinterface cimport *
from taos._parser cimport *
from taos._cinterface import SIZED_TYPE, UNSIZED_TYPE, CONVERT_FUNC
import datetime as dt
import pytz
from collections import namedtuple
from taos.error import ProgrammingError, OperationalError, ConnectionError, DatabaseError, StatementError, InternalError

_priv_tz = None
_utc_tz = pytz.timezone("UTC")
try:
    _datetime_epoch = dt.datetime.fromtimestamp(0)
except OSError:
    _datetime_epoch = dt.datetime.fromtimestamp(86400) - dt.timedelta(seconds=86400)
_utc_datetime_epoch = _utc_tz.localize(dt.datetime.utcfromtimestamp(0))
_priv_datetime_epoch = None


def set_tz(tz):
    global _priv_tz, _priv_datetime_epoch
    _priv_tz = tz
    _priv_datetime_epoch = _utc_datetime_epoch.astimezone(_priv_tz)

cdef void async_callback_wrapper(void *param, TAOS_RES *res, int code) nogil:
    with gil:
        callback = <object>param
        print("callback:", callback, <size_t>res, code)
        dt_epoch = _priv_datetime_epoch if _priv_datetime_epoch else _datetime_epoch
        fields = taos_fetch_fields(res)
        field_count = taos_field_count(res)
        taos_fields = [TaosField((<bytes>f.name).decode("utf-8"), f.type, f.bytes) for f in fields[:field_count]]
        block, num_of_rows = taos_fetch_block_v3(res, fields, field_count, dt_epoch)
        errno = taos_errno(res)

        if errno != 0:
            raise ProgrammingError(taos_errstr(res), errno)

        for row in map(tuple, zip(*block)):
            callback(row, taos_fields)


cdef class TaosConnection:
    cdef char *_host
    cdef char *_user
    cdef char *_password
    cdef char *_database
    cdef uint16_t _port
    cdef char *_tz
    cdef char *_config
    cdef TAOS *_raw_conn

    def __cinit__(self, host=None, user="root", password="taosdata", database=None, port=None, timezone=None, config=None):
        if host:
            host = host.encode("utf-8")
            self._host = <char*>host

        if user:
            user = user.encode("utf-8")
            self._user = <char*>user

        if password:
            password = password.encode("utf-8")
            self._password = <char*>password

        if database:
            database =database.encode("utf-8")
            self._database = <char*>database

        if port:
            self._port = port

        if timezone:
            timezone = timezone.encode("utf-8")
            self._tz = <char*>timezone

        if config:
            config = config.encode("utf-8")
            self._config = <char*>config

        self._init_options()
        self._init_conn()
        self._check_conn_error()

    def _init_options(self):
        if self._tz:
            tz = <bytes>self._tz
            set_tz(pytz.timezone(tz.decode("utf-8")))
            taos_options(TSDB_OPTION.TSDB_OPTION_TIMEZONE, self._tz)

        if self._config:
            taos_options(TSDB_OPTION.TSDB_OPTION_CONFIGDIR, self._config)

    def _init_conn(self):
        self._raw_conn = taos_connect(self._host, self._user, self._password, self._database, self._port)

    def _check_conn_error(self):
        errno = taos_errno(self._raw_conn)
        if errno != 0:
            errstr = taos_errstr(self._raw_conn)
            raise ConnectionError(errstr, errno)

    def __dealloc__(self):
        self.close()

    def close(self):
        if self._raw_conn is not NULL:
            taos_close(self._raw_conn)
            self._raw_conn = NULL

    @property
    def client_info(self):
        return (<bytes>taos_get_client_info()).decode("utf-8")

    @property
    def server_info(self):
        # type: () -> str
        if self._raw_conn is NULL:
            return
        return (<bytes>taos_get_server_info(self._raw_conn)).decode("utf-8")

    def select_db(self, database: str):
        _database = database.encode("utf-8")
        res = taos_select_db(self._raw_conn, <char*>_database)
        if res != 0:
            raise DatabaseError("select database error", res)

    def execute(self, sql: str, req_id: Optional[int] = None):
        return self.query(sql, req_id).affected_rows

    def query(self, sql: str, req_id: Optional[int] = None):
        _sql = sql.encode("utf-8")
        if req_id is None:
            res = taos_query(self._raw_conn, <char*>_sql)
        else:
            res = taos_query_with_reqid(self._raw_conn, <char*>_sql, req_id)

        return TaosResult(<size_t>res)

    def query_a(self, sql: str, callback, req_id: Optional[int] = None):
        _sql = sql.encode("utf-8")
        if req_id is None:
            taos_query_a(self._raw_conn, <char*>_sql, async_callback_wrapper, <void*>callback)
        else:
            taos_query_a_with_reqid(self._raw_conn, <char*>_sql, async_callback_wrapper, <void*>callback, req_id)

    def load_table_info(self, tables: list):
        # type: (str) -> None
        _tables = ",".join(tables).encode("utf-8")
        taos_load_table_info(self._raw_conn, <char*>_tables)

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

    def get_table_vgroup_id(self, db: str, table: str):
        # type: (str, str) -> int
        """
        get table's vgroup id. It's require db name and table name, and return an int type vgroup id.
        """
        cdef int vg_id
        _db = db.encode("utf-8")
        _table = table.encode("utf-8")
        code = taos_get_table_vgId(self._raw_conn, <char*>_db, <char*>_table, &vg_id)
        if code != 0:
            raise InternalError(taos_errstr(NULL))
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
    cdef TAOS_RES *_res
    cdef TAOS_FIELD *_fields
    cdef int _field_count
    cdef int _precision
    cdef int _row_count
    cdef int _affected_rows

    def __cinit__(self, size_t res):
        self._res = <TAOS_RES*>res
        self._check_result_error()
        self._field_count = taos_field_count(self._res)
        self._fields = taos_fetch_fields(self._res)
        self._precision = taos_result_precision(self._res)
        self._affected_rows = taos_affected_rows(self._res)
        self._row_count = 0

    def __str__(self):
        return "TaosResult{res: %s}" % (<size_t>self._res, )

    def __repr__(self):
        return "TaosResult{res: %s}" % (<size_t>self._res, )

    def __iter__(self):
        return self.rows_iter()

    @property
    def fields(self):
        return [TaosField((<bytes>f.name).decode("utf-8"), f.type, f.bytes) for f in self._fields[:self._field_count]]

    @property
    def field_count(self):
        return self._field_count

    @property
    def precision(self):
        return self._precision

    @property
    def affected_rows(self):
        return self._affected_rows

    @property
    def row_count(self):
        return self._row_count

    def _check_result_error(self):
        errno =  taos_errno(self._res)
        if errno != 0:
            errstr = taos_errstr(self._res)
            raise ProgrammingError(errstr, errno)

    def _fetch_block(self):
        cdef TAOS_ROW pblock
        num_of_rows = taos_fetch_block(self._res, &pblock)
        if num_of_rows == 0:
            return [], 0

        blocks = [None] * self._field_count
        dt_epoch = _priv_datetime_epoch if _priv_datetime_epoch else _datetime_epoch
        cdef int i
        for i in range(self._field_count):
            data = pblock[i]
            field = self._fields[i]

            if field.type in UNSIZED_TYPE:
                offsets = taos_get_column_data_offset(self._res, i)
                blocks[i] = _parse_string(<size_t>data, num_of_rows, offsets)
            elif field.type in SIZED_TYPE:
                is_null = taos_get_column_data_is_null(self._res, i, num_of_rows)
                blocks[i] = CONVERT_FUNC[field.type](<size_t>data, num_of_rows, is_null)
            elif field.type in (TSDB_DATA_TYPE_TIMESTAMP, ):
                is_null = taos_get_column_data_is_null(self._res, i, num_of_rows)
                blocks[i] = _parse_timestamp(<size_t>data, num_of_rows, is_null, self._precision, dt_epoch)
            else:
                pass

        return blocks, abs(num_of_rows)

    def fetch_block(self):
        if self._res is NULL:
            raise OperationalError("Invalid use of fetch iterator")

        block, num_of_rows = self._fetch_block()
        self._row_count += num_of_rows

        return [r for r in map(tuple, zip(*block))], num_of_rows

    def fetch_all(self):
        if self._res is NULL:
            raise OperationalError("Invalid use of fetchall")

        blocks = []
        while True:
            block, num_of_rows = self._fetch_block()
            self._check_result_error()

            if num_of_rows == 0:
                break

            self._row_count += num_of_rows
            blocks.append(block)

        return [r for b in blocks for r in map(tuple, zip(*b))]

    def fetch_all_into_dict(self):
        if self._res is NULL:
            raise OperationalError("Invalid use of fetchall")

        field_names = [field.name for field in self.fields]
        dict_row_cls = namedtuple('DictRow', field_names)
        blocks = []
        while True:
            block, num_of_rows = self._fetch_block()
            self._check_result_error()

            if num_of_rows == 0:
                break

            self._row_count += num_of_rows
            blocks.append(block)

        return [dict_row_cls(*r)._asdict() for b in blocks for r in map(tuple, zip(*b))]

    def rows_iter(self):
        if self._res is NULL:
            raise OperationalError("Invalid use of rows_iter")

        cdef TAOS_ROW taos_row
        cdef int i
        dt_epoch = _priv_datetime_epoch if _priv_datetime_epoch else _datetime_epoch
        is_null = [False]

        while True:
            taos_row = taos_fetch_row(self._res)
            if taos_row is NULL:
                break

            row = [None] * self._field_count
            for i in range(self._field_count):
                data = taos_row[i]
                field = self._fields[i]
                if field.type in (TSDB_DATA_TYPE_BINARY, TSDB_DATA_TYPE_VARBINARY):
                    row[i] = _parse_binary_string(<size_t>data, 1, field.bytes)[0]
                elif field.type in (TSDB_DATA_TYPE_NCHAR, TSDB_DATA_TYPE_VARCHAR):
                    row[i] = _parse_nchar_string(<size_t>data, 1, field.bytes)[0]
                elif field.type in SIZED_TYPE:
                    row[i] = CONVERT_FUNC[field.type](<size_t>data, 1, is_null)[0]
                elif field.type in (TSDB_DATA_TYPE_TIMESTAMP, ):
                    row[i] = _parse_timestamp(<size_t>data, 1, is_null, self._precision, dt_epoch)[0]
                else:
                    pass

            self._row_count += 1
            yield row

    def blocks_iter(self):
        if self._res is NULL:
            raise OperationalError("Invalid use of rows_iter")

        while True:
            block, num_of_rows = self._fetch_block()

            if num_of_rows == 0:
                break

            yield [r for r in map(tuple, zip(*block))], num_of_rows

    def fetch_rows_a(self, callback):
        taos_fetch_rows_a(self._res, async_callback_wrapper, <void*>callback)

    def taos_fetch_raw_block_a(self, callback):
        taos_fetch_raw_block_a(self._res, async_callback_wrapper, <void*>callback)

    def __dealloc__(self):
        if self._res is not NULL:
            taos_free_result(self._res)

        self._res = NULL
        self._fields = NULL

# class TaosStmt(object):
#     cdef TAOS_STMT *_stmt
#
#     def __cinit__(self, size_t stmt):
#         self._stmt = <TAOS_STMT*>stmt
#
#     def set_tbname(self, name: str):
#         if self._stmt is NULL:
#             raise StatementError("Invalid use of set_tbname")
#
#         _name = name.decode("utf-8")
#         taos_stmt_set_tbname(self._stmt, <char*>_name)
#
#     def prepare(self, sql: str):
#         _sql = sql.decode("utf-8")
#         taos_stmt_prepare(self._stmt, <char*>_sql, len(_sql))
#
#     def set_tbname_tags(self, name: str, tags: list):
#         # type: (str, Array[TaosBind]) -> None
#         """Set table name with tags, tags is array of BindParams"""
#         if self._stmt is NULL:
#             raise StatementError("Invalid use of set_tbname_tags")
#         taos_stmt_set_tbname_tags(self._stmt, name, NULL)
#
#     def bind_param(self, params, add_batch=True):
#         # type: (Array[TaosBind], bool) -> None
#         if self._stmt is None:
#             raise StatementError("Invalid use of stmt")
#         taos_stmt_bind_param(self._stmt, params)
#         if add_batch:
#             taos_stmt_add_batch(self._stmt)
#
#     def bind_param_batch(self, binds, add_batch=True):
#         # type: (Array[TaosMultiBind], bool) -> None
#         if self._stmt is NULL:
#             raise StatementError("Invalid use of stmt")
#         taos_stmt_bind_param_batch(self._stmt, binds)
#         if add_batch:
#             taos_stmt_add_batch(self._stmt)
#
#     def add_batch(self):
#         if self._stmt is NULL:
#             raise StatementError("Invalid use of stmt")
#         taos_stmt_add_batch(self._stmt)
#
#     def execute(self):
#         if self._stmt is NULL:
#             raise StatementError("Invalid use of execute")
#         taos_stmt_execute(self._stmt)
#
#     def use_result(self):
#         """NOTE: Don't use a stmt result more than once."""
#         result = taos_stmt_use_result(self._stmt)
#         return TaosResult(<size_t>result)
#
#     @property
#     def affected_rows(self):
#         # type: () -> int
#         return taos_stmt_affected_rows(self._stmt)
#
#     def close(self):
#         """Close stmt."""
#         if self._stmt is NULL:
#             return
#         taos_stmt_close(self._stmt)
#         self._stmt = NULL
#
#     def __dealloc__(self):
#         self.close()
#
#
# class TaosMultiBind:
#     cdef int       buffer_type
#     cdef void     *buffer
#     cdef uintptr_t buffer_length
#     cdef int32_t  *length
#     cdef char     *is_null
#     cdef int       num
