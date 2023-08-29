# cython: profile=True

import asyncio
from typing import Optional
from taos._cinterface cimport *
from taos._parser cimport _parse_string, _parse_timestamp, _parse_binary_string, _parse_nchar_string
from taos._cinterface import SIZED_TYPE, UNSIZED_TYPE, CONVERT_FUNC
import datetime as dt
import pytz
from collections import namedtuple
from taos.error import ProgrammingError, OperationalError, ConnectionError, DatabaseError, StatementError, InternalError, TmqError

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


cdef void async_result_future_wrapper(void *param, TAOS_RES *res, int code) nogil:
    with gil:
        fut = <object>param
        if code != 0:
            e = ProgrammingError(taos_errstr(res), code)
            fut.get_loop().call_soon_threadsafe(fut.set_exception, e)
        else:
            taos_result = TaosResult(<size_t>res)
            fut.get_loop().call_soon_threadsafe(fut.set_result, taos_result)


cdef void async_rows_future_wrapper(void *param, TAOS_RES *res, int num_of_rows) nogil:
    cdef int i = 0
    with gil:
        taos_result, fut = <tuple>param
        rows = []
        if num_of_rows > 0:
            for row in taos_result.rows_iter():
                rows.append(row)
                i += 1
                if i >= num_of_rows:
                    break

        fut.get_loop().call_soon_threadsafe(fut.set_result, rows)


cdef void async_block_future_wrapper(void *param, TAOS_RES *res, int num_of_rows) nogil:
    cdef int i = 0
    with gil:
        taos_result, fut = <tuple>param
        block, n = [], 0
        if num_of_rows > 0:
            block, n = taos_result.fetch_block()

        fut.get_loop().call_soon_threadsafe(fut.set_result, (block, n))


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
            self._host = host

        if user:
            user = user.encode("utf-8")
            self._user = user

        if password:
            password = password.encode("utf-8")
            self._password = password

        if database:
            database =database.encode("utf-8")
            self._database = database

        if port:
            self._port = port or 0

        if timezone:
            timezone = timezone.encode("utf-8")
            self._tz = timezone

        if config:
            config = config.encode("utf-8")
            self._config = config

        self._init_options()
        self._init_conn()
        self._check_conn_error()

    def _init_options(self):
        if self._tz:
            set_tz(pytz.timezone(self._tz.decode("utf-8")))
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

    @property
    def client_info(self):
        return taos_get_client_info().decode("utf-8")

    @property
    def server_info(self):
        # type: () -> str
        if self._raw_conn is NULL:
            return
        return taos_get_server_info(self._raw_conn).decode("utf-8")

    def select_db(self, database: str):
        _database = database.encode("utf-8")
        res = taos_select_db(self._raw_conn, _database)
        if res != 0:
            raise DatabaseError("select database error", res)

    def execute(self, sql: str, req_id: Optional[int] = None):
        return self.query(sql, req_id).affected_rows

    def query(self, sql: str, req_id: Optional[int] = None):
        _sql = sql.encode("utf-8")
        if req_id is None:
            res = taos_query(self._raw_conn, _sql)
        else:
            res = taos_query_with_reqid(self._raw_conn, _sql, req_id)

        return TaosResult(<size_t>res)

    async def query_async(self, sql: str, req_id: Optional[int] = None):
        loop = asyncio.get_event_loop()
        _sql = sql.encode("utf-8")
        fut = loop.create_future()
        if req_id is None:
            taos_query_a(self._raw_conn, _sql, async_result_future_wrapper, <void*>fut)
        else:
            taos_query_a_with_reqid(self._raw_conn, _sql, async_result_future_wrapper, <void*>fut, req_id)

        res = await fut
        return res

    def subscribe(self, configs: dict[str, str], callback):
        if self._raw_conn is NULL:
            return None

        tmq_conf = tmq_conf_new()
        for k, v in configs.items():
            _k = k.encode("utf-8")
            _v = v.encode("utf-8")
            tmq_conf_res = tmq_conf_set(tmq_conf, _k, _v)

    def load_table_info(self, tables: list):
        _tables = ",".join(tables).encode("utf-8")
        taos_load_table_info(self._raw_conn, _tables)

    def close(self):
        if self._raw_conn is not NULL:
            taos_close(self._raw_conn)
            self._raw_conn = NULL

    def commit(self):
        """Commit any pending transaction to the database.

        Since TDengine do not support transactions, the implement is void functionality.
        """
        pass

    def rollback(self):
        """Void functionality"""
        pass

    def cursor(self):
        return TaosCursor(self)

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
        code = taos_get_table_vgId(self._raw_conn, _db, _table, &vg_id)
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
        return [TaosField(f.name.decode("utf-8"), f.type, f.bytes) for f in self._fields[:self._field_count]]

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
        cdef int[1] offsets = [-2]
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
                if field.type in UNSIZED_TYPE:
                    row[i] = _parse_string(<size_t>data, 1, offsets)[0]
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

    async def rows_iter_a(self):
        if self._res is NULL:
            raise OperationalError("Invalid use of rows_iter_a")

        loop = asyncio.get_event_loop()
        while True:
            fut = loop.create_future()
            param = (self, fut)
            taos_fetch_rows_a(self._res, async_rows_future_wrapper, <void*>param)

            rows = await fut
            if not rows:
                break
            
            for row in rows:
                yield row

    async def blocks_iter_a(self):
        if self._res is NULL:
            raise OperationalError("Invalid use of blocks_iter_a")

        loop = asyncio.get_event_loop()
        while True:
            fut = loop.create_future()
            param = (self, fut)
            taos_fetch_rows_a(self._res, async_block_future_wrapper, <void*>param)
            # taos_fetch_raw_block_a(self._res, async_block_future_wrapper, <void*>param)

            block, n = await fut
            if not block:
                break
            
            yield block, n

    def __dealloc__(self):
        if self._res is not NULL:
            taos_free_result(self._res)

        self._res = NULL
        self._fields = NULL


cdef class TaosCursor:
    cdef list _description
    cdef TaosConnection _connection
    cdef TaosResult _result

    def __init__(self, connection: TaosConnection=None) -> None:
        self._description = []
        self._connection = connection
        self._result = None

    def __iter__(self):
        for block, _ in self._result.blocks_iter():
            for row in block:
                yield row

    def next(self):
        return next(self)

    @property
    def description(self):
        return self._description

    @property
    def rowcount(self):
        return self._result.row_count

    @property
    def affected_rows(self):
        """Return the rowcount of insertion"""
        return self._result.affected_rows

    def callproc(self, procname, *args):
        """Call a stored database procedure with the given name.

        Void functionality since no stored procedures.
        """
        pass

    def close(self):
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

    def execute_many(self, operation, data_list, req_id: Optional[int] = None):
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

    def fetchone(self):
        try:
            row = next(self)
        except StopIteration:
            row = None
        return row

    def fetchmany(self, size=None):
        size = size or 1
        rows = []
        for row in self:
            rows.append(row)
        
        return rows

    def fetchall(self):
        return [r for r in self]

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


# ---------------------------------------- TMQ --------------------------------------------------------------- v
cdef void async_commit_future_wrapper(tmq_t *tmq, int32_t code, void *param) nogil:
    with gil:
        fut = <object>param
        fut.get_loop().call_soon_threadsafe(fut.set_result, code)


cdef class TopicPartition:
    cdef char *_topic
    cdef int32_t _partition
    cdef int64_t _offset
    cdef int64_t _begin
    cdef int64_t _end

    def __cinit__(self, str topic, int32_t partition, int64_t offset, int64_t begin, int64_t end):
        _topic = topic.encode("utf-8")
        self._topic = _topic
        self._partition = partition
        self._offset = offset
        self._begin = begin
        self._end = end

    @property
    def topic(self):
        return self._topic.decode("utf-8")

    @property
    def partition(self):
        return self._partition

    @property
    def offset(self):
        return self._offset

    @property
    def begin(self):
        return self._begin

    @property
    def end(self):
        return self._end


cdef class TaosConsumer:
    cdef dict _configs
    cdef tmq_conf_t *_tmq_conf
    cdef tmq_t *_tmq
    cdef bool _subscribed
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

    def __cinit__(self, configs: dict[str, str]):
        self._init_config()
        self._init_consumer()
        self._subscribed = False

    def _init_config(self, configs: dict[str, str]):
        if 'group.id' not in configs:
            raise TmqError('missing group.id in consumer config setting')

        self._configs = configs
        self._tmq_conf = tmq_conf_new()
        if self._tmq_conf:
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
        
        self._tmq = tmq_consumer_new(self._tmq_conf, NULL, 0)
        if self._tmq is NULL:
            raise TmqError("new tmq consumer failed")
    
    def subscribe(self, topics: list[str]):
        tmq_list = tmq_list_new()
        if tmq_list is NULL:
            raise TmqError("new tmq list failed!")
        
        for tp in topics:
            _tp = tp.encode("utf-8")
            tmq_errno = tmq_list_append(tmq_list, _tp)
            if tmq_errno != 0:
                tmq_list_destroy(tmq_list)
                raise TmqError(tmq_err2str(tmq_errno), tmq_errno)

        tmq_errno = tmq_subscribe(self._tmq, tmq_list)
        if tmq_errno != 0:
            tmq_list_destroy(tmq_list)
            raise TmqError(tmq_err2str(tmq_errno), tmq_errno)
        
        self._subscribed = True

    def unsubscribe(self):
        tmq_errno = tmq_unsubscribe(self._tmq)
        if tmq_errno != 0:
            raise TmqError(tmq_err2str(tmq_errno), tmq_errno)
        
        self._subscribed = False

    def close(self):
        if self._tmq_conf is not NULL:
            tmq_conf_destroy(self._tmq_conf)
            self._tmq_conf = NULL

        if self._tmq is not NULL:
            tmq_unsubscribe(self._tmq)
            tmq_consumer_close(self._tmq)
            self._tmq = NULL

    def poll(self, float timeout=1.0):
        if not self._subscribed:
            raise TmqError("unsubscribe topic")

        timeout_ms = int(timeout * 1000)
        res = tmq_consumer_poll(self._tmq, timeout_ms)
        if res is NULL:
            return None
        
        return TaosResult(<size_t>res)

    def commit(self, taos_res: TaosResult):
        self.result_commit(taos_res)

    async def commit_a(self, taos_res: TaosResult):
        await self.result_commit_a(taos_res)

    def result_commit(self, taos_res: TaosResult):
        tmq_errno = tmq_commit_sync(self._tmq, taos_res._res)
        if tmq_errno != 0:
            raise TmqError(tmq_err2str(tmq_errno), tmq_errno)

    async def result_commit_a(self, taos_res: TaosResult):
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        tmq_commit_async(self._tmq, taos_res._res, async_commit_future_wrapper, <void*>fut)
        tmq_errno = await fut
        if tmq_errno != 0:
            raise TmqError(tmq_err2str(tmq_errno), tmq_errno)

    def offset_commit(self, topic: str, vg_id: int, offset: int):
        _topic = topic.encode("utf-8")
        tmq_errno = tmq_commit_offset_sync(self._tmq, _topic, vg_id, offset)
        if tmq_errno != 0:
            raise TmqError(tmq_err2str(tmq_errno), tmq_errno)
    
    async def offset_commit_a(self, topic: str, vg_id: int, offset: int):
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        _topic = topic.encode("utf-8")
        tmq_commit_offset_async(self._tmq, _topic, vg_id, offset, async_commit_future_wrapper, <void*>fut)
        tmq_errno = await fut
        if tmq_errno != 0:
            raise TmqError(tmq_err2str(tmq_errno), tmq_errno)

    def assignment(self):
        cdef int32_t i
        cdef int32_t num_of_assignment
        cdef tmq_topic_assignment *p_assignment = NULL

        topics = self.list_topics()
        topic_partitions = []
        for topic in topics:
            _topic = topic.encode("utf-8")
            tmq_errno = tmq_get_topic_assignment(self._tmq, _topic, &p_assignment, &num_of_assignment)
            if tmq_errno != 0:
                tmq_free_assignment(p_assignment)
                raise TmqError(tmq_err2str(tmq_errno), tmq_errno)

            for i in range(num_of_assignment):
                assignment = p_assignment[i]
                tp = TopicPartition(topic, assignment.vgId, assignment.currentOffset, assignment.begin, assignment.end)
                topic_partitions.append(tp)

            tmq_free_assignment(p_assignment)

        return topic_partitions

    def seek(self, partition: TopicPartition):
        """
        Set consume position for partition to offset.
        """
        tmq_errno = tmq_offset_seek(self._tmq, partition._topic, partition._partition, partition._offset)
        if tmq_errno != 0:
            raise TmqError(tmq_err2str(tmq_errno), tmq_errno)

        tmq_offset_seek(self._tmq, partition.topic, partition.partition, partition.offset)

    def committed(self, partitions: list[TopicPartition]) -> list[TopicPartition]:
        """
        Retrieve committed offsets for the specified partitions.

        :param list(TopicPartition) partitions: List of topic+partitions to query for stored offsets.
        :returns: List of topic+partitions with offset and possibly error set.
        :rtype: list(TopicPartition)
        """
        for partition in partitions:
            if not isinstance(partition, TopicPartition):
                raise TmqError(msg='Invalid partition type')
            offset = tmq_committed(self._tmq, partition._topic, partition._partition)

            if offset < 0:
                raise TmqError(tmq_err2str(offset), offset)
            
            partition._offset = offset

        return partitions

    def position(self, partitions: list[TopicPartition]) -> list[TopicPartition]:
        """
        Retrieve current positions (offsets) for the specified partitions.

        :param list(TopicPartition) partitions: List of topic+partitions to return current offsets for.
        :returns: List of topic+partitions with offset and possibly error set.
        :rtype: list(TopicPartition)
        """
        for partition in partitions:
            if not isinstance(partition, TopicPartition):
                raise TmqError(msg='Invalid partition type')
            offset = tmq_position(self._tmq, partition._topic, partition._partition)

            if offset < 0:
                raise TmqError(tmq_err2str(offset), offset)

            partition._offset = offset

        return partitions

    def list_topics(self):
        cdef int i
        cdef tmq_list_t *topics
        tmq_errno = tmq_subscription(self._tmq, &topics)
        if tmq_errno != 0:
            raise TmqError(tmq_err2str(tmq_errno), tmq_errno)

        n = tmq_list_get_size(topics)
        ca = tmq_list_to_c_array(topics)
        tp_list = []
        for i in range(n):
            tp_list.append(ca[i])

        tmq_list_destroy(topics)

        return tp_list

    def get_topic_name(self, taos_res: TaosResult):
        return tmq_get_topic_name(taos_res._res)

    def get_db_name(self, taos_res: TaosResult):
        return tmq_get_db_name(taos_res._res)

    def get_vgroup_id(self, taos_res: TaosResult):
        return tmq_get_vgroup_id(taos_res._res)

    def get_vgroup_offset(self, taos_res: TaosResult):
        return tmq_get_vgroup_offset(taos_res._res)

    def __dealloc__(self):
        self.close()

# ---------------------------------------- TMQ --------------------------------------------------------------- ^

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
