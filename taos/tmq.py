from taos.cinterface import *
from taos.error import *
from taos.result import TaosResult


# deprecated. we will delete TaosTmqList in the feature version
class TaosConsumer:
    DEFAULT_CONFIG = {
        'group.id',
        'client.id',
        'enable.auto.commit',
        'auto.commit.interval.ms',
        'auto.offset.reset',
        'msg.with.table.name',
        'experimental.snapshot.enable',
        'enable.heartbeat.background',
        'experimental.snapshot.batch.size',
        'td.connect.ip',
        'td.connect.user',
        'td.connect.pass',
        'td.connect.port',
        'td.connect.db',
        'timeout'
    }

    def __init__(self, *topics, **configs):
        self._closed = True
        self._conf = None
        self._list = None
        self._tmq = None

        keys = list(configs.keys())
        for k in keys:
            configs.update({k.replace('_', '.'): configs.pop(k)})

        extra_configs = set(configs).difference(self.DEFAULT_CONFIG)
        if extra_configs:
            raise TmqError("Unrecognized configs: %s" % (extra_configs,))

        self._conf = tmq_conf_new()
        self._list = tmq_list_new()

        # set poll timeout
        if 'timeout' in configs:
            self._timeout = configs['timeout']
            del configs['timeout']
        else:
            self._timeout = 0

        # check if group id is set

        if 'group.id' not in configs:
            raise TmqError("missing group.id in consumer config setting")

        for key, value in configs.items():
            tmq_conf_set(self._conf, key, value)

        self._tmq = tmq_consumer_new(self._conf)

        if not topics:
            raise TmqError("Unset topic for Consumer")

        for topic in topics:
            tmq_list_append(self._list, topic)

        tmq_subscribe(self._tmq, self._list)

    def __iter__(self):
        return self

    def __next__(self):
        if not self._tmq:
            raise StopIteration('TaosConsumer closed')
        return next(self.sync_next())

    def sync_next(self):
        while 1:
            res = tmq_consumer_poll(self._tmq, self._timeout)
            if res:
                break
        yield TaosResult(res)

    def subscription(self):
        if self._tmq is None:
            return None
        return tmq_subscription(self._tmq)

    def unsubscribe(self):
        tmq_unsubscribe(self._tmq)

    def close(self):
        if self._tmq:
            tmq_consumer_close(self._tmq)
            self._tmq = None

    def __del__(self):
        if self._conf:
            tmq_conf_destroy(self._conf)
        if self._list:
            tmq_list_destroy(self._list)
        if self._tmq:
            tmq_consumer_close(self._tmq)


class TaosTmqConf(object):
    def __init__(self):
        self._conf = tmq_conf_new()

    def set(self, key, value):
        tmq_conf_set(self._conf, key, value)

    def set_auto_commit_cb(self, cb, param):
        tmq_conf_set_auto_commit_cb(self._conf, cb, param)

    def __del__(self):
        tmq_conf_destroy(self._conf)

    def new_consumer(self):
        return TaosTmq(self)

    def conf(self):
        return self._conf


# deprecated. we will delete TaosTmqList in the feature version
class TaosTmq(object):
    def __init__(self, conf):
        self._tmq = tmq_consumer_new(conf.conf())
        self._result = None

    def subscribe(self, list):
        tmq_subscribe(self._tmq, list.list())

    def unsubscribe(self):
        tmq_unsubscribe(self._tmq)

    def subscription(self):
        return tmq_subscription(self._tmq)

    def poll(self, time):
        # type: (int) -> TaosResult
        result = tmq_consumer_poll(self._tmq, time)

        if result:
            self._result = TaosResult(result, True)
            return self._result
        else:
            return None

    def __del__(self):
        try:
            tmq_unsubscribe(self._tmq)
            tmq_consumer_close(self._tmq)
        except TmqError:
            pass

    def commit(self, result):
        # type: (TaosResult) -> None
        if result is None and not isinstance(result, TaosResult):
            tmq_commit_sync(self._tmq, None)
        else:
            tmq_commit_sync(self._tmq, result._result)


# deprecated. we will delete TaosTmqList in the feature version
class TaosTmqList(object):
    def __init__(self):
        self._list = tmq_list_new()

    def append(self, topic):
        tmq_list_append(self._list, topic)

    def __del__(self):
        tmq_list_destroy(self._list)

    def to_array(self):
        return tmq_list_to_c_array(self._list)

    def list(self):
        return self._list


TMQ_RES_INVALID = -1
TMQ_RES_DATA = 1
TMQ_RES_TABLE_META = 2
TMQ_RES_METADATA = 3


class MessageBlock:

    def __init__(self, block=None, fields=None, row_count=0, col_count=0, table=''):
        # type (list[tuple], TaosField, int, int, str)
        self._block = block
        self._fields = fields
        self._rows = row_count
        self._cols = col_count
        self._table = table

    def fields(self):
        # type: () -> TaosField
        """
        Get fields in message block
        """
        return self._fields

    def nrows(self):
        # type: () -> int
        """
        get total count of rows of message block
        """
        return self._rows

    def ncols(self):
        # type: () -> int
        """
        get total count of rows of message block
        """
        return self._cols

    def fetchall(self):
        # type: () -> list[tuple]
        """
        get all data in message block
        """
        return list(map(tuple, zip(*self._block)))

    def table(self):
        # type: () -> str
        """
        get table name of message block
        """
        return self._table

    def __iter__(self):
        return iter(self.fetchall())


class Message:

    def __init__(self, msg: c_void_p = None, error=None):
        self._error = error
        if not msg:
            return
        self.msg = msg
        err_no = taos_errno(self.msg)
        if err_no:
            self._error = TmqError(msg=taos_errstr(self.msg))

    def error(self):
        # type: () -> TmqError | None
        """

        The message object is also used to propagate errors and events, an application must check error() to determine
        if the Message is a proper message (error() returns None) or an error or event (error() returns a TmqError
         object)

          :rtype: None or :py:class:`TmqError
        """
        return self._error

    def topic(self):
        # type: () -> str
        """

        :returns: topic name.
          :rtype: str
        """
        return tmq_get_topic_name(self.msg)

    def database(self):
        # type () -> str
        """

        :returns: database name.
          :rtype: str
        """
        return tmq_get_db_name(self.msg)

    def value(self):
        # type: () -> list[MessageBlock] | None
        """
        :returns: message value (payload).
          :rtype: list[tuple]
        """

        res_type = tmq_get_res_type(self.msg)
        if res_type == TMQ_RES_TABLE_META or res_type == TMQ_RES_INVALID:
            return None

        message_blocks = []
        while True:
            block, num_rows = taos_fetch_block_raw(self.msg)
            if num_rows == 0:
                break
            field_count = taos_num_fields(self.msg)
            fields = taos_fetch_fields(self.msg)
            precision = taos_result_precision(self.msg)

            blocks = [None] * field_count
            for i in range(len(fields)):
                if fields[i]["type"] not in CONVERT_FUNC_BLOCK_v3 and fields[i]["type"] not in CONVERT_FUNC_BLOCK:
                    raise TmqError("Invalid data type returned from database")

                block_data = ctypes.cast(block, ctypes.POINTER(ctypes.c_void_p))[i]
                if fields[i]["type"] in (FieldType.C_VARCHAR, FieldType.C_NCHAR, FieldType.C_JSON):
                    offsets = taos_get_column_data_offset(self.msg, i, num_rows)
                    blocks[i] = CONVERT_FUNC_BLOCK_v3[fields[i]["type"]](block_data, [], num_rows, offsets, precision)
                else:
                    is_null = [taos_is_null(self.msg, j, i) for j in range(num_rows)]
                    blocks[i] = CONVERT_FUNC_BLOCK[fields[i]["type"]](block_data, is_null, num_rows, [], precision)

            message_blocks.append(
                MessageBlock(block=blocks, fields=fields, row_count=num_rows, col_count=field_count,
                             table=tmq_get_table_name(self.msg)))
        return message_blocks

    def __del__(self):
        if not self.msg:
            return
        taos_free_result(self.msg)

    def __iter__(self):
        return iter(self.value())


class Consumer:
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

    def __init__(self, configs):
        if 'group.id' not in configs:
            raise TmqError('missing group.id in consumer config setting')

        self._tmq = None
        self._subscribed = False
        tmq_conf = tmq_conf_new()
        try:
            for key in configs:
                if key not in self.default_config:
                    raise TmqError('Unrecognized configs: %s' % key)
                tmq_conf_set(tmq_conf, key=key, value=configs[key])

            self._tmq = tmq_consumer_new(tmq_conf)
        finally:
            tmq_conf_destroy(tmq_conf)

    def subscribe(self, topics):
        # type ([str]) -> None
        """
        Set subscription to supplied list of topics.
        :param list(str) topics: List of topics (strings) to subscribe to.
        """
        if not topics or len(topics) == 0:
            raise TmqError('Unset topic for Consumer')

        topic_list = tmq_list_new()
        for topic in topics:
            res = tmq_list_append(topic_list, topic)
            if res != 0:
                raise TmqError(msg="fail on parse topics", errno=res)
        tmq_subscribe(self._tmq, topic_list)

        self._subscribed = True
        tmq_list_destroy(topic_list)

    def unsubscribe(self):
        """
        Remove current subscription.
        """
        tmq_unsubscribe(self._tmq)
        self._subscribed = False

    def poll(self, timeout: float = 1.0):
        # type (float) -> Message | None
        """
        Consumes a single message and returns events.

        The application must check the returned `Message` object's `Message.error()` method to distinguish between
        proper messages (error() returns None).

        :param float timeout: Maximum time to block waiting for message, event or callback (default: 1). (second)
        :returns: A Message object or None on timeout
        :rtype: `Message` or None
        """
        mill_timeout = int(timeout * 1000)
        if not self._subscribed:
            raise TmqError(msg='unsubscribe topic')

        msg = tmq_consumer_poll(self._tmq, wait_time=mill_timeout)
        if msg:
            return Message(msg=msg)
        return None

    def close(self):
        """
        Close down and terminate the Kafka Consumer.
        """
        if self._tmq:
            tmq_consumer_close(self._tmq)
            self._tmq = None

    def commit(self, message):
        # type (Message) -> None
        """
        Commit a message.

        The `message` parameters are mutually exclusive. If `message` is None, the current partition assignment's
        offsets are used instead. Use this method to commit offsets if you have 'enable.auto.commit' set to False.

        :param Message message: Commit the message's offset.
        """
        if message is None and not isinstance(message, Message):
            tmq_commit_sync(self._tmq, None)
        else:
            tmq_commit_sync(self._tmq, message.msg)

    def __del__(self):
        self.close()

    def __next__(self):
        if not self._tmq:
            raise StopIteration('Tmq consumer is closed')
        return next(self._sync_next())

    def __iter__(self):
        return self

    def _sync_next(self):
        while True:
            message = self.poll()
            if message:
                break
        yield message
