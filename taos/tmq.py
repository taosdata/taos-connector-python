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


class Message:

    def __init__(self, msg: c_void_p = None, error=None):
        self._error = error
        if not msg:
            return
        self.msg = msg
        err_no = taos_errno(self.msg)
        if err_no:
            self._error = TmqError(msg=taos_errstr(self.msg))
        self._db = tmq_get_db_name(self.msg)
        self._table = tmq_get_table_name(self.msg)
        self._fields = taos_fetch_fields(self.msg)
        self._topic = tmq_get_topic_name(self.msg)

    def error(self) -> TmqError | None:
        """

        The message object is also used to propagate errors and events, an application must check error() to determine
        if the Message is a proper message (error() returns None) or an error or event (error() returns a TmqError
         object)

          :rtype: None or :py:class:`TmqError
        """
        return self._error

    def topic(self) -> str:
        """

        :returns: topic name.
          :rtype: str or None
        """
        return self._topic

    def table(self) -> str:
        """

        :returns: table name.
          :rtype: str or None
        """
        return self._table

    def db(self) -> str:
        """

        :returns: table name.
          :rtype: str or None
        """
        return self._db

    def value(self) -> list:
        """
        :returns: message value (payload).
          :rtype: list[tuple]
        """
        buffer = [[] for i in range(len(self._fields))]
        while True:
            blocks, num_of_fields = taos_fetch_block(result=self.msg, fields=self._fields)
            if num_of_fields == 0:
                break
            for i in range(len(blocks)):
                buffer[i].extend(blocks[i])
        return list(map(tuple, zip(*buffer)))

    def __del__(self):
        if not self.msg:
            return
        taos_free_result(self.msg)


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

    def __init__(self, configs: dict):
        self._tmq_conf = tmq_conf_new()
        self._topics = tmq_list_new()
        self._tmq = None

        if 'group.id' not in configs:
            raise TmqError('missing group.id in consumer config setting')

        for key in configs:
            if key not in self.default_config:
                raise TmqError('Unrecognized configs: %s' % key)
            tmq_conf_set(self._tmq_conf, key=key, value=configs[key])

        self._tmq = tmq_consumer_new(self._tmq_conf)
        self._subscribed = False

    def subscribe(self, topics: [str]):
        """
        Set subscription to supplied list of topics.
        :param list(str) topics: List of topics (strings) to subscribe to.
        """
        if not topics or len(topics) == 0:
            raise TmqError('Unset topic for Consumer')
        for topic in topics:
            tmq_list_append(self._topics, topic)
        tmq_subscribe(self._tmq, self._topics)
        self._subscribed = True

    def unsubscribe(self):
        """
        Remove current subscription.
        """
        tmq_unsubscribe(self._tmq)
        self._subscribed = False

    def poll(self, timeout: int = 10) -> Message | None:
        """
        Consumes a single message and returns events.

        The application must check the returned `Message` object's `Message.error()` method to distinguish between
        proper messages (error() returns None).

        :param int timeout: Maximum time to block waiting for message, event or callback (default: 10). (MillSecond)
        :returns: A Message object or None on timeout
        :rtype: `Message` or None
        """
        if not self._subscribed:
            return Message(error=TmqError(msg='unsubscribe topic'))

        msg = tmq_consumer_poll(self._tmq, wait_time=timeout)
        if msg:
            return Message(msg=msg)
        return None

    def _sync_next(self):
        while True:
            message = self.poll()
            if message:
                break
        yield message

    def close(self):
        """
        Close down and terminate the Kafka Consumer.
        """
        if self._tmq:
            tmq_consumer_close(self._tmq)
            self._tmq = None
        if self._tmq_conf:
            tmq_conf_destroy(self._tmq_conf)
            self._tmq_conf = None
        if self._topics:
            tmq_list_destroy(self._topics)
            self._topics = None

    def commit(self, message):
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
