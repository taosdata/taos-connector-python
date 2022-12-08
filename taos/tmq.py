from re import T
from taos.cinterface import *
from taos.error import *
from taos.result import *

class TaosConsumer():
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
            configs.update({k.replace('_','.'): configs.pop(k)})

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
    
class TaosTmq(object):
    def __init__(self, conf):
        self._tmq  = tmq_consumer_new(conf.conf())
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
