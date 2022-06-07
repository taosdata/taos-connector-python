from taos.cinterface import *
from taos.error import *
from taos.result import *

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
        
    def subscribe(self, list):
        tmq_subscribe(self._tmq, list.list())
        
    def unsubscribe(self):
        tmq_unsubscribe(self._tmq)
        
    def subscription(self):
        return tmq_subscription(self._tmq)
        
    def poll(self, time):
        result = tmq_consumer_poll(self._tmq, time)
        if result:
            return TaosResult(result)
        else:
            return None
        
    def __del__(self):
        tmq_consumer_close(self._tmq)
        
    def commit(self, offset, _async):
        tmq_commit(self._tmq, offset, _async)
        
        
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