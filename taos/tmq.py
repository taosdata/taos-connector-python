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
    
    def destroy(self):
        tmq_conf_destroy(self._conf)

    def conf(self):
        return self._conf
    
class TaosTmq(object):
    def __init__(self, conf):
        self._tmq  = tmq_consumer_new(conf.conf())
        
    def subscribe(self, list):
        tmq_subscribe(self._tmq, list.list())
        
    def unsubscribe(self):
        tmq_unsubscribe(self._tmq)
        
    def subscription(self, topics):
        tmq_subscription(self._tmq, topics.list())
        
    def poll(self, time):
        return tmq_consumer_poll(self._tmq, time)
        
    def close(self):
        tmq_consumer_close(self._tmq)
        
    def commit(self, offset, _async):
        tmq_commit(self._tmq, offset, _async)
        
        
class TaosTmqList(object):
    def __init__(self):
        self._list = tmq_list_new()
        
    def append(self, topic):
        tmq_list_append(self._list, topic)
        
    def size(self):
        return tmq_list_get_size(self._list)
    
    def destroy(self):
        tmq_list_destroy(self._list)
        
    def to_array(self):
        array = []
        c_array =  tmq_list_to_c_array(self._list)
        size = self.size()
        for i in range(size):
            array.append(c_array[i].decode("utf-8"))
        return array
        
    
    def list(self):
        return self._list