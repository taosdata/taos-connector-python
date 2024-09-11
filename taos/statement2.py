from taos.cinterface import *
from taos.error import StatementError
from taos.result import TaosResult



_taos_async_fn_t = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int)

class TaosStmt2Option(ctypes.Structure):
    _fields_ = [
        ("reqid", ctypes.c_int64),
        ("singleStbInsert", ctypes.c_bool),
        ("singleTableBindOnce", ctypes.c_bool),
        ("asyncExecFn", _taos_async_fn_t),
        ("userdata", ctypes.c_void_p)
    ]


class TaosStmt2(object):
    """TDengine STMT2 interface"""

    def __init__(self, stmt2, decode_binary=True):
        self._stmt2 = stmt2
        self._decode_binary = decode_binary

    # def set_tbname(self, name):
    #     """Set table name if needed.
    #
    #     Note that the set_tbname* method should only used in insert statement
    #     """
    #     if self._stmt2 is None:
    #         raise StatementError("Invalid use of set_tbname")
    #     taos_stmt_set_tbname(self._stmt2, name)

    def prepare(self, sql):
        # type: (str) -> None
        taos_stmt2_prepare(self._stmt2, sql)

   
    def checkConsistent(self, tbnames, tags, datas):
        return True
    
    def createBindV(self, tbnames, tags, datas):
        pass

    def bind_param(self, tbnames, tags, datas):
        if self._stmt2 is None:
            raise StatementError("stmt2 object is null.")
        
        # check consistent
        if self.checkConsistent(tbnames, tags, datas) == False:
            raise StatementError("check consistent failed.")
        
        # bindV
        bindv = self.createBindV(tbnames, tags, datas)
        if bindv == None:
            raise StatementError("create stmt2 bindV failed.")
        
        # call engine
        taos_stmt2_bind_param(self._stmt2, bindv, -1)


    def execute(self) -> int:
        if self._stmt2 is None:
            raise StatementError("Invalid use of execute")
        #
        self._affected_rows = taos_stmt2_exec(self._stmt2)
        return self._affected_rows

    def result(self):
        """NOTE: Don't use a stmt result more than once."""
        result = taos_stmt2_result(self._stmt2)
        return TaosResult(result, close_after=False, decode_binary=self._decode_binary)

    def close(self):
        """Close stmt."""
        if self._stmt2 is None:
            return
        taos_stmt2_close(self._stmt2)
        self._stmt2 = None

    def is_insert(self):
        return True
    
    def get_fields(self, field_type):
        fields = []
        # todo get from engine
        return fields
    
    def free_fields(self, fields):
        # todo get from engine
        pass

    def error(self):
        lasterr = "no init engine"
        # todo get from engine
        return lasterr

    def __del__(self):
        self.close()

    @property
    def affected_rows(self):
        # type: () -> int
        return self._affected_rows
