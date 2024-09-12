from taos.cinterface import *
from taos.error import StatementError
from taos.result import TaosResult
import bind2



_taos_async_fn_t = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int)

class TaosStmt2OptionImpl(ctypes.Structure):
    _fields_ = [
        ("reqid", ctypes.c_int64),
        ("singleStbInsert", ctypes.c_bool),
        ("singleTableBindOnce", ctypes.c_bool),
        ("asyncExecFn", _taos_async_fn_t),
        ("userdata", ctypes.c_void_p)
    ]


class TaosStmt2Option:
    def __init__(self, reqid: int, single_stb_insert: bool=False, single_table_bind_once: bool=False):
        self._impl = TaosStmt2OptionImpl()
        self.reqid = reqid
        self.single_stb_insert = single_stb_insert
        self.single_table_bind_once = single_table_bind_once

    @property
    def reqid(self) -> int:
        return self._impl.reqid

    @reqid.setter
    def reqid(self, value: int):
        self._impl.reqid = ctypes.c_int64(value)

    @property
    def single_stb_insert(self) -> bool:
        return self._impl.singleStbInsert

    @single_stb_insert.setter
    def single_stb_insert(self, value: bool):
        self._impl.singleStbInsert = ctypes.c_bool(value)

    @property
    def single_table_bind_once(self) -> bool:
        return self._impl.singleTableBindOnce

    @single_table_bind_once.setter
    def single_table_bind_once(self, value: bool):
        self._impl.singleTableBindOnce = ctypes.c_bool(value)

    @property
    def async_exec_fn(self) -> _taos_async_fn_t:
        return self._impl.asyncExecFn

    @async_exec_fn.setter
    def async_exec_fn(self, value: _taos_async_fn_t):
        self._impl.asyncExecFn = value

    @property
    def userdata(self) -> ctypes.c_void_p:
        return self._impl.userdata

    @userdata.setter
    def userdata(self, value: ctypes.c_void_p):
        self._impl.userdata = value

    def get_impl(self):
        return self._impl


class TaosStmt2(object):
    """TDengine STMT2 interface"""

    def __init__(self, stmt2, decode_binary=True):
        self._stmt2 = stmt2
        self._decode_binary = decode_binary
        self.fields     = None
        self.tag_fields = None
        self.types      = None
        

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
    

    def obtainSchema(self, count):
        if self._stmt2 is None:
            raise StatementError("stmt2 object is null.")
        
        self.fields     = self.get_fields(TAOS_FIELD_COL)
        self.tag_fields = self.get_fields(TAOS_FIELD_TAG)


    def getFieldType(self, index, isTag):
        if isTag:
            return self.tag_fields[index]
        
        # col
        if self.types is not None:
            # user set column types
            return self.types[index]
        else:
            return self.fields[index]

    # create stmt2Bind list from tags
    def createTagsBind(self, tagsTbs):
        binds = []
        # tables tags
        for tagsTb in tagsTbs:
            # table
            n = len(tagsTb)
            bindsTb = new_stmt2_binds(n)
            for i in range(n):
                type = self.getFieldType(i, True)
                bindsTb[i].set_value(type, [tagsTb[i]])
            binds.append(bindsTb)

        return binds    

    # create stmt2Bind list from columns
    def createColsBind(self, colsTbs):
        binds = []
        # tables columns data
        for colsTb in colsTbs:
            # table
            n = len(colsTb)
            bindsTb = new_stmt2_binds(n)
            for i in range(n):
                type = self.getFieldType(i, isTag=False)
                bindsTb[i].set_value(type, colsTb[i])
            binds.append(bindsTb)

        return binds    

    
    #
    # create bindv from list
    #
    def createBindV(self, tbnames, tags, datas):
        # count
        count = len(tbnames)

        # obtain schema
        
         

        # tags
        bindTags = self.createTagsBind(tags)

        # datas
        bindDatas = self.createColsBind(datas)

        # create
        return new_bindv(count, tbnames, bindTags, bindDatas)



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
    
    def set_columns_type(self, types):
        self.types = types

    def __del__(self):
        self.close()

    @property
    def affected_rows(self):
        # type: () -> int
        return self._affected_rows
