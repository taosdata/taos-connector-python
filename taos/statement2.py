from taos.cinterface import *
from taos.error import StatementError
from taos.result import TaosResult
from taos import bind2
from typing import Optional



_taos_async_fn_t = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int)

#
# bind data with table
#
class BindTable(object):
    def __init__(self, name, tags):
        self.name  = name
        self.tags  = tags
        self.datas = []

    # add column data
    def add_col_data(self, data):
        self.datas.append(data)


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

#
#    ------------- global fun define -------------
#

def checkConsistent(tbnames, tags, datas):
    # todo
    return True

def obtainSchema(statement2):
    if statement2._stmt2 is None:
        raise StatementError("stmt2 object is null.")
    
    # test
    statement2.tag_fields = [FieldType.C_BINARY, FieldType.C_INT]
    statement2.fields     = [FieldType.C_TIMESTAMP, FieldType.C_BINARY, FieldType.C_BOOL, FieldType.C_INT]

    #stmt2.fields     = stmt2.get_fields(TAOS_FIELD_COL)
    #stmt2.tag_fields = stmt2.get_fields(TAOS_FIELD_TAG)

def getFieldType(statement2, index, isTag):
    if isTag:
        return statement2.tag_fields[index]
    
    # col
    if statement2.types is not None:
        # user set column types
        return statement2.types[index]
    else:
        return statement2.fields[index]

# create stmt2Bind list from tags
def createTagsBind(statement2, tagsTbs):
    binds = []
    # tables tags
    for tagsTb in tagsTbs:
        # table
        print(f"tagsTb={tagsTb}")

        n = len(tagsTb)
        bindsTb = bind2.new_stmt2_binds(n)
        for i in range(n):
            type = getFieldType(statement2, i, True)
            values = [tagsTb[i]]
            print(f"tagasTb i={i} {values}")
            bindsTb[i].set_value(type, values)
        binds.append(bindsTb)

    return binds    

# create stmt2Bind list from columns
def createColsBind(statement2, colsTbs):
    binds = []
    # tables columns data
    for colsTb in colsTbs:
        # table
        n = len(colsTb)
        bindsTb = bind2.new_stmt2_binds(n)
        for i in range(n):
            type = getFieldType(statement2, i, isTag=False)
            bindsTb[i].set_value(type, colsTb[i])
        binds.append(bindsTb)

    return binds    


#
# create bindv from list
#
def createBindV(statement2, tbnames, tags, datas):
    # count
    count = len(tbnames)

    # tags
    bindTags = createTagsBind(statement2, tags)

    # datas
    bindDatas = createColsBind(statement2, datas)

    # create
    return bind2.new_bindv(count, tbnames, bindTags, bindDatas)




#
# -------------- stmt2 object --------------
#

class TaosStmt2(object):
    """TDengine STMT2 interface"""

    def __init__(self, stmt2, decode_binary=True):
        self._stmt2 = stmt2
        self._decode_binary = decode_binary
        self.fields     = None
        self.tag_fields = None
        self.types      = None

    def prepare(self, sql):
        if self._stmt2 is None:
            raise StatementError(ErrMsg.STMT2_NULL)
        
        taos_stmt2_prepare(self._stmt2, sql)


    def bind_param(self, tbnames, tags, datas):
        if self._stmt2 is None:
            raise StatementError(ErrMsg.STMT2_NULL)
        
        # obtain schema
        obtainSchema(self)
        
        # check consistent
        if checkConsistent(tbnames, tags, datas) == False:
            raise StatementError("check consistent failed.")
        
        # bindV
        bindv = createBindV(self, tbnames, tags, datas)
        if bindv == None:
            raise StatementError("create stmt2 bindV failed.")
        
        # call engine
        taos_stmt2_bind_param(self._stmt2, bindv.get_address(), -1)

    # with table bind
    def bind_param_with_tables(self, tables):

        tbnames = []
        tagsTbs = []
        datasTbs = []

        for table in tables:
            tbnames.append(table.name)
            tagsTbs.append(table.tags)
            datasTbs.append(table.datas)

        # call bind
        self.bind_param(tbnames, tagsTbs, datasTbs)


    def execute(self) -> int:
        if self._stmt2 is None:
            raise StatementError("stmt2 object is null.")
        #
        self._affected_rows = taos_stmt2_exec(self._stmt2)
        return self._affected_rows

    def result(self):
        if self._stmt2 is None:
            raise StatementError("stmt2 object is null.")

        result = taos_stmt2_result(self._stmt2)
        return TaosResult(result, close_after=False, decode_binary=self._decode_binary)

    def close(self):
        if self._stmt2 is None:
            raise StatementError("stmt2 object is null.")
        
        taos_stmt2_close(self._stmt2)
        self._stmt2 = None

    def is_insert(self):
        if self._stmt2 is None:
            raise StatementError("stmt2 object is null.")

        return taos_stmt2_is_insert(self._stmt2)
    
    def get_fields(self, field_type):
        if self._stmt2 is None:
            raise StatementError("stmt2 object is null.")

        return taos_stmt2_get_fields(self._stmt2, field_type)
    

    def error(self):
        if self._stmt2 is None:
            raise StatementError("stmt2 object is null.")
        
        return taos_stmt2_error(self._stmt2)
    
    # not engine interface
    def set_columns_type(self, types):
        self.types = types

    def __del__(self):
        self.close()

    @property
    def affected_rows(self):
        # type: () -> int
        return self._affected_rows
