# encoding:UTF-8
from taos.cinterface import *
from taos.error import StatementError
from taos.result import TaosResult
from taos import bind2
from taos import log
from taos import utils
from taos.precision import PrecisionEnum, PrecisionError
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


def taos_stmt2_async_exec(userdata, result_set, error_code):
    print(f"Executing asynchronously with userdata: {userdata}, result_set: {result_set}, error_code: {error_code}")


class TaosStmt2Option:
    def __init__(self, reqid: int=None, single_stb_insert: bool=False, single_table_bind_once: bool=False, **kwargs):
        self._impl = TaosStmt2OptionImpl()
        if reqid is None:
            reqid = utils.gen_req_id()
        else:
            if type(reqid) is not int:
                raise StatementError(f"reqid type error, expected int type but got {type(reqid)}.")

        self.reqid = reqid
        self.single_stb_insert = single_stb_insert
        self.single_table_bind_once = single_table_bind_once
        self.is_async = kwargs.get('is_async', False)
        self.async_exec_fn = taos_stmt2_async_exec if self.is_async else _taos_async_fn_t()
        self.userdata = ctypes.c_void_p(None)

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
        if value is not None and not isinstance(value, _taos_async_fn_t):
            raise TypeError("async_exec_fn must be an instance of _taos_async_fn_t or None")
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

    try:
        count, fields = statement2.get_fields()
        statement2.tag_fields = []
        statement2.fields     = []
        for field in fields:
            if field.field_type == TAOS_FIELD_TAG:
                statement2.tag_fields.append(field)
            elif field.field_type == TAOS_FIELD_COL:
                statement2.fields.append(field)
        log.debug(f"obtain schema tag fields = {statement2.tag_fields}")
        log.debug(f"obtain schema fields     = {statement2.fields}")
    except Exception as err:
        log.debug(f"obtain schema tag/col fields failed, reason: {repr(err)}")
        return False

    return len(statement2.fields) > 0


def getField(statement2, index, isTag):
    if isTag:
        return statement2.tag_fields[index]
    else:
        return statement2.fields[index]

# create stmt2Bind list from tags
def createTagsBind(statement2, tagsTbs):
    binds = []
    # tables tags
    for tagsTb in tagsTbs:
        # table
        n = len(tagsTb)
        bindsTb = bind2.new_stmt2_binds(n)
        for i in range(n):
            field = getField(statement2, i, True)
            values = [tagsTb[i]]
            log.debug(f"tag i = {i} type={field.type} precision={field.precision} length={field.bytes}  values = {values}  tagsTb = {tagsTb}\n")
            bindsTb[i].set_value(field.type, values, field.precision)
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
            field = getField(statement2, i, isTag=False)
            bindsTb[i].set_value(field.type, colsTb[i], field.precision)
        binds.append(bindsTb)

    return binds    


#
# create bindv from list
#
def createBindV(statement2, tbnames, tags, datas):

    if tbnames == None and tags == None and datas == None:
        raise StatementError("all bind params is None.")

    # count
    count  = -1
    ret = utils.detectListNone(tbnames)
    if ret   == utils.ALL_NONE:
        bindNames = None
    elif ret == utils.HAVE_NONE:
        raise StatementError("params tbnames some is None, some is not None, this is error.")
    else:
        # not found none
        if type(tbnames) not in [list, tuple]:
            raise StatementError(f"tbnames type error, expected list or tuple type but got {type(tbnames)}.")

        bindNames = tbnames 
        count = len(tbnames)

    # tags
    ret = utils.detectListNone(tags)
    if ret   == utils.ALL_NONE:
        bindTags = None
    elif ret == utils.HAVE_NONE:
        raise StatementError("params tags some is None, some is not None, this is error.")
    else:
        # not found none
        bindTags = createTagsBind(statement2, tags)
        if count == -1:
            count = len(tags)
        else:
            if count != len(tags):
                err = f"tags count is inconsistent. require count={count} real={len(tags)}"
                raise StatementError(err)

    # datas
    ret = utils.detectListNone(datas)
    if ret   == utils.ALL_NONE:
        bindDatas = None
    elif ret == utils.HAVE_NONE:
        raise StatementError("params datas some is None, some is not None, this is error.")
    else:
        # not found none
        bindDatas = createColsBind(statement2, datas)
        if count == -1:
            count = len(datas)
        else:
            if count != len(datas):
                err = f"datas count is inconsistent. require count={count} real={len(datas)}"
                raise StatementError(err)

    # create
    return bind2.new_bindv(count, bindNames, bindTags, bindDatas)




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
        self._is_insert = None

    def prepare(self, sql):
        if self._stmt2 is None:
            raise StatementError(ErrMsg.STMT2_NULL)

        if sql is None:
            raise StatementError("sql is null.")

        if not isinstance(sql, str):
            raise StatementError("sql is not str type.")

        if len(sql) == 0:
            raise StatementError("sql is empty.")

        self.fields     = None
        self.tag_fields = None
        self._is_insert = None

        taos_stmt2_prepare(self._stmt2, sql)

        # obtain schema if insert
        if self.is_insert():
            if obtainSchema(self) is False:
                raise StatementError(f"obtain schema failed, maybe this sql is not supported, sql: {sql}")

    def bind_param(self, tbnames, tags, datas):
        if self._stmt2 is None:
            raise StatementError(ErrMsg.STMT2_NULL)
        
        log.debug(f"bind_param tbnames  = {tbnames} \n")
        log.debug(f"bind_param tags     = {tags}    \n")
        log.debug(f"bind_param datasTbs = {datas}   \n")

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
            return 
        
        taos_stmt2_close(self._stmt2)
        self._stmt2 = None

    def is_insert(self):
        if self._stmt2 is None:
            raise StatementError("stmt2 object is null.")

        if self._is_insert is None:
            self._is_insert = taos_stmt2_is_insert(self._stmt2)

        return self._is_insert

    def get_fields(self):
        if self._stmt2 is None:
            raise StatementError("stmt2 object is null.")

        return taos_stmt2_get_fields(self._stmt2)

    def error(self):
        if self._stmt2 is None:
            raise StatementError("stmt2 object is null.")
        
        return taos_stmt2_error(self._stmt2)
    
    #
    # if query must set columns type (not engine interface),
    #  type is define class FieldType in constants.py
    #
    def set_columns_type(self, types):
        self.fields = []
        for type in types:
            item = TaosFieldAllCls(None, type, PrecisionEnum.Milliseconds, None, None, TAOS_FIELD_COL)
            self.fields.append(item)

    def __del__(self):
        self.close()

    @property
    def affected_rows(self):
        # type: () -> int
        return self._affected_rows
