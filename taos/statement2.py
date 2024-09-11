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

    def __init__(self, stmt, decode_binary=True):
        self._stmt = stmt
        self._decode_binary = decode_binary

    # def set_tbname(self, name):
    #     """Set table name if needed.
    #
    #     Note that the set_tbname* method should only used in insert statement
    #     """
    #     if self._stmt is None:
    #         raise StatementError("Invalid use of set_tbname")
    #     taos_stmt_set_tbname(self._stmt, name)

    def prepare(self, sql):
        # type: (str) -> None
        taos_stmt2_prepare(self._stmt, sql)

    # def set_tbname_tags(self, name, tags):
    #     # type: (str, Array[TaosBind]) -> None
    #     """Set table name with tags, tags is array of BindParams"""
    #     if self._stmt is None:
    #         raise StatementError("Invalid use of set_tbname")
    #     taos_stmt_set_tbname_tags(self._stmt, name, tags)

    # def bind_param_old(self, params, add_batch=True):
    #     # type: (Array[TaosBind], bool) -> None
    #     if self._stmt is None:
    #         raise StatementError("Invalid use of stmt")
    #     taos_stmt_bind_param(self._stmt, params)
    #     if add_batch:
    #         taos_stmt_add_batch(self._stmt)

    # def bind_param_batch(self, binds, add_batch=True):
    #     # type: (Array[TaosMultiBind], bool) -> None
    #     if self._stmt is None:
    #         raise StatementError("Invalid use of stmt")
    #     taos_stmt_bind_param_batch(self._stmt, binds)
    #     if add_batch:
    #         taos_stmt_add_batch(self._stmt)

    # def add_batch(self):
    #     if self._stmt is None:
    #         raise StatementError("Invalid use of stmt")
    #     taos_stmt_add_batch(self._stmt)

    def bind_param(self, bindv, col_idx):
        # type: (Array[TaosStmt2BindV], ctypes.c_int32) -> None
        if self._stmt is None:
            raise StatementError("Invalid use of stmt")
        taos_stmt2_bind_param(self._stmt, bindv, col_idx)


    def execute(self) -> int:
        if self._stmt is None:
            raise StatementError("Invalid use of execute")
        #
        self._affected_rows = taos_stmt2_exec(self._stmt)
        return self._affected_rows

    def result(self):
        """NOTE: Don't use a stmt result more than once."""
        result = taos_stmt2_result(self._stmt)
        return TaosResult(result, close_after=False, decode_binary=self._decode_binary)

    @property
    def affected_rows(self):
        # type: () -> int
        return self._affected_rows

    def close(self):
        """Close stmt."""
        if self._stmt is None:
            return
        taos_stmt2_close(self._stmt)
        self._stmt = None

    def is_insert(self):
        return True
    
    def get_fields(self, field_type):
        fields = []
        # todo get from engine
        return fields

    def __del__(self):
        self.close()
