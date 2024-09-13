# encoding:UTF-8
import sys
import ctypes
from ctypes import *
from datetime import datetime
from typing import List, Optional

from taos.cinterface import IS_V3
from taos.constants import FieldType
from taos.precision import PrecisionEnum, PrecisionError


_datetime_epoch = datetime.utcfromtimestamp(0)


def _datetime_to_timestamp(value, precision):
    # type: (datetime | float | int | str | c_int64, PrecisionEnum) -> c_int64
    if value is None:
        return FieldType.C_BIGINT_NULL
    if type(value) is datetime:
        if precision == PrecisionEnum.Milliseconds:
            return int(round((value - _datetime_epoch).total_seconds() * 1000))
        elif precision == PrecisionEnum.Microseconds:
            return int(round((value - _datetime_epoch).total_seconds() * 10000000))
        else:
            raise PrecisionError("datetime do not support nanosecond precision")
    elif type(value) is float:
        if precision == PrecisionEnum.Milliseconds:
            return int(round(value * 1000))
        elif precision == PrecisionEnum.Microseconds:
            return int(round(value * 10000000))
        else:
            raise PrecisionError("time float do not support nanosecond precision")
    elif isinstance(value, int) and not isinstance(value, bool):
        return c_int64(value)
    elif isinstance(value, str):
        value = datetime.fromisoformat(value)
        if precision == PrecisionEnum.Milliseconds:
            return int(round(value * 1000))
        elif precision == PrecisionEnum.Microseconds:
            return int(round(value * 10000000))
        else:
            raise PrecisionError("datetime do not support nanosecond precision")
    elif isinstance(value, c_int64):
        return value
    return FieldType.C_BIGINT_NULL


class TaosStmt2Bind(ctypes.Structure):
    _fields_ = [
        ("buffer_type", ctypes.c_int),
        ("buffer", ctypes.c_void_p),
        ("length", ctypes.POINTER(ctypes.c_int32)),
        ("is_null", ctypes.c_char_p),
        ("num", ctypes.c_int)
    ]


    #
    # set bind value with field type
    #
    def set_value(self, buffer_type, values):
        if buffer_type == FieldType.C_BOOL:
            self.bool(values)
        elif buffer_type == FieldType.C_TINYINT:
            self.tinyint(values)
        elif buffer_type == FieldType.C_SMALLINT:
            self.smallint(values)
        elif buffer_type == FieldType.C_INT:
            self.int(values)
        elif buffer_type == FieldType.C_BIGINT:
            self.bigint(values)
        elif buffer_type == FieldType.C_FLOAT:
            self.float(values)
        elif buffer_type == FieldType.C_DOUBLE:
            self.double(values)
        elif buffer_type == FieldType.C_VARCHAR:
            self.varchar(values)
        elif buffer_type == FieldType.C_BINARY:
            self.binary(values)
        elif buffer_type == FieldType.C_TIMESTAMP:
            self.timestamp(values)
        elif buffer_type == FieldType.C_NCHAR:
            self.nchar(values)
        elif buffer_type == FieldType.C_TINYINT_UNSIGNED:
            self.tinyint_unsigned(values)
        elif buffer_type == FieldType.C_SMALLINT_UNSIGNED:
            self.smallint_unsigned(values)
        elif buffer_type == FieldType.C_INT_UNSIGNED:
            self.int_unsigned(values)
        elif buffer_type == FieldType.C_BIGINT_UNSIGNED:
            self.bigint_unsigned(values)
        elif buffer_type == FieldType.C_JSON:
            self.json(values)
        elif buffer_type == FieldType.C_VARBINARY:
            self.varbinary(values)
        elif buffer_type == FieldType.C_GEOMETRY:
            self.geometry(values)

    def bool(self, values):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_int8 * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_BOOL_NULL for v in values])

        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)
        self.buffer_type = FieldType.C_BOOL
        # self.buffer_length = sizeof(c_bool)
        self.is_null = cast((c_char * len(values))(*[1 if value is None else 0 for value in values]), c_char_p)

    def tinyint(self, values):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        self.buffer_type = FieldType.C_TINYINT
        # self.buffer_length = sizeof(c_int8)
        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_int8 * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_TINYINT_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)
        self.is_null = cast((c_char * len(values))(*[1 if value is None else 0 for value in values]), c_char_p)

    def smallint(self, values):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        self.buffer_type = FieldType.C_SMALLINT
        # self.buffer_length = sizeof(c_int16)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_int16 * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_SMALLINT_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)
        self.is_null = cast((c_char * len(values))(*[1 if value is None else 0 for value in values]), c_char_p)

    def int(self, values):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        self.buffer_type = FieldType.C_INT
        # self.buffer_length = sizeof(c_int32)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_int32 * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_INT_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)
        self.is_null = cast((c_char * len(values))(*[1 if value is None else 0 for value in values]), c_char_p)

    def bigint(self, values):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        self.buffer_type = FieldType.C_BIGINT
        # self.buffer_length = sizeof(c_int64)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_int64 * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_BIGINT_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)
        self.is_null = cast((c_char * len(values))(*[1 if value is None else 0 for value in values]), c_char_p)

    def float(self, values):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        self.buffer_type = FieldType.C_FLOAT
        self.buffer_length = sizeof(c_float)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_float * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_FLOAT_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)
        self.is_null = cast((c_char * len(values))(*[1 if value is None else 0 for value in values]), c_char_p)

    def double(self, values):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        self.buffer_type = FieldType.C_DOUBLE
        self.buffer_length = sizeof(c_double)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_double * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_DOUBLE_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)
        self.is_null = cast((c_char * len(values))(*[1 if value is None else 0 for value in values]), c_char_p)

    def timestamp(self, values, precision=PrecisionEnum.Milliseconds):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_int64 * len(values)
            buffer = buffer_type(*[_datetime_to_timestamp(value, precision) for value in values])

        self.buffer_type = FieldType.C_TIMESTAMP
        self.buffer = cast(buffer, c_void_p)
        # self.buffer_length = sizeof(c_int64)
        self.num = len(values)
        self.is_null = cast((c_char * len(values))(*[1 if value is None else 0 for value in values]), c_char_p)

    def _str_to_buffer(self, values, encode=True):
        self.num = len(values)
        is_null = [1 if v is None else 0 for v in values]
        self.is_null = cast((c_byte * self.num)(*is_null), c_char_p)

        if sum(is_null) == self.num:
            self.length = (c_int32 * len(values))(0 * self.num)
            return
        if sys.version_info < (3, 0):
            _bytes = [bytes(value) if value is not None else None for value in values]
            buffer_length = max(len(b) + 1 for b in _bytes if b is not None)
            buffers = [
                create_string_buffer(b, buffer_length) if b is not None else create_string_buffer(buffer_length)
                for b in _bytes
            ]
            buffer_all = b"".join(v[:] for v in buffers)
            self.buffer = cast(c_char_p(buffer_all), c_void_p)
        else:
            _bytes = []
            if encode:
                _bytes = [value.encode("utf-8") if value is not None else None for value in values]
            else:
                _bytes = [bytes(value) if value is not None else None for value in values]

            buffer_length = max(len(b) for b in _bytes if b is not None)
            print(f"_bytes={_bytes} buffer_length={buffer_length}\n")
            self.buffer = cast(
                c_char_p(
                    b"".join(
                        [
                            create_string_buffer(b, buffer_length)
                            if b is not None
                            else create_string_buffer(buffer_length)
                            for b in _bytes
                        ]
                    )
                ),
                c_void_p,
            )
        self.length = (c_int32 * len(values))(*[len(b) if b is not None else 0 for b in _bytes])
        self.buffer_length = buffer_length

        '''
        self.num = len(values)
        is_null = [1 if value is None else 0 for value in values]
        self.is_null = cast((c_byte * self.num)(*is_null), c_char_p)

        if sum(is_null) == self.num:
            self.length = (c_int32 * len(values))(0 * self.num)
            return

        _bytes_list = []
        if encode:
            _bytes_list = [value.encode("utf-8") if value is not None else None for value in values]
        else:
            _bytes_list = [bytes(value) if value is not None else None for value in values]

        # buffer_length = max(len(b) for b in _bytes_list if b is not None)
        print(f"_bytes_list={_bytes_list}")
        _bytes = b"".join([b for b in _bytes_list if b is not None])
        print(f"_bytes={_bytes}")
        self.buffer = cast(create_string_buffer(_bytes), c_void_p)
        print(self.buffer)
        self.length = (c_int32 * len(values))(*[len(b) if b is not None else 0 for b in _bytes])
        # self.buffer_length = buffer_length
        '''

    def binary(self, values):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        self.buffer_type = FieldType.C_BINARY
        self._str_to_buffer(values)

    def nchar(self, values):
        # type: (list[str]) -> None
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        self.buffer_type = FieldType.C_NCHAR
        self._str_to_buffer(values)

    def json(self, values):
        # type: (list[str]) -> None
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        self.buffer_type = FieldType.C_JSON
        self._str_to_buffer(values)

    def tinyint_unsigned(self, values):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        self.buffer_type = FieldType.C_TINYINT_UNSIGNED
        # self.buffer_length = sizeof(c_uint8)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_uint8 * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_TINYINT_UNSIGNED_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)
        self.is_null = cast((c_char * len(values))(*[1 if value is None else 0 for value in values]), c_char_p)

    def smallint_unsigned(self, values):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        self.buffer_type = FieldType.C_SMALLINT_UNSIGNED
        # self.buffer_length = sizeof(c_uint16)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_uint16 * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_SMALLINT_UNSIGNED_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)
        self.is_null = cast((c_char * len(values))(*[1 if value is None else 0 for value in values]), c_char_p)

    def int_unsigned(self, values):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        self.buffer_type = FieldType.C_INT_UNSIGNED
        # self.buffer_length = sizeof(c_uint32)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_uint32 * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_INT_UNSIGNED_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)
        self.is_null = cast((c_char * len(values))(*[1 if value is None else 0 for value in values]), c_char_p)

    def bigint_unsigned(self, values):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        self.buffer_type = FieldType.C_BIGINT_UNSIGNED
        # self.buffer_length = sizeof(c_uint64)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_uint64 * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_BIGINT_UNSIGNED_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)
        self.is_null = cast((c_char * len(values))(*[1 if value is None else 0 for value in values]), c_char_p)

    def varchar(self, values):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        self.buffer_type = FieldType.C_VARCHAR
        self._str_to_buffer(values)

    def varbinary(self, values):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        self.buffer_type = FieldType.C_VARBINARY
        self._str_to_buffer(values, False)

    def geometry(self, values):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        self.buffer_type = FieldType.C_GEOMETRY
        self._str_to_buffer(values, False)


class TaosStmt2BindV(ctypes.Structure):
    _fields_ = [
        ("count", ctypes.c_int),
        ("tbnames", ctypes.POINTER(ctypes.c_char_p)),
        ("tagsTbs", ctypes.POINTER(ctypes.POINTER(TaosStmt2Bind))),
        ("colsTbs", ctypes.POINTER(ctypes.POINTER(TaosStmt2Bind)))
    ]

    def init(
            self,
            count: int,
            tbnames,
            tagsTbs,
            colsTbs
    ):
        self.count = count
        if tbnames is not None:
            self.tbnames = (ctypes.c_char_p * count)()
            for i, tbname in enumerate(tbnames):
                self.tbnames[i] = self.str_to_buffer(tbname)
        else:
            self.tbnames = None

        if tagsTbs is not None:
            self.tagsTbs = (ctypes.POINTER(TaosStmt2Bind) * len(tagsTbs))()
            for i, tagsTb in enumerate(tagsTbs):
                self.tagsTbs[i] = tagsTb
        else:
            self.tagsTbs = None
        
        if colsTbs is not None:
            self.colsTbs = (ctypes.POINTER(TaosStmt2Bind) * count)()
            for i, colsTb in enumerate(colsTbs):
                self.colsTbs[i] = colsTb
        else:
            self.colsTbs = None
        

    def str_to_buffer(self, value: str, encode=True):
        buffer = None
        if value is not None:
            _bytes = None
            if encode:
                _bytes = value.encode("utf-8")
            else:
                _bytes = bytes(value)

            buffer = cast(create_string_buffer(_bytes), c_char_p)
        else:
            buffer = c_void_p(None)

        return buffer


def new_stmt2_binds(size: int):
    # type: (int) -> Array[TaosStmt2Bind]
    return (TaosStmt2Bind * size)()


def new_bindv(
        count: int,
        tbnames, 
        tags,
        bind_cols):
    bindv = (TaosStmt2BindV * 1)()
    bindv[0].init(count, tbnames, tags, bind_cols)
    return bindv