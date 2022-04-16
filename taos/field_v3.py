# encoding:UTF-8
import ctypes

from ctypes import *

from .constants import FieldType
from .error import *

def _crow_binary_to_python_block_v3(data, is_null, num_of_rows, offsets, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C binary row to python row"""
    assert offsets is not None
    res = []
    for i in range(abs(num_of_rows)):
        if offsets[i] == -1:
            res.append(None)
        else:
            rbyte = ctypes.cast(data + offsets[i], ctypes.POINTER(ctypes.c_short))[:1].pop()
            chars = ctypes.cast(c_char_p(data + offsets[i] + 2), ctypes.POINTER(c_char * rbyte))
            buffer = create_string_buffer(rbyte + 1)
            buffer[:rbyte] = chars[0][:rbyte]
            res.append(cast(buffer, c_char_p).value.decode("utf-8"))
    return res


def _crow_nchar_to_python_block_v3(
    data,
    is_null,
    num_of_rows,
    offsets,
    precision=FieldType.C_TIMESTAMP_UNKNOWN,
):
    """Function to convert C nchar row to python row"""
    assert offsets is not None
    res = []
    for i in range(abs(num_of_rows)):
        if offsets[i] == -1:
            res.append(None)
        else:
            rbyte = ctypes.cast(data + offsets[i], ctypes.POINTER(ctypes.c_short))[:1].pop()
            chars = ctypes.cast(c_char_p(data + offsets[i] + 2), ctypes.POINTER(c_char * rbyte))
            buffer = create_string_buffer(rbyte + 1)
            buffer[:rbyte] = chars[0][:rbyte]
            res.append(cast(buffer, c_char_p).value.decode("utf-8"))
    return res


CONVERT_FUNC_BLOCK_v3 = {
    FieldType.C_VARCHAR: _crow_binary_to_python_block_v3,
    FieldType.C_BINARY: _crow_binary_to_python_block_v3,
    FieldType.C_NCHAR: _crow_nchar_to_python_block_v3,
    FieldType.C_JSON: _crow_nchar_to_python_block_v3,
}

# Corresponding TAOS_FIELD structure in C


class TaosField(ctypes.Structure):
    _fields_ = [
        ("_name", ctypes.c_char * 65),
        ("_type", ctypes.c_uint8),
        ("_bytes", ctypes.c_uint32),
    ]

    @property
    def name(self):
        return self._name.decode("utf-8")

    @property
    def length(self):
        """alias to self.bytes"""
        return self._bytes

    @property
    def bytes(self):
        return self._bytes

    @property
    def type(self):
        return self._type

    def __dict__(self):
        return {"name": self.name, "type": self.type, "bytes": self.length}

    def __str__(self):
        return "{name: %s, type: %d, bytes: %d}" % (self.name, self.type, self.length)

    def __getitem__(self, item):
        return getattr(self, item)


class TaosFields(object):
    def __init__(self, fields, count):
        if isinstance(fields, c_void_p):
            self._fields = cast(fields, POINTER(TaosField))
        if isinstance(fields, POINTER(TaosField)):
            self._fields = fields
        self._count = count
        self._iter = 0

    def as_ptr(self):
        return self._fields

    @property
    def count(self):
        return self._count

    @property
    def fields(self):
        return self._fields

    def __next__(self):
        return self._next_field()

    def next(self):
        return self._next_field()

    def _next_field(self):
        if self._iter < self.count:
            field = self._fields[self._iter]
            self._iter += 1
            return field
        else:
            raise StopIteration

    def __getitem__(self, item):
        return self._fields[item]

    def __iter__(self):
        return self

    def __len__(self):
        return self.count
