# encoding:UTF-8
import ctypes

from .constants import FieldType


def _crow_binary_to_python_block_v3(data, is_null, num_of_rows, offsets, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C binary row to python row."""
    assert offsets is not None
    res = []
    for i in range(abs(num_of_rows)):
        if offsets[i] == -1:
            res.append(None)
        else:
            rbyte = ctypes.cast(data + offsets[i], ctypes.POINTER(ctypes.c_uint16))[:1].pop()
            chars = ctypes.cast(ctypes.c_char_p(data + offsets[i] + 2), ctypes.POINTER(ctypes.c_char * rbyte))
            buffer = ctypes.create_string_buffer(rbyte + 1)
            buffer[:rbyte] = chars[0][:rbyte]
            res.append(ctypes.cast(buffer, ctypes.c_char_p).value.decode("utf-8"))
    return res


def _crow_nchar_to_python_block_v3(data, is_null, num_of_rows, offsets, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C nchar row to python row."""
    assert offsets is not None
    res = []
    for i in range(abs(num_of_rows)):
        if offsets[i] == -1:
            res.append(None)
        else:
            rbyte = ctypes.cast(data + offsets[i], ctypes.POINTER(ctypes.c_uint16))[:1].pop()
            chars = ctypes.cast(ctypes.c_char_p(data + offsets[i] + 2), ctypes.POINTER(ctypes.c_char * rbyte))
            buffer = ctypes.create_string_buffer(rbyte + 1)
            buffer[:rbyte] = chars[0][:rbyte]
            res.append(ctypes.cast(buffer, ctypes.c_char_p).value.decode("utf-8"))
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
        """Alias to self.bytes."""
        return self._bytes

    @property
    def bytes(self):
        return self._bytes

    @property
    def type(self):
        return self._type

    def __dict__(self):
        """Construct dict."""
        return {"name": self.name, "type": self.type, "bytes": self.length}

    def __str__(self):
        """Construct str."""
        return "{name: %s, type: %d, bytes: %d}" % (self.name, self.type, self.length)

    def __getitem__(self, item):
        """Get attr."""
        return getattr(self, item)


class TaosFields(object):
    def __init__(self, fields, count):
        """Init class."""
        if isinstance(fields, ctypes.c_void_p):
            self._fields = ctypes.cast(fields, ctypes.POINTER(TaosField))
        if isinstance(fields, ctypes.POINTER(TaosField)):
            self._fields = fields
        self._count = count
        self._iter = 0

    def as_ptr(self):
        """Return as ptr."""
        return self._fields

    @property
    def count(self):
        """Return count."""
        return self._count

    @property
    def fields(self):
        """Return fields."""
        return self._fields

    def __next__(self):
        """Next field."""
        return self._next_field()

    def next(self):
        """Next field."""
        return self._next_field()

    def _next_field(self):
        """Iter next_field."""
        if self._iter < self.count:
            field = self._fields[self._iter]
            self._iter += 1
        else:
            raise StopIteration
        return field

    def __getitem__(self, item):
        """Return field item."""
        return self._fields[item]

    def __iter__(self):
        """To iter."""
        self._iter = 0
        return self

    def __len__(self):
        """Get len."""
        return self.count

    def __str__(self):
        """Print"""
        return ",".join(str(f) for f in self)
