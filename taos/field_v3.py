# encoding:UTF-8
import ctypes

from taos.constants import FieldType

_RTYPE = ctypes.c_uint16
_RTYPE_SIZE = ctypes.sizeof(_RTYPE)

_RTYPE_32 = ctypes.c_uint32
_RTYPE_SIZE_32 = ctypes.sizeof(_RTYPE_32)


def _crow_var_data_to_python_block_v3(data, num_of_rows, offsets, decode_binary, block_version=1):
    assert offsets is not None
    rtype = _RTYPE
    rtype_size = _RTYPE_SIZE
    if block_version >= 2:
        rtype = _RTYPE_32
        rtype_size = _RTYPE_SIZE_32
    res = []
    for i in range(abs(num_of_rows)):
        if offsets[i] == -1:
            res.append(None)
        else:
            rbyte = rtype.from_address(data + offsets[i]).value
            chars = (ctypes.c_char * rbyte).from_address(data + offsets[i] + rtype_size).raw
            if decode_binary:
                chars = chars.decode("utf-8")
            res.append(chars)
    return res


def _crow_var_data_to_python_block_v3_decode(data, num_of_rows, offsets, block_version=1):
    return _crow_var_data_to_python_block_v3(data, num_of_rows, offsets, True, block_version)


def _crow_var_data_to_python_block_v3_no_decode(data, num_of_rows, offsets, block_version=1):
    return _crow_var_data_to_python_block_v3(data, num_of_rows, offsets, False, block_version)


def convert_block_func_v3(field_type: FieldType, decode_binary=True):
    """Get convert block func."""
    if (field_type == FieldType.C_VARCHAR or field_type == FieldType.C_BINARY) and not decode_binary:
        return _crow_var_data_to_python_block_v3_no_decode
    return CONVERT_FUNC_BLOCK_v3[field_type]


CONVERT_FUNC_BLOCK_v3 = {
    FieldType.C_VARCHAR: _crow_var_data_to_python_block_v3_decode,
    FieldType.C_BINARY: _crow_var_data_to_python_block_v3_decode,
    FieldType.C_NCHAR: _crow_var_data_to_python_block_v3_decode,
    FieldType.C_JSON: _crow_var_data_to_python_block_v3_decode,
    FieldType.C_VARBINARY: _crow_var_data_to_python_block_v3_no_decode,
    FieldType.C_GEOMETRY: _crow_var_data_to_python_block_v3_no_decode,
}


def is_var_data_type(field_type: FieldType):
    """Check if field type is var data type."""
    return field_type in [FieldType.C_VARCHAR, FieldType.C_BINARY, FieldType.C_JSON, FieldType.C_NCHAR,
                          FieldType.C_VARBINARY, FieldType.C_GEOMETRY]


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
