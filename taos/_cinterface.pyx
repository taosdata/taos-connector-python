# cython: profile=True

from libc.stdlib cimport malloc, free
import cython
import datetime as dt
import pytz
from collections import namedtuple
from taos.error import ProgrammingError, OperationalError, ConnectionError, DatabaseError, StatementError, InternalError
from taos._parser cimport (_parse_bool, _parse_int8_t, _parse_int16_t, _parse_int, _parse_int64_t, _parse_float, _parse_double, _parse_timestamp, 
                            _parse_uint8_t, _parse_uint16_t, _parse_uint, _parse_uint64_t, _parse_string, _parse_bytes, _parse_datetime)

cdef bool *taos_get_column_data_is_null(TAOS_RES *res, int field, int rows):
    is_null = <bool*>malloc(rows * sizeof(bool))
    cdef int r
    for r in range(rows):
        is_null[r] = taos_is_null(res, r, field)

    return is_null

# --------------------------------------------- parser ---------------------------------------------------------------v

# --------------------------------------------- parser ---------------------------------------------------------------^
SIZED_TYPE = {
    TSDB_DATA_TYPE_BOOL,
    TSDB_DATA_TYPE_TINYINT,
    TSDB_DATA_TYPE_SMALLINT,
    TSDB_DATA_TYPE_INT,
    TSDB_DATA_TYPE_BIGINT,
    TSDB_DATA_TYPE_FLOAT,
    TSDB_DATA_TYPE_DOUBLE,
    TSDB_DATA_TYPE_UTINYINT,
    TSDB_DATA_TYPE_USMALLINT,
    TSDB_DATA_TYPE_UINT,
    TSDB_DATA_TYPE_UBIGINT,
    TSDB_DATA_TYPE_TIMESTAMP,
}

UNSIZED_TYPE = {
    TSDB_DATA_TYPE_VARCHAR,
    TSDB_DATA_TYPE_NCHAR,
    TSDB_DATA_TYPE_JSON,
    TSDB_DATA_TYPE_VARBINARY,
    TSDB_DATA_TYPE_BINARY,
}

CONVERT_FUNC = {
    TSDB_DATA_TYPE_BOOL: _parse_bool,
    TSDB_DATA_TYPE_TINYINT: _parse_int8_t,
    TSDB_DATA_TYPE_SMALLINT: _parse_int16_t,
    TSDB_DATA_TYPE_INT: _parse_int,
    TSDB_DATA_TYPE_BIGINT: _parse_int64_t,
    TSDB_DATA_TYPE_FLOAT: _parse_float,
    TSDB_DATA_TYPE_DOUBLE: _parse_double,
    TSDB_DATA_TYPE_VARCHAR: _parse_string,
    TSDB_DATA_TYPE_TIMESTAMP: _parse_timestamp,
    TSDB_DATA_TYPE_NCHAR: _parse_string,
    TSDB_DATA_TYPE_UTINYINT: _parse_uint8_t,
    TSDB_DATA_TYPE_USMALLINT: _parse_uint16_t,
    TSDB_DATA_TYPE_UINT: _parse_uint,
    TSDB_DATA_TYPE_UBIGINT: _parse_uint64_t,
    TSDB_DATA_TYPE_JSON: _parse_string,
    TSDB_DATA_TYPE_VARBINARY: _parse_bytes,
    TSDB_DATA_TYPE_DECIMAL: None,
    TSDB_DATA_TYPE_BLOB: None,
    TSDB_DATA_TYPE_MEDIUMBLOB: None,
    TSDB_DATA_TYPE_BINARY: _parse_string,
    TSDB_DATA_TYPE_GEOMETRY: None,
}


cdef taos_fetch_block_v3(TAOS_RES *res, TAOS_FIELD *fields, int field_count, dt_epoch):
    cdef TAOS_ROW pblock
    num_of_rows = taos_fetch_block(res, &pblock)
    if num_of_rows == 0:
        return [], 0

    precision = taos_result_precision(res)
    blocks = [None] * field_count

    cdef int i
    for i in range(field_count):
        data = pblock[i]
        field = fields[i]

        if field.type in (TSDB_DATA_TYPE_TIMESTAMP, ):
            is_null = taos_get_column_data_is_null(res, i, num_of_rows)
            blocks[i] = _parse_datetime(<size_t>data, num_of_rows, <size_t>is_null, precision, dt_epoch)
            free(is_null)
        elif field.type in SIZED_TYPE:
            is_null = taos_get_column_data_is_null(res, i, num_of_rows)
            blocks[i] = CONVERT_FUNC[field.type](<size_t>data, num_of_rows, <size_t>is_null)
            free(is_null)
        elif field.type in UNSIZED_TYPE:
            offsets = taos_get_column_data_offset(res, i)
            blocks[i] = _parse_string(<size_t>data, num_of_rows, <size_t>offsets)
        else:
            pass

    return blocks, abs(num_of_rows)

cpdef fetch_all(size_t ptr, dt_epoch):
    res = <TAOS_RES*>ptr
    fields = taos_fetch_fields(res)
    field_count = taos_field_count(res)

    chunks = []
    cdef int row_count = 0
    while True:
         block, num_of_rows = taos_fetch_block_v3(res, fields, field_count, dt_epoch)
         errno = taos_errno(res)

         if errno != 0:
            raise ProgrammingError(taos_errstr(res), errno)

         if num_of_rows == 0:
            break

         row_count += num_of_rows
         chunks.append(block)

    return [row for chunk in chunks for row in map(tuple, zip(*chunk))]


# ---------------------------------------- Taos Object --------------------------------------------------------------- v


# ---------------------------------------- Taos Object --------------------------------------------------------------- ^
