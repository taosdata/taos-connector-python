import datetime as dt
from taos.error import ProgrammingError

cpdef print_type_size():
    print("bool:", sizeof(bool))
    print("int8_t", sizeof(int8_t))
    print("int16_t", sizeof(int16_t))
    print("int32_t", sizeof(int32_t))
    print("int64_t", sizeof(int64_t))
    print("uint8_t", sizeof(uint8_t))
    print("uint16_t", sizeof(uint16_t))
    print("uint32_t", sizeof(uint32_t))
    print("uint64_t", sizeof(uint64_t))
    print("int", sizeof(int))
    print("uint", sizeof(unsigned int))

cdef list taos_get_column_data_is_null(TAOS_RES *res, int field, int rows):
    cdef list is_null = []
    cdef int i
    for i in range(rows):
        is_null.append(taos_is_null(res, i, field))

    return is_null

cdef list taos_parse_string(size_t ptr, int num_of_rows, int *offsets):
    cdef list res = []
    cdef int i
    cdef size_t rbyte_ptr
    cdef size_t c_char_ptr
    for i in range(abs(num_of_rows)):
        if offsets[i] == -1:
            res.append(None)
        else:
            rbyte_ptr = ptr + offsets[i]
            rbyte = (<unsigned short *>rbyte_ptr)[0]
            c_char_ptr = rbyte_ptr + sizeof(unsigned short)
            py_string = (<char *>c_char_ptr)[:rbyte].decode("utf-8")
            res.append(py_string)

    return res

cdef list taos_parse_bool(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <bool*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list taos_parse_int8_t(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <int8_t*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list taos_parse_int16_t(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <int16_t*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list taos_parse_int32_t(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <int32_t*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list taos_parse_int64_t(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <int64_t*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list taos_parse_uint8_t(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <uint8_t*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list taos_parse_uint16_t(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <uint16_t*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list taos_parse_uint32_t(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <uint32_t*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list taos_parse_uint64_t(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <uint64_t*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list taos_parse_int(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <int*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list taos_parse_uint(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <unsigned int*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list taos_parse_float(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <float*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list taos_parse_double(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <double*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list taos_parse_timestamp(size_t ptr, int num_of_rows, list is_null, int precision, dt_epoch):
    cdef list res = []
    cdef int i
    v_ptr = <int64_t*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            raw_value = v_ptr[i]
            if precision <= 1:
                _dt = dt_epoch + dt.timedelta(seconds=raw_value / 10**((precision + 1) * 3))
            else:
                _dt = raw_value
            res.append(_dt)
    return res

cdef taos_fetch_block_v3_cython(TAOS_RES *res, TAOS_FIELD *fields, int field_count, dt_epoch):
    cdef TAOS_ROW pblock
    num_of_rows = taos_fetch_block(res, &pblock)
    if num_of_rows == 0:
        return None, 0

    precision = taos_result_precision(res)
    blocks = [None] * field_count

    cdef int i
    for i in range(field_count):
        data = pblock[i]
        field = fields[i]

        if field.type in (TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_NCHAR, TSDB_DATA_TYPE_JSON):
            offsets = taos_get_column_data_offset(res, i)
            blocks[i] = taos_parse_string(<size_t>data, num_of_rows, offsets)
        elif field.type in SIZED_TYPE:
            is_null = taos_get_column_data_is_null(res, i, num_of_rows)
            blocks[i] = CONVERT_FUNC[field.type](<size_t>data, num_of_rows, is_null)
        elif field.type in (TSDB_DATA_TYPE_TIMESTAMP, ):
            is_null = taos_get_column_data_is_null(res, i, num_of_rows)
            blocks[i] = taos_parse_timestamp(<size_t>data, num_of_rows, is_null, precision, dt_epoch)
        else:
            pass

    return blocks, abs(num_of_rows)

cpdef fetch_all_cython(size_t ptr, dt_epoch=None):
    res = <TAOS_RES*>ptr
    fields = taos_fetch_fields(res)
    field_count = taos_field_count(res)

    if dt_epoch is None:
        dt_epoch = dt.datetime.fromtimestamp(0)

    chunks = []
    cdef int row_count = 0
    while True:
         block, num_of_rows = taos_fetch_block_v3_cython(res, fields, field_count, dt_epoch)
         errno = taos_errno(res)

         if errno != 0:
            raise ProgrammingError(taos_errstr(res), errno)

         if num_of_rows == 0:
            break

         row_count += num_of_rows
         chunks.append(block)

    return [row for chunk in chunks for row in map(tuple, zip(*chunk))]

CONVERT_FUNC = {
    TSDB_DATA_TYPE_BOOL: taos_parse_bool,
    TSDB_DATA_TYPE_TINYINT: taos_parse_int8_t,
    TSDB_DATA_TYPE_SMALLINT: taos_parse_int16_t,
    TSDB_DATA_TYPE_INT: taos_parse_int,
    TSDB_DATA_TYPE_BIGINT: taos_parse_int64_t,
    TSDB_DATA_TYPE_FLOAT: taos_parse_float,
    TSDB_DATA_TYPE_DOUBLE: taos_parse_double,
    TSDB_DATA_TYPE_VARCHAR: None,
    TSDB_DATA_TYPE_TIMESTAMP: taos_parse_timestamp,
    TSDB_DATA_TYPE_NCHAR: None,
    TSDB_DATA_TYPE_UTINYINT: taos_parse_uint8_t,
    TSDB_DATA_TYPE_USMALLINT: taos_parse_uint16_t,
    TSDB_DATA_TYPE_UINT: taos_parse_uint,
    TSDB_DATA_TYPE_UBIGINT: taos_parse_uint64_t,
    TSDB_DATA_TYPE_JSON: None,
    TSDB_DATA_TYPE_VARBINARY: None,
    TSDB_DATA_TYPE_DECIMAL: None,
    TSDB_DATA_TYPE_BLOB: None,
    TSDB_DATA_TYPE_MEDIUMBLOB: None,
    TSDB_DATA_TYPE_BINARY: None,
    TSDB_DATA_TYPE_GEOMETRY: None,
}

SIZED_TYPE = (
    TSDB_DATA_TYPE_BOOL,
    TSDB_DATA_TYPE_TINYINT,
    TSDB_DATA_TYPE_SMALLINT,
    TSDB_DATA_TYPE_INT,
    TSDB_DATA_TYPE_BIGINT,
    TSDB_DATA_TYPE_FLOAT,
    TSDB_DATA_TYPE_DOUBLE,
    TSDB_DATA_TYPE_VARCHAR,
    TSDB_DATA_TYPE_UTINYINT,
    TSDB_DATA_TYPE_USMALLINT,
    TSDB_DATA_TYPE_UINT,
    TSDB_DATA_TYPE_UBIGINT,
)