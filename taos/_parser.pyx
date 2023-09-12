# cython: profile=True

import datetime as dt

cdef list _parse_binary_string(size_t ptr, int num_of_rows, int field_length):
    cdef list res = []
    cdef int i
    for i in range(abs(num_of_rows)):
        nchar_ptr = ptr + field_length * i
        py_string = (<char*>nchar_ptr).decode("utf-8")
        res.append(py_string)

    return res

cdef list _parse_nchar_string(size_t ptr, int num_of_rows, int field_length):
    cdef list res = []
    cdef int i
    for i in range(abs(num_of_rows)):
        c_char_ptr = ptr + field_length * i
        py_string = (<char *>c_char_ptr)[:field_length].decode("utf-8")
        res.append(py_string)

    return res

cdef list _parse_bytes(size_t ptr, int num_of_rows, size_t offsets):
    cdef list res = []
    cdef int i
    cdef size_t rbyte_ptr
    cdef size_t c_char_ptr
    cdef int *_offset = <int*>offsets
    for i in range(abs(num_of_rows)):
        if _offset[i] == -1:
            res.append(None)
        else:
            rbyte_ptr = ptr + _offset[i]
            rbyte = (<uint16_t*>rbyte_ptr)[0]
            c_char_ptr = rbyte_ptr + sizeof(uint16_t)
            py_bytes = (<char *>c_char_ptr)[:rbyte]
            res.append(py_bytes)

    return res

cdef list _parse_string(size_t ptr, int num_of_rows, size_t offsets):
    cdef list res = []
    cdef int i
    cdef size_t rbyte_ptr
    cdef size_t c_char_ptr
    cdef int *_offset = <int*>offsets
    for i in range(abs(num_of_rows)):
        if _offset[i] == -1:
            res.append(None)
        else:
            rbyte_ptr = ptr + _offset[i]
            rbyte = (<uint16_t*>rbyte_ptr)[0]
            c_char_ptr = rbyte_ptr + sizeof(uint16_t)
            py_string = (<char *>c_char_ptr)[:rbyte].decode("utf-8")
            res.append(py_string)

    return res

cdef list _parse_bool(size_t ptr, int num_of_rows, size_t is_null):
    cdef list res = []
    cdef int i
    cdef bool *_is_null = <bool*>is_null
    v_ptr = <bool*>ptr
    for i in range(abs(num_of_rows)):
        if _is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_int8_t(size_t ptr, int num_of_rows, size_t is_null):
    cdef list res = []
    cdef int i
    cdef bool *_is_null = <bool*>is_null
    v_ptr = <int8_t*>ptr
    for i in range(abs(num_of_rows)):
        if _is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_int16_t(size_t ptr, int num_of_rows, size_t is_null):
    cdef list res = []
    cdef int i
    cdef bool *_is_null = <bool*>is_null
    v_ptr = <int16_t*>ptr
    for i in range(abs(num_of_rows)):
        if _is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_int32_t(size_t ptr, int num_of_rows, size_t is_null):
    cdef list res = []
    cdef int i
    cdef bool *_is_null = <bool*>is_null
    v_ptr = <int32_t*>ptr
    for i in range(abs(num_of_rows)):
        if _is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_int64_t(size_t ptr, int num_of_rows, size_t is_null):
    cdef list res = []
    cdef int i
    cdef bool *_is_null = <bool*>is_null
    v_ptr = <int64_t*>ptr
    for i in range(abs(num_of_rows)):
        if _is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_uint8_t(size_t ptr, int num_of_rows, size_t is_null):
    cdef list res = []
    cdef int i
    cdef bool *_is_null = <bool*>is_null
    v_ptr = <uint8_t*>ptr
    for i in range(abs(num_of_rows)):
        if _is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_uint16_t(size_t ptr, int num_of_rows, size_t is_null):
    cdef list res = []
    cdef int i
    cdef bool *_is_null = <bool*>is_null
    v_ptr = <uint16_t*>ptr
    for i in range(abs(num_of_rows)):
        if _is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_uint32_t(size_t ptr, int num_of_rows, size_t is_null):
    cdef list res = []
    cdef int i
    cdef bool *_is_null = <bool*>is_null
    v_ptr = <uint32_t*>ptr
    for i in range(abs(num_of_rows)):
        if _is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_uint64_t(size_t ptr, int num_of_rows, size_t is_null):
    cdef list res = []
    cdef int i
    cdef bool *_is_null = <bool*>is_null
    v_ptr = <uint64_t*>ptr
    for i in range(abs(num_of_rows)):
        if _is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_int(size_t ptr, int num_of_rows, size_t is_null):
    cdef list res = []
    cdef int i
    cdef bool *_is_null = <bool*>is_null
    v_ptr = <int*>ptr
    for i in range(abs(num_of_rows)):
        if _is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_uint(size_t ptr, int num_of_rows, size_t is_null):
    cdef list res = []
    cdef int i
    cdef bool *_is_null = <bool*>is_null
    v_ptr = <unsigned int*>ptr
    for i in range(abs(num_of_rows)):
        if _is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_float(size_t ptr, int num_of_rows, size_t is_null):
    cdef list res = []
    cdef int i
    cdef bool *_is_null = <bool*>is_null
    v_ptr = <float*>ptr
    for i in range(abs(num_of_rows)):
        if _is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_double(size_t ptr, int num_of_rows, size_t is_null):
    cdef list res = []
    cdef int i
    cdef bool *_is_null = <bool*>is_null
    v_ptr = <double*>ptr
    for i in range(abs(num_of_rows)):
        if _is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_datetime(size_t ptr, int num_of_rows, size_t is_null, int precision, object dt_epoch):
    cdef list res = []
    cdef int i
    cdef bool *_is_null = <bool*>is_null
    cdef double denom = 10**((precision + 1) * 3)
    v_ptr = <int64_t*>ptr
    for i in range(abs(num_of_rows)):
        if _is_null[i]:
            res.append(None)
        else:
            raw_value = v_ptr[i]
            if precision <= 1:
                _dt = dt_epoch + dt.timedelta(seconds=raw_value / denom)
            else:
                _dt = raw_value
            res.append(_dt)
    return res

cdef list _parse_timestamp(size_t ptr, int num_of_rows, size_t is_null):
    cdef list res = []
    cdef int i
    cdef bool *_is_null = <bool*>is_null
    v_ptr = <int64_t*>ptr
    for i in range(abs(num_of_rows)):
        if _is_null[i]:
            res.append(None)
        else:
            raw_value = v_ptr[i]
            res.append(raw_value)
    return res

cdef list _convert_timestamp_to_datetime(list ts, int precision, object dt_epoch):
    cdef double denom = 10**((precision + 1) * 3)
    for i, t in enumerate(ts):
        if t is not None:
            ts[i] = dt_epoch + dt.timedelta(seconds=t / denom)

    return ts