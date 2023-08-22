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

cdef list _parse_string(size_t ptr, int num_of_rows, int *offsets):
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

cdef list _parse_bool(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <bool*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_int8_t(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <int8_t*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_int16_t(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <int16_t*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_int32_t(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <int32_t*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_int64_t(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <int64_t*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_uint8_t(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <uint8_t*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_uint16_t(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <uint16_t*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_uint32_t(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <uint32_t*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_uint64_t(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <uint64_t*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_int(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <int*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_uint(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <unsigned int*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_float(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <float*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_double(size_t ptr, int num_of_rows, list is_null):
    cdef list res = []
    cdef int i
    v_ptr = <double*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            res.append(v_ptr[i])

    return res

cdef list _parse_timestamp(size_t ptr, int num_of_rows, list is_null, int precision, dt_epoch):
    cdef list res = []
    cdef int i
    cdef double denom = 10**((precision + 1) * 3)
    v_ptr = <int64_t*>ptr
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            raw_value = v_ptr[i]
            if precision <= 1:
                _dt = dt_epoch + dt.timedelta(seconds=raw_value / denom)
            else:
                _dt = raw_value
            res.append(_dt)
    return res
