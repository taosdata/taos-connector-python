from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t, uintptr_t
from libcpp cimport bool

cdef list _parse_binary_string(size_t ptr, int num_of_rows, int field_length)

cdef list _parse_nchar_string(size_t ptr, int num_of_rows, int field_length)

cdef list _parse_string(size_t ptr, int num_of_rows, int *offsets)

cdef list _parse_bool(size_t ptr, int num_of_rows, list is_null)

cdef list _parse_int8_t(size_t ptr, int num_of_rows, list is_null)

cdef list _parse_int16_t(size_t ptr, int num_of_rows, list is_null)

cdef list _parse_int32_t(size_t ptr, int num_of_rows, list is_null)

cdef list _parse_int64_t(size_t ptr, int num_of_rows, list is_null)

cdef list _parse_uint8_t(size_t ptr, int num_of_rows, list is_null)

cdef list _parse_uint16_t(size_t ptr, int num_of_rows, list is_null)

cdef list _parse_uint32_t(size_t ptr, int num_of_rows, list is_null)

cdef list _parse_uint64_t(size_t ptr, int num_of_rows, list is_null)

cdef list _parse_int(size_t ptr, int num_of_rows, list is_null)

cdef list _parse_uint(size_t ptr, int num_of_rows, list is_null)

cdef list _parse_float(size_t ptr, int num_of_rows, list is_null)

cdef list _parse_double(size_t ptr, int num_of_rows, list is_null)

cdef list _parse_timestamp(size_t ptr, int num_of_rows, list is_null, int precision, dt_epoch)