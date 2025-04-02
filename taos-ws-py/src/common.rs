use std::ops::Range;

use chrono::{Datelike, Timelike};
use pyo3::{prelude::*, types::PyTuple};
use taos::{BorrowedValue, RawBlock};

use crate::ConsumerException;

pub fn to_py_datetime(dt: chrono::NaiveDateTime, py: Python) -> PyResult<PyObject> {
    let datetime = py.import("datetime")?;
    let datetime = datetime.getattr("datetime")?;
    let args = PyTuple::new(
        py,
        [
            dt.year() as i64,
            dt.month() as i64,
            dt.day() as _,
            dt.hour() as _,
            dt.minute() as _,
            dt.second() as _,
            dt.timestamp_subsec_micros() as _,
        ],
    );

    Ok(datetime.call(args, None)?.into_py(py))
}
// pub fn datetime_to_py(t: chrono py: Python)
pub unsafe fn get_row_of_block_unchecked(py: Python, block: &RawBlock, index: usize) -> PyObject {
    let mut vec = Vec::new();
    for i in 0..block.ncols() {
        let val = block.get_ref(index, i).unwrap();
        let val = match val {
            BorrowedValue::Null(_) => Option::<()>::None.into_py(py),
            BorrowedValue::Bool(v) => v.into_py(py),
            BorrowedValue::TinyInt(v) => v.into_py(py),
            BorrowedValue::SmallInt(v) => v.into_py(py),
            BorrowedValue::Int(v) => v.into_py(py),
            BorrowedValue::BigInt(v) => v.into_py(py),
            BorrowedValue::Float(v) => v.into_py(py),
            BorrowedValue::Double(v) => v.into_py(py),
            BorrowedValue::VarChar(v) => v.into_py(py),
            BorrowedValue::Timestamp(v) => match v.precision() {
                taos::Precision::Nanosecond => v.as_raw_i64().to_object(py),
                _ => to_py_datetime(v.to_datetime_with_tz().naive_local(), py).unwrap(),
            },
            BorrowedValue::NChar(v) => v.into_py(py),
            BorrowedValue::UTinyInt(v) => v.into_py(py),
            BorrowedValue::USmallInt(v) => v.into_py(py),
            BorrowedValue::UInt(v) => v.into_py(py),
            BorrowedValue::UBigInt(v) => v.into_py(py),
            BorrowedValue::Json(v) => std::str::from_utf8(&v)
                .map_err(|err| ConsumerException::new_err(err.to_string()))
                .unwrap()
                .to_string()
                .into_py(py),
            BorrowedValue::VarBinary(v) => v.into_py(py),
            BorrowedValue::Geometry(v) => v.into_py(py),
            BorrowedValue::Decimal64(v) => v.to_string().into_py(py),
            BorrowedValue::Decimal(v) => v.to_string().into_py(py),
            BorrowedValue::Blob(_) => todo!(),
            BorrowedValue::MediumBlob(_) => todo!(),
        };
        vec.push(val);
    }
    PyTuple::new(py, vec).to_object(py)
}
pub fn get_row_of_block(py: Python, block: &RawBlock, index: usize) -> Option<PyObject> {
    if block.nrows() > index {
        Some(unsafe { get_row_of_block_unchecked(py, block, index) })
    } else {
        None
    }
}

pub fn get_slice_of_block(
    py: Python,
    block: &RawBlock,
    range: Range<usize>,
) -> (Vec<PyObject>, Option<usize>) {
    debug_assert!(range.start < block.nrows());
    let start = range.start;
    let end = range.end;
    let (range, remain) = if end > block.nrows() {
        (start..block.nrows(), Some(end - block.nrows()))
    } else {
        (range, None)
    };

    let slices: Vec<_> = range
        .into_iter()
        .map(|index| unsafe { get_row_of_block_unchecked(py, block, index) })
        .collect();
    (slices, remain)
}

pub fn get_all_of_block(py: Python, block: &RawBlock) -> Vec<PyObject> {
    (0..block.nrows())
        .into_iter()
        .map(|index| unsafe { get_row_of_block_unchecked(py, block, index) })
        .collect()
}
