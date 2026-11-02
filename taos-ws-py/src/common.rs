// Copyright (c) 2022 TAOS Data, Inc.
//
// SPDX-License-Identifier: MIT
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use std::{ops::Range, sync::OnceLock};

use chrono::{Datelike, Local, TimeZone, Timelike};
use chrono_tz::Tz;
use iana_time_zone::get_timezone;
use pyo3::{
    prelude::*,
    types::{PyDict, PyTuple},
    IntoPyObjectExt,
};
use taos::{taos_query::common::Timestamp, BorrowedValue, RawBlock};

use crate::ConsumerException;

pub fn to_py_decimal(value: impl std::fmt::Display, py: Python<'_>) -> PyResult<Py<PyAny>> {
    let decimal_mod = py.import("decimal")?;
    let decimal_cls = decimal_mod.getattr("Decimal")?;
    let decimal = value.to_string();
    Ok(decimal_cls.call1((decimal,))?.unbind())
}

pub fn to_py_datetime(ts: Timestamp, tz: Option<Tz>, py: Python) -> PyResult<Py<PyAny>> {
    let datetime_mod = py.import("datetime")?;
    let datetime = datetime_mod.getattr("datetime")?;

    if let Ok(zoneinfo) = py.import("zoneinfo") {
        let zone_info = zoneinfo.getattr("ZoneInfo")?;
        let (args, tz) = get_datetime_args_and_tz_name(py, ts, tz)?;
        let kwargs = PyDict::new(py);
        let tz = zone_info.call1((tz,))?;
        kwargs.set_item("tzinfo", tz)?;
        return Ok(datetime.call(args, Some(&kwargs))?.unbind());
    }

    if let Ok(pytz) = py.import("pytz") {
        let (args, tz) = get_datetime_args_and_tz_name(py, ts, tz)?;
        let naive_dt = datetime.call(args, None)?;
        let tz = pytz.call_method1("timezone", (tz,))?;
        return Ok(tz.call_method1("localize", (naive_dt,))?.unbind());
    }

    warn_missing_timezone_libs(tz);

    let timezone = datetime_mod.getattr("timezone")?;
    let utc = timezone.getattr("utc")?;
    let args = get_datetime_args(py, ts, Tz::UTC)?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("tzinfo", utc)?;
    Ok(datetime.call(args, Some(&kwargs))?.unbind())
}

fn get_local_timezone() -> &'static str {
    static LOCAL_TZ: OnceLock<String> = OnceLock::new();
    LOCAL_TZ.get_or_init(|| get_timezone().unwrap_or("UTC".to_string()))
}

fn get_datetime_args_and_tz_name(
    py: Python<'_>,
    ts: Timestamp,
    tz: Option<Tz>,
) -> PyResult<(Bound<'_, PyTuple>, &'static str)> {
    if let Some(tz) = tz {
        let args = get_datetime_args(py, ts, tz)?;
        Ok((args, tz.name()))
    } else {
        let args = get_datetime_args(py, ts, Local)?;
        Ok((args, get_local_timezone()))
    }
}

fn get_datetime_args<Tz: TimeZone>(py: Python<'_>, ts: Timestamp, tz: Tz) -> PyResult<Bound<'_, PyTuple>> {
    let dt = tz.from_utc_datetime(&ts.to_naive_datetime());
    PyTuple::new(
        py,
        [
            dt.year() as i64,
            dt.month() as _,
            dt.day() as _,
            dt.hour() as _,
            dt.minute() as _,
            dt.second() as _,
            dt.timestamp_subsec_micros() as _,
        ],
    )
}

fn warn_missing_timezone_libs(tz: Option<Tz>) {
    static WARN: OnceLock<()> = OnceLock::new();
    WARN.get_or_init(|| {
        Python::attach(|py| {
            let warnings = py.import("warnings").unwrap();
            let tz = tz.map_or(get_local_timezone(), |t| t.name());
            warnings
                .call_method1(
                    "warn",
                    (format!(
                        "zoneinfo and pytz are not available, fallback to UTC for tz '{tz}'",
                    ),),
                )
                .ok();
        });
    });
}

pub unsafe fn get_row_of_block_unchecked(
    py: Python,
    block: &RawBlock,
    index: usize,
) -> PyResult<Py<PyAny>> {
    let mut vec = Vec::new();
    for i in 0..block.ncols() {
        let val = block.get_ref(index, i).unwrap();
        let val = match val {
            BorrowedValue::Null(_) => Option::<()>::None.into_py_any(py)?,
            BorrowedValue::Bool(v) => v.into_py_any(py)?,
            BorrowedValue::TinyInt(v) => v.into_py_any(py)?,
            BorrowedValue::SmallInt(v) => v.into_py_any(py)?,
            BorrowedValue::Int(v) => v.into_py_any(py)?,
            BorrowedValue::BigInt(v) => v.into_py_any(py)?,
            BorrowedValue::Float(v) => v.into_py_any(py)?,
            BorrowedValue::Double(v) => v.into_py_any(py)?,
            BorrowedValue::VarChar(v) => v.into_py_any(py)?,
            BorrowedValue::Timestamp(v) => match v.precision() {
                taos::Precision::Nanosecond => v.as_raw_i64().into_py_any(py)?,
                _ => to_py_datetime(v, block.timezone(), py)?,
            },
            BorrowedValue::NChar(v) => v.into_py_any(py)?,
            BorrowedValue::UTinyInt(v) => v.into_py_any(py)?,
            BorrowedValue::USmallInt(v) => v.into_py_any(py)?,
            BorrowedValue::UInt(v) => v.into_py_any(py)?,
            BorrowedValue::UBigInt(v) => v.into_py_any(py)?,
            BorrowedValue::Json(v) => std::str::from_utf8(&v)
                .map_err(|err| ConsumerException::new_err(err.to_string()))?
                .to_string()
                .into_py_any(py)?,
            BorrowedValue::VarBinary(v) => v.as_ref().into_py_any(py)?,
            BorrowedValue::Geometry(v) => v.as_ref().into_py_any(py)?,
            BorrowedValue::Decimal64(v) => to_py_decimal(v, py)?,
            BorrowedValue::Decimal(v) => to_py_decimal(v, py)?,
            BorrowedValue::Blob(v) => v.as_ref().into_py_any(py)?,
            BorrowedValue::MediumBlob(_) => todo!(),
        };
        vec.push(val);
    }
    Ok(PyTuple::new(py, vec)?.into_any().unbind())
}

pub fn get_row_of_block(py: Python, block: &RawBlock, index: usize) -> PyResult<Option<Py<PyAny>>> {
    if block.nrows() > index {
        Ok(Some(unsafe {
            get_row_of_block_unchecked(py, block, index)?
        }))
    } else {
        Ok(None)
    }
}

pub fn get_slice_of_block(
    py: Python,
    block: &RawBlock,
    range: Range<usize>,
) -> PyResult<(Vec<Py<PyAny>>, Option<usize>)> {
    debug_assert!(range.start < block.nrows());
    let start = range.start;
    let end = range.end;
    let (range, remain) = if end > block.nrows() {
        (start..block.nrows(), Some(end - block.nrows()))
    } else {
        (range, None)
    };

    let slices = range
        .into_iter()
        .map(|index| {
            // Bound Python object conversion batches without changing row order or types.
            if index > start && (index - start) % 1024 == 0 {
                py.detach(std::thread::yield_now);
            }
            unsafe { get_row_of_block_unchecked(py, block, index) }
        })
        .collect::<PyResult<Vec<_>>>()?;

    Ok((slices, remain))
}

pub fn get_all_of_block(py: Python, block: &RawBlock) -> PyResult<Vec<Py<PyAny>>> {
    (0..block.nrows())
        .into_iter()
        .map(|index| {
            if index > 0 && index % 1024 == 0 {
                py.detach(std::thread::yield_now);
            }
            unsafe { get_row_of_block_unchecked(py, block, index) }
        })
        .collect::<PyResult<Vec<_>>>()
}
