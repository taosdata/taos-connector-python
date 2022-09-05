use pyo3::types::PyTuple;
use pyo3::PyIterProtocol;
use pyo3::{create_exception, exceptions::PyException};
use pyo3::{prelude::*, PyObjectProtocol};
use taos_query::prelude::Field;
use taos_query::{common::RawBlock as Block, prelude::BorrowedValue, Fetchable};
use taos_ws::query::sync::*;

create_exception!(taosws, ConnectionError, PyException);
create_exception!(taosws, QueryError, PyException);
create_exception!(taosws, FetchError, PyException);

#[pyclass]
struct Taosws {
    _inner: WsClient,
}

#[pyclass]
struct TaosField {
    _inner: Field,
}

impl TaosField {
    fn new(inner: &Field) -> Self {
        Self {
            _inner: inner.clone(),
        }
    }
}

#[pymethods]
impl TaosField {
    fn name(&self) -> &str {
        self._inner.name()
    }

    fn r#type(&self) -> &str {
        self._inner.ty().name()
    }

    fn bytes(&self) -> u32 {
        self._inner.bytes()
    }
}

#[pyproto]
impl PyObjectProtocol for TaosField {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "{{name: {}, type: {}, bytes: {}}}",
            self.name(),
            self.r#type(),
            self.bytes()
        ))
    }

    fn __str__(&self) -> PyResult<String> {
        Ok(format!(
            "{{name: {}, type: {}, bytes: {}}}",
            self.name(),
            self.r#type(),
            self.bytes()
        ))
    }
}

#[pyclass]
struct TaosResult {
    _inner: ResultSet,
    _block: Option<Block>,
    _current: usize,
    _num_of_fields: i32,
}

#[pymethods]
impl Taosws {
    fn query(&self, sql: &str) -> PyResult<TaosResult> {
        match self._inner.s_query(sql) {
            Ok(rs) => {
                let cols = rs.num_of_fields();
                Ok(TaosResult {
                    _inner: rs,
                    _block: None,
                    _current: 0,
                    _num_of_fields: cols as _,
                })
            }
            Err(err) => Err(QueryError::new_err(err.errstr())),
        }
    }

    fn execute(&self, sql: &str) -> PyResult<i32> {
        match self._inner.s_query(sql) {
            Ok(rs) => Ok(rs.affected_rows()),
            Err(err) => Err(QueryError::new_err(err.errstr())),
        }
    }
}

#[pyproto]
impl PyIterProtocol for TaosResult {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<Self>) -> Option<PyObject> {
        if let Some(block) = slf._block.as_ref() {
            if slf._current >= block.nrows() {
                slf._block = slf._inner.fetch_block().unwrap_or_default();
            }
        } else {
            slf._block = slf._inner.fetch_block().unwrap_or_default();
        }
        Python::with_gil(|py| -> Option<PyObject> {
            if let Some(block) = slf._block.as_ref() {
                let mut vec = Vec::new();
                for col in 0..block.ncols() {
                    let value = block.get_ref(slf._current, col).unwrap();
                    let value = match value {
                        BorrowedValue::Null(_) => Option::<()>::None.into_py(py),
                        BorrowedValue::Bool(v) => v.into_py(py),
                        BorrowedValue::TinyInt(v) => v.into_py(py),
                        BorrowedValue::SmallInt(v) => v.into_py(py),
                        BorrowedValue::Int(v) => v.into_py(py),
                        BorrowedValue::BigInt(v) => v.into_py(py),
                        BorrowedValue::UTinyInt(v) => v.into_py(py),
                        BorrowedValue::USmallInt(v) => v.into_py(py),
                        BorrowedValue::UInt(v) => v.into_py(py),
                        BorrowedValue::UBigInt(v) => v.into_py(py),
                        BorrowedValue::Float(v) => v.into_py(py),
                        BorrowedValue::Double(v) => v.into_py(py),
                        BorrowedValue::Timestamp(ts) => {
                            ts.to_datetime_with_tz().to_string().into_py(py)
                        }
                        BorrowedValue::VarChar(s) => s.into_py(py),
                        BorrowedValue::NChar(v) => v.as_ref().into_py(py),
                        BorrowedValue::Json(j) => std::str::from_utf8(&j).unwrap().into_py(py),
                        _ => Option::<()>::None.into_py(py),
                    };
                    vec.push(value);
                }
                slf._current += 1;
                return Some(PyTuple::new(py, vec).to_object(py));
            }
            None
        })
    }
}

#[pymethods]
impl TaosResult {
    #[getter]
    fn fields(&self) -> PyResult<Vec<TaosField>> {
        Ok(self
            ._inner
            .fields()
            .into_iter()
            .map(TaosField::new)
            .collect())
    }

    #[getter]
    fn field_count(&self) -> PyResult<i32> {
        Ok(self._num_of_fields)
    }
}

#[pyfunction]
fn connect(dsn: &str) -> PyResult<Taosws> {
    match WsClient::from_dsn(dsn) {
        Ok(client) => Ok(Taosws { _inner: client }),
        Err(err) => Err(ConnectionError::new_err(err.errstr())),
    }
}

#[pymodule]
fn taosws(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Taosws>()?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add("ConnectionError", py.get_type::<ConnectionError>())?;
    m.add("QueryError", py.get_type::<QueryError>())?;
    m.add("FetchError", py.get_type::<FetchError>())?;
    Ok(())
}
