use std::str::FromStr;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use pyo3::{create_exception, exceptions::PyException};

shadow_rs::shadow!(build);

use ::taos::{sync::*, RawBlock, ResultSet};

// PEP249 Exceptions Definition
//
// Layout:
//
// ```
// Exception
// |__Warning
// |__Error
//    |__InterfaceError
//    |__DatabaseError
//       |__DataError
//       |__OperationalError
//       |__IntegrityError
//       |__InternalError
//       |__ProgrammingError
//       |__NotSupportedError
// ```

create_exception!(
    taosws,
    Warning,
    PyException,
    "Calling some methods will produce warning."
);
create_exception!(taosws, Error, PyException, "The root error exception");
create_exception!(
    taosws,
    InterfaceError,
    PyException,
    "The low-level api caused exception"
);
create_exception!(taosws, DatabaseError, Error);
create_exception!(taosws, DataError, DatabaseError);
create_exception!(taosws, OperationalError, DatabaseError);
create_exception!(taosws, IntegrityError, DatabaseError);
create_exception!(taosws, InternalError, DatabaseError);
create_exception!(taosws, ProgrammingError, DatabaseError);
create_exception!(taosws, NotSupportedError, DatabaseError);

create_exception!(taosws, QueryError, DatabaseError);
create_exception!(taosws, FetchError, DatabaseError);

create_exception!(taosws, ConnectionError, Error, "Connection error"); // custom
create_exception!(
    taosws,
    AlreadyClosedError,
    ConnectionError,
    "Connection error"
); // custom
create_exception!(taosws, ConsumerException, Error);

mod common;

mod consumer;
use consumer::{Consumer, Message};

mod cursor;
use cursor::*;

mod field;
use field::TaosField;

#[pyclass]
struct Connection {
    _builder: Option<TaosBuilder>,
    _inner: Option<Taos>,
}

#[pyclass]
struct TaosResult {
    _inner: ResultSet,
    _block: Option<RawBlock>,
    _current: usize,
    _num_of_fields: i32,
}

impl Connection {
    fn current_cursor(&self) -> PyResult<&Taos> {
        Ok(self
            ._inner
            .as_ref()
            .ok_or_else(|| ConnectionError::new_err("Connection was already closed"))?)
    }

    fn builder(&self) -> PyResult<&TaosBuilder> {
        Ok(self
            ._builder
            .as_ref()
            .ok_or_else(|| ConnectionError::new_err("Connection was already closed"))?)
    }
}

#[pymethods]
impl Connection {
    /// Create new connection
    ///
    /// @dsn: Data Source Name string, optional.
    /// @args:
    #[new]
    pub fn new(dsn: Option<&str>, args: Option<&PyDict>) -> PyResult<Self> {
        todo!()
    }
    pub fn query(&self, sql: &str) -> PyResult<TaosResult> {
        match self.current_cursor()?.query(sql) {
            Ok(rs) => {
                let cols = rs.num_of_fields();
                Ok(TaosResult {
                    _inner: rs,
                    _block: None,
                    _current: 0,
                    _num_of_fields: cols as _,
                })
            }
            Err(err) => Err(QueryError::new_err(err.to_string())),
        }
    }

    pub fn execute(&self, sql: &str) -> PyResult<i32> {
        match self.current_cursor()?.query(sql) {
            Ok(rs) => Ok(rs.affected_rows()),
            Err(err) => Err(QueryError::new_err(err.to_string())),
        }
    }

    /// PEP249 close() method.
    pub fn close(&mut self) {
        self._inner.take();
        self._builder.take();
    }

    /// PEP249 commit() method, do nothing here.
    pub fn commit(&self) {}

    /// PEP249 commit() method, do nothing here.
    pub fn rollback(&self) {}

    ///
    /// PEP249 cursor() method.
    pub fn cursor(&self) -> PyResult<Cursor> {
        Ok(Cursor::new(self.builder()?.build().map_err(|err| {
            ConnectionError::new_err(err.to_string())
        })?))
    }
}

#[pymethods]
impl TaosResult {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<Self>) -> PyResult<Option<PyObject>> {
        if let Some(block) = slf._block.as_ref() {
            if slf._current >= block.nrows() {
                slf._block = slf
                    ._inner
                    .fetch_raw_block()
                    .map_err(|err| FetchError::new_err(err.to_string()))?;
            }
        } else {
            slf._block = slf
                ._inner
                .fetch_raw_block()
                .map_err(|err| FetchError::new_err(err.to_string()))?;
        }
        Ok(Python::with_gil(|py| -> Option<PyObject> {
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
        }))
    }

    #[getter]
    fn fields(&self) -> Vec<TaosField> {
        self._inner.fields().into_iter().map(Into::into).collect()
    }

    #[getter]
    fn field_count(&self) -> i32 {
        self._num_of_fields
    }
}

static API_LEVEL: &str = "2.0";
static THREAD_SAFETY: u8 = 2;
static PARAMS_STYLE: &str = "pyformat";

#[pyfunction(args = "**")]
fn connect(
    dsn: Option<&str>,
    url: Option<&str>,
    user: Option<&str>,
    username: Option<&str>,
    password: Option<&str>,
    database: Option<&str>,
    host: Option<&str>,
    port: Option<u16>,
    websocket: Option<bool>,
    args: Option<&PyDict>,
) -> PyResult<Connection> {
    let _ = args;
    let user = user.or(username);
    let dsn = dsn.or(url).unwrap_or("taos://");

    let mut dsn = Dsn::from_str(dsn).map_err(|err| ConnectionError::new_err(err.to_string()))?;

    if let Some(value) = user {
        dsn.username.replace(value.to_string());
    }
    if let Some(value) = password {
        dsn.password.replace(value.to_string());
    }
    if let Some(value) = database {
        dsn.subject.replace(value.to_string());
    }

    let mut addr = Address::default();

    if let Some(scheme) = websocket {
        if scheme {
            dsn.protocol = Some("ws".to_string());
        }
    } else {
    }
    match (host, port) {
        (Some(host), Some(port)) => {
            addr.host.replace(host.to_string());
            addr.port.replace(port);
        }
        (Some(host), None) => {
            addr.host.replace(host.to_string());
        }
        (_, Some(port)) => {
            addr.port.replace(port);
        }
        _ => {
            addr.host.replace("localhost".to_string());
        }
    }

    if dsn.protocol.is_none() {
        dsn.protocol.replace("ws".to_string());
    }

    let builder =
        TaosBuilder::from_dsn(dsn).map_err(|err| ConnectionError::new_err(err.to_string()))?;

    let client = builder
        .build()
        .map_err(|err| ConnectionError::new_err(err.to_string()))?;
    Ok(Connection {
        _builder: Some(builder),
        _inner: Some(client),
    })
}

#[pymodule]
fn taosws(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Connection>()?;
    m.add_class::<Cursor>()?;
    m.add_class::<TaosField>()?;
    m.add_class::<TaosResult>()?;
    m.add_class::<Consumer>()?;
    m.add_class::<Message>()?;

    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add("apilevel", API_LEVEL)?;
    m.add("threadsafety", THREAD_SAFETY)?;
    m.add("paramstyle", PARAMS_STYLE)?;
    m.add("__version__", crate::build::PKG_VERSION)?;

    macro_rules! add_type {
        ($m:ident $(, $type:ident)*) => {
            $($m.add(stringify!($type), py.get_type::<$type>())?;)*
        };
    }

    add_type!(
        m,
        Error,
        Warning,
        DatabaseError,
        InterfaceError,
        ConnectionError,
        DataError,
        OperationalError,
        IntegrityError,
        InternalError,
        ProgrammingError,
        NotSupportedError,
        QueryError,
        FetchError,
        ConsumerException
    );
    Ok(())
}
