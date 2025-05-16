use std::str::FromStr;

use ::taos::{sync::*, RawBlock, ResultSet};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString, PyTuple};
use pyo3::{create_exception, exceptions::PyException};
use taos::taos_query;
use taos::taos_query::common::{SchemalessPrecision, SchemalessProtocol, SmlDataBuilder};
use taos::Value::{
    BigInt, Bool, Double, Float, Geometry, Int, Json, NChar, Null, SmallInt, Timestamp, TinyInt,
    UBigInt, UInt, USmallInt, UTinyInt, VarBinary, VarChar,
};

use consumer::{Consumer, Message};
use cursor::*;
use field::TaosField;

shadow_rs::shadow!(build);

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

mod cursor;

mod field;

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
    pub fn new(_dsn: Option<&str>, _args: Option<&PyDict>) -> PyResult<Self> {
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

    pub fn query_with_req_id(&self, sql: &str, req_id: u64) -> PyResult<TaosResult> {
        match self.current_cursor()?.query_with_req_id(sql, req_id) {
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

    pub fn execute_with_req_id(&self, sql: &str, req_id: u64) -> PyResult<i32> {
        match self.current_cursor()?.query_with_req_id(sql, req_id) {
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

    /// schemaless data to taos
    pub fn schemaless_insert(
        &self,
        lines: Vec<String>,
        protocol: PySchemalessProtocol,
        precision: PySchemalessPrecision,
        ttl: i32,
        req_id: u64,
    ) -> PyResult<()> {
        let protocol: SchemalessProtocol = protocol.into();
        let precision: SchemalessPrecision = precision.into();

        let data = SmlDataBuilder::default()
            .protocol(protocol)
            .precision(precision)
            .data(lines)
            .ttl(ttl)
            .req_id(req_id)
            .build()
            .map_err(|err| DataError::new_err(err.to_string()))?;

        self.current_cursor()?
            .put(&data)
            .map_err(|err| OperationalError::new_err(err.to_string()))?;

        Ok(())
    }

    pub fn statement(&self) -> PyResult<TaosStmt> {
        let stmt = TaosStmt::init(self)?;
        Ok(stmt)
    }

    pub fn stmt2_statement(&self) -> PyResult<TaosStmt2> {
        let stmt2 = TaosStmt2::init(self)?;
        Ok(stmt2)
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

                slf._current = 0;
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
                    if let Some(value) = block.get_ref(slf._current, col) {
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
                            BorrowedValue::VarBinary(v) => v.as_ref().into_py(py),
                            BorrowedValue::Geometry(v) => v.as_ref().into_py(py),
                            _ => Option::<()>::None.into_py(py),
                        };
                        vec.push(value);
                    }
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
fn connect(dsn: Option<&str>, args: Option<&PyDict>) -> PyResult<Connection> {
    let dsn = dsn.unwrap_or("taosws://");

    let mut dsn = Dsn::from_str(dsn).map_err(|err| ConnectionError::new_err(err.to_string()))?;

    if let Some(args) = args {
        const NONE_TAOS_CFG: &[&str] = &[
            "user",
            "username",
            "password",
            "database",
            "host",
            "port",
            "websocket",
            "native",
        ];
        if let Some(value) = args.get_item("user").or(args.get_item("username")) {
            dsn.username.replace(value.to_string());
        }
        if let Some(value) = args.get_item("pass").or(args.get_item("password")) {
            dsn.password.replace(value.to_string());
        }
        if let Some(value) = args.get_item("db").or(args.get_item("database")) {
            dsn.subject.replace(value.to_string());
        }

        let mut addr = Address::default();

        if let Some(scheme) = args.get_item("websocket").or(args.get_item("ws")) {
            if scheme.is_instance_of::<pyo3::types::PyBool>()? {
                if scheme.extract()? {
                    dsn.protocol = Some("ws".to_string());
                }
            } else {
                dsn.protocol = Some(scheme.extract::<String>()?);
            }
        } else if let Some(native) = args.get_item("native") {
            let _ = native;
        } else if dsn.protocol.is_none() {
            dsn.protocol.replace("ws".to_string());
        }

        let host = args
            .get_item("host")
            .or(args.get_item("url"))
            .or(args.get_item("ip"));
        let port = args.get_item("port");
        match (host, port) {
            (Some(host), Some(port)) => {
                addr.host.replace(host.extract::<String>()?);
                if port.is_instance_of::<pyo3::types::PyInt>()? {
                    addr.port.replace(port.extract()?);
                } else if port.is_instance_of::<PyString>()? {
                    addr.port.replace(port.extract::<String>()?.parse()?);
                } else {
                    Err(ConsumerException::new_err(format!("Invalid port: {port}")))?;
                }
            }
            (Some(host), None) => {
                addr.host.replace(host.extract::<String>()?);
            }
            (_, Some(port)) => {
                if port.is_instance_of::<pyo3::types::PyInt>()? {
                    addr.port.replace(port.extract()?);
                } else if port.is_instance_of::<PyString>()? {
                    addr.port.replace(port.extract::<String>()?.parse()?);
                } else {
                    Err(ConsumerException::new_err(format!("Invalid port: {port}")))?;
                }
            }
            _ => {
                // addr.host.replace("localhost".to_string());
            }
        }

        for (key, value) in args
            .into_iter()
            .filter(|(k, _)| !NONE_TAOS_CFG.contains(&k.extract::<&str>().unwrap()))
        {
            dsn.set(key.extract::<&str>()?, value.extract::<&str>()?);
        }
        dsn.addresses.push(addr);
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

#[pyclass]
#[derive(Debug)]
struct TaosStmt {
    _inner: Stmt,
}

#[pymethods]
impl TaosStmt {
    #[new]
    fn init(conn: &Connection) -> PyResult<TaosStmt> {
        let stmt = Stmt::init(conn.current_cursor()?)
            .map_err(|err| ConnectionError::new_err(err.to_string()))?;
        let stmt = TaosStmt { _inner: stmt };
        return Ok(stmt);
    }

    fn prepare(&mut self, sql: &str) -> PyResult<()> {
        self._inner
            .prepare(sql)
            .map_err(|err| ProgrammingError::new_err(err.to_string()))?;
        Ok(())
    }

    fn set_tbname(&mut self, table_name: &str) -> PyResult<()> {
        self._inner
            .set_tbname(table_name)
            .map_err(|err| ProgrammingError::new_err(err.to_string()))?;
        Ok(())
    }

    fn set_tags(&mut self, tags: Vec<PyTagView>) -> PyResult<()> {
        let tags = tags.into_iter().map(|tag| tag._inner).collect_vec();
        self._inner
            .set_tags(&*tags)
            .map_err(|err| ProgrammingError::new_err(err.to_string()))?;
        Ok(())
    }

    fn set_tbname_tags(&mut self, table_name: &str, tags: Vec<PyTagView>) -> PyResult<()> {
        let tags = tags.into_iter().map(|tag| tag._inner).collect_vec();
        self._inner
            .set_tbname(table_name)
            .map_err(|err| ProgrammingError::new_err(err.to_string()))?
            .set_tags(&*tags)
            .map_err(|err| ProgrammingError::new_err(err.to_string()))?;
        Ok(())
    }

    fn bind_param(&mut self, params: Vec<PyColumnView>) -> PyResult<()> {
        let params = params.into_iter().map(|tag| tag._inner).collect_vec();
        self._inner
            .bind(&*params)
            .map_err(|err| ProgrammingError::new_err(err.to_string()))?;
        Ok(())
    }

    fn add_batch(&mut self) -> PyResult<()> {
        self._inner
            .add_batch()
            .map_err(|err| ProgrammingError::new_err(err.to_string()))?;
        Ok(())
    }

    fn execute(&mut self) -> PyResult<usize> {
        let rows = self
            ._inner
            .execute()
            .map_err(|err| QueryError::new_err(err.to_string()))?;
        Ok(rows)
    }

    fn affect_rows(&mut self) -> PyResult<usize> {
        let rows = self._inner.affected_rows();
        Ok(rows)
    }

    fn close(&self) -> PyResult<()> {
        Ok(())
    }
}

#[pyclass]
#[derive(Debug)]
struct TaosStmt2 {
    _inner: Stmt2,
}

#[pyclass]
#[derive(Debug, Clone)]
struct PyStmt2BindParam {
    _inner: Stmt2BindParam,
}

#[pymethods]
impl TaosStmt2 {
    #[new]
    fn init(conn: &Connection) -> PyResult<TaosStmt2> {
        let stmt: Stmt2 = Stmt2::init(conn.current_cursor()?)
            .map_err(|err| ConnectionError::new_err(err.to_string()))?;
        let stmt: TaosStmt2 = TaosStmt2 { _inner: stmt };
        return Ok(stmt);
    }

    fn prepare(&mut self, sql: &str) -> PyResult<()> {
        self._inner
            .prepare(sql)
            .map_err(|err| ProgrammingError::new_err(err.to_string()))?;
        Ok(())
    }

    fn bind(&mut self, params: Vec<PyStmt2BindParam>) -> PyResult<()> {
        let params = params.into_iter().map(|param| param._inner).collect_vec();
        self._inner
            .bind(&params)
            .map_err(|err| ProgrammingError::new_err(err.to_string()))?;
        Ok(())
    }

    fn execute(&mut self) -> PyResult<usize> {
        let rows = self
            ._inner
            .exec()
            .map_err(|err| QueryError::new_err(err.to_string()))?;
        Ok(rows)
    }

    fn affect_rows(&mut self) -> PyResult<usize> {
        let rows = self._inner.affected_rows();
        Ok(rows)
    }

    fn result_set(&mut self) -> PyResult<TaosResult> {
        match self._inner.result_set() {
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


    fn close(&self) -> PyResult<()> {
        Ok(())
    }
}

#[pyfunction]
fn stmt2_bind_param_view(table_name: Option<&str>, tags: Option<Vec<PyTagView>>, columns: Vec<PyColumnView>) -> PyResult<PyStmt2BindParam> {
    if columns.is_empty() {
        return Err(ProgrammingError::new_err("stmt2 columns cannot be empty"));
    }

    let table_name_opt = table_name.map(|s| s.to_string());

    let tag_params = tags.map(|ts| ts.into_iter().map(|tag| tag._inner).collect::<Vec<Value>>());

    let params = columns.into_iter().map(|column| column._inner).collect_vec();
    
    Ok(PyStmt2BindParam {
        _inner: Stmt2BindParam::new(            
            table_name_opt,  
            tag_params,                   
            Some(params)                 
        ),
    })
}

#[pyclass]
#[derive(Debug, Clone)]
enum PyPrecision {
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

impl Into<Precision> for PyPrecision {
    fn into(self) -> Precision {
        match self {
            PyPrecision::Milliseconds => Precision::Millisecond,
            PyPrecision::Microseconds => Precision::Microsecond,
            PyPrecision::Nanoseconds => Precision::Nanosecond,
        }
    }
}

#[pyclass]
#[derive(Debug, Clone)]
enum PyColumnType {
    Bool,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    UTinyInt,
    USmallInt,
    UInt,
    UBigInt,
    Float,
    Double,
    Timestamp,
    VarChar,
    NChar,
    Json,
    VarBinary,
    Decimal,
    Blob,
    MediumBlob,
    Geometry,
}

#[pyclass]
#[derive(Debug, Clone)]
struct PyTagView {
    _inner: Value,
}

#[pyfunction]
fn bool_to_tag(value: Option<bool>) -> PyTagView {
    match value {
        Some(value) => PyTagView {
            _inner: Bool(value),
        },
        None => PyTagView {
            _inner: Null(Ty::Bool),
        },
    }
}

#[pyfunction]
fn tiny_int_to_tag(value: Option<i8>) -> PyTagView {
    match value {
        Some(value) => PyTagView {
            _inner: TinyInt(value),
        },
        None => PyTagView {
            _inner: Null(Ty::TinyInt),
        },
    }
}

#[pyfunction]
fn small_int_to_tag(value: Option<i16>) -> PyTagView {
    match value {
        Some(value) => PyTagView {
            _inner: SmallInt(value),
        },
        None => PyTagView {
            _inner: Null(Ty::SmallInt),
        },
    }
}

#[pyfunction]
fn int_to_tag(value: Option<i32>) -> PyTagView {
    match value {
        Some(value) => PyTagView { _inner: Int(value) },
        None => PyTagView {
            _inner: Null(Ty::Int),
        },
    }
}

#[pyfunction]
fn big_int_to_tag(value: Option<i64>) -> PyTagView {
    match value {
        Some(value) => PyTagView {
            _inner: BigInt(value),
        },
        None => PyTagView {
            _inner: Null(Ty::BigInt),
        },
    }
}

#[pyfunction]
fn float_to_tag(value: Option<f32>) -> PyTagView {
    match value {
        Some(value) => PyTagView {
            _inner: Float(value),
        },
        None => PyTagView {
            _inner: Null(Ty::Float),
        },
    }
}

#[pyfunction]
fn double_to_tag(value: Option<f64>) -> PyTagView {
    match value {
        Some(value) => PyTagView {
            _inner: Double(value),
        },
        None => PyTagView {
            _inner: Null(Ty::Double),
        },
    }
}

#[pyfunction]
fn varchar_to_tag(value: Option<String>) -> PyTagView {
    match value {
        Some(value) => PyTagView {
            _inner: VarChar(value),
        },
        None => PyTagView {
            _inner: Null(Ty::VarChar),
        },
    }
}

#[pyfunction]
fn timestamp_to_tag(value: Option<i64>, precision: PyPrecision) -> PyTagView {
    match value {
        Some(value) => PyTagView {
            _inner: Timestamp(taos_query::common::Timestamp::new(value, precision.into())),
        },
        None => PyTagView {
            _inner: Null(Ty::Timestamp),
        },
    }
}

#[pyfunction]
fn nchar_to_tag(value: Option<String>) -> PyTagView {
    match value {
        Some(value) => PyTagView {
            _inner: NChar(value),
        },
        None => PyTagView {
            _inner: Null(Ty::NChar),
        },
    }
}

#[pyfunction]
fn u_tiny_int_to_tag(value: Option<u8>) -> PyTagView {
    match value {
        Some(value) => PyTagView {
            _inner: UTinyInt(value),
        },
        None => PyTagView {
            _inner: Null(Ty::UTinyInt),
        },
    }
}

#[pyfunction]
fn u_small_int_to_tag(value: Option<u16>) -> PyTagView {
    match value {
        Some(value) => PyTagView {
            _inner: USmallInt(value),
        },
        None => PyTagView {
            _inner: Null(Ty::USmallInt),
        },
    }
}

#[pyfunction]
fn u_int_to_tag(value: Option<u32>) -> PyTagView {
    match value {
        Some(value) => PyTagView {
            _inner: UInt(value),
        },
        None => PyTagView {
            _inner: Null(Ty::UInt),
        },
    }
}

#[pyfunction]
fn u_big_int_to_tag(value: Option<u64>) -> PyTagView {
    match value {
        Some(value) => PyTagView {
            _inner: UBigInt(value),
        },
        None => PyTagView {
            _inner: Null(Ty::UBigInt),
        },
    }
}

#[pyfunction]
fn json_to_tag(value: Option<String>) -> PyTagView {
    match value {
        Some(value) => PyTagView {
            _inner: Json(serde_json::Value::String(value)),
        },
        None => PyTagView {
            _inner: Null(Ty::Json),
        },
    }
}

#[pyfunction]
fn varbinary_to_tag(value: Option<Vec<u8>>) -> PyTagView {
    match value {
        Some(value) => PyTagView {
            _inner: VarBinary(value.into()),
        },
        None => PyTagView {
            _inner: Null(Ty::VarBinary),
        },
    }
}

#[pyfunction]
fn geometry_to_tag(value: Option<Vec<u8>>) -> PyTagView {
    match value {
        Some(value) => PyTagView {
            _inner: Geometry(value.into()),
        },
        None => PyTagView {
            _inner: Null(Ty::Geometry),
        },
    }
}

#[pyclass]
#[derive(Debug, Clone)]
struct PyColumnView {
    _inner: ColumnView,
}

#[pyfunction]
fn millis_timestamps_to_column(values: Vec<Option<i64>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_millis_timestamp(values),
    }
}

#[pyfunction]
fn micros_timestamps_to_column(values: Vec<Option<i64>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_micros_timestamp(values),
    }
}

#[pyfunction]
fn nanos_timestamps_to_column(values: Vec<Option<i64>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_nanos_timestamp(values),
    }
}

#[pyfunction]
fn bools_to_column(values: Vec<Option<bool>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_bools(values),
    }
}

#[pyfunction]
fn tiny_ints_to_column(values: Vec<Option<i8>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_tiny_ints(values),
    }
}

#[pyfunction]
fn small_ints_to_column(values: Vec<Option<i16>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_small_ints(values),
    }
}

#[pyfunction]
fn ints_to_column(values: Vec<Option<i32>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_ints(values),
    }
}

#[pyfunction]
fn big_ints_to_column(values: Vec<Option<i64>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_big_ints(values),
    }
}

#[pyfunction]
fn unsigned_tiny_ints_to_column(values: Vec<Option<u8>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_unsigned_tiny_ints(values),
    }
}

#[pyfunction]
fn unsigned_small_ints_to_column(values: Vec<Option<u16>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_unsigned_small_ints(values),
    }
}

#[pyfunction]
fn unsigned_ints_to_column(values: Vec<Option<u32>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_unsigned_ints(values),
    }
}

#[pyfunction]
fn unsigned_big_ints_to_column(values: Vec<Option<u64>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_unsigned_big_ints(values),
    }
}

#[pyfunction]
fn floats_to_column(values: Vec<Option<f32>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_floats(values),
    }
}

#[pyfunction]
fn doubles_to_column(values: Vec<Option<f64>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_doubles(values),
    }
}

#[pyfunction]
fn varchar_to_column(values: Vec<Option<String>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_varchar::<
            String,
            Option<String>,
            std::vec::IntoIter<Option<String>>,
            Vec<Option<String>>,
        >(values),
    }
}

#[pyfunction]
fn nchar_to_column(values: Vec<Option<String>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_nchar::<
            String,
            Option<String>,
            std::vec::IntoIter<Option<String>>,
            Vec<Option<String>>,
        >(values),
    }
}

#[pyfunction]
fn json_to_column(values: Vec<Option<String>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_json::<
            String,
            Option<String>,
            std::vec::IntoIter<Option<String>>,
            Vec<Option<String>>,
        >(values),
    }
}

#[pyfunction]
fn binary_to_column(values: Vec<Option<String>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_varchar::<
            String,
            Option<String>,
            std::vec::IntoIter<Option<String>>,
            Vec<Option<String>>,
        >(values),
    }
}

#[pyfunction]
fn varbinary_to_column(values: Vec<Option<Vec<u8>>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_bytes::<
            Vec<u8>,
            Option<Vec<u8>>,
            std::vec::IntoIter<Option<Vec<u8>>>,
            Vec<Option<Vec<u8>>>,
        >(values),
    }
}

#[pyfunction]
fn geometry_to_column(values: Vec<Option<Vec<u8>>>) -> PyColumnView {
    PyColumnView {
        _inner: ColumnView::from_geobytes::<
            Vec<u8>,
            Option<Vec<u8>>,
            std::vec::IntoIter<Option<Vec<u8>>>,
            Vec<Option<Vec<u8>>>,
        >(values),
    }
}

#[pymodule]
fn taosws(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    if std::env::var("RUST_LOG").is_ok() {
        let _ = pretty_env_logger::try_init();
    }
    m.add_class::<Connection>()?;
    m.add_class::<Cursor>()?;
    m.add_class::<TaosField>()?;
    m.add_class::<TaosResult>()?;
    m.add_class::<Consumer>()?;
    m.add_class::<Message>()?;
    m.add_class::<PySchemalessProtocol>()?;
    m.add_class::<PySchemalessPrecision>()?;
    m.add_class::<TaosStmt>()?;
    m.add_class::<PyPrecision>()?;
    m.add_class::<PyTagView>()?;
    m.add_class::<PyColumnView>()?;
    m.add_class::<TaosStmt2>()?;
    m.add_class::<PyStmt2BindParam>()?;

    m.add_function(wrap_pyfunction!(connect, m)?)?;

    m.add_function(wrap_pyfunction!(bool_to_tag, m)?)?;
    m.add_function(wrap_pyfunction!(tiny_int_to_tag, m)?)?;
    m.add_function(wrap_pyfunction!(small_int_to_tag, m)?)?;
    m.add_function(wrap_pyfunction!(int_to_tag, m)?)?;
    m.add_function(wrap_pyfunction!(big_int_to_tag, m)?)?;
    m.add_function(wrap_pyfunction!(float_to_tag, m)?)?;
    m.add_function(wrap_pyfunction!(double_to_tag, m)?)?;
    m.add_function(wrap_pyfunction!(varchar_to_tag, m)?)?;
    m.add_function(wrap_pyfunction!(timestamp_to_tag, m)?)?;
    m.add_function(wrap_pyfunction!(nchar_to_tag, m)?)?;
    m.add_function(wrap_pyfunction!(u_tiny_int_to_tag, m)?)?;
    m.add_function(wrap_pyfunction!(u_small_int_to_tag, m)?)?;
    m.add_function(wrap_pyfunction!(u_int_to_tag, m)?)?;
    m.add_function(wrap_pyfunction!(u_big_int_to_tag, m)?)?;
    m.add_function(wrap_pyfunction!(json_to_tag, m)?)?;
    m.add_function(wrap_pyfunction!(varbinary_to_tag, m)?)?;
    m.add_function(wrap_pyfunction!(geometry_to_tag, m)?)?;

    m.add_function(wrap_pyfunction!(millis_timestamps_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(micros_timestamps_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(nanos_timestamps_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(bools_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(tiny_ints_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(small_ints_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(ints_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(big_ints_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(unsigned_tiny_ints_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(unsigned_small_ints_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(unsigned_ints_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(unsigned_big_ints_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(floats_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(doubles_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(varchar_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(nchar_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(json_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(binary_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(varbinary_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(geometry_to_column, m)?)?;
    m.add_function(wrap_pyfunction!(stmt2_bind_param_view, m)?)?;

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

#[pyfunction]
pub fn schemaless_protocol(protocol: &str) -> PyResult<PySchemalessProtocol> {
    let protocol = match protocol {
        "line" => PySchemalessProtocol::Line,
        "telnet" => PySchemalessProtocol::Telnet,
        "json" => PySchemalessProtocol::Json,
        _ => PySchemalessProtocol::Line,
    };
    Ok(protocol)
}

#[pyfunction]
pub fn schemaless_precision(precision: &str) -> PyResult<PySchemalessPrecision> {
    let precision = match precision {
        "hour" => PySchemalessPrecision::Hour,
        "minute" => PySchemalessPrecision::Minute,
        "second" => PySchemalessPrecision::Second,
        "millisecond" => PySchemalessPrecision::Millisecond,
        "microsecond" => PySchemalessPrecision::Microsecond,
        "Nanosecond" => PySchemalessPrecision::Nanosecond,
        _ => PySchemalessPrecision::Millisecond,
    };
    Ok(precision)
}

#[pyclass]
#[derive(Default, Clone, Debug)]
pub enum PySchemalessProtocol {
    Unknown,
    #[default]
    Line,
    Telnet,
    Json,
}

#[pyclass]
#[derive(Default, Clone, Debug)]
pub enum PySchemalessPrecision {
    NonConfigured,
    Hour,
    Minute,
    Second,
    #[default]
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl Into<SchemalessProtocol> for PySchemalessProtocol {
    fn into(self) -> SchemalessProtocol {
        match self {
            PySchemalessProtocol::Unknown => SchemalessProtocol::Unknown,
            PySchemalessProtocol::Line => SchemalessProtocol::Line,
            PySchemalessProtocol::Telnet => SchemalessProtocol::Telnet,
            PySchemalessProtocol::Json => SchemalessProtocol::Json,
        }
    }
}

impl Into<SchemalessPrecision> for PySchemalessPrecision {
    fn into(self) -> SchemalessPrecision {
        match self {
            PySchemalessPrecision::NonConfigured => SchemalessPrecision::NonConfigured,
            PySchemalessPrecision::Hour => SchemalessPrecision::Hours,
            PySchemalessPrecision::Minute => SchemalessPrecision::Minutes,
            PySchemalessPrecision::Second => SchemalessPrecision::Seconds,
            PySchemalessPrecision::Millisecond => SchemalessPrecision::Millisecond,
            PySchemalessPrecision::Microsecond => SchemalessPrecision::Millisecond,
            PySchemalessPrecision::Nanosecond => SchemalessPrecision::Nanosecond,
        }
    }
}
