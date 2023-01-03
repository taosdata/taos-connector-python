use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Context;
use pyo3::{
    ffi::PyLongObject,
    prelude::*,
    types::{PyDict, PyList, PyString, PyTuple},
};
use taos::{
    sync::{AsConsumer, Fetchable, Queryable},
    Address, BorrowedValue, Data, Dsn, IsOffset, Itertools, MessageSet, Meta, Offset, RawBlock,
    TBuilder, Timeout, TmqBuilder,
};

use crate::ConsumerException;

#[pyclass]
pub(crate) struct Consumer(Option<taos::Consumer>);

impl Drop for Consumer {
    fn drop(&mut self) {
        self.close()
    }
}

#[pyclass]
pub(crate) struct Message {
    _offset: Option<Offset>,
    _msg: MessageSet<Meta, Data>,
}

impl Consumer {
    fn inner(&mut self) -> PyResult<&mut taos::Consumer> {
        Ok(self
            .0
            .as_mut()
            .ok_or_else(|| ConsumerException::new_err("consumer has been already closed"))?)
    }
}

#[pymethods]
impl Consumer {
    #[new]
    pub fn new(conf: Option<&PyDict>, dsn: Option<&str>) -> PyResult<Self> {
        let mut builder = Dsn::default();
        builder.driver = "taos".to_string();
        if let Some(value) = dsn {
            builder = value.parse().map_err(|err| {
                ConsumerException::new_err(format!("parse dsn(`{value}`) error: {err}"))
            })?;
        }
        if let Some(args) = conf {
            let mut addr = Address::default();

            if let Some(scheme) = args.get_item("td.connect.websocket.scheme") {
                let scheme = scheme.downcast::<PyString>().map_err(|err| {
                    ConsumerException::new_err(format!("Invalid td.connect.websocket.scheme value type: {}, only `'ws'|'wss'` is supported", scheme.get_type().to_string()))
                })?;
                builder.protocol = Some(scheme.to_string())
            }
            match (
                args.get_item("td.connect.ip"),
                args.get_item("td.connect.port"),
            ) {
                (Some(host), Some(port)) => {
                    addr.host
                        .replace(host.cast_as::<PyString>()?.to_str()?.to_string());
                    addr.port
                        .replace(port.cast_as::<pyo3::types::PyInt>()?.extract()?);
                }
                (Some(host), None) => {
                    addr.host
                        .replace(host.cast_as::<PyString>()?.to_str()?.to_string());
                }
                (_, Some(port)) => {
                    addr.port
                        .replace(port.cast_as::<pyo3::types::PyInt>()?.extract()?);
                }
                _ => {
                    addr.host.replace("localhost".to_string());
                }
            }
            builder.addresses.push(addr);

            if let Some(value) = args.get_item("td.connect.user") {
                builder.username.replace(value.extract()?);
            }
            if let Some(value) = args.get_item("td.connect.pass") {
                builder.password.replace(value.extract()?);
            }

            if let Some(value) = args.get_item("group.id") {
                builder.set("group.id", value.extract::<String>()?);
            } else {
                Err(ConsumerException::new_err(
                    "group.id must be set in configurations",
                ))?;
            }

            const KEYS: &[&str] = &[
                "client.id",
                "auto.offset.reset",
                "enable.auto.commit",
                "auto.commit.interval.ms",
                "enable.heartbeat.background",
                "experimental.snapshot.enable",
            ];
            for key in KEYS {
                if let Some(value) = args.get_item(key) {
                    builder.set(*key, value.extract::<String>()?);
                }
            }
        }
        dbg!(&builder);
        let builder = TmqBuilder::from_dsn(builder)
            .map_err(|err| ConsumerException::new_err(err.to_string()))?;
        Ok(Consumer(Some(builder.build().map_err(|err| {
            ConsumerException::new_err(err.to_string())
        })?)))
    }

    pub fn subscribe(&mut self, topics: &PyList) -> PyResult<()> {
        self.inner()?
            .subscribe(topics.extract::<Vec<String>>()?)
            .map_err(|err| ConsumerException::new_err(format!("{err}")))
    }
    ///
    pub fn poll(&mut self, timeout: Option<f64>) -> PyResult<Option<Message>> {
        let timeout = if let Some(timeout) = timeout {
            Timeout::Duration(Duration::from_secs_f64(timeout))
        } else {
            Timeout::Never
        };
        let message = self
            .inner()?
            .recv_timeout(timeout)
            .map_err(|err| ConsumerException::new_err(format!("{err}")))?;

        if let Some((offset, message)) = message {
            Ok(Some(Message {
                _offset: Some(offset),
                _msg: message,
            }))
        } else {
            Ok(None)
        }
    }

    /// Commit a `message`.
    pub fn commit(&mut self, message: &mut Message) -> PyResult<()> {
        self.inner()?
            .commit(message._offset.take().unwrap())
            .unwrap();
        Ok(())
    }

    /// Unsubscribe and close the consumer.
    pub fn close(&mut self) {
        if let Some(consumer) = self.0.take() {
            consumer.unsubscribe();
        }
    }
}

#[pymethods]
impl Message {
    /// VGroup id, which is similar to kafka [confluent_kafka.Message.partition()][kafka.Message.partition].
    ///
    /// kafka.Message.partition: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Message.partition
    pub fn vgroup(&self) -> i32 {
        self._offset.as_ref().unwrap().vgroup_id()
    }

    #[deprecated]
    pub fn get_vgroup_id(&self) -> i32 {
        println!("# get_vgroup_id is deprecated, use vgroup() instead.");
        self._offset.as_ref().unwrap().vgroup_id()
    }
    /// Topic name.
    pub fn topic(&self) -> &str {
        self._offset.as_ref().unwrap().topic()
    }

    /// Database name.
    pub fn database(&self) -> &str {
        self._offset.as_ref().unwrap().database()
    }

    pub fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }
    pub fn __next__(mut slf: PyRefMut<Self>) -> PyResult<Option<MessageBlock>> {
        if let Some(data) = slf._msg.data() {
            let block = data
                .next()
                .transpose()
                .map_err(|err| ConsumerException::new_err(format!("{err}")))?;
            if let Some(block) = block {
                Ok(Some(MessageBlock {
                    block: Arc::new(block),
                }))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}
#[pyclass]
pub(crate) struct MessageBlock {
    block: Arc<RawBlock>,
}

#[pymethods]
impl MessageBlock {
    pub fn fields(&self) -> Vec<super::TaosField> {
        self.block
            .fields()
            .into_iter()
            .map(|f| f.into())
            .collect_vec()
    }

    pub fn nrows(&self) -> usize {
        self.block.nrows()
    }

    pub fn ncols(&self) -> usize {
        self.block.ncols()
    }

    pub fn fetchall(&self) -> Py<PyAny> {
        Python::with_gil(|py| {
            let values = self.block.to_values();
            let mut vec = Vec::new();
            for row in values {
                let tuple = PyTuple::new(
                    py,
                    row.into_iter().map(|val| match val {
                        taos::Value::Null(_) => Option::<()>::None.into_py(py),
                        taos::Value::Bool(v) => v.into_py(py),
                        taos::Value::TinyInt(v) => v.into_py(py),
                        taos::Value::SmallInt(v) => v.into_py(py),
                        taos::Value::Int(v) => v.into_py(py),
                        taos::Value::BigInt(v) => v.into_py(py),
                        taos::Value::Float(v) => v.into_py(py),
                        taos::Value::Double(v) => v.into_py(py),
                        taos::Value::VarChar(v) => v.into_py(py),
                        taos::Value::Timestamp(v) => v.to_datetime_with_tz().into_py(py),
                        taos::Value::NChar(v) => v.into_py(py),
                        taos::Value::UTinyInt(v) => v.into_py(py),
                        taos::Value::USmallInt(v) => v.into_py(py),
                        taos::Value::UInt(v) => v.into_py(py),
                        taos::Value::UBigInt(v) => v.into_py(py),
                        taos::Value::Json(v) => v.to_string().into_py(py),
                        taos::Value::VarBinary(_) => todo!(),
                        taos::Value::Decimal(_) => todo!(),
                        taos::Value::Blob(_) => todo!(),
                        taos::Value::MediumBlob(_) => todo!(),
                    }),
                );
                vec.push(tuple);
            }

            vec.into_py(py)
        })
    }

    pub fn __iter__(slf: PyRef<Self>) -> MessageBlockIter {
        MessageBlockIter {
            block: slf.block.clone(),
            index: 0,
        }
    }
}

/// An iterable object of Block.
#[pyclass]
pub(crate) struct MessageBlockIter {
    block: Arc<RawBlock>,
    index: usize,
}

#[pymethods]
impl MessageBlockIter {
    pub fn __next__(mut slf: PyRefMut<Self>) -> PyResult<Option<Py<PyAny>>> {
        if slf.index >= slf.block.nrows() {
            Ok(None)
        } else {
            Python::with_gil(|py| {
                let values = (0..slf.block.ncols())
                    .map(|col| match slf.block.get_ref(slf.index, col).unwrap() {
                        BorrowedValue::Null(_) => Option::<()>::None.into_py(py),
                        BorrowedValue::Bool(v) => v.into_py(py),
                        BorrowedValue::TinyInt(v) => v.into_py(py),
                        BorrowedValue::SmallInt(v) => v.into_py(py),
                        BorrowedValue::Int(v) => v.into_py(py),
                        BorrowedValue::BigInt(v) => v.into_py(py),
                        BorrowedValue::Float(v) => v.into_py(py),
                        BorrowedValue::Double(v) => v.into_py(py),
                        BorrowedValue::VarChar(v) => v.into_py(py),
                        BorrowedValue::Timestamp(v) => v.to_datetime_with_tz().into_py(py),
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
                        BorrowedValue::VarBinary(_) => todo!(),
                        BorrowedValue::Decimal(_) => todo!(),
                        BorrowedValue::Blob(_) => todo!(),
                        BorrowedValue::MediumBlob(_) => todo!(),
                    })
                    .collect_vec();
                slf.index += 1;
                Ok(Some(values.into_py(py)))
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use pyo3::{prelude::*, types::IntoPyDict};

    use super::Consumer;
    #[test]
    fn consumer_demo() -> PyResult<()> {
        Python::with_gil(|py| {
            let mut args = HashMap::new();
            args.insert("td.connect.ip", "localhost");
            args.insert("td.connect.user", "root");
            args.insert("td.connect.pass", "taosdata");
            args.insert("td.connect.port", "6041");
            args.insert("td.connect.websocket.scheme", "ws");
            args.insert("group.id", "0");
            let args = args.into_py_dict(py);

            let consumer = Consumer::new(Some(args), None)?;

            Ok(())
        })
    }
}
