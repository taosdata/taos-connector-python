use std::{sync::Arc, time::Duration};

use pyo3::{
    prelude::*,
    types::{PyDict, PyList, PyString},
};
use taos::{
    sync::AsConsumer, Address, Data, Dsn, IsOffset, Itertools, MessageSet, Meta, Offset, RawBlock,
    taos_query::TBuilder, 
    Timeout, TmqBuilder, 
};

use crate::{
    common::{get_all_of_block, get_row_of_block_unchecked},
    ConsumerException,
};

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
                ConsumerException::new_err(format!("Parse dsn(`{value}`) error: {err}"))
            })?;
        }
        if let Some(args) = conf {
            let mut addr = Address::default();

            if let Some(scheme) = args
                .get_item("td.connect.websocket.scheme")
                .or(args.get_item("protocol"))
                .or(args.get_item("driver"))
            {
                let scheme = scheme.downcast::<PyString>().map_err(|err| {
                    ConsumerException::new_err(format!("Invalid td.connect.websocket.scheme value type: {}, only `'ws'|'wss'` is supported", scheme.get_type().to_string()))
                })?;
                builder.protocol = Some(scheme.to_string())
            }
            match (
                args.get_item("td.connect.ip").or(args.get_item("host")),
                args.get_item("td.connect.port").or(args.get_item("port")),
            ) {
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
                    addr.host.replace("localhost".to_string());
                }
            }
            builder.addresses.push(addr);

            if let Some(value) = args
                .get_item("td.connect.user")
                .or(args.get_item("username"))
                .or(args.get_item("user"))
            {
                builder.username.replace(value.extract()?);
            }
            if let Some(value) = args
                .get_item("td.connect.pass")
                .or(args.get_item("password"))
            {
                builder.password.replace(value.extract()?);
            }
            if let Some(value) = args.get_item("td.connect.token").or(args.get_item("token")) {
                builder.set("token", value.extract::<String>()?);
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

    pub fn unsubscribe(&mut self) {
        if let Some(consumer) = self.0.take() {
            consumer.unsubscribe();
        }
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

    pub fn fetchall(&self) -> Vec<PyObject> {
        Python::with_gil(|py| get_all_of_block(py, &self.block))
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
                let values = unsafe { get_row_of_block_unchecked(py, &slf.block, slf.index) };
                slf.index += 1;
                Ok(Some(values))
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
