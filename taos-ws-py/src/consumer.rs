use std::{sync::Arc, time::Duration};

use pyo3::{
    prelude::*,
    types::{PyDict, PyList, PyString},
};
use taos::sync::{AsConsumer, TBuilder};
use taos::{
    Address, Data, Dsn, IsOffset, Itertools, MessageSet, Meta, Offset, RawBlock, Timeout,
    TmqBuilder,
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
                let scheme = scheme.downcast::<PyString>().map_err(|_err| {
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

            builder.set("enable.auto.commit", "true");
            builder.set("experimental.snapshot.enable", "false");

            let skip_keys = [
                "protocol",
                "driver",
                "username",
                "user",
                "password",
                "td.connect.websocket.scheme",
                "td.connect.ip",
                "host",
                "td.connect.port",
                "port",
                "td.connect.token",
                "td.connect.user",
                "td.connect.pass",
                "group.id",
            ];

            // enum args and set
            for (key, value) in args.iter() {
                let key_str = key.downcast::<PyString>()?.to_str()?;
                if skip_keys.contains(&key_str) {
                    continue;
                }
                builder.set(key_str, value.extract::<String>()?);
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

    /// return list of topics
    pub fn list_topics(&mut self) -> PyResult<Vec<String>> {
        let topics = self.inner()?.list_topics().unwrap();
        Ok(topics)
    }

    /// Poll for a message with an optional timeout.
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

    /// Commit a offset
    pub fn commit_offset(&mut self, topic: &str, vg_id: i32, offset: i64) -> PyResult<()> {
        self.inner()?.commit_offset(topic, vg_id, offset).unwrap();
        Ok(())
    }

    /// get topics assignment
    pub fn assignment(&mut self) -> PyResult<Option<Vec<TopicAssignment>>> {
        if let Some(assignments) = self.inner()?.assignments() {
            let result = assignments
                .into_iter()
                .map(|(topic, topic_assignments)| {
                    let py_assignments = topic_assignments
                        .into_iter()
                        .map(|item| Assignment {
                            _vg_id: item.vgroup_id(),
                            _offset: item.current_offset(),
                            _begin: item.begin(),
                            _end: item.end(),
                        })
                        .collect();
                    TopicAssignment {
                        _topic: topic,
                        _assignment: py_assignments,
                    }
                })
                .collect();
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    /// seek topic to offset
    pub fn seek(&mut self, topic: &str, vg_id: i32, offset: i64) -> PyResult<()> {
        self.inner()?.offset_seek(topic, vg_id, offset).unwrap();
        Ok(())
    }

    /// return committed
    pub fn committed(&mut self, topic: &str, vg_id: i32) -> PyResult<i64> {
        let offset = self.inner()?.committed(topic, vg_id).unwrap();
        Ok(offset)
    }

    /// return position
    pub fn position(&mut self, topic: &str, vg_id: i32) -> PyResult<i64> {
        let offset = self.inner()?.position(topic, vg_id).unwrap();
        Ok(offset)
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

#[derive(Debug)]
#[pyclass]
pub(crate) struct TopicAssignment {
    _topic: String,
    _assignment: Vec<Assignment>,
}

#[derive(Debug)]
#[pyclass]
pub(crate) struct Assignment {
    _vg_id: i32,
    _offset: i64,
    _begin: i64,
    _end: i64,
}

#[pymethods]
impl TopicAssignment {
    fn topic(&self) -> &str {
        self._topic.as_str()
    }

    fn assignments(&self) -> Vec<Assignment> {
        let mut assignments = Vec::with_capacity(self._assignment.len());
        for _assignment in &self._assignment {
            assignments.push(Assignment {
                _vg_id: _assignment._vg_id,
                _offset: _assignment._offset,
                _begin: _assignment._begin,
                _end: _assignment._end,
            });
        }
        assignments
    }

    fn to_string(&self) -> String {
        let mut s = format!("topic: {},", self._topic);
        for assignment in &self._assignment {
            let ass = format!(
                "vgoup_id: {}, offset: {}, begin: {}, end: {}",
                assignment._vg_id, assignment._offset, assignment._begin, assignment._end
            );
            s.push_str(&ass);
        }
        s
    }
}

#[pymethods]
impl Assignment {
    fn vg_id(&self) -> i32 {
        self._vg_id
    }
    fn offset(&self) -> i64 {
        self._offset
    }
    fn begin(&self) -> i64 {
        self._begin
    }
    fn end(&self) -> i64 {
        self._end
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

            let _consumer = Consumer::new(Some(args), None)?;

            Ok(())
        })
    }
}
