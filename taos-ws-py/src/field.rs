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

use pyo3::prelude::*;

use taos::Field;

#[pyclass]
pub(crate) struct TaosField {
    _inner: Field,
}

impl From<Field> for TaosField {
    fn from(value: Field) -> Self {
        Self { _inner: value }
    }
}

impl From<&Field> for TaosField {
    fn from(value: &Field) -> Self {
        Self {
            _inner: value.clone(),
        }
    }
}

impl TaosField {
    #[allow(dead_code)]
    fn new(inner: &Field) -> Self {
        Self {
            _inner: inner.clone(),
        }
    }
}

#[pymethods]
impl TaosField {
    /// Field name.
    fn name(&self) -> &str {
        self._inner.name()
    }

    /// Field type name
    fn r#type(&self) -> &str {
        self._inner.ty().name()
    }

    /// Declaration max-bytes in field.
    fn bytes(&self) -> u32 {
        self._inner.bytes()
    }

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
