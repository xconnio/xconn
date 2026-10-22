use pyo3::{
    exceptions::{PyRuntimeError, PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple},
};
use std::collections::HashMap;
use xconn_binding_runtime::{self as bridge, ByteArray, Value};

fn from_python(value: &Bound<'_, PyAny>, depth: usize) -> PyResult<Value> {
    if depth > 64 {
        return Err(PyValueError::new_err("WAMP values exceed maximum nesting depth of 64"));
    }

    if value.is_none() {
        return Ok(Value::Null);
    }

    if value.is_instance_of::<PyBool>() {
        return Ok(Value::Bool(value.extract()?));
    }

    if value.is_instance_of::<PyInt>() {
        return match value.extract::<i64>() {
            Ok(value) => Ok(Value::Int(value)),
            Err(_) => Ok(Value::UInt(value.extract()?)),
        };
    }

    if value.is_instance_of::<PyFloat>() {
        return Ok(Value::Float(value.extract()?));
    }

    if value.is_instance_of::<PyString>() {
        return Ok(Value::Str(value.extract()?));
    }

    if let Ok(bytes) = value.cast::<PyBytes>() {
        return Ok(Value::Bytes(ByteArray(bytes.as_bytes().to_vec())));
    }

    if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
        return value
            .try_iter()?
            .map(|item| from_python(&item?, depth + 1))
            .collect::<PyResult<Vec<_>>>()
            .map(Value::List);
    }

    if let Ok(dict) = value.cast::<PyDict>() {
        return dict
            .iter()
            .map(|(key, value)| Ok((key.extract::<String>()?, from_python(&value, depth + 1)?)))
            .collect::<PyResult<HashMap<_, _>>>()
            .map(Value::Dict);
    }

    Err(PyTypeError::new_err(
        "expected None, bool, int, float, str, bytes, list, tuple, or dict with string keys",
    ))
}

fn to_python(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    Ok(match value {
        Value::Null => py.None(),
        Value::Bool(value) => value.into_pyobject(py)?.to_owned().into_any().unbind(),
        Value::Int(value) => value.into_pyobject(py)?.into_any().unbind(),
        Value::UInt(value) => value.into_pyobject(py)?.into_any().unbind(),
        Value::Float(value) => value.into_pyobject(py)?.into_any().unbind(),
        Value::Str(value) => value.into_pyobject(py)?.into_any().unbind(),
        Value::Bytes(value) => PyBytes::new(py, &value.0).into_any().unbind(),
        Value::List(values) => PyList::new(
            py,
            values
                .iter()
                .map(|value| to_python(py, value))
                .collect::<PyResult<Vec<_>>>()?,
        )?
        .into_any()
        .unbind(),
        Value::Dict(values) => {
            let dict = PyDict::new(py);

            for (key, value) in values {
                dict.set_item(key, to_python(py, value)?)?;
            }

            dict.into_any().unbind()
        }
    })
}

fn error(message: String) -> PyErr {
    PyRuntimeError::new_err(message)
}

#[pyfunction]
fn submit(request: &Bound<'_, PyAny>) -> PyResult<u64> {
    Ok(bridge::submit(from_python(request, 0)?))
}

#[pyfunction]
fn take(py: Python<'_>, handle: u64) -> PyResult<Py<PyAny>> {
    to_python(py, &bridge::take(handle).map_err(error)?)
}

#[pyfunction]
fn cancel(handle: u64) {
    bridge::cancel(handle);
}

#[pyfunction]
fn execute(py: Python<'_>, request: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let request = from_python(request, 0)?;
    let value = py.detach(move || bridge::execute(request)).map_err(error)?;
    to_python(py, &value)
}

#[pyfunction]
fn wait_async(py: Python<'_>, handle: u64) -> PyResult<Bound<'_, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        bridge::ready(handle).await.map_err(error)?;
        Ok(())
    })
}

#[pyfunction]
fn wait(py: Python<'_>, handle: u64) -> PyResult<()> {
    py.detach(move || bridge::runtime().block_on(bridge::ready(handle)))
        .map_err(error)
}

#[pyfunction]
fn protocol(py: Python<'_>, name: &str, args: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let input = Value::List(vec![Value::Str(name.into()), from_python(args, 0)?]);
    let value = py.detach(move || bridge::protocol::call(input)).map_err(error)?;
    to_python(py, &value)
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = pyo3_async_runtimes::tokio::init_with_runtime(bridge::runtime());
    m.add_function(wrap_pyfunction!(protocol, m)?)?;
    m.add_function(wrap_pyfunction!(submit, m)?)?;
    m.add_function(wrap_pyfunction!(take, m)?)?;
    m.add_function(wrap_pyfunction!(cancel, m)?)?;
    m.add_function(wrap_pyfunction!(execute, m)?)?;
    m.add_function(wrap_pyfunction!(wait_async, m)?)?;
    m.add_function(wrap_pyfunction!(wait, m)?)?;
    Ok(())
}
