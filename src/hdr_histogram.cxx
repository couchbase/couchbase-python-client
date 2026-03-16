/*
 *   Copyright 2016-2026. Couchbase, Inc.
 *   All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "hdr_histogram.hxx"
#include "exceptions.hxx"
#include "pytype_utils.hxx"

#include <vector>

namespace pycbc
{

namespace
{

/**
 * Helper function to close the histogram safely.
 * Should be called with mutex held.
 */
void
cb_hdr_histogram_close(pycbc_hdr_histogram* hdr_histogram_data)
{
  if (hdr_histogram_data->histogram != nullptr) {
    hdr_close(hdr_histogram_data->histogram);
    hdr_histogram_data->histogram = nullptr;
  }
}

/**
 * Destructor - cleanup histogram resources.
 */
static void
pycbc_hdr_histogram__dealloc__(pycbc_hdr_histogram* self)
{
  if (self->histogram != nullptr) {
    hdr_close(self->histogram);
    self->histogram = nullptr;
  }
  // Explicitly destroy the mutex
  self->mutex.~shared_mutex();
  Py_TYPE(self)->tp_free((PyObject*)self);
}

/**
 * Allocator using placement new pattern.
 */
static PyObject*
pycbc_hdr_histogram__new__(PyTypeObject* type, PyObject*, PyObject*)
{
  auto* self = reinterpret_cast<pycbc_hdr_histogram*>(type->tp_alloc(type, 0));
  if (self != nullptr) {
    // Use placement new to construct the mutex in-place
    new (&self->mutex) std::shared_mutex();
    self->histogram = nullptr;
  }
  return reinterpret_cast<PyObject*>(self);
}

/**
 * __init__(lowest_discernible_value, highest_trackable_value, significant_figures)
 *
 * Initialize the HDR histogram.
 */
static int
pycbc_hdr_histogram__init__(pycbc_hdr_histogram* self, PyObject* args, PyObject* kwargs)
{
  long long lowest_discernible_value = 0;
  long long highest_trackable_value = 0;
  int significant_figures = 0;

  const char* kw_list[] = {
    "lowest_discernible_value", "highest_trackable_value", "significant_figures", nullptr
  };
  const char* kw_format = "LLi";

  if (!PyArg_ParseTupleAndKeywords(args,
                                   kwargs,
                                   kw_format,
                                   const_cast<char**>(kw_list),
                                   &lowest_discernible_value,
                                   &highest_trackable_value,
                                   &significant_figures)) {
    PyErr_SetString(PyExc_ValueError, "Failed to parse arguments for HDR histogram initialization");
    return -1;
  }

  // Validate parameters
  if (lowest_discernible_value < 1) {
    PyErr_SetString(PyExc_ValueError, "lowest_discernible_value must be >= 1");
    return -1;
  }

  if (significant_figures < 1 || significant_figures > 5) {
    PyErr_SetString(PyExc_ValueError, "significant_figures must be between 1 and 5 (inclusive)");
    return -1;
  }

  if (highest_trackable_value < lowest_discernible_value) {
    PyErr_SetString(PyExc_ValueError,
                    "highest_trackable_value must be >= lowest_discernible_value");
    return -1;
  }

  // Initialize the histogram
  int res;
  {
    const std::unique_lock lock(self->mutex);
    res = hdr_init(
      lowest_discernible_value, highest_trackable_value, significant_figures, &self->histogram);
  }

  if (res != 0) {
    if (res == ENOMEM) {
      PyErr_SetString(PyExc_MemoryError, "Failed to allocate memory for HDR histogram");
    } else {
      PyErr_Format(PyExc_RuntimeError, "Failed to initialize HDR histogram (error code: %d)", res);
    }
    return -1;
  }

  return 0;
}

/**
 * close() -> None
 *
 * Close and free the histogram.
 */
static PyObject*
pycbc_hdr_histogram__close__(PyObject* self, PyObject* Py_UNUSED(ignored))
{
  auto* hdr_histogram = reinterpret_cast<pycbc_hdr_histogram*>(self);
  {
    const std::unique_lock lock(hdr_histogram->mutex);
    cb_hdr_histogram_close(hdr_histogram);
  }
  Py_RETURN_NONE;
}

/**
 * record_value(value: int) -> None
 *
 * Record a value atomically in the histogram.
 */
static PyObject*
pycbc_hdr_histogram__record_value__(PyObject* self, PyObject* value)
{
  if (!PyLong_Check(value)) {
    PyErr_SetString(PyExc_TypeError, "value must be an integer");
    return nullptr;
  }

  long long val = PyLong_AsLongLong(value);
  if (val == -1 && PyErr_Occurred()) {
    return nullptr;
  }

  auto* hdr_histogram = reinterpret_cast<pycbc_hdr_histogram*>(self);

  {
    const std::shared_lock lock(hdr_histogram->mutex);
    if (hdr_histogram->histogram == nullptr) {
      PyErr_SetString(PyExc_RuntimeError, "Histogram is not initialized or has been closed");
      return nullptr;
    }
    hdr_record_value_atomic(hdr_histogram->histogram, val);
  }

  Py_RETURN_NONE;
}

/**
 * value_at_percentile(percentile: float) -> int
 *
 * Get the value at a given percentile.
 */
static PyObject*
pycbc_hdr_histogram__value_at_percentile__(PyObject* self, PyObject* percentile)
{
  double perc = 0.0;

  if (PyFloat_Check(percentile)) {
    perc = PyFloat_AsDouble(percentile);
  } else if (PyLong_Check(percentile)) {
    perc = static_cast<double>(PyLong_AsLongLong(percentile));
  } else {
    PyErr_SetString(PyExc_TypeError, "percentile must be a float or int");
    return nullptr;
  }

  if (perc < 0.0 || perc > 100.0) {
    PyErr_SetString(PyExc_ValueError, "percentile must be between 0.0 and 100.0");
    return nullptr;
  }

  auto* hdr_histogram = reinterpret_cast<pycbc_hdr_histogram*>(self);

  int64_t value_at_perc;
  {
    const std::shared_lock lock(hdr_histogram->mutex);
    if (hdr_histogram->histogram == nullptr) {
      PyErr_SetString(PyExc_RuntimeError, "Histogram is not initialized or has been closed");
      return nullptr;
    }
    value_at_perc = hdr_value_at_percentile(hdr_histogram->histogram, perc);
  }

  return PyLong_FromLongLong(value_at_perc);
}

/**
 * get_percentiles_and_reset(percentiles: List[float]) -> Dict[str, Union[int, List[int]]]
 *
 * Get multiple percentile values and reset the histogram atomically.
 * Returns a dict with 'total_count' and 'percentiles' keys.
 */
static PyObject*
pycbc_hdr_histogram__get_percentiles_and_reset__(PyObject* self, PyObject* percentiles)
{
  if (!PyList_Check(percentiles)) {
    PyErr_SetString(PyExc_TypeError, "percentiles must be a list");
    return nullptr;
  }

  Py_ssize_t num_percentiles = PyList_Size(percentiles);
  if (num_percentiles == 0) {
    PyErr_SetString(PyExc_ValueError, "percentiles list cannot be empty");
    return nullptr;
  }

  std::vector<double> input_percentiles;
  input_percentiles.reserve(num_percentiles);

  for (Py_ssize_t i = 0; i < num_percentiles; ++i) {
    PyObject* entry = PyList_GetItem(percentiles, i);
    if (entry == nullptr) {
      return nullptr;
    }

    double perc = 0.0;
    if (PyFloat_Check(entry)) {
      perc = PyFloat_AsDouble(entry);
    } else if (PyLong_Check(entry)) {
      perc = static_cast<double>(PyLong_AsLongLong(entry));
    } else {
      PyErr_SetString(PyExc_TypeError, "percentile values must be float or int");
      return nullptr;
    }

    if (perc < 0.0 || perc > 100.0) {
      PyErr_Format(PyExc_ValueError, "percentile at index %zd must be between 0.0 and 100.0", i);
      return nullptr;
    }

    input_percentiles.push_back(perc);
  }

  auto* hdr_histogram = reinterpret_cast<pycbc_hdr_histogram*>(self);
  std::vector<int64_t> output_percentiles;
  output_percentiles.reserve(num_percentiles);
  int64_t total_count = 0;

  {
    const std::unique_lock lock(hdr_histogram->mutex);
    if (hdr_histogram->histogram == nullptr) {
      PyErr_SetString(PyExc_RuntimeError, "Histogram closed");
      return nullptr;
    }

    total_count = hdr_histogram->histogram->total_count;
    for (double perc : input_percentiles) {
      output_percentiles.push_back(hdr_value_at_percentile(hdr_histogram->histogram, perc));
    }
    hdr_reset(hdr_histogram->histogram);
  }

  PyObject* result = PyDict_New();
  if (result == nullptr) {
    return nullptr;
  }

  // total count
  PyObject* py_total_count = PyLong_FromLongLong(total_count);
  if (py_total_count == nullptr) {
    Py_DECREF(result);
    return nullptr;
  }
  if (PyDict_SetItemString(result, "total_count", py_total_count) < 0) {
    Py_DECREF(py_total_count);
    Py_DECREF(result);
    return nullptr;
  }
  Py_DECREF(py_total_count);

  // percentiles
  PyObject* pyObj_percentiles = PyList_New(output_percentiles.size());
  if (pyObj_percentiles == nullptr) {
    Py_DECREF(result);
    return nullptr;
  }
  for (size_t i = 0; i < output_percentiles.size(); ++i) {
    PyObject* val = PyLong_FromLongLong(output_percentiles[i]);
    if (val == nullptr) {
      Py_DECREF(pyObj_percentiles);
      Py_DECREF(result);
      return nullptr;
    }
    PyList_SET_ITEM(pyObj_percentiles, i, val); // Steals reference to val
  }
  if (PyDict_SetItemString(result, "percentiles", pyObj_percentiles) < 0) {
    Py_DECREF(pyObj_percentiles);
    Py_DECREF(result);
    return nullptr;
  }
  Py_DECREF(pyObj_percentiles);

  return result;
}

/**
 * reset() -> None
 *
 * Reset the histogram, clearing all recorded values.
 */
static PyObject*
pycbc_hdr_histogram__reset__(PyObject* self, PyObject* Py_UNUSED(ignored))
{
  auto* hdr_histogram = reinterpret_cast<pycbc_hdr_histogram*>(self);
  {
    const std::unique_lock lock(hdr_histogram->mutex);
    if (hdr_histogram->histogram == nullptr) {
      PyErr_SetString(PyExc_RuntimeError, "Histogram is not initialized or has been closed");
      return nullptr;
    }
    hdr_reset(hdr_histogram->histogram);
  }
  Py_RETURN_NONE;
}

/**
 * Method definitions for pycbc_hdr_histogram.
 */
static PyMethodDef pycbc_hdr_histogram_methods[] = {
  { "close",
    (PyCFunction)pycbc_hdr_histogram__close__,
    METH_NOARGS,
    PyDoc_STR("Close and free the histogram") },
  { "record_value",
    (PyCFunction)pycbc_hdr_histogram__record_value__,
    METH_O,
    PyDoc_STR("Record a value atomically in the histogram") },
  { "value_at_percentile",
    (PyCFunction)pycbc_hdr_histogram__value_at_percentile__,
    METH_O,
    PyDoc_STR("Get the value at a given percentile (0.0-100.0)") },
  { "get_percentiles_and_reset",
    (PyCFunction)pycbc_hdr_histogram__get_percentiles_and_reset__,
    METH_O,
    PyDoc_STR("Get multiple percentiles and reset the histogram atomically") },
  { "reset",
    (PyCFunction)pycbc_hdr_histogram__reset__,
    METH_NOARGS,
    PyDoc_STR("Reset the histogram, clearing all recorded values") },
  { nullptr }
};

/**
 * Initialize the type object.
 */
static PyTypeObject
init_pycbc_hdr_histogram_type()
{
  PyTypeObject obj = {};
  obj.ob_base = PyVarObject_HEAD_INIT(NULL, 0) obj.tp_name = "pycbc_core.pycbc_hdr_histogram";
  obj.tp_doc =
    PyDoc_STR("HDR (High Dynamic Range) Histogram for recording and analyzing value distributions");
  obj.tp_basicsize = sizeof(pycbc_hdr_histogram);
  obj.tp_itemsize = 0;
  obj.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  obj.tp_new = pycbc_hdr_histogram__new__;
  obj.tp_init = (initproc)pycbc_hdr_histogram__init__;
  obj.tp_dealloc = (destructor)pycbc_hdr_histogram__dealloc__;
  obj.tp_methods = pycbc_hdr_histogram_methods;
  return obj;
}

static PyTypeObject pycbc_hdr_histogram_type = init_pycbc_hdr_histogram_type();

} // anonymous namespace

/**
 * Register the pycbc_hdr_histogram type with the module.
 */
int
add_histogram_objects(PyObject* module)
{
  if (register_pytype(module, &pycbc_hdr_histogram_type, "pycbc_hdr_histogram") < 0) {
    return -1;
  }
  return 0;
}

} // namespace pycbc
