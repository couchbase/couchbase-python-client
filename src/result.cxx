/*
 *   Copyright 2016-2022. Couchbase, Inc.
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

#include "client.hxx"
#include "result.hxx"

PyTypeObject result_type = { PyObject_HEAD_INIT(NULL) 0 };

static PyObject*
result__strerror__(result* self, [[maybe_unused]] PyObject* args)
{
    if (self->ec) {
        return PyUnicode_FromString(self->ec.message().c_str());
    }
    Py_RETURN_NONE;
}

static PyObject*
result__err__(result* self, [[maybe_unused]] PyObject* args)
{
    if (self->ec) {
        return PyLong_FromLong(self->ec.value());
    }
    Py_RETURN_NONE;
}

static PyObject*
result__category__(result* self, [[maybe_unused]] PyObject* args)
{
    if (self->ec) {
        return PyUnicode_FromString(self->ec.category().name());
    }
    Py_RETURN_NONE;
}

static PyObject*
result__get__(result* self, PyObject* args)
{
    const char* field_name = nullptr;
    PyObject* default_value = nullptr;

    if (!PyArg_ParseTuple(args, "s|O", &field_name, &default_value)) {
        PyErr_Print();
        PyErr_Clear();
        Py_RETURN_NONE;
    }
    // PyDict_GetItem will return NULL if key doesn't exist; also suppresses errors
    PyObject* val = PyDict_GetItemString(self->dict, field_name);

    if (val == nullptr && default_value == nullptr) {
        Py_RETURN_NONE;
    }
    if (val == nullptr) {
        val = default_value;
    }
    Py_INCREF(val);
    if (default_value != nullptr) {
        Py_XDECREF(default_value);
    }

    return val;
}

static PyObject*
result__str__(result* self)
{
    const char* format_string = "result:{err=%i, err_string=%s, value=%S}";
    return PyUnicode_FromFormat(format_string, self->ec.value(), self->ec.message().c_str(), self->dict);
}

static void
result_dealloc([[maybe_unused]] result* self)
{
    if (self->dict) {
        PyDict_Clear(self->dict);
        Py_DECREF(self->dict);
    }
    // CB_LOG_DEBUG("pycbc - dealloc result: result->refcnt: {}, result->dict->refcnt: {}", Py_REFCNT(self), Py_REFCNT(self->dict));
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject*
result_new(PyTypeObject* type, PyObject*, PyObject*)
{
    result* self = reinterpret_cast<result*>(type->tp_alloc(type, 0));
    self->dict = PyDict_New();
    self->ec = std::error_code();
    return reinterpret_cast<PyObject*>(self);
}

static PyMethodDef result_methods[] = {
    { "strerror", (PyCFunction)result__strerror__, METH_NOARGS, PyDoc_STR("String description of error") },
    { "err", (PyCFunction)result__err__, METH_NOARGS, PyDoc_STR("Integer error code") },
    { "err_category", (PyCFunction)result__category__, METH_NOARGS, PyDoc_STR("error category, expressed as a string") },
    { "get", (PyCFunction)result__get__, METH_VARARGS, PyDoc_STR("get field in result object") },
    { NULL, NULL, 0, NULL }
};

static struct PyMemberDef result_members[] = {
    { "raw_result", T_OBJECT_EX, offsetof(result, dict), 0, PyDoc_STR("Object for the raw result data.\n") },
    { NULL }
};

int
pycbc_result_type_init(PyObject** ptr)
{
    PyTypeObject* p = &result_type;

    *ptr = (PyObject*)p;
    if (p->tp_name) {
        return 0;
    }

    p->tp_name = "pycbc_core.result";
    p->tp_doc = "Result of operation on client";
    p->tp_basicsize = sizeof(result);
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_new = result_new;
    p->tp_dealloc = (destructor)result_dealloc;
    p->tp_methods = result_methods;
    p->tp_members = result_members;
    p->tp_repr = (reprfunc)result__str__;

    return PyType_Ready(p);
}

PyObject*
create_result_obj()
{
    return PyObject_CallObject(reinterpret_cast<PyObject*>(&result_type), nullptr);
}

PyTypeObject mutation_token_type = { PyObject_HEAD_INIT(NULL) 0 };

static void
mutation_token_dealloc([[maybe_unused]] mutation_token* self)
{
    delete self->token;
    // CB_LOG_DEBUG("pycbc - dealloc mutation_token: token->refcnt: {}", Py_REFCNT(self));
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject*
mutation_token_new(PyTypeObject* type, PyObject*, PyObject*)
{
    mutation_token* self = reinterpret_cast<mutation_token*>(type->tp_alloc(type, 0));
    self->token = new couchbase::mutation_token();
    return reinterpret_cast<PyObject*>(self);
}

static PyObject*
mutation_token__get__(mutation_token* self, [[maybe_unused]] PyObject* args)
{
    PyObject* pyObj_mutation_token = PyDict_New();

    PyObject* pyObj_tmp = PyUnicode_FromString(self->token->bucket_name().c_str());
    if (-1 == PyDict_SetItemString(pyObj_mutation_token, "bucket_name", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(self->token->partition_uuid());
    if (-1 == PyDict_SetItemString(pyObj_mutation_token, "partition_uuid", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLongLong(self->token->sequence_number());
    if (-1 == PyDict_SetItemString(pyObj_mutation_token, "sequence_number", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    pyObj_tmp = PyLong_FromUnsignedLong(self->token->partition_id());
    if (-1 == PyDict_SetItemString(pyObj_mutation_token, "partition_id", pyObj_tmp)) {
        PyErr_Print();
        PyErr_Clear();
    }
    Py_XDECREF(pyObj_tmp);

    return pyObj_mutation_token;
}

static PyMethodDef mutation_token_methods[] = {
    { "get", (PyCFunction)mutation_token__get__, METH_NOARGS, PyDoc_STR("get mutation token as dict") },
    { NULL }
};

int
pycbc_mutation_token_type_init(PyObject** ptr)
{
    PyTypeObject* p = &mutation_token_type;

    *ptr = (PyObject*)p;
    if (p->tp_name) {
        return 0;
    }

    p->tp_name = "pycbc_core.mutation_token";
    p->tp_doc = "Object for c++ client mutation token";
    p->tp_basicsize = sizeof(mutation_token);
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_new = mutation_token_new;
    p->tp_dealloc = (destructor)mutation_token_dealloc;
    p->tp_methods = mutation_token_methods;

    return PyType_Ready(p);
}

PyObject*
create_mutation_token_obj(couchbase::mutation_token mt)
{
    PyObject* pyObj_mut = PyObject_CallObject(reinterpret_cast<PyObject*>(&mutation_token_type), nullptr);
    mutation_token* mut_token = reinterpret_cast<mutation_token*>(pyObj_mut);
    auto token = couchbase::mutation_token{ mt.partition_uuid(), mt.sequence_number(), mt.partition_id(), mt.bucket_name() };
    *mut_token->token = token;
    return reinterpret_cast<PyObject*>(mut_token);
}

PyTypeObject streamed_result_type = { PyObject_HEAD_INIT(NULL) 0 };

static void
streamed_result_dealloc([[maybe_unused]] streamed_result* self)
{
    // CB_LOG_DEBUG("pycbc - dealloc streamed_result: result->refcnt: {}", Py_REFCNT(self));
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject*
streamed_result_new(PyTypeObject* type, PyObject*, PyObject*)
{
    streamed_result* self = reinterpret_cast<streamed_result*>(type->tp_alloc(type, 0));
    self->ec = std::error_code();
    self->rows = std::make_shared<rows_queue<PyObject*>>();
    return reinterpret_cast<PyObject*>(self);
}

static PyMethodDef streamed_result_TABLE_methods[] = { { NULL } };

PyObject*
streamed_result_iter(PyObject* self)
{
    Py_INCREF(self);
    return self;
}

PyObject*
streamed_result_iternext(PyObject* self)
{
    streamed_result* s_res = reinterpret_cast<streamed_result*>(self);
    PyObject* row;
    {
        Py_BEGIN_ALLOW_THREADS row = s_res->rows->get(s_res->timeout_ms);
        Py_END_ALLOW_THREADS
    }

    if (row != nullptr) {
        return row;
    } else {
        PyErr_SetString(PyExc_StopIteration, "Timeout occurred waiting for next item in queue.");
        return nullptr;
    }
}

int
pycbc_streamed_result_type_init(PyObject** ptr)
{
    PyTypeObject* p = &streamed_result_type;

    *ptr = (PyObject*)p;
    if (p->tp_name) {
        return 0;
    }

    p->tp_name = "pycbc_core.streamed_result";
    p->tp_doc = "Result of streaming operation on client";
    p->tp_basicsize = sizeof(streamed_result);
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_new = streamed_result_new;
    p->tp_dealloc = (destructor)streamed_result_dealloc;
    p->tp_methods = streamed_result_TABLE_methods;
    p->tp_iter = streamed_result_iter;
    p->tp_iternext = streamed_result_iternext;

    return PyType_Ready(p);
}

streamed_result*
create_streamed_result_obj(std::chrono::milliseconds timeout_ms)
{
    PyObject* pyObj_res = PyObject_CallObject(reinterpret_cast<PyObject*>(&streamed_result_type), nullptr);
    streamed_result* streamed_res = reinterpret_cast<streamed_result*>(pyObj_res);
    streamed_res->timeout_ms = timeout_ms;
    return streamed_res;
}
