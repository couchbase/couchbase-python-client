/**
 *     Copyright 2013 Couchbase, Inc.
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
 **/

/**
 * An optimized Trancoder class. Users may subclass this object and only use
 * some of the transcoder methods
 */
#include "pycbc.h"
#include "structmember.h"

static PyObject *encode_key(PyObject *self, PyObject *args)
{
    int rv;
    char *buf;
    size_t nbuf;
    PyObject *kobj;

    rv = PyArg_ParseTuple(args, "O", &kobj);
    if (!rv) {
        return NULL;
    }

    rv = pycbc_tc_simple_encode(&kobj, &buf, &nbuf, PYCBC_FMT_UTF8);
    if (rv < 0) {
        return NULL;
    }

    (void)self;
    return kobj;
}

static PyObject *decode_key(PyObject *self, PyObject *args)
{
    int rv;
    char *buf;
    PyObject *bobj;
    Py_ssize_t plen;

    rv = PyArg_ParseTuple(args, "O", &bobj);
    if (!rv) {
        return NULL;
    }

    rv = PyBytes_AsStringAndSize(bobj, &buf, &plen);
    if (rv < 0) {
        return NULL;
    }

    rv = pycbc_tc_simple_decode(&bobj, buf, plen, PYCBC_FMT_UTF8);
    if (rv < 0) {
        return NULL;
    }

    (void)self;
    return bobj;
}

static PyObject *encode_value(PyObject *self, PyObject *args)
{
    unsigned long flags;
    int rv;
    PyObject *vobj;
    PyObject *flagsobj;
    char *buf;
    PyObject *ret;
    size_t nbuf;

    rv = PyArg_ParseTuple(args, "OO", &vobj, &flagsobj);
    if (!rv) {
        return NULL;
    }

    rv = pycbc_get_u32(flagsobj, &flags);
    if (rv < 0) {
        return NULL;
    }

    rv = pycbc_tc_simple_encode(&vobj, &buf, &nbuf, flags);
    if (rv < 0) {
        return NULL;
    }

    ret = PyTuple_New(2);
    PyTuple_SET_ITEM(ret, 0, vobj);
    PyTuple_SET_ITEM(ret, 1, flagsobj);

    /** INCREF flags because we got it as an argument */
    Py_INCREF(flagsobj);

    (void)self;
    return ret;
}

static PyObject *decode_value(PyObject *self, PyObject *args)
{
    PyObject *flagsobj;
    PyObject *vobj;
    char *buf;
    Py_ssize_t nbuf;
    int rv;
    unsigned long flags;

    rv = PyArg_ParseTuple(args, "OO", &vobj, &flagsobj);
    if (!rv) {
        return NULL;
    }

    rv = PyBytes_AsStringAndSize(vobj, &buf, &nbuf);
    if (rv < 0) {
        return NULL;
    }

    rv = pycbc_get_u32(flagsobj, &flags);
    if (rv < 0) {
        return NULL;
    }

    rv = pycbc_tc_simple_decode(&vobj, buf, nbuf, flags);
    if (rv < 0) {
        return NULL;
    }

    (void)self;
    return vobj;
}

static PyTypeObject TranscoderType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

typedef struct {
    PyObject_HEAD
} TranscoderObject;

PyDoc_STRVAR(encode_key_doc,
"Encode the key as a bytes object.\n"
"\n"
":param key: This is an object passed as a string key.\n"
"   There is no restriction on this type\n"
"\n"
":return: a bytes object\n"
"   The default implementation encodes the key as UTF-8.\n"
"   On Python 2.x, ``bytes`` is a synonym for ``str``. On Python 3.x,\n"
"   ``bytes`` and ``str`` are distinct objects, in which one must first\n"
"   *encode* a string to a specific encoding\n"
"\n");

PyDoc_STRVAR(decode_key_doc,
"Convert the key from bytes into something else.\n"
"\n"
":param bytes key: The key, in the form of a bytearray\n"
"\n"
":return: a string or other object your application will use\n"
"   The returned key *must* be hashable\n"
"\n"
"The default implementation decodes the keys from UTF-8.\n"
"\n");

PyDoc_STRVAR(encode_value_doc,
"Encode the value into something meaningful\n"
"\n"
":param any value: A value. This may be a string or a complex python\n"
"   object.\n"
":param any format: The `format` argument as passed to the mutator\n"
"\n"
":return: A tuple of ``(value, flags)``\n"
"   ``value`` must be a ``bytes`` object. ``flags`` must be an integer type\n"
"   whose value does not exceed 32 bits\n"
"\n");

PyDoc_STRVAR(decode_value_doc,
"Decode the value from the raw bytes representation into something\n"
"meaningful\n"
"\n"
":param bytes value: Raw bytes, as stored on the server\n"
":param int flags: The flags for the value\n"
"\n"
":return: Something meaningful to be used as a value within the\n"
"   application\n"
"\n");

static PyMethodDef cTranscoder_methods[] = {
        { PYCBC_TCNAME_ENCODE_KEY, (PyCFunction)encode_key,
                METH_VARARGS, encode_key_doc
        },
        { PYCBC_TCNAME_DECODE_KEY, (PyCFunction)decode_key,
                METH_VARARGS, decode_key_doc
        },
        { PYCBC_TCNAME_ENCODE_VALUE, (PyCFunction)encode_value,
                METH_VARARGS, encode_value_doc
        },
        { PYCBC_TCNAME_DECODE_VALUE, (PyCFunction)decode_value,
                METH_VARARGS, decode_value_doc
        },
        { NULL }
};

void transcoder_dealloc(PyObject *o)
{
    Py_TYPE(o)->tp_free(o);
}

int pycbc_TranscoderType_init(PyObject **ptr)
{
    PyTypeObject *p = &TranscoderType;

    *ptr = (PyObject*)p;
    if (p->tp_name) {
        return 0;
    }

    p->tp_name = "Transcoder";
    p->tp_doc = "Efficient, subclassable transcoder interface/class";
    p->tp_dealloc = transcoder_dealloc;
    p->tp_basicsize = sizeof(TranscoderObject);
    p->tp_methods = cTranscoder_methods;
    p->tp_new = PyType_GenericNew;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    return PyType_Ready(p);
}
