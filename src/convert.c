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

#include "pycbc.h"
/**
 * Conversion functions
 */

/**
 * This is only called if 'o' is not bytes
 */
static PyObject* convert_to_bytesobj(PyObject *o)
{
    PyObject *bytesobj = NULL;
    assert(!PyBytes_Check(o));

    if (PyUnicode_Check(o)) {
        bytesobj = PyUnicode_AsUTF8String(o);
    }

    if (!bytesobj) {
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ENCODING,
                           0, "Couldn't convert object to bytes",
                           o);
    }
    return bytesobj;
}

enum {
    CONVERT_MODE_UTF8_FIRST,
    CONVERT_MODE_UTF8_ONLY,
    CONVERT_MODE_BYTES_ONLY
};

static PyObject *convert_to_string(const char *buf, size_t nbuf, int mode)
{
    PyObject *ret = NULL;

    if (mode == CONVERT_MODE_BYTES_ONLY) {
        goto GT_BYTES;
    }

    ret = PyUnicode_DecodeUTF8(buf, nbuf, "strict");

    if (ret) {
        return ret;
    }

    if (mode == CONVERT_MODE_UTF8_ONLY) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ENCODING, 0, "Couldn't decode as UTF-8");
        return NULL;
    }

    PyErr_Clear();
    GT_BYTES:

    return PyBytes_FromStringAndSize(buf, nbuf);
}

static int encode_common(PyObject **o, void **buf, size_t *nbuf, lcb_uint32_t flags)
{
    PyObject *bytesobj;
    Py_ssize_t plen;

    int rv;

    if ((flags & PYCBC_FMT_UTF8) == PYCBC_FMT_UTF8) {
#if PY_MAJOR_VERSION == 2
        if (PyString_Check(*o)) {
#else
        if (0) {
#endif
            bytesobj = *o;
            Py_INCREF(*o);
        } else {
            if (!PyUnicode_Check(*o)) {
                PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ENCODING,
                                   0, "Must be unicode or string", *o);
                return -1;
            }
            bytesobj = PyUnicode_AsUTF8String(*o);
        }

    } else if ((flags & PYCBC_FMT_BYTES) == PYCBC_FMT_BYTES) {
        if (PyBytes_Check(*o) || PyByteArray_Check(*o)) {
            bytesobj = *o;
            Py_INCREF(*o);

        } else {
            PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ENCODING, 0,
                               "Must be bytes or bytearray", *o);
            return -1;
        }

    } else {
        PyObject *args = NULL;
        PyObject *helper;

        if ((flags & PYCBC_FMT_PICKLE) == PYCBC_FMT_PICKLE) {
            helper = pycbc_helpers.pickle_encode;

        } else if ((flags & PYCBC_FMT_JSON) == PYCBC_FMT_JSON) {
            helper = pycbc_helpers.json_encode;

        } else {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Unrecognized format");
            return -1;
        }

        args = PyTuple_Pack(1, *o);
        bytesobj = PyObject_CallObject(helper, args);
        Py_DECREF(args);

        if (!bytesobj) {
            PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ENCODING,
                               0, "Couldn't encode value", *o);
            return -1;
        }

        if (!PyBytes_Check(bytesobj)) {
            PyObject *old = bytesobj;
            bytesobj = convert_to_bytesobj(old);
            Py_DECREF(old);
            if (!bytesobj) {
                return -1;
            }
        }
    }

    if (PyByteArray_Check(bytesobj)) {
        *buf = PyByteArray_AS_STRING(bytesobj);
        plen = PyByteArray_GET_SIZE(bytesobj);
        rv = 0;

    } else {
        rv = PyBytes_AsStringAndSize(bytesobj, (char**)buf, &plen);
    }

    if (rv < 0) {
        Py_DECREF(bytesobj);
        PYCBC_EXC_WRAP(PYCBC_EXC_ENCODING, 0, "Couldn't encode value");
        return -1;
    }

    *nbuf = plen;
    *o = bytesobj;
    return 0;
}

static int decode_common(PyObject **vp,
                         const char *buf,
                         size_t nbuf,
                         lcb_uint32_t flags)
{
    PyObject *decoded = NULL;

    if ((flags & PYCBC_FMT_UTF8) == PYCBC_FMT_UTF8) {
        decoded = convert_to_string(buf, nbuf, CONVERT_MODE_UTF8_ONLY);
        if (!decoded) {
            return -1;
        }

    } else if ((flags & PYCBC_FMT_BYTES) == PYCBC_FMT_BYTES) {
        GT_BYTES:
        decoded = convert_to_string(buf, nbuf, CONVERT_MODE_BYTES_ONLY);
        assert(decoded);

    } else {
        PyObject *converter = NULL;
        PyObject *args = NULL;
        PyObject *first_arg = NULL;

        if ((flags & PYCBC_FMT_PICKLE) == PYCBC_FMT_PICKLE) {
            converter = pycbc_helpers.pickle_decode;
            first_arg = convert_to_string(buf, nbuf, CONVERT_MODE_BYTES_ONLY);
            assert(first_arg);

        } else if ((flags & PYCBC_FMT_JSON) == PYCBC_FMT_JSON) {
            converter = pycbc_helpers.json_decode;
            first_arg = convert_to_string(buf, nbuf, CONVERT_MODE_UTF8_ONLY);

            if (!first_arg) {
                return -1;
            }

        } else {
            PyErr_Warn(PyExc_UserWarning, "Unrecognized flags. Forcing bytes");
            goto GT_BYTES;
        }

        assert(first_arg);
        args = PyTuple_Pack(1, first_arg);
        decoded = PyObject_CallObject(converter, args);

        Py_DECREF(args);
        Py_DECREF(first_arg);
    }

    if (!decoded) {
        PyObject *bytes_tmp = PyBytes_FromStringAndSize(buf, nbuf);
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ENCODING, 0, "Failed to decode bytes",
                           bytes_tmp);
        Py_XDECREF(bytes_tmp);
        return -1;
    }

    *vp = decoded;
    return 0;
}

int pycbc_tc_simple_encode(PyObject **p,
                           void *buf,
                           size_t *nbuf,
                           lcb_uint32_t flags)
{
    return encode_common(p, buf, nbuf, flags);
}

int pycbc_tc_simple_decode(PyObject **vp,
                           const char *buf,
                           size_t nbuf,
                           lcb_uint32_t flags)
{
    return decode_common(vp, buf, nbuf, flags);
}

int pycbc_tc_encode_key(pycbc_ConnectionObject *conn,
                        PyObject **key,
                        void **buf,
                        size_t *nbuf)
{
    int rv;
    Py_ssize_t plen;

    PyObject *orig_key;
    PyObject *new_key;

    if (!conn->tc) {
        return encode_common(key, buf, nbuf, PYCBC_FMT_UTF8);
    }

    orig_key = *key;
    assert(orig_key);

    new_key = PyObject_CallMethod(conn->tc, "encode_key", "(O)", orig_key);

    if (new_key == NULL) {
        PYCBC_EXC_WRAP_KEY(PYCBC_EXC_ENCODING,
                           0,
                           "Couldn't call encode method",
                           orig_key);
        return -1;
    }

    rv = PyBytes_AsStringAndSize(new_key, (char**)buf, &plen);

    if (rv == -1) {
        PYCBC_EXC_WRAP_KEY(PYCBC_EXC_ENCODING,
                           0,
                           "Couldn't convert encoded key to bytes",
                           new_key);

        Py_XDECREF(new_key);
        return -1;
    }

    *nbuf = plen;
    *key = new_key;
    return 0;
}

int pycbc_tc_decode_key(pycbc_ConnectionObject *conn,
                         const void *key,
                         size_t nkey,
                         PyObject **pobj)
{
    PyObject *bobj;

    if (conn->data_passthrough) {
        bobj = PyBytes_FromStringAndSize(key, nkey);
        *pobj = bobj;

    } else if (!conn->tc) {
        return decode_common(pobj, key, nkey, PYCBC_FMT_UTF8);

    } else {
        bobj = PyBytes_FromStringAndSize(key, nkey);
        *pobj = PyObject_CallMethod(conn->tc, "decode_key", "(O)", bobj);
        Py_XDECREF(bobj);
    }

    if (*pobj == NULL) {
        PYCBC_EXC_WRAP_KEY(PYCBC_EXC_ENCODING, 0, "couldn't decode key", bobj);
        return -1;
    }

    return 0;
}

int pycbc_tc_encode_value(pycbc_ConnectionObject *conn,
                           PyObject **value,
                           PyObject *flag_v,
                           void **buf,
                           size_t *nbuf,
                           lcb_uint32_t *flags)
{
    PyObject *flags_obj;
    PyObject *orig_value;
    PyObject *new_value;
    PyObject *result_tuple;
    unsigned long flags_stackval;
    int rv;
    Py_ssize_t plen;

    orig_value = *value;

    if (!flag_v) {
        flag_v = conn->dfl_fmt;
    }

    if (!conn->tc) {
        lcb_uint32_t flags_priv = pycbc_IntAsUL(flag_v);
        if (flags_priv == -1 && PyErr_Occurred()) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Bad value for flags");
        }
        *flags = flags_priv & PYCBC_FMT_MASK;
        return encode_common(value, buf, nbuf, flags_priv);
    }

    result_tuple = PyObject_CallMethod(conn->tc,
                                       "encode_value",
                                       "(O,O)",
                                       orig_value,
                                       flag_v);
    if (!result_tuple) {
        PYCBC_EXC_WRAP_VALUE(PYCBC_EXC_ENCODING, 0, "couldn't call encode_value",
                             orig_value);
        return -1;
    }

    new_value = PyTuple_GetItem(result_tuple, 0);
    flags_obj = PyTuple_GetItem(result_tuple, 1);

    if (new_value == NULL || flags_obj == NULL) {
        if (!PyErr_Occurred()) {
            PyErr_SetString(PyExc_ValueError, "expected return of (bytes, flags)");
        }
        PYCBC_EXC_WRAP_VALUE(PYCBC_EXC_ENCODING, 0, "Bad return from encode function",
                             orig_value);

        Py_XDECREF(result_tuple);
        return -1;
    }

    flags_stackval = pycbc_IntAsUL(flags_obj);
    if (flags_stackval == -1 && PyErr_Occurred()) {
        Py_XDECREF(result_tuple);
        PYCBC_EXC_WRAP_VALUE(PYCBC_EXC_ENCODING, 0, "Bad type for returned flags",
                             orig_value);
        return -1;
    }

    *flags = flags_stackval;
    rv = PyBytes_AsStringAndSize(new_value, (char**)buf, &plen);
    if (rv == -1) {
        Py_XDECREF(result_tuple);

        PYCBC_EXC_WRAP_VALUE(PYCBC_EXC_ENCODING, 0, "Couldn't convert encoded value to bytes",
                orig_value);
        return -1;
    }

    *value = new_value;
    *nbuf = plen;

    Py_INCREF(new_value);
    Py_XDECREF(result_tuple);

    return 0;
}

int pycbc_tc_decode_value(pycbc_ConnectionObject *conn,
                           const void *value,
                           size_t nvalue,
                           lcb_uint32_t flags,
                           PyObject **pobj)
{
    PyObject *result;
    PyObject *pint;
    PyObject *pbuf;

    if (conn->data_passthrough == 0 && conn->tc == NULL) {
        return decode_common(pobj, value, nvalue, flags);
    }

    if (conn->data_passthrough) {
        *pobj = PyBytes_FromStringAndSize(value, nvalue);
        if (*pobj) {
            return 0;
        }
        return -1;
    }

    pbuf = PyBytes_FromStringAndSize(value, nvalue);
    pint = pycbc_IntFromUL(flags);

    result = PyObject_CallMethod(conn->tc,
                                 "decode_value",
                                 "(O,O)",
                                 pbuf,
                                 pint);
    Py_XDECREF(pint);
    Py_XDECREF(pbuf);

    if (result == NULL) {
        PYCBC_EXC_WRAP_VALUE(PYCBC_EXC_ENCODING, 0, "Couldn't decode value", pbuf);
        return -1;
    }

    *pobj = result;
    return 0;
}
