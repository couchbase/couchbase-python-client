/**
 *     Copyright 2014 Couchbase, Inc.
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
 * We're only using a subset of commands we actually care about. We're not
 * including the header directly because we might be using constants not
 * defined in older headers (which would result in a runtime
 * ERR_UNSUPPORTED_OPERATION error rather than a compilation failure).
 */
#define CNTL_SET 0x01
#define CNTL_GET 0x00
#define CNTL_OP_TIMEOUT             0x00
#define CNTL_VIEW_TIMEOUT           0x01
#define CNTL_RBUFSIZE               0x02
#define CNTL_WBUFSIZE               0x03
#define CNTL_VBMAP                  0x07
#define CNTL_CONFERRTHRESH          0x0c
#define CNTL_DURABILITY_TIMEOUT     0x0d
#define CNTL_DURABILITY_INTERVAL    0x0e
#define CNTL_HTTP_TIMEOUT           0x0f
#define CNTL_CONFIGURATION_TIMEOUT   0x12
#define CNTL_SKIP_CONFIGURATION_ERRORS_ON_CONNECT 0x13
#define CNTL_RANDOMIZE_BOOTSTRAP_HOSTS 0x14
#define CNTL_CONFIG_CACHE_LOADED 0x15
#define CNTL_MAX_REDIRECTS 0x17
#define CNTL_ENABLE_COLLECTIONS 0x4a

struct vbinfo_st {
    int version;

    union {
        struct {
            /** Input parameters */
            const void *key;
            lcb_size_t nkey;
            /** Output */
            int vbucket;
            int server_index;
        } v0;
    } v;
};

static PyObject *
handle_float_tmo(lcb_t instance,
                 int cmd, int mode, PyObject *val, lcb_STATUS *err)
{
    lcb_uint32_t cval;

    if (val != NULL) {
        if (PyFloat_Check(val)) {
            double dv = PyFloat_AsDouble(val);
            if (dv < 0) {
                PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0,
                               "Timeout cannot be < 0");
                return NULL;
            }
            cval = dv * 1000000;

        } else {
        cval = pycbc_IntAsL(val);
            if (cval == (lcb_uint32_t)-1 && PyErr_Occurred()) {
                PYCBC_EXCTHROW_ARGS();
                return NULL;
            }
        }
    }
    if ( (*err = lcb_cntl(instance, mode, cmd, &cval)) != LCB_SUCCESS) {
        return NULL;
    }

    return pycbc_IntFromUL(cval);
}

static PyObject *
handle_boolean(lcb_t instance,
               int cmd, int mode, PyObject *val, lcb_STATUS *err)
{
    int cval;
    PyObject *ret;

    if (val != NULL) {
        cval = PyObject_IsTrue(val);
    }

    if ( (*err = lcb_cntl(instance, mode, cmd, &cval)) != LCB_SUCCESS) {
        return NULL;
    }

    if (cval) {
        ret = Py_True;
    } else {
        ret = Py_False;
    }
    Py_INCREF(ret);
    return ret;
}

static PyObject *
handle_intval(lcb_t instance,
              int cmd, int mode, PyObject *val, lcb_STATUS *err)
{
    int cval;

    if (val != NULL) {
        cval = pycbc_IntAsL(val);
        if (cval == -1 && PyErr_Occurred()) {
            PYCBC_EXCTHROW_ARGS();
        }
    }

    if ((*err = lcb_cntl(instance, mode, cmd, &cval)) != LCB_SUCCESS) {
        return NULL;
    }

    return pycbc_IntFromL(cval);

}

typedef union {
    float f;
    int i;
    unsigned u;
    lcb_uint32_t u32;
    lcb_size_t sz;
    const char *str;
} uCNTL;

typedef enum {
    CTL_TYPE_INVALID,
    CTL_TYPE_STRING,
    CTL_TYPE_INT,
    CTL_TYPE_SIZET,
    CTL_TYPE_U32,
    CTL_TYPE_FLOAT,
    CTL_TYPE_UNSIGNED,
    CTL_TYPE_TIMEOUT,
    CTL_TYPE_COMPAT
} CTLTYPE;

static CTLTYPE
get_ctltype(const char *s)
{
    if (!strcmp(s, "str") || !strcmp(s, "string")) {
        return CTL_TYPE_STRING;
    }
    if (!strcmp(s, "int")) {
        return CTL_TYPE_INT;
    }
    if (!strcmp(s, "uint") || !strcmp(s, "unsigned")) {
        return CTL_TYPE_UNSIGNED;
    }
    if (!strcmp(s, "size_t") || !strcmp(s, "lcb_size_t")) {
        return CTL_TYPE_SIZET;
    }
    if (!strcmp(s, "float")) {
        return CTL_TYPE_FLOAT;
    }
    if (!strcmp(s, "uint32_t") || !strcmp(s, "lcb_uint32_t")) {
        return CTL_TYPE_U32;
    }
    if (!strcmp(s, "timeout") || !strcmp(s, "interval")) {
        return CTL_TYPE_TIMEOUT;
    }
    return CTL_TYPE_INVALID;
}
/**
 * Convert an input object to a proper pointer target based on the value
 * @param type The type string
 * @param input The input PyObject
 * @param output Target for value
 * @return 1 on success, 0 on failure
 */
static int
convert_object_input(CTLTYPE t, PyObject* input, uCNTL *output)
{
    int rv = 1;
    PyObject *tuple = PyTuple_New(1);
    PyTuple_SET_ITEM(tuple, 0, input);
    Py_INCREF(input);

    if (t == CTL_TYPE_STRING) {
        rv = PyArg_ParseTuple(tuple, "s", &output->str);

    } else if (t == CTL_TYPE_INT) {
        rv = PyArg_ParseTuple(tuple, "i", &output->i);

    } else if (t == CTL_TYPE_UNSIGNED) {
        rv = PyArg_ParseTuple(tuple, "I", &output->u);

    } else if (t == CTL_TYPE_U32) {
        unsigned long tmp = 0;
        rv = PyArg_ParseTuple(tuple, "k", &tmp);
        if (rv) {
            output->u32 = tmp;
        }

    } else if (t == CTL_TYPE_TIMEOUT) {
        double d;
        rv = PyArg_ParseTuple(tuple, "d", &d);
        if (rv) {
            if (d <= 0) {
                PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0,
                    "Cannot set timeout of value <= 0. Use uint32 for that");
                return 0;
            }
            output->u32 = d * 1000000;
        }
    } else if (t == CTL_TYPE_FLOAT) {
        rv = PyArg_ParseTuple(tuple, "f", &output->f);

    } else if (t == CTL_TYPE_SIZET) {
        unsigned long tmp = 0;
        rv = PyArg_ParseTuple(tuple, "k", &tmp);
        if (rv) {
            output->sz = tmp;
        }
    } else {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Bad format for value");
        rv = 0;
    }

    Py_DECREF(tuple);
    return rv;
}

static PyObject *
convert_object_output(CTLTYPE t, void *retval)
{
    if (t == CTL_TYPE_STRING) {
        return PyUnicode_FromString(*(char**)retval);
    } else if (t == CTL_TYPE_UNSIGNED) {
        return pycbc_IntFromUL(*(unsigned*)retval);
    } else if (t == CTL_TYPE_U32) {
        return pycbc_IntFromUL(*(lcb_uint32_t*)retval);
    } else if (t == CTL_TYPE_INT) {
        return pycbc_IntFromL(*(int*)retval);
    } else if (t == CTL_TYPE_TIMEOUT) {
        double d = *(lcb_uint32_t*)retval;
        d /= 1000000;
        return PyFloat_FromDouble(d);
    } else if (t == CTL_TYPE_SIZET) {
        return pycbc_IntFromULL(*(lcb_size_t*)retval);
    } else if (t == CTL_TYPE_FLOAT) {
        return PyFloat_FromDouble(*(float*)retval);
    } else {
        PYCBC_EXC_WRAP(PYCBC_EXC_INTERNAL, 0, "oops");
        return NULL;
    }
}

static PyObject *
handle_old_ctl(pycbc_Bucket *self, int cmd, PyObject *val)
{
    PyObject *ret = NULL;
    int mode = (val == NULL) ? CNTL_GET : CNTL_SET;
    lcb_STATUS err = LCB_SUCCESS;

    switch (cmd) {
    /** Timeout parameters */
    case CNTL_OP_TIMEOUT:
    case CNTL_VIEW_TIMEOUT:
    case CNTL_HTTP_TIMEOUT:
    case CNTL_DURABILITY_INTERVAL:
    case CNTL_DURABILITY_TIMEOUT:
    case CNTL_CONFIGURATION_TIMEOUT: {
        ret = handle_float_tmo(self->instance, cmd, mode, val, &err);
        if (ret) {
            return ret;
        }
        break;
    }

    /** Boolean values */
    case CNTL_SKIP_CONFIGURATION_ERRORS_ON_CONNECT:
    case CNTL_RANDOMIZE_BOOTSTRAP_HOSTS:
    case CNTL_CONFIG_CACHE_LOADED:
    case CNTL_ENABLE_COLLECTIONS: {
        ret = handle_boolean(self->instance, cmd, mode, val, &err);
        break;
    }

    /** int values */
    case CNTL_MAX_REDIRECTS: {
        ret = handle_intval(self->instance, cmd, mode, val, &err);
        break;
    }

    default:
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Couldn't determine type for cntl");
        break;
    }

    if (ret) {
        return ret;
    }

    PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, err, "lcb_cntl failed");
    return NULL;
}

PyObject *
pycbc_Bucket__cntl(pycbc_Bucket *self, PyObject *args, PyObject *kwargs)
{
    int cmd = 0;
    CTLTYPE type = CTL_TYPE_COMPAT;
    const char *argt = NULL;
    PyObject *val = NULL;
    lcb_STATUS err = LCB_SUCCESS;
    uCNTL input;
    if (!self->instance)
    {
        Py_RETURN_NONE;
    }
    char *kwnames[] = { "op", "value", "value_type", NULL };
    if (!PyArg_ParseTupleAndKeywords(
        args, kwargs, "i|Os", kwnames, &cmd, &val, &argt)) {

        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }

    if (argt) {
        type = get_ctltype(argt);
        if (type == CTL_TYPE_INVALID) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Invalid type string");
            return NULL;
        }
    }
    if (type == CTL_TYPE_COMPAT) {
        return handle_old_ctl(self, cmd, val);
    }

    if (val) {
        int rv;
        rv = convert_object_input(type, val, &input);
        if (!rv) {
            return NULL; /* error raised */
        }
        err = lcb_cntl(self->instance, LCB_CNTL_SET, cmd, &input);
        if (err == LCB_SUCCESS) {
            Py_RETURN_TRUE;
        } else {
            PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err, "lcb_cntl: Problem setting value");
            return NULL;
        }
    } else {
        err = lcb_cntl(self->instance, LCB_CNTL_GET, cmd, &input);
        if (err != LCB_SUCCESS) {
            PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err, "lcb_cntl: problem retrieving value");
            return NULL;
        }
        return convert_object_output(type, &input);
    }
}

PyObject *
pycbc_Bucket__cntlstr(pycbc_Bucket *conn, PyObject *args, PyObject *kw)
{
    const char *key, *value;
    lcb_STATUS err=LCB_SUCCESS;
    char *kwlist[] = { "key", "value", NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kw, "ss", kwlist, &key, &value)) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }

    err = lcb_cntl_string(conn->instance, key, value);
    if (err != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err, "Couldn't modify setting");
        return NULL;
    }
    Py_RETURN_NONE;
}

PyObject *
pycbc_Bucket__vbmap(pycbc_Bucket *conn, PyObject *args)
{
    pycbc_strlen_t slen = 0;
    const char *s = NULL;
    PyObject *rtuple;
    struct vbinfo_st info;
    lcb_STATUS err=LCB_SUCCESS;

    if (!PyArg_ParseTuple(args, "s#", &s, &slen)) {
        PYCBC_EXCTHROW_ARGS();
    }

    memset(&info, 0, sizeof(info));
    info.v.v0.key = s;
    info.v.v0.nkey = slen;
    err = lcb_cntl(conn->instance, CNTL_GET, CNTL_VBMAP, &info);
    if (err != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "lcb_cntl failed");
        return NULL;
    }

    rtuple = PyTuple_New(2);
    PyTuple_SET_ITEM(rtuple, 0, pycbc_IntFromL(info.v.v0.vbucket));
    PyTuple_SET_ITEM(rtuple, 1, pycbc_IntFromL(info.v.v0.server_index));
    return rtuple;
}
