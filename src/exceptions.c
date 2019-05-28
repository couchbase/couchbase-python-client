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

int pycbc_handle_assert(const char *msg, const char* file, int line)
{
    const char *assert_props = getenv("PYCBC_ASSERT_CONTINUE");
    if (assert_props == NULL || *assert_props == '\0') {
        fprintf(stderr,
                "python-couchbase: %s at %s:%d. Abort", msg, file, line);
        abort();
    }

    fprintf(stderr,
            "!!! python-couchbase: Assertion failure detected.. \n"
            "!!! Not aborting because os.environ['PYCBC_ASSERT_CONTINUE'] was set\n"
            "!!! Depending on what went wrong, further exceptions may \n"
            "!!! still be raised, or the program may abort due to \n"
            "!!! invalid state\n"
            "!!! (debuggers should break at pycbc_handle_assert in exceptions.c)\n");

    fprintf(stderr, "!!! Assertion: '%s' at %s:%d\n", msg, file, line);
    return 0;
}

void
pycbc_exc_wrap_REAL(int mode, struct pycbc_exception_params *p)
{
    PyObject *type = NULL, *value = NULL, *traceback = NULL;
    PyObject *excls;
    PyObject *excparams;
    PyObject *excinstance;
    PyObject *ctor_args;
    Py_ssize_t excinstance_refcnt;

    PyErr_Fetch(&type, &value, &traceback);
    PyErr_Clear();

    excls = pycbc_exc_map(mode, p->err);

    excparams = PyDict_New();
    pycbc_assert(excparams);

    if (p->err) {
        PyObject *errtmp = pycbc_IntFromL(p->err);
        PyDict_SetItemString(excparams, "rc", errtmp);
        Py_DECREF(errtmp);
    }

    if (type) {
        PyErr_NormalizeException(&type, &value, &traceback);
        PyDict_SetItemString(excparams, "inner_cause", value);
        Py_XDECREF(type);
        Py_XDECREF(value);
    }

    if (p->msg) {
        PyObject *msgstr = pycbc_SimpleStringZ(p->msg);
        PyDict_SetItemString(excparams, "message", msgstr);
        Py_DECREF(msgstr);
    }

    if (p->key) {
        PyDict_SetItemString(excparams, "key", p->key);
    }

    if (p->objextra) {
        PyDict_SetItemString(excparams, "objextra", p->objextra);
    }

    if (p->err_info) {
        PyDict_Update(excparams, p->err_info);
        Py_XDECREF(p->err_info);
        p->err_info = NULL;
    }

    {
        PyObject *csrc_info = Py_BuildValue("(s,i)", p->file, p->line);
        PyDict_SetItemString(excparams, "csrc_info", csrc_info);
        Py_DECREF(csrc_info);
    }

    ctor_args = Py_BuildValue("(O)", excparams);
    excinstance = PyObject_CallObject(excls, ctor_args);
    Py_XDECREF(ctor_args);
    Py_XDECREF(excparams);

    if (!excinstance) {
        Py_XDECREF(traceback);

    } else {
        excinstance_refcnt = Py_REFCNT(excinstance);
        Py_INCREF(Py_TYPE(excinstance));
        PYCBC_STASH_EXCEPTION(
                PYCBC_DEBUG_PYFORMAT("About to raise %R, traceback %R",
                                     pycbc_none_or_value(excinstance),
                                     pycbc_none_or_value(traceback)))
        PyErr_Restore((PyObject*)Py_TYPE(excinstance), excinstance, traceback);
        PYCBC_REFCNT_ASSERT(Py_REFCNT(excinstance) == excinstance_refcnt);
    }
}

PyObject *
pycbc_exc_map(int mode, lcb_STATUS err)
{
    PyObject *ikey;
    PyObject *excls;

    if (mode == PYCBC_EXC_LCBERR) {
        ikey = pycbc_IntFromL(err);
        excls = PyDict_GetItem(pycbc_helpers.lcb_errno_map, ikey);
        if (!excls) {
            excls = PyObject_CallMethod(pycbc_helpers.default_exception,
                                        "rc_to_exctype", "O", ikey);
        }
    } else {
        ikey = pycbc_IntFromL(mode);
        excls = PyDict_GetItem(pycbc_helpers.misc_errno_map, ikey);
    }

    if (!excls) {
        excls = pycbc_helpers.default_exception;
    }

    Py_DECREF(ikey);
    return excls;
}

PyObject *
pycbc_exc_message(int mode, lcb_STATUS err, const char *msg)
{
    PyObject *instance;
    PyObject *args;
    PyObject *excls = pycbc_exc_map(mode, err);

    args = PyTuple_New(1);
    PyTuple_SET_ITEM(args, 0, pycbc_SimpleStringZ(msg));

    instance = PyObject_CallObject(excls, args);
    Py_DECREF(args);

    pycbc_assert(instance);
    return instance;
}

PyObject *
pycbc_exc_get_categories(PyObject *self, PyObject *arg)
{
    int rv = 0;
    int rc = 0;

    (void)self;
    rv = PyArg_ParseTuple(arg, "i", &rc);
    if (!rv) {
        return NULL;
    }
    rv = lcb_get_errtype(rc);
    return pycbc_IntFromL(rv);
}

PyObject *
pycbc_exc_mktuple(void)
{
    PyObject *type, *value, *traceback;
    PyObject *ret;

    pycbc_assert(PyErr_Occurred());

    PyErr_Fetch(&type, &value, &traceback);
    PyErr_Clear();

    if (value == NULL) {
        value = Py_None; Py_INCREF(value);
    }
    if (traceback == NULL) {
        traceback = Py_None; Py_INCREF(traceback);
    }

    ret = PyTuple_New(3);
    /** Steal references from PyErr_Fetch() */
    PyTuple_SET_ITEM(ret, 0, type);
    PyTuple_SET_ITEM(ret, 1, value);
    PyTuple_SET_ITEM(ret, 2, traceback);

    return ret;
}
