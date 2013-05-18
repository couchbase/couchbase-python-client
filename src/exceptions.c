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

void
pycbc_exc_wrap_REAL(int mode, struct pycbc_exception_params *p)
{
    PyObject *type = NULL, *value = NULL, *traceback = NULL;
    PyObject *ikey;
    PyObject *excls;
    PyObject *excparams;
    PyObject *excinstance;
    PyObject *ctor_args;

    PyErr_Fetch(&type, &value, &traceback);

    if (mode == PYCBC_EXC_LCBERR) {
        ikey = pycbc_IntFromL(p->err);
        excls = PyDict_GetItem(pycbc_helpers.lcb_errno_map, ikey);

    } else {
        ikey = pycbc_IntFromL(mode);
        excls = PyDict_GetItem(pycbc_helpers.misc_errno_map, ikey);
    }

    Py_DECREF(ikey);

    if (!excls) {
        excls = pycbc_helpers.default_exception;
    }

    PyErr_Clear();

    excparams = PyDict_New();
    assert(excparams);

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
        Py_INCREF(Py_TYPE(excinstance));
        PyErr_Restore((PyObject*)Py_TYPE(excinstance), excinstance, traceback);
        assert(Py_REFCNT(excinstance) == 1);
    }
}
