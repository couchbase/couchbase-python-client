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
#include "oputil.h"

PyObject *
pycbc_Bucket__start_pipeline(pycbc_Bucket *self)
{
    if (self->pipeline_queue) {
        PYCBC_EXC_WRAP(PYCBC_EXC_PIPELINE, 0,
                       "A pipeline is already in progress");
        return NULL;
    }

    if (self->flags & PYCBC_CONN_F_ASYNC) {
        PYCBC_EXC_WRAP(PYCBC_EXC_PIPELINE, 0,
                       "Pipeline mode not valid in async handle");
        return NULL;
    }

    self->pipeline_queue = PyList_New(0);
    Py_INCREF(self->pipeline_queue);
    return self->pipeline_queue;
}

PyObject *
pycbc_Bucket__end_pipeline(pycbc_Bucket *self)
{
    PyObject *rv;
    int ii;

    if (!self->pipeline_queue) {
        PYCBC_EXC_WRAP(PYCBC_EXC_PIPELINE, 0,
                       "No pipeline in progress");
        return NULL;
    }

    rv = self->pipeline_queue;

    if (!self->nremaining) {
        goto GT_DONE;
    }

    pycbc_oputil_wait_common(self);

    pycbc_assert(self->nremaining == 0);

    for (ii = 0; ii < PyList_GET_SIZE(self->pipeline_queue); ii++) {
        PyObject *retitem;
        pycbc_MultiResult *mres =
                (pycbc_MultiResult *)PyList_GET_ITEM(self->pipeline_queue, ii);

        if (pycbc_multiresult_maybe_raise(mres)) {
            rv = NULL;
            break;
        }

        /** Returns new reference to something */
        retitem = pycbc_multiresult_get_result(mres);
        if (retitem != (PyObject *)mres) {
            PyList_SetItem(self->pipeline_queue, ii, retitem);
        } else {
            Py_DECREF(mres);
        }
    }


    GT_DONE:
    if (rv) {
        Py_INCREF(rv);
        pycbc_assert(rv == self->pipeline_queue);
    }

    Py_XDECREF(self->pipeline_queue);
    self->pipeline_queue = NULL;

    return rv;
}
