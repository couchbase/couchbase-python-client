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

#include "oputil.h"
#include "pycbc.h"

struct arithmetic_common_vars {
    lcb_S64 delta;
    lcb_uint64_t initial;
    unsigned long ttl;
    int create;
};

TRACED_FUNCTION(LCBTRACE_OP_REQUEST_ENCODING,
                static,
                int,
                handle_single_arith,
                pycbc_oputil_keyhandler_raw_Bucket *original,
                pycbc_Collection_t *collection,
                struct pycbc_common_vars *cv,
                int optype,
                PyObject *curkey,
                PyObject *curvalue,
                PyObject *options,
                pycbc_Item *item,
                void *arg)
{
    pycbc_Bucket *self = collection->bucket;
    int rv = 0;
    lcb_STATUS err;
    struct arithmetic_common_vars my_params;
    static const char *kwlist[] = {"delta", "initial", "ttl", NULL};
    pycbc_pybuffer keybuf = { 0 };
    (void)original;
    my_params = *(struct arithmetic_common_vars *)arg;

    (void)item;

    rv = pycbc_tc_encode_key(self, curkey, &keybuf);
    if (rv < 0) {
        return -1;
    }

    if (options) {
        curvalue = options;
    }

    if (curvalue) {
        if (PyDict_Check(curvalue)) {
            PyObject *initial_O = NULL;
            rv = PyArg_ParseTupleAndKeywords(pycbc_DummyTuple,
                                             curvalue,
                                             "L|Ok",
                                             (char **)kwlist,
                                             &my_params.delta,
                                             &initial_O,
                                             &my_params.ttl);
            if (!rv) {
                PYCBC_EXC_WRAP_KEY(PYCBC_EXC_ARGUMENTS, 0, "Couldn't parse parameter for key", curkey);
                rv = -1;
                goto GT_DONE;
            }

            if (initial_O) {
                if (PyNumber_Check(initial_O)) {
                    my_params.create = 1;
                    my_params.initial = pycbc_IntAsULL(initial_O);
                } else {
                    my_params.create = 0;
                }
            }

        } else if (PyNumber_Check(curvalue)) {
            my_params.delta = pycbc_IntAsLL(curvalue);
        } else {
            PYCBC_EXC_WRAP_KEY(PYCBC_EXC_ARGUMENTS, 0, "value for key must be an integer amount or a dict of parameters", curkey);
            return -1;
        }
    }

    {
        CMDSCOPE_NG(COUNTER, counter)
        {
            PYCBC_DEBUG_LOG("Encoding delta %llu", my_params.delta)
            lcb_cmdcounter_delta(cmd, my_params.delta);
            PYCBC_DEBUG_LOG("Encoded delta %llu", my_params.delta)
            PYCBC_DEBUG_LOG_CONTEXT(
                    context, "Encoding initial %d", my_params.initial);
            if (my_params.create) {
                lcb_cmdcounter_initial(cmd, my_params.initial);
            }
            PYCBC_DEBUG_LOG_CONTEXT(
                    context, "Encoding timeout %d", my_params.ttl);
            lcb_cmdcounter_expiration(cmd, my_params.ttl);
            PYCBC_CMD_SET_KEY_SCOPE(counter, cmd, keybuf);
            PYCBC_TRACECMD_TYPED(counter, cmd, context, cv->mres, curkey, self);
            err = pycbc_counter(collection, cv->mres, cmd);
        }
    }
    GT_ERR:
    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_SCHED(err);
        rv = -1;
    } else {
        rv = 0;
    }

    GT_DONE:
    PYCBC_PYBUF_RELEASE(&keybuf);
    return rv;
}

PyObject *arithmetic_common(pycbc_Collection_t *cb_collection,
                            PyObject *args,
                            PyObject *kwargs,
                            int optype,
                            int argopts,
                            pycbc_stack_context_handle context)
{
    pycbc_Bucket *self = cb_collection->bucket;
    int rv;
    Py_ssize_t ncmds;
    struct arithmetic_common_vars global_params = { 0 };
    pycbc_seqtype_t seqtype;
    PyObject *all_initial_O = NULL;
    PyObject *all_ttl_O = NULL;
    PyObject *sequence;
    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;

    static const char *kwlist[] = {"keys", "delta", "initial", "ttl", NULL};

    global_params.delta = 1;

    rv = PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "O|LOO",
                                     (char **)kwlist,
                                     &sequence,
                                     &global_params.delta,
                                     &all_initial_O,
                                     &all_ttl_O);
    if (!rv) {
        PYCBC_EXCTHROW_ARGS();
        return NULL;
    }

    rv = pycbc_get_ttl(all_ttl_O, &global_params.ttl, 1);
    if (rv < 0) {
        return NULL;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = pycbc_oputil_check_sequence(sequence, 1, &ncmds, &seqtype);
        if (rv < 0) {
            return NULL;
        }
    } else {
        ncmds = 1;
    }


    if (all_initial_O && PyNumber_Check(all_initial_O)) {
        global_params.create = 1;
        global_params.initial = pycbc_IntAsULL(all_initial_O);
    }

    rv = pycbc_common_vars_init(&cv, self, argopts, ncmds, 0);

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = PYCBC_OPUTIL_ITER_MULTI_COLLECTION(cb_collection,
                                                seqtype,
                                                sequence,
                                                &cv,
                                                optype,
                                                handle_single_arith,
                                                &global_params,
                                                context);
    } else {
        rv = handle_single_arith(NULL,
                                 cb_collection,
                                 &cv,
                                 optype,
                                 sequence,
                                 NULL,
                                 NULL,
                                 NULL,
                                 &global_params,
                                 context);
    }

    if (rv < 0) {
        pycbc_wait_for_scheduled(self, kwargs, &context, &cv);
        goto GT_DONE;
    }

    if (-1 == pycbc_common_vars_wait(&cv, self, context)) {
        goto GT_DONE;
    }

    GT_DONE:
    pycbc_common_vars_finalize(&cv, self);
    return cv.ret;
}

PyObject *arithmetic_common_bucket(pycbc_Bucket *self,
                                   PyObject *args,
                                   PyObject *kwargs,
                                   int optype,
                                   int argopts,
                                   pycbc_stack_context_handle context)
{
    pycbc_Collection_t cb_collection = pycbc_Collection_as_value(self, kwargs);
    PyObject *result = arithmetic_common(
            &cb_collection, args, kwargs, optype, argopts, context);
    pycbc_Collection_free_unmanaged_contents(&cb_collection);
    return result;
}

#define DECLFUNC(name, operation, mode)                           \
    PyObject *pycbc_Bucket_##name(                                \
            pycbc_Bucket *self, PyObject *args, PyObject *kwargs) \
    {                                                             \
        PyObject *result;                                         \
        PYCBC_TRACE_WRAP_TOPLEVEL(result,                         \
                                  LCBTRACE_OP_REQUEST_ENCODING,   \
                                  arithmetic_common_bucket,       \
                                  self->tracer,                   \
                                  self,                           \
                                  args,                           \
                                  kwargs,                         \
                                  operation,                      \
                                  mode);                          \
        return result;                                            \
    }

DECLFUNC(counter, PYCBC_CMD_COUNTER, PYCBC_ARGOPT_SINGLE)
DECLFUNC(counter_multi, PYCBC_CMD_COUNTER, PYCBC_ARGOPT_MULTI)
