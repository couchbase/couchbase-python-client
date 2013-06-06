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

static int
handle_single_kv(pycbc_Connection *self,
                 PyObject *curkey,
                 PyObject *curvalue,
                 PyObject *flagsobj,
                 unsigned long ttl,
                 int ii,
                 int operation,
                 struct pycbc_common_vars *cv)
{
    int rv;
    unsigned long cur_ttl = ttl;
    PyObject *opval = NULL;
    lcb_uint64_t cas = 0;
    static char *opt_kwlist[] = { "value", "cas", "ttl", NULL };
    lcb_store_cmd_t *scmd;

    scmd = cv->cmds.store + ii;

    rv = pycbc_tc_encode_key(self,
                             &curkey,
                             (void**) &scmd->v.v0.key,
                             &scmd->v.v0.nkey);
    if (rv < 0) {
        return -1;
    }

    cv->enckeys[ii] = curkey;

    if (PyObject_IsInstance(curvalue, (PyObject*)&pycbc_ArgumentType)) {
        rv = PyArg_ParseTupleAndKeywords(pycbc_DummyTuple, curvalue,
                                         "O|Kk",
                                         opt_kwlist,
                                         &opval,
                                         &cas,
                                         &cur_ttl);
        if (!rv) {
            PYCBC_EXC_WRAP_KEY(PYCBC_EXC_ARGUMENTS,
                               0,
                               "couldn't extract sub-args",
                               curkey);
            return -1;
        }
        if (!cur_ttl) {
            cur_ttl = ttl;
        }
    } else {
        opval = curvalue;
    }

    rv = pycbc_tc_encode_value(self,
                               &opval,
                               flagsobj,
                               (void**)&scmd->v.v0.bytes,
                               &scmd->v.v0.nbytes,
                               &scmd->v.v0.flags);
    if (rv < 0) {
        return -1;
    }

    scmd->v.v0.operation = operation;
    scmd->v.v0.cas = cas;
    scmd->v.v0.exptime = cur_ttl;
    cv->encvals[ii] = opval;
    cv->cmdlist.store[ii] = scmd;
    return 0;
}


static int
handle_append_flags(pycbc_Connection *self, PyObject **flagsobj)
{
    unsigned long val = 0;

    if (*flagsobj == NULL || *flagsobj == Py_None) {
        *flagsobj = pycbc_helpers.fmt_utf8_flags;
        return 0;
    }

    if (self->tc) {
        return 0; /* let the transcoder handle it */
    }

    val = pycbc_IntAsUL(*flagsobj);
    if (val == (unsigned long)-1) {
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0, "invalid flags",
                           *flagsobj);
        return -1;
    }

    if ((val & PYCBC_FMT_BYTES) == PYCBC_FMT_BYTES) {
        return 0;
    } else if ((val & PYCBC_FMT_UTF8) == PYCBC_FMT_UTF8) {
        return 0;
    }

    PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0,
                       "Only FMT_BYTES and FMT_UTF8 are supported "
                       "for append/prepend",
                       *flagsobj);
    return -1;

}

static PyObject *
set_common(pycbc_Connection *self,
           PyObject *args,
           PyObject *kwargs,
           lcb_storage_t operation,
           int argopts)
{
    int rv;
    int ii;
    Py_ssize_t ncmds = 0;
    unsigned long ttl = 0;
    PyObject *ttl_O = NULL;
    Py_ssize_t dictpos = 0;
    lcb_uint64_t single_cas = 0;
    PyObject *dict = NULL;
    PyObject *curkey;
    PyObject *curvalue;
    lcb_error_t err;
    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;

    PyObject *flagsobj = NULL;

    static char *kwlist_multi[] = { "kv", "ttl", "format", NULL };
    static char *kwlist_single[] = { "key", "value", "cas", "ttl", "format", NULL };

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = PyArg_ParseTupleAndKeywords(args,
                                         kwargs,
                                         "O|OO",
                                         kwlist_multi,
                                         &dict,
                                         &ttl_O,
                                         &flagsobj);

    } else {
        rv = PyArg_ParseTupleAndKeywords(args,
                                         kwargs,
                                         "OO|KOO",
                                         kwlist_single,
                                         &curkey,
                                         &curvalue,
                                         &single_cas,
                                         &ttl_O,
                                         &flagsobj);
    }
    if (!rv) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "couldn't parse arguments");
        return NULL;
    }

    rv = pycbc_get_ttl(ttl_O, &ttl, 1);
    if (rv < 0) {
        return NULL;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = pycbc_oputil_check_sequence(dict, 0, &ncmds, NULL);
        if (rv < 0) {
            return NULL;
        }

    } else {
        ncmds = 1;
    }

    if (operation == LCB_APPEND || operation == LCB_PREPEND) {
        rv = handle_append_flags(self, &flagsobj);
        if (rv < 0) {
            return NULL;
        }

    } else if (flagsobj == NULL || flagsobj == Py_None) {
        flagsobj = self->dfl_fmt;
    }

    ii = 0;

    rv = pycbc_common_vars_init(&cv,
                                self,
                                argopts,
                                ncmds,
                                sizeof(lcb_store_cmd_t),
                                1);

    if (rv < 0) {
        return NULL;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        while (PyDict_Next(dict, &dictpos, &curkey, &curvalue)) {
            rv = handle_single_kv(self,
                                  curkey,
                                  curvalue,
                                  flagsobj,
                                  ttl,
                                  ii,
                                  operation,
                                  &cv);
            if (rv < 0) {
                goto GT_DONE;
            }

            ii++;
        }
    } else {
        rv = handle_single_kv(self, curkey, curvalue, flagsobj, ttl, 0, operation, &cv);
        if (rv < 0) {
            goto GT_DONE;
        }
        cv.cmds.store->v.v0.cas = single_cas;
    }

    err = lcb_store(self->instance, cv.mres, ncmds, cv.cmdlist.store);
    if (err != LCB_SUCCESS) {
        PYCBC_EXCTHROW_SCHED(err);
        goto GT_DONE;
    }

    if (-1 == pycbc_common_vars_wait(&cv, self)) {
        goto GT_DONE;
    }

GT_DONE:
    pycbc_common_vars_finalize(&cv);
    return cv.ret;
}

#define DECLFUNC(name, operation, mode) \
    PyObject *pycbc_Connection_##name(pycbc_Connection *self, \
                                      PyObject *args, PyObject *kwargs) { \
    return set_common(self, args, kwargs, operation, mode); \
}

DECLFUNC(set_multi, LCB_SET, PYCBC_ARGOPT_MULTI)
DECLFUNC(add_multi, LCB_ADD, PYCBC_ARGOPT_MULTI)
DECLFUNC(replace_multi, LCB_REPLACE, PYCBC_ARGOPT_MULTI)

DECLFUNC(append_multi, LCB_APPEND, PYCBC_ARGOPT_MULTI)
DECLFUNC(prepend_multi, LCB_PREPEND, PYCBC_ARGOPT_MULTI)

DECLFUNC(set, LCB_SET, PYCBC_ARGOPT_SINGLE)
DECLFUNC(add, LCB_ADD, PYCBC_ARGOPT_SINGLE)
DECLFUNC(replace, LCB_REPLACE, PYCBC_ARGOPT_SINGLE)

DECLFUNC(append, LCB_APPEND, PYCBC_ARGOPT_SINGLE)
DECLFUNC(prepend, LCB_PREPEND, PYCBC_ARGOPT_SINGLE)
