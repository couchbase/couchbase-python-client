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

struct storecmd_vars {
    lcb_STORE_OPERATION operation;
    int argopts;
    unsigned int sd_doc_flags;
    unsigned long ttl;
    PyObject *flagsobj;
    lcb_U64 single_cas;
};

struct single_key_context {
    PyObject *value;
    PyObject *flagsobj;
    lcb_uint64_t cas;
    unsigned long ttl;

};

static int
handle_item_kv(pycbc_Item *itm, PyObject *options, const struct storecmd_vars *scv,
    struct single_key_context *skc)
{
    int rv;
    PyObject *ttl_O = NULL, *flagsobj_Oalt = NULL, *igncas_O = NULL;
    PyObject *frag_O = NULL;
    static char *itm_optlist[] = {
            "ttl", "format", "ignore_cas", "fragment", NULL };

    unsigned long itmcas = itm->cas;
    skc->value = itm->value;

    if (options) {
        rv = PyArg_ParseTupleAndKeywords(pycbc_DummyTuple, options, "|OOOO",
            itm_optlist, &ttl_O, &flagsobj_Oalt, &igncas_O, &frag_O);
        if (!rv) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0,
                           "Couldn't parse item options");
            return -1;
        }

        if (ttl_O) {
            if (-1 == pycbc_get_duration(ttl_O, &skc->ttl, 1)) {
                return -1;
            }

            if (!skc->ttl) {
                skc->ttl = scv->ttl;
            }
        }

        if (flagsobj_Oalt && flagsobj_Oalt != Py_None) {
            skc->flagsobj = flagsobj_Oalt;
        }

        if (igncas_O && PyObject_IsTrue(igncas_O)) {
            itmcas = 0;
        }

        if (frag_O == NULL) {
            if (scv->operation == LCB_STORE_APPEND ||
                scv->operation == LCB_STORE_PREPEND) {
                PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "append/prepend must provide options with 'fragment' specifier");
                return -1;
            }

        } else {
            if (scv->operation != LCB_STORE_APPEND &&
                scv->operation != LCB_STORE_PREPEND) {
                PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "'fragment' only valid for append/prepend");
                return -1;
            }

            skc->value = frag_O;
        }

    } else {
        if (scv->operation == LCB_STORE_APPEND ||
            scv->operation == LCB_STORE_PREPEND) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "append/prepend must provide options with 'fragment' specifier");
            return -1;
        }
    }

    if (!skc->value) {
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0, "Value is empty", skc->value);
        return -1;
    }

    skc->cas = itmcas;
    return 0;
}

TRACED_FUNCTION(LCBTRACE_OP_REQUEST_ENCODING,
                static,
                int,
                handle_multi_mutate,
                pycbc_oputil_keyhandler_Bucket *handler,
                pycbc_Collection_t *collection,
                struct pycbc_common_vars *cv,
                int optype,
                PyObject *curkey,
                PyObject *curvalue,
                PyObject *options,
                pycbc_Item *itm,
                void *arg)
{
    pycbc_Bucket *self = collection->bucket;
    int rv=0;
    const struct storecmd_vars *scv = (const struct storecmd_vars *) arg;
    pycbc_pybuffer keybuf = {NULL};
    if (itm) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Item not supported in subdoc mode");
        return -1;
    }

    if (pycbc_tc_encode_key(self, curkey, &keybuf) != 0) {
        return -1;
    }
    CMDSCOPE_NG(SUBDOC, subdoc)
    {
        lcb_cmdsubdoc_cas(cmd, scv->single_cas);
        lcb_cmdsubdoc_expiry(cmd, scv->ttl);
        pycbc_cmdsubdoc_flags_from_scv(scv->sd_doc_flags, cmd);
        PYCBC_CMD_SET_KEY_SCOPE(subdoc, cmd, keybuf);
        rv = PYCBC_TRACE_WRAP(pycbc_sd_handle_speclist,
                              NULL,
                              collection,
                              cv->mres,
                              curkey,
                              curvalue,
                              cmd);
    }
GT_ERR:
GT_DONE:
    PYCBC_PYBUF_RELEASE(&keybuf);
    return rv;
}

TRACED_FUNCTION(LCBTRACE_OP_REQUEST_ENCODING,
                static,
                int,
                handle_single_kv,
                pycbc_oputil_keyhandler_raw_Bucket *handler,
                pycbc_Collection_t *collection,
                struct pycbc_common_vars *cv,
                int optype,
                PyObject *curkey,
                PyObject *curvalue,
                PyObject *options,
                pycbc_Item *itm,
                void *arg)
{
    pycbc_Bucket *self = collection->bucket;
    int rv;
    const struct storecmd_vars *scv = (struct storecmd_vars *) arg;
    struct single_key_context skc = {NULL};
    pycbc_pybuffer keybuf = {NULL}, valbuf = {NULL};
    lcb_STATUS err = LCB_SUCCESS;
    lcb_U32 flags = 0;
    (void)handler;
    if (scv->argopts & PYCBC_ARGOPT_SDMULTI) {
        return PYCBC_TRACE_WRAP(handle_multi_mutate,
                                NULL,
                                NULL,
                                collection,
                                cv,
                                optype,
                                curkey,
                                curvalue,
                                options,
                                itm,
                                arg);
    }

    skc.ttl = scv->ttl;
    skc.flagsobj = scv->flagsobj;
    skc.value = curvalue;
    skc.cas = scv->single_cas;

    rv = pycbc_tc_encode_key(self, curkey, &keybuf);
    if (rv < 0) {
        return -1;
    }

    if (itm) {
        rv = handle_item_kv(itm, options, scv, &skc);
        if (rv < 0) {
            rv = -1;
            goto GT_DONE;
        }
    }

    rv = pycbc_tc_encode_value(self, skc.value, skc.flagsobj, &valbuf, &flags);
    if (rv < 0) {
        rv = -1;
        goto GT_DONE;
    }
    {
        CMDSCOPE_NG_PARAMS(STORE, store, scv->operation)
        {
            lcb_cmdstore_flags(cmd, flags);
            PYCBC_DUR_INIT(err, cmd, store, cv->mres->dur);
            if (err) {
                CMDSCOPE_GENERIC_FAIL(,STORE,store)
            };
            if (scv->operation == LCB_STORE_APPEND ||
                scv->operation == LCB_STORE_PREPEND) {
                /* The server ignores these flags and libcouchbase will throw an
                 * error if the flags are present. We check elsewhere here to
                 * ensure that only UTF8/BYTES are accepted for append/prepend
                 * anyway */
                lcb_cmdstore_flags(cmd, 0);
            }

            PYCBC_CMD_SET_KEY_SCOPE(store, cmd, keybuf);
            PYCBC_CMD_SET_VALUE_SCOPE(store, cmd, valbuf);

            lcb_cmdstore_cas(cmd, skc.cas);
            lcb_cmdstore_expiry(cmd, (uint32_t)skc.ttl);
            lcb_cmdstore_timeout(cmd, cv->timeout);
            PYCBC_TRACECMD_TYPED(store, cmd, context, cv->mres, curkey, self);
            err = pycbc_store(collection, cv->mres, cmd);
        }
    }
GT_ERR:
    PYCBC_DEBUG_LOG_CONTEXT(context, "got result %d", err)
    if (err == LCB_SUCCESS) {
        rv = 0;
    } else {
        rv = -1;
        PYCBC_EXCTHROW_SCHED(err);
    }


    GT_DONE:

        PYCBC_DEBUG_LOG_CONTEXT(context, "got rv %d", rv)
        /* Clean up our encoded keys and values */
        PYCBC_PYBUF_RELEASE(&keybuf);
        PYCBC_PYBUF_RELEASE(&valbuf);
        return rv;
    }

static int
handle_append_flags(pycbc_Bucket *self, PyObject **flagsobj) {
    unsigned long val = 0;

    if (*flagsobj == NULL || *flagsobj == Py_None) {
        *flagsobj = pycbc_helpers.fmt_utf8_flags;
        return 0;
    }

    if (self->tc) {
        return 0; /* let the transcoder handle it */
    }

    val = pycbc_IntAsUL(*flagsobj);
    if (val == (unsigned long) -1) {
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0, "invalid flags", *flagsobj);
        return -1;
    }

    if ((val & PYCBC_FMT_BYTES) == PYCBC_FMT_BYTES) {
        return 0;
    } else if ((val & PYCBC_FMT_UTF8) == PYCBC_FMT_UTF8) {
        return 0;
    }

    PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0, "Only FMT_BYTES and FMT_UTF8 are supported for append/prepend",
                       *flagsobj);
    return -1;

}

TRACED_FUNCTION(LCBTRACE_OP_REQUEST_ENCODING,
                static, PyObject *,
                set_common, pycbc_Bucket *self, PyObject *args, PyObject *kwargs,
                int operation, int argopts) {
    int rv;

    Py_ssize_t ncmds = 0;
    PyObject *ttl_O = NULL;
    PyObject *timeout_O =NULL;
    PyObject *dict = NULL;
    PyObject *key = NULL;
    PyObject *value = NULL;
    pycbc_seqtype_t seqtype = PYCBC_SEQTYPE_GENERIC;
    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;
    struct storecmd_vars scv = { 0 };
    char persist_to = 0, replicate_to = 0;
    pycbc_DURABILITY_LEVEL dur_level = LCB_DURABILITYLEVEL_NONE;
    pycbc_Collection_t collection = pycbc_Collection_as_value(self, kwargs);
    static char *kwlist_multi[] = {"kv",
                                   "ttl",
                                   "format",
                                   "persist_to",
                                   "replicate_to",
                                   "durability_level",
                                   "timeout",
                                   NULL};

    static char *kwlist_single[] = {"key",
                                    "value",
                                    "cas",
                                    "ttl",
                                    "format",
                                    "persist_to",
                                    "replicate_to",
                                    "_sd_doc_flags",
                                    "durability_level",
                                    "timeout",
                                    NULL};

    scv.operation = operation;
    scv.argopts = argopts;

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = PyArg_ParseTupleAndKeywords(args,
                                         kwargs,
                                         "O|OOBBIO",
                                         kwlist_multi,
                                         &dict,
                                         &ttl_O,
                                         &scv.flagsobj,
                                         &persist_to,
                                         &replicate_to,
                                         &dur_level,
                                         &timeout_O);

    } else {
        rv = PyArg_ParseTupleAndKeywords(args,
                                         kwargs,
                                         "OO|KOOBBIIO",
                                         kwlist_single,
                                         &key,
                                         &value,
                                         &scv.single_cas,
                                         &ttl_O,
                                         &scv.flagsobj,
                                         &persist_to,
                                         &replicate_to,
                                         &scv.sd_doc_flags,
                                         &dur_level,
                                         &timeout_O);
    }

    if (!rv) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "couldn't parse arguments");
        goto GT_FAIL;
    }

    rv = pycbc_get_duration(ttl_O, &scv.ttl, 1);
    if (rv < 0) {
        goto GT_FAIL;
    }

    rv = pycbc_get_duration(timeout_O, &cv.timeout, 1);
    if (rv < 0) {
        goto GT_FAIL;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = pycbc_oputil_check_sequence(dict, 0, &ncmds, &seqtype);
        if (rv < 0) {
            goto GT_FAIL;
        }

    } else {
        ncmds = 1;
    }

    if (operation == LCB_STORE_APPEND || operation == LCB_STORE_PREPEND) {
        rv = handle_append_flags(self, &scv.flagsobj);
        if (rv < 0) {
            goto GT_FAIL;
        }

    } else if (scv.flagsobj == NULL || scv.flagsobj == Py_None) {
        scv.flagsobj = self->dfl_fmt;
    }

    rv = pycbc_common_vars_init(&cv, self, argopts, ncmds, 1);
    if (rv < 0) {
        goto GT_FAIL;
    }

    rv = pycbc_handle_durability_args(
            self, &cv.mres->dur, persist_to, replicate_to, dur_level);

    if (rv == 1) {
        cv.mres->mropts |= PYCBC_MRES_F_DURABILITY;

    } else if (rv == -1) {
        goto GT_DONE;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = PYCBC_OPUTIL_ITER_MULTI_COLLECTION(&collection,
                                                seqtype,
                                                dict,
                                                &cv,
                                                0,
                                                handle_single_kv,
                                                &scv,
                                                context);
    } else {
        rv = PYCBC_TRACE_WRAP_NOTERV(handle_single_kv,
                                     kwargs,
                                     0,
                                     &cv,
                                     &context,
                                     self,
                                     NULL,
                                     &collection,
                                     &cv,
                                     0,
                                     key,
                                     value,
                                     NULL,
                                     NULL,
                                     &scv);
#ifndef PYCBC_GLOBAL_SCHED
        if (!rv) {
            cv.sched_cmds++;
        }
#endif
    }

    if (rv < 0) {
        pycbc_wait_for_scheduled(self, kwargs, &context, &cv);
        goto GT_DONE;
    }
    PYCBC_DEBUG_LOG_CONTEXT(context,
                            "Got rv %d, cv.is_seqcmd %d and cv.sched_cmds %d",
                            rv,
                            cv.is_seqcmd,
                            cv.sched_cmds)

    if (-1 == pycbc_common_vars_wait(&cv, self,context)) {
        goto GT_DONE;
    }

    GT_DONE:
    pycbc_common_vars_finalize(&cv, self);
    GT_FINALLY:
        pycbc_Collection_free_unmanaged_contents(&collection);
        return cv.ret;
    GT_FAIL:
        cv.ret = NULL;
        goto GT_FINALLY;
}

#define DECLFUNC(name, operation, mode) \
    PyObject *pycbc_Bucket_##name(pycbc_Bucket *self, \
                                      PyObject *args, PyObject *kwargs) {\
        PyObject* result;\
        PYCBC_TRACE_WRAP_TOPLEVEL(result, LCBTRACE_OP_REQUEST_ENCODING, set_common, self->tracer, self, args, kwargs, operation, mode);\
        return result;\
}

DECLFUNC(upsert_multi, LCB_STORE_UPSERT, PYCBC_ARGOPT_MULTI)
DECLFUNC(insert_multi, LCB_STORE_INSERT, PYCBC_ARGOPT_MULTI)
DECLFUNC(replace_multi, LCB_STORE_REPLACE, PYCBC_ARGOPT_MULTI)

DECLFUNC(append_multi, LCB_STORE_APPEND, PYCBC_ARGOPT_MULTI)
DECLFUNC(prepend_multi, LCB_STORE_PREPEND, PYCBC_ARGOPT_MULTI)

DECLFUNC(upsert, LCB_STORE_UPSERT, PYCBC_ARGOPT_SINGLE)
DECLFUNC(insert, LCB_STORE_INSERT, PYCBC_ARGOPT_SINGLE)
DECLFUNC(replace, LCB_STORE_REPLACE, PYCBC_ARGOPT_SINGLE)

DECLFUNC(append, LCB_STORE_APPEND, PYCBC_ARGOPT_SINGLE)
DECLFUNC(prepend, LCB_STORE_PREPEND, PYCBC_ARGOPT_SINGLE)

DECLFUNC(mutate_in, 0, PYCBC_ARGOPT_SINGLE | PYCBC_ARGOPT_SDMULTI)
