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

struct storecmd_vars {
    int operation;
    int argopts;
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

    lcb_cas_t itmcas = itm->cas;
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
            if (-1 == pycbc_get_ttl(ttl_O, &skc->ttl, 1)) {
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
            if (scv->operation == LCB_APPEND || scv->operation == LCB_PREPEND) {
                PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "append/prepend must provide options with 'fragment' specifier");
                return -1;
            }

        } else {
            if (scv->operation != LCB_APPEND && scv->operation != LCB_PREPEND) {
                PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "'fragment' only valid for append/prepend");
                return -1;
            }

            skc->value = frag_O;
        }

    } else {
        if (scv->operation == LCB_APPEND || scv->operation == LCB_PREPEND) {
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

static int
handle_multi_mutate(pycbc_Bucket *self, struct pycbc_common_vars *cv, int optype,
    PyObject *curkey, PyObject *curvalue, PyObject *options, pycbc_Item *itm,
    void *arg);

static int
handle_single_kv(pycbc_Bucket *self, struct pycbc_common_vars *cv, int optype,
    PyObject *curkey, PyObject *curvalue, PyObject *options, pycbc_Item *itm,
    void *arg)
{
    int rv;
    const struct storecmd_vars *scv = (struct storecmd_vars *)arg;
    struct single_key_context skc = { NULL };
    pycbc_pybuffer keybuf = { NULL }, valbuf = { NULL };
    lcb_error_t err;
    lcb_CMDSTORE cmd = { 0 };

    if (scv->argopts & PYCBC_ARGOPT_SDMULTI) {
        return handle_multi_mutate(self, cv, optype, curkey, curvalue, options, itm, arg);
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

    rv = pycbc_tc_encode_value(self, skc.value, skc.flagsobj,
        &valbuf, &cmd.flags);
    if (rv < 0) {
        rv = -1;
        goto GT_DONE;
    }

    if (scv->operation == LCB_APPEND || scv->operation == LCB_PREPEND) {
        /* The server ignores these flags and libcouchbase will throw an error
         * if the flags are present. We check elsewhere here to ensure that
         * only UTF8/BYTES are accepted for append/prepend anyway */
        cmd.flags = 0;
    }

    LCB_CMD_SET_KEY(&cmd, keybuf.buffer, keybuf.length);
    LCB_CMD_SET_VALUE(&cmd, valbuf.buffer, valbuf.length);
    cmd.cas = skc.cas;
    cmd.operation = scv->operation;
    cmd.exptime = skc.ttl;
    err = lcb_store3(self->instance, cv->mres, &cmd);
    if (err == LCB_SUCCESS) {
        rv = 0;
    } else {
        rv = -1;
        PYCBC_EXCTHROW_SCHED(err);
    }


    GT_DONE:
    /* Clean up our encoded keys and values */
    PYCBC_PYBUF_RELEASE(&keybuf);
    PYCBC_PYBUF_RELEASE(&valbuf);
    return rv;
}

static int
handle_multi_mutate(pycbc_Bucket *self, struct pycbc_common_vars *cv, int optype,
    PyObject *curkey, PyObject *curvalue, PyObject *options, pycbc_Item *itm,
    void *arg)
{
    int rv;
    const struct storecmd_vars *scv = arg;
    pycbc_pybuffer keybuf = { NULL };
    lcb_CMDSUBDOC cmd = { 0 };

    if (itm) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Item not supported in subdoc mode");
        return -1;
    }

    if (pycbc_tc_encode_key(self, curkey, &keybuf) != 0) {
        return -1;
    }

    cmd.cas = scv->single_cas;
    cmd.exptime = scv->ttl;
    LCB_CMD_SET_KEY(&cmd, keybuf.buffer, keybuf.length);
    rv = pycbc_sd_handle_speclist(self, cv->mres, curkey, curvalue, &cmd);
    PYCBC_PYBUF_RELEASE(&keybuf);
    return rv;
}

static int
handle_append_flags(pycbc_Bucket *self, PyObject **flagsobj)
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
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0, "invalid flags", *flagsobj);
        return -1;
    }

    if ((val & PYCBC_FMT_BYTES) == PYCBC_FMT_BYTES) {
        return 0;
    } else if ((val & PYCBC_FMT_UTF8) == PYCBC_FMT_UTF8) {
        return 0;
    }

    PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0, "Only FMT_BYTES and FMT_UTF8 are supported for append/prepend", *flagsobj);
    return -1;

}

static PyObject *
set_common(pycbc_Bucket *self, PyObject *args, PyObject *kwargs,
    int operation, int argopts)
{
    int rv;
    Py_ssize_t ncmds = 0;
    PyObject *ttl_O = NULL;
    PyObject *dict = NULL;
    PyObject *key;
    PyObject *value;
    pycbc_seqtype_t seqtype;
    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;
    struct storecmd_vars scv = { 0 };
    char persist_to = 0, replicate_to = 0;


    static char *kwlist_multi[] = {
            "kv", "ttl", "format",
            "persist_to", "replicate_to",
            NULL
    };

    static char *kwlist_single[] = {
            "key", "value", "cas", "ttl", "format",
            "persist_to", "replicate_to",
            NULL
    };

    scv.operation = operation;
    scv.argopts = argopts;

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = PyArg_ParseTupleAndKeywords(args, kwargs, "O|OOBB", kwlist_multi,
                                         &dict,
                                         &ttl_O, &scv.flagsobj,
                                         &persist_to, &replicate_to);

    } else {
        rv = PyArg_ParseTupleAndKeywords(args, kwargs, "OO|KOOBB", kwlist_single,
                                         &key, &value,
                                         &scv.single_cas, &ttl_O, &scv.flagsobj,
                                         &persist_to, &replicate_to);
    }

    if (!rv) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "couldn't parse arguments");
        return NULL;
    }

    rv = pycbc_get_ttl(ttl_O, &scv.ttl, 1);
    if (rv < 0) {
        return NULL;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = pycbc_oputil_check_sequence(dict, 0, &ncmds, &seqtype);
        if (rv < 0) {
            return NULL;
        }

    } else {
        ncmds = 1;
    }

    if (operation == LCB_APPEND || operation == LCB_PREPEND) {
        rv = handle_append_flags(self, &scv.flagsobj);
        if (rv < 0) {
            return NULL;
        }

    } else if (scv.flagsobj == NULL || scv.flagsobj == Py_None) {
        scv.flagsobj = self->dfl_fmt;
    }

    rv = pycbc_common_vars_init(&cv, self, argopts, ncmds, 1);
    if (rv < 0) {
        return NULL;
    }

    rv = pycbc_handle_durability_args(self, &cv.mres->dur,
                                      persist_to, replicate_to);

    if (rv == 1) {
        cv.mres->mropts |= PYCBC_MRES_F_DURABILITY;

    } else if (rv == -1) {
        goto GT_DONE;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = pycbc_oputil_iter_multi(self, seqtype, dict, &cv, 0, handle_single_kv, &scv);

    } else {
        rv = handle_single_kv(self, &cv, 0, key, value, NULL, NULL, &scv);
    }

    if (rv < 0) {
        goto GT_DONE;
    }

    if (-1 == pycbc_common_vars_wait(&cv, self)) {
        goto GT_DONE;
    }

GT_DONE:
    pycbc_common_vars_finalize(&cv, self);
    return cv.ret;
}

#define DECLFUNC(name, operation, mode) \
    PyObject *pycbc_Bucket_##name(pycbc_Bucket *self, \
                                      PyObject *args, PyObject *kwargs) { \
    return set_common(self, args, kwargs, operation, mode); \
}

DECLFUNC(upsert_multi, LCB_SET, PYCBC_ARGOPT_MULTI)
DECLFUNC(insert_multi, LCB_ADD, PYCBC_ARGOPT_MULTI)
DECLFUNC(replace_multi, LCB_REPLACE, PYCBC_ARGOPT_MULTI)

DECLFUNC(append_multi, LCB_APPEND, PYCBC_ARGOPT_MULTI)
DECLFUNC(prepend_multi, LCB_PREPEND, PYCBC_ARGOPT_MULTI)

DECLFUNC(upsert, LCB_SET, PYCBC_ARGOPT_SINGLE)
DECLFUNC(insert, LCB_ADD, PYCBC_ARGOPT_SINGLE)
DECLFUNC(replace, LCB_REPLACE, PYCBC_ARGOPT_SINGLE)

DECLFUNC(append, LCB_APPEND, PYCBC_ARGOPT_SINGLE)
DECLFUNC(prepend, LCB_PREPEND, PYCBC_ARGOPT_SINGLE)

DECLFUNC(mutate_in, 0, PYCBC_ARGOPT_SINGLE | PYCBC_ARGOPT_SDMULTI)
