#include "oputil.h"

static int handle_single_keyop(pycbc_ConnectionObject *self,
                               PyObject *curkey,
                               PyObject *curval,
                               int ii,
                               int optype,
                               struct pycbc_common_vars *cv)
{
    int rv;
    char *key;
    size_t nkey;
    lcb_uint64_t cas = 0;

    rv = pycbc_tc_encode_key(self, &curkey, (void**)&key, &nkey);
    if (rv == -1) {
        return -1;
    }

    cv->enckeys[ii] = curkey;

    if (curval) {

        if (PyDict_Check(curval)) {
            PyObject *cas_o = PyDict_GetItemString(curval, "cas");

            if (!cas_o) {
                PyErr_Clear();
            }
            cas = pycbc_IntAsULL(curval);

        } else if (PyObject_IsInstance(curval, (PyObject*)&pycbc_ResultType)) {
            cas = ((pycbc_ResultObject*)curval)->cas;

        } else if (PyNumber_Check(curval)) {
            cas = pycbc_IntAsULL(curval);

        }

        if (cas == -1 && PyErr_Occurred()) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Invalid CAS specified");
            return -1;
        }
    }

    if (optype == PYCBC_CMD_UNLOCK) {
        lcb_unlock_cmd_t *ucmd = cv->cmds.unlock + ii;

        if (!cas) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS,
                           0,
                           "CAS must be specified for unlock");
        }

        ucmd->v.v0.key = key;
        ucmd->v.v0.nkey = nkey;
        ucmd->v.v0.cas = cas;
        cv->cmdlist.unlock[ii] = ucmd;

        return 0;

    } else {
        lcb_remove_cmd_t *rcmd = cv->cmds.remove + ii;
        rcmd->v.v0.key = key;
        rcmd->v.v0.nkey = nkey;
        rcmd->v.v0.cas = cas;
        cv->cmdlist.remove[ii] = rcmd;
        return 0;
    }
}

PyObject *
keyop_common(pycbc_ConnectionObject *self,
                         PyObject *args,
                         PyObject *kwargs,
                         int optype,
                         int argopts)
{
    int rv;
    int ii;
    int ncmds = 0;
    pycbc_seqtype_t seqtype;
    PyObject *casobj = NULL;

    PyObject *ret = NULL;
    PyObject *is_quiet = NULL;
    PyObject *kobj = NULL;
    pycbc_MultiResultObject *mres = NULL;
    lcb_error_t err;
    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;

    static char *kwlist[] = { "keys", "cas", "quiet", NULL };

    rv = PyArg_ParseTupleAndKeywords(args,
                                     kwargs,
                                     "O|OO",
                                     kwlist,
                                     &kobj,
                                     &casobj,
                                     &is_quiet);

    if (!rv) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "couldn't parse arguments");
        return NULL;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        rv = pycbc_oputil_check_sequence(kobj, 1, &ncmds, &seqtype);
        if (rv < 0) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "bad parameter type");
            return NULL;
        }

        if (casobj && PyObject_IsTrue(casobj)) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Can't pass CAS for multiple keys");
        }

    } else {
        ncmds = 1;
    }

    rv = pycbc_common_vars_init(&cv, ncmds, sizeof(lcb_remove_cmd_t), 0);
    if (rv < 0) {
        return NULL;
    }

    if (argopts & PYCBC_ARGOPT_MULTI) {
        Py_ssize_t dictpos = 0;
        PyObject *curseq, *iter = NULL;
        curseq = pycbc_oputil_iter_prepare(seqtype, kobj, &iter, &dictpos);

        if (!curseq) {
            goto GT_DONE;
        }

        for (ii = 0; ii < ncmds; ii++) {
            PyObject *curkey, *curvalue;

            rv = pycbc_oputil_sequence_next(seqtype,
                                            curseq,
                                            &dictpos,
                                            ii,
                                            &curkey,
                                            &curvalue);
            if (rv < 0) {
                goto GT_ITER_DONE;
            }

            rv = handle_single_keyop(self, curkey, curvalue, ii, optype, &cv);
            if (rv < 0) {
                goto GT_ITER_DONE;
            }
        }

        GT_ITER_DONE:
        Py_XDECREF(iter);

        if (rv < 0) {
            goto GT_DONE;
        }

    } else {
        rv = handle_single_keyop(self, kobj, casobj, 0, optype, &cv);
        if (rv < 0) {
            goto GT_DONE;
        }
    }

    mres = (pycbc_MultiResultObject*)pycbc_multiresult_new(self);
    Py_INCREF(mres);


    if (optype == PYCBC_CMD_DELETE) {
        if (pycbc_maybe_set_quiet(mres, is_quiet) == -1) {
            goto GT_DONE;
        }
        err = lcb_remove(self->instance, mres, ncmds, cv.cmdlist.remove);

    } else {
        err = lcb_unlock(self->instance, mres, ncmds, cv.cmdlist.unlock);
    }

    if (err != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err, "Couldn't schedule command");
        goto GT_DONE;
    }

    PYCBC_CONN_THR_BEGIN(self);
    err = lcb_wait(self->instance);
    PYCBC_CONN_THR_END(self);

    if (err != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err, "Couldn't wait for operation");
        goto GT_DONE;
    }

    if (!pycbc_multiresult_maybe_raise(mres)) {
        ret = (PyObject*)mres;
    }

    GT_DONE:
    pycbc_common_vars_free(&cv);

    if (argopts & PYCBC_ARGOPT_SINGLE) {
        if (mres && (void*)ret == (void*)mres) {
            ret = pycbc_ret_to_single(mres);
        }
    }

    Py_XDECREF(mres);

    return ret;
}



#define DECLFUNC(name, operation, mode) \
    PyObject *pycbc_Connection_##name(pycbc_ConnectionObject *self, \
                                      PyObject *args, PyObject *kwargs) { \
    return keyop_common(self, args, kwargs, operation, mode); \
}

DECLFUNC(delete, PYCBC_CMD_DELETE, PYCBC_ARGOPT_SINGLE);
DECLFUNC(unlock, PYCBC_CMD_UNLOCK, PYCBC_ARGOPT_SINGLE);
DECLFUNC(delete_multi, PYCBC_CMD_DELETE, PYCBC_ARGOPT_MULTI);
DECLFUNC(unlock_multi, PYCBC_CMD_UNLOCK, PYCBC_ARGOPT_MULTI);


PyObject *
pycbc_Connection__stats(pycbc_ConnectionObject *self, PyObject *args, PyObject *kwargs)
{
    int rv;
    int ii;
    int ncmds;
    lcb_error_t err;
    PyObject *keys = NULL;
    PyObject *ret = NULL;
    pycbc_MultiResultObject *mres = NULL;
    struct pycbc_common_vars cv = PYCBC_COMMON_VARS_STATIC_INIT;
    static char *kwlist[] = {  "keys", NULL };

    rv = PyArg_ParseTupleAndKeywords(args, kwargs, "|O", kwlist, &keys);

    if (!rv) {
        PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "couldn't parse arguments");
        return NULL;
    }

    if (keys == NULL || PyObject_IsTrue(keys) == 0) {
        keys = NULL;
        ncmds = 1;

    } else {
        if (!PySequence_Check(keys)) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "keys argument must be sequence");
            return NULL;
        }
        ncmds = PySequence_Size(keys);
    }

    rv = pycbc_common_vars_init(&cv, ncmds, sizeof(lcb_server_stats_cmd_t), 0);
    if (rv < 0) {
        return NULL;
    }

    if (keys) {
        for (ii =0; ii < ncmds; ii++) {
            char *key;
            Py_ssize_t nkey;
            PyObject *newkey = NULL;

            PyObject *curkey = PySequence_GetItem(keys, ii);
            lcb_server_stats_cmd_t *cmd = cv.cmds.stats + ii;
            rv = pycbc_BufFromString(curkey, &key, &nkey, &newkey);
            if (rv < 0) {
                PYCBC_EXC_WRAP_KEY(PYCBC_EXC_ARGUMENTS,
                                   0,
                                   "bad key type in stats",
                                   curkey);
                goto GT_DONE;
            }

            cmd->v.v0.name = key;
            cmd->v.v0.nname = nkey;
            cv.cmdlist.stats[ii] = cmd;
            cv.enckeys[ii] = newkey;
        }

    } else {
        cv.cmdlist.stats[0] = cv.cmds.stats;
    }


    mres = (pycbc_MultiResultObject*)pycbc_multiresult_new(self);
    Py_INCREF(mres);

    err = lcb_server_stats(self->instance, mres, ncmds, cv.cmdlist.stats);
    if (err != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err, "couldn't schedule command");
        goto GT_DONE;
    }

    PYCBC_CONN_THR_BEGIN(self);
    err = lcb_wait(self->instance);
    PYCBC_CONN_THR_END(self);

    if (err != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err, "couldn't wait for command");
        goto GT_DONE;
    }

    ret = (PyObject*)mres;

    GT_DONE:
    pycbc_common_vars_free(&cv);
    Py_XDECREF(mres);
    return ret;
}
