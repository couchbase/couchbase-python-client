#include "oputil.h"

void pycbc_common_vars_free(struct pycbc_common_vars *cv)
{
    int ii;

    for (ii = 0; ii < cv->ncmds; ii++) {

        if (cv->enckeys && cv->enckeys[ii]) {
            Py_DECREF(cv->enckeys[ii]);
        }

        if (cv->encvals && cv->encvals[ii]) {
            Py_DECREF(cv->encvals[ii]);
        }
    }

    if (cv->ncmds > 1) {
        free(cv->cmds.get);
        free((void*)cv->cmdlist.get);
        free(cv->enckeys);
        free(cv->encvals);
    }
}

int pycbc_common_vars_init(struct pycbc_common_vars *cv,
                            int ncmds,
                            size_t tsize,
                            int want_vals)
{
    int ok;
    cv->ncmds = ncmds;

    if (ncmds == 1) {
        cv->cmds.get = &cv->_single_cmd.get;
        cv->cmdlist.get = (void*)&cv->cmds.get;
        cv->enckeys = cv->_po_single;
        cv->encvals = cv->_po_single + 1;
        return 0;
    }

    cv->cmds.get = calloc(ncmds, tsize);
    cv->cmdlist.get = malloc(ncmds * sizeof(void*));
    cv->enckeys = calloc(ncmds, sizeof(PyObject*));

    if (want_vals) {
        cv->encvals = calloc(ncmds, sizeof(PyObject*));

    } else {
        cv->encvals = NULL;
    }

    ok = (cv->cmds.get && cv->cmdlist.get && cv->enckeys);
    if (ok) {
        ok = (want_vals == 0 || cv->encvals);
    }

    if (!ok) {
        pycbc_common_vars_free(cv);
        PyErr_SetNone(PyExc_MemoryError);
        return -1;
    }
    return 0;
}

int pycbc_oputil_check_sequence(PyObject *sequence,
                           int allow_list,
                           int *ncmds,
                           int *is_dict)
{
    int ret;
    int dummy;
    if (!is_dict) {
        is_dict = &dummy;
    }

    *ncmds = 0;

    if (PyDict_Check(sequence)) {
        *ncmds = PyDict_Size(sequence);
        *is_dict = 1;
        ret = 0;

    } else if (!allow_list) {
        PyErr_SetString(PyExc_ValueError,
                        "Only dictionaries are supported for this method");
        ret = -1;

    } else if (PyList_Check(sequence)) {
        *ncmds = PyList_GET_SIZE(sequence);
        *is_dict = 0;
        ret = 0;

    } else if (PyTuple_Check(sequence)) {
        *ncmds = PyTuple_GET_SIZE(sequence);
        *is_dict = 0;
        ret = 0;

    } else {
        PyErr_SetString(PyExc_ValueError,
                        "Argument must be a tuple, list, or dict");
        ret = -1;
    }

    if (ret == 0 && *ncmds == 0) {
        PyErr_SetString(PyExc_ValueError, "parameter list is empty");
        ret = -1;
    }

    return ret;
}

int pycbc_maybe_set_quiet(pycbc_MultiResultObject *mres, PyObject *quiet)
{
    if (quiet == NULL || quiet == Py_None) {
        mres->no_raise_enoent = mres->parent->quiet;
        return 0;
    }
    mres->no_raise_enoent = PyObject_IsTrue(quiet);

    if (mres->no_raise_enoent == -1) {
        PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS,
                           0, "bad value for 'quiet'", quiet);
        return -1;
    }
    return 0;
}

PyObject* pycbc_ret_to_single(pycbc_MultiResultObject *mres)
{
    Py_ssize_t dictpos = 0;
    PyObject *key, *value;
    PyDict_Next((PyObject*)mres, &dictpos, &key, &value);
    Py_INCREF(value);
    Py_DECREF(mres);
    return value;
}
