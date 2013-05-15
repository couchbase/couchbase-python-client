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

    /**
     * We only free the other malloc'd structures if we had more than
     * one command.
     */
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

    /**
     * If we have a single command, use the stack-allocated space.
     */
    if (ncmds == 1) {
        cv->cmds.get = &cv->_single_cmd.get;
        cv->cmdlist.get = (void*)&cv->cmds.get;
        cv->enckeys = cv->_po_single;
        cv->encvals = cv->_po_single + 1;
        return 0;
    }

    /**
     * TODO: Maybe Python has a memory pool we can use?
     */
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

/**
 * Check that the object is not one of Python's typical string types
 */
#define _is_not_strtype(o) \
    (PyBytes_Check(o) == 0 && PyByteArray_Check(o) == 0 && PyUnicode_Check(o) == 0)

int pycbc_oputil_check_sequence(PyObject *sequence,
                           int allow_list,
                           int *ncmds,
                           pycbc_seqtype_t *seqtype)
{
    int ret = 0;
    pycbc_seqtype_t dummy;
    if (!seqtype) {
        seqtype = &dummy;
    }

    *ncmds = 0;

    if (PyDict_Check(sequence)) {
        *ncmds = PyDict_Size(sequence);
        *seqtype = PYCBC_SEQTYPE_DICT;
        ret = 0;

    } else if (!allow_list) {
        PyErr_SetString(PyExc_ValueError,
                        "Only dictionaries are supported for this method");
        ret = -1;

    } else if (PyList_Check(sequence)) {
        *seqtype = PYCBC_SEQTYPE_LIST;
        *ncmds = PyList_GET_SIZE(sequence);

    } else if (PyTuple_Check(sequence)) {
        *seqtype = PYCBC_SEQTYPE_TUPLE;
        *ncmds = PyTuple_GET_SIZE(sequence);

    } else if (_is_not_strtype(sequence)) {
        /**
         * Previously we used PySequence_Check, but this failed on things
         * which didn't have __getitem__ (they had a length, but the elements
         * were not ordered, but we don't care about that here
         */
        *seqtype = PYCBC_SEQTYPE_GENERIC;
        *ncmds = PyObject_Length(sequence);

        if (*ncmds == -1) {
            PyErr_Clear();
            PyErr_SetString(PyExc_ValueError,
                            "Argument must be iterable and have known length");
            ret = -1;
        }

    } else {
        PyErr_SetString(PyExc_ValueError,
                        "Argument must be a non-string/bytes sequence");
        ret = -1;
    }

    if (ret == 0 && *ncmds < 1) {
        PyErr_SetString(PyExc_ValueError, "parameter list is empty");
        ret = -1;
    }

    return ret;
}

int pycbc_maybe_set_quiet(pycbc_MultiResultObject *mres, PyObject *quiet)
{
    /**
     * If quiet is 'None', then we default to Connection.quiet
     */
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

PyObject *pycbc_make_retval(int argopts,
                            PyObject **ret,
                            pycbc_MultiResultObject **mres)
{
    Py_ssize_t dictpos = 0;
    PyObject *key, *value;

    if (*ret == NULL || *mres == NULL) {
        return NULL;
    }

    if (!(argopts & PYCBC_ARGOPT_SINGLE)) {
        *ret = (PyObject*)*mres;
        *mres = NULL;
        return *ret;
    }


    PyDict_Next((PyObject*)*mres, &dictpos, &key, &value);
    Py_INCREF(value);
    Py_DECREF(*mres);
    *mres = NULL;
    *ret = value;
    return *ret;
}


PyObject *
pycbc_oputil_iter_prepare(pycbc_seqtype_t seqtype,
                          PyObject *sequence,
                          PyObject **iter,
                          Py_ssize_t *dictpos)
{
    if (seqtype == PYCBC_SEQTYPE_GENERIC) {
        *iter = PyObject_GetIter(sequence);
        if (!*iter) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Couldn't attain iterator");
        }
        return *iter;
    }
    *iter = NULL;
    return sequence;
}

/**
 * I thought it better to make the function call a bit more complex, so as to
 * have the iteration logic unified in a single place
 */
int
pycbc_oputil_sequence_next(pycbc_seqtype_t seqtype,
                           PyObject *seqobj,
                           Py_ssize_t *dictpos,
                           int ii,
                           PyObject **key,
                           PyObject **value)
{
    if (seqtype == PYCBC_SEQTYPE_DICT) {
        int rv = PyDict_Next(seqobj, dictpos, key, value);
        if (rv < 0) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Couldn't iterate");
        }

        Py_XINCREF(*key);
        Py_XINCREF(*value);
        return 0;
    }

    *value = NULL;
    if (seqtype == PYCBC_SEQTYPE_LIST) {
        *key = PyList_GET_ITEM(seqobj, ii);
        Py_INCREF(*key);
    } else if (seqtype == PYCBC_SEQTYPE_TUPLE) {
        *key = PyTuple_GET_ITEM(seqobj, ii);
        Py_INCREF(*key);
    } else {
        *key = PyIter_Next(seqobj);
        if (!*key) {
            PYCBC_EXC_WRAP(PYCBC_EXC_ARGUMENTS, 0, "Bad iterator");
            return -1;
        }
    }

    return 0;
}
