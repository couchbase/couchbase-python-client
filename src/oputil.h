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

#ifndef PYCBC_OPUTIL_H
#define PYCBC_OPUTIL_H

/**
 * This file contains various utilities needed by operation entry points.
 */

#include "pycbc.h"

/**
 * This enumerates the command structures (i.e. lcb_X_cmd_t) with a friendly
 * name for each one.
 */

#define XLCBCMD(X) \
    X(lcb_get_cmd_t, get) \
    X(lcb_touch_cmd_t, touch) \
    X(lcb_store_cmd_t, store) \
    X(lcb_remove_cmd_t, remove) \
    X(lcb_arithmetic_cmd_t, arith) \
    X(lcb_unlock_cmd_t, unlock) \
    X(lcb_server_stats_cmd_t, stats)

/**
 * Union for 'common_vars'; actual commands
 */
union pycbc_u_cmd {
#define X(t, name) t name;
    XLCBCMD(X)
#undef X
};

/**
 * Union for 'common_vars'; command lists
 */
union pycbc_u_pcmd {
#define X(t, name) t *name;
    XLCBCMD(X)
#undef X
};

/**
 * Union for 'common_vars'; pointers to command pointers.
 */
union pycbc_u_ppcmd {
#define X(t, name) const t **name;
    XLCBCMD(X)
#undef X
};

/**
 * Populated by pycbc_oputil_check_sequence, indicates the type of
 * sequence being used.
 *
 * We optimize here as the tuple and list objects have highly efficient
 * access methods.
 */
typedef enum {

    /** Generic sequence. A PyObject_Iter is used to make an iterator */
    PYCBC_SEQTYPE_GENERIC = 0,

    /** Dictionary. We use PyDict_Next */
    PYCBC_SEQTYPE_DICT,

    /** Tuple. PyTuple_GET_ITEM */
    PYCBC_SEQTYPE_TUPLE,

    /** List, PyList_GET_ITEM */
    PYCBC_SEQTYPE_LIST
} pycbc_seqtype_t;

/**
 * Structure containing variables needed for commands.
 * As a bonus, this also contains optimizations for single command situations.
 */
struct pycbc_common_vars {
    /**
     * A single command. This is used if the number of items passed is
     * 1 (i.e. 'single' mode). This eliminates the need for allocating a
     * pointer to a single command
     */
    union pycbc_u_cmd _single_cmd;

    /**
     * Actual command list. If ncmds is 1, this is a pointer to _single_cmd
     */
    union pycbc_u_pcmd cmds;

    /**
     * Actual command pointer list. If ncmds is 1, this is a pointer to cmds
     */
    union pycbc_u_ppcmd cmdlist;

    /**
     * Pair of stacked pointers, again, used if ncmds == 1.
     */
    PyObject *_po_single[2];

    /**
     * List of backing PyObject* for keys.
     * As we possibly need conversion to/from bytes, we need to keep the
     * PyObjects around until the command finishes (or lcb_cmd is scheduled).
     * If ncmds == 1, then this is set to _po_single[0]
     */
    PyObject **enckeys;

    /**
     * List of backing PyObject* for values. Only used for storage operations.
     */
    PyObject **encvals;
    int ncmds;
};

#define PYCBC_COMMON_VARS_STATIC_INIT { { { 0 } } }

/**
 * Dummy tuple/keywords, used for PyArg_ParseTupleAndKeywordArgs, which dies
 * if one of the arguments is NULL, so these contain empty tuples and dicts,
 * respectively.
 */
extern PyObject *pycbc_DummyTuple;
extern PyObject *pycbc_DummyKeywords;

/**
 * Examine the 'quiet' parameter and see if we should set the MultiResult's
 * 'no_raise_enoent' flag.
 */
int pycbc_maybe_set_quiet(pycbc_MultiResultObject *mres, PyObject *quiet);


/**
 * Verify the sequence passed to a multi_* method is valid.
 *
 * This function also weeds out strings (which are perfectly valid Python
 * sequences) since passing a bare string to a 'multi_*' method is usually
 * not what a user wants :)
 *
 * @param sequence the object the user passed as a sequence
 * @param allow_list whether the sequence can be a 'list' (i.e. not a dict)
 * @param ncmds populated with the actual size of the commands
 * @param seqtype populated with the sequence type (see enum doc above)
 *
 * @return 0 on success, -1 on error. Error might be if:
 *  Object is not a dict, and 'allow_list' is false
 *  The object is a valid sequence, but is empty
 *  Other stuff.
 */
int pycbc_oputil_check_sequence(PyObject *sequence,
                          int allow_list,
                          int *ncmds,
                          pycbc_seqtype_t *seqtype);


/**
 * 'Prepares' the sequence object for iteration. This may happen
 * if we needan actual Iterator object. Otherwise it doesn't do anything.
 *
 * I'm sorry that the functions here are a bit obtuse, but Python doesn't make
 * it simple without sacrificing performance. The generic iterator classes
 * and APIs are fairly slowish, and we should optimize if a user passes a simple
 * object.
 *
 * @param seqtype The seqtype, as populated by a call to check_sequence
 * @param sequence the sequence received from the user
 * @param iter a pointer to an empty PyObject pointer. May be filled with an
 * iterator object (if one is needed). Call Py_XDECREF when done iterating
 * @param pointer to a dictionary position variable, if the item is a dictionary
 *
 * @return a sequence object which is to be fed to 'sequence_next' (see
 * below). This may not be the same as the sequence object itself (i.e. it
 * could be the iterator object).
 *
 * If an error is encountered, a CouchbaseError is set, and this function
 * returns NULL.
 */

PyObject *pycbc_oputil_iter_prepare(pycbc_seqtype_t seqtype,
                                    PyObject *sequence,
                                    PyObject **iter,
                                    Py_ssize_t *dictpos);


/**
 * Iterates over the sequence, getting the relevant keys and values for the
 * current iteration
 * @param seqtype the sequence type, as populated by check_sequence
 * @param seqobj the object returned by 'iter_prepare'
 * @param dictpos the dictionary position variable initialized by 'iter_prepare'
 * @param ii the current position (should be incremented by used by one for
 * each iteration)
 * @param key the key for the current iteration. This is populated with a
 * new reference.
 * @param value the value for the current iteration. If the sequence is not
 * a dictionary, this is always NULL. This is populated with a new referenced
 * @return 0 on success, -1 on failure (with a CouchbaseError set)
 */
int pycbc_oputil_sequence_next(pycbc_seqtype_t seqtype,
                               PyObject *seqobj,
                               Py_ssize_t *dictpos,
                               int ii,
                               PyObject **key,
                               PyObject **value);

/**
 * Initialize the 'common_vars' structure.
 * @param cv a pointer to a zero-populated common_vars struct
 * @param ncmds the number of keys in the operation
 * @param tsize the size of the lcb_cmd_t structure to use
 * @param want_vals whether this operation will need to use values. This is
 * used to determine if the 'encvals' field should be allocated
 */
int pycbc_common_vars_init(struct pycbc_common_vars *cv,
                            int ncmds,
                            size_t tsize,
                            int want_vals);

/**
 * Clean up the 'common_vars' structure and free/decref any data. This
 * automatically DECREFs any PyObject enckeys and encvaks.
 */
void pycbc_common_vars_free(struct pycbc_common_vars *cv);


/**
 * Convers the MultiResult object into a return value.
 *
 * @param argopts the argument options passed
 * @params ret a pointer to the return value (which should be == *mres)
 * @params mres a multi result object.
 *
 * @return the return value.
 *
 * If *ret != *mres, then it is assumed that an error condition occured, i.e.
 * it is expected that code assign ret to mres once all operations have
 * succeeded.
 *
 * If *ret == *mres, then *mres' refcount is decremented (and the pointer
 * set to NULL), and *ret will contain *mres.
 *
 * If argopt & PYCBC_ARGOPT_SINGLE, the single Result object is
 * extracted from it, and mres is freed (i.e. XDECREF'd)
 *
 * Since *mret is set to NULL, a future DECREF will not affect us :) - the
 * idea being that Py_XDECREF(mres) is called if ret != mres, but
 * Py_XDECREF(NULL) is called otherwise.
 */
PyObject *pycbc_make_retval(int argopts,
                            PyObject **ret,
                            pycbc_MultiResultObject **mres);


/**
 * Macro to declare prototypes for entry points.
 * If the entry point is foo, then it is expected that there exist a C
 * function called 'pycbc_Connection_foo'.
 *
 * We might want to expand this in the future.
 */
#define PYCBC_DECL_OP(name) \
        PyObject* pycbc_Connection_##name(pycbc_ConnectionObject*, PyObject*, PyObject*)


/* store.c */
PYCBC_DECL_OP(set_multi);
PYCBC_DECL_OP(add_multi);
PYCBC_DECL_OP(replace_multi);
PYCBC_DECL_OP(append_multi);
PYCBC_DECL_OP(prepend_multi);
PYCBC_DECL_OP(set);
PYCBC_DECL_OP(add);
PYCBC_DECL_OP(replace);
PYCBC_DECL_OP(append);
PYCBC_DECL_OP(prepend);

/* arithmetic.c */
PYCBC_DECL_OP(arithmetic);
PYCBC_DECL_OP(incr);
PYCBC_DECL_OP(decr);
PYCBC_DECL_OP(arithmetic_multi);
PYCBC_DECL_OP(incr_multi);
PYCBC_DECL_OP(decr_multi);


/* miscops.c */
PYCBC_DECL_OP(delete);
PYCBC_DECL_OP(unlock);
PYCBC_DECL_OP(delete_multi);
PYCBC_DECL_OP(unlock_multi);

PYCBC_DECL_OP(_stats);


/* get.c */
PYCBC_DECL_OP(get);
PYCBC_DECL_OP(touch);
PYCBC_DECL_OP(lock);
PYCBC_DECL_OP(get_multi);
PYCBC_DECL_OP(touch_multi);
PYCBC_DECL_OP(lock_multi);

#endif /* PYCBC_OPUTIL_H */
