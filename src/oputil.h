#ifndef PYCBC_OPUTIL_H
#define PYCBC_OPUTIL_H

#include "pycbc.h"

#define XLCBCMD(X) \
    X(lcb_get_cmd_t, get) \
    X(lcb_touch_cmd_t, touch) \
    X(lcb_store_cmd_t, store) \
    X(lcb_remove_cmd_t, remove) \
    X(lcb_arithmetic_cmd_t, arith) \
    X(lcb_unlock_cmd_t, unlock) \
    X(lcb_server_stats_cmd_t, stats)

union pycbc_u_cmd {
#define X(t, name) t name;
    XLCBCMD(X)
#undef X
};

union pycbc_u_pcmd {
#define X(t, name) t *name;
    XLCBCMD(X)
#undef X
};

union pycbc_u_ppcmd {
#define X(t, name) const t **name;
    XLCBCMD(X)
#undef X
};

/**
 * Structure containing variables needed for commands.
 * As a bonus, this also contains optimizations for single command situations.
 */
struct pycbc_common_vars {
    union pycbc_u_cmd _single_cmd;
    union pycbc_u_pcmd cmds;
    union pycbc_u_ppcmd cmdlist;

    PyObject *_po_single[2];

    PyObject **enckeys;
    PyObject **encvals;
    int ncmds;

};

#define PYCBC_COMMON_VARS_STATIC_INIT { { { 0 } } }

extern PyObject *pycbc_DummyTuple;
extern PyObject *pycbc_DummyKeywords;


int pycbc_maybe_set_quiet(pycbc_MultiResultObject *mres, PyObject *quiet);

int pycbc_oputil_check_sequence(PyObject *sequence,
                          int allow_list,
                          int *ncmds,
                          int *is_dict);

void pycbc_common_vars_free(struct pycbc_common_vars *cv);

int pycbc_common_vars_init(struct pycbc_common_vars *cv,
                            int ncmds,
                            size_t tsize,
                            int want_vals);

PyObject* pycbc_ret_to_single(pycbc_MultiResultObject *mres);

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
