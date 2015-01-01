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

#ifndef PYCBC_IOPS_H
#define PYCBC_IOPS_H
#include "pycbc.h"

typedef enum {
    /** Activate the event so that it may fire upon the trigger */
    PYCBC_EVACTION_WATCH = 1 << 0,

    /** Deactive the event; ignoring the trigger */
    PYCBC_EVACTION_UNWATCH = 1 << 1,

    /** Unused for now */
    PYCBC_EVACTION_SUSPEND = 1 << 2,

    /** Unused for now */
    PYCBC_EVACTION_RESUME = 1 << 3,

    /** Cleanup the event, removing all references of it */
    PYCBC_EVACTION_CLEANUP = 1 << 4
} pycbc_evaction_t;

typedef enum {
    PYCBC_EVSTATE_INITIALIZED,
    PYCBC_EVSTATE_ACTIVE,
    PYCBC_EVSTATE_SUSPENDED,
    PYCBC_EVSTATE_FREED
} pycbc_evstate_t;

typedef enum {
    PYCBC_EVTYPE_IO,
    PYCBC_EVTYPE_TIMER
} pycbc_evtype_t;

typedef void (*pycbc_lcb_cb_t)(lcb_socket_t,short,void*);

#define pycbc_Event_HEAD \
    PyObject_HEAD \
    struct { \
        pycbc_lcb_cb_t handler; \
        void *data; \
    } cb; \
    PyObject *vdict; \
    PyObject *parent; \
    pycbc_evstate_t state; \
    pycbc_evtype_t type;

typedef struct {
    pycbc_Event_HEAD
} pycbc_Event;

typedef struct {
    pycbc_Event_HEAD
    PY_LONG_LONG fd;
    short flags;
} pycbc_IOEvent;

typedef struct {
    pycbc_Event_HEAD
} pycbc_TimerEvent;

/** Wrapper around the IOPS structure. */
typedef struct pycbc_IOPSWrapper_st {
    PyObject_HEAD

    /** LCB's iops */
    struct lcb_io_opt_st *iops;

    PyObject *pyio;
    pycbc_Bucket *parent;

    /** Whether the loop is currently active */
    int in_loop;

    /** Cached method references */
    PyObject *mkevent;
    PyObject *mktimer;
    PyObject *modevent;
    PyObject *modtimer;
    PyObject *startwatch;
    PyObject *stopwatch;
} pycbc_IOPSWrapper;

#define PYCBC_IOW_FROM_IOPS(p) LCB_IOPS_BASEFLD(p, cookie)

#endif
