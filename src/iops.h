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
    PYCBC_EVACTION_WATCH = 1 << 0,
    PYCBC_EVACTION_UNWATCH = 1 << 1,
    PYCBC_EVACTION_SUSPEND = 1 << 2,
    PYCBC_EVACTION_RESUME = 1 << 3
} pycbc_evaction_t;

typedef enum {
    PYCBC_EVSTATE_INITIALIZED,
    PYCBC_EVSTATE_ACTIVE,
    PYCBC_EVSTATE_SUSPENDED
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
    PyObject *pypriv; \
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
    unsigned PY_LONG_LONG usecs;
} pycbc_TimerEvent;

/**
 * Wrapper around the IOPS structure.
 */
typedef struct pycbc_iops_st {
    /** Actual method table */
    struct lcb_io_opt_st iops;

    /** Python object being used */
    PyObject *pyio;

    /** Connection object we're instantiated from */
    pycbc_Connection *conn;

    /** Whether the loop is currently active */
    int in_loop;
} pycbc_iops_t;

#endif
