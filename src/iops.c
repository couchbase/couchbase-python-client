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
#include "iops.h"
#include "structmember.h"

static PyTypeObject pycbc_EventType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

static PyTypeObject pycbc_IOEventType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

static PyTypeObject pycbc_TimerEventType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
        0
};

static struct PyMemberDef pycbc_Event_TABLE_members[] = {

        { "pydata", T_OBJECT_EX, offsetof(pycbc_Event, pypriv), 0 },
        { "evtype", T_INT, offsetof(pycbc_Event, type), READONLY },
        { "state", T_INT, offsetof(pycbc_Event, state), READONLY },
        { NULL }
};

static struct PyMemberDef pycbc_IOEvent_TABLE_members[] = {
        { "fd", T_LONGLONG, offsetof(pycbc_IOEvent, fd), READONLY },
        { "flags", T_SHORT, offsetof(pycbc_IOEvent, flags), READONLY },
        { NULL }
};


static struct PyMemberDef pycbc_TimerEvent_TABLE_members[] = {
        { "usecs", T_ULONGLONG, offsetof(pycbc_TimerEvent, usecs), READONLY },
        { NULL }
};


/**
 * e.g.:
 * event.event_received(PYCBC_LCB_READ_EVENT)
 */
static PyObject *
Event_on_ready(pycbc_Event *ev, PyObject *args)
{
    short flags;
    int rv;
    lcb_socket_t fd = 0;

    rv = PyArg_ParseTuple(args, "h", &flags);
    if (!rv) {
        return NULL;
    }

    if (ev->type == PYCBC_EVTYPE_IO) {
        fd = (lcb_socket_t)((pycbc_IOEvent*)ev)->fd;
    }

    ev->cb.handler(fd, flags, ev->cb.data);
    Py_RETURN_NONE;
}

static PyObject *
IOEvent_fileno(pycbc_IOEvent *self, PyObject *args)
{
    (void)args;
    return pycbc_IntFromL((lcb_socket_t)self->fd);
}

static PyObject *
IOEvent__repr__(pycbc_IOEvent *self)
{
    return PyUnicode_FromFormat("%s<fd=%lu,flags=0x%x @%p>",
                                Py_TYPE(self)->tp_name,
                                (unsigned long)self->fd,
                                (int)self->flags);
}


static struct PyMethodDef pycbc_Event_TABLE_methods[] = {
        { "ready",
                (PyCFunction)Event_on_ready,
                METH_VARARGS,
                PyDoc_STR("Called when an event is ready")
        },
        { NULL }
};

static struct PyMethodDef pycbc_IOEvent_TABLE_methods[] = {
        { "fileno", (PyCFunction)IOEvent_fileno, METH_VARARGS },
        { NULL }
};

static void
Event_dealloc(pycbc_Event *self)
{
    Py_XDECREF(self->pypriv);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

int
pycbc_EventType_init(PyObject **ptr)
{
    PyTypeObject *p = &pycbc_EventType;
    *ptr = (PyObject*)p;

    if (p->tp_name) {
        return 0;
    }

    p->tp_name = "Event";
    p->tp_doc = PyDoc_STR("Internal event handle");
    p->tp_new = PyType_GenericNew;
    p->tp_basicsize = sizeof(pycbc_Event);
    p->tp_members = pycbc_Event_TABLE_members;
    p->tp_methods = pycbc_Event_TABLE_methods;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_dealloc = (destructor)Event_dealloc;
    return PyType_Ready(p);
}

int
pycbc_IOEventType_init(PyObject **ptr)
{
    PyTypeObject *p = &pycbc_IOEventType;
    *ptr = (PyObject*)p;
    if (p->tp_name) {
        return 0;
    }
    p->tp_name = "IOEvent";
    p->tp_new = PyType_GenericNew;
    p->tp_basicsize = sizeof(pycbc_IOEvent);
    p->tp_members = pycbc_IOEvent_TABLE_members;
    p->tp_methods = pycbc_IOEvent_TABLE_methods;
    p->tp_repr = (reprfunc)IOEvent__repr__;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_dealloc = (destructor)Event_dealloc;
    p->tp_base = &pycbc_EventType;
    return PyType_Ready(p);
}

int
pycbc_TimerEventType_init(PyObject **ptr)
{
    PyTypeObject *p = &pycbc_TimerEventType;
    *ptr = (PyObject*)p;
    if (p->tp_name) {
        return 0;
    }
    p->tp_name = "TimerEvent";
    p->tp_new  = PyType_GenericNew;
    p->tp_basicsize = sizeof(pycbc_TimerEvent);
    p->tp_members = pycbc_TimerEvent_TABLE_members;
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
    p->tp_dealloc = (destructor)Event_dealloc;
    p->tp_base = &pycbc_EventType;
    return PyType_Ready(p);
}


static int
modify_event_python(pycbc_iops_t *pio,
                    pycbc_Event *ev,
                    pycbc_evaction_t action,
                    lcb_socket_t newsock,
                    void *arg)
{
    int ret;
    PyObject *argtuple = NULL;
    PyObject *result = NULL;
    PyObject *meth = NULL;
    short flags = 0;
    unsigned long usecs = 0;

    if (ev->type == PYCBC_EVTYPE_IO) {
        flags = *(short*)arg;
        argtuple = Py_BuildValue("(O,i,i,i)", ev, action, flags, newsock);
        meth = PyObject_GetAttr(pio->pyio, pycbc_helpers.ioname_modevent);

    } else {
        usecs = *(lcb_uint32_t*)arg;
        argtuple = Py_BuildValue("(O,i,k)", ev, action, usecs);
        meth = PyObject_GetAttr(pio->pyio, pycbc_helpers.ioname_modtimer);
    }

    assert(meth);

    result = PyObject_CallObject(meth, argtuple);
    Py_XDECREF(meth);
    Py_XDECREF(result);
    Py_XDECREF(argtuple);

    if (ev->type == PYCBC_EVTYPE_IO) {
        pycbc_IOEvent *evio = (pycbc_IOEvent*)ev;
        evio->fd = newsock;
        evio->flags = flags;
    }

    if (action == PYCBC_EVACTION_WATCH) {
        ev->state = PYCBC_EVSTATE_ACTIVE;
    } else {
        ev->state = PYCBC_EVSTATE_SUSPENDED;
    }

    if (!result) {
        ret = -1;
        PYCBC_EXC_WRAP(PYCBC_EXC_INTERNAL, 0, "Couldn't invoke IO Function");
    } else {
        ret = 0;
    }

    return ret;
}

/**
 * Begin GLUE
 */
static void *
create_event(lcb_io_opt_t io)
{
    PyObject *ret;

    ret = PYCBC_TYPE_CTOR(&pycbc_IOEventType);
    ((pycbc_IOEvent*)ret)->type = PYCBC_EVTYPE_IO;

    (void)io;
    return ret;
}

static void *
create_timer(lcb_io_opt_t io)
{
    pycbc_TimerEvent *ret;

    ret = (pycbc_TimerEvent*)PYCBC_TYPE_CTOR(&pycbc_TimerEventType);
    ret->type = PYCBC_EVTYPE_TIMER;

    (void)io;
    return ret;
}

static void
destroy_event_common(lcb_io_opt_t io, void *arg)
{
    pycbc_IOEvent *ev = (pycbc_IOEvent*)arg;
    ev->state = PYCBC_EVSTATE_SUSPENDED;

    (void)io;
    Py_DECREF(arg);
}

static int
update_event(lcb_io_opt_t io,
             lcb_socket_t sock,
             void *event,
             short flags,
             void *data,
             pycbc_lcb_cb_t handler)
{
    pycbc_IOEvent *ev = (pycbc_IOEvent*)event;
    pycbc_evaction_t action;
    pycbc_evstate_t new_state;

    if (!flags) {
        action = PYCBC_EVACTION_UNWATCH;
        new_state = PYCBC_EVSTATE_SUSPENDED;

    } else {
        action = PYCBC_EVACTION_WATCH;
        new_state = PYCBC_EVSTATE_ACTIVE;
    }

    ev->cb.handler = handler;
    ev->cb.data = data;

    if (ev->flags == flags && new_state == ev->state && ev->fd == sock) {
        return 0;
    }

    return modify_event_python((pycbc_iops_t*)io,
                               (pycbc_Event*)ev,
                               action,
                               sock,
                               &flags);
}

static void
delete_event(lcb_io_opt_t io, lcb_socket_t sock, void *event)
{
    pycbc_IOEvent *ev = (pycbc_IOEvent*)event;

    modify_event_python((pycbc_iops_t*) io,
                        (pycbc_Event*) ev,
                        PYCBC_EVACTION_UNWATCH,
                        sock,
                        &ev->flags);
}

static void
delete_timer(lcb_io_opt_t io, void *timer)
{
    lcb_uint32_t dummy = 0;
    modify_event_python((pycbc_iops_t*)io,
                        (pycbc_Event*)timer,
                        PYCBC_EVACTION_UNWATCH,
                        -1,
                        &dummy);
}

static int
update_timer(lcb_io_opt_t io,
             void *timer,
             lcb_uint32_t usec,
             void *data,
             pycbc_lcb_cb_t handler)
{
    pycbc_TimerEvent *ev = (pycbc_TimerEvent*)timer;
    ev->cb.data = data;
    ev->cb.handler = handler;
    ev->usecs = usec;

    if (((pycbc_TimerEvent*)ev)->usecs == usec) {
        return 0;
    }

    return modify_event_python((pycbc_iops_t*)io,
                               (pycbc_Event*)ev,
                               PYCBC_EVACTION_WATCH,
                               -1,
                               &usec);
}

static void
run_event_loop(lcb_io_opt_t io)
{
    PyObject *fn;
    pycbc_iops_t *pio = (pycbc_iops_t*)io;
    pio->in_loop = 1;

    fn = PyObject_GetAttr(pio->pyio, pycbc_helpers.ioname_startwatch);
    PyObject_CallFunctionObjArgs(fn, NULL);
    Py_XDECREF(fn);

}

static void
stop_event_loop(lcb_io_opt_t io)
{
    PyObject *fn;
    pycbc_iops_t *pio = (pycbc_iops_t*)io;
    pio->in_loop = 0;

    fn = PyObject_GetAttr(pio->pyio, pycbc_helpers.ioname_stopwatch);
    PyObject_CallFunctionObjArgs(fn, NULL);

    Py_XDECREF(fn);
}

static void
iops_destructor(lcb_io_opt_t io)
{
    (void)io;
}

lcb_io_opt_t
pycbc_iops_new(pycbc_Connection *conn, PyObject *pyio)
{
    lcb_io_opt_t ret = NULL;
    lcb_io_opt_t dfl = NULL;

    lcb_error_t err;
    struct lcb_create_io_ops_st options = { 0 };
    pycbc_iops_t *pio;

    pio = calloc(1, sizeof(*pio));
    ret = &pio->iops;
    pio->conn = conn;
    pio->pyio = pyio;

    Py_INCREF(pyio);

    /**
     * We create the select 'iops' handle and copy over its functionality
     * from there. Now that libcouchbase has the 'select' iops build in, we use
     * that instead.
     *
     * We discard the default iops loop data at the expense of leaking a
     * dlhandle.
     */
    options.v.v0.type = LCB_IO_OPS_SELECT;
    err = lcb_create_io_ops(&dfl, &options);
    if (err != LCB_SUCCESS) {
        PYCBC_EXC_WRAP(PYCBC_EXC_LCBERR, err, "Couldn't create IOPS");
        return NULL;
    }

    memcpy(&pio->iops, dfl, sizeof(*dfl));
    /* hide the dlsym */
    dfl->dlhandle = NULL;

    lcb_destroy_io_ops(dfl);
    dfl = NULL;

    ret->v.v0.create_event = create_event;
    ret->v.v0.create_timer = create_timer;
    ret->v.v0.destroy_event = destroy_event_common;
    ret->v.v0.destroy_timer = destroy_event_common;
    ret->v.v0.update_event = update_event;
    ret->v.v0.delete_event = delete_event;
    ret->v.v0.delete_timer = delete_timer;
    ret->v.v0.update_timer = update_timer;
    ret->v.v0.run_event_loop = run_event_loop;
    ret->v.v0.stop_event_loop = stop_event_loop;
    ret->destructor = iops_destructor;

    return ret;
}

void
pycbc_iops_free(lcb_io_opt_t io)
{
    pycbc_iops_t *pio = (pycbc_iops_t*)io;
    Py_XDECREF(pio->pyio);
    free(pio);
}
