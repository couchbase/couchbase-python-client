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

        { "__dict__", T_OBJECT_EX, offsetof(pycbc_Event, vdict), 0 },
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
        { NULL }
};


static void event_fire_common(pycbc_Event *ev, short which)
{
    lcb_socket_t fd = 0;
    if (ev->state == PYCBC_EVSTATE_FREED) {
        return;
    }

    if (ev->type == PYCBC_EVTYPE_IO) {
        fd = (lcb_socket_t)((pycbc_IOEvent*)ev)->fd;
    }

    ev->cb.handler(fd, which, ev->cb.data);

}

#define READY_RETURN() \
    if (PyErr_Occurred()) { return NULL; } Py_RETURN_NONE

/**
 * e.g.:
 * event.event_received(PYCBC_LCB_READ_EVENT)
 */
static PyObject *
Event_on_ready(pycbc_Event *ev, PyObject *args)
{
    short flags;
    int rv;

    rv = PyArg_ParseTuple(args, "h", &flags);
    if (!rv) {
        return NULL;
    }

    event_fire_common(ev, flags);
    READY_RETURN();
}

static PyObject *
Event_on_read(pycbc_Event *ev)
{
    event_fire_common(ev, LCB_READ_EVENT);
    READY_RETURN();
}

static PyObject *
Event_on_write(pycbc_Event *ev)
{
    event_fire_common(ev, LCB_WRITE_EVENT);
    READY_RETURN();
}

static PyObject *
Event_on_readwrite(pycbc_Event *ev)
{
    event_fire_common(ev, LCB_RW_EVENT);
    READY_RETURN();
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
                                (int)self->flags,
                                self);
}


static struct PyMethodDef pycbc_Event_TABLE_methods[] = {
        { "ready",
                (PyCFunction)Event_on_ready,
                METH_VARARGS,
                PyDoc_STR("Called when an event is ready")
        },

        { "ready_r",
                (PyCFunction)Event_on_read,
                METH_NOARGS,
                PyDoc_STR("Called for read events")
        },

        { "ready_w",
                (PyCFunction)Event_on_write,
                METH_NOARGS,
                PyDoc_STR("Called for write events")
        },

        { "ready_rw",
                (PyCFunction)Event_on_readwrite,
                METH_NOARGS,
                PyDoc_STR("Called for rw events")
        },

        { NULL }
};

static struct PyMethodDef pycbc_IOEvent_TABLE_methods[] = {
        { "fileno", (PyCFunction)IOEvent_fileno, METH_VARARGS },
        { NULL }
};

static int
Event__init__(pycbc_Event *self, PyObject *args, PyObject *kwargs)
{
    if (PyBaseObject_Type.tp_init((PyObject *)self, args, kwargs) != 0) {
        return -1;
    }
    if (!self->vdict) {
        self->vdict = PyDict_New();
    }
    return 0;
}

static void
Event_dealloc(pycbc_Event *self)
{
    Py_XDECREF(self->vdict);
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
    p->tp_init = (initproc)Event__init__;
    p->tp_dealloc = (destructor)Event_dealloc;
    p->tp_dictoffset = offsetof(pycbc_Event, vdict);
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
    PyObject *result;
    PyObject *o_arg;
    PyObject *meth = NULL;
    PyObject *argtuple;

    short flags = 0;
    unsigned long usecs = 0;

    argtuple = PyTuple_New(3);
    Py_INCREF((PyObject *)ev);
    PyTuple_SET_ITEM(argtuple, 0, (PyObject *)ev);
    PyTuple_SET_ITEM(argtuple, 1, pycbc_IntFromL(action));

    if (ev->type == PYCBC_EVTYPE_IO) {
        flags = *(short*)arg;
        o_arg = pycbc_IntFromL(flags);
        ((pycbc_IOEvent *)ev)->fd = newsock;
        meth = pio->meths.modevent;

    } else {
        usecs = *(lcb_uint32_t*)arg;
        o_arg = pycbc_IntFromL(usecs);
        meth = pio->meths.modtimer;
    }
    PyTuple_SET_ITEM(argtuple, 2, o_arg);

    result = PyObject_CallObject(meth, argtuple);
    Py_DECREF(argtuple);
    Py_XDECREF(result);

    if (ev->type == PYCBC_EVTYPE_IO) {
        pycbc_IOEvent *evio = (pycbc_IOEvent*)ev;
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
create_event_python(lcb_io_opt_t io, pycbc_evtype_t evtype)
{
    PyObject *meth, *ret;
    PyTypeObject *defltype;
    pycbc_iops_t *pio = (pycbc_iops_t *)io;

    if (evtype == PYCBC_EVTYPE_IO) {
        defltype = &pycbc_IOEventType;
        meth = pio->meths.mkevent;

    } else {
        defltype = &pycbc_TimerEventType;
        meth = pio->meths.mktimer;
    }

    if (meth) {
        ret = PyObject_CallObject(meth, NULL);

    } else {
        PyErr_Clear();
        ret = PYCBC_TYPE_CTOR(defltype);
    }

    ((pycbc_Event *)ret)->type = evtype;
    return ret;
}

static void *
create_event(lcb_io_opt_t io)
{
    (void)io;
    return create_event_python(io, PYCBC_EVTYPE_IO);
}

static void *
create_timer(lcb_io_opt_t io)
{
    (void)io;
    return create_event_python(io, PYCBC_EVTYPE_TIMER);
}

static void
destroy_event_common(lcb_io_opt_t io, void *arg)
{
    pycbc_Event *ev = arg;
    lcb_uint32_t dummy = 0;
    (void)io;
    pycbc_assert(ev->state != PYCBC_EVSTATE_ACTIVE);

    modify_event_python((pycbc_iops_t *)io,
                        ev,
                        PYCBC_EVACTION_CLEANUP,
                        0,
                        &dummy);

    ev->state = PYCBC_EVSTATE_FREED;
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

    return modify_event_python((pycbc_iops_t*)io,
                               (pycbc_Event*)ev,
                               PYCBC_EVACTION_WATCH,
                               -1,
                               &usec);
}

static void
run_event_loop(lcb_io_opt_t io)
{
    pycbc_iops_t *pio = (pycbc_iops_t*)io;
    pio->in_loop = 1;
    PyObject_CallFunctionObjArgs(pio->meths.startwatch, NULL);
}

static void
stop_event_loop(lcb_io_opt_t io)
{
    pycbc_iops_t *pio = (pycbc_iops_t*)io;
    pio->in_loop = 0;
    PyObject_CallFunctionObjArgs(pio->meths.stopwatch, NULL);
}

static void
iops_destructor(lcb_io_opt_t io)
{
    pycbc_iops_t *pio = (pycbc_iops_t *)io;
    Py_XDECREF(pio->meths.mkevent);
    Py_XDECREF(pio->meths.mktimer);
    Py_XDECREF(pio->meths.modevent);
    Py_XDECREF(pio->meths.modtimer);
    Py_XDECREF(pio->meths.startwatch);
    Py_XDECREF(pio->meths.stopwatch);
}

struct meth_entry {
    PyObject *lookup;
    PyObject **target;
    int optional;
};

static int
cache_io_methods(pycbc_iops_t *pio, PyObject *obj)
{
    struct meth_entry entries[] = {
            { pycbc_helpers.ioname_modevent, &pio->meths.modevent },
            { pycbc_helpers.ioname_modtimer, &pio->meths.modtimer },
            { pycbc_helpers.ioname_startwatch, &pio->meths.startwatch },
            { pycbc_helpers.ioname_stopwatch, &pio->meths.stopwatch },
            { pycbc_helpers.ioname_mkevent, &pio->meths.mkevent, 1 },
            { pycbc_helpers.ioname_mktimer, &pio->meths.mktimer, 1 },
            { NULL, NULL }
    };


    struct meth_entry *m_ent;
    for (m_ent = entries; m_ent->lookup; m_ent++) {
        *m_ent->target = PyObject_GetAttr(obj, m_ent->lookup);
        if (!*m_ent->target) {
            if (m_ent->optional) {
                continue;
            }

            return -1;
        }
        if (!PyCallable_Check(*m_ent->target)) {
            PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0, "Invalid IOPS object",
                               obj);
            return -1;
        }
    }

    return 0;
}

lcb_io_opt_t
pycbc_iops_new(pycbc_Connection *unused, PyObject *pyio)
{
    lcb_io_opt_t ret = NULL;
    lcb_io_opt_t dfl = NULL;

    lcb_error_t err;
    struct lcb_create_io_ops_st options = { 0 };
    pycbc_iops_t *pio;

    pio = calloc(1, sizeof(*pio));
    ret = &pio->iops;
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

    if (-1 == cache_io_methods(pio, pyio)) {
        return NULL;
    }

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

PyObject *
pycbc_event_new(void)
{
    pycbc_TimerEvent *ev;

    ev =(pycbc_TimerEvent *)PYCBC_TYPE_CTOR(&pycbc_TimerEventType);
    ev->type = PYCBC_EVTYPE_TIMER;
    return (PyObject *)ev;
}
