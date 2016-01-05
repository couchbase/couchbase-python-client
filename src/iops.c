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

#define XIONAME_CACHENTRIES(X) \
    X(modevent, 0) \
    X(modtimer, 0) \
    X(startwatch, 0) \
    X(stopwatch, 0) \
    X(mkevent, 1) \
    X(mktimer, 1)

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

static PyTypeObject pycbc_IOPSWrapperType = {
        PYCBC_POBJ_HEAD_INIT(NULL)
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
    PyObject *parent;

    if (ev->state == PYCBC_EVSTATE_FREED) {
        return;
    }

    if (ev->type == PYCBC_EVTYPE_IO) {
        fd = (lcb_socket_t)((pycbc_IOEvent*)ev)->fd;
    }
    Py_INCREF(ev);
    parent = ev->parent;
    Py_XINCREF(parent);
    ev->cb.handler(fd, which, ev->cb.data);
    Py_XDECREF(parent);
    Py_DECREF(ev);
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
                                (unsigned long)self->fd, (int)self->flags, self);
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
                PyDoc_STR("Called for read events. This is the efficient \n"
                          "form of ``ready(LCB_READ_EVENT)``\n")
        },

        { "ready_w",
                (PyCFunction)Event_on_write,
                METH_NOARGS,
                PyDoc_STR("Called for write events. This is equivalent to\n"
                          "``ready(LCB_WRITE_EVENT)``\n")
        },

        { "ready_rw",
                (PyCFunction)Event_on_readwrite,
                METH_NOARGS,
                PyDoc_STR("Called for rw events. This is equivalent to\n"
                          "``ready(LCB_READ_EVENT|LCB_WRITE_EVENT)``\n")
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

static int
Event_gc_traverse(pycbc_Event *ev, visitproc visit, void *arg)
{
    Py_VISIT(ev->vdict);
    Py_VISIT(ev->parent);
    return 0;
}

static void
Event_gc_clear(pycbc_Event *ev)
{
    Py_CLEAR(ev->vdict);
    Py_CLEAR(ev->parent);
}

static void
Event_dealloc(pycbc_Event *self)
{
    Event_gc_clear(self);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

#define SET_EVENT_GCFUNCS(type) do {\
    (type)->tp_flags |= Py_TPFLAGS_HAVE_GC; \
    (type)->tp_traverse = (traverseproc)Event_gc_traverse; \
    (type)->tp_clear = (inquiry)Event_gc_clear; \
    (type)->tp_dealloc = (destructor)Event_dealloc; \
} while (0);

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
    p->tp_flags = Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE;
    p->tp_init = (initproc)Event__init__;
    p->tp_dictoffset = offsetof(pycbc_Event, vdict);

    SET_EVENT_GCFUNCS(p);
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
    p->tp_base = &pycbc_EventType;

    SET_EVENT_GCFUNCS(p);
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
    p->tp_base = &pycbc_EventType;

    SET_EVENT_GCFUNCS(p);
    return PyType_Ready(p);
}

static int
IOPSWrapper_traverse(pycbc_IOPSWrapper *self, visitproc visit, void *arg)
{
    #define X(n, ign) Py_VISIT(self->n);
    XIONAME_CACHENTRIES(X);
    #undef X
    Py_VISIT(self->parent);
    Py_VISIT(self->pyio);
    return 0;
}

static void
IOPSWrapper_clear(pycbc_IOPSWrapper *self)
{
    #define X(n, ign) Py_CLEAR(self->n);
    XIONAME_CACHENTRIES(X);
    #undef X
    Py_CLEAR(self->parent);
    Py_CLEAR(self->pyio);
}

static void
IOPSWrapper_dealloc(pycbc_IOPSWrapper *self)
{
    IOPSWrapper_clear(self);
    free(self->iops);

    Py_TYPE(self)->tp_free((PyObject*)self);
}

int
pycbc_IOPSWrapperType_init(PyObject **ptr)
{
    PyTypeObject *p = &pycbc_IOPSWrapperType;
    *ptr = (PyObject *)p;
    if (p->tp_name) {
        return 0;
    }

    p->tp_name = "_IOPSWrapper";
    p->tp_new = PyType_GenericNew;
    p->tp_basicsize = sizeof(pycbc_IOPSWrapper);
    p->tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC;
    p->tp_dealloc = (destructor)IOPSWrapper_dealloc;
    p->tp_traverse = (traverseproc)IOPSWrapper_traverse;
    p->tp_clear = (inquiry)IOPSWrapper_clear;

    return PyType_Ready(p);
}

static PyObject *
do_safecall(PyObject *callable, PyObject *args)
{
    int has_error = 0;
    PyObject *exctype = NULL, *excval = NULL, *exctb = NULL;
    PyObject *result;

    if (PyErr_Occurred()) {
        /* Calling from within handler */
        has_error = 1;
        PyErr_Fetch(&exctype, &excval, &exctb);
        PyErr_Clear();
    }

    result = PyObject_CallObject(callable, args);
    if (!has_error) {
        /* No special handling here... */
        return result;
    }

    if (!result) {

        #if PY_MAJOR_VERSION == 3
        PyObject *exctype2, *excval2, *exctb2;
        PyErr_NormalizeException(&exctype, &excval, &exctb);
        PyErr_Fetch(&exctype2, &excval2, &exctb2);
        PyErr_NormalizeException(&exctype2, &excval, &exctb2);
        /* Py3K has exception contexts we can use! */
        PyException_SetContext(excval2, excval);
        excval = NULL; /* Since SetContext steals a reference */
        PyErr_Restore(exctype2, excval2, exctb2);
        #else
        PyErr_PrintEx(0);
        #endif

        /* Clean up remaining variables */
        Py_XDECREF(exctype);
        Py_XDECREF(excval);
        Py_XDECREF(exctb);
    } else {
        PyErr_Restore(exctype, excval, exctb);
    }
    return result;
}

static int
modify_event_python(pycbc_IOPSWrapper *pio, pycbc_Event *ev,
                    pycbc_evaction_t action, lcb_socket_t newsock, void *arg)
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
        meth = pio->modevent;

    } else {
        usecs = *(lcb_uint32_t*)arg;
        o_arg = pycbc_IntFromL(usecs);
        meth = pio->modtimer;
    }
    PyTuple_SET_ITEM(argtuple, 2, o_arg);

    result = do_safecall(meth, argtuple);
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
    pycbc_IOPSWrapper *pio = PYCBC_IOW_FROM_IOPS(io);

    if (evtype == PYCBC_EVTYPE_IO) {
        defltype = &pycbc_IOEventType;
        meth = pio->mkevent;

    } else {
        defltype = &pycbc_TimerEventType;
        meth = pio->mktimer;
    }

    if (meth) {
        ret = do_safecall(meth, NULL);
        if (!ret) {
            PyErr_PrintEx(0);
            abort();
        }

    } else {
        PyErr_Clear();
        ret = PYCBC_TYPE_CTOR(defltype);
    }

    ((pycbc_Event *)ret)->type = evtype;
    ((pycbc_Event *)ret)->parent = (PyObject *)pio;
    Py_INCREF(pio);
    return ret;
}

static void *
create_event(lcb_io_opt_t io)
{
    return create_event_python(io, PYCBC_EVTYPE_IO);
}

static void *
create_timer(lcb_io_opt_t io)
{
    return create_event_python(io, PYCBC_EVTYPE_TIMER);
}

static void
destroy_event_common(lcb_io_opt_t io, void *arg)
{
    pycbc_Event *ev = arg;
    lcb_U32 dummy = 0;
    pycbc_assert(ev->state != PYCBC_EVSTATE_ACTIVE);

    modify_event_python(PYCBC_IOW_FROM_IOPS(io), ev, PYCBC_EVACTION_CLEANUP,
                        0, &dummy);

    ev->state = PYCBC_EVSTATE_FREED;
    Py_DECREF(ev);
}

static int
update_event(lcb_io_opt_t io, lcb_socket_t sock, void *event, short flags,
             void *data, pycbc_lcb_cb_t handler)
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

    return modify_event_python(PYCBC_IOW_FROM_IOPS(io), (pycbc_Event*)ev,
                               action, sock, &flags);
}

static void
delete_event(lcb_io_opt_t io, lcb_socket_t sock, void *event)
{
    pycbc_Event *ev = (pycbc_Event*)event;
    pycbc_IOPSWrapper *pio = PYCBC_IOW_FROM_IOPS(io);
    short tmp = 0;

    modify_event_python(pio, ev, PYCBC_EVACTION_UNWATCH, sock, &tmp);
}

static void
delete_timer(lcb_io_opt_t io, void *timer)
{
    lcb_U32 dummy = 0;
    pycbc_IOPSWrapper *pio = PYCBC_IOW_FROM_IOPS(io);
    modify_event_python(pio, (pycbc_Event*)timer, PYCBC_EVACTION_UNWATCH, -1,
                        &dummy);
}

static int
update_timer(lcb_io_opt_t io, void *timer, lcb_U32 usec, void *data,
             pycbc_lcb_cb_t handler)
{
    pycbc_TimerEvent *ev = (pycbc_TimerEvent*)timer;
    ev->cb.data = data;
    ev->cb.handler = handler;

    return modify_event_python(PYCBC_IOW_FROM_IOPS(io), (pycbc_Event*)ev,
                               PYCBC_EVACTION_WATCH, -1, &usec);
}

static void
run_event_loop(lcb_io_opt_t io)
{
    pycbc_IOPSWrapper *pio = PYCBC_IOW_FROM_IOPS(io);
    pio->in_loop = 1;
    PyObject_CallFunctionObjArgs(pio->startwatch, NULL);
}

static void
stop_event_loop(lcb_io_opt_t io)
{
    pycbc_IOPSWrapper *pio = PYCBC_IOW_FROM_IOPS(io);
    pio->in_loop = 0;
    PyObject_CallFunctionObjArgs(pio->stopwatch, NULL);
}

static void
iops_destructor(lcb_io_opt_t io)
{
    /* Empty. The IOPS object is not scoped by the library */
    (void)io;
}

static int
load_cached_method(PyObject *obj,
                   PyObject *attr, PyObject **target, int optional)
{
    *target = PyObject_GetAttr(obj, attr);
    if (*target) {
        if (!PyCallable_Check(*target)) {
            PYCBC_EXC_WRAP_OBJ(PYCBC_EXC_ARGUMENTS, 0,
                               "Invalid IOPS object", obj);
            return -1;
        }
        return 0;
    }

    if (optional) {
        PyErr_Clear();
        return 0;
    }

    return -1;
}

static int
cache_io_methods(pycbc_IOPSWrapper *pio, PyObject *obj)
{

#define X(b, is_optional) \
    if (load_cached_method(obj, pycbc_helpers.ioname_##b, &pio->b, \
                           is_optional) == -1) { \
        return -1; \
    }

    XIONAME_CACHENTRIES(X)
    return 0;
#undef X
#undef XIONAME_CACHENTRIES
}

static void iops_getprocs(int version,
    lcb_loop_procs *loop_procs, lcb_timer_procs *timer_procs,
    lcb_bsd_procs *bsd_procs, lcb_ev_procs *ev_procs,
    lcb_completion_procs *completion_procs, lcb_iomodel_t *iomodel) {

    /* Call the parent function */
    lcb_iops_wire_bsd_impl2(bsd_procs, version);

    /* Now apply our new I/O functionality */
    ev_procs->create = create_event;
    ev_procs->destroy = destroy_event_common;
    ev_procs->watch = update_event;
    ev_procs->cancel = delete_event;

    timer_procs->create = create_timer;
    timer_procs->destroy = destroy_event_common;
    timer_procs->schedule = update_timer;
    timer_procs->cancel = delete_timer;

    loop_procs->start = run_event_loop;
    loop_procs->stop = stop_event_loop;
}


PyObject *
pycbc_iowrap_new(pycbc_Bucket *unused, PyObject *pyio)
{
    lcb_io_opt_t iops = NULL;
    pycbc_IOPSWrapper *wrapper;

    (void)unused;

    wrapper = (pycbc_IOPSWrapper *)PYCBC_TYPE_CTOR(&pycbc_IOPSWrapperType);
    wrapper->pyio = pyio;
    Py_INCREF(pyio);

    iops = calloc(1, sizeof(*iops));
    iops->dlhandle = NULL;
    iops->destructor = iops_destructor;
    iops->version = 2;
    iops->v.v2.get_procs = iops_getprocs;

    LCB_IOPS_BASEFLD(iops, cookie) = wrapper;
    wrapper->iops = iops;

    if (-1 == cache_io_methods(wrapper, pyio)) {
        return NULL;
    }
    return (PyObject *)wrapper;
}

lcb_io_opt_t
pycbc_iowrap_getiops(PyObject *iowrap)
{
    pycbc_assert(Py_TYPE(iowrap) == &pycbc_IOPSWrapperType);
    return ((pycbc_IOPSWrapper*)iowrap)->iops;
}
