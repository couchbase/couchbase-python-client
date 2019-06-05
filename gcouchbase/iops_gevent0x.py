from gevent.core import event as LibeventEvent
from gevent.core import timer as LibeventTimer
from gevent.core import EV_READ, EV_WRITE

from couchbase_core.iops.base import (
    IOEvent, TimerEvent,
    LCB_READ_EVENT, LCB_WRITE_EVENT, LCB_RW_EVENT,
    PYCBC_EVACTION_WATCH, PYCBC_EVACTION_UNWATCH
)

EVENTMAP = {
    LCB_READ_EVENT: EV_READ,
    LCB_WRITE_EVENT: EV_WRITE,
    LCB_RW_EVENT: EV_READ|EV_WRITE
}

REVERSERMAP = {
    EV_READ: LCB_READ_EVENT,
    EV_WRITE: LCB_WRITE_EVENT,
    EV_READ|EV_WRITE: LCB_RW_EVENT
}

class GeventIOEvent(IOEvent):
    def __init__(self):
        super(GeventIOEvent, self).__init__()
        self.ev = None
        self._last_events = -1

    def _ready_pre(self, unused, flags):
        self.update(self.flags)
        lcbflags = REVERSERMAP[flags]
        self.ready(lcbflags)

    def update(self, flags):
        if not self.ev:
            self.ev = LibeventEvent(flags, self.fd, self._ready_pre)

        if self._last_events != self.ev.events:
            self.ev.cancel()
            # DANGER: this relies on the implementation details of the
            # cython-level class.
            LibeventEvent.__init__(self.ev, flags, self.fd, self._ready_pre)

        self.ev.add()

    def cancel(self):
        if not self.ev:
            return
        self.ev.cancel()

class GeventTimer(TimerEvent):
    def __init__(self):
        super(GeventTimer, self).__init__()
        self._tmev = LibeventTimer(0, lambda: self.ready(0))
        self._tmev.cancel()

    def reset(self, usecs):
        self._tmev.add(usecs / 1000000.0)

    def cancel(self):
        self._tmev.cancel()

class IOPS(object):
    def update_event(self, event, action, flags):
        if action == PYCBC_EVACTION_WATCH:
            event.update(EVENTMAP[flags])

        elif action == PYCBC_EVACTION_UNWATCH:
            event.cancel()

    def update_timer(self, event, action, usecs):
        if action == PYCBC_EVACTION_WATCH:
            event.reset(usecs)
        else:
            event.cancel()

    def start_watching(self):
        pass

    def stop_watching(self):
        pass

    def io_event_factory(self):
        return GeventIOEvent()

    def timer_event_factory(self):
        return GeventTimer()
