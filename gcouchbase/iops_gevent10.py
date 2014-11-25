from gevent.hub import get_hub
from gevent.core import timer as _PyxTimer
from time import time

from couchbase.iops.base import (
    IOEvent, TimerEvent,
    LCB_READ_EVENT, LCB_WRITE_EVENT, LCB_RW_EVENT,
    PYCBC_EVACTION_WATCH, PYCBC_EVACTION_UNWATCH
)

from couchbase.iops.base import (
    IOEvent, TimerEvent,
    LCB_READ_EVENT, LCB_WRITE_EVENT, LCB_RW_EVENT,
    PYCBC_EVACTION_WATCH, PYCBC_EVACTION_UNWATCH
)

EVENTMAP = {
    LCB_READ_EVENT: 1,
    LCB_WRITE_EVENT: 2,
    LCB_RW_EVENT: 3
}

REVERSERMAP = {
    1: LCB_READ_EVENT,
    2: LCB_WRITE_EVENT,
    3: LCB_RW_EVENT
}

class GEventIOEvent(IOEvent):
    def __init__(self):
        self.ev = get_hub().loop.io(0,0)

    def ready_proxy(self, event):
        self.ready(REVERSERMAP[event])

    def watch(self, events):
        self.ev.stop()
        self.ev.fd = self.fd
        self.ev.events = events

        self.ev.start(self.ready_proxy, pass_events=True)

class GEventTimer(TimerEvent):
    def __init__(self):
        self.ev = get_hub().loop.timer(0)

    def ready_proxy(self, *args):
        self.ready(0)

    def schedule(self, usecs):
        seconds = usecs / 1000000.0
        # This isn't the "clean" way, but it's much quicker.. and
        # since we're already using undocumented APIs, why not..
        _PyxTimer.__init__(self.ev, get_hub().loop, seconds)
        self.ev.start(self.ready_proxy, 0)


class IOPS(object):
    def update_event(self, event, action, flags):
        if action == PYCBC_EVACTION_UNWATCH:
            event.ev.stop()
            return

        elif action == PYCBC_EVACTION_WATCH:
            ev_event = EVENTMAP[flags]
            event.watch(ev_event)

    def update_timer(self, event, action, usecs):
        if action == PYCBC_EVACTION_UNWATCH:
            event.ev.stop()
            return

        elif action == PYCBC_EVACTION_WATCH:
            event.schedule(usecs)

    def start_watching(self):
        pass

    def stop_watching(self):
        pass

    def io_event_factory(self):
        return GEventIOEvent()

    def timer_event_factory(self):
        return GEventTimer()
