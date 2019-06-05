try:
    import asyncio
except ImportError:
    import trollius as asyncio

from couchbase_core.iops.base import (
    TimerEvent, LCB_READ_EVENT, LCB_WRITE_EVENT,
    PYCBC_EVACTION_WATCH, PYCBC_EVACTION_UNWATCH
)


class AsyncioTimer(TimerEvent):
    def __init__(self):
        super(AsyncioTimer, self).__init__()
        self._ashandle = None

    def cancel(self):
        if self._ashandle:
            self._ashandle.cancel()
            self._ashandle = None

    def schedule(self, loop, usec):
        sec = float(usec) / 1000000.0
        self._ashandle = loop.call_later(sec, self.ready, 0)


class IOPS(object):
    def __init__(self, evloop = None):
        if evloop is None:
            evloop = asyncio.get_event_loop()
        self.loop = evloop

    def update_event(self, event, action, flags):
        if action == PYCBC_EVACTION_WATCH:
            if flags & LCB_READ_EVENT:
                self.loop.add_reader(event.fd, event.ready_r)
            else:
                self.loop.remove_reader(event.fd)

            if flags & LCB_WRITE_EVENT:
                self.loop.add_writer(event.fd, event.ready_w)
            else:
                self.loop.remove_writer(event.fd)

        elif action == PYCBC_EVACTION_UNWATCH:
            if event.flags & LCB_READ_EVENT:
                self.loop.remove_reader(event.fd)
            if event.flags & LCB_WRITE_EVENT:
                self.loop.remove_writer(event.fd)

    def update_timer(self, timer, action, usecs):
        timer.cancel()
        if action == PYCBC_EVACTION_UNWATCH:
            return
        elif action == PYCBC_EVACTION_WATCH:
            timer.schedule(self.loop, usecs)

    def start_watching(self):
        pass
    def stop_watching(self):
        pass
    def timer_event_factory(self):
        return AsyncioTimer()
