from twisted.internet import error as TxErrors

import couchbase._libcouchbase as LCB
from couchbase._libcouchbase import (
    Event, TimerEvent, IOEvent,
    LCB_READ_EVENT, LCB_WRITE_EVENT, LCB_RW_EVENT,
    PYCBC_EVSTATE_ACTIVE,
    PYCBC_EVACTION_WATCH,
    PYCBC_EVACTION_UNWATCH,
    PYCBC_EVACTION_CLEANUP
)

class TxIOEvent(IOEvent):
    """
    IOEvent is a class implemented in C. It exposes
    a 'fileno()' method, so we don't have to.
    """
    __slots__ = []

    def __init__(self):
        super(TxIOEvent, self).__init__()

    def doRead(self):
        self.ready_r()

    def doWrite(self):
        self.ready_w()

    def connectionLost(self, reason):
        if self.state == PYCBC_EVSTATE_ACTIVE:
            self.ready_w()

    def logPrefix(self):
        return "Couchbase IOEvent"


class TxTimer(TimerEvent):
    __slots__ = ['_txev', 'lcb_active']

    def __init__(self):
        super(TxTimer, self).__init__()
        self.lcb_active = False
        self._txev = None


    def _timer_wrap(self):
        if not self.lcb_active:
            return

        self.lcb_active = False
        self.ready(0)


    def schedule(self, usecs, reactor):
        nsecs = usecs / 1000000.0
        if not self._txev or not self._txev.active():
            self._txev = reactor.callLater(nsecs, self._timer_wrap)
        else:
            self._txev.reset(nsecs)

        self.lcb_active = True

    def cancel(self):
        self.lcb_active = False

    def cleanup(self):
        if not self._txev:
            return

        try:
            self._txev.cancel()
        except (TxErrors.AlreadyCalled, TxErrors.AlreadyCancelled):
            pass

        self._txev = None


class v0Iops(object):
    """
    IOPS Implementation to be used with Twisted's "FD" based reactors
    """

    __slots__ = [ 'reactor', 'is_sync', '_stop' ]

    def __init__(self, reactor, is_sync=False):
        self.reactor = reactor
        self.is_sync = is_sync
        self._stop = False

    def update_event(self, event, action, flags):
        """
        Called by libcouchbase to add/remove event watchers
        """
        if action == PYCBC_EVACTION_UNWATCH:
            if event.flags & LCB_READ_EVENT:
                self.reactor.removeReader(event)
            if event.flags & LCB_WRITE_EVENT:
                self.reactor.removeWriter(event)

        elif action == PYCBC_EVACTION_WATCH:
            if flags & LCB_READ_EVENT:
                self.reactor.addReader(event)
            if flags & LCB_WRITE_EVENT:
                self.reactor.addWriter(event)

            if flags & LCB_READ_EVENT == 0:
                self.reactor.removeReader(event)
            if flags & LCB_WRITE_EVENT == 0:
                self.reactor.removeWriter(event)

    def update_timer(self, timer, action, usecs):
        """
        Called by libcouchbase to add/remove timers
        """
        if action == PYCBC_EVACTION_WATCH:
            timer.schedule(usecs, self.reactor)

        elif action == PYCBC_EVACTION_UNWATCH:
            timer.cancel()

        elif action == PYCBC_EVACTION_CLEANUP:
            timer.cleanup()

    def io_event_factory(self):
        return TxIOEvent()

    def timer_event_factory(self):
        return TxTimer()

    def start_watching(self):
        """
        Start/Stop operations. This is a no-op in twisted because
        it's a continuously running async loop
        """
        if not self.is_sync:
            return

        self._stop = False
        while not self._stop:
            self.reactor.doIteration(0)

    def stop_watching(self):
        self._stop = True
