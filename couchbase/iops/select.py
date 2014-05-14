#
# Copyright 2013, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import absolute_import

import select
import time

import couchbase._libcouchbase as LCB
from couchbase._libcouchbase import (
    Event, TimerEvent, IOEvent,
    LCB_READ_EVENT, LCB_WRITE_EVENT, LCB_RW_EVENT,
    PYCBC_EVSTATE_ACTIVE,
    PYCBC_EVACTION_WATCH,
    PYCBC_EVACTION_UNWATCH
)

class SelectTimer(TimerEvent):
    def __init__(self):
        super(SelectTimer, self).__init__()
        self.pydata = 0

    @property
    def exptime(self):
        return self.pydata

    @exptime.setter
    def exptime(self, val):
        self.pydata = val

    def activate(self, usecs):
        self.exptime = time.time() + usecs / 1000000

    def deactivate(self):
        pass

    @property
    def active(self):
        return self.state == PYCBC_EVSTATE_ACTIVE

    # Rich comparison operators implemented - __cmp__ not used in Py3
    def __lt__(self, other): return self.exptime < other.exptime
    def __le__(self, other): return self.exptime <= other.exptime
    def __gt__(self, other): return self.exptime > other.exptime
    def __ge__(self, other): return self.exptime >= other.exptime
    def __ne__(self, other): return self.exptime != other.exptime
    def __eq__(self, other): return self.exptime == other.exptime


class SelectIOPS(object):
    def __init__(self):
        self._do_watch = False
        self._ioevents = set()
        self._timers = []

        # Active readers and writers
        self._evwr = set()
        self._evrd = set()



    def _unregister_timer(self, timer):
        timer.deactivate()
        if timer in self._timers:
            self._timers.remove(timer)

    def _unregister_event(self, event):
        try:
            self._evrd.remove(event)
        except KeyError:
            pass
        try:
            self._evwr.remove(event)
        except KeyError:
            pass
        try:
            self._ioevents.remove(event)
        except KeyError:
            pass

    def update_timer(self, timer, action, usecs):
        if action == PYCBC_EVACTION_UNWATCH:
            self._unregister_timer(timer)
            return

        if timer.active:
            self._unregister_timer(timer)
        timer.activate(usecs)
        self._timers.append(timer)

    def update_event(self, event, action, flags, fd=None):
        if action == PYCBC_EVACTION_UNWATCH:
            self._unregister_event(event)
            return

        elif action == PYCBC_EVACTION_WATCH:
            if flags & LCB_READ_EVENT:
                self._evrd.add(event)
            else:
                try:
                    self._evrd.remove(event)
                except KeyError:
                    pass

            if flags & LCB_WRITE_EVENT:
                self._evwr.add(event)
            else:
                try:
                    self._evwr.remove(event)
                except KeyError:
                    pass

    def _poll(self):
        rin = self._evrd
        win = self._evwr
        ein = list(rin) + list(win)

        self._timers.sort()
        mintime = self._timers[0].exptime - time.time()
        if mintime < 0:
            mintime = 0

        if not (rin or win or ein):
            time.sleep(mintime)
            rout = tuple()
            wout = tuple()
            eout = tuple()
        else:
            rout, wout, eout = select.select(rin, win, ein, mintime)

        now = time.time()

        ready_events = {}
        for ev in rout:
            ready_events[ev] = LCB_READ_EVENT

        for ev in wout:
            if ev in ready_events:
                ready_events[ev] |= LCB_WRITE_EVENT
            else:
                ready_events[ev] = LCB_WRITE_EVENT

        for ev in eout:
            ready_events[ev] = LCB_RW_EVENT

        for ev, flags in ready_events.items():
            if ev.state == PYCBC_EVSTATE_ACTIVE:
                ev.ready(flags)

        for timer in self._timers[:]:
            if not timer.active:
                continue

            if timer.exptime > now:
                continue

            timer.ready(0)

    def start_watching(self):
        if self._do_watch:
            return

        self._do_watch = True
        while self._do_watch:
            self._poll()

    def stop_watching(self):
        self._do_watch = False

    def timer_event_factory(self):
        return SelectTimer()
