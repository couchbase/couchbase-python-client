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



class SelectIOPS(object):
    def __init__(self):
        self._do_watch = False
        self._next_timeout = 0

        self._ioevents = set()
        self._timers = set()

        # Active readers and writers
        self._evwr = set()
        self._evrd = set()



    def _unregister_timer(self, event):
        if event in self._timers:
            self._timers.remove(event)

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

        # Otherwise, usecs are already there
        self._timers.add(timer)

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

        now = time.time()
        rout, wout, eout = select.select(rin, win, ein)
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

        for ev in tuple(self._timers):
            if not ev.state == PYCBC_EVSTATE_ACTIVE:
                continue

            if ev.usecs / 1000000 > now:
                continue

            ev.ready(0)

    def start_watching(self):
        if self._do_watch:
            return

        self._do_watch = True
        while self._do_watch:
            self._poll()

    def stop_watching(self):
        self._do_watch = False
