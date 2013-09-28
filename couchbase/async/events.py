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

"""
This file contains various utility classes for scheduling
and destroying events
"""

class EventQueue(object):
    def __init__(self):
        self.called = False
        self.waiters = []

    def fire_async(self, event):
        """
        Fire this event 'immediately', but in the next event iteration
        """

    def maybe_raise(self, *args, **kwargs):
        """
        Given the arguments from '__call__', see if we should raise an error
        """

    def call_single_success(self, event, *args, **kwargs):
        """
        Call a single event with success
        """

    def call_single_failure(self, event, *args, **kwargs):
        """
        Call a single event with a failure. This will be from within
        the 'except' block and thus will have sys.exc_info() available
        """

    def schedule(self, event):
        if self.called:
            self.fire_async(event)
            return

        self.waiters.append(event)

    def __hash__(self):
        return hash(self.name)

    def __len__(self):
        return len(self.waiters)

    def __iter__(self):
        return iter(self.waiters)

    def invoke_waiters(self, *args, **kwargs):
        self.called = True
        try:
            self.maybe_raise(*args, **kwargs)
            for event in self.waiters:
                try:
                    self.call_single_success(event, *args, **kwargs)
                except:
                    pass
        except:
            for event in self.waiters:
                try:
                    self.call_single_failure(event, event, *args, **kwargs)
                except Exception as e:
                    pass

        self.waiters = None

    def __call__(self, *args, **kwargs):
        self.invoke_waiters(*args, **kwargs)
