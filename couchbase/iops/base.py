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

"""
This file is here for example purposes only. It demonstrates the basic
IOPS API.

This is not yet considered stable interface, although this is currently
the only means by which an external event loop can be integrated with
Couchbase through Python
"""

from couchbase._libcouchbase import (
    PYCBC_EVACTION_WATCH,
    PYCBC_EVACTION_UNWATCH,
    PYCBC_EVACTION_CLEANUP,
    LCB_READ_EVENT,
    LCB_WRITE_EVENT,
    LCB_RW_EVENT,
    IOEvent,
    TimerEvent,
    Event
)

class IOPS(object):
    def __init__(self):
        """
        The IOPS class is intended as an efficient and multiplexing
        manager of one or more :class:`Event` objects.

        As this represents an interface with methods only,
        there is no required behavior in the constructor of this object
        """

    def update_event(self, event, action, flags):
        """
        This method shall perform an action modifying an event.

        :param event: An :class:`IOEvent` object which shall have its
          watcher settings modified. The ``IOEvent`` object is an object
          which provides a ``fileno()`` method.

        :param int action: one of:

          * ``PYCBC_EVACTION_WATCH``: Watch this file for events
          * ``PYCBC_EVACTION_UNWATCH``: Remove this file from all watches
          * ``PYCBC_EVACTION_CLEANUP``: Destroy any references to this object

        :param int flags: Event details, this indicates which events this
          file should be watched for. This is only applicable if ``action``
          was ``PYCBC_EVACTION_WATCH``. It can a bitmask of the following:

          * ``LCB_READ_EVENT``: Watch this file until it becomes readable
          * ``LCB_WRITE_EVENT``: Watch this file until it becomes writeable

        If the action is to watch the event for readability or writeability,
        the ``IOPS`` implementation shall schedule the underlying event system
        to call one of the ``ready_r``, ``ready_w`` or ``ready_rw`` methods
        (for readbility, writeability or both readability and writability
        respectively) at such a time when the underlying reactor/event loop
        implementation has signalled it being so.

        Event watchers are non-repeatable. This means that once the event
        has been delivered, the ``IOEvent`` object shall be removed from a
        watching state. The extension shall call this method again for each
        time an event is requested.

        This method must be implemented
        """

    def update_timer(self, timer, action, usecs):
        """
        This method shall schedule or unschedule a timer.

        :param timer: A :class:`TimerEvent` object.
        :param action: See :meth:`update_event` for meaning
        :param usecs: A relative offset in microseconds when this timer
          shall be fired.

        This method follows the same semantics as :meth:`update_event`,
        except that there is no file.

        When the underlying event system shall invoke the timer, the
        ``TimerEvent`` ``ready`` method shall be called with ``0`` as its
        argument.

        Like ``IOEvents``, ``TimerEvents`` are non-repeatable.

        This method must be implemented
        """

    def io_event_factory(self):
        """
        Returns a new instance of :class:`IOEvent`.

        This method is optional, and is useful in case an implementation
        wishes to utilize its own subclass of ``IOEvent``.

        As with most Python subclasses, the user should ensure that the
        base implementation's ``__init__`` is called.
        """

    def timer_event_factory(self):
        """
        Returns a new instance of :class:`TimerEvent`. Like the
        :meth:`io_event_factory`, this is optional
        """

    def start_watching(self):
        """
        Called by the extension when all scheduled IO events have been
        submitted. Depending on the I/O model, this method can either
        drive the event loop until :meth:`stop_watching` is called, or
        do nothing.

        This method must be implemented
        """

    def stop_watching(self):
        """
        Called by the extension when it no longer needs to wait for events.
        Its function is to undo anything which was done in the
        :meth:`start_watching` method

        This method must be implemented
        """
