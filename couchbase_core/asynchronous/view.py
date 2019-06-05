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
This file contains the view implementation for Async
"""

from couchbase_core.views.iterator import View
from .rowsbase import AsyncRowsBase


class AsyncViewBase(AsyncRowsBase, View):
    def __init__(self, *args, **kwargs):
        """
        Initialize a new AsyncViewBase object. This is intended to be
        subclassed in order to implement the require methods to be
        invoked on error, data, and row events.

        Usage of this class is not as a standalone, but rather as
        an ``itercls`` parameter to the
        :meth:`~couchbase_core.connection.Connection.query` method of the
        connection object.
        """
        View.__init__(self, *args, **kwargs)
