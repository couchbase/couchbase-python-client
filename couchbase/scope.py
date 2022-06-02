#  Copyright 2016-2022. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from couchbase.logic.scope import ScopeLogic

"""
** DEPRECATION NOTICE **

Once the deprecated Scope import from couchbase.collection is removed, this class
can be replaced w/ the ScopeLogic class and the ScopeLogic class removed.  The
hierarchy was created to help w/ 3.x imports.

"""


class Scope(ScopeLogic):
    """Create a Couchbase Scope instance.

    Exposes the operations which are available to be performed against a scope. Namely the ability to access
    to Collections for performing operations.

    Args:
        bucket (:class:`~couchbase.bucket.Bucket`): A :class:`~couchbase.bucket.Bucket` instance.
        scope_name (str): Name of the scope.

    """
