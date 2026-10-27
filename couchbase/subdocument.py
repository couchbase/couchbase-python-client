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


"""Public import path for `couchbase.subdocument`.

The spec builders and `SubDocStatus` live in `couchbase.common.subdocument`, which is shared by
every protocol.  They are re-exported here so this import path keeps working.
"""

__all__ = [
    'ArrayValues',
    'LookupInMacro',
    'MutationMacro',
    'Spec',
    'StoreSemantics',
    'SubDocOp',
    'SubDocStatus',
    'array_addunique',
    'array_append',
    'array_insert',
    'array_prepend',
    'convert_macro_cas_to_cas',
    'count',
    'counter',
    'decrement',
    'exists',
    'get',
    'get_full',
    'increment',
    'insert',
    'remove',
    'replace',
    'upsert',
    'with_expiry',
]

from couchbase.common import options
from couchbase.common._supportability import Supportability
from couchbase.common.subdocument import ArrayValues  # noqa: F401
from couchbase.common.subdocument import LookupInMacro  # noqa: F401
from couchbase.common.subdocument import MutationMacro  # noqa: F401
from couchbase.common.subdocument import Spec  # noqa: F401
from couchbase.common.subdocument import StoreSemantics  # noqa: F401
from couchbase.common.subdocument import SubDocOp  # noqa: F401
from couchbase.common.subdocument import SubDocStatus  # noqa: F401
from couchbase.common.subdocument import array_addunique  # noqa: F401
from couchbase.common.subdocument import array_append  # noqa: F401
from couchbase.common.subdocument import array_insert  # noqa: F401
from couchbase.common.subdocument import array_prepend  # noqa: F401
from couchbase.common.subdocument import convert_macro_cas_to_cas  # noqa: F401
from couchbase.common.subdocument import count  # noqa: F401
from couchbase.common.subdocument import counter  # noqa: F401
from couchbase.common.subdocument import decrement  # noqa: F401
from couchbase.common.subdocument import exists  # noqa: F401
from couchbase.common.subdocument import get  # noqa: F401
from couchbase.common.subdocument import get_full  # noqa: F401
from couchbase.common.subdocument import increment  # noqa: F401
from couchbase.common.subdocument import insert  # noqa: F401
from couchbase.common.subdocument import remove  # noqa: F401
from couchbase.common.subdocument import replace  # noqa: F401
from couchbase.common.subdocument import upsert  # noqa: F401
from couchbase.common.subdocument import with_expiry  # noqa: F401

"""
** DEPRECATION NOTICE **

The classes below are deprecated for 3.x compatibility.  They should not be used.
Instead use:
    * All options should be imported from `couchbase.options`.
    * Scope object should be imported from `couchbase.scope`.

"""


@Supportability.import_deprecated('couchbase.subdocument', 'couchbase.options')
class MutateInOptions(options.MutateInOptions):
    pass
