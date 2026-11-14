#  Copyright 2016-2026. Couchbase, Inc.
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

"""What a `mutate_in` caller's arguments mean, whichever protocol sends them."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.subdocument import StoreSemantics, SubDocOp

if TYPE_CHECKING:
    from couchbase.common.subdocument import Spec
    from couchbase.serializer import Serializer

# An array op carries its values as a bare list, without the enclosing brackets.
ARRAY_VALUES_OPS = frozenset({SubDocOp.ARRAY_PUSH_FIRST, SubDocOp.ARRAY_PUSH_LAST, SubDocOp.ARRAY_INSERT})

_STORE_SEMANTICS_KEYWORDS = {
    'insert_doc': StoreSemantics.INSERT,
    'upsert_doc': StoreSemantics.UPSERT,
    'replace_doc': StoreSemantics.REPLACE,
}


def read_store_semantics_keywords(final_args: dict[str, Any]) -> None:
    """Fold the `insert_doc`, `upsert_doc` and `replace_doc` keywords into `store_semantics`.

    Writing one, whatever its value, chooses its semantics over `store_semantics`.
    """
    semantics = [v for k, v in _STORE_SEMANTICS_KEYWORDS.items() if final_args.pop(k, None) is not None]
    if len(semantics) > 1:
        raise InvalidArgumentException('Cannot set multiple store semantics.')
    if semantics:
        final_args['store_semantics'] = semantics[0]


def check_preserve_expiry(specs: Sequence[Spec], final_args: dict[str, Any]) -> None:
    if final_args.get('preserve_expiry', False) is not True:
        return
    spec_ops = {s[0] for s in specs}
    if SubDocOp.DICT_ADD in spec_ops:
        raise InvalidArgumentException('The preserve_expiry option cannot be set for mutate_in with insert operations.')
    if SubDocOp.REPLACE in spec_ops and final_args.get('expiry', None):
        raise InvalidArgumentException('The expiry and preserve_expiry options cannot both be set for mutate_in '
                                       'with replace operations.')


def encode_spec_value(serializer: Serializer, opcode: SubDocOp, value: Any) -> bytes:
    encoded = serializer.serialize(value)
    if opcode in ARRAY_VALUES_OPS:
        return encoded.strip()[1:-1]
    return encoded
