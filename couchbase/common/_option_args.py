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

"""Options kept as the caller wrote them: the connection's and the transactions'.

`couchbase/common/_option_spec.py` states every options class, one entry each, and what holds of
each key whatever the protocol.  The connection options and the transaction options are judged
here, before any protocol is chosen, and kept under the spelling the class receives; a protocol
maps them to what it sends when it uses them.  An operation's options are judged and mapped by
its protocol's request builders, which reuse `merge` and `resolve_deprecated`.

The connection classes and the transaction classes refuse a key their entry does not name, so
on those paths the entry is the acceptance filter as well as the judgement.
"""

from __future__ import annotations

import copy
from typing import Any

from couchbase.common._option_diagnostics import check_options, mode_of
from couchbase.common._option_spec import BLOCKS, BlockSpec
from couchbase.common._supportability import Supportability
from couchbase.common.protocol import CouchbaseProtocol


def merge(arg_vars: dict[str, Any] | None, *options: dict[str, Any] | None) -> dict[str, Any]:
    """The caller's options and its keyword arguments as one dict, arguments winning.

    Copied rather than updated in place because the options object belongs to the caller and
    an operation that edited it would change the next call made with the same object.
    """
    arg_vars = copy.copy(arg_vars) if arg_vars else {}
    merged = copy.copy(options[0]) if (options and options[0]) else {}
    merged.update(arg_vars.pop('kwargs', {}))
    merged.update(arg_vars)
    return merged


def cluster_keys(block: str) -> list[str]:
    """One connection block's keys, under the spelling `ClusterOptions` stores them.

    That spelling is how `ClusterSettings` takes the block back out of the options
    `ClusterOptions.__init__` flattened it into.
    """
    return [spec.stored or spec.key for spec in BLOCKS[block].keys.values()]


def resolve_deprecated(entry: BlockSpec, merged: dict[str, Any]) -> dict[str, Any]:
    """The caller's keywords with each deprecated one reported, and handed to its `into` if it has one.

    Copied, since the dict is the caller's.  A key with an `into` loses to that key wherever both
    are written, which is why it is resolved before the map: the map takes each key without seeing
    its neighbours.
    """
    stated = dict(merged)
    for old in entry.retired:
        if old not in stated:
            continue
        spec = entry.keys[old]
        Supportability.option_deprecated(old, message=spec.deprecated)
        if spec.into is not None:
            stated.setdefault(spec.into, stated.pop(old))
    return stated


def accepted(stated: dict[str, Any], block: str) -> dict[str, Any]:
    """The keys `block`'s entry takes, each deprecated one reported and handed to its `into`.

    A key the entry does not name is dropped where the class refuses one and kept where it does
    not, and a key given as `None` is not given.
    """
    entry = BLOCKS[block]
    if entry.retired:
        stated = resolve_deprecated(entry, stated)
    return {key: value for key, value in stated.items()
            if value is not None and (key in entry.keys or not entry.refuses_unknown)}


def accepted_args(opt_type: type[dict[str, Any]],
                  arg_vars: dict[str, Any] | None,
                  *options: dict,
                  protocol: CouchbaseProtocol) -> dict[str, Any]:
    """The connection path: merge, judge, and keep what the entry accepts.

    The mode is read out of the same mapping being judged, which is what makes it settable on
    the very object whose keys it governs.  Reading one key before judging the rest costs
    nothing, so nothing forces the dial to live anywhere else.
    """
    merged = merge(arg_vars, *options)
    check_options(opt_type.__name__, merged, mode_of(merged), protocol=protocol)
    return accepted(merged, opt_type.__name__)


def transaction_args(block: str, options: dict[str, Any]) -> dict[str, Any]:
    """The transactions path: judge, and keep what was stated.

    No protocol, since the options classes calling this are built before any cluster is named;
    a key one protocol cannot act on is that protocol's transactions impl to judge, and the
    impl maps what is kept here.
    """
    check_options(block, options, protocol=None)
    return accepted(options, block)
