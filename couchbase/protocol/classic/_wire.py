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

"""What an option becomes on the classic wire.

`couchbase/protocol/classic/_option_wire.py` holds what each key is sent to the binding as, and
`couchbase/common/_option_spec.py` what each options class accepts.  The request builders judge
a caller's keywords and map them here.  The connection and transaction options arrive judged and
kept as the caller wrote them, and are mapped here when the binding is handed them.
"""

from __future__ import annotations

from typing import Any

from couchbase.common._option_args import merge, resolve_deprecated
from couchbase.common._option_diagnostics import (OptionsBlock,
                                                  check_options,
                                                  mode_of)
from couchbase.common._option_spec import BLOCKS, BlockSpec
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.option_enums import OptionValidationMode
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.protocol.classic._option_wire import (WIRES,
                                                     Wire,
                                                     WireBlock)

_AS_STORED = Wire()
_UNLISTED = WireBlock({})


def wire_of(block: str, key: str) -> Wire:
    return WIRES.get(block, _UNLISTED).keys.get(key, _AS_STORED)


def mapped(stated: dict[str, Any], block: str, drop_none: bool = True) -> dict[str, Any]:
    """Every option the binding will be handed, under the key it reads it as.

    The two tables decide everything: what a key the entry names is sent as and converted by,
    and whether a key it does not name reaches the binding unexamined or is dropped.  A block a
    request builder maps has no entry this can drive, and is refused rather than half mapped.

    `drop_none` decides one case only: a transform that returns `None`.  A key the entry does
    not name is never dropped for what it holds, so an explicit `transcoder=None` reaches the
    binding from both key-value paths.  The single-document path drops a converted `None` and
    the multi-document path keeps it; which is right is not settled here.
    """
    if WIRES.get(block, _UNLISTED).by_builder:
        raise ValueError('{} is mapped by its request builder, not by to_wire'.format(block))
    entry = BLOCKS[block]
    final: dict[str, Any] = {}
    for key, value in stated.items():
        spec = entry.keys.get(key)
        if spec is None:
            if not entry.refuses_unknown:
                final[key] = value
            continue
        wire = wire_of(block, key)
        name = wire.name_for(spec.stored or spec.key)
        if name is None:
            continue
        converted = value if wire.transform is None else wire.transform(value)
        if converted is None and drop_none:
            continue
        final[name] = converted
    return final


def to_wire(merged: dict[str, Any], block: str, drop_none: bool = True) -> dict[str, Any]:
    """An operation's keywords as the binding reads them, each deprecated one reported first.

    A deprecated key warns here, where its block first reads it, and a block with nothing
    deprecated pays one read of an empty tuple per call.
    """
    entry = BLOCKS[block]
    if entry.retired:
        merged = resolve_deprecated(entry, merged)
    return mapped(merged, block, drop_none)


def get_valid_args(opt_type: type[dict[str, Any]],
                   arg_vars: dict[str, Any] | None,
                   *options: dict,
                   protocol: CouchbaseProtocol) -> dict[str, Any]:
    """Merge, judge and map a connection block's options in one call.

    The facades export it under this name, which is the only reason it is one call: the SDK
    judges the connection options before a protocol is chosen and maps them at connect.
    """
    merged = merge(arg_vars, *options)
    check_options(opt_type.__name__, merged, mode_of(merged), protocol=protocol)
    return to_wire(merged, opt_type.__name__)


def forward_args(arg_vars: dict[str, Any] | None,
                 *options: dict[str, Any] | None,
                 block: OptionsBlock,
                 mode: OptionValidationMode | None = None) -> dict[str, Any]:
    """The single-document and management path: merge, judge and map.

    `block` is what the caller knows and the merged dict does not.  Every keyword here was
    written for one options class, that class is named where the method is written, and by the
    time the dict arrives nothing about it says which.  It is required, because the name is
    what selects both the judgement and the map.

    Judged before the map, because a key the entry renames is judged under the spelling the
    caller wrote and the message has to name that one back.
    """
    merged = merge(arg_vars, *options)
    check_options(block, merged, mode, protocol=block.protocol)
    return to_wire(merged, block)


def check_streaming_args(options: tuple[object, ...],
                         kwargs: dict[str, object],
                         *,
                         block: OptionsBlock,
                         options_type: type[dict[str, Any]],
                         mode: OptionValidationMode | None = None) -> None:
    """The streaming path: judge only, because each service's query builder maps the keys.

    Query and analytics send every key their blocks do not declare as a named parameter, and
    views and search drop it, so a near miss is the one thing the judgement can report and
    this is the only place it is.  The options object's keys count as much as the keywords.
    A query's `options` also holds its positional parameters, and only an instance of the
    options class is read by any of the builders, which is why it is picked out by its type.
    """
    keys = [key for option in options if isinstance(option, options_type) for key in option]
    check_options(block, [*keys, *kwargs], mode, protocol=block.protocol)


def _judged(block: str,
            entry: BlockSpec,
            opts: dict[str, Any],
            mode: OptionValidationMode | None,
            protocol: CouchbaseProtocol) -> dict[str, Any]:
    """One document's options, checked against the block, with each deprecated key reported."""
    check_options(block, opts, mode, protocol=protocol)
    return resolve_deprecated(entry, opts) if entry.retired else opts


def _get_per_key_opts(per_key_opts: dict[str, Any],
                      opt_type: type[dict[str, Any]],
                      durability_type: type[dict] | type[int] | None,
                      protocol: CouchbaseProtocol,
                      mode: OptionValidationMode | None = None) -> dict[str, Any]:
    """The same mapping again, per document, plus the one key that becomes two.

    A client durability arrives as a pair and has to be spelled as one, so `durability` is
    the only option here whose wire key is decided by its own value rather than by the
    entry.  The check above it is the reason this is not a plain second call to `to_wire`:
    a batch whose documents ask for different kinds of durability is rejected, and the
    first document's kind is what the rest are held to.
    """
    per_key_block = opt_type.__name__
    entry = BLOCKS[per_key_block]
    final_key_opts = {}
    for key, opts in per_key_opts.items():
        if not isinstance(opts, (opt_type, dict)):
            raise InvalidArgumentException(message=f'Expected options to be of type {opt_type.__name__} | dict')
        key_opts = {}
        opts = _judged(per_key_block, entry, opts, mode, protocol)
        for opt_key, opt_value in opts.items():
            spec = entry.keys.get(opt_key)
            if spec is None:
                continue
            wire = wire_of(per_key_block, opt_key)
            wire_key = wire.name_for(spec.stored or spec.key)
            if wire_key is None:
                continue
            transformed_value = opt_value if wire.transform is None else wire.transform(opt_value)
            if opt_key == 'durability':
                if durability_type is None:
                    durability_type = dict if isinstance(transformed_value, dict) else int
                if not isinstance(transformed_value, durability_type):
                    raise InvalidArgumentException('Durability must be either client or server for all operations.')
                if isinstance(transformed_value, dict):
                    key_opts['persist_to'] = transformed_value['persist_to']
                    key_opts['replicate_to'] = transformed_value['replicate_to']
                else:
                    key_opts['durability_level'] = transformed_value
            else:
                key_opts[wire_key] = transformed_value

        final_key_opts[key] = key_opts

    return final_key_opts


def get_valid_multi_args(opt_type: type[dict[str, Any]],
                         arg_vars: dict[str, Any] | None,
                         *options: dict[str, Any] | None,
                         protocol: CouchbaseProtocol,
                         mode: OptionValidationMode | None = None) -> dict[str, Any]:
    """The multi-document path: the block's entry is the filter, so a typo cannot pass.

    `drop_none=False` is the difference from the single-document path and it is deliberate:
    an option explicitly given as `None` reaches the core here and is dropped there, and
    which of the two is right is not this function's to decide.
    """
    temp_options = merge(arg_vars, *options)
    check_options(opt_type.__name__, temp_options, mode, protocol=protocol)

    final_opts = to_wire(temp_options, opt_type.__name__, drop_none=False)

    if not temp_options:
        return final_opts

    per_key_opts = temp_options.pop('per_key_options', None)
    if not per_key_opts:
        return final_opts

    durability = final_opts.get('durability', None)
    durability_type = None
    if durability is not None:
        durability_type = dict if isinstance(durability, dict) else int

    final_key_opts = _get_per_key_opts(per_key_opts, opt_type, durability_type, protocol, mode=mode)

    final_opts['per_key_options'] = final_key_opts
    return final_opts


# One value read as several, each from a named attribute of it.  This is the one thing a
# transform cannot say, because a transform answers with a value and these answer with keys.
#
# The attributes are named rather than assumed because the two value types spell them
# differently, `.bucket`/`.scope`/`.collection` on a `TransactionKeyspace` against
# `.bucket_name`/`.name` on a `Scope`.  A value type missing one is then wrong here, where it
# can be read, instead of an `AttributeError` raised at whoever called the options class.
EXPANDS: dict[str, dict[str, dict[str, str]]] = {
    'TransactionConfig': {
        'metadata_collection': {'metadata_bucket': 'bucket',
                                'metadata_scope': 'scope',
                                'metadata_collection': 'collection'},
    },
    'TransactionOptions': {
        'metadata_collection': {'metadata_bucket': 'bucket',
                                'metadata_scope': 'scope',
                                'metadata_collection': 'collection'},
    },
    'TransactionQueryOptions': {
        'scope': {'bucket_name': 'bucket_name',
                  'scope_name': 'name'},
    },
}


def expand(wired: dict[str, Any], expansions: dict[str, dict[str, str]]) -> dict[str, Any]:
    """The keys a value object stands for, in place of the value object.

    The key a `TransactionKeyspace` arrives under is also one of the three it leaves as, which is
    why the value comes out before any of them goes in.
    """
    for key, targets in expansions.items():
        if key not in wired:
            continue
        value = wired.pop(key)
        for target, attribute in targets.items():
            wired[target] = getattr(value, attribute)
    return wired


def transaction_wire(block: str, options: dict[str, Any]) -> dict[str, Any]:
    """A transaction class's options as the binding reads them.

    The class judged them and kept them as the caller wrote them, with no `None` among them, so
    every transform here is handed a value it can convert.  It costs nothing at the binding,
    where every destination is an optional that reads an absent key and an explicit `None` the
    same way.
    """
    return expand(mapped(options, block), EXPANDS[block])
