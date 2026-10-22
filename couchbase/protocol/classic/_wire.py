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
a caller's keywords and map them here, and the cluster settings map the connection options here
as they are built.
"""

from __future__ import annotations

from typing import Any

from couchbase.common._option_args import merge, resolve_deprecated
from couchbase.common._option_diagnostics import (OptionsBlock,
                                                  check_options,
                                                  mode_of)
from couchbase.common._option_spec import BLOCKS
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


def wire_keys(block: str) -> list[str]:
    """BRIDGE: one connection block's keys as the binding reads them, for the cluster settings
    that map the connection options as they are built.  Removed by PYCBC-1994.
    """
    return [name for key, spec in BLOCKS[block].keys.items()
            if (name := wire_of(block, key).name_for(spec.stored or spec.key)) is not None]


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
            raise InvalidArgumentException(message=f'Expected options to be of type Union[{opt_type.__name__}, dict]')
        key_opts = {}
        check_options(per_key_block, opts, mode, protocol=protocol)
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
