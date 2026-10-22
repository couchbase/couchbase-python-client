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

"""The one place a caller's option keys are judged, above the transform and below the API.

One point rather than one per surface, because every surface merges its keywords into a single
mapping before anything looks at them.  What each surface still owns is the block name, since
the class the keywords were meant for is a fact about the call site and not about the dict.

`check_options` never removes a key.  Filtering belongs to the transform, which knows what the
core will accept.

**The keys have to arrive as the caller wrote them, before any default is filled in.**  A key
nobody asked for is nothing to report, and the same key set deliberately is worth a sentence;
inject defaults first and every defaulted key looks like one the user chose.

De-duplication is `warnings`' own, which is once per (message, category, attributed line).  So
the message names the key and never its value, or a loop over bad input emits once per
iteration; and the attribution has to reach the caller, or the first user call site consumes
the registry entry and every other site carrying the same message goes unheard.

The SDK installs no filters of its own.  Any `filterwarnings` call invalidates every existing
de-duplication registry in the process, including the ones the user's own code is relying on,
so `ignore` means a warning that is never emitted rather than one emitted and hidden.
"""

from __future__ import annotations

import os
import warnings
from collections.abc import Iterable, Mapping
from difflib import get_close_matches
from typing import Any, ClassVar

from couchbase.common._option_spec import (BLOCKS,
                                           SUPERSEDED,
                                           BlockSpec)
from couchbase.common._supportability import _caller_stacklevel
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.option_enums import OptionValidationMode, _StrEnum
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.common.warnings import CouchbaseApplicabilityWarning, CouchbaseOptionWarning


class OptionsBlock(_StrEnum):
    """The base a block enum takes, so a member is the name of the options class it stands for.

    A block enum is written by the protocol whose request builders name it, and says which at its
    class line, so naming a block also names whose `inert` answer applies.
    """

    protocol: ClassVar[CouchbaseProtocol]

    def __init_subclass__(cls, *, protocol: CouchbaseProtocol, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.protocol = protocol


# Derived rather than written, so a fourth mode is one line in the enum and nothing else.
MODES = tuple(OptionValidationMode)

# The process-wide default, for the case where the user cannot edit the code that builds
# `ClusterOptions`.
ENV_VAR = 'PYCBC_OPTION_VALIDATION'

# The keyword `ClusterOptions` carries the same dial under, which overrides the variable for
# one cluster.  Neither raises because of the other: a dial has nothing to conflict over.
ENV_KEY = 'option_validation'

# How close a key has to be to a real one to be called a likely typo of it.  The number does
# two jobs.  Where an unrecognised keyword is legitimate input it decides whether anything is
# said at all, so a false positive is noise on correct code; everywhere else the report fires
# whichever way it falls and this only picks the suggestion.
#
# Measured over every key the schema declares and 219065 generated single-edit typos: 0.7
# catches 99.4% of them overall, 99% and 98% on the two blocks where it governs whether to
# warn, and flags none of the 77 field names the sample datasets those two query carry.  One
# number rather than a looser one for the cosmetic half, because the looser one answers
# `an_invalid_option` with `tracing_options` and buys 0.6% more suggestions for it.
NEAR_MISS_CUTOFF = 0.7


def _as_mode(written: Any) -> OptionValidationMode | None:
    return OptionValidationMode(written) if written in MODES else None


def default_mode() -> OptionValidationMode:
    """The mode a cluster takes when it is given none, which the environment may set.

    A variable holding something that is not a mode is reported and then ignored.  It cannot
    raise: the variable is read on the first operation of a process that may have been running
    for hours, and refusing to run an operation over a misspelling in the environment is a
    larger failure than the one being reported.  A value passed in code is the opposite case
    and `validated_mode` raises on it.
    """
    written = (os.environ.get(ENV_VAR) or '').strip().lower()
    if not written:
        return OptionValidationMode.Warn
    mode = _as_mode(written)
    if mode is None:
        warnings.warn('{} is set to {}, which is not one of {}, so option validation falls '
                      'back to {}.'.format(ENV_VAR, written, ', '.join(MODES),
                                           OptionValidationMode.Warn),
                      CouchbaseOptionWarning, stacklevel=_caller_stacklevel())
        return OptionValidationMode.Warn
    return mode


def validated_mode(written: str | OptionValidationMode | None) -> OptionValidationMode | None:
    """One mode as the caller wrote it in code, or `InvalidArgumentException`.

    Raises where `default_mode` warns, and the difference is where the value came from.  A mode
    written in code is written once, by someone who can fix it, before anything the cluster
    does; a misspelling there means every operation under that cluster runs at a mode nobody
    chose.

    `None` is not a value.  A cluster given no mode passes it down and every operation under it
    reads the environment, so setting the variable after the cluster is built still takes
    effect.
    """
    if written is None:
        return None
    mode = _as_mode(written)
    if mode is None:
        raise InvalidArgumentException(
            message='{} must be one of {}; got {!r}.'.format(ENV_KEY, ', '.join(MODES), written))
    return mode


def mode_of(*mappings: Mapping[str, Any] | None) -> str | OptionValidationMode | None:
    """The mode the caller asked for, read out of the mappings a call was given, unvalidated.

    Later wins, matching the order `merge` resolves the same mappings in, so a keyword passed
    at the call overrides the same keyword on the options object.  Answers `None` where nobody
    asked, which is not the default: that is read from the environment at the point of use.
    """
    found = None
    for mapping in mappings:
        written = (mapping or {}).get(ENV_KEY)
        if written:
            found = written
    return found


def _closest(key: str, entry: BlockSpec, cutoff: float) -> str | None:
    """The advertised key this one is most likely a typo of, under its current spelling.

    A retired or unadvertised key is never offered: the first answers a typo with something the
    caller then has to stop using, and the second advertises it.
    """
    suggestions = [name for name, spec in entry.keys.items() if spec.suggest]
    found = get_close_matches(key, suggestions, n=1, cutoff=cutoff)
    return found[0] if found else None


def _report(message: str, category: type, mode: OptionValidationMode) -> None:
    if mode == OptionValidationMode.Strict:
        raise InvalidArgumentException(message=message)
    warnings.warn(message, category, stacklevel=_caller_stacklevel())


def check_options(block: str | OptionsBlock,
                  keys: Iterable[str],
                  mode: str | OptionValidationMode | None = None,
                  *,
                  protocol: CouchbaseProtocol | None) -> None:
    """Judge one call's keys against the block they were written for.

    A block the spec does not name is a gap in the spec and not a mistake by the caller, so it
    answers nothing.  What keeps that from being a silent way to switch validation off is
    that the name is never written at a call site: every caller names a member of an
    `OptionsBlock` enum, and each member is the name of an options class.

    Args:
        block (str | OptionsBlock): The options class the keywords belong to.
        keys (Iterable[str]): The keys the caller set, before any default is filled in.
        mode (str | OptionValidationMode | None): The process default where omitted.
        protocol (CouchbaseProtocol | None): The protocol the keys are sent over, whose `inert`
            answer is read.  `None` where the options are built before any cluster is named,
            which judges everything but applicability.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If `mode` is not a mode, or
            in `strict` on the first key the block does not carry or cannot act on.
    """
    mode = validated_mode(mode) or default_mode()
    if mode == OptionValidationMode.Ignore:
        return
    entry = BLOCKS.get(block)
    if entry is None:
        return
    # The schema names a protocol by its enum name in lower case.
    inert_for = protocol.name.lower() if protocol is not None else None
    for key in keys:
        spec = entry.keys.get(key)
        if spec is not None:
            if inert_for in spec.inert:
                _report('Option {} is accepted by {} and the protocol in use cannot act on it, '
                        'so setting it changes nothing.'.format(key, block),
                        CouchbaseApplicabilityWarning, mode)
            continue
        record = SUPERSEDED.get(key)
        if record is not None and record.superseded_by in entry.keys:
            _report('Option {} is not carried by {}; use {}.'
                    .format(key, block, record.superseded_by),
                    CouchbaseOptionWarning, mode)
            continue
        if entry.free_keywords:
            near = _closest(key, entry, NEAR_MISS_CUTOFF)
            if near is None:
                continue
            _report('Option {} is not carried by {} and will be sent as a named query '
                    'parameter. Did you mean {}?'.format(key, block, near),
                    CouchbaseOptionWarning, mode)
            continue
        near = _closest(key, entry, NEAR_MISS_CUTOFF)
        suffix = ' Did you mean {}?'.format(near) if near else ''
        _report('Option {} is not carried by {}.{}'.format(key, block, suffix),
                CouchbaseOptionWarning, mode)
