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

"""What a query index manager caller's arguments mean, whichever protocol sends them."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from couchbase.common.exceptions import (InvalidArgumentException,
                                         QueryIndexNotFoundException,
                                         WatchQueryIndexTimeoutException)
from couchbase.common.management.queries import QueryIndex

# How long a watch waits between listings of the indexes.
WATCH_POLL_SECONDS = 0.1


def validate_bucket_name(bucket_name: object) -> None:
    if not isinstance(bucket_name, str):
        raise InvalidArgumentException('The bucket_name must be provided when creating a secondary index.')


def validate_index_name(index_name: object) -> None:
    if not isinstance(index_name, str):
        raise InvalidArgumentException('The index_name must be provided when creating a secondary index.')


def watched_names(names: Iterable[str]) -> list[str]:
    """The names to watch, read once, since a watch checks them on every listing."""
    if isinstance(names, str) or not isinstance(names, Iterable):
        raise InvalidArgumentException('An iterable of index names must be provided when watching indexes.')
    return list(names)


def index_keys(keys: Iterable[str]) -> list[str]:
    if isinstance(keys, str) or not isinstance(keys, Iterable):
        raise InvalidArgumentException('Index keys must be provided when creating a secondary index.')
    return list(keys)


def check_create(primary: bool, keys: list[str], condition: str | None) -> None:
    if primary and keys:
        raise InvalidArgumentException('Cannot create primary index with explicit keys')
    if not primary and not keys:
        raise InvalidArgumentException('Keys required for non-primary index')
    if condition and primary:
        raise InvalidArgumentException('cannot specify condition for primary index')


def scope_and_collection(final_args: dict[str, Any],
                         collection_context: tuple[str, str] | None) -> tuple[str | None, str | None]:
    """The scope and collection an operation addresses, taken out of `final_args`.

    `collection_context` is the collection and scope of a collection's own manager, which
    addresses nothing else, so naming a scope or collection in its options is refused.
    """
    if collection_context is None:
        return final_args.pop('scope_name', None), final_args.pop('collection_name', None)
    for name in ('scope_name', 'collection_name'):
        if name in final_args:
            raise InvalidArgumentException(f'{name} cannot be set in the options when using the '
                                           'collection-level query index manager')
    collection_name, scope_name = collection_context
    return scope_name, collection_name


def indexes_online(names: Iterable[str], indexes: Iterable[QueryIndex]) -> bool:
    """Whether every watched index is online; a watched name that is not listed is an error."""
    listed = list(indexes)
    watched = []
    for name in names:
        match = next((i for i in listed if i.name == name), None)
        if not match:
            raise QueryIndexNotFoundException(f'Cannot find index with name: {name}')
        watched.append(match)

    # Only the named indexes: another index on the keyspace, deferred and never built, is not
    # the caller's to wait for.
    return all(i.state == 'online' for i in watched)


def watch_timed_out() -> WatchQueryIndexTimeoutException:
    return WatchQueryIndexTimeoutException('Failed to find all indexes online within the alloted time.')
