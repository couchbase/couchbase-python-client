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

"""The requests a search index manager's builder returns, and the index definitions the gateway sends back.

The gateway carries each of a definition's params, plan params and source params as a map from the
object's top-level members to each member's JSON, rather than as one JSON object.  A member that is
not JSON fails the whole conversion: the caller edits what `get_index` returned and upserts it, so a
member dropped on the way in would be deleted from the index by a call that reports success.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass
from typing import (TYPE_CHECKING,
                    Any,
                    Generic,
                    TypeVar)

from couchbase.common._option_diagnostics import OptionsBlock
from couchbase.common.exceptions import ParsingFailedException, SearchIndexNotFoundException
from couchbase.common.management.search import SearchIndex
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.protocol.couchbase2._generated.admin.search.v1 import search_pb2

if TYPE_CHECKING:
    from couchbase.common._observability import ObservableRequestHandler

M = TypeVar('M')


class SearchIndexMgmtOptionsBlock(OptionsBlock, protocol=CouchbaseProtocol.COUCHBASE2):
    AllowQueryingSearchIndex = 'AllowQueryingSearchIndexOptions'
    AnalyzeDocumentSearchIndex = 'AnalyzeDocumentSearchIndexOptions'
    DisallowQueryingSearchIndex = 'DisallowQueryingSearchIndexOptions'
    DropSearchIndex = 'DropSearchIndexOptions'
    FreezePlanSearchIndex = 'FreezePlanSearchIndexOptions'
    GetAllSearchIndexes = 'GetAllSearchIndexesOptions'
    GetSearchIndex = 'GetSearchIndexOptions'
    GetSearchIndexedDocumentsCount = 'GetSearchIndexedDocumentsCountOptions'
    PauseIngestSearchIndex = 'PauseIngestSearchIndexOptions'
    ResumeIngestSearchIndex = 'ResumeIngestSearchIndexOptions'
    UnfreezePlanSearchIndex = 'UnfreezePlanSearchIndexOptions'
    UpsertSearchIndex = 'UpsertSearchIndexOptions'


@dataclass(frozen=True)
class SearchIndexMgmtRequest(Generic[M]):
    message: M
    timeout: float
    obs_handler: ObservableRequestHandler | None


def set_index_fields(index: SearchIndex, message: search_pb2.CreateIndexRequest | search_pb2.Index) -> None:
    """`index`'s definition, written into a create or into the index an update carries.

    The two messages spell the definition alike but are unrelated types.  Neither carries the uuid
    here: only an update has one, and the caller sets it.
    """
    message.name = index.name
    message.type = index.idx_type
    if index.source_name:
        message.source_name = index.source_name
    if index.source_type:
        message.source_type = index.source_type
    if index.source_uuid:
        message.source_uuid = index.source_uuid
    _fill(message.params, index.params)
    _fill(message.plan_params, index.plan_params)
    _fill(message.source_params, index.source_params)


def search_index(index: search_pb2.Index) -> SearchIndex:
    return SearchIndex(index.name,
                       index.source_type,
                       index.type,
                       index.source_name or None,
                       index.uuid or None,
                       _read(index.name, 'params', index.params),
                       index.source_uuid or None,
                       _read(index.name, 'source_params', index.source_params),
                       _read(index.name, 'plan_params', index.plan_params))


def found_index(index_name: str, response: search_pb2.GetIndexResponse) -> SearchIndex:
    """The index a get found.  A success that carries none describes no index, so it is reported as missing."""
    if not response.HasField('index'):
        raise SearchIndexNotFoundException(f'Search index {index_name!r} was not found.')
    return search_index(response.index)


def analysis(response: search_pb2.AnalyzeDocumentResponse) -> dict[str, Any]:
    """The analysis, with the search service's own status, or `ok` when it gave none."""
    try:
        analyzed = json.loads(response.analyzed)
    except ValueError as ex:
        raise ParsingFailedException('The document analysis is not JSON.') from ex
    return {'analysis': analyzed, 'status': response.status or 'ok'}


def _fill(target: MutableMapping[str, bytes], params: Mapping[str, Any]) -> None:
    for key, value in params.items():
        target[key] = json.dumps(value).encode()


def _read(index_name: str, what: str, source: Mapping[str, bytes]) -> dict[str, Any]:
    read = {}
    for key, value in source.items():
        try:
            read[key] = json.loads(value)
        except ValueError as ex:
            raise ParsingFailedException(f'The {what} member {key!r} of search index {index_name!r} '
                                         'is not JSON.') from ex
    return read
