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

"""What a search index manager caller's arguments mean, whichever protocol sends them."""

from __future__ import annotations

import json
from typing import Any

from couchbase.common._utils import is_null_or_empty
from couchbase.common.exceptions import InvalidArgumentException
from couchbase.common.management.search import SearchIndex


def validated_index_name(index_name: object) -> str:
    if not isinstance(index_name, str):
        raise InvalidArgumentException('The index_name must be provided.')
    if is_null_or_empty(index_name):
        raise InvalidArgumentException('Expected non-empty index_name')
    return index_name


def validated_index(index: object) -> SearchIndex:
    if not index:
        raise InvalidArgumentException('Expected index to not be None')
    if not isinstance(index, SearchIndex):
        raise InvalidArgumentException('Expected index to be a SearchIndex')
    if not index.is_valid():
        raise InvalidArgumentException('Index must have name, source set')
    return index


def encoded_document(document: Any) -> str:
    """The document to analyze, as JSON."""
    if not document:
        raise InvalidArgumentException('Expected a non-empty document to analyze.')
    try:
        return json.dumps(document)
    except Exception as ex:
        raise InvalidArgumentException('Cannot convert doc to json to analyze') from ex
