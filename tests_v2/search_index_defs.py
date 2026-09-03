#  Copyright 2016-2024. Couchbase, Inc.
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

"""FTS index names and definitions for the search test suites.

Api-independent: all three environment modules and all three generated suite trees read the
same names and the same params.

**The definitions are built in code because their keys are only known at run time.**  An index
over a collection keys its mapping on `<scope>.<collection>`, and tests_v2 suffixes both names
with the xdist worker id, so under `-n 2` the real keyspace is
`test-scope-gw0.test-collection-gw0`.  Checked-in JSON can only spell that literally, and an
index whose mapping matches no document is not an error: it returns zero rows, so the suite
fails on a row count with no hint of why.

The index *names* carry the worker id for a different reason: an FTS index is cluster-global,
so two workers running this suite would otherwise upsert and drop one shared index underneath
each other.
"""

from __future__ import annotations

import os
from typing import Any

DEFAULT_INDEX_NAME = 'test-search-index'
COLLECTION_INDEX_NAME = 'test-search-coll-index'
MGMT_INDEX_NAME = 'test-search-mgmt-index'
MGMT_SECONDARY_INDEX_NAME = 'test-search-mgmt-index-2'

# (field name, FTS field type).  Every other flag is identical across all six fields and
# both indexes, so it lives in _field() rather than being repeated per row.  Types are the
# ones the vehicle documents DataProvider seeds actually carry.
_INDEXED_FIELDS: tuple[tuple[str, str], ...] = (
    ('make', 'text'),
    ('model', 'text'),
    ('year', 'number'),
    ('description', 'text'),
    ('rating', 'number'),
    ('last_updated', 'datetime'),
)


def _worker_suffix() -> str:
    """`-gw0` under xdist, empty otherwise.

    Read from the environment rather than from cb_env, so the suites can name an index before
    an environment exists.  It is the same variable the scope and collection names are built
    from, so the three always agree.
    """
    worker_id = os.environ.get('PYTEST_XDIST_WORKER', '')
    return f'-{worker_id}' if worker_id else ''


def default_index_name() -> str:
    """The cluster-level index over the whole bucket, one per xdist worker."""
    return f'{DEFAULT_INDEX_NAME}{_worker_suffix()}'


def collection_index_name() -> str:
    """The collection-aware index, one per xdist worker."""
    return f'{COLLECTION_INDEX_NAME}{_worker_suffix()}'


def mgmt_index_name() -> str:
    """The index the management suites create and drop, one per xdist worker.

    Distinct from default_index_name() and collection_index_name() because the management
    suites drop and re-create theirs on nearly every test, and a search suite sharing the name
    would find its index missing mid-run.
    """
    return f'{MGMT_INDEX_NAME}{_worker_suffix()}'


def mgmt_secondary_index_name() -> str:
    """A second management index, for the test that asserts get_all_indexes sees both."""
    return f'{MGMT_SECONDARY_INDEX_NAME}{_worker_suffix()}'


def _field(name: str, field_type: str) -> dict[str, Any]:
    return {
        'docvalues': True,
        'include_in_all': True,
        'include_term_vectors': True,
        'index': True,
        'name': name,
        'store': True,
        'type': field_type,
    }


def _type_mapping() -> dict[str, Any]:
    """The one mapping both indexes use, for every type they declare."""
    return {
        'dynamic': False,
        'enabled': True,
        'properties': {
            name: {'enabled': True, 'dynamic': False, 'fields': [_field(name, field_type)]}
            for name, field_type in _INDEXED_FIELDS
        },
    }


def _build(doc_config_mode: str, types: dict[str, Any]) -> dict[str, Any]:
    return {
        'doc_config': {
            'docid_prefix_delim': '',
            'docid_regexp': '',
            'mode': doc_config_mode,
            'type_field': 'type',
        },
        'mapping': {
            'default_analyzer': 'standard',
            'default_datetime_parser': 'dateTimeOptional',
            'default_field': '_all',
            'default_mapping': {'dynamic': True, 'enabled': False},
            'default_type': '_default',
            'docvalues_dynamic': True,
            'index_dynamic': False,
            'store_dynamic': False,
            'type_field': '_type',
            'types': types,
        },
        'store': {'indexType': 'scorch'},
    }


def default_index_params() -> dict[str, Any]:
    """Index every `type: vehicle` document in the bucket, ignoring scope and collection.

    `doc_config.mode` is `type_field`, so the type is read from the document's own `type`
    field and the mapping key is the value that field holds.
    """
    return _build('type_field', {'vehicle': _type_mapping()})


def collection_index_params(scope_name: str, collection_names: list[str]) -> dict[str, Any]:
    """Index the named collections, keeping `_$c` available as a returnable field.

    `doc_config.mode` is `scope.collection.type_field`, so a mapping key is
    `<scope>.<collection>` and the documents' own `type` field no longer selects the
    mapping.  That is what lets a scope-level query filter on `collections=[...]` and read
    back which collection a row came from.
    """
    types = {f'{scope_name}.{name}': _type_mapping() for name in collection_names}
    return _build('scope.collection.type_field', types)


# ---------------------------------------------------------------------------
# The "exported from the UI" definition, for the from-json tests
# ---------------------------------------------------------------------------
#
# Code here for the module docstring's reason and for one more.  A scope-level export names
# its index `default.test-scope.<name>` and keys its mapping on `test-scope.test-collection`,
# both literal, so under xdist it addresses a scope that does not exist.  Unlike the search
# definitions, that failure is not a quiet zero-row result; the upsert itself is rejected.
#
# What this definition carries that the search definitions do not is a `sourceType` that is not
# the SDK default, a non-empty `planParams`, and a uuid pair, and each of those is a
# from_json behaviour the suite asserts on rather than decoration.

UI_INDEX_NAME = 'test-search-index-from-json'

# A real UI export carries both.  SearchIndex.from_json drops them on purpose ("cannot pass
# in the UUID or sourceUUID"), so they are here to be dropped, and the suite checks it.
UI_INDEX_UUID = '24b4a143417c7c22'
UI_INDEX_SOURCE_UUID = '367a3da917a4d29dfd28d75fe755c280'

# gocbcore rather than the SearchIndex default of 'couchbase', so a round trip proves the
# value came from the JSON and was not filled in by the dataclass.
UI_INDEX_SOURCE_TYPE = 'gocbcore'

# Non-empty, so plan_params round-trips through something rather than {} == {}.  The values
# are the ones the UI writes for a single-partition index.
UI_INDEX_PLAN_PARAMS: dict[str, Any] = {'maxPartitionsPerPIndex': 1024, 'indexPartitions': 1}


def ui_index_name() -> str:
    """The index the from-json tests create and drop, one per xdist worker."""
    return f'{UI_INDEX_NAME}{_worker_suffix()}'


def ui_index_definition(source_name: str,
                        scope_name: str | None = None,
                        collection_name: str | None = None) -> dict[str, Any]:
    """A whole index definition in the shape the UI exports, for SearchIndex.from_json.

    This is the JSON *document*, not the params block: `name`, `type`, `sourceType` and the
    two uuids sit beside `params`, and the from-json tests read all of them.

    Pass scope_name and collection_name for the scope-level form.  That qualifies the name
    as `<bucket>.<scope>.<index>`, which is what the UI writes for a scope-level index and
    what from_json is expected to strip back to the last token, and it switches the mapping
    to the collection-aware build so the definition is one a scope-level upsert accepts.
    """
    if scope_name is not None and collection_name is not None:
        name = f'{source_name}.{scope_name}.{ui_index_name()}'
        params = collection_index_params(scope_name, [collection_name])
    else:
        name = ui_index_name()
        params = default_index_params()

    return {
        'name': name,
        'type': 'fulltext-index',
        'sourceType': UI_INDEX_SOURCE_TYPE,
        'sourceName': source_name,
        'sourceUUID': UI_INDEX_SOURCE_UUID,
        'uuid': UI_INDEX_UUID,
        'params': params,
        'planParams': dict(UI_INDEX_PLAN_PARAMS),
        # Null in a real UI export, and from_json turns that into {}.  Spelled out
        # rather than omitted so the None-to-{} path is the one under test.
        'sourceParams': None,
    }
