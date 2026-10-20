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

import copy
import pathlib

import pytest

from tools.autogen.core.operations import (OperationsError,
                                           parse,
                                           stale)

ROOT = pathlib.Path(__file__).resolve().parents[2]

_BASE = {
    'kinds': {'KeyValue': '', 'KeyValueMulti': '', 'Management': ''},
    'services': {'KeyValue': 'kv', 'Search': 'search'},
    'groups': [
        {'kind': 'KeyValue', 'service': 'KeyValue', 'span': 'name',
         'ops': {'Get': {}, 'Upsert': {}}},
        {'kind': 'KeyValueMulti', 'service': 'KeyValue', 'span': 'name',
         'ops': {'GetMulti': {'single': 'Get'}}},
        {'kind': 'Management', 'service': 'Search',
         'ops': {'SearchIndexPauseIngest': {'span': 'manager_search_pause_ingest'}}},
    ],
}


def _with(group: int, op: str, spec: dict) -> dict:
    data = copy.deepcopy(_BASE)
    data['groups'][group]['ops'][op] = spec
    return data


def test_the_tree_is_current() -> None:
    assert not stale(ROOT)


def test_rows() -> None:
    rows = {r.name: r for r in parse(_BASE).rows}
    assert rows['Get'].span_name == 'get'
    assert rows['SearchIndexPauseIngest'].value == 'search_index_pause_ingest'
    assert rows['SearchIndexPauseIngest'].span_name == 'manager_search_pause_ingest'


@pytest.mark.parametrize('data, message', [
    (_with(1, 'UpsertMulti', {}), 'exactly the KeyValueMulti rows'),
    (_with(0, 'Touch', {'single': 'Get'}), 'exactly the KeyValueMulti rows'),
    (_with(1, 'GetMulti', {'single': 'GetMulti'}), 'is not a KeyValue row'),
    (_with(2, 'SearchIndexResumeIngest', {}), 'no span'),
    (_with(2, 'SearchIndexResumeIngest', {'span': 'manager_search_pause_ingest'}), 'span name given twice'),
    (_with(2, 'Get', {'span': 'other'}), 'row given twice'),
    (_with(0, 'Touch', {'retries': True}), 'unknown keys'),
    (_with(0, 'GetAnyReplica', {'single': 'Get'}), 'exactly the KeyValueMulti rows'),
])
def test_a_bad_table_is_refused(data: dict, message: str) -> None:
    with pytest.raises(OperationsError, match=message):
        parse(data)
