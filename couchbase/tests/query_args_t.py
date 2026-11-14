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

from __future__ import annotations

from typing import Any

import pytest

from couchbase.common._query_args import QueryArgs, query_args
from couchbase.options import AnalyticsOptions, QueryOptions

OptionsType = type[QueryOptions] | type[AnalyticsOptions]


@pytest.mark.parametrize('options_type', [QueryOptions, AnalyticsOptions])
class QueryArgsUnitTests:

    def test_options_and_keywords_merge_with_keywords_winning(self, options_type: OptionsType) -> None:
        args = query_args(options_type, (options_type(read_only=True, metrics=False),), {'metrics': True})
        assert args.options == {'read_only': True, 'metrics': True}

    def test_the_last_options_win_and_none_is_a_parameter(self, options_type: OptionsType) -> None:
        args = query_args(options_type, (options_type(read_only=True), None, options_type(metrics=True)), {})
        assert args == QueryArgs(options={'metrics': True}, positional=[None])

    def test_adjacent_options_are_not_parameters(self, options_type: OptionsType) -> None:
        args = query_args(options_type, (options_type(read_only=True), options_type(metrics=True)), {})
        assert args == QueryArgs(options={'metrics': True})

    def test_positional_arguments_are_positional_parameters(self, options_type: OptionsType) -> None:
        args = query_args(options_type, (options_type(positional_parameters=['ignored']), 'a', 2), {})
        assert args.positional == ['a', 2]
        assert 'positional_parameters' not in args.options

    def test_positional_parameters_from_the_options(self, options_type: OptionsType) -> None:
        assert query_args(options_type, (options_type(positional_parameters=['a', 2]),), {}).positional == ['a', 2]

    def test_a_keyword_that_names_no_option_is_a_named_parameter(self, options_type: OptionsType) -> None:
        named: dict[str, Any] = {'city': 'Paris', 'limit': 1}
        args = query_args(options_type, (options_type(named_parameters=named),), {'limit': 5, 'read_only': True})
        assert args.named == {'city': 'Paris', 'limit': 5}
        assert args.options == {'read_only': True}
        assert named == {'city': 'Paris', 'limit': 1}

    def test_a_named_parameter_loses_its_leading_dollar(self, options_type: OptionsType) -> None:
        args = query_args(options_type, (options_type(named_parameters={'$city': 'Paris'}),), {'$limit': 5})
        assert args.named == {'city': 'Paris', 'limit': 5}

    def test_num_workers_is_discarded(self, options_type: OptionsType) -> None:
        assert query_args(options_type, (), {'num_workers': 4}) == QueryArgs()
