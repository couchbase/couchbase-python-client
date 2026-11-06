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

__all__ = [
    'SearchIndex',
]

import json
from dataclasses import dataclass, field
from typing import Any

from couchbase.common._utils import is_null_or_empty
from couchbase.common.exceptions import InvalidArgumentException


@dataclass
class SearchIndex:
    """Object representation for a Couchbase search index.

    Args:
        name (str): Name of the index.
        source_type (str, optional): Type of the data source.
        idx_type (str, optional): Type of the index.
        source_name (str): Name of the source of the data for the index (e.g. bucket name).
        uuid (str, optional): UUID of the index. It must be specified for an update.
        params (dict, optional): Index properties such as store type and mappings
        source_uuid (str, optional): The UUID of the data source, this can be used to more tightly tie the index to a
            source.
        source_params (dict, optional): Extra parameters for the source. These can include advanced connection settings
            and tuning.
        plan_params (dict, optional): Plan properties such as number of replicas and number of partitions.
    """

    name: str
    source_type: str = 'couchbase'
    idx_type: str = 'fulltext-index'
    source_name: str | None = None
    uuid: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    source_uuid: str | None = None
    source_params: dict[str, Any] = field(default_factory=dict)
    plan_params: dict[str, Any] = field(default_factory=dict)

    def is_valid(self) -> bool:
        """Check that the index has a name, a type and a source type.

        Returns:
            bool: True if none of the three is empty.
        """
        return not (is_null_or_empty(self.name)
                    or is_null_or_empty(self.idx_type)
                    or is_null_or_empty(self.source_type))

    def as_dict(self) -> dict[str, Any]:
        """**INTERNAL** The index as the dict the management request is built from."""
        output = {
            'name': self.name,
            'type': self.idx_type,
            'source_type': self.source_type
        }
        if self.uuid:
            output['uuid'] = self.uuid
        if len(self.params) > 0:
            output['params_json'] = json.dumps(self.params)
        if self.source_uuid:
            output['source_uuid'] = self.source_uuid
        if self.source_name:
            output['source_name'] = self.source_name
        if len(self.source_params) > 0:
            output['source_params_json'] = json.dumps(self.source_params)
        if len(self.plan_params) > 0:
            output['plan_params_json'] = json.dumps(self.plan_params)
        return output

    @classmethod
    def from_server(cls, json_data: dict[str, Any]) -> SearchIndex:
        """**INTERNAL** The index the search service described."""
        params = {}
        params_json = json_data.get('params_json', None)
        if params_json:
            params = json.loads(params_json)

        source_params = {}
        source_params_json = json_data.get('source_params_json', None)
        if source_params_json:
            source_params = json.loads(source_params_json)

        plan_params = {}
        plan_params_json = json_data.get('plan_params_json', None)
        if plan_params_json:
            plan_params = json.loads(plan_params_json)

        return cls(json_data['name'],
                   json_data['source_type'],
                   json_data['type'],
                   json_data.get('source_name', None),
                   json_data.get('uuid', None),
                   params,
                   json_data.get('source_uuid', None),
                   source_params,
                   plan_params
                   )

    @classmethod
    def from_json(cls, json_input: str | dict[str, Any]) -> SearchIndex:
        """Creates a :class:`.SearchIndex` from a provided JSON str or Python dict derived from JSON.

        Args:
            json_input (str | dict[str, Any]): JSON representation of the search index.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the provided JSON is not str or dict.
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the provided JSON does not include an index
                name.
        """
        json_data = json_input
        if isinstance(json_input, str):
            json_data = json.loads(json_input)

        if not isinstance(json_data, dict):
            msg = 'Provided JSON input is either not a Python dict or a JSON str that produces a Python dict.'
            raise InvalidArgumentException(msg)

        name = json_data.get('name', None)
        if name is None:
            raise InvalidArgumentException('Provided JSON input does not contain an index name.')
        if not isinstance(name, str):
            raise InvalidArgumentException('Index name must be a str.')

        name_tokens = name.split('.')
        if len(name_tokens) > 1:
            name = name_tokens[len(name_tokens)-1]

        source_type = json_data.get('sourceType', 'couchbase')
        idx_type = json_data.get('type', 'fulltext-index')
        source_name = json_data.get('sourceName', None)
        # cannot pass in the UUID or sourceUUID
        uuid = None
        source_uuid = None
        params = json_data.get('params', None)
        if params is None:
            params = {}

        source_params = json_data.get('sourceParams', None)
        if source_params is None:
            source_params = {}
        plan_params = json_data.get('planParams', None)
        if plan_params is None:
            plan_params = {}

        return cls(name,
                   source_type,
                   idx_type,
                   source_name,
                   uuid,
                   params,
                   source_uuid,
                   source_params,
                   plan_params)
