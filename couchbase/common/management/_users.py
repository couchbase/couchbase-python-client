#  Copyright 2016-2023. Couchbase, Inc.
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

from couchbase.common.exceptions import InvalidArgumentException


class UserManagementUtils:
    """
    ** INTERNAL **
    """

    @staticmethod
    def to_set(value: Any, valid_type: Any, display_name: str) -> set[Any]:

        if not value:
            # An empty set, never None: Group.as_dict iterates self.roles unguarded.
            return set()
        elif isinstance(value, set):
            UserManagementUtils.validate_all_set_types(value, valid_type, display_name)
            return value
        elif isinstance(value, list):
            UserManagementUtils.validate_all_set_types(value, valid_type, display_name)
            return set(value)
        elif isinstance(value, tuple):
            UserManagementUtils.validate_all_set_types(value, valid_type, display_name)
            return set(value)
        elif isinstance(value, valid_type):
            return set([value])
        else:
            raise InvalidArgumentException(f'{display_name} must be of type {valid_type.__name__}.')

    @staticmethod
    def validate_all_set_types(value: Any, valid_type: Any, display_name: str) -> None:
        if not all(isinstance(r, valid_type) for r in value):
            raise InvalidArgumentException(f'{display_name} must contain only objects of type {valid_type.__name__}.')
