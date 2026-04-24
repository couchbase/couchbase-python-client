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

from dataclasses import dataclass
from typing import (Any,
                    Dict,
                    Tuple)

from couchbase.transcoder import Transcoder


@dataclass
class CollectionDetails:
    bucket_name: str
    scope_name: str
    collection_name: str
    default_transcoder: Transcoder

    def get_details(self) -> Tuple[str, str, str]:
        return self.bucket_name, self.scope_name, self.collection_name

    def get_details_as_dict(self) -> Dict[str, str]:
        return {
            'bucket_name': self.bucket_name,
            'scope_name': self.scope_name,
            'collection_name': self.collection_name
        }

    def get_details_as_txn_dict(self) -> Dict[str, str]:
        return {
            'bucket': self.bucket_name,
            'scope': self.scope_name,
            'collection_name': self.collection_name
        }

    def get_request_transcoder(self, op_args: Dict[str, Any]) -> Transcoder:
        return op_args.pop('transcoder', self.default_transcoder)
