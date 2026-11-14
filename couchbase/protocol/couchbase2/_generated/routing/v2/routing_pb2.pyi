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

# !! AUTO-GENERATED from tools/autogen/config/protos.yaml: DO NOT EDIT MANUALLY !!
# Regenerate with `python -m tools.autogen protos generate --write`.

from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class WatchRoutingRequest(_message.Message):
    __slots__ = ("bucket_name",)
    BUCKET_NAME_FIELD_NUMBER: _ClassVar[int]
    bucket_name: str
    def __init__(self, bucket_name: _Optional[str] = ...) -> None: ...

class ServerRouting(_message.Message):
    __slots__ = ("num_local_servers", "num_group_servers")
    NUM_LOCAL_SERVERS_FIELD_NUMBER: _ClassVar[int]
    NUM_GROUP_SERVERS_FIELD_NUMBER: _ClassVar[int]
    num_local_servers: int
    num_group_servers: int
    def __init__(self, num_local_servers: _Optional[int] = ..., num_group_servers: _Optional[int] = ...) -> None: ...

class VbucketRouting(_message.Message):
    __slots__ = ("num_vbuckets", "local_vbuckets", "group_vbuckets")
    NUM_VBUCKETS_FIELD_NUMBER: _ClassVar[int]
    LOCAL_VBUCKETS_FIELD_NUMBER: _ClassVar[int]
    GROUP_VBUCKETS_FIELD_NUMBER: _ClassVar[int]
    num_vbuckets: int
    local_vbuckets: _containers.RepeatedScalarFieldContainer[int]
    group_vbuckets: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, num_vbuckets: _Optional[int] = ..., local_vbuckets: _Optional[_Iterable[int]] = ..., group_vbuckets: _Optional[_Iterable[int]] = ...) -> None: ...

class WatchRoutingResponse(_message.Message):
    __slots__ = ("server_routing", "views_routing", "query_routing", "search_routing", "analytics_routing", "vbucket_data_routing")
    SERVER_ROUTING_FIELD_NUMBER: _ClassVar[int]
    VIEWS_ROUTING_FIELD_NUMBER: _ClassVar[int]
    QUERY_ROUTING_FIELD_NUMBER: _ClassVar[int]
    SEARCH_ROUTING_FIELD_NUMBER: _ClassVar[int]
    ANALYTICS_ROUTING_FIELD_NUMBER: _ClassVar[int]
    VBUCKET_DATA_ROUTING_FIELD_NUMBER: _ClassVar[int]
    server_routing: ServerRouting
    views_routing: ServerRouting
    query_routing: ServerRouting
    search_routing: ServerRouting
    analytics_routing: ServerRouting
    vbucket_data_routing: VbucketRouting
    def __init__(self, server_routing: _Optional[_Union[ServerRouting, _Mapping]] = ..., views_routing: _Optional[_Union[ServerRouting, _Mapping]] = ..., query_routing: _Optional[_Union[ServerRouting, _Mapping]] = ..., search_routing: _Optional[_Union[ServerRouting, _Mapping]] = ..., analytics_routing: _Optional[_Union[ServerRouting, _Mapping]] = ..., vbucket_data_routing: _Optional[_Union[VbucketRouting, _Mapping]] = ...) -> None: ...
