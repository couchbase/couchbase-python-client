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

from collections.abc import Callable
from dataclasses import dataclass, fields
from typing import Any

from couchbase.common._observability import ObservableRequestHandler
from couchbase.common._option_diagnostics import OptionsBlock
from couchbase.common.exceptions import (AnalyticsLinkExistsException,
                                         AnalyticsLinkNotFoundException,
                                         CouchbaseException,
                                         DatasetAlreadyExistsException,
                                         DatasetNotFoundException,
                                         DataverseAlreadyExistsException,
                                         DataverseNotFoundException)
from couchbase.common.protocol import CouchbaseProtocol
from couchbase.protocol.classic._bindings import AnalyticsMgmtBinding
from couchbase.protocol.classic._management._mgmt_req import MgmtRequest


class AnalyticsMgmtOptionsBlock(OptionsBlock, protocol=CouchbaseProtocol.CLASSIC):
    ConnectLink = 'ConnectLinkOptions'
    CreateAnalyticsIndex = 'CreateAnalyticsIndexOptions'
    CreateDataset = 'CreateDatasetOptions'
    CreateDataverse = 'CreateDataverseOptions'
    CreateLinkAnalytics = 'CreateLinkAnalyticsOptions'
    DisconnectLink = 'DisconnectLinkOptions'
    DropAnalyticsIndex = 'DropAnalyticsIndexOptions'
    DropDataset = 'DropDatasetOptions'
    DropDataverse = 'DropDataverseOptions'
    DropLinkAnalytics = 'DropLinkAnalyticsOptions'
    GetAllAnalyticsIndexes = 'GetAllAnalyticsIndexesOptions'
    GetAllDataset = 'GetAllDatasetOptions'
    GetLinksAnalytics = 'GetLinksAnalyticsOptions'
    GetPendingMutations = 'GetPendingMutationsOptions'
    ReplaceLinkAnalytics = 'ReplaceLinkAnalyticsOptions'


# we have these params on the top-level pycbc_core request
OPARG_SKIP_LIST = ['error_map']
_OPARG_SKIP_SET = frozenset(OPARG_SKIP_LIST)
_FIELDS_CACHE: dict[type, list] = {}


@dataclass
class AnalyticsMgmtRequest(MgmtRequest):

    def req_to_dict(self,
                    obs_handler: ObservableRequestHandler | None = None,
                    callback: Callable[..., None] | None = None,
                    errback: Callable[..., None] | None = None) -> dict[str, Any]:
        cls = type(self)
        cached_fields = _FIELDS_CACHE.get(cls)
        if cached_fields is None:
            cached_fields = [f for f in fields(cls) if f.name not in _OPARG_SKIP_SET]
            _FIELDS_CACHE[cls] = cached_fields

        mgmt_kwargs = {
            f.name: getattr(self, f.name)
            for f in cached_fields
            if getattr(self, f.name) is not None
        }
        if callback is not None:
            mgmt_kwargs['callback'] = callback

        if errback is not None:
            mgmt_kwargs['errback'] = errback

        if obs_handler:
            # TODO(PYCBC-1746): Update once legacy tracing logic is removed
            if obs_handler.is_legacy_tracer:
                legacy_request_span = obs_handler.legacy_request_span
                if legacy_request_span:
                    mgmt_kwargs['parent_span'] = legacy_request_span
            else:
                mgmt_kwargs['wrapper_span_name'] = obs_handler.wrapper_span_name

        return mgmt_kwargs


@dataclass
class ConnectLinkRequest(AnalyticsMgmtRequest):
    dataverse_name: str | None = None
    link_name: str | None = None
    force: bool | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsLinkConnect.value


@dataclass
class CreateDatasetRequest(AnalyticsMgmtRequest):
    dataset_name: str
    bucket_name: str
    dataverse_name: str | None = None
    ignore_if_exists: bool | None = None
    condition: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsDatasetCreate.value


@dataclass
class CreateDataverseRequest(AnalyticsMgmtRequest):
    dataverse_name: str
    ignore_if_exists: bool | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsDataverseCreate.value


@dataclass
class CreateIndexRequest(AnalyticsMgmtRequest):
    index_name: str
    dataset_name: str
    fields: dict[str, str]
    dataverse_name: str | None = None
    ignore_if_exists: bool | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsIndexCreate.value


@dataclass
class CreateAzureBlobExternalLinkRequest(AnalyticsMgmtRequest):
    link: dict[str, Any]
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsLinkCreateAzureBlobExternalLink.value


@dataclass
class CreateCouchbaseRemoteLinkRequest(AnalyticsMgmtRequest):
    link: dict[str, Any]
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsLinkCreateCouchbaseRemoteLink.value


@dataclass
class CreateS3ExternalLinkRequest(AnalyticsMgmtRequest):
    link: dict[str, Any]
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsLinkCreateS3ExternalLink.value


@dataclass
class DisconnectLinkRequest(AnalyticsMgmtRequest):
    dataverse_name: str | None = None
    link_name: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsLinkDisconnect.value


@dataclass
class DropDatasetRequest(AnalyticsMgmtRequest):
    dataset_name: str
    dataverse_name: str | None = None
    ignore_if_does_not_exist: bool | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsDatasetDrop.value


@dataclass
class DropDataverseRequest(AnalyticsMgmtRequest):
    dataverse_name: str
    ignore_if_does_not_exist: bool | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsDataverseDrop.value


@dataclass
class DropIndexRequest(AnalyticsMgmtRequest):
    index_name: str
    dataset_name: str
    dataverse_name: str | None = None
    ignore_if_does_not_exist: bool | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsIndexDrop.value


@dataclass
class DropLinkRequest(AnalyticsMgmtRequest):
    link_name: str
    dataverse_name: str
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsLinkDrop.value


@dataclass
class GetAllDatasetsRequest(AnalyticsMgmtRequest):
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsDatasetGetAll.value


@dataclass
class GetAllIndexesRequest(AnalyticsMgmtRequest):
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsIndexGetAll.value


@dataclass
class GetLinksRequest(AnalyticsMgmtRequest):
    dataverse_name: str | None = None
    link_name: str | None = None
    link_type: str | None = None
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsLinkGetAll.value


@dataclass
class GetPendingMutationsRequest(AnalyticsMgmtRequest):
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsGetPendingMutations.value


@dataclass
class ReplaceAzureBlobExternalLinkRequest(AnalyticsMgmtRequest):
    link: dict[str, Any]
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsLinkReplaceAzureBlobExternalLink.value


@dataclass
class ReplaceCouchbaseRemoteLinkRequest(AnalyticsMgmtRequest):
    link: dict[str, Any]
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsLinkReplaceCouchbaseRemoteLink.value


@dataclass
class ReplaceS3ExternalLinkRequest(AnalyticsMgmtRequest):
    link: dict[str, Any]
    timeout: int | None = None

    @property
    def binding_method(self) -> str:
        return AnalyticsMgmtBinding.AnalyticsLinkReplaceS3ExternalLink.value


ANALYTICS_MGMT_ERROR_MAP: dict[str, type[CouchbaseException]] = {
    r'.*24055.*already exists': AnalyticsLinkExistsException,
    r'.*24006.*does not exist': AnalyticsLinkNotFoundException,
    r'.*24034.*Cannot find': DataverseNotFoundException,
    r'.*24025.*Cannot find analytics (collection)*': DatasetNotFoundException,
    r'.*24039.*An analytics\s+(scope)*.*already exists': DataverseAlreadyExistsException,
    r'.*24040.*An analytics\s+(collection)*.*already exists': DatasetAlreadyExistsException
}


CreateLinkRequest = CreateAzureBlobExternalLinkRequest | CreateCouchbaseRemoteLinkRequest | CreateS3ExternalLinkRequest

ReplaceLinkRequest = (ReplaceAzureBlobExternalLinkRequest |
                      ReplaceCouchbaseRemoteLinkRequest |
                      ReplaceS3ExternalLinkRequest)
