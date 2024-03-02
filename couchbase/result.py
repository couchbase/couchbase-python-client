#  Copyright 2016-2022. Couchbase, Inc.
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

import json
from datetime import datetime
from typing import (Any,
                    Dict,
                    Optional,
                    Tuple,
                    Union)

from acouchbase.analytics import AsyncAnalyticsRequest
from acouchbase.n1ql import AsyncN1QLRequest
from acouchbase.search import AsyncFullTextSearchRequest
from acouchbase.views import AsyncViewRequest
from couchbase.diagnostics import (ClusterState,
                                   EndpointDiagnosticsReport,
                                   EndpointPingReport,
                                   EndpointState,
                                   ServiceType)
from couchbase.exceptions import ErrorMapper, InvalidArgumentException
from couchbase.exceptions import exception as CouchbaseBaseException
from couchbase.pycbc_core import result
from couchbase.subdocument import parse_subdocument_content_as, parse_subdocument_exists


class Result:
    def __init__(
        self,
        orig,  # type: result
    ):

        self._orig = orig

    @property
    def value(self) -> Optional[Any]:
        """
            Optional[Any]: The content of the document, if it exists.
        """
        return self._orig.raw_result.get("value", None)

    @property
    def cas(self) -> Optional[int]:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        return self._orig.raw_result.get("cas", 0)

    @property
    def flags(self) -> Optional[int]:
        """
            Optional[int]: Flags associated with the document.  Used for transcoding.
        """
        return self._orig.raw_result.get("flags", 0)

    @property
    def key(self) -> Optional[str]:
        """
            Optional[str]: Key for the operation, if it exists.
        """
        return self._orig.raw_result.get("key", None)

    @property
    def success(self) -> bool:
        """
            bool: Indicates if the operation was successful or not.
        """
        return self.cas != 0


class ContentProxy:
    """
    Used to provide access to Result content via Result.content_as[type]
    """

    def __init__(self, content):
        self._content = content

    def __getitem__(self,
                    type_       # type: Any
                    ) -> Any:
        """

        :param type_: the type to attempt to cast the result to
        :return: the content cast to the given type, if possible
        """
        return type_(self._content)


class ContentSubdocProxy:
    """
    Used to provide access to LookUpResult content via Result.content_as[type](index)
    """

    def __init__(self, content, key):
        self._content = content
        self._key = key

    def _parse_content_at_index(self, index, type_):
        item = parse_subdocument_content_as(self._content, index, self._key)
        return type_(item)

    def __getitem__(self,
                    type_       # type: Any
                    ) -> Any:
        """

        :param type_: the type to attempt to cast the result to
        :return: the content cast to the given type, if possible
        """
        return lambda index: self._parse_content_at_index(index, type_)


class DiagnosticsResult(Result):

    def __init__(
        self,
        orig,  # type: result
    ):
        super().__init__(orig)
        svc_endpoints = self._orig.raw_result.get("endpoints", None)
        self._endpoints = {}
        if svc_endpoints:
            for service, endpoints in svc_endpoints.items():
                service_type = ServiceType(service)
                self._endpoints[service_type] = []
                for endpoint in endpoints:
                    self._endpoints[service_type].append(
                        EndpointDiagnosticsReport(service_type, endpoint))

    @property
    def id(self) -> str:
        """
            str: The unique identifier for this report.
        """
        return self._orig.raw_result.get("id", None)

    @property
    def version(self) -> int:
        """
            int: The version number of this report.
        """
        return self._orig.raw_result.get("version", None)

    @property
    def sdk(self) -> str:
        """
            str: The name of the SDK which generated this report.
        """
        return self._orig.raw_result.get("sdk", None)

    @property
    def endpoints(self) -> Dict[str, Any]:
        """
            Dict[str, Any]: A map of service endpoints and their diagnostic status.
        """
        return self._endpoints

    @property
    def state(self) -> ClusterState:
        """
            :class:`~couchbase.diagnostics.ClusterState`: The cluster state.
        """
        num_found = 0
        num_connected = 0
        for endpoints in self._endpoints.values():
            for endpoint in endpoints:
                num_found += 1
                if endpoint.state == EndpointState.Connected:
                    num_connected += 1

        if num_found == num_connected:
            return ClusterState.Online
        if num_connected > 0:
            return ClusterState.Degraded
        return ClusterState.Offline

    def as_json(self) -> str:
        """Returns a JSON formatted diagnostics report.

        Returns:
            str: JSON formatted diagnostics report.
        """
        return_val = {
            'version': self.version,
            'id': self.id,
            'sdk': self.sdk,
            'services': {k.value: list(map(lambda epr: epr.as_dict(), v)) for k, v in self.endpoints.items()}
        }

        return json.dumps(return_val)

    def __repr__(self):
        return "DiagnosticsResult:{}".format(self._orig)


class PingResult(Result):

    def __init__(
        self,
        orig,  # type: result
    ):
        super().__init__(orig)
        svc_endpoints = self._orig.raw_result.get("endpoints", None)
        self._endpoints = {}
        if svc_endpoints:
            for service, endpoints in svc_endpoints.items():
                service_type = ServiceType(service)
                self._endpoints[service_type] = []
                for endpoint in endpoints:
                    self._endpoints[service_type].append(
                        EndpointPingReport(service_type, endpoint))

    @property
    def id(self) -> str:
        """
            str: The unique identifier for this report.
        """
        return self._orig.raw_result.get("id", None)

    @property
    def version(self) -> int:
        """
            int: The version number of this report.
        """
        return self._orig.raw_result.get("version", None)

    @property
    def sdk(self) -> str:
        """
            str: The name of the SDK which generated this report.
        """
        return self._orig.raw_result.get("sdk", None)

    @property
    def endpoints(self) -> Dict[str, Any]:
        """
            Dict[str, Any]: A map of service endpoints and their ping status.
        """
        return self._endpoints

    def as_json(self) -> str:
        """Returns a JSON formatted diagnostics report.

        Returns:
            str: JSON formatted diagnostics report.
        """
        return_val = {
            'version': self.version,
            'id': self.id,
            'sdk': self.sdk,
            'services': {k.value: list(map(lambda epr: epr.as_dict(), v)) for k, v in self.endpoints.items()}
        }

        return json.dumps(return_val)

    def __repr__(self):
        return "PingResult:{}".format(self._orig)


class GetReplicaResult(Result):

    @property
    def is_active(self) -> bool:
        """
        ** DEPRECATED ** use is_replica

        bool: True if the result is the active document, False otherwise.
        """
        return not self._orig.raw_result.get('is_replica')

    @property
    def is_replica(self) -> bool:
        """
            bool: True if the result is a replica, False otherwise.
        """
        return self._orig.raw_result.get('is_replica')

    @property
    def content_as(self) -> Any:
        """
            Any: The contents of the document.

            Get the value as a dict::

                res = collection.get_replica(key)
                value = res.content_as[dict]

        """
        return ContentProxy(self.value)

    def __repr__(self):
        return "GetReplicaResult:{}".format(self._orig)


class GetResult(Result):

    @property
    def expiry_time(self) -> Optional[datetime]:
        """
            Optional[datetime]: The expiry of the document, if it was requested.
        """
        time_ms = self._orig.raw_result.get("expiry", None)
        if time_ms:
            return datetime.fromtimestamp(time_ms)
        return None

    @property
    def expiryTime(self) -> Optional[datetime]:
        """
        ** DEPRECATED ** use expiry_time

        Optional[datetime]: The expiry of the document, if it was requested.
        """
        # make this a datetime!
        time_ms = self._orig.raw_result.get("expiry", None)
        if time_ms:
            return datetime.fromtimestamp(time_ms)
        return None

    @property
    def content_as(self) -> Any:
        """
            Any: The contents of the document.

            Get the value as a dict::

                res = collection.get(key)
                value = res.content_as[dict]

        """
        return ContentProxy(self.value)

    def __repr__(self):
        return "GetResult:{}".format(self._orig)


class MultiResult:
    def __init__(self,
                 orig,  # type: result
                 result_type,  # type: Union[GetReplicaResult, GetResult]
                 return_exceptions  # type: bool
                 ):
        self._orig = orig
        self._all_ok = self._orig.raw_result.pop('all_okay', False)
        self._results = {}
        self._result_type = result_type
        for k, v in self._orig.raw_result.items():
            if isinstance(v, CouchbaseBaseException):
                if not return_exceptions:
                    raise ErrorMapper.build_exception(v)
                else:
                    self._results[k] = ErrorMapper.build_exception(v)
            else:
                if isinstance(v, list):
                    self._results[k] = v
                else:
                    self._results[k] = result_type(v)

    @property
    def all_ok(self) -> bool:
        """
            bool: True if all operations succeeded, false otherwise.
        """
        return self._all_ok

    @property
    def exceptions(self) -> Dict[str, CouchbaseBaseException]:
        """
            Dict[str, Exception]: Map of keys to their respective exceptions, if the
                operation had an exception.
        """
        exc = {}
        for k, v in self._results.items():
            if not isinstance(v, self._result_type) and not isinstance(v, list):
                exc[k] = v
        return exc


class MultiGetReplicaResult(MultiResult):
    def __init__(self,
                 orig,  # type: result
                 return_exceptions  # type: bool
                 ):
        super().__init__(orig, GetReplicaResult, return_exceptions)

    @property
    def results(self) -> Dict[str, GetReplicaResult]:
        """
            Dict[str, :class:`.GetReplicaResult`]: Map of keys to their respective :class:`.GetReplicaResult`, if the
                operation has a result.
        """
        res = {}
        for k, v in self._results.items():
            okay = (isinstance(v, GetReplicaResult) or
                    isinstance(v, list) and all(map(lambda r: isinstance(r, GetReplicaResult), v)))
            if okay:
                res[k] = v
        return res

    def __repr__(self):
        output_results = []
        for k, v in self._results.items():
            output_results.append(f'{k}:{v}')

        return f'MultiGetReplicaResult( {", ".join(output_results)} )'


class MultiGetResult(MultiResult):
    def __init__(self,
                 orig,  # type: result
                 return_exceptions  # type: bool
                 ):
        super().__init__(orig, GetResult, return_exceptions)

    @property
    def results(self) -> Dict[str, GetResult]:
        """
            Dict[str, :class:`.GetResult`]: Map of keys to their respective :class:`.GetResult`, if the
                operation has a result.
        """
        res = {}
        for k, v in self._results.items():
            if isinstance(v, GetResult):
                res[k] = v
        return res

    def __repr__(self):
        output_results = []
        for k, v in self._results.items():
            output_results.append(f'{k}:{v}')

        return f'MultiGetResult( {", ".join(output_results)} )'


class ExistsResult(Result):

    @property
    def exists(self) -> bool:
        """
            bool: True if the document exists, false otherwise.
        """
        return self._orig.raw_result.get("exists", False)

    def __repr__(self):
        return "ExistsResult:{}".format(self._orig)


class MultiExistsResult:
    def __init__(self,
                 orig,  # type: result
                 return_exceptions  # type: bool
                 ):

        self._orig = orig
        self._all_ok = self._orig.raw_result.pop('all_okay', False)
        self._results = {}
        for k, v in self._orig.raw_result.items():
            if isinstance(v, CouchbaseBaseException):
                if not return_exceptions:
                    raise ErrorMapper.build_exception(v)
                else:
                    self._results[k] = ErrorMapper.build_exception(v)
            else:
                self._results[k] = ExistsResult(v)

    @property
    def all_ok(self) -> bool:
        """
            bool: True if all operations succeeded, false otherwise.
        """
        return self._all_ok

    @property
    def exceptions(self) -> Dict[str, CouchbaseBaseException]:
        """
            Dict[str, Exception]: Map of keys to their respective exceptions, if the
                operation had an exception.
        """
        exc = {}
        for k, v in self._results.items():
            if not isinstance(v, ExistsResult):
                exc[k] = v
        return exc

    @property
    def results(self) -> Dict[str, ExistsResult]:
        """
            Dict[str, :class:`.MutationResult`]: Map of keys to their respective :class:`.MutationResult`, if the
                operation has a result.
        """
        res = {}
        for k, v in self._results.items():
            if isinstance(v, ExistsResult):
                res[k] = v
        return res

    def __repr__(self):
        output_results = []
        for k, v in self._results.items():
            output_results.append(f'{k}:{v}')

        return f'MultiExistsResult( {", ".join(output_results)} )'


class MutationResult(Result):
    def __init__(self,
                 orig,  # type: result
                 ):
        super().__init__(orig)
        self._raw_mutation_token = self._orig.raw_result.get('mutation_token', None)
        self._mutation_token = None

    def mutation_token(self) -> Optional[MutationToken]:
        """Get the operation's mutation token, if it exists.

        Returns:
            Optional[:class:`.MutationToken`]: The operation's mutation token.
        """
        if self._raw_mutation_token is not None and self._mutation_token is None:
            self._mutation_token = MutationToken(self._raw_mutation_token.get())
        return self._mutation_token

    def __repr__(self):
        return "MutationResult:{}".format(self._orig)


class MultiMutationResult:
    def __init__(self,
                 orig,  # type: result
                 return_exceptions  # type: bool
                 ):

        self._orig = orig
        self._all_ok = self._orig.raw_result.pop('all_okay', False)
        self._results = {}
        for k, v in self._orig.raw_result.items():
            if isinstance(v, CouchbaseBaseException):
                if not return_exceptions:
                    raise ErrorMapper.build_exception(v)
                else:
                    self._results[k] = ErrorMapper.build_exception(v)
            else:
                self._results[k] = MutationResult(v)

    @property
    def all_ok(self) -> bool:
        """
            bool: True if all operations succeeded, false otherwise.
        """
        return self._all_ok

    @property
    def exceptions(self) -> Dict[str, CouchbaseBaseException]:
        """
            Dict[str, Exception]: Map of keys to their respective exceptions, if the
                operation had an exception.
        """
        exc = {}
        for k, v in self._results.items():
            if not isinstance(v, MutationResult):
                exc[k] = v
        return exc

    @property
    def results(self) -> Dict[str, MutationResult]:
        """
            Dict[str, :class:`.MutationResult`]: Map of keys to their respective :class:`.MutationResult`, if the
                operation has a result.
        """
        res = {}
        for k, v in self._results.items():
            if isinstance(v, MutationResult):
                res[k] = v
        return res

    def __repr__(self):
        output_results = []
        for k, v in self._results.items():
            output_results.append(f'{k}:{v}')

        return f'MultiMutationResult( {", ".join(output_results)} )'


MultiResultType = Union[MultiGetResult, MultiMutationResult]


class MutationToken:
    def __init__(self, token  # type: Dict[str, Union[str, int]]
                 ):
        self._token = token

    @property
    def partition_id(self) -> int:
        """
            int:  The token's partition id.
        """
        return self._token['partition_id']

    @property
    def partition_uuid(self) -> int:
        """
            int:  The token's partition uuid.
        """
        return self._token['partition_uuid']

    @property
    def sequence_number(self) -> int:
        """
            int:  The token's sequence number.
        """
        return self._token['sequence_number']

    @property
    def bucket_name(self) -> str:
        """
            str:  The token's bucket name.
        """
        return self._token['bucket_name']

    def as_tuple(self) -> Tuple[int, int, int, str]:
        return (self.partition_id, self.partition_uuid,
                self.sequence_number, self.bucket_name)

    def as_dict(self) -> Dict[str, Union[str, int]]:
        return self._token

    def __repr__(self):
        return "MutationToken:{}".format(self._token)

    def __hash__(self):
        return hash(self.as_tuple())

    def __eq__(self, other):
        if not isinstance(other, MutationToken):
            return False
        return (self.partition_id == other.partition_id
                and self.partition_uuid == other.partition_uuid
                and self.sequence_number == other.sequence_number
                and self.bucket_name == other.bucket_name)


class LookupInResult(Result):
    def exists(self,  # type: LookupInResult
               index  # type: int
               ) -> bool:
        """Check if the subdocument path exists.

        Raises:
            :class:`~couchbase.exceptions.InvalidIndexException`: If the provided index is out of range.

        Returns:
            bool: True if the path exists.  False if the path does not exist.
        """
        return parse_subdocument_exists(self.value, index, self.key)

    @property
    def content_as(self) -> ContentSubdocProxy:
        """
            :class:`.ContentSubdocProxy`: A proxy to return the value at the specified index.

            Get first value as a dict::

                res = collection.lookup_in(key, (SD.get("geo"), SD.exists("city")))
                value = res.content_as[dict](0)
        """
        return ContentSubdocProxy(self.value, self.key)

    def __repr__(self):
        return "LookupInResult:{}".format(self._orig)


class LookupInReplicaResult(Result):
    def exists(self,  # type: LookupInReplicaResult
               index  # type: int
               ) -> bool:
        """Check if the subdocument path exists.

        Raises:
            :class:`~couchbase.exceptions.InvalidIndexException`: If the provided index is out of range.

        Returns:
            bool: True if the path exists.  False if the path does not exist.
        """
        return parse_subdocument_exists(self.value, index, self.key)

    @property
    def content_as(self) -> ContentSubdocProxy:
        """
            :class:`.ContentSubdocProxy`: A proxy to return the value at the specified index.

            Get first value as a dict::

                res = collection.lookup_in(key, (SD.get("geo"), SD.exists("city")))
                value = res.content_as[dict](0)
        """
        return ContentSubdocProxy(self.value, self.key)

    @property
    def is_replica(self) -> bool:
        """
            bool: True if the result is a replica, False otherwise.
        """
        return self._orig.raw_result.get('is_replica')

    def __repr__(self):
        return "LookupInReplicaResult:{}".format(self._orig)


class MutateInResult(MutationResult):

    @property
    def content_as(self) -> ContentSubdocProxy:
        """
            :class:`.ContentSubdocProxy`: A proxy to return the value at the specified index.

            Get first value as a str::

                res = collection.mutate_in(key, (SD.upsert("city", "New City"),
                                                SD.replace("faa", "CTY")))
                value = res.content_as[str](0)
        """
        return ContentSubdocProxy(self.value, self.key)

    def __repr__(self):
        return "MutateInResult:{}".format(self._orig)


class CounterResult(MutationResult):

    # Uncomment and delete previous property when ready to remove cas CounterResult.
    # cas = RemoveProperty('cas')

    @property
    def cas(self) -> int:
        """
            .. warning::
                This property is deprecated and will be removed in a future version.

            int: **DEPRECATED** The CAS of the document.
        """
        return self._orig.raw_result.get("cas", 0)

    @property
    def content(self) -> Optional[int]:
        """
            Optional[int]: The value of the document after the operation completed.
        """
        return self._orig.raw_result.get("content", None)

    def __repr__(self):
        # Uncomment and delete previous return when ready to remove cas from CounterResult. Or, ideally,
        # remove cas from the cxx client's response.
        # return "CounterResult:{}".format({k:v for k,v in self._orig.raw_result.items() if k != 'cas'})
        return "CounterResult:{}".format(self._orig.raw_result)


class MultiCounterResult:
    def __init__(self,
                 orig,  # type: result
                 return_exceptions  # type: bool
                 ):

        self._orig = orig
        self._all_ok = self._orig.raw_result.pop('all_okay', False)
        self._results = {}
        for k, v in self._orig.raw_result.items():
            if isinstance(v, CouchbaseBaseException):
                if not return_exceptions:
                    raise ErrorMapper.build_exception(v)
                else:
                    self._results[k] = ErrorMapper.build_exception(v)
            else:
                self._results[k] = CounterResult(v)

    @property
    def all_ok(self) -> bool:
        """
            bool: True if all operations succeeded, false otherwise.
        """
        return self._all_ok

    @property
    def exceptions(self) -> Dict[str, CouchbaseBaseException]:
        """
            Dict[str, Exception]: Map of keys to their respective exceptions, if the
                operation had an exception.
        """
        exc = {}
        for k, v in self._results.items():
            if not isinstance(v, CounterResult):
                exc[k] = v
        return exc

    @property
    def results(self) -> Dict[str, CounterResult]:
        """
            Dict[str, :class:`.MutationResult`]: Map of keys to their respective :class:`.MutationResult`, if the
                operation has a result.
        """
        res = {}
        for k, v in self._results.items():
            if isinstance(v, CounterResult):
                res[k] = v
        return res

    def __repr__(self):
        output_results = []
        for k, v in self._results.items():
            output_results.append(f'{k}:{v}')

        return f'MultiCounterResult( {", ".join(output_results)} )'


class ClusterInfoResult:
    def __init__(
        self,
        orig  # type: result
    ):
        self._orig = orig
        # version string should be X.Y.Z-XXXX-YYYY
        self._server_version_raw = None
        self._server_version = None
        self._server_version_short = None
        self._server_build = None
        self._is_enterprise = None

    @property
    def nodes(self):
        return self._orig.raw_result.get("nodes", None)

    @property
    def server_version(self) -> Optional[str]:
        if not self._server_version:
            self._set_server_version()

        if self._server_version_raw:
            self._server_version = self._server_version_raw[:10]

        return self._server_version

    @property
    def server_version_short(self) -> Optional[float]:
        """
            Optional[float]: The version of the connected Couchbase Server in Major.Minor form.
        """
        if not self._server_version_short:
            self._set_server_version()

        if self._server_version_raw:
            self._server_version_short = float(self._server_version_raw[:3])

        return self._server_version_short

    @property
    def server_version_full(self) -> Optional[str]:
        """
            Optional[str]: The full version details of the connected Couchbase Server.
        """
        if not self._server_version_raw:
            self._set_server_version()

        return self._server_version_raw

    @property
    def server_version_build(self) -> Optional[int]:
        """
            Optional[int]: The build version of the connected Couchbase Server.
        """
        if not self._server_build:
            self._set_server_version()

        if self._server_version_raw:
            tokens = self._server_version_raw.split("-")
            if len(tokens) == 3:
                self._server_build = int(tokens[1])

        return self._server_build

    @property
    def is_enterprise(self) -> Optional[bool]:
        """
            bool: True if connected Couchbase Server is Enterprise edition, false otherwise.
        """
        if not self._is_enterprise:
            self._set_server_version()

        if self._server_version_raw:
            tokens = self._server_version_raw.split("-")
            if len(tokens) == 3:
                self._is_enterprise = tokens[2].upper() == "ENTERPRISE"

        return self._is_enterprise

    @property
    def is_community(self) -> Optional[bool]:
        """
            bool: True if connected Couchbase Server is Community edition, false otherwise.
        """
        if not self._is_community:
            self._set_server_version()

        if self._server_version_raw:
            tokens = self._server_version_raw.split("-")
            if len(tokens) == 3:
                self._is_community = tokens[2].upper() == "COMMUNITY"

        return self._is_community

    def _set_server_version(self):
        version = None
        for n in self.nodes:
            v = n["version"]
            if version is None:
                version = v
            elif v != version:
                # mixed versions -- not supported
                version = None
                break

        self._server_version_raw = version

    def __repr__(self):
        return "ClusterInfoResult:{}".format(self._orig)


class HttpResult:
    def __init__(
        self,
        orig  # type: result
    ):
        self._orig = orig


class ScanResult(Result):

    def __init__(self, orig, ids_only):
        super().__init__(orig)
        self._ids_only = ids_only

    @property
    def id(self) -> Optional[str]:
        """
            Optional[str]: Id for the operation, if it exists.
        """
        return self._orig.raw_result.get("key", None)

    @property
    def ids_only(self) -> bool:
        """
            bool: True is KV range scan request options set ids_only to True.  False otherwise.
        """
        return self._ids_only

    @property
    def cas(self) -> int:
        """
            Optional[int]: The CAS of the document, if it exists
        """
        if self.ids_only:
            raise InvalidArgumentException(("No cas available when scan is requested with "
                                            "`ScanOptions` ids_only set to True."))
        return self._orig.raw_result.get("cas", 0)

    @property
    def expiry_time(self) -> Optional[datetime]:
        """
            Optional[datetime]: The expiry of the document, if it was requested.
        """
        if self.ids_only:
            raise InvalidArgumentException(("No expiry_time available when scan is requested with "
                                            "`ScanOptions` ids_only set to True."))
        time_ms = self._orig.raw_result.get("expiry", None)
        if time_ms:
            return datetime.fromtimestamp(time_ms)
        return None

    @property
    def content_as(self) -> Any:
        """
            Any: The contents of the document.

            Get the value as a dict::

                res = collection.get(key)
                value = res.content_as[dict]

            Raises:
                :class:`~couchbase.exceptions.InvalidArgumentException`: If called when KV range scan options set
                    without_content to True.

        """
        if self.ids_only:
            raise InvalidArgumentException(("No content available when scan is requested with "
                                            "`ScanOptions` ids_only set to True."))
        return ContentProxy(self.value)

    def __repr__(self):
        return "ScanResult:{}".format(self._orig)


class ScanResultIterable:
    def __init__(
        self,
        scan_request
    ):
        self._request = scan_request

    def rows(self):
        """The rows which have been returned by the query.

        .. note::
            If using the *acouchbase* API be sure to use ``async for`` when looping over rows.

        Returns:
            Iterable: Either an iterable or async iterable.
        """
        # avoid circular import
        from acouchbase.kv_range_scan import AsyncRangeScanRequest  # noqa: F811
        if isinstance(self._request, AsyncRangeScanRequest):
            return self.__aiter__()
        return self.__iter__()

    def cancel_scan(self):
        self._request.cancel_scan()

    def __iter__(self):
        return self._request.__iter__()

    def __aiter__(self):
        return self._request.__aiter__()

    def __repr__(self):
        return "ScanResultIterable:{}".format(self._request)


class QueryResult:
    def __init__(
        self,
        n1ql_request
    ):
        self._request = n1ql_request

    def rows(self):
        """The rows which have been returned by the query.

        .. note::
            If using the *acouchbase* API be sure to use ``async for`` when looping over rows.

        Returns:
            Iterable: Either an iterable or async iterable.
        """
        if isinstance(self._request, AsyncN1QLRequest):
            return self.__aiter__()
        return self.__iter__()

    def execute(self):
        """Convenience method to execute the query.

        Returns:
            List[Any]:  A list of query results.

        Example:
            q_rows = cluster.query('SELECT * FROM `travel-sample` WHERE country LIKE 'United%' LIMIT 2;').execute()

        """
        return self._request.execute()

    def metadata(self):
        """The meta-data which has been returned by the query.

        Returns:
            :class:`~couchbase.n1ql.QueryMetaData`: An instance of :class:`~couchbase.n1ql.QueryMetaData`.
        """
        return self._request.metadata()

    def __iter__(self):
        return self._request.__iter__()

    def __aiter__(self):
        return self._request.__aiter__()

    def __repr__(self):
        return "QueryResult:{}".format(self._request)


class AnalyticsResult:
    def __init__(
        self,
        analytics_request
    ):
        self._request = analytics_request

    def __repr__(self):
        return "AnalyticsResult:{}".format(self._request)

    def rows(self):
        """The rows which have been returned by the analytics query.

        .. note::
            If using the *acouchbase* API be sure to use ``async for`` when looping over rows.

        Returns:
            Iterable: Either an iterable or async iterable.
        """
        if isinstance(self._request, AsyncAnalyticsRequest):
            return self.__aiter__()
        return self.__iter__()

    def metadata(self):
        """The meta-data which has been returned by the analytics query.

        Returns:
            :class:`~couchbase.analytics.AnalyticsMetaData`: An instance of
            :class:`~couchbase.analytics.AnalyticsMetaData`.
        """
        return self._request.metadata()

    def __iter__(self):
        return self._request.__iter__()

    def __aiter__(self):
        return self._request.__aiter__()


class SearchResult:
    def __init__(
        self,
        search_request
    ):
        self._request = search_request

    def __repr__(self):
        return "SearchResult:{}".format(self._request)

    def rows(self):
        """The rows which have been returned by the search query.

        .. note::
            If using the *acouchbase* API be sure to use ``async for`` when looping over rows.

        Returns:
            Iterable: Either an iterable or async iterable.
        """
        if isinstance(self._request, AsyncFullTextSearchRequest):
            return self.__aiter__()
        return self.__iter__()

    def metadata(self):
        """The meta-data which has been returned by the search query.

        Returns:
            :class:`~couchbase.search.SearchMetaData`: An instance of
            :class:`~couchbase.search.SearchMetaData`.
        """
        return self._request.metadata()

    def result_rows(self):
        return self._request.result_rows()

    def facets(self):
        return self._request.result_facets()

    def __iter__(self):
        return self._request.__iter__()

    def __aiter__(self):
        return self._request.__aiter__()


class ViewResult:
    def __init__(
        self,
        search_request
    ):
        self._request = search_request

    def __repr__(self):
        return "ViewResult:{}".format(self._request)

    def rows(self):
        """The rows which have been returned by the view query.

        .. note::
            If using the *acouchbase* API be sure to use ``async for`` when looping over rows.

        Returns:
            Iterable: Either an iterable or async iterable.
        """
        if isinstance(self._request, AsyncViewRequest):
            return self.__aiter__()
        return self.__iter__()

    def metadata(self):
        """The meta-data which has been returned by the view query.

        Returns:
            :class:`~couchbase.views.ViewMetaData`: An instance of
            :class:`~couchbase.views.ViewMetaData`.
        """
        return self._request.metadata()

    def __iter__(self):
        return self._request.__iter__()

    def __aiter__(self):
        return self._request.__aiter__()


class OperationResult:
    """ **DEPRECATED** """

    def __init__(self, cas, mut_token):
        self._mutation_token = mut_token
        self._cas = cas

    @property
    def cas(self) -> Optional[int]:
        return self._cas

    def mutation_token(self) -> Optional[MutationToken]:
        return self._mutation_token

    def __repr__(self):
        return f'OperationResult(mutation_token={self._mutation_token}, cas={self._cas})'


class ValueResult:
    """ **DEPRECATED** """

    def __init__(self, cas, mut_token, value):
        self._mutation_token = mut_token
        self._cas = cas
        self._value = value

    @property
    def cas(self) -> Optional[int]:
        return self._cas

    @property
    def value(self) -> Optional[Any]:
        return self._value

    def mutation_token(self) -> Optional[MutationToken]:
        return self._mutation_token

    def __repr__(self):
        return f'OperationResult(mutation_token={self._mutation_token}, cas={self._cas})'
