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
from acouchbase.search import AsyncSearchRequest
from acouchbase.views import AsyncViewRequest
from couchbase.diagnostics import (ClusterState,
                                   EndpointDiagnosticsReport,
                                   EndpointPingReport,
                                   EndpointState,
                                   ServiceType)
from couchbase.exceptions import (CLIENT_ERROR_MAP,
                                  CouchbaseException,
                                  DocumentNotFoundException,
                                  InvalidIndexException,
                                  PathExistsException,
                                  PathMismatchException,
                                  PathNotFoundException,
                                  SubdocCantInsertValueException)
from couchbase.pycbc_core import exception, result
from couchbase.subdocument import SubDocStatus


class Result:
    def __init__(
        self,
        orig,  # type: result
        should_raise=True,  # type: bool
    ):
        if should_raise and orig.err():
            base = exception(orig)
            klass = CLIENT_ERROR_MAP.get(orig.err(), CouchbaseException)
            raise klass(base)

        self._orig = orig

    @property
    def value(self):
        return self._orig.raw_result.get("value", None)

    @property
    def cas(self):
        return self._orig.raw_result.get("cas", 0)

    @property
    def flags(self):
        return self._orig.raw_result.get("flags", 0)

    @property
    def key(self):
        return self._orig.raw_result.get("key", None)


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
        if index > len(self._content) - 1 or index < 0:
            raise InvalidIndexException(
                f"Provided index ({index}) is invalid.")

        item = self._content[index].get("value", None)
        if item is None:
            # TODO:  implement exc_from_rc()??
            status = self._content[index].get("status", None)
            if not status:
                raise DocumentNotFoundException(
                    f"Could not find document for key: {self._key}")
            if status == SubDocStatus.PathNotFound:
                path = self._content[index].get("path", None)
                raise PathNotFoundException(
                    f"Path ({path}) could not be found for key: {self._key}")
            if status == SubDocStatus.PathMismatch:
                path = self._content[index].get("path", None)
                raise PathMismatchException(
                    f"Path ({path}) mismatch for key: {self._key}")
            if status == SubDocStatus.ValueCannotInsert:
                path = self._content[index].get("path", None)
                raise SubdocCantInsertValueException(
                    f"Cannot insert value at path ({path}) for key: {self._key}")
            if status == SubDocStatus.PathExists:
                path = self._content[index].get("path", None)
                raise PathExistsException(
                    f"Path ({path}) already exists for key: {self._key}")

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
        return self._orig.raw_result.get("id", None)

    @property
    def version(self) -> int:
        return self._orig.raw_result.get("version", None)

    @property
    def sdk(self) -> str:
        return self._orig.raw_result.get("sdk", None)

    @property
    def endpoints(self) -> Dict[str, Any]:
        return self._endpoints

    @property
    def state(self) -> ClusterState:
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
        return self._orig.raw_result.get("id", None)

    @property
    def version(self) -> int:
        return self._orig.raw_result.get("version", None)

    @property
    def sdk(self) -> str:
        return self._orig.raw_result.get("sdk", None)

    @property
    def endpoints(self) -> Dict[str, Any]:
        return self._endpoints

    def as_json(self) -> str:

        return_val = {
            'version': self.version,
            'id': self.id,
            'sdk': self.sdk,
            'services': {k.value: list(map(lambda epr: epr.as_dict(), v)) for k, v in self.endpoints.items()}
        }

        return json.dumps(return_val)

    def __repr__(self):
        return "PingResult:{}".format(self._orig)


class GetResult(Result):
    @property
    def expiry_time(self):
        # make this a datetime!
        time_ms = self._orig.raw_result.get("expiry", None)
        if time_ms:
            return datetime.fromtimestamp(time_ms)
        return None

    @property
    def expiryTime(self) -> datetime:
        """Document expiry

        ** DEPRECATED ** use expiry_time

        Returns:
            datetime: Document expiry as datetime
        """
        # make this a datetime!
        time_ms = self._orig.raw_result.get("expiry", None)
        if time_ms:
            return datetime.fromtimestamp(time_ms)
        return None

    @property
    def content_as(self  # type: GetResult
                   ) -> Any:
        return ContentProxy(self.value)

    def __repr__(self):
        return "GetResult:{}".format(self._orig)


class ExistsResult(Result):

    def __init__(
        self,
        orig,  # type: result
    ):
        should_raise = False
        if orig.strerror() is not None:
            should_raise = orig.strerror().lower() != "document_not_found"
        super().__init__(orig, should_raise=should_raise)

    @property
    def exists(self):
        return self._orig.raw_result.get("exists", False)

    def __repr__(self):
        return "ExistsResult:{}".format(self._orig)


class MutationResult(Result):
    def __init__(self,
                 orig,  # type: result
                 ):
        super().__init__(orig)
        self._raw_mutation_token = self._orig.raw_result.get('mutation_token', None)
        self._mutation_token = None

    def mutation_token(self) -> Optional[MutationToken]:
        if self._raw_mutation_token is not None and self._mutation_token is None:
            self._mutation_token = MutationToken(self._raw_mutation_token.get())
        return self._mutation_token

    def __repr__(self):
        return "MutationResult:{}".format(self._orig)


class MutationToken:
    def __init__(self, token  # type: Dict[str, Union[str, int]]
                 ):
        self._token = token

    @property
    def partition_id(self) -> int:
        return self._token['partition_id']

    @property
    def partition_uuid(self) -> int:
        return self._token['partition_uuid']

    @property
    def sequence_number(self) -> int:
        return self._token['sequence_number']

    @property
    def bucket_name(self) -> str:
        return self._token['bucket_name']

    def as_tuple(self) -> Tuple(int, int, int, str):
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
    def exists(self,  # type: "LookupInResult"
               index  # type: int
               ) -> bool:

        if index > len(self.value) - 1 or index < 0:
            raise InvalidIndexException(
                f"Provided index ({index}) is invalid.")

        exists = self.value[index].get("exists", None)
        return exists is not None and exists is True

    @property
    def content_as(self) -> ContentSubdocProxy:

        return ContentSubdocProxy(self.value, self.key)

    def __repr__(self):
        return "LookupInResult:{}".format(self._orig)


class MutateInResult(MutationResult):

    @property
    def content_as(self) -> ContentSubdocProxy:

        return ContentSubdocProxy(self.value, self.key)

    def __repr__(self):
        return "MutateInResult:{}".format(self._orig)


class CounterResult(MutationResult):

    @property
    def content(self) -> int:
        return self._orig.raw_result.get("content", False)

    def __repr__(self):
        return "CounterResult:{}".format(self._orig)


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
        if not self._server_version_short:
            self._set_server_version()

        if self._server_version_raw:
            self._server_version_short = float(self._server_version_raw[:3])

        return self._server_version_short

    @property
    def server_version_build(self) -> Optional[int]:
        if not self._server_build:
            self._set_server_version()

        if self._server_version_raw:
            tokens = self._server_version_raw.split("-")
            if len(tokens) == 3:
                self._server_build = int(tokens[1])

        return self._server_build

    @property
    def is_enterprise(self) -> Optional[bool]:
        if not self._is_enterprise:
            self._set_server_version()

        if self._server_version_raw:
            tokens = self._server_version_raw.split("-")
            if len(tokens) == 3:
                self._is_enterprise = tokens[2].upper() == "ENTERPRISE"

        return self._is_enterprise

    @property
    def is_community(self) -> Optional[bool]:
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


class QueryResult:
    def __init__(
        self,
        n1ql_request
    ):
        self._request = n1ql_request

    def __repr__(self):
        return "QueryResult:{}".format(self._request)

    def rows(self):
        if isinstance(self._request, AsyncN1QLRequest):
            return self.__aiter__()
        return self.__iter__()

    def execute(self):
        """
        Convenience method
        """
        return self._request.execute()

    def metadata(self):
        return self._request.metadata()

    def __iter__(self):
        return self._request.__iter__()

    def __aiter__(self):
        return self._request.__aiter__()


class AnalyticsResult:
    def __init__(
        self,
        analytics_request
    ):
        self._request = analytics_request

    def __repr__(self):
        return "AnalyticsResult:{}".format(self._request)

    def rows(self):
        if isinstance(self._request, AsyncAnalyticsRequest):
            return self.__aiter__()
        return self.__iter__()

    def metadata(self):
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
        if isinstance(self._request, AsyncSearchRequest):
            return self.__aiter__()
        return self.__iter__()

    def metadata(self):
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
        if isinstance(self._request, AsyncViewRequest):
            return self.__aiter__()
        return self.__iter__()

    def metadata(self):
        return self._request.metadata()

    def __iter__(self):
        return self._request.__iter__()

    def __aiter__(self):
        return self._request.__aiter__()
