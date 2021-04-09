from abc import abstractmethod
from typing import Optional, Mapping, Union, Any
from enum import Enum
from couchbase_core import JSON
from datetime import timedelta
from couchbase.exceptions import InvalidArgumentException
import json
import copy


class EndpointState(Enum):
    Disconnected = "disconnected"
    Connecting = "connecting"
    Connected = "connected"
    Disconnecting = "disconnecting"


class ClusterState(Enum):
    Online = "online"
    Degraded = "degraded"
    Offline = "offline"


class ServiceType(Enum):
    View = "views"
    KeyValue = "kv"
    Query = "n1ql"
    Search = "fts"
    Analytics = "cbas"
    Config = "config"
    Management = "mgmt"


class PingState(Enum):
    OK = 'ok'
    TIMEOUT = 'timeout'
    ERROR = 'error'

class EndPointDiagnostics(object):
    def __init__(self,          # type: EndPointDiagnostics
                 service_type,  # type: ServiceType
                 raw_endpoint   # type: JSON
                 ):
        self._raw_endpoint = raw_endpoint
        self._raw_endpoint['type'] = service_type.value

    @property
    def type(self):
        # type: (...) -> ServiceType
        return ServiceType(self._raw_endpoint.get('type'))

    @property
    def id(self):
        # type: (...) -> str
        return self._raw_endpoint.get('id')

    @property
    def local(self):
        # type: (...) -> str
        return self._raw_endpoint.get('local')

    @property
    def remote(self):
        # type: (...) -> str
        return self._raw_endpoint.get('remote')

    @property
    def last_activity(self):
        # type: (...) -> timedelta
        return timedelta(microseconds=self._raw_endpoint.get('last_activity_us'))

    @property
    def namespace(self):
        # type: (...) -> str
        return self._raw_endpoint.get('scope')

    @property
    def state(self):
        # type: (...) -> EndpointState
        return EndpointState(self._raw_endpoint.get('status'))

    def as_dict(self):
        # type: (...) -> dict
        return self._raw_endpoint

    def as_json(self):
        # type: (...) -> str
        return json.dumps(self.as_dict())


class DiagnosticsResult(object):
    def __init__(self,  # type: DiagnosticsResult
                 source_diagnostics  # type: Union[Mapping[str,Any], list[Mapping[str,Any]]]
                 ):
        self._id = self._version = self._sdk = self._endpoints = None
        # we could have an array of dicts, or just a single dict
        if isinstance(source_diagnostics, dict):
            source_diagnostics = [source_diagnostics]
        if not isinstance(source_diagnostics, list):
            raise InvalidArgumentException("DiagnosticsResult expects a dict or list(dict)")
        for d in source_diagnostics:
            self.append_endpoints(d)

    def as_json(self):
        # type: (...) -> str
        tmp = copy.deepcopy(self.__dict__)
        for k, val in tmp['_endpoints'].items():
            json_vals=[]
            for v in val:
                v_dict = v.as_dict()
                v_dict.pop('type')
                status = v_dict.pop('status')
                v_dict['state'] = status
                json_vals.append(v_dict)
            tmp['_endpoints'][k] = json_vals
        return_val = {
            'version': self.version,
            'id':self.id,
            'sdk': self.sdk
        }
        return_val['services'] = {k.value: v for k, v in tmp['_endpoints'].items()}
        return json.dumps(return_val)

    def append_endpoints(self, source_diagnostics):
        # type: (...) -> None
        # now the remaining keys are the endpoints...
        self._id = source_diagnostics.pop('id', None)
        self._version = source_diagnostics.pop('version', None)
        self._sdk = source_diagnostics.pop('sdk', None)
        if not self._endpoints:
            self._endpoints = dict()
        for k, v in source_diagnostics.items():
            # construct an endpointpingreport for each
            k = ServiceType(k)
            endpoints = self._endpoints.get(k, list())
            for value in v:
                endpoints.append(EndPointDiagnostics(k, value))
            self._endpoints[k] = endpoints

    @property
    def id(self):
        # type: (...) -> str
        return self._id

    @property
    def version(self):
        # type: (...) -> int
        return self._version

    @property
    def sdk(self):
        # type: (...) -> str
        return self._sdk

    @property
    def endpoints(self):
        # type: (...) -> Mapping[ServiceType, list[EndPointDiagnostics]]
        return self._endpoints

    @property
    def state(self):
        # type: (...)-> ClusterState
        num_found = 0
        num_connected = 0
        for k, v in self._endpoints.items():
            for endpoint in v:
                num_found += 1
                if endpoint.state == EndpointState.Connected:
                    num_connected += 1

        if num_found == num_connected:
            return ClusterState.Online
        if num_connected > 0 :
            return ClusterState.Degraded
        return ClusterState.Offline



class EndpointPingReport(object):
    def __init__(self,
                 service_type,  # type: ServiceType
                 source  # type: Mapping[str, Any]
                 ):
        self._src_ping = source
        self._src_ping['service_type'] = service_type

    @property
    def service_type(self):
        # type: (...) -> ServiceType
        return self._src_ping.get('service_type', None)

    @property
    def id(self):
        # type: (...) -> str
        return self._src_ping.get('id', None)

    @property
    def local(self):
        # type: (...) -> str
        return self._src_ping.get('local', None)

    @property
    def remote(self):
        # type: (...) -> str
        return self._src_ping.get('remote', None)

    @property
    def namespace(self):
        # type: (...) -> str
        # was 'scope', now 'namespace'
        return self._src_ping.get('namespace', self._src_ping.get('scope', None))

    @property
    def latency(self):
        # type: (...) -> timedelta
        return timedelta(microseconds=self._src_ping.get('latency_us', None))

    @property
    def state(self):
        # type: (...) -> PingState
        return PingState(self._src_ping.get('status', None))

    def as_dict(self):
        # type: (...) -> dict
        return self._src_ping


