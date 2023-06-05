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

import copy
import ctypes
from datetime import timedelta
from enum import Enum, IntEnum
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    List,
                    Optional,
                    Tuple,
                    Union,
                    overload)

from couchbase._utils import (timedelta_as_microseconds,
                              timedelta_as_timestamp,
                              validate_bool,
                              validate_int,
                              validate_str)
from couchbase.exceptions import InvalidArgumentException

if TYPE_CHECKING:
    from couchbase._utils import JSONType
    from couchbase.analytics import AnalyticsScanConsistency
    from couchbase.auth import Authenticator
    from couchbase.diagnostics import ClusterState, ServiceType
    from couchbase.durability import DurabilityType
    from couchbase.management.views import DesignDocumentNamespace
    from couchbase.metrics import CouchbaseMeter
    from couchbase.mutation_state import MutationState
    from couchbase.n1ql import QueryProfile, QueryScanConsistency
    from couchbase.search import (Facet,
                                  HighlightStyle,
                                  SearchScanConsistency,
                                  Sort)
    from couchbase.serializer import Serializer
    from couchbase.subdocument import StoreSemantics
    from couchbase.tracing import CouchbaseTracer
    from couchbase.transcoder import Transcoder
    from couchbase.views import (ViewErrorMode,
                                 ViewOrdering,
                                 ViewScanConsistency)


OptionsBase = dict


def _get_temp_opts(
    arg_vars,  # type: Optional[Dict[str,Any]]
    *options  # type: OptionsBase
) -> Dict[str, Any]:
    arg_vars = copy.copy(arg_vars) if arg_vars else {}
    temp_options = (
        copy.copy(
            options[0]) if (
            options and options[0]) else dict())
    kwargs = arg_vars.pop("kwargs", {})
    temp_options.update(kwargs)
    temp_options.update(arg_vars)

    return temp_options


def get_valid_args(
    opt_type,  # type: OptionsBase
    arg_vars,  # type: Optional[Dict[str,Any]]
    *options  # type: OptionsBase
) -> Dict[str, Any]:

    arg_vars = copy.copy(arg_vars) if arg_vars else {}
    temp_options = (
        copy.copy(
            options[0]) if (
            options and options[0]) else dict())
    kwargs = arg_vars.pop("kwargs", {})
    temp_options.update(kwargs)
    temp_options.update(arg_vars)

    valid_opts = opt_type.get_valid_options()
    final_options = {}
    for opt_key, opt_value in temp_options.items():
        if opt_key not in valid_opts:
            continue
        for final_key, transform in valid_opts[opt_key].items():
            converted = transform(opt_value)
            if converted is not None:
                final_options[final_key] = converted

    return final_options


VALID_MULTI_OPTS = {
    'timeout': timedelta_as_microseconds,
    'expiry': timedelta_as_timestamp,
    'preserve_expiry': validate_bool,
    'with_expiry': validate_bool,
    'cas': validate_int,
    'durability': lambda x: x,
    'transcoder': lambda x: x,
    'span': lambda x: x,
    'project': lambda x: x,
    'delta': lambda x: x,
    'initial': lambda x: x,
    'per_key_options': lambda x: x,
    'return_exceptions': validate_bool
}


def _get_valid_global_multi_opts(
    temp_options,  # type: Dict[str, Any]
    valid_opt_keys  # type: List[str]
):
    final_opts = {}
    if not temp_options:
        return final_opts

    for opt_key, opt_value in temp_options.items():
        if opt_key not in valid_opt_keys:
            continue
        transform = VALID_MULTI_OPTS.get(opt_key, None)
        if transform:
            final_opts[opt_key] = transform(opt_value)

    return final_opts


def _get_per_key_opts(
    per_key_opts,  # type: Dict[str, Any]
    opt_type,  # type: OptionsBase
    valid_opt_keys  # type: List[str]
) -> Dict[str, Any]:
    final_key_opts = {}
    for key, opts in per_key_opts.items():
        if not isinstance(opts, (opt_type, dict)):
            raise InvalidArgumentException(message=f'Expected options to be of type Union[{opt_type.__name__}, dict]')
        key_opts = {}
        for opt_key, opt_value in opts.items():
            if opt_key not in valid_opt_keys:
                continue
            transform = VALID_MULTI_OPTS.get(opt_key, None)
            if transform:
                key_opts[opt_key] = transform(opt_value)

        final_key_opts[key] = key_opts

    return final_key_opts


def get_valid_multi_args(
    opt_type,  # type: OptionsBase
    arg_vars,  # type: Optional[Dict[str,Any]]
    *options  # type: OptionsBase
) -> Dict[str, Any]:

    temp_options = _get_temp_opts(arg_vars, *options)
    valid_opt_keys = opt_type.get_valid_keys()

    final_opts = _get_valid_global_multi_opts(temp_options, valid_opt_keys)

    if not temp_options:
        return final_opts

    per_key_opts = temp_options.pop('per_key_options', None)
    if not per_key_opts:
        return final_opts

    final_key_opts = _get_per_key_opts(per_key_opts, opt_type, valid_opt_keys)

    final_opts['per_key_options'] = final_key_opts
    return final_opts


"""

Couchbase Python SDK Options related Enumerations

"""


class LockMode(IntEnum):
    WAIT = 0
    EXC = 1
    NONE = 2


class TLSVerifyMode(Enum):
    NONE = 'none'
    PEER = 'peer'
    NO_VERIFY = 'no_verify'

    @classmethod
    def from_str(cls, value  # type: str
                 ) -> str:
        if isinstance(value, str):
            if value == cls.NONE.value:
                return cls.NONE
            elif value == cls.PEER.value:
                return cls.PEER
            elif value == cls.NO_VERIFY.value:
                return cls.NONE

        raise InvalidArgumentException(message=(f"{value} is not a valid TLSVerifyMode option. "
                                                "Excepted str representation of type TLSVerifyMode."))

    @classmethod
    def to_str(cls, value  # type: Union[TLSVerifyMode, str]
               ) -> str:
        if isinstance(value, TLSVerifyMode):
            if value == cls.NO_VERIFY:
                return cls.NONE.value
            return value.value
        if isinstance(value, str):
            if value == cls.NONE.value:
                return cls.NONE.value
            elif value == cls.PEER.value:
                return cls.PEER.value
            elif value == cls.NO_VERIFY.value:
                return cls.NONE.value

        raise InvalidArgumentException(message=(f"{value} is not a valid TLSVerifyMode option. "
                                                "Excepted TLS verify mode to be either of type "
                                                "TLSVerifyMode or str representation "
                                                "of TLSVerifyMode."))


class IpProtocol(Enum):
    Any = 'any'
    ForceIPv4 = 'force_ipv4'
    ForceIPv6 = 'force_ipv6'

    @classmethod
    def from_str(cls, value  # type: str
                 ) -> str:
        if isinstance(value, str):
            if value == cls.Any.value:
                return cls.Any
            elif value == cls.ForceIPv4.value:
                return cls.ForceIPv4
            elif value == cls.ForceIPv6.value:
                return cls.ForceIPv6

        raise InvalidArgumentException(message=(f"{value} is not a valid IpProtocol option. "
                                                "Excepted str representation of type IpProtocol."))

    @classmethod
    def to_str(cls, value  # type: Union[IpProtocol, str]
               ) -> str:
        if isinstance(value, IpProtocol):
            return value.value
        if isinstance(value, str):
            if value == cls.Any.value:
                return cls.Any.value
            elif value == cls.ForceIPv4.value:
                return cls.ForceIPv4.value
            elif value == cls.ForceIPv6.value:
                return cls.ForceIPv6.value

        raise InvalidArgumentException(message=(f"{value} is not a valid IpProtocol option. "
                                                "Excepted IP Protocol mode to be either of type "
                                                "IpProtocol or str representation "
                                                "of IpProtocol."))


class Compression(Enum):
    """
    Can be one of:
        NONE:
            The client will not compress or decompress the data.
        IN:
            The data coming back from the server will be decompressed, if it was compressed.
        OUT:
            The data coming into server will be compressed.
        INOUT:
            The data will be compressed on way in, decompressed on way out of server.
        FORCE:
            By default the library will send a HELLO command to the server to determine whether compression
            is supported or not.  Because commands may be
            pipelined prior to the scheduing of the HELLO command it is possible that the first few commands
            may not be compressed when schedule due to the library not yet having negotiated settings with the
            server. Setting this flag will force the client to assume that all servers support compression
            despite a HELLO not having been intially negotiated.
    """

    @classmethod
    def from_int(cls, val):
        if val == 0:
            return cls.NONE
        elif val == 1:
            return cls.IN
        elif val == 2:
            return cls.OUT
        elif val == 3:
            return cls.INOUT
        elif val == 7:
            # note that the lcb flag is a 4, but when you set "force" in the connection
            # string, it sets it as INOUT|FORCE.
            return cls.FORCE
        else:
            raise InvalidArgumentException(
                "cannot convert {} to a Compression".format(val)
            )

    NONE = "off"
    IN = "inflate_only"
    OUT = "deflate_only"
    INOUT = "on"
    FORCE = "force"


class KnownConfigProfiles(Enum):
    """
    **VOLATILE** This API is subject to change at any time.

    Represents the name of a specific configuration profile that is associated with predetermined cluster options.

    """
    WanDevelopment = 'wan_development'

    @classmethod
    def from_str(cls, value  # type: str
                 ) -> str:
        if isinstance(value, str):
            if value == cls.WanDevelopment.value:
                return cls.WanDevelopment

        raise InvalidArgumentException(message=(f"{value} is not a valid KnownConfigProfiles option. "
                                                "Excepted str representation of type KnownConfigProfiles."))

    @classmethod
    def to_str(cls, value  # type: Union[KnownConfigProfiles, str]
               ) -> str:
        if isinstance(value, KnownConfigProfiles):
            return value.value

        # just retun the str to allow for future customer config profiles
        if isinstance(value, str):
            return value

        raise InvalidArgumentException(message=(f"{value} is not a valid KnownConfigProfiles option. "
                                                "Excepted config profile to be either of type "
                                                "KnownConfigProfiles or str representation "
                                                "of KnownConfigProfiles."))


"""

Couchbase Python SDK Cluster related Options

"""


class ClusterTimeoutOptionsBase(dict):

    _VALID_OPTS = {
        "bootstrap_timeout": {"bootstrap_timeout": timedelta_as_microseconds},
        "resolve_timeout": {"resolve_timeout": timedelta_as_microseconds},
        "connect_timeout": {"connect_timeout": timedelta_as_microseconds},
        "kv_timeout": {"key_value_timeout": timedelta_as_microseconds},
        "kv_durable_timeout": {"key_value_durable_timeout": timedelta_as_microseconds},
        "views_timeout": {"view_timeout": timedelta_as_microseconds},
        "query_timeout": {"query_timeout": timedelta_as_microseconds},
        "analytics_timeout": {"analytics_timeout": timedelta_as_microseconds},
        "search_timeout": {"search_timeout": timedelta_as_microseconds},
        "management_timeout": {"management_timeout": timedelta_as_microseconds},
        "dns_srv_timeout": {"dns_srv_timeout": timedelta_as_microseconds},
        "idle_http_connection_timeout": {"idle_http_connection_timeout": timedelta_as_microseconds},
        "config_idle_redial_timeout": {"config_idle_redial_timeout": timedelta_as_microseconds}
    }

    @overload
    def __init__(
        self,
        bootstrap_timeout=None,  # type: Optional[timedelta]
        resolve_timeout=None,  # type: Optional[timedelta]
        connect_timeout=None,  # type: Optional[timedelta]
        kv_timeout=None,  # type: Optional[timedelta]
        kv_durable_timeout=None,  # type: Optional[timedelta]
        views_timeout=None,  # type: Optional[timedelta]
        query_timeout=None,  # type: Optional[timedelta]
        analytics_timeout=None,  # type: Optional[timedelta]
        search_timeout=None,  # type: Optional[timedelta]
        management_timeout=None,  # type: Optional[timedelta]
        dns_srv_timeout=None,  # type: Optional[timedelta]
        idle_http_connection_timeout=None,  # type: Optional[timedelta]
        config_idle_redial_timeout=None,  # type: Optional[timedelta]
        config_total_timeout=None  # type: Optional[timedelta]
    ):
        """ClusterTimeoutOptions instance."""

    def __init__(self, **kwargs):
        # kv_timeout = kwargs.pop('kv_timeout', None)
        # if kv_timeout:
        #     kwargs["key_value_timeout"] = kv_timeout
        # kv_durable_timeout = kwargs.pop('kv_durable_timeout', None)
        # if kv_durable_timeout:
        #     kwargs["key_value_durable_timeout"] = kv_durable_timeout

        # legacy...
        kwargs.pop('config_total_timeout', None)

        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        super().__init__(**kwargs)

    def as_dict(self):
        opts = {}
        allowed_opts = ClusterTimeoutOptionsBase.get_allowed_option_keys()
        for k, v in self.items():
            if k not in allowed_opts:
                continue
            if v is None:
                continue
            if isinstance(v, timedelta):
                opts[k] = v.total_seconds()
            elif isinstance(v, (int, float)):
                opts[k] = v
        return opts

    @staticmethod
    def get_allowed_option_keys(use_transform_keys=False  # type: Optional[bool]
                                ) -> List[str]:
        if use_transform_keys is True:
            keys = []
            for val in ClusterTimeoutOptionsBase._VALID_OPTS.values():
                keys.append(list(val.keys())[0])
            return keys

        return list(ClusterTimeoutOptionsBase._VALID_OPTS.keys())


class ClusterTracingOptionsBase(dict):

    _VALID_OPTS = {
        "tracing_threshold_kv": {"key_value_threshold": timedelta_as_microseconds},
        "tracing_threshold_view": {"view_threshold": timedelta_as_microseconds},
        "tracing_threshold_query": {"query_threshold": timedelta_as_microseconds},
        "tracing_threshold_search": {"search_threshold": timedelta_as_microseconds},
        "tracing_threshold_analytics": {"analytics_threshold": timedelta_as_microseconds},
        "tracing_threshold_eventing": {"eventing_threshold": timedelta_as_microseconds},
        "tracing_threshold_management": {"management_threshold": timedelta_as_microseconds},
        "tracing_threshold_queue_size": {"threshold_sample_size": validate_int},
        "tracing_threshold_queue_flush_interval": {"threshold_emit_interval": timedelta_as_microseconds},
        "tracing_orphaned_queue_size": {"orphaned_sample_size": validate_int},
        "tracing_orphaned_queue_flush_interval": {"orphaned_emit_interval": timedelta_as_microseconds}
    }

    @overload
    def __init__(
        self,
        tracing_threshold_kv=None,  # type: Optional[timedelta]
        tracing_threshold_view=None,  # type: Optional[timedelta]
        tracing_threshold_query=None,  # type: Optional[timedelta]
        tracing_threshold_search=None,  # type: Optional[timedelta]
        tracing_threshold_analytics=None,  # type: Optional[timedelta]
        tracing_threshold_eventing=None,  # type: Optional[timedelta]
        tracing_threshold_management=None,  # type: Optional[timedelta]
        tracing_threshold_queue_size=None,  # type: Optional[int]
        tracing_threshold_queue_flush_interval=None,  # type: Optional[timedelta]
        tracing_orphaned_queue_size=None,  # type: Optional[int]
        tracing_orphaned_queue_flush_interval=None,  # type: Optional[timedelta]
    ):
        """ClusterTracingOptions instance."""

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    def as_dict(self):
        opts = {}
        allowed_opts = ClusterTracingOptionsBase.get_allowed_option_keys()
        for k, v in self.items():
            if k not in allowed_opts:
                continue
            if v is None:
                continue
            if isinstance(v, timedelta):
                opts[k] = v.total_seconds()
            elif isinstance(v, (int, float)):
                opts[k] = v
        return opts

    @staticmethod
    def get_allowed_option_keys(use_transform_keys=False  # type: Optional[bool]
                                ) -> List[str]:
        if use_transform_keys is True:
            keys = []
            for val in ClusterTracingOptionsBase._VALID_OPTS.values():
                keys.append(list(val.keys())[0])
            return keys

        return list(ClusterTracingOptionsBase._VALID_OPTS.keys())


# @TODO: remove this when we remove all the deprecated 3.x imports
TransactionConfig = Union[Dict[str, Any], Any]


class ClusterOptionsBase(dict):

    _VALID_OPTS = {
        'allowed_sasl_mechanisms': {'allowed_sasl_mechanisms': lambda x: x.split(',') if isinstance(x, str) else x},
        "authenticator": {"authenticator": lambda x: x},
        "enable_tls": {"enable_tls": validate_bool},
        "enable_mutation_tokens": {"enable_mutation_tokens": validate_bool},
        "enable_tcp_keep_alive": {"enable_tcp_keep_alive": validate_bool},
        "ip_protocol": {"use_ip_protocol": IpProtocol.to_str},
        "enable_dns_srv": {"enable_dns_srv": validate_bool},
        "show_queries": {"show_queries": validate_bool},
        "enable_unordered_execution": {"enable_unordered_execution": validate_bool},
        "enable_clustermap_notification": {"enable_clustermap_notification": validate_bool},
        "enable_compression": {"enable_compression": validate_bool},
        "enable_tracing": {"enable_tracing": validate_bool},
        "enable_metrics": {"enable_metrics": validate_bool},
        "network": {"network": validate_str},
        "tls_verify": {"tls_verify": TLSVerifyMode.to_str},
        "serializer": {"serializer": lambda x: x},
        "transcoder": {"transcoder": lambda x: x},
        "span": {"span": lambda x: x},
        "tcp_keep_alive_interval": {"tcp_keep_alive_interval": timedelta_as_microseconds},
        "config_poll_interval": {"config_poll_interval": timedelta_as_microseconds},
        "config_poll_floor": {"config_poll_floor": timedelta_as_microseconds},
        "max_http_connections": {"max_http_connections": validate_int},
        "user_agent_extra": {"user_agent_extra": validate_str},
        "trust_store_path": {"trust_store_path": validate_str},
        "cert_path": {"cert_path": validate_str},
        "disable_mozilla_ca_certificates": {"disable_mozilla_ca_certificates": validate_bool},
        "logging_meter_emit_interval": {"emit_interval": timedelta_as_microseconds},
        "num_io_threads": {"num_io_threads": validate_int},
        "transaction_config": {"transaction_config": lambda x: x},
        "tracer": {"tracer": lambda x: x},
        "meter": {"meter": lambda x: x},
        "dns_nameserver": {"dns_nameserver": validate_str},
        "dns_port": {"dns_port": validate_int},
        "dump_configuration": {"dump_configuration": validate_bool},
    }

    @overload
    def __init__(
        self,
        authenticator,  # type: Authenticator
        timeout_options=None,  # type: Optional[ClusterTimeoutOptionsBase]
        tracing_options=None,  # type: Optional[ClusterTracingOptionsBase]
        enable_tls=None,    # type: Optional[bool]
        enable_mutation_tokens=None,    # type: Optional[bool]
        enable_tcp_keep_alive=None,    # type: Optional[bool]
        ip_protocol=None,    # type: Optional[Union[IpProtocol, str]]
        enable_dns_srv=None,    # type: Optional[bool]
        show_queries=None,    # type: Optional[bool]
        enable_unordered_execution=None,    # type: Optional[bool]
        enable_clustermap_notification=None,    # type: Optional[bool]
        enable_compression=None,    # type: Optional[bool]
        enable_tracing=None,    # type: Optional[bool]
        enable_metrics=None,    # type: Optional[bool]
        network=None,    # type: Optional[str]
        tls_verify=None,    # type: Optional[Union[TLSVerifyMode, str]]
        serializer=None,  # type: Optional[Serializer]
        transcoder=None,  # type: Optional[Transcoder]
        tcp_keep_alive_interval=None,  # type: Optional[timedelta]
        config_poll_interval=None,  # type: Optional[timedelta]
        config_poll_floor=None,  # type: Optional[timedelta]
        max_http_connections=None,  # type: Optional[int]
        user_agent_extra=None,  # type: Optional[str]
        logging_meter_emit_interval=None,  # type: Optional[timedelta]
        transaction_config=None,  # type: Optional[TransactionConfig]
        log_redaction=None,  # type: Optional[bool]
        compression=None,  # type: Optional[Compression]
        compression_min_size=None,  # type: Optional[int]
        compression_min_ratio=None,  # type: Optional[float]
        lockmode=None,  # type: Optional[LockMode]
        tracer=None,  # type: Optional[CouchbaseTracer]
        meter=None,  # type: Optional[CouchbaseMeter]
        dns_nameserver=None,  # type: Optional[str]
        dns_port=None,  # type: Optional[int]
        disable_mozilla_ca_certificates=None,  # type: Optional[bool]
        dump_configuration=None,  # type: Optional[bool]
    ):
        """ClusterOptions instance."""

    def __init__(self,
                 authenticator,  # type: Authenticator
                 **kwargs
                 ):

        if authenticator:
            kwargs["authenticator"] = authenticator

        # flatten tracing and timeout options
        tracing_opts = kwargs.pop('tracing_options', {})
        if tracing_opts:
            for k, v in tracing_opts.items():
                if k not in kwargs:
                    kwargs[k] = v

        timeout_opts = kwargs.pop('timeout_options', {})
        if timeout_opts:
            for k, v in timeout_opts.items():
                if k not in kwargs:
                    kwargs[k] = v

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @staticmethod
    def get_allowed_option_keys(cluster_opts_only=False,  # type: Optional[bool]
                                use_transform_keys=False  # type: Optional[bool]
                                ) -> List[str]:
        if use_transform_keys is True:
            keys = []
            for val in ClusterOptionsBase._VALID_OPTS.values():
                keys.append(list(val.keys())[0])

            if cluster_opts_only is True:
                return keys

            keys.extend(ClusterTimeoutOptionsBase.get_allowed_option_keys(use_transform_keys=True))
            keys.extend(ClusterTracingOptionsBase.get_allowed_option_keys(use_transform_keys=True))

            return keys

        if cluster_opts_only is True:
            return list(ClusterOptionsBase._VALID_OPTS.keys())

        valid_keys = ClusterTimeoutOptionsBase.get_allowed_option_keys()
        valid_keys.extend(ClusterTracingOptionsBase.get_allowed_option_keys())
        valid_keys.extend(list(ClusterOptionsBase._VALID_OPTS.keys()))

        return valid_keys

    @staticmethod
    def get_valid_options() -> Dict[str, Any]:
        valid_opts = copy.copy(ClusterTimeoutOptionsBase._VALID_OPTS)
        valid_opts.update(copy.copy(ClusterTracingOptionsBase._VALID_OPTS))
        valid_opts.update(copy.copy(ClusterOptionsBase._VALID_OPTS))
        return valid_opts


"""

Couchbase Python SDK Key-Value related Options

"""


class OptionsTimeoutBase(OptionsBase):
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 span=None,  # type: Optional[Any]
                 **kwargs  # type: Dict[str, Any]
                 ) -> None:
        """
        Base options with timeout and span options
        :param timeout: Timeout for this operation
        :param span: Parent tracing span to use for this operation
        """
        if timeout:
            kwargs["timeout"] = timeout

        if span:
            kwargs["span"] = span

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    def timeout(self,
                timeout,  # type: timedelta
                ) -> OptionsTimeoutBase:
        self["timeout"] = timeout
        return self

    def span(self,
             span,  # type: Any
             ) -> OptionsTimeoutBase:
        self["span"] = span
        return self


# Diagnostic Operations

class PingOptionsBase(OptionsTimeoutBase):
    @overload
    def __init__(self,
                 timeout=None,       # type: timedelta
                 report_id=None,     # type: str
                 service_types=None  # type: Iterable[ServiceType]
                 ):
        pass

    def __init__(self,
                 **kwargs
                 ):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class DiagnosticsOptionsBase(OptionsTimeoutBase):
    @overload
    def __init__(self,
                 report_id=None     # type: str
                 ):
        pass

    def __init__(self,
                 **kwargs
                 ):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class WaitUntilReadyOptionsBase(OptionsTimeoutBase):
    @overload
    def __init__(self,
                 desired_state=None,     # type: ClusterState
                 service_types=None  # type: Iterable[ServiceType]
                 ):
        pass

    def __init__(self,
                 **kwargs
                 ):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

# Key-Value Operations


class DurabilityOptionBlockBase(OptionsTimeoutBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 durability=None,  # type: Optional[DurabilityType]
                 expiry=None,  # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @property
    def expiry(self):
        return self.get("expiry", None)


class InsertOptionsBase(DurabilityOptionBlockBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 expiry=None,  # type: Optional[timedelta]
                 durability=None,  # type: Optional[DurabilityType]
                 transcoder=None  # type: Optional[Transcoder]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class UpsertOptionsBase(DurabilityOptionBlockBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 expiry=None,  # type: Optional[timedelta]
                 preserve_expiry=False,  # type: Optional[bool]
                 durability=None,  # type: Optional[DurabilityType]
                 transcoder=None  # type: Optional[Transcoder]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class ReplaceOptionsBase(DurabilityOptionBlockBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 expiry=None,  # type: Optional[timedelta]
                 cas=None,  # type: Optional[int]
                 preserve_expiry=False,  # type: Optional[bool]
                 durability=None,  # type: Optional[DurabilityType]
                 transcoder=None  # type: Optional[Transcoder]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class RemoveOptionsBase(DurabilityOptionBlockBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 cas=None,  # type: Optional[int]
                 durability=None  # type: Optional[DurabilityType]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetOptionsBase(OptionsTimeoutBase):
    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
        with_expiry=None,  # type: Optional[bool]
        project=None,  # type: Optional[Iterable[str]]
        transcoder=None  # type: Optional[Transcoder]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @property
    def with_expiry(self) -> bool:
        return self.get("with_expiry", False)

    @property
    def project(self) -> Iterable[str]:
        return self.get("project", [])


class ExistsOptionsBase(OptionsTimeoutBase):
    @overload
    def __init__(self,
                 timeout=None  # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class TouchOptionsBase(OptionsTimeoutBase):
    @overload
    def __init__(self,
                 timeout=None  # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetAllReplicasOptionsBase(OptionsTimeoutBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 transcoder=None  # type: Optional[Transcoder]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetAndTouchOptionsBase(OptionsTimeoutBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 transcoder=None  # type: Optional[Transcoder]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetAndLockOptionsBase(OptionsTimeoutBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 transcoder=None  # type: Optional[Transcoder]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetAnyReplicaOptionsBase(OptionsTimeoutBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 transcoder=None  # type: Optional[Transcoder]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class UnlockOptionsBase(OptionsTimeoutBase):
    @overload
    def __init__(self,
                 timeout=None  # type: Optional[timedelta]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


# Sub-document Operations


class LookupInOptionsBase(DurabilityOptionBlockBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 access_deleted=None  # type: Optional[bool]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class MutateInOptionsBase(DurabilityOptionBlockBase):
    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 cas=0,          # type: Optional[int]
                 durability=None,  # type: Optional[DurabilityType]
                 store_semantics=None,  # type: Optional[StoreSemantics]
                 access_deleted=None,  # type: Optional[bool]
                 preserve_expiry=None  # type: Optional[bool]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


# Binary Operations

class IncrementOptionsBase(DurabilityOptionBlockBase):
    @overload
    def __init__(self,
                 timeout=None,      # type: Optional[timedelta]
                 expiry=None,       # type: Optional[timedelta]
                 durability=None,   # type: Optional[DurabilityType]
                 delta=None,         # type: Optional[DeltaValueBase]
                 initial=None,      # type: Optional[SignedInt64Base]
                 span=None         # type: Optional[Any]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class DecrementOptionsBase(DurabilityOptionBlockBase):
    @overload
    def __init__(self,
                 timeout=None,      # type: Optional[timedelta]
                 expiry=None,       # type: Optional[timedelta]
                 durability=None,   # type: Optional[DurabilityType]
                 delta=None,         # type: Optional[DeltaValueBase]
                 initial=None,      # type: Optional[SignedInt64Base]
                 span=None         # type: Optional[Any]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class AppendOptionsBase(DurabilityOptionBlockBase):
    @overload
    def __init__(self,
                 timeout=None,      # type: Optional[timedelta]
                 durability=None,   # type: Optional[DurabilityType]
                 cas=None,          # type: Optional[int]
                 span=None         # type: Optional[Any]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class PrependOptionsBase(DurabilityOptionBlockBase):
    @overload
    def __init__(self,
                 timeout=None,      # type: Optional[timedelta]
                 durability=None,   # type: Optional[DurabilityType]
                 cas=None,          # type: Optional[int]
                 span=None         # type: Optional[Any]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


"""

Couchbase Python SDK N1QL related Options

"""


class QueryOptionsBase(dict):

    # @TODO: span
    @overload
    def __init__(
        self,
        timeout=None,  # type: Optional[timedelta]
        read_only=None,  # type: Optional[bool]
        scan_consistency=None,  # type: Optional[QueryScanConsistency]
        adhoc=None,  # type: Optional[bool]
        client_context_id=None,  # type: Optional[str]
        max_parallelism=None,  # type: Optional[int]
        positional_parameters=None,  # type: Optional[Iterable[JSONType]]
        named_parameters=None,  # type: Optional[Dict[str, JSONType]]
        pipeline_batch=None,  # type: Optional[int]
        pipeline_cap=None,  # type: Optional[int]
        profile=None,  # type: Optional[QueryProfile]
        query_context=None,  # type: Optional[str]
        scan_cap=None,  # type: Optional[int]
        scan_wait=None,  # type: Optional[timedelta]
        metrics=None,  # type: Optional[bool]
        flex_index=None,  # type: Optional[bool]
        preserve_expiry=None,  # type: Optional[bool]
        consistent_with=None,  # type: Optional[MutationState]
        send_to_node=None,  # type: Optional[str]
        raw=None,  # type: Optional[Dict[str,Any]]
        span=None,  # type: Optional[Any]
        serializer=None  # type: Optional[Serializer]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


"""

Couchbase Python SDK Analytics related Options

"""


class AnalyticsOptionsBase(OptionsTimeoutBase):

    @overload
    def __init__(self,
                 timeout=None,  # type: Optional[timedelta]
                 read_only=None,  # type: Optional[bool]
                 scan_consistency=None,  # type: Optional[AnalyticsScanConsistency]
                 positional_parameters=None,  # type: Optional[Iterable[JSONType]]
                 named_parameters=None,  # type: Optional[Dict[str, JSONType]]
                 client_context_id=None,  # type: Optional[str]
                 priority=None,  # type: Optional[bool]
                 metrics=None,  # type: Optional[bool]
                 query_context=None,  # type: Optional[str]
                 raw=None,              # type: Optional[Dict[str, Any]]
                 serializer=None  # type: Optional[Serializer]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


"""

Couchbase Python SDK Full Text Search (FTS) related Options

"""


class SearchOptionsBase(OptionsTimeoutBase):
    @overload
    def __init__(self,
                 timeout=None,           # type: Optional[timedelta]
                 limit=None,             # type: Optional[int]
                 skip=None,              # type: Optional[int]
                 explain=None,           # type: Optional[bool]
                 fields=None,            # type: Optional[List[str]]
                 highlight_style=None,   # type: Optional[HighlightStyle]
                 highlight_fields=None,  # type: Optional[List[str]]
                 scan_consistency=None,  # type: Optional[SearchScanConsistency]
                 consistent_with=None,   # type: Optional[MutationState]
                 facets=None,            # type: Optional[Dict[str, Facet]]
                 raw=None,               # type: Optional[Dict[str, Any]]
                 sort=None,              # type: Optional[Union[List[str],List[Sort]]]
                 disable_scoring=None,   # type: Optional[bool]
                 scope_name=None,  # type: Optional[str]
                 collections=None,       # type: Optional[List[str]]
                 include_locations=None,  # type: Optional[bool]
                 client_context_id=None,  # type: Optional[str]
                 serializer=None  # type: Optional[Serializer]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


"""

Couchbase Python SDK View related Options

"""


class ViewOptionsBase(OptionsTimeoutBase):
    @overload
    def __init__(self,
                 timeout=None,               # type: Optional[timedelta]
                 scan_consistency=None,      # type: Optional[ViewScanConsistency]
                 skip=None,                  # type: Optional[int]
                 limit=None,                 # type: Optional[int]
                 startkey=None,              # type: Optional[JSONType]
                 endkey=None,                # type: Optional[JSONType]
                 startkey_docid=None,        # type: Optional[str]
                 endkey_docid=None,          # type: Optional[str]
                 inclusive_end=None,         # type: Optional[bool]
                 group=None,                 # type: Optional[bool]
                 group_level=None,           # type: Optional[int]
                 key=None,                   # type: Optional[JSONType]
                 keys=None,                  # type: Optional[List[JSONType]]
                 order=None,                 # type: Optional[ViewOrdering]
                 reduce=None,                # type: Optional[bool]
                 on_error=None,              # type: Optional[ViewErrorMode]
                 debug=None,                 # type: Optional[bool]
                 raw=None,                   # type: Optional[Tuple(str,Any)]
                 namespace=None,             # type: Optional[DesignDocumentNamespace]
                 query_string=None,          # type: Optional[List[str]]
                 client_context_id=None      # type: Optional[str]
                 ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        # @TODO:  do we need this??  Don't think so...
        # val = kwargs.pop('scan_consistency', None)
        # if val:
        #     kwargs['stale'] = val.value
        # val = kwargs.pop('order', None)
        # if val:
        #     kwargs['descending'] = val.value
        # val = kwargs.pop('on_error', None)
        # if val:
        #     kwargs['on_error'] = val.value
        # val = kwargs.pop('raw', None)
        # if val:
        #     kwargs[val[0]] = val[1]
        # val = kwargs.pop('namespace', None)
        # if val:
        #     kwargs['use_devmode'] = (
        #         val == DesignDocumentNamespace.DEVELOPMENT)

        super().__init__(**kwargs)


"""

Couchbase Python SDK constrained integer classes

"""


AcceptableInts = Union[ctypes.c_int64, ctypes.c_uint64, int]


class ConstrainedIntBase():
    def __init__(self, value):
        """
        A signed integer between cls.min() and cls.max() inclusive

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.InvalidArgumentException` if not in range
        """
        self.value = type(self).verify_value(value)

    @classmethod
    def verify_value(cls, item  # type: AcceptableInts
                     ):
        # type: (...) -> int
        value = getattr(item, 'value', item)
        if not isinstance(value, int) or not (cls.min() <= value <= cls.max()):
            raise InvalidArgumentException(
                "Integer in range {} and {} inclusiverequired".format(cls.min(), cls.max()))
        return value

    @classmethod
    def is_valid(cls,
                 item  # type: AcceptableInts
                 ):
        return isinstance(item, cls)

    def __neg__(self):
        return -self.value

    # Python 3.8 deprecated the implicit conversion to integers using __int__
    # use __index__ instead
    # still needed for Python 3.7
    def __int__(self):
        return self.value

    # __int__ falls back to __index__
    def __index__(self):
        return self.value

    def __add__(self, other):
        if not (self.min() <= (self.value + int(other)) <= self.max()):
            raise InvalidArgumentException(
                "{} + {} would be out of range {}-{}".format(self.value, other, self.min(), self.min()))

    @classmethod
    def max(cls):
        raise NotImplementedError()

    @classmethod
    def min(cls):
        raise NotImplementedError()

    def __str__(self):
        return "{cls_name} with value {value}".format(
            cls_name=type(self), value=self.value)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return isinstance(self, type(other)) and self.value == other.value

    def __gt__(self, other):
        return self.value > other.value

    def __lt__(self, other):
        return self.value < other.value


class SignedInt64Base(ConstrainedIntBase):
    def __init__(self, value):
        """
        A signed integer between -0x8000000000000000 and +0x7FFFFFFFFFFFFFFF inclusive.

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.InvalidArgumentException` if not in range
        """
        super().__init__(value)

    @classmethod
    def max(cls):
        return 0x7FFFFFFFFFFFFFFF

    @classmethod
    def min(cls):
        return -0x8000000000000000


class UnsignedInt32Base(ConstrainedIntBase):
    def __init__(self, value):
        """
        An unsigned integer between 0x00000000 and +0x80000000 inclusive.

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.ArgumentError` if not in range
        """
        super().__init__(value)

    @classmethod
    def max(cls):
        return 0x00000000

    @classmethod
    def min(cls):
        return 0x80000000


class UnsignedInt64Base(ConstrainedIntBase):
    def __init__(self, value):
        """
        An unsigned integer between 0x0000000000000000 and +0x8000000000000000 inclusive.

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.ArgumentError` if not in range
        """
        super().__init__(value)

    @classmethod
    def min(cls):
        return 0x0000000000000000

    @classmethod
    def max(cls):
        return 0x8000000000000000


class DeltaValueBase(ConstrainedIntBase):
    def __init__(self,
                 value  # type: AcceptableInts
                 ):
        """
        A non-negative integer between 0 and +0x7FFFFFFFFFFFFFFF inclusive.
        Used as an argument for :meth:`Collection.increment` and :meth:`Collection.decrement`

        :param value: the value to initialise this with.

        :raise: :exc:`~couchbase.exceptions.InvalidArgumentException` if not in range
        """
        super().__init__(value)

    @ classmethod
    def max(cls):
        return 0x7FFFFFFFFFFFFFFF

    @ classmethod
    def min(cls):
        return 0
