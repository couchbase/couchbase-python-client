# used to allow for unquoted (i.e. forward reference, Python >= 3.7, PEP563)
from __future__ import annotations

import copy
import ctypes
import time
from abc import ABCMeta, abstractmethod
from datetime import timedelta
from enum import Enum, IntEnum
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    List,
                    Optional,
                    Tuple,
                    TypeVar,
                    Union,
                    overload)

from couchbase._utils import timedelta_as_microseconds
from couchbase.durability import DurabilityParser
from couchbase.exceptions import InvalidArgumentException
from couchbase.pycbc_core import transaction_config

# allows for imports only during type checking and not during runtime -- :)
if TYPE_CHECKING:
    from couchbase._utils import JSONType
    from couchbase.analytics import AnalyticsScanConsistency
    from couchbase.auth import Authenticator
    from couchbase.diagnostics import ClusterState, ServiceType
    from couchbase.durability import DurabilityType
    from couchbase.management.views import DesignDocumentNamespace
    from couchbase.mutation_state import MutationState
    from couchbase.n1ql import QueryProfile, QueryScanConsistency
    from couchbase.search import (Facet,
                                  HighlightStyle,
                                  SearchScanConsistency,
                                  Sort)
    from couchbase.serializer import Serializer
    from couchbase.subdocument import StoreSemantics
    from couchbase.transcoder import Transcoder
    from couchbase.views import (ViewErrorMode,
                                 ViewOrdering,
                                 ViewScanConsistency)
    from couchbase.collection import Collection
    from couchbase.durability import ServerDurability
    from couchbase.logic.n1ql import QueryScanConsistency


OptionsBase = dict

T = TypeVar("T", bound=OptionsBase)


"""
@TODO(jc): CouchbaseSpan types
"""


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


class OptionsTimeout(OptionsBase):
    def __init__(
        self,  # type: T
        timeout=None,  # type: timedelta
        span=None,  # type: Any
        **kwargs  # type: Any
    ):
        # type: (...)-> None
        """
        Base options with timeout and span options
        :param timeout: Timeout for this operation
        :param span: Parent tracing span to use for this operation
        """
        if timeout:
            kwargs["timeout"] = timeout

        if span:
            kwargs["span"] = span
        super().__init__(**kwargs)

    def timeout(
        self,  # type: T
        timeout,  # type: timedelta
    ):
        self["timeout"] = timeout
        return self

    def span(
        self,  # type: T
        span,  # type: Any
    ):
        self["span"] = span
        return self


class ClusterTimeoutOptions(dict):

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
        """ClusterTimeoutOptions

        These will be the default timeouts for operations for the entire cluster

        Args:
            bootstrap_timeout (timedelta, optional): bootstrap timeout. Defaults to None.
            resolve_timeout (timedelta, optional): bootstrap timeout. Defaults to None.
            connect_timeout (timedelta, optional): connect timeout. Defaults to None.
            kv_timeout (timedelta, optional): KV operations timeout. Defaults to None.
            kv_durable_timeout (timedelta, optional): KV durability operations timeout. Defaults to None.
            views_timeout (timedelta, optional): views operations timeout. Defaults to None.
            query_timeout (timedelta, optional): query operations timeout. Defaults to None.
            analytics_timeout (timedelta, optional): analytics operations timeout. Defaults to None.
            search_timeout (timedelta, optional): search operations timeout. Defaults to None.
            management_timeout (timedelta, optional): management operations timeout. Defaults to None.
            dns_srv_timeout (timedelta, optional): DNS SRV connection timeout. Defaults to None.
            idle_http_connection_timeout (timedelta, optional): Idle HTTP connection timeout. Defaults to None.
            config_idle_redial_timeout (timedelta, optional): Idle redial timeout. Defaults to None.
            config_total_timeout (timedelta, optional): **DEPRECATED** complete bootstrap timeout. Defaults to None.
        """
        pass

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
        allowed_opts = ClusterTimeoutOptions.get_allowed_option_keys()
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
            for val in ClusterTimeoutOptions._VALID_OPTS.values():
                keys.append(list(val.keys())[0])
            return keys

        return list(ClusterTimeoutOptions._VALID_OPTS.keys())


class ClusterTracingOptions(dict):

    _VALID_OPTS = {
        "tracing_threshold_kv": {"key_value_threshold": timedelta_as_microseconds},
        "tracing_threshold_view": {"view_threshold": timedelta_as_microseconds},
        "tracing_threshold_query": {"query_threshold": timedelta_as_microseconds},
        "tracing_threshold_search": {"search_threshold": timedelta_as_microseconds},
        "tracing_threshold_analytics": {"analytics_threshold": timedelta_as_microseconds},
        "tracing_threshold_eventing": {"eventing_threshold": timedelta_as_microseconds},
        "tracing_threshold_management": {"management_threshold": timedelta_as_microseconds},
        "tracing_threshold_queue_size": {"threshold_sample_size": lambda x: x},
        "tracing_threshold_queue_flush_interval": {"threshold_emit_interval": timedelta_as_microseconds},
        "tracing_orphaned_queue_size": {"orphaned_sample_size": lambda x: x},
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
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    def as_dict(self):
        opts = {}
        allowed_opts = ClusterTracingOptions.get_allowed_option_keys()
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
            for val in ClusterTracingOptions._VALID_OPTS.values():
                keys.append(list(val.keys())[0])
            return keys

        return list(ClusterTracingOptions._VALID_OPTS.keys())


class LockMode(IntEnum):
    WAIT = 0
    EXC = 1
    NONE = 2


class TLSVerifyMode(Enum):
    NONE = 'none'
    PEER = 'peer'
    NO_VERIFY = 'no_verify'

    @classmethod
    def from_str(cls, value):
        if value == cls.NONE.value:
            return cls.NONE
        elif value == cls.PEER.value:
            return cls.PEER
        elif value == cls.NO_VERIFY.value:
            return cls.NONE
        else:
            raise ValueError(f'{value} is not a valid TLSVerifyMode option.  See couchbase.options.TLSVerifyMode.')


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

# @TODO: compression, lockmode, log_redaction


class ClusterOptions(dict):

    _VALID_OPTS = {
        "authenticator": {"authenticator": lambda x: x},
        "enable_tls": {"enable_tls": lambda x: x},
        "enable_mutation_tokens": {"enable_mutation_tokens": lambda x: x},
        "enable_tcp_keep_alive": {"enable_tcp_keep_alive": lambda x: x},
        "force_ipv4": {"force_ipv4": lambda x: x},
        "enable_dns_srv": {"enable_dns_srv": lambda x: x},
        "show_queries": {"show_queries": lambda x: x},
        "enable_unordered_execution": {"enable_unordered_execution": lambda x: x},
        "enable_clustermap_notification": {"enable_clustermap_notification": lambda x: x},
        "enable_compression": {"enable_compression": lambda x: x},
        "enable_tracing": {"enable_tracing": lambda x: x},
        "enable_metrics": {"enable_metrics": lambda x: x},
        "network": {"network": lambda x: x},
        "tls_verify": {"tls_verify": lambda x: x},
        "transcoder": {"transcoder": lambda x: x},
        "span": {"span": lambda x: x},
        "tcp_keep_alive_interval": {"tcp_keep_alive_interval": timedelta_as_microseconds},
        "config_poll_interval": {"config_poll_interval": timedelta_as_microseconds},
        "config_poll_floor": {"config_poll_floor": timedelta_as_microseconds},
        "max_http_connections": {"max_http_connections": lambda x: x},
        "user_agent_extra": {"user_agent_extra": lambda x: x},
        "logging_meter_emit_interval": {"emit_interval": lambda x: x},
        "num_io_threads": {"num_io_threads": lambda x: x},
        "transaction_config": {"transaction_config": lambda x: x},
    }

    @overload
    def __init__(
        self,
        authenticator,  # type: Authenticator
        timeout_options=None,  # type: Optional[ClusterTimeoutOptions]
        tracing_options=None,  # type: Optional[ClusterTracingOptions]
        enable_tls=None,    # type: Optional[bool]
        enable_mutation_tokens=None,    # type: Optional[bool]
        enable_tcp_keep_alive=None,    # type: Optional[bool]
        force_ipv4=None,    # type: Optional[bool]
        enable_dns_srv=None,    # type: Optional[bool]
        show_queries=None,    # type: Optional[bool]
        enable_unordered_execution=None,    # type: Optional[bool]
        enable_clustermap_notification=None,    # type: Optional[bool]
        enable_compression=None,    # type: Optional[bool]
        enable_tracing=None,    # type: Optional[bool]
        enable_metrics=None,    # type: Optional[bool]
        network=None,    # type: Optional[str]
        tls_verify=None,    # type: Optional[bool]
        transcoder=None,  # type: Optional[Transcoder]
        span=None,  # type: Optional[Any]
        tcp_keep_alive_interval=None,  # type: Optional[timedelta]
        config_poll_interval=None,  # type: Optional[timedelta]
        config_poll_floor=None,  # type: Optional[timedelta]
        max_http_connections=None,  # type: Optional[int]
        user_agent_extra=None,  # type: Optional[str]
        logging_meter_emit_interval=None,  # type: Optional[int]
        num_io_threads=None,  # type: Optional[int]
        transaction_config=None  # type: Optional[TransactionConfig]
    ):
        pass

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
            for val in ClusterOptions._VALID_OPTS.values():
                keys.append(list(val.keys())[0])

            if cluster_opts_only is True:
                return keys

            keys.extend(ClusterTimeoutOptions.get_allowed_option_keys(use_transform_keys=True))
            keys.extend(ClusterTracingOptions.get_allowed_option_keys(use_transform_keys=True))

            return keys

        if cluster_opts_only is True:
            return list(ClusterOptions._VALID_OPTS.keys())

        valid_keys = ClusterTimeoutOptions.get_allowed_option_keys()
        valid_keys.extend(ClusterTracingOptions.get_allowed_option_keys())
        valid_keys.extend(list(ClusterOptions._VALID_OPTS.keys()))

    @staticmethod
    def get_valid_options() -> Dict[str, Any]:
        valid_opts = copy.copy(ClusterTimeoutOptions._VALID_OPTS)
        valid_opts.update(copy.copy(ClusterTracingOptions._VALID_OPTS))
        valid_opts.update(copy.copy(ClusterOptions._VALID_OPTS))
        return valid_opts


class PingOptions(OptionsTimeout):
    @overload
    def __init__(self,
                 timeout=None,       # type: timedelta
                 report_id=None,     # type: str
                 service_types=None  # type: Iterable[ServiceType]
                 ):
        """
        Create options used for ping command.

        :param timedelta timeout: Currently not implemented, coming soon.
        :param str report_id: Add an id to the request, which you can track in logging, etc...
        :param Iterable[ServiceType] service_types: Restrict the ping to the services passed in here.
        """
        pass

    def __init__(self,
                 **kwargs
                 ):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class DiagnosticsOptions(OptionsBase):
    @overload
    def __init__(self,
                 report_id=None     # type: str
                 ):
        """
        Create options used for diagnostics command.

        :param str report_id: Add an id to the request, which you can track in logging, etc...
        """
        pass

    def __init__(self,
                 **kwargs
                 ):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class WaitUntilReadyOptions(OptionsBase):
    @overload
    def __init__(self,
                 desired_state=None,     # type: ClusterState
                 num_kv_endpoints=None,  # type: int
                 service_types=None  # type: Iterable[ServiceType]
                 ):
        """
        Create options used for ping command.

        :param ClusterState desired_state: TBD.
        :param int num_kv_endpoints: TBD (testing only??)
        :param Iterable[ServiceType] service_types: Restrict the ping to the services passed in here.
        """
        pass

    def __init__(self,
                 **kwargs
                 ):
        # if 'service_types' in kwargs:
        #     kwargs['service_types'] = list(
        #         map(lambda x: x.value, kwargs['service_types']))

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class DurabilityOptionBlock(OptionsTimeout):
    @overload
    def __init__(
        self,  # type: DurabilityOptionBlock
        timeout=None,  # type: timedelta
        durability=None,  # type: DurabilityType
        expiry=None,  # type: timedelta
    ):
        # type: (...) -> None
        """
        Options for operations with any type of durability

        :param durability: Durability setting
        :param expiry: When any mutation should expire
        :param timeout: Timeout for operation
        """
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @property
    def expiry(self):
        return self.get("expiry", None)


class InsertOptions(DurabilityOptionBlock):
    @overload
    def __init__(
        self,  # type: "InsertOptions"
        timeout=None,  # type: timedelta
        expiry=None,  # type: timedelta
        durability=None,  # type: DurabilityType
        transcoder=None  # type: Transcoder
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class UpsertOptions(DurabilityOptionBlock):
    @overload
    def __init__(
        self,  # type: "UpsertOptions"
        timeout=None,  # type: timedelta
        expiry=None,  # type: timedelta
        preserve_expiry=False,  # type: bool
        durability=None,  # type: DurabilityType
        transcoder=None  # type: Transcoder
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class ReplaceOptions(DurabilityOptionBlock):
    @overload
    def __init__(
        self,  # type: ReplaceOptions
        timeout=None,  # type: timedelta
        expiry=None,  # type: timedelta
        cas=0,  # type: int
        preserve_expiry=False,  # type: bool
        durability=None,  # type: DurabilityType
        transcoder=None  # type: Transcoder
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class RemoveOptions(DurabilityOptionBlock):
    @overload
    def __init__(
        self,  # type: RemoveOptions
        timeout=None,  # type: timedelta
        cas=0,  # type: int
        durability=None,  # type: DurabilityType
        transcoder=None  # type: Transcoder
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetOptions(OptionsTimeout):
    @overload
    def __init__(
        self,  # type: T
        timeout=None,  # type: timedelta
        with_expiry=None,  # type: bool
        project=None,  # type: Iterable[str]
        transcoder=None  # type: Transcoder
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @property
    def with_expiry(self):
        # type: (...) -> bool
        return self.get("with_expiry", False)

    @property
    def project(self):
        # type: (...) -> Iterable[str]
        return self.get("project", [])


class ExistsOptions(OptionsTimeout):
    @overload
    def __init__(
        self,  # type: T
        timeout=None  # type: timedelta
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class TouchOptions(OptionsTimeout):
    @overload
    def __init__(
        self,  # type: T
        timeout=None  # type: timedelta
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetAndTouchOptions(OptionsTimeout):
    @overload
    def __init__(
        self,  # type: T
        timeout=None,  # type: timedelta
        transcoder=None  # type: Transcoder
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class GetAndLockOptions(OptionsTimeout):
    @overload
    def __init__(
        self,  # type: T
        timeout=None,  # type: timedelta
        transcoder=None  # type: Transcoder
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class UnlockOptions(OptionsTimeout):
    @overload
    def __init__(
        self,  # type: T
        timeout=None  # type: timedelta
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class LookupInOptions(DurabilityOptionBlock):
    @overload
    def __init__(
        self,  # type: LookupInOptions
        timeout=None,  # type: timedelta
        access_deleted=None  # type: bool
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class MutateInOptions(DurabilityOptionBlock):
    @overload
    def __init__(
        self,  # type: MutateInOptions
        timeout=None,  # type: timedelta
        expiry=None,  # type: timedelta
        cas=0,          # type: int
        durability=None,  # type: DurabilityType
        store_semantics=None,  # type: StoreSemantics
        access_deleted=None,  # type: bool
        preserve_expiry=None  # type: bool
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class IncrementOptions(DurabilityOptionBlock):
    @overload
    def __init__(
        self,
        timeout=None,      # type: timedelta
        expiry=None,       # type: timedelta
        durability=None,   # type: DurabilityType
        delta=None,         # type: DeltaValue
        initial=None,      # type: SignedInt64
        span=None         # type: Any

    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class DecrementOptions(DurabilityOptionBlock):
    @overload
    def __init__(
        self,
        timeout=None,      # type: timedelta
        expiry=None,       # type: timedelta
        durability=None,   # type: DurabilityType
        delta=None,         # type: DeltaValue
        initial=None,      # type: SignedInt64
        span=None         # type: Any
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class AppendOptions(DurabilityOptionBlock):
    @overload
    def __init__(
        self,
        timeout=None,      # type: timedelta
        durability=None,   # type: DurabilityType
        cas=None,          # type: int
        span=None         # type: Any
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class PrependOptions(DurabilityOptionBlock):
    @overload
    def __init__(
        self,
        timeout=None,      # type: timedelta
        durability=None,   # type: DurabilityType
        cas=None,          # type: int
        span=None         # type: Any
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class QueryOptions(dict):

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
        scap_cap=None,  # type: Optional[int]
        scap_wait=None,  # type: Optional[timedelta]
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


class AnalyticsOptions(OptionsTimeout):

    @overload
    def __init__(
        self,  # type: T
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


class SearchOptions(OptionsTimeout):
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
                 raw=None,               # type: Optional[JSONType]
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


class ViewOptions(OptionsTimeout):
    @overload
    def __init__(self,
                 timeout=None,               # type: Optional[timedelta]
                 scan_consistency=None,      # type: Optional[ViewScanConsistency]
                 skip=None,                  # type: Optional[int]
                 limit=None,                 # type: Optional[int]
                 startkey=None,              # type: Optional[str]
                 endkey=None,                # type: Optional[str]
                 startkey_docid=None,        # type: Optional[str]
                 endkey_docid=None,          # type: Optional[str]
                 inclusive_end=None,         # type: Optional[bool]
                 group=None,                 # type: Optional[bool]
                 group_level=None,           # type: Optional[int]
                 key=None,                   # type: Optional[str]
                 keys=None,                  # type: Optional[List[str]]
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


class Forwarder(metaclass=ABCMeta):
    def forward_args(
        self,
        arg_vars,  # type: Optional[Dict[str,Any]]
        *options  # type: OptionsBase
    ):
        # type: (...) -> OptionsBase[str,Any]
        arg_vars = copy.copy(arg_vars) if arg_vars else {}
        temp_options = (
            copy.copy(
                options[0]) if (
                options and options[0]) else OptionsBase())
        kwargs = arg_vars.pop("kwargs", {})
        temp_options.update(kwargs)
        temp_options.update(arg_vars)

        end_options = {}
        for k, v in temp_options.items():
            map_item = self.arg_mapping().get(k, None)
            if not (map_item is None):
                for out_k, out_f in map_item.items():
                    converted = out_f(v)
                    if converted is not None:
                        end_options[out_k] = converted
            else:
                end_options[k] = v
        return end_options

    @abstractmethod
    def arg_mapping(self):
        pass


THIRTY_DAYS_IN_SECONDS = 30 * 24 * 60 * 60


def timedelta_as_timestamp(
    duration,  # type: timedelta
) -> int:
    if not isinstance(duration, timedelta):
        raise InvalidArgumentException(
            "Expected timedelta instead of {}".format(duration))

    # PYCBC-1177 remove deprecated heuristic from PYCBC-948:
    #   if (duration > 50 years):
    #     log.warn(“suspicious duration; don’t do this”)
    #     return duration in seconds;
    #
    seconds = int(duration.total_seconds())
    if seconds < THIRTY_DAYS_IN_SECONDS:
        return seconds

    return seconds + int(time.time())


class DefaultForwarder(Forwarder):
    def arg_mapping(self):
        return {
            "spec": {"specs": lambda x: x},
            "id": {},
            "timeout": {"timeout": timedelta_as_microseconds},
            "expiry": {"expiry": timedelta_as_timestamp},
            "lock_time": {"lock_time": lambda x: int(x.total_seconds())},
            "self": {},
            "options": {},
            "durability": {
                "durability": DurabilityParser.parse_durability},
            "disable_scoring": {
                "disable_scoring": lambda dis_score: True if dis_score else None
            },
            "preserve_expiry": {"preserve_expiry": lambda x: x},
            "report_id": {"report_id": lambda x: str(x)}
        }


forward_args = DefaultForwarder().forward_args


# TODO:  do these belong somewhere else??

AcceptableInts = Union[ctypes.c_int64, ctypes.c_uint64, int]


class ConstrainedInt():
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


class SignedInt64(ConstrainedInt):
    def __init__(self, value):
        """
        A signed integer between -0x8000000000000000 and +0x7FFFFFFFFFFFFFFF inclusive.

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.InvalidArgumentException` if not in range
        """
        super(SignedInt64, self).__init__(value)

    @classmethod
    def max(cls):
        return 0x7FFFFFFFFFFFFFFF

    @classmethod
    def min(cls):
        return -0x8000000000000000


class UnsignedInt32(ConstrainedInt):
    def __init__(self, value):
        """
        An unsigned integer between 0x00000000 and +0x80000000 inclusive.

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.ArgumentError` if not in range
        """
        super(UnsignedInt32, self).__init__(value)

    @classmethod
    def max(cls):
        return 0x00000000

    @classmethod
    def min(cls):
        return 0x80000000


class UnsignedInt64(ConstrainedInt):
    def __init__(self, value):
        """
        An unsigned integer between 0x0000000000000000 and +0x8000000000000000 inclusive.

        :param couchbase.options.AcceptableInts value: the value to initialise this with.
        :raise: :exc:`~couchbase.exceptions.ArgumentError` if not in range
        """
        super(UnsignedInt64, self).__init__(value)

    @classmethod
    def min(cls):
        return 0x0000000000000000

    @classmethod
    def max(cls):
        return 0x8000000000000000


class DeltaValue(ConstrainedInt):
    def __init__(self,
                 value  # type: AcceptableInts
                 ):
        # type: (...) -> None
        """
        A non-negative integer between 0 and +0x7FFFFFFFFFFFFFFF inclusive.
        Used as an argument for :meth:`Collection.increment` and :meth:`Collection.decrement`

        :param value: the value to initialise this with.

        :raise: :exc:`~couchbase.exceptions.InvalidArgumentException` if not in range
        """
        super(DeltaValue, self).__init__(value)

    @ classmethod
    def max(cls):
        return 0x7FFFFFFFFFFFFFFF

    @ classmethod
    def min(cls):
        return 0


class TransactionConfig:
    _TXN_ALLOWED_KEYS = {"durability_level", "cleanup_window", "kv_timeout",
                         "expiration_time", "cleanup_lost_attempts", "cleanup_client_attempts",
                         "custom_metadata_collection", "scan_consistency"}

    @overload
    def __init__(self,
                 durability=None,   # type: Optional[ServerDurability]
                 cleanup_window=None,  # type: Optional[timedelta]
                 kv_timeout=None,  # type: Optional[timedelta]
                 expiration_time=None,  # type: Optional[timedelta]
                 cleanup_lost_attempts=None,  # type: Optional[bool]
                 cleanup_client_attempts=None,  # type: Optional[bool]
                 custom_metadata_collection=None,  # type: Optional[Collection]
                 scan_consistency=None  # type: Optional[QueryScanConsistency]
                 ):
        pass

    def __init__(self,
                 **kwargs  # type: dict[str, Any]
                 ):
        kwargs = {k: v for k, v in kwargs.items() if k in TransactionConfig._TXN_ALLOWED_KEYS}
        # convert everything here...
        if kwargs.get("durability_level", None):
            kwargs["durability_level"] = kwargs["durability_level"].level.value
        for k in ["cleanup_window", "kv_timeout", "expiration_time"]:
            if kwargs.get(k, None):
                kwargs[k] = int(kwargs[k].total_seconds() * 1000000)
        coll = kwargs.pop("custom_metadata_collection", None)
        if coll:
            kwargs["metadata_bucket"] = coll._scope.bucket.name
            kwargs["metadata_scope"] = coll._scope.name
            kwargs["metadata_colleciton"] = coll.name

        # don't pass None
        for key in [k for k, v in kwargs.items() if v is None]:
            del(kwargs[key])

        # TODO: handle scan consistency
        print(f'creating transaction_config with {kwargs}')
        self._base = transaction_config(**kwargs)

    def __str__(self):
        return f'TransactionConfig{{{self._base}}}'
