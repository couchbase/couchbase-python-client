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

# used to allow for unquoted (i.e. forward reference, Python >= 3.7, PEP563)
from __future__ import annotations

import copy
from abc import (ABC,
                 ABCMeta,
                 abstractmethod)
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    Optional,
                    Tuple,
                    TypeVar,
                    Union,
                    overload)

from couchbase._utils import (timedelta_as_microseconds,
                              timedelta_as_timestamp,
                              validate_int)
from couchbase.durability import DurabilityParser
from couchbase.exceptions import InvalidArgumentException
from couchbase.logic.options import AcceptableInts  # noqa: F401
from couchbase.logic.options import Compression  # noqa: F401
from couchbase.logic.options import IpProtocol  # noqa: F401
from couchbase.logic.options import KnownConfigProfiles  # noqa: F401
from couchbase.logic.options import LockMode  # noqa: F401
from couchbase.logic.options import TLSVerifyMode  # noqa: F401
from couchbase.logic.options import get_valid_args  # noqa: F401
from couchbase.logic.options import get_valid_multi_args  # noqa: F401
from couchbase.logic.options import (AnalyticsOptionsBase,
                                     AppendOptionsBase,
                                     ClusterOptionsBase,
                                     ClusterTimeoutOptionsBase,
                                     ClusterTracingOptionsBase,
                                     ConstrainedIntBase,
                                     DecrementOptionsBase,
                                     DeltaValueBase,
                                     DiagnosticsOptionsBase,
                                     DurabilityOptionBlockBase,
                                     ExistsOptionsBase,
                                     GetAllReplicasOptionsBase,
                                     GetAndLockOptionsBase,
                                     GetAndTouchOptionsBase,
                                     GetAnyReplicaOptionsBase,
                                     GetOptionsBase,
                                     IncrementOptionsBase,
                                     InsertOptionsBase,
                                     LookupInAllReplicasOptionsBase,
                                     LookupInAnyReplicaOptionsBase,
                                     LookupInOptionsBase,
                                     MutateInOptionsBase,
                                     OptionsTimeoutBase,
                                     PingOptionsBase,
                                     PrependOptionsBase,
                                     QueryOptionsBase,
                                     RemoveOptionsBase,
                                     ReplaceOptionsBase,
                                     ScanOptionsBase,
                                     SearchOptionsBase,
                                     SignedInt64Base,
                                     TouchOptionsBase,
                                     UnlockOptionsBase,
                                     UnsignedInt32Base,
                                     UnsignedInt64Base,
                                     UpsertOptionsBase,
                                     VectorSearchOptionsBase,
                                     ViewOptionsBase,
                                     WaitUntilReadyOptionsBase)
from couchbase.logic.supportability import Supportability
from couchbase.pycbc_core import (transaction_config,
                                  transaction_options,
                                  transaction_query_options)
from couchbase.serializer import DefaultJsonSerializer

# allows for imports only during type checking and not during runtime -- :)
if TYPE_CHECKING:
    from couchbase._utils import JSONType
    from couchbase.auth import Authenticator
    from couchbase.collection import Collection
    from couchbase.durability import DurabilityType, ServerDurability
    from couchbase.n1ql import QueryScanConsistency
    from couchbase.transactions import TransactionKeyspace
    from couchbase.transcoder import Transcoder


class ClusterTracingOptions(ClusterTracingOptionsBase):
    """Available tracing options to set when creating a cluster.

    .. warning::
        Importing options from ``couchbase.cluster`` is deprecated.
        All options should be imported from ``couchbase.options``.

    These will be the default timeouts for operations for the entire cluster

    Args:
        tracing_threshold_kv (timedelta, optional): KV operations threshold. Defaults to None.
        tracing_threshold_view (timedelta, optional): Views operations threshold. Defaults to None.
        tracing_threshold_query (timedelta, optional): Query operations threshold. Defaults to None.
        tracing_threshold_search (timedelta, optional): Search operations threshold.. Defaults to None.
        tracing_threshold_analytics (timedelta, optional): Analytics operations threshold. Defaults to None.
        tracing_threshold_eventing (timedelta, optional): Eventing operations threshold. Defaults to None.
        tracing_threshold_management (timedelta, optional): Management operations threshold. Defaults to None.
        tracing_threshold_queue_size (int, optional): Size of tracing operations queue. Defaults to None.
        tracing_threshold_queue_flush_interval (timedelta, optional): Interveral to flush tracing operations queue.
            Defaults to None.
        tracing_orphaned_queue_size (int, optional): Size of tracing orphaned operations queue. Defaults to None.
        tracing_orphaned_queue_flush_interval (timedelta, optional): Interveral to flush tracing orphaned operations
            queue. Defaults to None.
    """


class ClusterTimeoutOptions(ClusterTimeoutOptionsBase):
    """Available timeout options to set when creating a cluster.

    .. warning::
        Importing options from ``couchbase.cluster`` is deprecated.
        All options should be imported from ``couchbase.options``.

    These will set the default timeouts for operations for the cluster.  Some operations allow the timeout to
    be overridden on a per operation basis.

    Args:
        bootstrap_timeout (timedelta, optional): Overall bootstrap timeout. Defaults to None.
        resolve_timeout (timedelta, optional): Time to resolve hostnames. Defaults to None.
        connect_timeout (timedelta, optional): connect timeout. Defaults to None.
        kv_timeout (timedelta, optional): KV operations timeout. Defaults to None.
        kv_durable_timeout (timedelta, optional): KV durability operations timeout. Defaults to None.
        views_timeout (timedelta, optional): views operations timeout. Defaults to None.
        query_timeout (timedelta, optional): query operations timeout. Defaults to None.
        analytics_timeout (timedelta, optional): analytics operations timeout. Defaults to None.
        search_timeout (timedelta, optional): search operations timeout. Defaults to None.
        management_timeout (timedelta, optional): management operations timeout. Defaults to None.
        dns_srv_timeout (timedelta, optional): Time to make DNS-SRV query. Defaults to None.
        idle_http_connection_timeout (timedelta, optional): Idle HTTP connection timeout. Defaults to None.
        config_idle_redial_timeout (timedelta, optional): Idle redial timeout. Defaults to None.
        config_total_timeout (timedelta, optional): **DEPRECATED** complete bootstrap timeout. Defaults to None.
    """


class ConfigProfile(ABC):
    """
    **VOLATILE** This API is subject to change at any time.

    This is an abstract base class intended to use with creating Configuration Profiles.  Any derived class
    will need to implement the :meth:`apply` method.
    """

    def __init__(self):
        super().__init__()

    @abstractmethod
    def apply(self,
              options  # type: ClusterOptions
              ) -> None:
        """
        **VOLATILE** This API is subject to change at any time.

        Apply the provided options to ClusterOptions. This method will need to be implemented in derived classes.

        Args:
            options (:class:`~couchbase.options.ClusterOptions`): The options the profile will apply toward.
        """
        pass


class WanDevelopmentProfile(ConfigProfile):
    """
    **VOLATILE** This API is subject to change at any time.

    The WAN Development profile sets various timeout options that are useful when develoption in a WAN environment.
    """

    def __init__(self):
        super().__init__()

    def apply(self,
              options  # type: ClusterOptions
              ) -> None:
        # Need to use keys in couchbase.logic.ClusterTimeoutOptionsBase._VALID_OPTS
        options['kv_timeout'] = timedelta(seconds=20)
        options['kv_durable_timeout'] = timedelta(seconds=20)
        options['connect_timeout'] = timedelta(seconds=20)
        options['analytics_timeout'] = timedelta(seconds=120)
        options['query_timeout'] = timedelta(seconds=120)
        options['search_timeout'] = timedelta(seconds=120)
        options['management_timeout'] = timedelta(seconds=120)
        options['views_timeout'] = timedelta(seconds=120)
        options['dns_srv_timeout'] = timedelta(seconds=20)  # time to make DNS-SRV query
        options['resolve_timeout'] = timedelta(seconds=20)  # time to resolve hostnames
        options['bootstrap_timeout'] = timedelta(seconds=120)  # overall bootstrap timeout


class ConfigProfiles():
    """
    **VOLATILE** This API is subject to change at any time.

    The `ConfigProfiles` class is responsible for keeping track of registered/known Configuration
    Profiles.
    """

    def __init__(self):
        self._profiles = {}
        self.register_profile(KnownConfigProfiles.WanDevelopment.value, WanDevelopmentProfile())

    def apply_profile(self,
                      profile_name,  # type: str
                      options  # type: ClusterOptions
                      ) -> None:
        """
        **VOLATILE** This API is subject to change at any time.

        Apply the provided ConfigProfile options.

        Args:
            profile_name (str):  The name of the profile to apply.
            options (:class:`~couchbase.options.ClusterOptions`): The options to apply the ConfigProfile options
                toward. The ConfigProfile options will override any matching option(s) previously set.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the specified profile is not registered.
        """
        if profile_name not in self._profiles:
            raise InvalidArgumentException(f'{profile_name} is not a registered profile.')

        self._profiles[profile_name].apply(options)

    def register_profile(self,
                         profile_name,  # type: str
                         profile,  # type: ConfigProfile
                         ) -> None:
        """
        **VOLATILE** This API is subject to change at any time.

        Register a :class:`~couchbase.options.ConfigProfile`.

        Args:
            profile_name (str):  The name of the :class:`~couchbase.options.ConfigProfile` to register.
            profile (:class:`~couchbase.options.ConfigProfile`): The :class:`~couchbase.options.ConfigProfile`
                to register.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the specified profile is not derived
            from :class:`~couchbase.options.ConfigProfile`.

        """
        if not issubclass(profile.__class__, ConfigProfile):
            raise InvalidArgumentException('A Configuration Profile must be derived from ConfigProfile')

        self._profiles[profile_name] = profile

    def unregister_profile(self,
                           profile_name  # type: str
                           ) -> Optional[ConfigProfile]:
        """
        **VOLATILE** This API is subject to change at any time.

        Unregister a :class:`~couchbase.options.ConfigProfile`.

        Args:
            profile_name (str):  The name of the :class:`~couchbase.options.ConfigProfile` to unregister.

        Returns
            Optional(:class:`~couchbase.options.ConfigProfile`): The unregistered :class:`~couchbase.options.ConfigProfile`
        """  # noqa: E501

        return self._profiles.pop(profile_name, None)


"""
**VOLATILE** The ConfigProfiles API is subject to change at any time.
"""
CONFIG_PROFILES = ConfigProfiles()


class ClusterOptions(ClusterOptionsBase):
    """Available options to set when creating a cluster.

    .. warning::
        Importing options from ``couchbase.cluster`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Cluster options enable the configuration of various global cluster settings.
    Some options can be set globally for the cluster, but overridden for specific
    operations (i.e. ClusterTimeoutOptions)

    .. note::

        The authenticator is mandatory, all the other cluster options are optional.

    Args:
        authenticator (Union[:class:`~.PasswordAuthenticator`, :class:`~.CertificateAuthenticator`]): An
            authenticator instance
        timeout_options (:class:`~.ClusterTimeoutOptions`): Timeout options for
            various SDK operations. See :class:`~.options.ClusterTimeoutOptions` for details.
        tracing_options (:class:`~.options.ClusterTimeoutOptions`): Tracing options for SDK tracing bevavior.
            See :class:`~.options.ClusterTracingOptions` for details.  These are ignored if an external tracer
            is specified.
        enable_tls (bool, optional): Set to True to enable tls. Defaults to False (disabled).
        enable_mutation_tokens (bool, optional): Set to False to disable mutation tokens in mutation results.
            Defaults to True (enabled).
        enable_tcp_keep_alive (bool, optional): Set to False to disable tcp keep alive. Defaults to True (enabled).
        ip_protocol (Union[str, :class:`.IpProtocol`): Set IP protocol. Defaults to IpProtocol.Any.
        enable_dns_srv (bool, optional): Set to False to disable DNS SRV. Defaults to True (enabled).
        show_queries (bool, optional): Set to True to enabled showing queries. Defaults to False (disabled).
        enable_unordered_execution (bool, optional): Set to False to disable unordered query execution.
            Defaults to True (enabled).
        enable_clustermap_notification (bool, optional): Set to False to disable cluster map notification.
            Defaults to True (enabled).
        enable_compression (bool, optional): Set to False to disable compression. Defaults to True (enabled).
        enable_tracing (bool, optional): Set to False to disable tracing (enables no-op tracer).
            Defaults to True (enabled).
        enable_metrics (bool, optional): Set to False to disable metrics (enables no-op meter).
            Defaults to True (enabled).
        network (str, optional): Set network resolution method. Can be set to 'default' (if the client is running on the
            same network as the server) or 'external' (if the client is running on a different network). Defaults to
            'auto'.
        tls_verify (Union[str, :class:`.TLSVerifyMode`], optional): Set tls verify mode. Defaults to
            TLSVerifyMode.PEER.
        disable_mozilla_ca_certificates (bool, optional): Set to True to disable loading Mozilla's list of CA
            certificates for TLS verification. Defaults to False (enabled).
        serializer (:class:`~.serializer.Serializer`, optional): Global serializer to translate JSON to Python objects.
            Defaults to :class:`~.serializer.DefaultJsonSerializer`.
        transcoder (:class:`~.transcoder.Transcoder`, optional): Global transcoder to use for kv-operations.
            Defaults to :class:`~.transcoder.JsonTranscoder`.
        tcp_keep_alive_interval (timedelta, optional): TCP keep-alive interval. Defaults to None.
        config_poll_interval (timedelta, optional): Config polling floor interval.
            Defaults to None.
        config_poll_floor (timedelta, optional): Config polling floor interval.
            Defaults to None.
        max_http_connections (int, optional): Maximum number of HTTP connections.  Defaults to None.
        logging_meter_emit_interval (timedelta, optional): Logging meter emit interval.  Defaults to 10 minutes.
        transaction_config (:class:`.TransactionConfig`, optional): Global configuration for transactions.
            Defaults to None.
        log_redaction (bool, optional): Set to True to enable log redaction. Defaults to False (disabled).
        compression (:class:`~.Compression`, optional): Set compression mode.  Defaults to None.
        compression_min_size (int, optional): Set compression min size.  Defaults to None.
        compression_min_ratio (float, optional): Set compression min size.  Defaults to None.
        lockmode (:class:`~.LockMode`, optional): **DEPRECATED** This option will be removed in a future version of the SDK.
            Set LockMode mode.  Defaults to None.
        tracer (:class:`~couchbase.tracing.CouchbaseTracer`, optional): Set an external tracer.  Defaults to None,
            enabling the `threshold_logging_tracer`. Note when this is set, all tracing_options
            (see :class:`~.ClusterTracingOptions`) and then `enable_tracing` option are ignored.
        meter (:class:`~couchbase.metrics.CouchbaseMeter`, optional): Set an external meter.  Defaults to None,
            enabling the `logging_meter`.   Note when this is set, the `logging_meter_emit_interval` option is ignored.
        dns_nameserver (str, optional):  **VOLATILE** This API is subject to change at any time. Set to configure custom DNS nameserver. Defaults to None.
        dns_port (int, optional):  **VOLATILE** This API is subject to change at any time. Set to configure custom DNS port. Defaults to None.
        dump_configuration (bool, optional): Set to True to dump every new configuration when TRACE level logging. Defaults to False (disabled).
    """  # noqa: E501

    def apply_profile(self,
                      profile_name  # type: Union[KnownConfigProfiles, str]
                      ) -> None:
        """
        **VOLATILE** This API is subject to change at any time.

        Apply the provided ConfigProfile options.

        Args:
            profile_name ([:class:`~couchbase.options.KnownConfigProfiles`, str]):  The name of the profile to apply
                toward ClusterOptions.
            authenticator (Union[:class:`~couchbase.auth.PasswordAuthenticator`, :class:`~couchbaes.auth.CertificateAuthenticator`]): An authenticator instance.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the specified profile is not registered.

        """  # noqa: E501
        prof_name = profile_name.value if isinstance(profile_name, KnownConfigProfiles) else profile_name
        CONFIG_PROFILES.apply_profile(prof_name, self)

    @classmethod
    def create_options_with_profile(cls,
                                    authenticator,  # type: Optional[Authenticator]
                                    profile_name  # type: Union[KnownConfigProfiles, str]
                                    ) -> ClusterOptions:
        """
        **VOLATILE** This API is subject to change at any time.

        Create a ClusterOptions instance and apply the provided ConfigProfile options.

        Args:
            authenticator (Union[:class:`~couchbase.auth.PasswordAuthenticator`, :class:`~couchbaes.auth.CertificateAuthenticator`]): An authenticator instance.
            profile_name ([:class:`~couchbase.options.KnownConfigProfiles`, str]):  The name of the profile to apply
                toward ClusterOptions.

        Raises:
            :class:`~couchbase.exceptions.InvalidArgumentException`: If the specified profile is not registered.

        """  # noqa: E501
        opts = cls(authenticator)
        prof_name = profile_name.value if isinstance(profile_name, KnownConfigProfiles) else profile_name
        CONFIG_PROFILES.apply_profile(prof_name, opts)
        return opts

# Diagnostics Operations


class PingOptions(PingOptionsBase):
    """Available options to for a ping operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        report_id (str, optional): A unique identifier for the report generated by this operation.
        service_types (Iterable[class:`~couchbase.diagnostics.ServiceType`]): The services which should be pinged.
    """


class DiagnosticsOptions(DiagnosticsOptionsBase):
    """Available options to for a diagnostics operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        report_id (str, optional): A unique identifier for the report generated by this operation.
    """


class WaitUntilReadyOptions(WaitUntilReadyOptionsBase):
    """Available options to for a wait until ready operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        desired_state (:class:`~.couchbase.diagnostics.ClusterState`, optional): The desired state to wait for in
            order to determine the cluster or bucket is ready.  Defaults to `Online`.
        service_types (Iterable[class:`~couchbase.diagnostics.ServiceType`]): The services which should be pinged.
    """


# Key-Value Operations

class OptionsTimeout(OptionsTimeoutBase):
    pass


class DurabilityOptionBlock(DurabilityOptionBlockBase):
    pass


class ExistsOptions(ExistsOptionsBase):
    """Available options to for a key-value exists operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
    """


class GetOptions(GetOptionsBase):
    """Available options to for a key-value get operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        with_expiry (bool, optional): Indicates that the expiry of the document should be
            fetched alongside the data itself. Defaults to False.
        project (Iterable[str], optional): Specifies a list of fields within the document which should be fetched.
            This allows for easy retrieval of select fields without incurring the overhead of fetching the
            whole document.
        transcoder (:class:`~.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """


class GetAllReplicasOptions(GetAllReplicasOptionsBase):
    """Available options to for a key-value get and touch operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        transcoder (:class:`~.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """


class GetAndLockOptions(GetAndLockOptionsBase):
    """Available options to for a key-value get and lock operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        transcoder (:class:`~.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """


class GetAndTouchOptions(GetAndTouchOptionsBase):
    """Available options to for a key-value get and touch operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        transcoder (:class:`~.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """


class GetAnyReplicaOptions(GetAnyReplicaOptionsBase):
    """Available options to for a key-value get and touch operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        transcoder (:class:`~.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """


class InsertOptions(InsertOptionsBase):
    """Available options to for a key-value insert operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        expiry (timedelta, optional): Specifies the expiry time for this document.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """


class RemoveOptions(RemoveOptionsBase):
    """Available options to for a key-value remove operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from
            this value, indicating that the document has changed.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
    """


class ReplaceOptions(ReplaceOptionsBase):
    """Available options to for a key-value replace operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from
            this value, indicating that the document has changed.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        expiry (timedelta, optional): Specifies the expiry time for this document.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        preserve_expiry (bool, optional): Specifies that any existing expiry on the document should be preserved.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """


class TouchOptions(TouchOptionsBase):
    """Available options to for a key-value exists operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
    """


class UnlockOptions(UnlockOptionsBase):
    """Available options to for a key-value exists operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
    """


class UpsertOptions(UpsertOptionsBase):
    """Available options to for a key-value upsert operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        expiry (timedelta, optional): Specifies the expiry time for this document.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        preserve_expiry (bool, optional): Specifies that any existing expiry on the document should be preserved.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """


class ScanOptions(ScanOptionsBase):
    """Available options to for a key-value scan operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            range scan operation timeout.
        ids_only (bool, optional): Specifies that scan should only return document ids. Defaults to False.
        consistent_with (:class:`~couchbase.mutation_state.MutationState`, optional): Specifies a
            :class:`~couchbase.mutation_state.MutationState` which the scan should be consistent with. Defaults to None.
        batch_byte_limit (int, optional): The limit applied to the number of bytes returned from the server
            for each partition batch. Defaults to 15k.
        batch_item_limit (int, optional): The limit applied to the number of items returned from the server
            for each partition batch. Defaults to 50.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~couchbase.transcoder.JsonTranscoder`.
        concurrency (int, optional): The upper bound on the number of vbuckets that should be scanned in parallel.
            Defaults to 1.
    """  # noqa: E501


# Sub-document Operations


class LookupInOptions(LookupInOptionsBase):
    """Available options to for a subdocument lookup-in operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            subdocument operation timeout.
    """


class LookupInAnyReplicaOptions(LookupInAnyReplicaOptionsBase):
    """Available options to for a subdocument lookup-in operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            subdocument operation timeout.
    """


class LookupInAllReplicasOptions(LookupInAllReplicasOptionsBase):
    """Available options to for a subdocument lookup-in operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            subdocument operation timeout.
    """


class MutateInOptions(MutateInOptionsBase):
    """Available options to for a subdocument mutate-in operation.

    .. warning::
        Importing options from ``couchbase.subdocument`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from
            this value, indicating that the document has changed.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            subdocument operation timeout.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        preserve_expiry (bool, optional): Specifies that any existing expiry on the document should be preserved.
        store_semantics (:class:`~couchbase.subdocument.StoreSemantics`, optional): Specifies the store semantics
            to use for this operation.
    """

# Binary Operations


class AppendOptions(AppendOptionsBase):
    """Available options to for a binary append operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from
            this value, indicating that the document has changed.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
    """


class PrependOptions(PrependOptionsBase):
    """Available options to for a binary prepend operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from
            this value, indicating that the document has changed.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
    """


class IncrementOptions(IncrementOptionsBase):
    """Available options to for a binary increment operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        delta (:class:`.DeltaValue`, optional): The amount to increment the key. Defaults to 1.
        initial (:class:`.SignedInt64`, optional): The initial value to use for the document if it does not already
            exist. Setting it to a negative value means that the document will not be created if it does not exist.
            Defaults to 0.
    """


class DecrementOptions(DecrementOptionsBase):
    """Available options to for a decrement append operation.

    .. warning::
        Importing options from ``couchbase.collection`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        delta (:class:`.DeltaValue`, optional): The amount to increment the key. Defaults to 1.
        initial (:class:`.SignedInt64`, optional): The initial value to use for the document if it does not already
            exist. Setting it to a negative value means that the document will not be created if it does not exist.
            Defaults to 0.
    """


"""

Multi-operations Options

"""


class GetAllReplicasMultiOptions(dict):
    """Available options to for a key-value multi-get-all-replicas operation.

    Options can be set at a global level (i.e. for all get operations handled with this multi-get-all-replicas
    operation). Use *per_key_options* to set specific :class:`.GetAllReplicasOptions` for specific keys.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
        per_key_options (Dict[str, :class:`.GetAllReplicasOptions`], optional): Specify
            :class:`.GetAllReplicasOptions` per key.
        return_exceptions(bool, optional): If False, raise an Exception when encountered.  If True return the
            Exception without raising.  Defaults to True.
    """
    @overload
    def __init__(
        self,
        transcoder=None,  # type: Transcoder
        per_key_options=None,       # type: Dict[str, GetAllReplicasOptions]
        return_exceptions=None      # type: Optional[bool]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @classmethod
    def get_valid_keys(cls):
        return ['timeout', 'transcoder', 'per_key_options', 'return_exceptions']


class GetAnyReplicaMultiOptions(dict):
    """Available options to for a key-value multi-get-any-replica operation.

    Options can be set at a global level (i.e. for all get operations handled with this multi-get-any-replica
    operation). Use *per_key_options* to set specific :class:`.GetAnyReplicaOptions` for specific keys.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
        per_key_options (Dict[str, :class:`.GetAnyReplicaOptions`], optional): Specify
            :class:`.GetAnyReplicaOptions` per key.
        return_exceptions(bool, optional): If False, raise an Exception when encountered.  If True return the
            Exception without raising.  Defaults to True.
    """
    @overload
    def __init__(
        self,
        transcoder=None,  # type: Transcoder
        per_key_options=None,       # type: Dict[str, GetAnyReplicaOptions]
        return_exceptions=None      # type: Optional[bool]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @classmethod
    def get_valid_keys(cls):
        return ['timeout', 'transcoder', 'per_key_options', 'return_exceptions']


class GetMultiOptions(dict):
    """Available options to for a key-value multi-get operation.

    Options can be set at a global level (i.e. for all get operations handled with this multi-get operation).
    Use *per_key_options* to set specific :class:`.GetOptions` for specific keys.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        with_expiry (bool, optional): Indicates that the expiry of the document should be
            fetched alongside the data itself. Defaults to False.
        project (Iterable[str], optional): Specifies a list of fields within the document which should be fetched.
            This allows for easy retrieval of select fields without incurring the overhead of fetching the
            whole document.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
        per_key_options (Dict[str, :class:`.GetOptions`], optional): Specify :class:`.GetOptions` per key.
        return_exceptions(bool, optional): If False, raise an Exception when encountered.  If True return the
            Exception without raising.  Defaults to True.
    """
    @overload
    def __init__(
        self,
        timeout=None,  # type: timedelta
        with_expiry=None,  # type: bool
        project=None,  # type: Iterable[str]
        transcoder=None,  # type: Transcoder
        per_key_options=None,       # type: Dict[str, GetOptions]
        return_exceptions=None      # type: Optional[bool]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @classmethod
    def get_valid_keys(cls):
        return ['timeout', 'with_expiry', 'project', 'transcoder',
                'per_key_options', 'return_exceptions']


class ExistsMultiOptions(dict):
    """Available options to for a key-value multi-exists operation.

    Options can be set at a global level (i.e. for all exists operations handled with this multi-exists operation).
    Use *per_key_options* to set specific :class:`.ExistsOptions` for specific keys.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        per_key_options (Dict[str, :class:`.ExistsOptions`], optional): Specify :class:`.ExistsOptions` per key.
        return_exceptions(bool, optional): If False, raise an Exception when encountered.  If True return the
            Exception without raising.  Defaults to True.
    """
    @overload
    def __init__(
        self,
        timeout=None,  # type: timedelta
        per_key_options=None,       # type: Dict[str, ExistsOptions]
        return_exceptions=None      # type: Optional[bool]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @classmethod
    def get_valid_keys(cls):
        return ['timeout', 'per_key_options', 'return_exceptions']


class UpsertMultiOptions(dict):
    """Available options to for a key-value multi-upsert operation.

    Options can be set at a global level (i.e. for all upsert operations handled with this multi-upsert operation).
    Use *per_key_options* to set specific :class:`.UpsertOptions` for specific keys.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        expiry (timedelta, optional): Specifies the expiry time for this document.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        preserve_expiry (bool, optional): Specifies that any existing expiry on the document should be preserved.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
        per_key_options (Dict[str, :class:`.UpsertOptions`], optional): Specify :class:`.UpsertOptions` per key.
        return_exceptions(bool, optional): If False, raise an Exception when encountered.  If True return the
            Exception without raising.  Defaults to True.
    """
    @overload
    def __init__(
        self,
        timeout=None,  # type: timedelta
        expiry=None,  # type: timedelta
        preserve_expiry=False,  # type: bool
        durability=None,  # type: DurabilityType
        transcoder=None,  # type: Transcoder
        per_key_options=None,       # type: Dict[str, UpsertOptions]
        return_exceptions=None      # type: Optional[bool]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @classmethod
    def get_valid_keys(cls):
        return ['timeout', 'expiry', 'preserve_expiry', 'durability',
                'transcoder', 'per_key_options', 'return_exceptions']


class InsertMultiOptions(dict):
    """Available options to for a key-value multi-insert operation.

    Options can be set at a global level (i.e. for all insert operations handled with this multi-insert operation).
    Use *per_key_options* to set specific :class:`.InsertOptions` for specific keys.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        expiry (timedelta, optional): Specifies the expiry time for this document.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
        per_key_options (Dict[str, :class:`.InsertOptions`], optional): Specify :class:`.InsertOptions` per key.
        return_exceptions(bool, optional): If False, raise an Exception when encountered.  If True return the
            Exception without raising.  Defaults to True.
    """
    @overload
    def __init__(
        self,
        timeout=None,  # type: timedelta
        expiry=None,  # type: timedelta
        durability=None,  # type: DurabilityType
        transcoder=None,  # type: Transcoder
        per_key_options=None,       # type: Dict[str, InsertOptions]
        return_exceptions=None      # type: Optional[bool]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @classmethod
    def get_valid_keys(cls):
        return ['timeout', 'expiry', 'durability', 'transcoder', 'per_key_options', 'return_exceptions']


class ReplaceMultiOptions(dict):
    """Available options to for a key-value multi-replace operation.

    Options can be set at a global level (i.e. for all replace operations handled with this multi-replace operation).
    Use *per_key_options* to set specific :class:`.ReplaceOptions` for specific keys.

    Args:
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from
            this value, indicating that the document has changed.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        expiry (timedelta, optional): Specifies the expiry time for this document.
        preserve_expiry (bool, optional): Specifies that any existing expiry on the document should be preserved.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
        per_key_options (Dict[str, :class:`.ReplaceOptions`], optional): Specify :class:`.ReplaceOptions` per key.
        return_exceptions(bool, optional): If False, raise an Exception when encountered.  If True return the
            Exception without raising.  Defaults to True.
    """
    @overload
    def __init__(
        self,
        timeout=None,  # type: timedelta
        expiry=None,  # type: timedelta
        cas=0,  # type: int
        preserve_expiry=False,  # type: bool
        durability=None,  # type: DurabilityType
        transcoder=None,  # type: Transcoder
        per_key_options=None,       # type: Dict[str, ReplaceOptions]
        return_exceptions=None      # type: Optional[bool]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @classmethod
    def get_valid_keys(cls):
        return ['timeout', 'expiry', 'cas', 'preserve_expiry',
                'durability', 'transcoder', 'per_key_options', 'return_exceptions']


class RemoveMultiOptions(dict):
    """Available options to for a key-value multi-remove operation.

    Options can be set at a global level (i.e. for all remove operations handled with this multi-remove operation).
    Use *per_key_options* to set specific :class:`.RemoveOptions` for specific keys.

    Args:
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from
            this value, indicating that the document has changed.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        transcoder (:class:`~couchbase.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
        per_key_options (Dict[str, :class:`.RemoveOptions`], optional): Specify :class:`.RemoveOptions` per key.
        return_exceptions(bool, optional): If False, raise an Exception when encountered.  If True return the
            Exception without raising.  Defaults to True.
    """
    @overload
    def __init__(
        self,
        timeout=None,  # type: timedelta
        cas=0,  # type: int
        durability=None,  # type: DurabilityType
        transcoder=None,  # type: Transcoder
        per_key_options=None,       # type: Dict[str, RemoveOptions]
        return_exceptions=None      # type: Optional[bool]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @classmethod
    def get_valid_keys(cls):
        return ['timeout', 'cas', 'durability', 'transcoder', 'per_key_options', 'return_exceptions']


class TouchMultiOptions(dict):
    """Available options to for a key-value multi-touch operation.

    Options can be set at a global level (i.e. for all touch operations handled with this multi-touch operation).
    Use *per_key_options* to set specific :class:`.TouchOptions` for specific keys.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        per_key_options (Dict[str, :class:`.TouchOptions`], optional): Specify :class:`.TouchOptions` per key.
        return_exceptions(bool, optional): If False, raise an Exception when encountered.  If True return the
            Exception without raising.  Defaults to True.
    """
    @overload
    def __init__(
        self,
        timeout=None,  # type: timedelta
        per_key_options=None,       # type: Dict[str, TouchOptions]
        return_exceptions=None      # type: Optional[bool]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @classmethod
    def get_valid_keys(cls):
        return ['timeout', 'expiry', 'per_key_options', 'return_exceptions']


class GetAndLockMultiOptions(dict):
    """Available options to for a key-value multi-lock operation.

    Options can be set at a global level (i.e. for all lock operations handled with this multi-lock operation).
    Use *per_key_options* to set specific :class:`.GetAndLockOptions` for specific keys.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        per_key_options (Dict[str, :class:`.GetAndLockOptions`], optional): Specify :class:`.GetAndLockOptions` per
            key.
        return_exceptions(bool, optional): If False, raise an Exception when encountered.  If True return the
            Exception without raising.  Defaults to True.
    """
    @overload
    def __init__(
        self,
        timeout=None,  # type: timedelta
        transcoder=None,  # type: Transcoder
        per_key_options=None,       # type: Dict[str, GetAndLockOptions]
        return_exceptions=None      # type: Optional[bool]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @classmethod
    def get_valid_keys(cls):
        return ['timeout', 'transcoder', 'per_key_options', 'return_exceptions']


LockMultiOptions = GetAndLockMultiOptions


class UnlockMultiOptions(dict):
    """Available options to for a key-value multi-unlock operation.

    Options can be set at a global level (i.e. for all unlock operations handled with this multi-unlock operation).
    Use *per_key_options* to set specific :class:`.UnlockOptions` for specific keys.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        per_key_options (Dict[str, :class:`.UnlockOptions`], optional): Specify :class:`.UnlockOptions` per
            key.
        return_exceptions(bool, optional): If False, raise an Exception when encountered.  If True return the
            Exception without raising.  Defaults to True.
    """
    @overload
    def __init__(
        self,
        timeout=None,  # type: timedelta
        per_key_options=None,       # type: Dict[str, UnlockOptions]
        return_exceptions=None      # type: Optional[bool]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @classmethod
    def get_valid_keys(cls):
        return ['timeout', 'per_key_options', 'return_exceptions']


class IncrementMultiOptions(dict):
    """Available options to for a binary multi-increment operation.

    Options can be set at a global level (i.e. for all increment operations handled with this multi-increment operation).
    Use *per_key_options* to set specific :class:`.IncrementOptions` for specific keys.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        delta (:class:`.DeltaValue`, optional): The amount to increment the key. Defaults to 1.
        initial (:class:`.SignedInt64`, optional): The initial value to use for the document if it does not already
            exist. Defaults to 0.
        per_key_options (Dict[str, :class:`.IncrementOptions`], optional): Specify :class:`.IncrementOptions` per
            key.
        return_exceptions(bool, optional): If False, raise an Exception when encountered.  If True return the
            Exception without raising.  Defaults to True.
    """  # noqa: E501
    @overload
    def __init__(
        self,
        timeout=None,      # type: Optional[timedelta]
        durability=None,   # type: Optional[DurabilityType]
        delta=None,         # type: Optional[DeltaValue]
        initial=None,      # type: Optional[SignedInt64]
        span=None,         # type: Optional[Any]
        per_key_options=None,       # type: Optional[Dict[str, IncrementOptions]]
        return_exceptions=None      # type: Optional[bool]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @classmethod
    def get_valid_keys(cls):
        return ['timeout', 'durability', 'delta',
                'initial', 'span', 'per_key_options', 'return_exceptions']


class DecrementMultiOptions(dict):
    """Available options to for a binary multi-decrement operation.

    Options can be set at a global level (i.e. for all decrement operations handled with this multi-decrement operation).
    Use *per_key_options* to set specific :class:`.DecrementOptions` for specific keys.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        delta (:class:`.DeltaValue`, optional): The amount to decrement the key. Defaults to 1.
        initial (:class:`.SignedInt64`, optional): The initial value to use for the document if it does not already
            exist. Defaults to 0.
        per_key_options (Dict[str, :class:`.DecrementOptions`], optional): Specify :class:`.DecrementOptions` per
            key.
        return_exceptions(bool, optional): If False, raise an Exception when encountered.  If True return the
            Exception without raising.  Defaults to True.
    """  # noqa: E501
    @overload
    def __init__(
        self,
        timeout=None,      # type: Optional[timedelta]
        durability=None,   # type: Optional[DurabilityType]
        delta=None,         # type: Optional[DeltaValue]
        initial=None,      # type: Optional[SignedInt64]
        span=None,         # type: Optional[Any]
        per_key_options=None,       # type: Optional[Dict[str, DecrementOptions]]
        return_exceptions=None      # type: Optional[bool]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @classmethod
    def get_valid_keys(cls):
        return ['timeout', 'durability', 'delta',
                'initial', 'span', 'per_key_options', 'return_exceptions']


class AppendMultiOptions(dict):
    """Available options to for a binary multi-append operation.

    Options can be set at a global level (i.e. for all append operations handled with this multi-append operation).
    Use *per_key_options* to set specific :class:`.AppendOptions` for specific keys.

    Args:
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from
            this value, indicating that the document has changed.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        per_key_options (Dict[str, :class:`.AppendOptions`], optional): Specify :class:`.AppendOptions` per key.
        return_exceptions(bool, optional): If False, raise an Exception when encountered.  If True return the
            Exception without raising.  Defaults to True.
    """  # noqa: E501
    @overload
    def __init__(
        self,
        timeout=None,      # type: Optional[timedelta]
        durability=None,   # type: Optional[DurabilityType]
        cas=None,          # type: Optional[int]
        span=None,         # type: Optional[Any]
        per_key_options=None,       # type: Optional[Dict[str, AppendOptions]]
        return_exceptions=None      # type: Optional[bool]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @classmethod
    def get_valid_keys(cls):
        return ['timeout', 'durability', 'cas',
                'span', 'per_key_options', 'return_exceptions']


class PrependMultiOptions(dict):
    """Available options to for a binary multi-prepend operation.

    Options can be set at a global level (i.e. for all prepend operations handled with this multi-prepend operation).
    Use *per_key_options* to set specific :class:`.PrependOptions` for specific keys.

    Args:
        cas (int, optional): If specified, indicates that operation should be failed if the CAS has changed from
            this value, indicating that the document has changed.
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            key-value operation timeout.
        durability (:class:`~couchbase.durability.DurabilityType`, optional): Specifies the level of durability
            for this operation.
        per_key_options (Dict[str, :class:`.PrependOptions`], optional): Specify :class:`.PrependOptions` per key.
        return_exceptions(bool, optional): If False, raise an Exception when encountered.  If True return the
            Exception without raising.  Defaults to True.
    """  # noqa: E501
    @overload
    def __init__(
        self,
        timeout=None,      # type: Optional[timedelta]
        durability=None,   # type: Optional[DurabilityType]
        cas=None,          # type: Optional[int]
        span=None,         # type: Optional[Any]
        per_key_options=None,       # type: Optional[Dict[str, PrependOptions]]
        return_exceptions=None      # type: Optional[bool]
    ):
        pass

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)

    @classmethod
    def get_valid_keys(cls):
        return ['timeout', 'durability', 'cas',
                'span', 'per_key_options', 'return_exceptions']


NoValueMultiOptions = Union[GetMultiOptions, ExistsMultiOptions,
                            RemoveMultiOptions, TouchMultiOptions, LockMultiOptions, UnlockMultiOptions]
MutationMultiOptions = Union[InsertMultiOptions, UpsertMultiOptions, ReplaceMultiOptions]
MutationOptions = Union[InsertOptions,
                        UpsertOptions,
                        ReplaceOptions,
                        RemoveOptions,
                        MutateInOptions,
                        AppendOptions,
                        PrependOptions,
                        IncrementOptions,
                        DecrementOptions]


class QueryOptions(QueryOptionsBase):
    """Available options to for a N1QL (SQL++) query.

    .. warning::
        Importing options from ``couchbase.cluster`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            query operation timeout.
        read_only (bool, optional): Specifies that this query should be executed in read-only mode,
            disabling the ability for the query to make any changes to the data. Defaults to False.
        scan_consistency (:class:`~couchbase.n1ql.QueryScanConsistency`, optional): Specifies the consistency
            requirements when executing the query.
        adhoc (bool, optional): Specifies whether this is an ad-hoc query, or if it should be prepared for
            faster execution in the future. Defaults to True.
        client_context_id (str, optional): The returned client context id for this query. Defaults to None.
        max_parallelism (int, optional): This is an advanced option, see the query service reference for more
            information on the proper use and tuning of this option. Defaults to None.
        positional_parameters (Iterable[JSONType], optional): Positional values to be used for the placeholders
            within the query. Defaults to None.
        named_parameters (Iterable[Dict[str, JSONType]], optional): Named values to be used for the placeholders
            within the query. Defaults to None.
        pipeline_batch (int, optional): This is an advanced option, see the query service reference for more
            information on the proper use and tuning of this option. Defaults to None.
        pipeline_cap (int, optional):  This is an advanced option, see the query service reference for more
            information on the proper use and tuning of this option. Defaults to None.
        profile (:class:`~couchbase.n1ql.QueryProfile`, optional): Specifies the level of profiling that should
            be used for the query. Defaults to `Off`.
        query_context (str, optional): Specifies the context within which this query should be executed. This can
            be scoped to a scope or a collection within the dataset. Defaults to None.
        scan_cap (int, optional):  This is an advanced option, see the query service reference for more
            information on the proper use and tuning of this option. Defaults to None.
        scan_wait (timedelta, optional):  This is an advanced option, see the query service reference for more
            information on the proper use and tuning of this option. Defaults to None.
        metrics (bool, optional): Specifies whether metrics should be captured as part of the execution of the query.
            Defaults to False.
        flex_index (bool, optional): Specifies whether flex-indexes should be enabled. Allowing the use of full-text
            search from the query service. Defaults to False.
        preserve_expiry (bool, optional): Specifies that any existing expiry on the document should be preserved.
            Defaults to False.
        use_replica (bool, optional): Specifies that the query engine should use replica nodes for KV fetches if the
            active node is down. Defaults to None.
        consistent_with (:class:`~couchbase.mutation_state.MutationState`, optional): Specifies a
            :class:`~couchbase.mutation_state.MutationState` which the query should be consistent with. Defaults to
            None.
        serializer (:class:`~couchbase.serializer.Serializer`, optional): Specifies an explicit serializer
            to use for this specific N1QL operation. Defaults to
            :class:`~couchbase.serializer.DefaultJsonSerializer`.
        raw (Dict[str, Any], optional): Specifies any additional parameters which should be passed to the query engine
            when executing the query. Defaults to None.
    """


class AnalyticsOptions(AnalyticsOptionsBase):
    """Available options to for an analytics query.

    .. warning::
        Importing options from ``couchbase.analytics`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            query operation timeout.
        read_only (bool, optional): Specifies that this analytics query should be executed in read-only mode,
            disabling the ability for the query to make any changes to the data. Defaults to False.
        scan_consistency (:class:`~couchbase.analytics.AnalyticsScanConsistency`, optional): Specifies the consistency
            requirements when executing the analytics query.
        client_context_id (str, optional): The returned client context id for this analytics query. Defaults to None.
        positional_parameters (Iterable[JSONType], optional): Positional values to be used for the placeholders
            within the analytics query. Defaults to None.
        named_parameters (Iterable[Dict[str, JSONType]], optional): Named values to be used for the placeholders
            within the analytics query. Defaults to None.
        priority (bool, optional): Indicates whether this analytics query should be executed with a specific priority
            level. Defaults to False.
        query_context (str, optional): Specifies the context within which this analytics query should be executed.
            This can be scoped to a scope or a collection within the dataset. Defaults to None.
        serializer (:class:`~couchbase.serializer.Serializer`, optional): Specifies an explicit serializer
            to use for this specific analytics query. Defaults to
            :class:`~couchbase.serializer.DefaultJsonSerializer`.
        raw (Dict[str, Any], optional): Specifies any additional parameters which should be passed to the analytics
            query engine when executing the analytics query. Defaults to None.
    """


class SearchOptions(SearchOptionsBase):
    """Available options to for a search (FTS) query.

    .. warning::
        Importing options from ``couchbase.search`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            search query operation timeout.
        limit (int, optional): Specifies the limit to the number of results that should be returned.
            Defaults to None.
        skip (int, optional): Specifies the number of results to skip from the index before returning
            results. Defaults to None.
        explain (bool, optional): Configures whether the result should contain the execution plan for the
            search query. Defaults to False.
        fields (List[str], optional): Specifies the list of fields which should be searched. Defaults to None.
        highlight_style (:class:`~couchbase.search.HighlightStyle`, optional): Specifies the mode used for
            highlighting.  Defaults to None.
        highlight_fields (List[str], optional): Specifies the list of fields that should be highlighted.
            Defaults to None.
        scan_consistency (:class:`~couchbase.search.SearchScanConsistency`, optional): Specifies the consistency
            requirements when executing the search query.
        facets (Dict[str, :class:`~couchbase.search.Facet`], optional): Specifies any facets that should be included
            in the search query. Defaults to None.
        client_context_id (str, optional): The returned client context id for this query. Defaults to None.
        disable_scoring (bool, optional): Specifies that scoring should be disabled. This improves performance but
            makes it impossible to sort based on how well a particular result scored. Defaults to False.
        include_locations (bool optional): If set to True, will include the locations in the search result.
            Defaults to False.
        sort (Union[List[str],List[:class:`~couchbase.search.Sort`]], optional): Specifies a list of fields or search
            :class:`~couchbase.search.Sort`'s to use when sorting the result sets. Defaults to None.
        scope_name (string, optional): Specifies the scope which should be searched as part of the search
            query. Defaults to None.
        collections (List[str], optional):  Specifies the collections which should be searched as part of the search
            query. Defaults to None.
        consistent_with (:class:`~couchbase.mutation_state.MutationState`, optional): Specifies a
            :class:`~couchbase.mutation_state.MutationState` which the search query should be consistent with.
            Defaults to None.
        serializer (:class:`~couchbase.serializer.Serializer`, optional): Specifies an explicit serializer
            to use for this specific search query. Defaults to
            :class:`~couchbase.serializer.DefaultJsonSerializer`.
        raw (Dict[str, Any], optional): Specifies any additional parameters which should be passed to the search query
            engine when executing the search query. Defaults to None.
        show_request (bool, optional): Specifies if the search response should contain the request for the search query. Defaults to False.
        log_request (bool, optional): **UNCOMMITTED** Specifies if search request body should appear the log. Defaults to False.
        log_response (bool, optional): **UNCOMMITTED** Specifies if search response should appear in the log. Defaults to False.
    """  # noqa: E501


class VectorSearchOptions(VectorSearchOptionsBase):
    """Available options to for a FTS vector search.

    Args:
        vector_query_combination (:class:`~couchbase.vector_search.VectorQueryCombination`, optional): Specifies logical operation
            to use with multiple vector queries.
    """  # noqa: E501


class ViewOptions(ViewOptionsBase):
    """Available options to for a view query.

    .. warning::
        Importing options from ``couchbase.bucket`` is deprecated.
        All options should be imported from ``couchbase.options``.

    Args:
        timeout (timedelta, optional): The timeout for this operation. Defaults to global
            view query operation timeout.
        scan_consistency (:class:`~couchbase.views.ViewScanConsistency`, optional): Specifies the consistency
            requirements when executing the view query. Defaults to None.
        limit (int, optional): Specifies the limit to the number of results that should be returned.
            Defaults to None.
        skip (int, optional): Specifies the number of results to skip from the index before returning
            results. Defaults to None.
        startkey (JSONType, optional): Specifies the first key that should be included in the results.
            Defaults to None.
        endkey (JSONType, optional): Specifies the last key that should be included in the results.
            Defaults to None.
        startkey_docid (str, optional): Specifies the first document ID that should be included in the results.
            Defaults to None.
        endkey_docid (str, optional): Specifies the last document ID that should be included in the results.
            Defaults to None.
        inclusive_end (bool, optional): Specifies whether the end key should be considered inclusive or exclusive.
            Defaults to None.
        group (bool, optional): Specifies whether the results should be grouped together. Defaults to None.
        group_level (int, optional): Specifies the level to which results should be group. Defaults to None.
        key (JSONType, optional): Specifies a specific key which should be fetched from the index. Defaults to None.
        keys (List[JSONType], optional): Specifies a list of keys which should be fetched from the index.
            Defaults to None.
        order (:class:`~couchbase.views.ViewOrdering`, optional): Specifies the ordering that should be used when
            returning results. Defaults to None.
        reduce (bool, optional): Specifies whether reduction should be performed as part of the view query.
            Defaults to None.
        on_error (:class:`~couchbase.views.ViewErrorMode`, optional): Specifies the error-handling behaviour
            that should be used when an error occurs. Defaults to None.
        namespace(:class:`~couchbase.management.views.DesignDocumentNamespace`, optional): Specifies the namespace
            for the design document.  Defaults to ``Development``.
        client_context_id (str, optional): The returned client context id for this view query. Defaults to None.
        raw (Dict[str, str], optional): Specifies any additional parameters which should be passed to the view engine
            when executing the view query. Defaults to None.
        full_set (bool, optional): Specifies whether the query should force the entire set of document in the index
            to be included in the result.  Defaults to None.
    """


"""

Couchbase Python SDK constrained integer classes

"""


class ConstrainedInt(ConstrainedIntBase):
    pass


class SignedInt64(SignedInt64Base):
    pass


class UnsignedInt32(UnsignedInt32Base):
    pass


class UnsignedInt64(UnsignedInt64Base):
    pass


class DeltaValue(DeltaValueBase):
    pass


class TransactionConfig:
    _TXN_ALLOWED_KEYS = {"durability", "cleanup_window", "timeout",
                         "expiration_time", "cleanup_lost_attempts", "cleanup_client_attempts",
                         "metadata_collection", "scan_consistency"}

    @overload
    def __init__(self,
                 durability=None,   # type: Optional[ServerDurability]
                 cleanup_window=None,  # type: Optional[timedelta]
                 kv_timeout=None,  # type: Optional[timedelta]
                 expiration_time=None,  # type: Optional[timedelta]
                 cleanup_lost_attempts=None,  # type: Optional[bool]
                 cleanup_client_attempts=None,  # type: Optional[bool]
                 metadata_collection=None,  # type: Optional[TransactionKeyspace]
                 scan_consistency=None  # type: Optional[QueryScanConsistency]
                 ):
        """
        Configuration for Transactions.

        Args:
            durability (:class:`ServerDurability`, optional): Desired durability level for all transaction operations.
            cleanup_window (timedelta, optional): The query metadata is cleaned up over a the cleanup_window.
              Longer windows mean less background activity, shorter intervals will clean things faster.
            kv_timeout: (timedelta, optional): **DEPRECATED** Currently a no-op. KV operation timeout.
            expiration_time: (timedelta, optional): **DEPRECATED** Use timeout instead. Maximum amount of time a transaction can take before rolling back.
            cleanup_lost_attempts: (bool, optional): If False, then we don't do any background cleanup.
            cleanup_client_attempts: (bool, optional): if False, we don't do any cleanup as a transaction finishes.
            metadata_collection: (:class:`couchbase.transactions.TransactionKeyspace, optional): All transaction
              metadata uses the specified bucket/scope/collection.
            scan_consistency: (:class:`QueryScanConsistency`, optional): Scan consistency to use for all transactional
              queries.
            timeout: (timedelta, optional): Maximum amount of time a transaction can take before rolling back.
        """  # noqa: E501

    def __init__(self,   # noqa: C901
                 **kwargs  # type: dict[str, Any]
                 ):   # noqa: C901
        # CXXCBC-391: Adds support for ExtSDKIntegration which removes kv_timeout, the cluster kv_durable
        # timeout is used internally
        if 'kv_timeout' in kwargs:
            kwargs.pop('kv_timeout')
            Supportability.option_deprecated('kv_timeout')
        kwargs = {k: v for k, v in kwargs.items() if k in TransactionConfig._TXN_ALLOWED_KEYS}
        # convert everything here...
        durability = kwargs.pop("durability", None)
        if durability:
            kwargs["durability_level"] = durability.level.value
        if kwargs.get('cleanup_window', None):
            kwargs['cleanup_window'] = int(kwargs['cleanup_window'].total_seconds() * 1000000)
        coll = kwargs.pop("metadata_collection", None)
        # CXXCBC-391: Adds support for ExtSDKIntegration which changes expiration_time -> timeout
        if 'expiration_time' in kwargs or 'timeout' in kwargs:
            Supportability.option_deprecated('expiration_time', 'timeout')
            timeout = kwargs.pop('expiration_time', None)
            # if timeout is also in the options, override expiration_time
            if 'timeout' in kwargs:
                timeout = kwargs.get('timeout', None)
            if timeout:
                kwargs['timeout'] = int(timeout.total_seconds() * 1000000)
        if coll:
            kwargs["metadata_bucket"] = coll.bucket
            kwargs["metadata_scope"] = coll.scope
            kwargs["metadata_collection"] = coll.collection
        # don't pass None
        if kwargs.get('scan_consistency', None):
            kwargs['scan_consistency'] = kwargs['scan_consistency'].value
            if kwargs["scan_consistency"] == "at_plus":
                raise InvalidArgumentException("QueryScanConsistency.AT_PLUS not valid for transactions")
        for key in [k for k, v in kwargs.items() if v is None]:
            del (kwargs[key])
        self._base = transaction_config(**kwargs)


class TransactionOptions:
    _TXN_ALLOWED_KEYS = {"durability", "timeout", "expiration_time", "scan_consistency", "metadata_collection"}

    @overload
    def __init__(self,
                 durability=None,   # type: Optional[ServerDurability]
                 kv_timeout=None,  # type: Optional[timedelta]
                 expiration_time=None,  # type: Optional[timedelta]
                 scan_consistency=None,  # type: Optional[QueryScanConsistency]
                 metadata_collection=None,  # type: Optional[Collection]
                 timeout=None,  # type: Optional[timedelta]
                 ):
        """
        Overrides a subset of the ``TransactionConfig`` parameters for a single query.
        Args:
            durability (:class:`ServerDurability`, optional): Desired durability level for all operations
                in this transaction.
            kv_timeout: (timedelta, optional): **DEPRECATED** Currently a no-op.  KV timeout to use for this transaction.
            expiration_time: (timedelta, optional): **DEPRECATED** Use timeout instead. Expiry for this transaction.
            scan_consistency: (:class:`QueryScanConsistency`, optional): Scan consistency for queries in
              this transaction.
            metadata_collection: (:class: `couchbase.collection.Collection, optional): This transaction will
              put all metadata in the specified bucket/scope/collection.
            timeout: (timedelta, optional): Expiry for this transaction.
        """  # noqa: E501

    def __init__(self,
                 **kwargs  # type: Dict[str, Any]
                 ):
        # CXXCBC-391: Adds support for ExtSDKIntegration which removes kv_timeout, the cluster kv_durable
        # timeout is used internally
        if 'kv_timeout' in kwargs:
            kwargs.pop('kv_timeout')
            Supportability.option_deprecated('kv_timeout')
        kwargs = {k: v for k, v in kwargs.items() if k in TransactionOptions._TXN_ALLOWED_KEYS}
        # convert everything here...
        durability = kwargs.pop("durability", None)
        if durability:
            kwargs["durability_level"] = durability.level.value
        # CXXCBC-391: Adds support for ExtSDKIntegration which changes expiration_time -> timeout
        if 'expiration_time' in kwargs or 'timeout' in kwargs:
            Supportability.option_deprecated('expiration_time', 'timeout')
            timeout = kwargs.pop('expiration_time', None)
            # if timeout is also in the options, override expiration_time
            if 'timeout' in kwargs:
                timeout = kwargs.get('timeout', None)
            if timeout:
                kwargs['timeout'] = int(timeout.total_seconds() * 1000000)
        if kwargs.get('scan_consistency', None):
            kwargs['scan_consistency'] = kwargs['scan_consistency'].value
            if kwargs["scan_consistency"] == "at_plus":
                raise InvalidArgumentException("QueryScanConsistency.AT_PLUS not valid for transactions")
        coll = kwargs.pop('metadata_collection', None)
        if coll:
            kwargs['metadata_bucket'] = coll.bucket
            kwargs['metadata_scope'] = coll.scope
            kwargs['metadata_collection'] = coll.collection
        # don't pass None
        for key in [k for k, v in kwargs.items() if v is None]:
            del (kwargs[key])
        self._base = transaction_options(**kwargs)

    def __str__(self):
        return f'TransactionOptions(base_:{self._base}'


# @TODO:  lets replace this....

OptionsBase = dict
T = TypeVar("T", bound=OptionsBase)


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
            "report_id": {"report_id": lambda x: str(x)},
            "batch_byte_limit": {"batch_byte_limit": validate_int},
            "batch_item_limit": {"batch_item_limit": validate_int},
            "concurrency": {"concurrency": validate_int}
        }


forward_args = DefaultForwarder().forward_args


class TransactionQueryOptions:
    ALLOWED_KEYS = {"raw", "adhoc", "scan_consistency", "profile", "client_context_id",
                    "scan_wait", "read_only", "scan_cap", "pipeline_batch", "pipeline_cap",
                    "scope", "metrics", "max_parallelism", "positional_parameters", "named_parameters"}

    @overload
    def __init__(self,
                 raw=None,  # type: Optional[Dict[str, JSONType]]
                 adhoc=None,  # type: Optional[bool]
                 scan_consistency=None,  # type: Optional[QueryScanConsistency]
                 profile=None,  # type: Optional[Any]
                 client_context_id=None,  # type: Optional[str]
                 scan_wait=None,  # type: Optional[timedelta]
                 read_only=None,  # type: Optional[bool]
                 scan_cap=None,  # type: Optional[int]
                 pipeline_batch=None,  # type: Optional[int]
                 pipeline_cap=None,  # type: Optional[int]
                 positional_parameters=None,  # type: Optional[Iterable[JSONType]]
                 named_parameters=None,  # type: Optional[Dict[str, JSONType]]
                 scope=None,  # type: Optional[Any]
                 metrics=None,  # type: Optional[bool]
                 max_parallelism=None  # type: Optional[int]
                 ):
        """
        QueryOptions for transactions.

        Args:
            raw (Dict[str, Any], optional): Specifies any additional parameters which should be passed to the query
                engine when executing the query. Defaults to None.
            adhoc (bool, optional): Specifies whether this is an ad-hoc query, or if it should be prepared for
                faster execution in the future. Defaults to True.
            scan_consistency (:class:`~couchbase.analytics.AnalyticsScanConsistency`, optional): Specifies
                the consistency requirements when executing the transactional query.
            profile (:class:`~couchbase.n1ql.QueryProfile`, optional): Specifies the level of profiling that should
                be used for the transactional query. Defaults to `Off`.
            client_context_id (str, optional): Specifies an client id for this query.  This is returned with the
                response, and can be helpful when debugging.
            scan_cap (int, optional):  This is an advanced option, see the query service reference for more
                information on the proper use and tuning of this option. Defaults to None.
            scan_wait (timedelta, optional):  This is an advanced option, see the query service reference for more
                information on the proper use and tuning of this option. Defaults to None.
            metrics (bool, optional): Specifies whether metrics should be captured as part of the execution of the
                query. Defaults to False.
            read_only: (bool, optional): Specifies that the query should be considered read-only, and not allowed to
                mutate documents on the server-side.  See query service reference for more details.
            pipeline_batch (int, optional): This is an advanced option, see the query service reference for more
                information on the proper use and tuning of this option. Defaults to None.
            pipeline_cap (int, optional):  This is an advanced option, see the query service reference for more
                information on the proper use and tuning of this option. Defaults to None.
            positional_parameters (Iterable[JSONType], optional): Positional values to be used for the placeholders
                within the query. Defaults to None.
            named_parameters (Dict[str, JSONType], optional): Named values to be used for the placeholders
                within the query. Defaults to None.
            scope (Union[:class:`~acouchbase.scope.Scope`,:class:`~couchbase.scope.Scope`], optional): Specify the
                scope of the query. Defaults to None.
            max_parallelism (int, optional): This is an advanced option, see the query service reference for more
                information on the proper use and tuning of this option. Defaults to None.
        """
        pass

    def __init__(self,   # noqa: C901
                 **kwargs  # type: Dict[str, JSONType]
                 ):
        kwargs = {k: v for k, v in kwargs.items() if k in TransactionQueryOptions.ALLOWED_KEYS}
        # TODO: mapping similar to the options elsewhere.
        scope = kwargs.pop("scope", None)
        if scope:
            kwargs["bucket_name"] = scope.bucket_name
            kwargs["scope_name"] = scope.name
        if kwargs.get("scan_wait", None):
            kwargs["scan_wait"] = kwargs["scan_wait"].total_seconds/1000
        if kwargs.get("scan_consistency", None):
            kwargs["scan_consistency"] = kwargs["scan_consistency"].value
            if kwargs["scan_consistency"] == "at_plus":
                raise InvalidArgumentException("QueryScanConsistency.AT_PLUS not valid for transactions")
        raw = kwargs.pop('raw', None)
        if raw:
            kwargs['raw'] = dict()
            for k, v in raw.items():
                kwargs['raw'][k] = DefaultJsonSerializer().serialize(v)
        adhoc = kwargs.pop("adhoc", None)
        if adhoc is not None:
            kwargs["adhoc"] = adhoc
        readonly = kwargs.pop("read_only", None)
        if readonly is not None:
            kwargs["readonly"] = readonly
        profile = kwargs.pop("profile", None)
        if profile:
            kwargs["profile_mode"] = profile.value
        positional = kwargs.pop("positional_parameters", None)
        if positional:
            kwargs["positional_parameters"] = list(
                map(lambda param: DefaultJsonSerializer().serialize(param), positional))
        named = kwargs.pop("named_parameters", None)
        if named:
            kwargs["named_parameters"] = {key: DefaultJsonSerializer().serialize(val) for key, val in named.items()}

        self._base = transaction_query_options(query_args=kwargs)

    def split_scope_qualifier(self,
                              remove_backticks=True  # type: Optional[bool]
                              ) -> Optional[Tuple[str, str]]:
        scope_qualifier = self._base.to_dict().get('scope_qualifier', None)
        if not scope_qualifier:
            return None

        # expected format namespace:`bucket`:`scope`
        namespace_tokens = scope_qualifier.split(':')
        if len(namespace_tokens) != 2:
            return None

        bucket_tokens = namespace_tokens[1].split('.')
        if len(bucket_tokens) == 2:
            if remove_backticks:
                return bucket_tokens[0].replace('`', ''), bucket_tokens[1].replace('`', '')
            return bucket_tokens[0], bucket_tokens[1]

        return None


class TransactionGetOptions(dict):
    """Available options to for transaction get operation.

    Args:
        transcoder (:class:`~.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """  # noqa: E501

    @overload
    def __init__(self,
                 transcoder=None  # type: Optional[Transcoder]
                 ):
        ...

    def __init__(self, **kwargs):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class TransactionInsertOptions(dict):
    """Available options to for transaction insert operation.

    Args:
        transcoder (:class:`~.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """  # noqa: E501

    @overload
    def __init__(self,
                 transcoder=None  # type: Optional[Transcoder]
                 ):
        ...

    def __init__(self, **kwargs):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class TransactionReplaceOptions(dict):
    """Available options to for transaction replace operation.

    Args:
        transcoder (:class:`~.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """  # noqa: E501

    @overload
    def __init__(self,
                 transcoder=None  # type: Optional[Transcoder]
                 ):
        ...

    def __init__(self, **kwargs):

        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)
