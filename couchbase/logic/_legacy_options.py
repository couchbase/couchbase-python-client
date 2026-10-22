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


"""BRIDGE: the option forwarding and the transaction options the request builders not yet ported still
read.

This module exists only for the bridge.  Removed by PYCBC-2007.
"""

from __future__ import annotations

import copy
from abc import ABCMeta, abstractmethod
from datetime import timedelta
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    Optional,
                    Tuple,
                    TypeVar,
                    overload)

import couchbase.common.options as _options
from couchbase._utils import (timedelta_as_milliseconds,
                              timedelta_as_timestamp,
                              validate_int)
from couchbase.common._durability import DurabilityParser
from couchbase.common._supportability import Supportability
from couchbase.exceptions import InvalidArgumentException
from couchbase.protocol.classic._pycbc_core import (transaction_config,
                                                    transaction_options,
                                                    transaction_query_options)
from couchbase.serializer import DefaultJsonSerializer

if TYPE_CHECKING:
    from couchbase._utils import JSONType
    from couchbase.collection import Collection
    from couchbase.durability import ServerDurability
    from couchbase.n1ql import QueryScanConsistency
    from couchbase.transactions import (TransactionGetMultiMode,
                                        TransactionGetMultiReplicasFromPreferredServerGroupMode,
                                        TransactionKeyspace)
    from couchbase.transcoder import Transcoder


class TransactionConfig(_options.TransactionConfig):
    _TXN_ALLOWED_KEYS = {"durability", "cleanup_window", "timeout",
                         "expiration_time", "cleanup_lost_attempts", "cleanup_client_attempts",
                         "metadata_collection", "scan_consistency"}

    # Typed as the final class, which is a dict; this one carries its state elsewhere and is always set.
    def __bool__(self) -> bool:
        return True

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
                 **kwargs  # type: Any
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
            kwargs['cleanup_window'] = int(kwargs['cleanup_window'].total_seconds() * 1e6)
        coll = kwargs.pop("metadata_collection", None)
        # CXXCBC-391: Adds support for ExtSDKIntegration which changes expiration_time -> timeout
        if 'expiration_time' in kwargs or 'timeout' in kwargs:
            Supportability.option_deprecated('expiration_time', 'timeout')
            timeout = kwargs.pop('expiration_time', None)
            # if timeout is also in the options, override expiration_time
            if 'timeout' in kwargs:
                timeout = kwargs.get('timeout', None)
            if timeout:
                kwargs['timeout'] = int(timeout.total_seconds() * 1e6)
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


class TransactionOptions(_options.TransactionOptions):
    _TXN_ALLOWED_KEYS = {"durability", "timeout", "expiration_time", "scan_consistency", "metadata_collection"}

    # Typed as the final class, which is a dict; this one carries its state elsewhere and is always set.
    def __bool__(self) -> bool:
        return True

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
                 **kwargs  # type: Any
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
                kwargs['timeout'] = int(timeout.total_seconds() * 1e6)
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
        return f'TransactionOptions(base_: {self._base})'


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
            "timeout": {"timeout": timedelta_as_milliseconds},
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
            "concurrency": {"concurrency": validate_int},
            "read_preference": {"read_preference": lambda r: r.value}
        }


forward_args = DefaultForwarder().forward_args


class TransactionQueryOptions(_options.TransactionQueryOptions):
    ALLOWED_KEYS = {"raw", "adhoc", "scan_consistency", "profile", "client_context_id",
                    "scan_wait", "read_only", "scan_cap", "pipeline_batch", "pipeline_cap",
                    "scope", "metrics", "max_parallelism", "positional_parameters", "named_parameters"}

    # Typed as the final class, which is a dict; this one carries its state elsewhere and is always set.
    def __bool__(self) -> bool:
        return True

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
                 **kwargs  # type: JSONType
                 ):
        kwargs = {k: v for k, v in kwargs.items() if k in TransactionQueryOptions.ALLOWED_KEYS}
        # TODO: mapping similar to the options elsewhere.
        scope = kwargs.pop("scope", None)
        if scope:
            kwargs["bucket_name"] = scope.bucket_name
            kwargs["scope_name"] = scope.name
        if kwargs.get("scan_wait", None):
            kwargs["scan_wait"] = timedelta_as_milliseconds(kwargs["scan_wait"])
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
            kwargs["profile"] = profile.value
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


class TransactionGetOptions(_options.TransactionGetOptions):
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


class TransactionGetMultiOptions(_options.TransactionGetMultiOptions):
    """Available options to for transaction get_multi operation.

    Args:
        mode (:class:`~.transactions.TransactionGetMultiMode`, optional): Specifies the mode
            to use for this specific operation. Defaults to None.
    """  # noqa: E501

    @overload
    def __init__(self,
                 mode=None  # type: Optional[TransactionGetMultiMode]
                 ):
        ...

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


_FinalGetReplicaOpts = _options.TransactionGetReplicaFromPreferredServerGroupOptions


class TransactionGetReplicaFromPreferredServerGroupOptions(_FinalGetReplicaOpts):
    """Available options to for transaction get_replica_from_preferred_server_group operation.

    Args:
        transcoder (:class:`~.transcoder.Transcoder`, optional): Specifies an explicit transcoder
            to use for this specific operation. Defaults to :class:`~.transcoder.JsonTranscoder`.
    """

    @overload
    def __init__(self,
                 transcoder=None  # type: Optional[Transcoder]
                 ):
        ...

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


_FinalGetMultiReplicasOpts = _options.TransactionGetMultiReplicasFromPreferredServerGroupOptions


class TransactionGetMultiReplicasFromPreferredServerGroupOptions(_FinalGetMultiReplicasOpts):
    """Available options to for transaction get_multi_replicas_from_preferred_server_group operation.

    Args:
        mode (:class:`~.transactions.TransactionGetMultiReplicasFromPreferredServerGroupMode`, optional): Specifies the mode
            to use for this specific operation. Defaults to None.
    """  # noqa: E501

    @overload
    def __init__(self,
                 mode=None  # type: Optional[TransactionGetMultiReplicasFromPreferredServerGroupMode]
                 ):
        ...

    def __init__(self, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        super().__init__(**kwargs)


class TransactionInsertOptions(_options.TransactionInsertOptions):
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


class TransactionReplaceOptions(_options.TransactionReplaceOptions):
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
