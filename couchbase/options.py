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

"""Public import path for `couchbase.options`.

The declarations live in `couchbase.common.options`, which is shared by every
protocol.  They are re-exported here so this import path keeps working.
"""

__all__ = [
    'AcceptableInts',
    'AnalyticsOptions',
    'AppendMultiOptions',
    'AppendOptions',
    'CONFIG_PROFILES',
    'ClusterMetricsOptions',
    'ClusterOptions',
    'ClusterOrphanReportingOptions',
    'ClusterTimeoutOptions',
    'ClusterTracingOptions',
    'Compression',
    'ConfigProfile',
    'ConfigProfiles',
    'ConstrainedInt',
    'DecrementMultiOptions',
    'DecrementOptions',
    'DeltaValue',
    'DiagnosticsOptions',
    'ExistsMultiOptions',
    'ExistsOptions',
    'GetAllReplicasMultiOptions',
    'GetAllReplicasOptions',
    'GetAndLockMultiOptions',
    'GetAndLockOptions',
    'GetAndTouchOptions',
    'GetAnyReplicaMultiOptions',
    'GetAnyReplicaOptions',
    'GetMultiOptions',
    'GetOptions',
    'IncrementMultiOptions',
    'IncrementOptions',
    'InsertMultiOptions',
    'InsertOptions',
    'IpProtocol',
    'KnownConfigProfiles',
    'LockMode',
    'LockMultiOptions',
    'LookupInAllReplicasOptions',
    'LookupInAnyReplicaOptions',
    'LookupInOptions',
    'MutateInOptions',
    'MutationMultiOptions',
    'MutationOptions',
    'NoValueMultiOptions',
    'OptionValidationMode',
    'PingOptions',
    'PrependMultiOptions',
    'PrependOptions',
    'QueryOptions',
    'RemoveMultiOptions',
    'RemoveOptions',
    'ReplaceMultiOptions',
    'ReplaceOptions',
    'ScanOptions',
    'SearchOptions',
    'SignedInt64',
    'TLSVerifyMode',
    'TouchMultiOptions',
    'TouchOptions',
    'TransactionConfig',
    'TransactionGetMultiOptions',
    'TransactionGetMultiReplicasFromPreferredServerGroupOptions',
    'TransactionGetOptions',
    'TransactionGetReplicaFromPreferredServerGroupOptions',
    'TransactionInsertOptions',
    'TransactionOptions',
    'TransactionQueryOptions',
    'TransactionReplaceOptions',
    'UnlockMultiOptions',
    'UnlockOptions',
    'UnsignedInt32',
    'UnsignedInt64',
    'UpsertMultiOptions',
    'UpsertOptions',
    'VectorSearchOptions',
    'ViewOptions',
    'WaitUntilReadyOptions',
    'WanDevelopmentProfile',
]

from typing import TYPE_CHECKING

from couchbase.common._supportability import Supportability as _Supportability
from couchbase.common.config_profiles import CONFIG_PROFILES  # noqa: F401
from couchbase.common.config_profiles import ConfigProfile  # noqa: F401
from couchbase.common.config_profiles import ConfigProfiles  # noqa: F401
from couchbase.common.config_profiles import KnownConfigProfiles  # noqa: F401
from couchbase.common.config_profiles import WanDevelopmentProfile  # noqa: F401
from couchbase.common.option_enums import Compression  # noqa: F401
from couchbase.common.option_enums import IpProtocol  # noqa: F401
from couchbase.common.option_enums import LockMode  # noqa: F401
from couchbase.common.option_enums import OptionValidationMode  # noqa: F401
from couchbase.common.option_enums import TLSVerifyMode  # noqa: F401
from couchbase.common.options import AcceptableInts  # noqa: F401
from couchbase.common.options import AnalyticsOptions  # noqa: F401
from couchbase.common.options import AppendMultiOptions  # noqa: F401
from couchbase.common.options import AppendOptions  # noqa: F401
from couchbase.common.options import ClusterMetricsOptions  # noqa: F401
from couchbase.common.options import ClusterOptions  # noqa: F401
from couchbase.common.options import ClusterOrphanReportingOptions  # noqa: F401
from couchbase.common.options import ClusterTimeoutOptions  # noqa: F401
from couchbase.common.options import ClusterTracingOptions  # noqa: F401
from couchbase.common.options import ConstrainedInt  # noqa: F401
from couchbase.common.options import DecrementMultiOptions  # noqa: F401
from couchbase.common.options import DecrementOptions  # noqa: F401
from couchbase.common.options import DeltaValue  # noqa: F401
from couchbase.common.options import DiagnosticsOptions  # noqa: F401
from couchbase.common.options import ExistsMultiOptions  # noqa: F401
from couchbase.common.options import ExistsOptions  # noqa: F401
from couchbase.common.options import GetAllReplicasMultiOptions  # noqa: F401
from couchbase.common.options import GetAllReplicasOptions  # noqa: F401
from couchbase.common.options import GetAndLockMultiOptions  # noqa: F401
from couchbase.common.options import GetAndLockOptions  # noqa: F401
from couchbase.common.options import GetAndTouchOptions  # noqa: F401
from couchbase.common.options import GetAnyReplicaMultiOptions  # noqa: F401
from couchbase.common.options import GetAnyReplicaOptions  # noqa: F401
from couchbase.common.options import GetMultiOptions  # noqa: F401
from couchbase.common.options import GetOptions  # noqa: F401
from couchbase.common.options import IncrementMultiOptions  # noqa: F401
from couchbase.common.options import IncrementOptions  # noqa: F401
from couchbase.common.options import InsertMultiOptions  # noqa: F401
from couchbase.common.options import InsertOptions  # noqa: F401
from couchbase.common.options import LockMultiOptions  # noqa: F401
from couchbase.common.options import LookupInAllReplicasOptions  # noqa: F401
from couchbase.common.options import LookupInAnyReplicaOptions  # noqa: F401
from couchbase.common.options import LookupInOptions  # noqa: F401
from couchbase.common.options import MutateInOptions  # noqa: F401
from couchbase.common.options import MutationMultiOptions  # noqa: F401
from couchbase.common.options import MutationOptions  # noqa: F401
from couchbase.common.options import NoValueMultiOptions  # noqa: F401
from couchbase.common.options import PingOptions  # noqa: F401
from couchbase.common.options import PrependMultiOptions  # noqa: F401
from couchbase.common.options import PrependOptions  # noqa: F401
from couchbase.common.options import QueryOptions  # noqa: F401
from couchbase.common.options import RemoveMultiOptions  # noqa: F401
from couchbase.common.options import RemoveOptions  # noqa: F401
from couchbase.common.options import ReplaceMultiOptions  # noqa: F401
from couchbase.common.options import ReplaceOptions  # noqa: F401
from couchbase.common.options import ScanOptions  # noqa: F401
from couchbase.common.options import SearchOptions  # noqa: F401
from couchbase.common.options import SignedInt64  # noqa: F401
from couchbase.common.options import TouchMultiOptions  # noqa: F401
from couchbase.common.options import TouchOptions  # noqa: F401
from couchbase.common.options import TransactionConfig  # noqa: F401
from couchbase.common.options import TransactionGetMultiOptions  # noqa: F401
from couchbase.common.options import TransactionGetMultiReplicasFromPreferredServerGroupOptions  # noqa: F401
from couchbase.common.options import TransactionGetOptions  # noqa: F401
from couchbase.common.options import TransactionGetReplicaFromPreferredServerGroupOptions  # noqa: F401
from couchbase.common.options import TransactionInsertOptions  # noqa: F401
from couchbase.common.options import TransactionOptions  # noqa: F401
from couchbase.common.options import TransactionQueryOptions  # noqa: F401
from couchbase.common.options import TransactionReplaceOptions  # noqa: F401
from couchbase.common.options import UnlockMultiOptions  # noqa: F401
from couchbase.common.options import UnlockOptions  # noqa: F401
from couchbase.common.options import UnsignedInt32  # noqa: F401
from couchbase.common.options import UnsignedInt64  # noqa: F401
from couchbase.common.options import UpsertMultiOptions  # noqa: F401
from couchbase.common.options import UpsertOptions  # noqa: F401
from couchbase.common.options import VectorSearchOptions  # noqa: F401
from couchbase.common.options import ViewOptions  # noqa: F401
from couchbase.common.options import WaitUntilReadyOptions  # noqa: F401


# Bases no operation takes or returns, kept so that importing them keeps working.
@_Supportability.class_deprecated("each operation's own options class")
class DurabilityOptionBlock(dict):
    pass


@_Supportability.class_deprecated("each operation's own options class")
class OptionsTimeout(dict):
    pass


if not TYPE_CHECKING:
    __getattr__ = _Supportability.deprecated_paths(__name__)
