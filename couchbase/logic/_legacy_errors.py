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

"""BRIDGE: the error-code table the logic package still reads.

This module exists only for the bridge.  Removed by PYCBC-2016.
"""

from enum import Enum

import couchbase.common.exceptions as _exceptions


class ExceptionMap(Enum):
    RequestCanceledException = 2
    InvalidArgumentException = 3
    ServiceUnavailableException = 4
    InternalServerFailureException = 5
    AuthenticationException = 6
    TemporaryFailException = 7
    ParsingFailedException = 8
    CasMismatchException = 9
    BucketNotFoundException = 10
    CollectionNotFoundException = 11
    UnsupportedOperation = 12
    AmbiguousTimeoutException = 13
    UnAmbiguousTimeoutException = 14
    FeatureUnavailableException = 15
    ScopeNotFoundException = 16
    QueryIndexNotFoundException = 17
    QueryIndexAlreadyExistsException = 18
    RateLimitedException = 21
    QuotaLimitedException = 22
    DocumentNotFoundException = 101
    DocumentUnretrievableException = 102
    DocumentLockedException = 103
    DocumentExistsException = 105
    DurabilityInvalidLevelException = 107
    DurabilityImpossibleException = 108
    DurabilitySyncWriteAmbiguousException = 109
    DurabilitySyncWriteInProgressException = 110
    PathNotFoundException = 113
    PathMismatchException = 114
    PathInvalidException = 115
    PathTooBigException = 116
    PathTooDeepException = 117
    ValueTooDeepException = 118
    InvalidValueException = 119
    DocumentNotJsonException = 120
    NumberTooBigException = 121
    DeltaInvalidException = 122
    PathExistsException = 123
    DocumentNotLockedException = 131
    MutationTokenOutdatedException = 133
    RangeScanCompletedException = 134
    DatasetNotFoundException = 303
    DataverseNotFoundException = 304
    DatasetAlreadyExistsException = 305
    DataverseAlreadyExistsException = 306
    DesignDocumentNotFoundException = 502
    InternalSDKException = 5000
    HTTPException = 5001
    UnsuccessfulOperationException = 5002


PYCBC_ERROR_MAP = {e.value: getattr(_exceptions, e.name) for e in ExceptionMap}
