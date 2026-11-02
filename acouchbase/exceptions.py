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

"""The `acouchbase.exceptions` import path.

The same objects as `couchbase.exceptions`, re-exported so asyncio code imports them from its own
package.
"""

__all__ = [
    'AlreadyQueriedException',
    'AmbiguousTimeoutException',
    'AnalyticsErrorContext',
    'AnalyticsLinkExistsException',
    'AnalyticsLinkNotFoundException',
    'AuthenticationException',
    'BucketAlreadyExistsException',
    'BucketDoesNotExistException',
    'BucketNotFlushableException',
    'BucketNotFoundException',
    'CASMismatchException',
    'CannotReviveLivingDocumentException',
    'CasMismatchException',
    'CollectionAlreadyExistsException',
    'CollectionNotFoundException',
    'CouchbaseErrorCategory',
    'CouchbaseException',
    'CryptoException',
    'CryptoKeyNotFoundException',
    'DatasetAlreadyExistsException',
    'DatasetNotFoundException',
    'DataverseAlreadyExistsException',
    'DataverseNotFoundException',
    'DecrypterAlreadyExistsException',
    'DecrypterNotFoundException',
    'DecryptionFailureException',
    'DeltaInvalidException',
    'DesignDocumentNotFoundException',
    'DocumentExistsException',
    'DocumentLockedException',
    'DocumentNotFoundException',
    'DocumentNotJsonException',
    'DocumentNotLockedException',
    'DocumentUnretrievableException',
    'DurabilityImpossibleException',
    'DurabilityInvalidLevelException',
    'DurabilitySyncWriteAmbiguousException',
    'DurabilitySyncWriteInProgressException',
    'DurableWriteReCommitInProgressException',
    'EncrypterAlreadyExistsException',
    'EncrypterNotFoundException',
    'EncryptionFailureException',
    'ErrorContext',
    'ErrorContextType',
    'EventingFunctionAlreadyDeployedException',
    'EventingFunctionCollectionNotFoundException',
    'EventingFunctionCompilationFailureException',
    'EventingFunctionIdenticalKeyspaceException',
    'EventingFunctionNotBootstrappedException',
    'EventingFunctionNotDeployedException',
    'EventingFunctionNotFoundException',
    'EventingFunctionNotUnDeployedException',
    'FeatureNotFoundException',
    'FeatureUnavailableException',
    'GenericErrorContext',
    'GroupNotFoundException',
    'HTTPErrorContext',
    'HTTPException',
    'InternalSDKException',
    'InternalServerFailureException',
    'InvalidArgumentException',
    'InvalidCipherTextException',
    'InvalidCryptoKeyException',
    'InvalidIndexException',
    'InvalidValueException',
    'KeyValueErrorContext',
    'KeyspaceNotFoundException',
    'ManagementErrorContext',
    'MissingConnectionException',
    'MissingTokenException',
    'MutationTokenOutdatedException',
    'NoChildrenException',
    'NumberTooBigException',
    'ParsingFailedException',
    'PathExistsException',
    'PathInvalidException',
    'PathMismatchException',
    'PathNotFoundException',
    'PathTooBigException',
    'PathTooDeepException',
    'QueryErrorContext',
    'QueryIndexAlreadyExistsException',
    'QueryIndexNotFoundException',
    'QueueEmpty',
    'QuotaLimitedException',
    'RangeScanCompletedException',
    'RateLimitedException',
    'RequestCanceledException',
    'ScopeAlreadyExistsException',
    'ScopeNotFoundException',
    'SearchErrorContext',
    'SearchIndexNotFoundException',
    'ServiceUnavailableException',
    'SubdocCantInsertValueException',
    'SubdocPathMismatchException',
    'SubdocumentErrorContext',
    'TemporaryFailException',
    'TimeoutException',
    'TransactionCommitAmbiguous',
    'TransactionException',
    'TransactionExpired',
    'TransactionFailed',
    'TransactionOperationFailed',
    'TransactionsErrorContext',
    'UnAmbiguousTimeoutException',
    'UnsuccessfulOperationException',
    'UnsupportedOperation',
    'UserNotFoundException',
    'ValueFormatException',
    'ValueTooDeepException',
    'ValueTooLargeException',
    'ViewErrorContext',
    'WatchQueryIndexTimeoutException',
    'XattrCannotModifyVirtualAttributeException',
    'XattrInvalidKeyComboException',
    'XattrNoAccessException',
    'XattrUnknownMacroException',
    'XattrUnknownVirtualAttributeException',
]

from couchbase.common.exceptions import AlreadyQueriedException  # noqa: F401
from couchbase.common.exceptions import AmbiguousTimeoutException  # noqa: F401
from couchbase.common.exceptions import AnalyticsErrorContext  # noqa: F401
from couchbase.common.exceptions import AnalyticsLinkExistsException  # noqa: F401
from couchbase.common.exceptions import AnalyticsLinkNotFoundException  # noqa: F401
from couchbase.common.exceptions import AuthenticationException  # noqa: F401
from couchbase.common.exceptions import BucketAlreadyExistsException  # noqa: F401
from couchbase.common.exceptions import BucketDoesNotExistException  # noqa: F401
from couchbase.common.exceptions import BucketNotFlushableException  # noqa: F401
from couchbase.common.exceptions import BucketNotFoundException  # noqa: F401
from couchbase.common.exceptions import CannotReviveLivingDocumentException  # noqa: F401
from couchbase.common.exceptions import CASMismatchException  # noqa: F401
from couchbase.common.exceptions import CasMismatchException  # noqa: F401
from couchbase.common.exceptions import CollectionAlreadyExistsException  # noqa: F401
from couchbase.common.exceptions import CollectionNotFoundException  # noqa: F401
from couchbase.common.exceptions import CouchbaseErrorCategory  # noqa: F401
from couchbase.common.exceptions import CouchbaseException  # noqa: F401
from couchbase.common.exceptions import CryptoException  # noqa: F401
from couchbase.common.exceptions import CryptoKeyNotFoundException  # noqa: F401
from couchbase.common.exceptions import DatasetAlreadyExistsException  # noqa: F401
from couchbase.common.exceptions import DatasetNotFoundException  # noqa: F401
from couchbase.common.exceptions import DataverseAlreadyExistsException  # noqa: F401
from couchbase.common.exceptions import DataverseNotFoundException  # noqa: F401
from couchbase.common.exceptions import DecrypterAlreadyExistsException  # noqa: F401
from couchbase.common.exceptions import DecrypterNotFoundException  # noqa: F401
from couchbase.common.exceptions import DecryptionFailureException  # noqa: F401
from couchbase.common.exceptions import DeltaInvalidException  # noqa: F401
from couchbase.common.exceptions import DesignDocumentNotFoundException  # noqa: F401
from couchbase.common.exceptions import DocumentExistsException  # noqa: F401
from couchbase.common.exceptions import DocumentLockedException  # noqa: F401
from couchbase.common.exceptions import DocumentNotFoundException  # noqa: F401
from couchbase.common.exceptions import DocumentNotJsonException  # noqa: F401
from couchbase.common.exceptions import DocumentNotLockedException  # noqa: F401
from couchbase.common.exceptions import DocumentUnretrievableException  # noqa: F401
from couchbase.common.exceptions import DurabilityImpossibleException  # noqa: F401
from couchbase.common.exceptions import DurabilityInvalidLevelException  # noqa: F401
from couchbase.common.exceptions import DurabilitySyncWriteAmbiguousException  # noqa: F401
from couchbase.common.exceptions import DurabilitySyncWriteInProgressException  # noqa: F401
from couchbase.common.exceptions import DurableWriteReCommitInProgressException  # noqa: F401
from couchbase.common.exceptions import EncrypterAlreadyExistsException  # noqa: F401
from couchbase.common.exceptions import EncrypterNotFoundException  # noqa: F401
from couchbase.common.exceptions import EncryptionFailureException  # noqa: F401
from couchbase.common.exceptions import ErrorContext  # noqa: F401
from couchbase.common.exceptions import ErrorContextType  # noqa: F401
from couchbase.common.exceptions import EventingFunctionAlreadyDeployedException  # noqa: F401
from couchbase.common.exceptions import EventingFunctionCollectionNotFoundException  # noqa: F401
from couchbase.common.exceptions import EventingFunctionCompilationFailureException  # noqa: F401
from couchbase.common.exceptions import EventingFunctionIdenticalKeyspaceException  # noqa: F401
from couchbase.common.exceptions import EventingFunctionNotBootstrappedException  # noqa: F401
from couchbase.common.exceptions import EventingFunctionNotDeployedException  # noqa: F401
from couchbase.common.exceptions import EventingFunctionNotFoundException  # noqa: F401
from couchbase.common.exceptions import EventingFunctionNotUnDeployedException  # noqa: F401
from couchbase.common.exceptions import FeatureNotFoundException  # noqa: F401
from couchbase.common.exceptions import FeatureUnavailableException  # noqa: F401
from couchbase.common.exceptions import GenericErrorContext  # noqa: F401
from couchbase.common.exceptions import GroupNotFoundException  # noqa: F401
from couchbase.common.exceptions import HTTPErrorContext  # noqa: F401
from couchbase.common.exceptions import HTTPException  # noqa: F401
from couchbase.common.exceptions import InternalSDKException  # noqa: F401
from couchbase.common.exceptions import InternalServerFailureException  # noqa: F401
from couchbase.common.exceptions import InvalidArgumentException  # noqa: F401
from couchbase.common.exceptions import InvalidCipherTextException  # noqa: F401
from couchbase.common.exceptions import InvalidCryptoKeyException  # noqa: F401
from couchbase.common.exceptions import InvalidIndexException  # noqa: F401
from couchbase.common.exceptions import InvalidValueException  # noqa: F401
from couchbase.common.exceptions import KeyspaceNotFoundException  # noqa: F401
from couchbase.common.exceptions import KeyValueErrorContext  # noqa: F401
from couchbase.common.exceptions import ManagementErrorContext  # noqa: F401
from couchbase.common.exceptions import MissingConnectionException  # noqa: F401
from couchbase.common.exceptions import MissingTokenException  # noqa: F401
from couchbase.common.exceptions import MutationTokenOutdatedException  # noqa: F401
from couchbase.common.exceptions import NoChildrenException  # noqa: F401
from couchbase.common.exceptions import NumberTooBigException  # noqa: F401
from couchbase.common.exceptions import ParsingFailedException  # noqa: F401
from couchbase.common.exceptions import PathExistsException  # noqa: F401
from couchbase.common.exceptions import PathInvalidException  # noqa: F401
from couchbase.common.exceptions import PathMismatchException  # noqa: F401
from couchbase.common.exceptions import PathNotFoundException  # noqa: F401
from couchbase.common.exceptions import PathTooBigException  # noqa: F401
from couchbase.common.exceptions import PathTooDeepException  # noqa: F401
from couchbase.common.exceptions import QueryErrorContext  # noqa: F401
from couchbase.common.exceptions import QueryIndexAlreadyExistsException  # noqa: F401
from couchbase.common.exceptions import QueryIndexNotFoundException  # noqa: F401
from couchbase.common.exceptions import QueueEmpty  # noqa: F401
from couchbase.common.exceptions import QuotaLimitedException  # noqa: F401
from couchbase.common.exceptions import RangeScanCompletedException  # noqa: F401
from couchbase.common.exceptions import RateLimitedException  # noqa: F401
from couchbase.common.exceptions import RequestCanceledException  # noqa: F401
from couchbase.common.exceptions import ScopeAlreadyExistsException  # noqa: F401
from couchbase.common.exceptions import ScopeNotFoundException  # noqa: F401
from couchbase.common.exceptions import SearchErrorContext  # noqa: F401
from couchbase.common.exceptions import SearchIndexNotFoundException  # noqa: F401
from couchbase.common.exceptions import ServiceUnavailableException  # noqa: F401
from couchbase.common.exceptions import SubdocCantInsertValueException  # noqa: F401
from couchbase.common.exceptions import SubdocPathMismatchException  # noqa: F401
from couchbase.common.exceptions import SubdocumentErrorContext  # noqa: F401
from couchbase.common.exceptions import TemporaryFailException  # noqa: F401
from couchbase.common.exceptions import TimeoutException  # noqa: F401
from couchbase.common.exceptions import TransactionCommitAmbiguous  # noqa: F401
from couchbase.common.exceptions import TransactionException  # noqa: F401
from couchbase.common.exceptions import TransactionExpired  # noqa: F401
from couchbase.common.exceptions import TransactionFailed  # noqa: F401
from couchbase.common.exceptions import TransactionOperationFailed  # noqa: F401
from couchbase.common.exceptions import TransactionsErrorContext  # noqa: F401
from couchbase.common.exceptions import UnAmbiguousTimeoutException  # noqa: F401
from couchbase.common.exceptions import UnsuccessfulOperationException  # noqa: F401
from couchbase.common.exceptions import UnsupportedOperation  # noqa: F401
from couchbase.common.exceptions import UserNotFoundException  # noqa: F401
from couchbase.common.exceptions import ValueFormatException  # noqa: F401
from couchbase.common.exceptions import ValueTooDeepException  # noqa: F401
from couchbase.common.exceptions import ValueTooLargeException  # noqa: F401
from couchbase.common.exceptions import ViewErrorContext  # noqa: F401
from couchbase.common.exceptions import WatchQueryIndexTimeoutException  # noqa: F401
from couchbase.common.exceptions import XattrCannotModifyVirtualAttributeException  # noqa: F401
from couchbase.common.exceptions import XattrInvalidKeyComboException  # noqa: F401
from couchbase.common.exceptions import XattrNoAccessException  # noqa: F401
from couchbase.common.exceptions import XattrUnknownMacroException  # noqa: F401
from couchbase.common.exceptions import XattrUnknownVirtualAttributeException  # noqa: F401
