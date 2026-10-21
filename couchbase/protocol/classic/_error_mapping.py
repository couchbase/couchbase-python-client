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
import re
from typing import TYPE_CHECKING

from couchbase.common._utils import is_null_or_empty
from couchbase.common.exceptions import (AmbiguousTimeoutException,
                                         AnalyticsErrorContext,
                                         AuthenticationException,
                                         BucketNotFoundException,
                                         CannotReviveLivingDocumentException,
                                         CasMismatchException,
                                         CollectionNotFoundException,
                                         CouchbaseException,
                                         DatasetAlreadyExistsException,
                                         DatasetNotFoundException,
                                         DataverseAlreadyExistsException,
                                         DataverseNotFoundException,
                                         DeltaInvalidException,
                                         DesignDocumentNotFoundException,
                                         DocumentExistsException,
                                         DocumentLockedException,
                                         DocumentNotFoundException,
                                         DocumentNotJsonException,
                                         DocumentNotLockedException,
                                         DocumentUnretrievableException,
                                         DurabilityImpossibleException,
                                         DurabilityInvalidLevelException,
                                         DurabilitySyncWriteAmbiguousException,
                                         DurabilitySyncWriteInProgressException,
                                         DurableWriteReCommitInProgressException,
                                         ErrorContext,
                                         FeatureUnavailableException,
                                         HTTPErrorContext,
                                         HTTPException,
                                         InternalSDKException,
                                         InternalServerFailureException,
                                         InvalidArgumentException,
                                         InvalidValueException,
                                         KeyspaceNotFoundException,
                                         KeyValueErrorContext,
                                         MutationTokenOutdatedException,
                                         NumberTooBigException,
                                         ParsingFailedException,
                                         PathExistsException,
                                         PathInvalidException,
                                         PathMismatchException,
                                         PathNotFoundException,
                                         PathTooBigException,
                                         PathTooDeepException,
                                         QueryErrorContext,
                                         QueryIndexAlreadyExistsException,
                                         QueryIndexNotFoundException,
                                         QuotaLimitedException,
                                         RangeScanCompletedException,
                                         RateLimitedException,
                                         RequestCanceledException,
                                         ScopeNotFoundException,
                                         SearchErrorContext,
                                         ServiceUnavailableException,
                                         SubdocumentErrorContext,
                                         TemporaryFailException,
                                         UnAmbiguousTimeoutException,
                                         UnsuccessfulOperationException,
                                         UnsupportedOperation,
                                         ValueTooDeepException,
                                         ValueTooLargeException,
                                         ViewErrorContext,
                                         XattrCannotModifyVirtualAttributeException,
                                         XattrInvalidKeyComboException,
                                         XattrNoAccessException,
                                         XattrUnknownMacroException,
                                         XattrUnknownVirtualAttributeException)

if TYPE_CHECKING:
    from couchbase.common.exceptions import _ExceptionInfo
    from couchbase.protocol.classic._pycbc_core import pycbc_exception

ERROR_MAP: dict[int, type[CouchbaseException]] = {
    2: RequestCanceledException,
    3: InvalidArgumentException,
    4: ServiceUnavailableException,
    5: InternalServerFailureException,
    6: AuthenticationException,
    7: TemporaryFailException,
    8: ParsingFailedException,
    9: CasMismatchException,
    10: BucketNotFoundException,
    11: CollectionNotFoundException,
    12: UnsupportedOperation,
    13: AmbiguousTimeoutException,
    14: UnAmbiguousTimeoutException,
    15: FeatureUnavailableException,
    16: ScopeNotFoundException,
    17: QueryIndexNotFoundException,
    18: QueryIndexAlreadyExistsException,
    21: RateLimitedException,
    22: QuotaLimitedException,
    101: DocumentNotFoundException,
    102: DocumentUnretrievableException,
    103: DocumentLockedException,
    104: ValueTooLargeException,
    105: DocumentExistsException,
    107: DurabilityInvalidLevelException,
    108: DurabilityImpossibleException,
    109: DurabilitySyncWriteAmbiguousException,
    110: DurabilitySyncWriteInProgressException,
    111: DurableWriteReCommitInProgressException,
    113: PathNotFoundException,
    114: PathMismatchException,
    115: PathInvalidException,
    116: PathTooBigException,
    117: PathTooDeepException,
    118: ValueTooDeepException,
    119: InvalidValueException,
    120: DocumentNotJsonException,
    121: NumberTooBigException,
    122: DeltaInvalidException,
    123: PathExistsException,
    124: XattrUnknownMacroException,
    126: XattrInvalidKeyComboException,
    127: XattrUnknownVirtualAttributeException,
    128: XattrCannotModifyVirtualAttributeException,
    130: XattrNoAccessException,
    131: DocumentNotLockedException,
    132: CannotReviveLivingDocumentException,
    133: MutationTokenOutdatedException,
    134: RangeScanCompletedException,
    303: DatasetNotFoundException,
    304: DataverseNotFoundException,
    305: DatasetAlreadyExistsException,
    306: DataverseAlreadyExistsException,
    502: DesignDocumentNotFoundException,
    5000: InternalSDKException,
    5001: HTTPException,
    5002: UnsuccessfulOperationException,
}

# Keyed by the context_type the core writes into each error context it builds (error_contexts.hxx).
CONTEXT_TYPES: dict[str, type[ErrorContext]] = {
    'AnalyticsErrorContext': AnalyticsErrorContext,
    'HTTPErrorContext': HTTPErrorContext,
    'KeyValueErrorContext': KeyValueErrorContext,
    'QueryErrorContext': QueryErrorContext,
    'SearchErrorContext': SearchErrorContext,
    'SubdocumentErrorContext': SubdocumentErrorContext,
    'ViewErrorContext': ViewErrorContext,
}

KV_ERROR_CONTEXT_MAPPING: dict[str, type[CouchbaseException]] = {
    'key_value_locked': DocumentLockedException,
    'key_value_temporary_failure': TemporaryFailException,
}

QUERY_ERROR_MAPPING: dict[str, type[CouchbaseException]] = {
    r'.*Keyspace not found.*': KeyspaceNotFoundException,
    r'.*Scope not found.*': ScopeNotFoundException,
    r'.*No index available.*': QueryIndexNotFoundException,
    r'.*[iI]ndex.*not found.*': QueryIndexNotFoundException,
    r'.*[iI]ndex.*already exists.*': QueryIndexAlreadyExistsException,
}


class ErrorMapper:
    @staticmethod
    def _compile(mapping: dict[str, type[CouchbaseException]]
                 ) -> dict[re.Pattern[str], type[CouchbaseException]]:
        return {re.compile(k): v for k, v in mapping.items()}

    @staticmethod
    def _process_mapping(compiled_map: dict[re.Pattern[str], type[CouchbaseException]],
                         err_content: str
                         ) -> type[CouchbaseException] | None:
        matches = None
        for pattern, exc_class in compiled_map.items():
            try:
                matches = pattern.match(err_content)
            except Exception:  # nosec
                pass
            if matches:
                return exc_class

        return None

    @staticmethod  # noqa: C901
    def _parse_http_response_body(compiled_map: dict[re.Pattern[str], type[CouchbaseException]],  # noqa: C901
                                  response_body: str
                                  ) -> type[CouchbaseException] | None:

        err_text = None
        try:
            http_body = json.loads(response_body)
        except json.decoder.JSONDecodeError:
            return None

        if isinstance(http_body, str):
            exc_class = ErrorMapper._process_mapping(compiled_map, http_body)
            if exc_class is not None:
                return exc_class
        elif isinstance(http_body, dict) and http_body.get("errors", None) is not None:
            errors = http_body["errors"]
            if isinstance(errors, list):
                for err in errors:
                    err_text = f"{err.get('code', None)} {err.get('msg', None)}"
                    if err_text:
                        exc_class = ErrorMapper._process_mapping(compiled_map, err_text)
                        if exc_class is not None:
                            return exc_class
                        err_text = None
            elif isinstance(errors, dict):
                err_text = errors.get("name", None)
        # eventing function mgmt cases
        elif isinstance(http_body, dict) and http_body.get('name', None) is not None:
            exc = ErrorMapper._process_mapping(compiled_map, http_body['name'])
            if exc is not None:
                return exc

        if err_text is not None:
            exc_class = ErrorMapper._process_mapping(compiled_map, err_text)
            return exc_class

        return None

    @staticmethod
    def _parse_http_context(err_ctx: HTTPErrorContext,
                            mapping: dict[str, type[CouchbaseException]] | None = None,
                            err_info: _ExceptionInfo | None = None
                            ) -> type[CouchbaseException] | None:
        compiled_map = ErrorMapper._compile(mapping) if mapping else {}

        exc_msg = err_info.get('error_message', None) if err_info else None
        if exc_msg is not None and not is_null_or_empty(exc_msg):
            exc_class = ErrorMapper._process_mapping(compiled_map, exc_msg)
            if exc_class is not None:
                return exc_class

        err_text = err_ctx.response_body
        if err_text is not None and not is_null_or_empty(err_text):
            exc_class = ErrorMapper._process_mapping(compiled_map, err_text)
            if exc_class is not None:
                return exc_class

            exc_class = ErrorMapper._parse_http_response_body(compiled_map, err_text)
            if exc_class is not None:
                return exc_class

        return None

    @staticmethod
    def _parse_kv_context(err_ctx: KeyValueErrorContext,
                          mapping: dict[str, type[CouchbaseException]],
                          err_content: str | None = None
                          ) -> type[CouchbaseException] | None:
        compiled_map = ErrorMapper._compile(mapping)

        if err_content is not None and not is_null_or_empty(err_content):
            exc_class = ErrorMapper._process_mapping(compiled_map, err_content)
            if exc_class is not None:
                return exc_class

        if err_ctx.retry_reasons is not None:
            for rr in err_ctx.retry_reasons:
                exc_class = ErrorMapper._process_mapping(compiled_map, rr)
                if exc_class is not None:
                    return exc_class

        return None

    @classmethod
    def build_exception(cls,
                        base_exc: pycbc_exception,
                        mapping: dict[str, type[CouchbaseException]] | None = None,
                        ) -> CouchbaseException:
        exc_class: type[CouchbaseException] | None = None
        err_ctx: ErrorContext | None = None
        ctx = base_exc.error_context()
        if ctx is None:
            exc_class = ERROR_MAP.get(base_exc.err(), CouchbaseException)
            err_info = base_exc.error_info()
        else:
            # A context type a newer core sends before this table knows it still has the fields
            # every context carries, so it degrades to the base class.
            err_ctx = CONTEXT_TYPES.get(ctx.get('context_type', 'ErrorContext'), ErrorContext)(**ctx)
            err_info = base_exc.error_info()

            # QueryErrorContext is an HTTPErrorContext, so it has to be tested first.
            if isinstance(err_ctx, QueryErrorContext):
                if mapping is None:
                    mapping = QUERY_ERROR_MAPPING
                exc_class = ErrorMapper._parse_http_context(err_ctx, mapping, err_info=err_info)
            elif isinstance(err_ctx, HTTPErrorContext):
                exc_class = ErrorMapper._parse_http_context(err_ctx, mapping, err_info=err_info)
            elif isinstance(err_ctx, KeyValueErrorContext):
                if mapping is None:
                    mapping = KV_ERROR_CONTEXT_MAPPING
                exc_class = ErrorMapper._parse_kv_context(err_ctx, mapping)

        if exc_class is None:
            exc_class = ERROR_MAP.get(base_exc.err(), CouchbaseException)

        exc = exc_class(base_exc.strerror().replace('_', ' '),
                        context=err_ctx,
                        error_code=base_exc.err(),
                        category=base_exc.err_category(),
                        exc_info=err_info)
        return exc
