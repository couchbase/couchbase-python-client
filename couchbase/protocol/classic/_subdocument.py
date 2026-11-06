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

"""The C++ core's subdocument wire vocabulary: the path flags a request carries and the status
each field of a response comes back with."""

from __future__ import annotations

from enum import IntFlag

from couchbase.common.exceptions import (CannotReviveLivingDocumentException,
                                         CouchbaseException,
                                         DeltaInvalidException,
                                         DocumentNotJsonException,
                                         NumberTooBigException,
                                         PathExistsException,
                                         PathInvalidException,
                                         PathMismatchException,
                                         PathNotFoundException,
                                         PathTooBigException,
                                         PathTooDeepException,
                                         SubdocCantInsertValueException,
                                         ValueTooDeepException,
                                         XattrCannotModifyVirtualAttributeException,
                                         XattrInvalidKeyComboException,
                                         XattrUnknownMacroException,
                                         XattrUnknownVirtualAttributeException)
from couchbase.common.subdocument import SubDocStatus


class PathFlags(IntFlag):
    NONE = 0
    CREATE_PARENTS = 0b0000_0001
    XATTR = 0b0000_0100
    EXPAND_MACROS = 0b0001_0000
    BINARY_VALUE = 0b0010_0000


def build_mutate_in_path_flags(
    xattr: bool,
    create_path: bool,
    expand_macro: bool,
    binary: bool
) -> PathFlags:
    flags = PathFlags.NONE

    if xattr:
        flags |= PathFlags.XATTR
        if binary:
            flags |= PathFlags.BINARY_VALUE

    if create_path:
        flags |= PathFlags.CREATE_PARENTS

    if expand_macro:
        flags |= PathFlags.EXPAND_MACROS

    return flags


def build_lookup_in_path_flags(xattr: bool, binary: bool) -> PathFlags:
    flags = PathFlags.NONE
    if xattr:
        flags |= PathFlags.XATTR
        if binary:
            flags |= PathFlags.BINARY_VALUE
    return flags


def _subdoc_error(status: int, path: str, key: str | None) -> CouchbaseException | None:  # noqa: C901
    if status == 0:
        return None
    if status == SubDocStatus.PathNotFound:
        return PathNotFoundException(f"Path could not be found. Path={path}, key={key}.")
    if status == SubDocStatus.PathMismatch:
        return PathMismatchException(f"Path mismatch. Path={path}, key={key}.")
    if status == SubDocStatus.PathInvalid:
        return PathInvalidException(f"Path is invalid. Path={path}, key={key}.")
    if status == SubDocStatus.PathTooBig:
        msg = f"Path is too long, or contains too many independent components. Path={path}, key={key}."
        return PathTooBigException(msg)
    if status == SubDocStatus.TooDeep:
        return PathTooDeepException(f"Document contains too many levels to parse. Path={path}, key={key}.")
    if status == SubDocStatus.ValueCannotInsert:
        return SubdocCantInsertValueException(f"Cannot insert value. Path={path}, key={key}.")
    if status == SubDocStatus.DocNotJson:
        return DocumentNotJsonException(f"Cannot operate on non-JSON document. Path={path}, key={key}.")
    if status == SubDocStatus.NumRangeError:
        msg = f"Value is outside the valid range for arithmetic operations. Path={path}, key={key}."
        return NumberTooBigException(msg)
    if status == SubDocStatus.DeltaInvalid:
        return DeltaInvalidException(f"Delta value specified for operation is too large. Path={path}, key={key}.")
    if status == SubDocStatus.PathExists:
        return PathExistsException(f"Path already exists. Path={path}, key={key}.")
    if status == SubDocStatus.ValueTooDeep:
        return ValueTooDeepException(f"Value too deep for document. Path={path}, key={key}.")
    # The core folds the flag combo into the key combo, and the vattr macro into the unknown macro.
    if status in (SubDocStatus.XattrInvalidFlagCombo, SubDocStatus.XattrInvalidKeyCombo):
        msg = f"Invalid extended attribute combination. Path={path}, key={key}."
        return XattrInvalidKeyComboException(msg)
    if status in (SubDocStatus.XattrUnknownMacro, SubDocStatus.XattrUnknownVattrMacro):
        return XattrUnknownMacroException(f"Unknown macro. Path={path}, key={key}.")
    if status == SubDocStatus.XattrUnknownVattr:
        return XattrUnknownVirtualAttributeException(f"Unknown virtual attribute. Path={path}, key={key}.")
    if status == SubDocStatus.XattrCannotModifyVattr:
        msg = f"Virtual attributes cannot be modified. Path={path}, key={key}."
        return XattrCannotModifyVirtualAttributeException(msg)
    if status == SubDocStatus.CanOnlyReviveDeletedDocuments:
        msg = f"Only a deleted document can be revived. Path={path}, key={key}."
        return CannotReviveLivingDocumentException(msg)

    return CouchbaseException(message=f"Unknown status. Status={status}, path={path}, key={key}")
