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

"""BRIDGE: the subdocument helpers the logic package and the public result module still call.

This module exists only for the bridge.  Removed by PYCBC-2001.
"""

from enum import IntFlag
from typing import (Any,
                    Dict,
                    List)

from couchbase.common.exceptions import (CouchbaseException,
                                         DeltaInvalidException,
                                         DocumentNotFoundException,
                                         DocumentNotJsonException,
                                         InvalidIndexException,
                                         NumberTooBigException,
                                         PathExistsException,
                                         PathInvalidException,
                                         PathMismatchException,
                                         PathNotFoundException,
                                         PathTooBigException,
                                         PathTooDeepException,
                                         SubdocCantInsertValueException,
                                         ValueTooDeepException)
from couchbase.common.subdocument import SubDocOp, SubDocStatus


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


def parse_subdocument_content_as(content,  # type: List[Dict[str, Any]]
                                 index,    # type: int
                                 key,      # type: str
                                 ) -> Any:
    if index > len(content) - 1 or index < 0:
        raise InvalidIndexException(f"Provided index is invalid. Index={index}.")

    status = content[index].get('status', None)
    if status is None:
        raise DocumentNotFoundException(f"Could not find document. Key={key}.")

    op_code = content[index].get('opcode', None)
    if op_code == SubDocOp.EXISTS:
        return parse_subdocument_exists(content, index, key)
    if status == 0:
        return content[index].get('value', None)

    path = content[index].get('path', None)
    parse_subdocument_status(status, path, key)


def parse_subdocument_exists(content,  # type: List[Dict[str, Any]]
                             index,    # type: int
                             key,      # type: str
                             ) -> bool:
    if index > len(content) - 1 or index < 0:
        raise InvalidIndexException(f"Provided index is invalid. Index={index}.")

    status = content[index].get('status', None)
    if status is None:
        raise DocumentNotFoundException(f"Could not find document. Key={key}.")

    path = content[index].get('path', None)
    if status == 0:
        return True
    elif status == SubDocStatus.PathNotFound:
        return False

    parse_subdocument_status(status, path, key)


def parse_subdocument_status(status, path, key):  # noqa: C901
    if status == SubDocStatus.PathNotFound:
        raise PathNotFoundException(f"Path could not be found. Path={path}, key={key}.")
    if status == SubDocStatus.PathMismatch:
        raise PathMismatchException(f"Path mismatch. Path={path}, key={key}.")
    if status == SubDocStatus.PathInvalid:
        raise PathInvalidException(f"Path is invalid. Path={path}, key={key}.")
    if status == SubDocStatus.PathTooBig:
        msg = f"Path is too long, or contains too many independent components. Path={path}, key={key}."
        raise PathTooBigException(msg)
    if status == SubDocStatus.TooDeep:
        raise PathTooDeepException(f"Path contains too many levels to parse. Path={path}, key={key}.")
    if status == SubDocStatus.ValueCannotInsert:
        raise SubdocCantInsertValueException(f"Cannot insert value. Path={path}, key={key}.")
    if status == SubDocStatus.DocNotJson:
        raise DocumentNotJsonException(f"Cannot operate on non-JSON document. Path={path}, key={key}.")
    if status == SubDocStatus.NumRangeError:
        msg = f"Value is outside the valid range for arithmetic operations. Path={path}, key={key}."
        raise NumberTooBigException(msg)
    if status == SubDocStatus.DeltaInvalid:
        raise DeltaInvalidException(f"Delta value specified for operation is too large. Path={path}, key={key}.")
    if status == SubDocStatus.PathExists:
        raise PathExistsException(f"Path already exists. Path={path}, key={key}.")
    if status == SubDocStatus.ValueTooDeep:
        raise ValueTooDeepException(f"Value too deep for document. Path={path}, key={key}.")

    raise CouchbaseException(message=f"Unknown status. Status={status}, path={path}, key={key}")
