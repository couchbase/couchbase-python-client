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

from enum import IntEnum
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    List,
                    Optional,
                    Union)

from couchbase.exceptions import (CouchbaseException,
                                  DeltaInvalidException,
                                  DocumentNotFoundException,
                                  DocumentNotJsonException,
                                  InvalidArgumentException,
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
from couchbase.logic.supportability import Supportability

if TYPE_CHECKING:
    from couchbase._utils import JSONType

"""
couchbase++ couchbase/protocol/client_opcode.hxx
enum class subdoc_opcode : uint8_t {
    get_doc = 0x00,
    set_doc = 0x01,
    remove_doc = 0x04,
    get = 0xc5,
    exists = 0xc6,
    dict_add = 0xc7,
    dict_upsert = 0xc8,
    remove = 0xc9,
    replace = 0xca,
    array_push_last = 0xcb,
    array_push_first = 0xcc,
    array_insert = 0xcd,
    array_add_unique = 0xce,
    counter = 0xcf,
    get_count = 0xd2,
    replace_body_with_xattr = 0xd3,
};

"""


class SubDocOp(IntEnum):
    GET_DOC = 0
    SET_DOC = 1
    REMOVE_DOC = 4
    GET = 197
    EXISTS = 198
    DICT_ADD = 199
    DICT_UPSERT = 200
    REMOVE = 201
    REPLACE = 202
    ARRAY_PUSH_LAST = 203
    ARRAY_PUSH_FIRST = 204
    ARRAY_INSERT = 205
    ARRAY_ADD_UNIQUE = 206
    COUNTER = 207
    GET_COUNT = 210
    REPLACE_BODY_WITH_XATTR = 211


"""
couchbase++ couchbase/protocol/client_opcode.hxx
enum class status : std::uint16_t {
    ...
    subdoc_path_not_found = 0xc0,
    subdoc_path_mismatch = 0xc1,
    subdoc_path_invalid = 0xc2,
    subdoc_path_too_big = 0xc3,
    subdoc_doc_too_deep = 0xc4,
    subdoc_value_cannot_insert = 0xc5,
    subdoc_doc_not_json = 0xc6,
    subdoc_num_range_error = 0xc7,
    subdoc_delta_invalid = 0xc8,
    subdoc_path_exists = 0xc9,
    subdoc_value_too_deep = 0xca,
    subdoc_invalid_combo = 0xcb,
    subdoc_multi_path_failure = 0xcc,
    subdoc_success_deleted = 0xcd,
    subdoc_xattr_invalid_flag_combo = 0xce,
    subdoc_xattr_invalid_key_combo = 0xcf,
    subdoc_xattr_unknown_macro = 0xd0,
    subdoc_xattr_unknown_vattr = 0xd1,
    subdoc_xattr_cannot_modify_vattr = 0xd2,
    subdoc_multi_path_failure_deleted = 0xd3,
    subdoc_invalid_xattr_order = 0xd4,
    subdoc_xattr_unknown_vattr_macro = 0xd5,
    subdoc_can_only_revive_deleted_documents = 0xd6,
    subdoc_deleted_document_cannot_have_value = 0xd7,
};
"""


class SubDocStatus(IntEnum):
    PathNotFound = 192
    PathMismatch = 193
    PathInvalid = 194
    PathTooBig = 195
    TooDeep = 196
    ValueCannotInsert = 197
    DocNotJson = 198
    NumRangeError = 199
    DeltaInvalid = 200
    PathExists = 201
    ValueTooDeep = 202


class StoreSemantics(IntEnum):
    """Define subdocument mutation operation store semantics.
    """
    REPLACE = 0
    UPSERT = 1
    INSERT = 2


class LookupInMacro():

    @staticmethod
    def document() -> str:
        return '$document'

    @staticmethod
    def expiry_time() -> str:
        return '$document.exptime'

    @staticmethod
    def cas() -> str:
        return '$document.CAS'

    @staticmethod
    def seq_no() -> str:
        return '$document.seqno'

    @staticmethod
    def last_modified() -> str:
        return '$document.last_modified'

    @staticmethod
    def is_deleted() -> str:
        return '$document.deleted'

    @staticmethod
    def value_size_bytes() -> str:
        return '$document.value_bytes'

    @staticmethod
    def rev_id() -> str:
        return '$document.revid'


class MutationMacro():
    def __init__(self, value: str):
        self._value = value

    @property
    def value(self) -> str:
        return self._value

    @classmethod
    def cas(cls) -> MutationMacro:
        return cls("${Mutation.CAS}")

    @classmethod
    def seq_no(cls) -> MutationMacro:
        return cls("${Mutation.seqno}")

    @classmethod
    def value_crc32c(cls) -> MutationMacro:
        return cls("${Mutation.value_crc32c}")


class Spec(tuple):
    """Represents a sub-operation to perform."""

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls, tuple(args))

    def __repr__(self):
        details = []
        # details.append(_SPECMAP.get(self[0]))
        details.extend([repr(x) for x in self[1:]])
        return '{0}<{1}>'.format(self.__class__.__name__,
                                 ', '.join(details))


class ArrayValues(tuple):
    def __new__(cls, *args, **kwargs):
        return super(ArrayValues, cls).__new__(cls, tuple(args))

    def __repr__(self):
        return 'ArrayValues({0})'.format(tuple.__repr__(self))


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

    raise CouchbaseException(f"Unknown status. Status={status}, path={path}, key={key}")


def exists(path,  # type: str
           xattr=False  # type: Optional[bool]
           ) -> Spec:
    """Creates a :class:`.Spec` that returns whether a specific field exists in the document.

    Args:
        path (str): The path to the field.
        xattr (bool, optional): Whether this operation should reference the document body or the
            extended attributes data for the document.

    Returns:
        :class:`.Spec`: An instance of :class:`.Spec`.

    """
    return Spec(SubDocOp.EXISTS, path, xattr)


def get(path,  # type: str
        xattr=False  # type: Optional[bool]
        ) -> Spec:
    """Creates a :class:`.Spec` for retrieving an element's value given a path.

    Args:
        path (str): The path to the field.
        xattr (bool, optional): Whether this operation should reference the document body or the
            extended attributes data for the document.

    Returns:
        :class:`.Spec`: An instance of :class:`.Spec`.

    """
    return Spec(SubDocOp.GET, path, xattr)


def count(path,  # type: str
          xattr=False  # type: Optional[bool]
          ) -> Spec:
    """Creates a :class:`.Spec` that returns the number of elements in the array referenced by the path.

    Args:
        path (str): The path to the field.
        xattr (bool, optional): Whether this operation should reference the document body or the
            extended attributes data for the document.

    Returns:
        :class:`.Spec`: An instance of :class:`.Spec`.

    """
    return Spec(SubDocOp.GET_COUNT, path, xattr)


def insert(path,                     # type: str
           value,                    # type: Union[JSONType, MutationMacro]
           create_parents=False,     # type: Optional[bool]
           xattr=False,               # type: Optional[bool]
           **kwargs                 # type: Dict[str, Any]
           ) -> Spec:
    """Creates a :class:`.Spec` for inserting a field into the document. Failing if the field already
    exists at the specified path.

    Args:
        path (str): The path to the field.
        value (Union[JSONType, MutationMacro]): The value to insert.
        create_parents (bool, optional): Whether or not the path to the field should be created
            if it does not already exist.
        xattr (bool, optional): Whether this operation should reference the document body or the
            extended attributes data for the document.

    Returns:
        :class:`.Spec`: An instance of :class:`.Spec`.

    """
    expand_macros = kwargs.get('expand_macros', False)
    if isinstance(value, MutationMacro):
        value = value.value
        xattr = True
        expand_macros = True
    return Spec(SubDocOp.DICT_ADD, path, create_parents, xattr, expand_macros, value)


def upsert(path,                     # type: str
           value,                    # type: Union[JSONType, MutationMacro]
           create_parents=False,     # type: Optional[bool]
           xattr=False               # type: Optional[bool]
           ) -> Spec:
    """Creates a :class:`.Spec` for upserting a field into the document. This updates the value of the specified field,
    or creates the field if it does not exits.

    Args:
        path (str): The path to the field.
        value (Union[JSONType, MutationMacro]): The value to upsert.
        create_parents (bool, optional): Whether or not the path to the field should be created
            if it does not already exist.
        xattr (bool, optional): Whether this operation should reference the document body or the
            extended attributes data for the document.

    Returns:
        :class:`.Spec`: An instance of :class:`.Spec`.

    """
    expand_macros = False
    if isinstance(value, MutationMacro):
        value = value.value
        xattr = True
        expand_macros = True
    return Spec(SubDocOp.DICT_UPSERT, path, create_parents, xattr, expand_macros, value)


def replace(path,                     # type: str
            value,                    # type: Union[JSONType, MutationMacro]
            xattr=False,              # type: Optional[bool]
            ) -> Spec:
    """Creates a :class:`.Spec` for replacing a field into the document. Failing if the field already
    exists at the specified path.

    Args:
        path (str): The path to the field.
        value (Union[JSONType, MutationMacro]): The value to write.
        xattr (bool, optional): Whether this operation should reference the document body or the
            extended attributes data for the document.

    Returns:
        :class:`.Spec`: An instance of :class:`.Spec`.

    """
    expand_macros = False
    if isinstance(value, MutationMacro):
        value = value.value
        xattr = True
        expand_macros = True
    if not path:
        return Spec(SubDocOp.SET_DOC, '', False, xattr, expand_macros, value)
    return Spec(SubDocOp.REPLACE, path, False, xattr, expand_macros, value)


def remove(path,                     # type: str
           xattr=False,              # type: Optional[bool]
           ) -> Spec:
    """Creates a :class:`.Spec` for removing a field from a document.

    Args:
        path (str): The path to the field.
        xattr (bool, optional): Whether this operation should reference the document body or the
            extended attributes data for the document.

    Returns:
        :class:`.Spec`: An instance of :class:`.Spec`.

    """
    if not path:
        return Spec(SubDocOp.REMOVE_DOC, '', False, xattr, False)
    return Spec(SubDocOp.REMOVE, path, False, xattr, False)


def array_append(path,              # type: str
                 *values,                 # type: Iterable[Any]
                 create_parents=False,     # type: Optional[bool]
                 xattr=False               # type: Optional[bool]
                 ) -> Spec:
    """Creates a :class:`.Spec` for adding a value to the end of an array in a document.

    Args:
        path (str): The path to an element of an array.
        *values (Iterable[Any]): The values to add.
        create_parents (bool, optional): Whether or not the path to the field should be created
            if it does not already exist.
        xattr (bool, optional): Whether this operation should reference the document body or the
            extended attributes data for the document.

    Returns:
        :class:`.Spec`: An instance of :class:`.Spec`.

    """
    expand_macros = False
    if any(map(lambda m: isinstance(m, MutationMacro), values)):
        values = [v.value if isinstance(v, MutationMacro) else v for v in values]
        xattr = True
        expand_macros = True
    return Spec(SubDocOp.ARRAY_PUSH_LAST, path, create_parents, xattr, expand_macros, ArrayValues(*values))


def array_prepend(path,              # type: str
                  *values,                 # type: Iterable[Any]
                  create_parents=False,     # type: Optional[bool]
                  xattr=False               # type: Optional[bool]
                  ) -> Spec:
    """Creates a :class:`.Spec` for adding a value to the beginning of an array in a document.

    Args:
        path (str): The path to an element of an array.
        *values (Iterable[Any]): The values to add.
        create_parents (bool, optional): Whether or not the path to the field should be created
            if it does not already exist.
        xattr (bool, optional): Whether this operation should reference the document body or the
            extended attributes data for the document.

    Returns:
        :class:`.Spec`: An instance of :class:`.Spec`.

    """
    expand_macros = False
    if any(map(lambda m: isinstance(m, MutationMacro), values)):
        values = [v.value if isinstance(v, MutationMacro) else v for v in values]
        xattr = True
        expand_macros = True
    return Spec(SubDocOp.ARRAY_PUSH_FIRST, path, create_parents, xattr, expand_macros, ArrayValues(*values))


def array_insert(path,              # type: str
                 *values,                 # type: Iterable[Any]
                 create_parents=False,     # type: Optional[bool]
                 xattr=False               # type: Optional[bool]
                 ) -> Spec:
    """Creates a :class:`.Spec` for adding a value to a specified location in an array in a document.
    The path should specify a specific index in the array and the new values are inserted at this location.

    Args:
        path (str): The path to an element of an array.
        *values (Iterable[Any]): The values to add.
        create_parents (bool, optional): Whether or not the path to the field should be created
            if it does not already exist.
        xattr (bool, optional): Whether this operation should reference the document body or the
            extended attributes data for the document.

    Returns:
        :class:`.Spec`: An instance of :class:`.Spec`.

    """
    expand_macros = False
    if any(map(lambda m: isinstance(m, MutationMacro), values)):
        values = [v.value if isinstance(v, MutationMacro) else v for v in values]
        xattr = True
        expand_macros = True
    return Spec(SubDocOp.ARRAY_INSERT, path, create_parents, xattr, expand_macros, ArrayValues(*values))


def array_addunique(path,              # type: str
                    value,                 # type: Union[str, int, float, bool, None]
                    create_parents=False,     # type: Optional[bool]
                    xattr=False               # type: Optional[bool]
                    ) -> Spec:
    """Creates a :class:`.Spec` for adding unique values to an array in a document. This operation will only
    add values if they do not already exist elsewhere in the array.

    Args:
        path (str): The path to an element of an array.
        value (Union[str, int, float, bool, None]): The value to add into the array.
        create_parents (bool, optional): Whether or not the path to the field should be created
            if it does not already exist.
        xattr (bool, optional): Whether this operation should reference the document body or the
            extended attributes data for the document.

    Returns:
        :class:`.Spec`: An instance of :class:`.Spec`.

    """
    expand_macros = False
    if isinstance(value, MutationMacro):
        value = value.value
        xattr = True
        expand_macros = True
    return Spec(SubDocOp.ARRAY_ADD_UNIQUE, path, create_parents, xattr, expand_macros, value)


def counter(path,                   # type: str
            delta,                  # type: int
            xattr=False,            # type: Optional[bool]
            create_parents=False    # type: Optional[bool]
            ) -> Spec:
    """Creates a :class:`.Spec` for incrementing or decrementing the value of a field in a document. If
    the provided delta is >= 0 :meth:`~couchbase.subdocument.increment` is called, otherwise
    :meth:`~couchbase.subdocument.decrement` is called.

    .. warning::
        This method is **deprecated** use :meth:`~.subdocument.increment` or :meth:`~couchbase.subdocument.decrement`

    Args:
        path (str): The path to the field.
        delta (int): The value to increment or decrement from the document.
        xattr (bool, optional): Whether this operation should reference the document body or the
            extended attributes data for the document.
        create_parents (bool, optional): Whether or not the path to the field should be created
            if it does not already exist.

    Returns:
        :class:`.Spec`: An instance of :class:`.Spec`.
    """
    if delta >= 0:
        return increment(path, delta, xattr=xattr, create_parents=create_parents)
    else:
        return decrement(path, abs(delta), xattr=xattr, create_parents=create_parents)


def increment(path,                   # type: str
              delta,                  # type: int
              xattr=False,            # type: Optional[bool]
              create_parents=False    # type: Optional[bool]
              ) -> Spec:
    """Creates a :class:`.Spec` for incrementing the value of a field in a document.

    Args:
        path (str): The path to the field.
        delta (int): The value to increment from the document.
        xattr (bool, optional): Whether this operation should reference the document body or the
            extended attributes data for the document.
        create_parents (bool, optional): Whether or not the path to the field should be created
            if it does not already exist.

    Returns:
        :class:`.Spec`: An instance of :class:`.Spec`.


    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If the delta arugment is not >= 0 or not
            of type int.
    """
    if not isinstance(delta, int):
        raise InvalidArgumentException("Delta must be integer")
    if delta <= 0:
        raise InvalidArgumentException(
            "Delta must be integer greater than or equal to 0")

    return Spec(SubDocOp.COUNTER, path, create_parents, xattr, False, delta)


def decrement(path,                   # type: str
              delta,                  # type: int
              xattr=False,            # type: Optional[bool]
              create_parents=False    # type: Optional[bool]
              ) -> Spec:
    """Creates a :class:`.Spec` for decrementing the value of a field in a document.

    Args:
        path (str): The path to the field.
        delta (int): The value to decrement from the document.
        xattr (bool, optional): Whether this operation should reference the document body or the
            extended attributes data for the document.
        create_parents (bool, optional): Whether or not the path to the field should be created
            if it does not already exist.

    Returns:
        :class:`.Spec`: An instance of :class:`.Spec`.


    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If the delta arugment is not >= 0 or not
            of type int.
    """
    if not isinstance(delta, int):
        raise InvalidArgumentException("Delta must be integer")
    if delta <= 0:
        raise InvalidArgumentException(
            "Delta must be integer greater than or equal to 0")

    return Spec(SubDocOp.COUNTER, path, create_parents, xattr, False, -1 * delta)


def get_full() -> Spec:
    """
    Fetches the entire document.

    :return: Spec
    """
    return Spec(SubDocOp.GET_DOC, '', False)


def with_expiry() -> Spec:
    """
    Fetches the expiry from the xattrs of the doc

    :return: Spec
    """
    return Spec(SubDocOp.GET, LookupInMacro.expiry_time(), True)


def convert_macro_cas_to_cas(cas  # type: str
                             ) -> int:
    """
    Utility method to help encode CAS coming from MutationMacro.cas() stored in the xattr.
    Due to a server bug, CAS is encoded backwards, but b/c of legacy users we cannot make a change.
    """
    reversed_bytes = bytearray.fromhex(cas[2:] if cas.startswith('0x') else cas)
    reversed_bytes.reverse()
    return int(reversed_bytes.hex(), base=16)


"""
** DEPRECATION NOTICE **

The classes below are deprecated for 3.x compatibility.  They should not be used.
Instead use:
    * All options should be imported from `couchbase.options`.
    * Scope object should be imported from `couchbase.scope`.

"""

from couchbase.logic.options import MutateInOptionsBase  # nopep8 # isort:skip # noqa: E402


@Supportability.import_deprecated('couchbase.subdocument', 'couchbase.options')
class MutateInOptions(MutateInOptionsBase):
    pass
