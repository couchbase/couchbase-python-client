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

__all__ = [
    'ArrayValues',
    'LookupInMacro',
    'MutationMacro',
    'Spec',
    'StoreSemantics',
    'SubDocOp',
    'SubDocStatus',
    'array_addunique',
    'array_append',
    'array_insert',
    'array_prepend',
    'convert_macro_cas_to_cas',
    'count',
    'counter',
    'decrement',
    'exists',
    'get',
    'get_full',
    'increment',
    'insert',
    'remove',
    'replace',
    'upsert',
    'with_expiry',
]

from enum import IntEnum
from typing import TYPE_CHECKING, Any

from couchbase.common._supportability import Supportability
from couchbase.common.exceptions import InvalidArgumentException

if TYPE_CHECKING:
    from couchbase.common._utils import JSONType


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
    XattrInvalidFlagCombo = 206
    XattrInvalidKeyCombo = 207
    XattrUnknownMacro = 208
    XattrUnknownVattr = 209
    XattrCannotModifyVattr = 210
    XattrUnknownVattrMacro = 213
    CanOnlyReviveDeletedDocuments = 214


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
    def __init__(self, value: str) -> None:
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

    def __new__(cls, *args: Any, **kwargs: Any) -> Spec:
        return super().__new__(cls, tuple(args))

    def __repr__(self) -> str:
        details = []
        details.extend([repr(x) for x in self[1:]])
        return '{0}<{1}>'.format(self.__class__.__name__,
                                 ', '.join(details))


class ArrayValues(tuple):
    def __new__(cls, *args: Any, **kwargs: Any) -> ArrayValues:
        return super(ArrayValues, cls).__new__(cls, tuple(args))

    def __repr__(self) -> str:
        return 'ArrayValues({0})'.format(tuple.__repr__(self))


def exists(path: str,
           xattr: bool | None = False
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


def get(path: str,
        xattr: bool | None = False
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


def count(path: str,
          xattr: bool | None = False
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


def insert(path: str,
           value: JSONType | MutationMacro,
           create_parents: bool | None = False,
           xattr: bool | None = False,
           **kwargs: Any
           ) -> Spec:
    """Creates a :class:`.Spec` for inserting a field into the document. Failing if the field already
    exists at the specified path.

    Args:
        path (str): The path to the field.
        value (JSONType | MutationMacro): The value to insert.
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


def upsert(path: str,
           value: JSONType | MutationMacro,
           create_parents: bool | None = False,
           xattr: bool | None = False
           ) -> Spec:
    """Creates a :class:`.Spec` for upserting a field into the document. This updates the value of the specified field,
    or creates the field if it does not exits.

    Args:
        path (str): The path to the field.
        value (JSONType | MutationMacro): The value to upsert.
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


def replace(path: str,
            value: JSONType | MutationMacro,
            xattr: bool | None = False,
            ) -> Spec:
    """Creates a :class:`.Spec` for replacing a field into the document. Failing if the field already
    exists at the specified path.

    Args:
        path (str): The path to the field.
        value (JSONType | MutationMacro): The value to write.
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


def remove(path: str,
           xattr: bool | None = False,
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


def array_append(path: str,
                 *values: JSONType | MutationMacro,
                 create_parents: bool | None = False,
                 xattr: bool | None = False
                 ) -> Spec:
    """Creates a :class:`.Spec` for adding a value to the end of an array in a document.

    Args:
        path (str): The path to an element of an array.
        *values (JSONType | MutationMacro): The values to add.
        create_parents (bool, optional): Whether or not the path to the field should be created
            if it does not already exist.
        xattr (bool, optional): Whether this operation should reference the document body or the
            extended attributes data for the document.

    Returns:
        :class:`.Spec`: An instance of :class:`.Spec`.

    """
    expand_macros = False
    if any(map(lambda m: isinstance(m, MutationMacro), values)):
        values = tuple(v.value if isinstance(v, MutationMacro) else v for v in values)
        xattr = True
        expand_macros = True
    return Spec(SubDocOp.ARRAY_PUSH_LAST, path, create_parents, xattr, expand_macros, ArrayValues(*values))


def array_prepend(path: str,
                  *values: JSONType | MutationMacro,
                  create_parents: bool | None = False,
                  xattr: bool | None = False
                  ) -> Spec:
    """Creates a :class:`.Spec` for adding a value to the beginning of an array in a document.

    Args:
        path (str): The path to an element of an array.
        *values (JSONType | MutationMacro): The values to add.
        create_parents (bool, optional): Whether or not the path to the field should be created
            if it does not already exist.
        xattr (bool, optional): Whether this operation should reference the document body or the
            extended attributes data for the document.

    Returns:
        :class:`.Spec`: An instance of :class:`.Spec`.

    """
    expand_macros = False
    if any(map(lambda m: isinstance(m, MutationMacro), values)):
        values = tuple(v.value if isinstance(v, MutationMacro) else v for v in values)
        xattr = True
        expand_macros = True
    return Spec(SubDocOp.ARRAY_PUSH_FIRST, path, create_parents, xattr, expand_macros, ArrayValues(*values))


def array_insert(path: str,
                 *values: JSONType | MutationMacro,
                 create_parents: bool | None = False,
                 xattr: bool | None = False
                 ) -> Spec:
    """Creates a :class:`.Spec` for adding a value to a specified location in an array in a document.
    The path should specify a specific index in the array and the new values are inserted at this location.

    Args:
        path (str): The path to an element of an array.
        *values (JSONType | MutationMacro): The values to add.
        create_parents (bool, optional): Whether or not the path to the field should be created
            if it does not already exist.
        xattr (bool, optional): Whether this operation should reference the document body or the
            extended attributes data for the document.

    Returns:
        :class:`.Spec`: An instance of :class:`.Spec`.

    """
    expand_macros = False
    if any(map(lambda m: isinstance(m, MutationMacro), values)):
        values = tuple(v.value if isinstance(v, MutationMacro) else v for v in values)
        xattr = True
        expand_macros = True
    return Spec(SubDocOp.ARRAY_INSERT, path, create_parents, xattr, expand_macros, ArrayValues(*values))


def array_addunique(path: str,
                    value: str | int | float | bool | MutationMacro | None,
                    create_parents: bool | None = False,
                    xattr: bool | None = False
                    ) -> Spec:
    """Creates a :class:`.Spec` for adding unique values to an array in a document. This operation will only
    add values if they do not already exist elsewhere in the array.

    Args:
        path (str): The path to an element of an array.
        value (str | int | float | bool | MutationMacro | None): The value to add into the array.
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


def counter(path: str,
            delta: int,
            xattr: bool | None = False,
            create_parents: bool | None = False
            ) -> Spec:
    """Creates a :class:`.Spec` for incrementing or decrementing the value of a field in a document. If
    the provided delta is >= 0 :meth:`~couchbase.subdocument.increment` is called, otherwise
    :meth:`~couchbase.subdocument.decrement` is called.

    .. deprecated:: 4.0.0
        Use :meth:`~couchbase.subdocument.increment` or :meth:`~couchbase.subdocument.decrement` instead.

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
    Supportability.method_deprecated('counter', 'increment or decrement')
    if delta >= 0:
        return increment(path, delta, xattr=xattr, create_parents=create_parents)
    else:
        return decrement(path, abs(delta), xattr=xattr, create_parents=create_parents)


def increment(path: str,
              delta: int,
              xattr: bool | None = False,
              create_parents: bool | None = False
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


def decrement(path: str,
              delta: int,
              xattr: bool | None = False,
              create_parents: bool | None = False
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

    Returns:
        Spec
    """
    return Spec(SubDocOp.GET_DOC, '', False)


def with_expiry() -> Spec:
    """
    Fetches the expiry from the xattrs of the doc

    Returns:
        Spec
    """
    return Spec(SubDocOp.GET, LookupInMacro.expiry_time(), True)


def convert_macro_cas_to_cas(cas: str) -> int:
    """
    Utility method to help encode CAS coming from MutationMacro.cas() stored in the xattr.
    Due to a server bug, CAS is encoded backwards, but b/c of legacy users we cannot make a change.
    """
    reversed_bytes = bytearray.fromhex(cas[2:] if cas.startswith('0x') else cas)
    reversed_bytes.reverse()
    return int(reversed_bytes.hex(), base=16)
