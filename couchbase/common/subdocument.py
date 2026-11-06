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
from couchbase.common._utils import fields_repr
from couchbase.common.exceptions import InvalidArgumentException

if TYPE_CHECKING:
    from couchbase.common._utils import JSONType


class SubDocOp(IntEnum):
    """**INTERNAL** The sub-document opcodes of the memcached protocol."""

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
    """The sub-document status codes of the memcached protocol."""

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
    """What ``mutate_in`` does about the document itself.

    REPLACE requires the document to exist, UPSERT creates it if it does not, and INSERT requires that it does not.
    """
    REPLACE = 0
    UPSERT = 1
    INSERT = 2


class LookupInMacro():
    """Paths to the virtual attributes the server keeps on every document, for ``lookup_in``.

    Each is read with ``xattr=True``.
    """

    @staticmethod
    def document() -> str:
        """The path to all of the document's metadata, as one object."""
        return '$document'

    @staticmethod
    def expiry_time() -> str:
        """The path to the document's expiry time, in seconds since the epoch, 0 if it has none."""
        return '$document.exptime'

    @staticmethod
    def cas() -> str:
        """The path to the document's CAS, as a hex string."""
        return '$document.CAS'

    @staticmethod
    def seq_no() -> str:
        """The path to the document's sequence number, as a hex string."""
        return '$document.seqno'

    @staticmethod
    def last_modified() -> str:
        """The path to when the document last changed, in seconds since the epoch."""
        return '$document.last_modified'

    @staticmethod
    def is_deleted() -> str:
        """The path to whether the document is a tombstone."""
        return '$document.deleted'

    @staticmethod
    def value_size_bytes() -> str:
        """The path to the size of the document's body, in bytes."""
        return '$document.value_bytes'

    @staticmethod
    def rev_id() -> str:
        """The path to the document's revision ID."""
        return '$document.revid'


class MutationMacro():
    """A value the server fills in from the mutation that writes it.

    Args:
        value (str): The macro, as the server names it. The class methods return the ones it knows.
    """

    def __init__(self, value: str) -> None:
        self._value = value

    @property
    def value(self) -> str:
        """The macro, as the server names it."""
        return self._value

    @classmethod
    def cas(cls) -> MutationMacro:
        """The mutation's CAS.

        The server writes it as a hex string in little-endian byte order; :func:`convert_macro_cas_to_cas` reads it back
        as the CAS a result reports.
        """
        return cls("${Mutation.CAS}")

    @classmethod
    def seq_no(cls) -> MutationMacro:
        """The mutation's sequence number, as a hex string."""
        return cls("${Mutation.seqno}")

    @classmethod
    def value_crc32c(cls) -> MutationMacro:
        """The CRC32C checksum of the document's body after the mutation, as a hex string."""
        return cls("${Mutation.value_crc32c}")

    def __repr__(self) -> str:
        return fields_repr(self, value=self._value)


class Spec(tuple):
    """One operation of a ``lookup_in`` or ``mutate_in``, as the functions in this module create it."""

    def __new__(cls, *args: Any, **kwargs: Any) -> Spec:
        """**INTERNAL** The operation from its opcode and arguments."""
        return super().__new__(cls, tuple(args))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(repr(x) for x in self[1:])})"


class ArrayValues(tuple):
    """**INTERNAL** The values an array operation adds, kept apart from its other arguments."""

    def __new__(cls, *args: Any, **kwargs: Any) -> ArrayValues:
        """**INTERNAL** The values, in order."""
        return super(ArrayValues, cls).__new__(cls, tuple(args))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join(repr(x) for x in self)})"


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
    """Creates a :class:`.Spec` that adds a field, failing if the field already exists.

    A :class:`MutationMacro` value is written to the extended attributes, whatever ``xattr`` says, with the macro
    expanded by the server.

    Args:
        path (str): The path to the field.
        value (JSONType | MutationMacro): The value to add.
        create_parents (bool, optional): Whether to create the path's missing parent fields. Defaults to
            False.
        xattr (bool, optional): Whether the path is in the document's extended attributes rather than its
            body. Defaults to False.
        **kwargs: ``expand_macros`` (bool) asks the server to expand macros in a str value. A
            :class:`MutationMacro` value sets it.

    Returns:
        :class:`.Spec`: The operation, for ``mutate_in``.
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
    """Creates a :class:`.Spec` that sets a field, adding it if it does not exist.

    A :class:`MutationMacro` value is written to the extended attributes, whatever ``xattr`` says, with the macro
    expanded by the server.

    Args:
        path (str): The path to the field.
        value (JSONType | MutationMacro): The value to set.
        create_parents (bool, optional): Whether to create the path's missing parent fields. Defaults to
            False.
        xattr (bool, optional): Whether the path is in the document's extended attributes rather than its
            body. Defaults to False.

    Returns:
        :class:`.Spec`: The operation, for ``mutate_in``.
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
    """Creates a :class:`.Spec` that sets a field, failing if the field does not exist.

    An empty path replaces the whole body. A :class:`MutationMacro` value is written to the extended attributes,
    whatever ``xattr`` says, with the macro expanded by the server.

    Args:
        path (str): The path to the field, or an empty str for the whole body.
        value (JSONType | MutationMacro): The value to set.
        xattr (bool, optional): Whether the path is in the document's extended attributes rather than its
            body. Defaults to False.

    Returns:
        :class:`.Spec`: The operation, for ``mutate_in``.
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
    """Creates a :class:`.Spec` that removes a field.

    An empty path removes the whole document.

    Args:
        path (str): The path to the field, or an empty str for the whole document.
        xattr (bool, optional): Whether the path is in the document's extended attributes rather than its
            body. Defaults to False.

    Returns:
        :class:`.Spec`: The operation, for ``mutate_in``.
    """
    if not path:
        return Spec(SubDocOp.REMOVE_DOC, '', False, xattr, False)
    return Spec(SubDocOp.REMOVE, path, False, xattr, False)


def array_append(path: str,
                 *values: JSONType | MutationMacro,
                 create_parents: bool | None = False,
                 xattr: bool | None = False
                 ) -> Spec:
    """Creates a :class:`.Spec` that adds values to the end of an array.

    A :class:`MutationMacro` value is written to the extended attributes, whatever ``xattr`` says, with the macro
    expanded by the server.

    Args:
        path (str): The path to the array.
        *values (JSONType | MutationMacro): The values to add, in order.
        create_parents (bool, optional): Whether to create the path's missing parent fields. Defaults to
            False.
        xattr (bool, optional): Whether the path is in the document's extended attributes rather than its
            body. Defaults to False.

    Returns:
        :class:`.Spec`: The operation, for ``mutate_in``.
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
    """Creates a :class:`.Spec` that adds values to the start of an array.

    A :class:`MutationMacro` value is written to the extended attributes, whatever ``xattr`` says, with the macro
    expanded by the server.

    Args:
        path (str): The path to the array.
        *values (JSONType | MutationMacro): The values to add, in order.
        create_parents (bool, optional): Whether to create the path's missing parent fields. Defaults to
            False.
        xattr (bool, optional): Whether the path is in the document's extended attributes rather than its
            body. Defaults to False.

    Returns:
        :class:`.Spec`: The operation, for ``mutate_in``.
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
    """Creates a :class:`.Spec` that adds values at a position in an array.

    The path ends in the index to insert at, as in ``tags[2]``. A :class:`MutationMacro` value is written to the
    extended attributes, whatever ``xattr`` says, with the macro expanded by the server.

    Args:
        path (str): The path to the array, ending in the index.
        *values (JSONType | MutationMacro): The values to add, in order.
        create_parents (bool, optional): Whether to create the path's missing parent fields. Defaults to
            False.
        xattr (bool, optional): Whether the path is in the document's extended attributes rather than its
            body. Defaults to False.

    Returns:
        :class:`.Spec`: The operation, for ``mutate_in``.
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
    """Creates a :class:`.Spec` that adds a value to an array unless the array already holds it.

    A :class:`MutationMacro` value is written to the extended attributes, whatever ``xattr`` says, with the macro
    expanded by the server.

    Args:
        path (str): The path to the array.
        value (str | int | float | bool | MutationMacro | None): The value to add.
        create_parents (bool, optional): Whether to create the path's missing parent fields. Defaults to
            False.
        xattr (bool, optional): Whether the path is in the document's extended attributes rather than its
            body. Defaults to False.

    Returns:
        :class:`.Spec`: The operation, for ``mutate_in``.
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
    """**DEPRECATED** Creates a :class:`.Spec` that adds a delta to a number.

    A positive delta calls :func:`increment`, and a negative one :func:`decrement`.

    .. deprecated:: 4.0.0
        Use :func:`increment` or :func:`decrement` instead.

    Args:
        path (str): The path to the field.
        delta (int): The amount to add. Must not be 0.
        xattr (bool, optional): Whether the path is in the document's extended attributes rather than its
            body. Defaults to False.
        create_parents (bool, optional): Whether to create the path's missing parent fields. Defaults to
            False.

    Returns:
        :class:`.Spec`: The operation, for ``mutate_in``.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If the delta is 0 or not an int.
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
    """Creates a :class:`.Spec` that adds to a number.

    Args:
        path (str): The path to the field.
        delta (int): The amount to add. Must be greater than 0.
        xattr (bool, optional): Whether the path is in the document's extended attributes rather than its
            body. Defaults to False.
        create_parents (bool, optional): Whether to create the path's missing parent fields. Defaults to
            False.

    Returns:
        :class:`.Spec`: The operation, for ``mutate_in``.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If the delta is not an int greater than 0.
    """
    if not isinstance(delta, int):
        raise InvalidArgumentException("Delta must be integer")
    if delta <= 0:
        raise InvalidArgumentException(
            "Delta must be an integer greater than 0.")

    return Spec(SubDocOp.COUNTER, path, create_parents, xattr, False, delta)


def decrement(path: str,
              delta: int,
              xattr: bool | None = False,
              create_parents: bool | None = False
              ) -> Spec:
    """Creates a :class:`.Spec` that subtracts from a number.

    Args:
        path (str): The path to the field.
        delta (int): The amount to subtract. Must be greater than 0.
        xattr (bool, optional): Whether the path is in the document's extended attributes rather than its
            body. Defaults to False.
        create_parents (bool, optional): Whether to create the path's missing parent fields. Defaults to
            False.

    Returns:
        :class:`.Spec`: The operation, for ``mutate_in``.

    Raises:
        :class:`~couchbase.exceptions.InvalidArgumentException`: If the delta is not an int greater than 0.
    """
    if not isinstance(delta, int):
        raise InvalidArgumentException("Delta must be integer")
    if delta <= 0:
        raise InvalidArgumentException(
            "Delta must be an integer greater than 0.")

    return Spec(SubDocOp.COUNTER, path, create_parents, xattr, False, -1 * delta)


def get_full() -> Spec:
    """Creates a :class:`.Spec` that reads the whole body.

    Returns:
        :class:`.Spec`: The operation, for ``lookup_in``.
    """
    return Spec(SubDocOp.GET_DOC, '', False)


def with_expiry() -> Spec:
    """Creates a :class:`.Spec` that reads the document's expiry time.

    Returns:
        :class:`.Spec`: The operation, for ``lookup_in``.
    """
    return Spec(SubDocOp.GET, LookupInMacro.expiry_time(), True)


def convert_macro_cas_to_cas(cas: str) -> int:
    """The CAS a :meth:`MutationMacro.cas` field holds, as the int a result reports.

    The server writes that macro as a hex string in little-endian byte order, so its bytes are reversed before they are
    read.

    Args:
        cas (str): The field's value, with or without a ``0x`` prefix.

    Returns:
        int: The CAS.
    """
    reversed_bytes = bytearray.fromhex(cas[2:] if cas.startswith('0x') else cas)
    reversed_bytes.reverse()
    return int(reversed_bytes.hex(), base=16)
