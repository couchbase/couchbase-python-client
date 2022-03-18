from enum import IntEnum
from typing import (Any,
                    Dict,
                    Iterable)

from couchbase.exceptions import InvalidArgumentException

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
    REPLACE = 0
    UPSERT = 1
    INSERT = 2


class Spec(tuple):
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


def exists(
        path,  # type: str
        xattr=False  # type: bool
):
    # type: (...) -> Spec
    """
    Checks for the existence of a field given a path.

    :param str path: path to the element
    :param xattr: operation is done on an Extended Attribute.
    :return: Spec
    """

    return Spec(SubDocOp.EXISTS, path, xattr)


def get(path,  # type: str
        xattr=False  # type: bool
        ):
    # type: (...) -> Spec
    """
    Fetches an element's value given a path.

    :param str path: String path - path to the element
    :param bool xattr: operation is done on an Extended Attribute.
    :return: Spec
    """
    return Spec(SubDocOp.GET, path, xattr)


def count(path,  # type: str
          xattr=False  # type: bool
          ):
    # type: (...) -> Spec
    """
    Gets the count of a list or dictionary element given a path

    :param path: String path - path to the element
    :param bool xattr: operation is done on an Extended Attribute.
    :return: Spec
    """
    return Spec(SubDocOp.GET_COUNT, path, xattr)


def insert(path,                     # type: str
           value,                    # type: Dict[str, Any]
           create_parents=False,     # type: bool
           xattr=False,               # type: False
           **kwargs                 # type: Any
           ) -> Spec:
    """
    Insert a value at a given path in a document.

    :param str path:  Path to insert into document.
    :param JSON value: Value to insert at this path.
    :param create_parents: Whether or not to create the parents in the path,
        if they don't already exist.
    :param xattr: whether this is an xattr path
    :return: Spec
    """
    return Spec(
        SubDocOp.DICT_ADD,
        path,
        create_parents,
        xattr,
        kwargs.get(
            "expand_macros",
            False),
        value)


def upsert(path,                     # type: str
           value,                    # type: Dict[str, Any]
           create_parents=False,     # type: bool
           xattr=False               # type: bool
           ) -> Spec:
    """
    Upsert a value at a given path in a document.

    :param str path:  Path to upsert into document.
    :param JSON value: Value to upsert at this path.
    :param create_parents: Whether or not to create the parents in the path,
        if they don't already exist.
    :param xattr: whether this is an xattr path
    :return: Spec
    """
    return Spec(
        SubDocOp.DICT_UPSERT,
        path,
        create_parents,
        xattr,
        False,
        value)


def replace(path,                     # type: str
            value,                    # type: Dict[str, Any]
            xattr=False,              # type: bool
            ) -> Spec:
    """
    Upsert a value at a given path in a document.

    :param str path:  Path to upsert into document.
    :param JSON value: Value to upsert at this path.
    :param xattr: whether this is an xattr path
    :return: Spec
    """
    return Spec(SubDocOp.REPLACE, path, False, xattr, False, value)


def remove(path,                     # type: str
           xattr=False,              # type: bool
           ) -> Spec:
    """
    Remove a path from a document.

    :param str path: Path to remove from document.
    :param xattr: whether this is an xattr path
    :return: Spec
    """
    return Spec(SubDocOp.REMOVE, path, False, xattr, False)


def array_append(path,              # type: str
                 *values,                 # type: Iterable[Any]
                 create_parents=False,     # type: bool
                 xattr=False               # type: bool
                 ) -> Spec:
    """
    Add new values to the end of an array.

    :param path: Path to the array. The path should contain the *array itself*
        and not an element *within* the array
    :param values: one or more values to append
    :param create_parents: Create the array if it does not exist
    """
    return Spec(
        SubDocOp.ARRAY_PUSH_LAST,
        path,
        create_parents,
        xattr,
        False,
        ArrayValues(
            *values))


def array_prepend(path,              # type: str
                  *values,                 # type: Iterable[Any]
                  create_parents=False,     # type: bool
                  xattr=False               # type: bool
                  ) -> Spec:
    """
    Add new values to the beginning of an array.

    :param path: Path to the array. The path should contain the *array itself*
        and not an element *within* the array
    :param values: one or more values to append
    :param create_parents: Create the array if it does not exist

    This operation is only valid in :cb_bmeth:`mutate_in`.

    .. seealso:: :func:`array_append`, :func:`upsert`
    """

    return Spec(
        SubDocOp.ARRAY_PUSH_FIRST,
        path,
        create_parents,
        xattr,
        False,
        ArrayValues(
            *values))


def array_insert(path,              # type: str
                 *values,                 # type: Iterable[Any]
                 create_parents=False,     # type: bool
                 xattr=False               # type: bool
                 ) -> Spec:
    """
    Insert values at into an array in a document at the position
    given in the path.

    :param str path: Path to the spot in the array where the values
        should be inserted.  Note in this case, the path is a path
        to a specific location in an array.
    :param values: Value(s) to insert.
    :param xattr: whether this is an xattr path
    :return: Spec
    """
    return Spec(
        SubDocOp.ARRAY_INSERT,
        path,
        create_parents,
        xattr,
        False,
        ArrayValues(
            *values))


def array_addunique(path,              # type: str
                    *values,                 # type: Iterable[Any]
                    create_parents=False,     # type: bool
                    xattr=False               # type: bool
                    ) -> Spec:
    """
    Add a value to an existing array, if it doesn't currently exist in the array.  Note the
    position is unspecified -- it is up to the server to place it where it wants.
    :param str path: Path to an array in a document. Note this is the path *to*
        the array, not to the path to a specific element.
    :param value: Value to add, if it does not already exist in the array.
    :param xattr: whether this is an xattr path
    :param bool create_parents: If True, create the array if it does not already exist.
    :return: Spec
    """
    return Spec(
        SubDocOp.ARRAY_ADD_UNIQUE,
        path,
        create_parents,
        xattr,
        False,
        ArrayValues(
            *values))


def counter(path,                   # type: str
            delta,                  # type: int
            xattr=False,            # type: bool
            create_parents=False    # type: bool
            ) -> Spec:
    """
    **DEPRECATED** use increment() or decrement()

    Increment/Decrement a counter in a document.

    :param str path: Path to the counter
    :param int delta: Amount to change the counter.   Cannot be 0, and must be and integer.
    :param xattr: whether this is an xattr path
    :param bool create_parents: Create the counter if it doesn't exist.  Will be initialized
        to the value of delta.
    :return: Spec
    """
    if delta >= 0:
        return increment(path, delta, xattr=xattr, create_parents=create_parents)
    else:
        return decrement(path, abs(delta), xattr=xattr, create_parents=create_parents)


def increment(path,                   # type: str
              delta,                  # type: int
              xattr=False,            # type: bool
              create_parents=False    # type: bool
              ):
    # type: (...) -> Spec
    """
    Increment a counter in a document.

    :param str path: Path to the counter
    :param int delta: Amount to change the counter.   Cannot be 0, and must be and integer.
    :param xattr: whether this is an xattr path
    :param bool create_parents: Create the counter if it doesn't exist.  Will be initialized
        to the value of delta.
    :return: Spec
    """
    if not isinstance(delta, int):
        raise InvalidArgumentException("Delta must be integer")
    if delta <= 0:
        raise InvalidArgumentException(
            "Delta must be integer greater than or equal to 0")

    return Spec(SubDocOp.COUNTER, path, create_parents, xattr, False, delta)


def decrement(path,                   # type: str
              delta,                  # type: int
              xattr=False,            # type: bool
              create_parents=False    # type: bool
              ):
    # type: (...) -> Spec
    """
    Increment a counter in a document.

    :param str path: Path to the counter
    :param int delta: Amount to change the counter.   Cannot be 0, and must be and integer.
    :param xattr: whether this is an xattr path
    :param bool create_parents: Create the counter if it doesn't exist.  Will be initialized
        to the value of delta.
    :return: Spec
    """
    if not isinstance(delta, int):
        raise InvalidArgumentException("Delta must be integer")
    if delta <= 0:
        raise InvalidArgumentException(
            "Delta must be integer greater than or equal to 0")

    return Spec(SubDocOp.COUNTER, path, create_parents,
                xattr, False, -1 * delta)
