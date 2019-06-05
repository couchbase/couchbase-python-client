from couchbase_core._libcouchbase import (
    LCB_SDCMD_REPLACE, LCB_SDCMD_DICT_ADD, LCB_SDCMD_DICT_UPSERT,
    LCB_SDCMD_ARRAY_ADD_FIRST, LCB_SDCMD_ARRAY_ADD_LAST,
    LCB_SDCMD_ARRAY_ADD_UNIQUE, LCB_SDCMD_EXISTS, LCB_SDCMD_GET,
    LCB_SDCMD_COUNTER, LCB_SDCMD_REMOVE, LCB_SDCMD_ARRAY_INSERT
)

import couchbase_core.priv_constants as _P

# Defined in libcouchbase's headers. We don't want to force a hard dependency
# on a later version of libcouchbase just for these flags, especially
# considering that most users will only be using _F_MKDIR_P

_SPECMAP = {}
for k, v in tuple(globals().items()):
    if not k.startswith('LCB_SDCMD_'):
        continue
    k = k.replace('LCB_SDCMD_', '')
    _SPECMAP[v] = k


class Spec(tuple):
    def __new__(cls, *args, **kwargs):
        return super(Spec, cls).__new__(cls, tuple(args))

    def __repr__(self):
        details = []
        details.append(_SPECMAP.get(self[0]))
        details.extend([repr(x) for x in self[1:]])
        return '{0}<{1}>'.format(self.__class__.__name__,
                                 ', '.join(details))


def _gen_3spec(op, path, xattr=False):
    """
    Returns a Spec tuple suitable for passing to the underlying C extension.
    This variant is called for operations that lack an input value.

    :param str path: The path to fetch
    :param bool xattr: Whether this is an extended attribute
    :return: a spec suitable for passing to the underlying C extension
    """
    flags = 0
    if xattr:
        flags |= _P.SDSPEC_F_XATTR
    return Spec(op, path, flags)


def _gen_4spec(op, path, value,
               create_path=False, xattr=False, _expand_macros=False):
    """
    Like `_gen_3spec`, but also accepts a mandatory value as its third argument
    :param bool _expand_macros: Whether macros in the value should be expanded.
        The macros themselves are defined at the server side
    """
    flags = 0
    if create_path:
        flags |= _P.SDSPEC_F_MKDIR_P
    if xattr:
        flags |= _P.SDSPEC_F_XATTR
    if _expand_macros:
        flags |= _P.SDSPEC_F_EXPANDMACROS
    return Spec(op, path, flags, value)


class MultiValue(tuple):
    def __new__(cls, *args, **kwargs):
        return super(MultiValue, cls).__new__(cls, tuple(args))

    def __repr__(self):
        return 'MultiValue({0})'.format(tuple.__repr__(self))


# The following functions return either 2-tuples or 4-tuples for operations
# which are converted into mutation or lookup specifications

def get(path, **kwargs):
    """
    Retrieve the value from the given path. The value is returned in the result.
    Valid only in :cb_bmeth:`lookup_in`

    :param path: The path to retrieve

    .. seealso:: :meth:`exists`
    """
    return _gen_3spec(LCB_SDCMD_GET, path, **kwargs)


def exists(path, **kwargs):
    """
    Check if a given path exists. This is the same as :meth:`get()`,
    but the result will not contain the value.
    Valid only in :cb_bmeth:`lookup_in`

    :param path: The path to check
    """
    return _gen_3spec(LCB_SDCMD_EXISTS, path, **kwargs)


def upsert(path, value, create_parents=False, **kwargs):
    """
    Create or replace a dictionary path.

    :param path: The path to modify
    :param value: The new value for the path. This should be a native Python
        object which can be encoded into JSON (the SDK will do the encoding
        for you).
    :param create_parents: Whether intermediate parents should be created.
        This means creating any additional levels of hierarchy not already
        in the document, for example:

        .. code-block:: python

            {'foo': {}}

        Without `create_parents`, an operation such as

        .. code-block:: python

            cb.mutate_in("docid", SD.upsert("foo.bar.baz", "newValue"))

        would fail with :cb_exc:`SubdocPathNotFoundError` because `foo.bar`
        does not exist. However when using the `create_parents` option, the
        server creates the new `foo.bar` dictionary and then inserts the
        `baz` value.

    """
    return _gen_4spec(LCB_SDCMD_DICT_UPSERT, path, value,
                      create_path=create_parents, **kwargs)


def replace(path, value, **kwargs):
    """
    Replace an existing path. This works on any valid path if the path already
    exists. Valid only in :cb_bmeth:`mutate_in`

    :param path: The path to replace
    :param value: The new value
    """
    return _gen_4spec(LCB_SDCMD_REPLACE, path, value,
                      create_path=False, **kwargs)


def insert(path, value, create_parents=False, **kwargs):
    """
    Create a new path in the document. The final path element points to a
    dictionary key that should be created. Valid only in :cb_bmeth:`mutate_in`

    :param path: The path to create
    :param value: Value for the path
    :param create_parents: Whether intermediate parents should be created
    """
    return _gen_4spec(LCB_SDCMD_DICT_ADD, path, value,
                      create_path=create_parents, **kwargs)


def array_append(path, *values, **kwargs):
    """
    Add new values to the end of an array.

    :param path: Path to the array. The path should contain the *array itself*
        and not an element *within* the array
    :param values: one or more values to append
    :param create_parents: Create the array if it does not exist

    .. note::

        Specifying multiple values in `values` is more than just syntactical
        sugar. It allows the server to insert the values as one single unit.
        If you have multiple values to append to the same array, ensure they
        are specified as multiple arguments to `array_append` rather than
        multiple `array_append` commands to :cb_bmeth:`mutate_in`

    This operation is only valid in :cb_bmeth:`mutate_in`.

    .. seealso:: :func:`array_prepend`, :func:`upsert`
    """
    return _gen_4spec(LCB_SDCMD_ARRAY_ADD_LAST, path,
                      MultiValue(*values),
                      create_path=kwargs.pop('create_parents', False),
                      **kwargs)


def array_prepend(path, *values, **kwargs):
    """
    Add new values to the beginning of an array.

    :param path: Path to the array. The path should contain the *array itself*
        and not an element *within* the array
    :param values: one or more values to append
    :param create_parents: Create the array if it does not exist

    This operation is only valid in :cb_bmeth:`mutate_in`.

    .. seealso:: :func:`array_append`, :func:`upsert`
    """
    return _gen_4spec(LCB_SDCMD_ARRAY_ADD_FIRST, path,
                      MultiValue(*values),
                      create_path=kwargs.pop('create_parents', False),
                      **kwargs)


def array_insert(path, *values, **kwargs):
    """
    Insert items at a given position within an array.

    :param path: The path indicating where the item should be placed. The path
        _should_ contain the desired position
    :param values: Values to insert

    This operation is only valid in :cb_bmeth:`mutate_in`.

    .. seealso:: :func:`array_prepend`, :func:`upsert`
    """
    return _gen_4spec(LCB_SDCMD_ARRAY_INSERT, path,
                      MultiValue(*values), **kwargs)


def array_addunique(path, value, create_parents=False, **kwargs):
    """
    Add a new value to an array if the value does not exist.

    :param path: The path to the array
    :param value: Value to add to the array if it does not exist.
        Currently the value is restricted to primitives: strings, numbers,
        booleans, and `None` values.
    :param create_parents: Create the array if it does not exist

    .. note::

        The actual position of the new item is unspecified. This means
        it may be at the beginning, end, or middle of the existing
        array)

    This operation is only valid in :cb_bmeth:`mutate_in`.

    .. seealso:: :func:`array_append`, :func:`upsert`
    """
    return _gen_4spec(LCB_SDCMD_ARRAY_ADD_UNIQUE, path, value,
                      create_path=create_parents, **kwargs)


def counter(path, delta, create_parents=False, **kwargs):
    """
    Increment or decrement a counter in a document.

    :param path: Path to the counter
    :param delta: Amount by which to modify the value. The delta
        can be negative but not 0. It must be an integer (not a float)
        as well.
    :param create_parents: Create the counter (and apply the modification) if it
        does not exist

    .. note::

        Unlike :meth:`couchbase_core.bucket.Bucket.counter`,
        there is no `initial` argument. If the counter does not exist
        within the document (but its parent does, or `create_parents`
        is true), it will be initialized with the value of the `delta`.

    This operation is only valid in :cb_bmeth:`mutate_in`.

    .. seealso:: :func:`upsert`, :cb_bmeth:`counter` (in `Bucket`)
    """
    if not delta:
        raise ValueError("Delta must be positive or negative!")
    return _gen_4spec(LCB_SDCMD_COUNTER, path, delta,
                      create_path=create_parents, **kwargs)


def remove(path, **kwargs):
    """
    Remove an existing path in the document.

    This operation is only valid in :cb_bmeth:`mutate_in`.

    :param path: The path to remove
    """
    return _gen_3spec(LCB_SDCMD_REMOVE, path, **kwargs)


def get_count(path, **kwargs):
    """
    Return the number of items in an dictionary or array
    :param path: Path to the dictionary or array to count

    This operation is only valid in :cb_bmeth:`lookup_in`

    .. versionadded:: 2.2.5
    """
    return _gen_3spec(_P.SDCMD_GET_COUNT, path, **kwargs)

