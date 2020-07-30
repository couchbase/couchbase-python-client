from typing import *
from couchbase_core import JSON, subdocument as SD
import couchbase_core.priv_constants as _P
from couchbase.durability import DurabilityOptionBlock
from couchbase_core.subdocument import  Spec
from couchbase.exceptions import InvalidArgumentException

SDType = type(SD)


# noinspection PyPep8Naming
def GetSpec():
    # type: () -> SDType
    return SD


LookupInSpec = Iterable[Spec]
MutateInSpec = Iterable[Spec]


# noinspection PyPep8Naming
def MutateSpec():
    # type: () -> SDType
    return SD


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
    return SD.exists(path,xattr=xattr)


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
    return SD.get(path,xattr=xattr)


def upsert(path,                    # type: str,
           value,                   # type: JSON
           create_parents=False,    # type: bool
           xattr=False              # type: False
           ):
    # type: (...) -> Spec
    """
    Upsert a value into a document at a given path

    :param str path:  Path at which to upsert the value.
    :param JSON value:  Value to upsert.
    :param create_parents: Whether or not to create parents if needed.
    :param xattr: whether this is an xattr path

    :return: Spec
    """
    return SD.upsert(path, value, create_parents, xattr=xattr)


def replace(path,       # type: str
            value,       # type: JSON
            xattr=False # type: bool
            ):
    # type: (...) -> Spec
    """
    Replace value at a path with the value given.

    :param str path:  Path at which to replace the value.
    :param value: Value you would like at the path given.
    :param xattr: whether this is an xattr path
    :return: Spec
    """
    return SD.replace(path, value, xattr=xattr)


def insert(path,                     # type: str
           value,                    # type: JSON
           create_parents=False,     # type: bool
           xattr=False               # type: False
           ):
    # type: (...) -> Spec
    """
    Insert a value at a given path in a document.

    :param str path:  Path to insert into document.
    :param JSON value: Value to insert at this path.
    :param create_parents: Whether or not to create the parents in the path,
        if they don't already exist.
    :param xattr: whether this is an xattr path
    :return: Spec
    """
    return SD.insert(path, value, create_parents, xattr=xattr)


def remove(path,        # type: str
           xattr=False  # type: bool
           ):
    # type: (...) -> Spec
    """
    Remove a path from a document.

    :param str path: Path to remove from document.
    :param xattr: whether this is an xattr path
    :return: Spec
    """
    return SD.remove(path, xattr=xattr)


def array_append(path,                  # type: str
                 *values,               # type: JSON
                 xattr=False,           # type: bool
                 **kwargs               # type: Any
                 ):
    # type: (...) -> Spec
    """
    Append values to the end of an array in a document.

    :param str path: Path to an array in document.  Note this is the path *to*
        the array, not to the path to a specific element.
    :param JSON values: Value(s) to append to the array.
    :param xattr: whether this is an xattr path
    :param Any kwargs: Specify create_parents=True to create the array.
    :return: Spec
    """
    return SD.array_append(path, *values, xattr=xattr, **kwargs)


def array_prepend(path,             # type: str
                  *values,          # type: JSON
                  xattr=False,      # type: bool
                  **kwargs          # type: Any
                  ):
    # type: (...) -> Spec
    """
    Append values to the beginning of an array in a document.

    :param str path: Path to an array in document.  Note this is the path *to*
        the array, not to the path to a specific element.
    :param JSON values: Value(s) to prepend to the array.
    :param xattr: whether this is an xattr path
    :param Any kwargs: Specify create_parents=True to create the array.
    :return:
    """
    return SD.array_prepend(path, *values, xattr=xattr, **kwargs)


def array_insert(path,          # type: str
                 xattr=False,   # type: bool
                 *values        # type: JSON
                ):
    # type: (...) -> Spec
    """
    Insert values at into an array in a document at the position
    given in the path.

    :param str path: Path to the spot in the array where the values
        should be inserted.  Note in this case, the path is a path
        to a specific location in an array.
    :param xattr: whether this is an xattr path
    :param values: Value(s) to insert.
    :return: Spec
    """
    return SD.array_insert(path, *values, xattr=xattr)


def array_addunique(path,                   # type: str
                    value,                  # type Union[str|int|float|bool]
                    xattr=False,            # type: bool
                    create_parents=False    # type: bool
                    ):
    # type: (...) -> Spec
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
    return SD.array_addunique(path, value, xattr=xattr, create_parents=create_parents)


def counter(path,                   # type: str
            delta,                  # type: int
            xattr=False,            # type: bool
            create_parents=False    # type: bool
            ):
    # type: (...) -> Spec
    """
    Increment or decrement a counter in a document.

    :param str path: Path to the counter
    :param int delta: Amount to change the counter.   Cannot be 0, and must be and integer.
    :param xattr: whether this is an xattr path
    :param bool create_parents: Create the counter if it doesn't exist.  Will be initialized
        to the value of delta.
    :return: Spec
    """
    return SD.counter(path, delta, xattr=xattr, create_parents=create_parents)


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
    return SD.get_count(path, xattr=xattr)


def get_full():
    # type: (...) -> Spec
    """
    Fetches the entire document.

    :return: Spec
    """
    return SD._gen_3spec(_P.SDCMD_GET_FULLDOC, "")


def with_expiry():
    # type: (...) -> Spec
    """
    Fetches the expiry from the xattrs of the doc

    :return: Spec
    """
    return SD.get('$document.exptime', xattr=True)


def validate_project(project):
    if not project:
        return
    if isinstance(project, str):
        raise InvalidArgumentException("project must be an array of paths")
    if hasattr(project, "__getitem__") or hasattr(project, "__iter__"):
        return
    raise InvalidArgumentException("project must be an Iterable[str]")


# maximum number of specs we support
MAX_SPECS = 16


# used by Collection to do projections, only.  Note the janky
# hack logic will get refined in the near future as part of a
# revisit of the design.
def gen_projection_spec(project, with_exp=False):
    def generate(path):
        if path is None:
            return with_expiry()
        if path:
            return SD.get(path)
        return get_full()

    validate_project(project)
    # empty string = with_expiry
    # None = get_full
    if not project:
        project = [""]
    if with_exp:
        project = [None] + project

    if len(project) > MAX_SPECS:
        raise InvalidArgumentException(
            "Project only accepts {} operations or less".format(MAX_SPECS))

    return map(generate, project)


class MutateInOptions(DurabilityOptionBlock):
    def __init__(self, *args, **kwargs):
        super(MutateInOptions, self).__init__(*args, **kwargs)

