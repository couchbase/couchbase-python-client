from typing import *
from couchbase_core import subdocument as SD
import couchbase_core.priv_constants as _P
from .options import OptionBlockTimeOut
from couchbase_core.subdocument import array_addunique, array_append, array_insert, array_prepend, insert, remove, replace, upsert, counter, Spec

try:
    from abc import abstractmethod
except:
    import abstractmethod


SDType = type(SD)


# noinspection PyPep8Naming
def GetSpec():
    # type: ()-> SDType
    return SD

LookupInSpec=Iterable[Spec]


# noinspection PyPep8Naming
def MutateSpec():
    # type: ()-> SDType
    return SD


def exists(
        path,  # type: str
        xattr=False  # type: bool
):
    # type: (...)->Spec
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
    # type: (...)->Spec
    """
    Fetches an element's value given a path.

    :param str path: String path - path to the element
    :param bool xattr: operation is done on an Extended Attribute.
    :return: Spec
    """
    return SD.get(path,xattr=xattr)


def count(path,  # type: str
                  xattr=False  # type; bool
                  ):
    # type: (...)->Spec
    """
    Gets the count of a list or dictionary element given a path

    :param path: String path - path to the element
    :param bool xattr: operation is done on an Extended Attribute.
    :return: Spec
    """
    return SD.get_count(path)


def get_full():
    # type: (...)->Spec
    """
    Fetches the entire document.

    :return: Spec
    """
    return SD._gen_3spec(_P.SDCMD_GET_FULLDOC,None)


def gen_projection_spec(project):
    return map(SD.get, project)


class SubdocSpecItem(object):
    def __init__(self,
                 path,  # type: str
                 xattr=None,
                 **kwargs):
        self.kwargs = kwargs
        self.xattr = xattr
        self.path = path

    @abstractmethod
    def _as_spec(self):
        pass


SubdocSpec = Iterable[SubdocSpecItem]


class MutateInOptions(OptionBlockTimeOut):
    def __init__(self, *args, **kwargs):
        super(MutateInOptions, self).__init__(*args, **kwargs)


MutateInSpec = Iterable[Spec]


