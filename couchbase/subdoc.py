from abc import abstractmethod
from typing import *
from couchbase_core import subdocument as SD

SDType = type(SD)


# noinspection PyPep8Naming
def GetSpec():
    # type: ()-> SDType
    return SD


# noinspection PyPep8Naming
def LookupInSpec():
    # type: ()-> SDType
    return SD


# noinspection PyPep8Naming
def MutateSpec():
    # type: ()-> SDType
    return SD


def GetOperation(path,  # type: str
                 xattr=False  # type: bool
                 ):
        return SD.get(path,xattr=xattr)


def GetFullDocumentOperation(path,  # type: str
                 xattr=False  # type: bool
                 ):
    return SD.get(path,xattr=xattr)

def ExistsOperation(
                 path,  # type: str
                 xattr=False  # type: bool
                 ):
        return SD.exists(path,xattr=xattr)


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


def spec_to_SDK2(wrapped  # type: SubdocSpec
                ):
    return tuple(x._as_spec() for x in wrapped)