from typing import *

from boltons.funcutils import wraps

import couchbase_core.subdocument
from .subdoc import SubdocSpecItem
from .result import IResult, ResultPrecursor
from .options import OptionBlockTimeOut

try:
    from abc import abstractmethod
except:
    import abstractmethod


class MutateInOptions(OptionBlockTimeOut):
    def __init__(self, *args, **kwargs):
        super(MutateInOptions, self).__init__(*args, **kwargs)


class MutateInSpecItemBase(SubdocSpecItem):
    def __init__(self,
                 path,  # type: str
                 xattr=None,
                 create_parents=None,
                 **kwargs
                 ):
        # type: (...)->None
        super(MutateInSpecItemBase, self).__init__(path, xattr, **kwargs)
        self.create_parents = create_parents


class MutateReplace(MutateInSpecItemBase):
    def __init__(self, path, value, **kwargs):
        super(MutateReplace, self).__init__(path, **kwargs)
        self.value=value

    def _as_spec(self):
        return couchbase_core.subdocument.replace(self.path, self.value, **self.kwargs)


class MutateInsert(MutateInSpecItemBase):
    def __init__(self, path, value, *args, **kwargs):
        super(MutateInsert, self).__init__(path, *args, **kwargs)
        self.value=value

    def _as_spec(self):
        return couchbase_core.subdocument.insert(self.path, self.value, self.create_parents, **self.kwargs)


MutateInSpec = Iterable[MutateInSpecItemBase]


class MutationToken(object):
    def __init__(self, sequenceNumber,  # type: int
                 vbucketId,  # type: int
                 vbucketUUID  # type: int
                 ):
        self.sequenceNumber = sequenceNumber
        self.vbucketId = vbucketId
        self.vbucketUUID = vbucketUUID

    def partition_id(self):
        # type: (...)->int
        pass

    def partition_uuid(self):
        # type: (...)->int
        pass

    def sequence_number(self):
        # type: (...)->int
        pass

    def bucket_name(self):
        # type: (...)->str
        pass


class SDK2MutationToken(MutationToken):
    def __init__(self, token):
        token=token or (None,None,None)
        super(SDK2MutationToken,self).__init__(token[2],token[0],token[1])
            #LCB_MUTATION_TOKEN_VB(mutinfo),
            #LCB_MUTATION_TOKEN_ID(mutinfo),
            #LCB_MUTATION_TOKEN_SEQ(mutinfo),


class MutationResult(IResult):
    def __init__(self,
                 cas,  # type: int
                 mutation_token=None  # type: MutationToken
                 ):
        super(MutationResult, self).__init__(cas)
        self.mutationToken = mutation_token

    def mutation_token(self):
        # type: () -> MutationToken
        return self.mutationToken


def get_mutation_result(result  # type: ResultPrecursor
                        ):
    # type (...)->MutationResult
    return MutationResult(result.orig_result.cas, SDK2MutationToken(result.orig_result._mutinfo) if hasattr(result.orig_result, '_mutinfo') else None)


SDK2Result=TypeVar('SDK2Result')


def mutation_result(func  # type: Callable[[Any,...],SDK2Result]
                    ):
    # type: (...)->Callable[[Any,...],MutationResult]
    @wraps(func)
    def mutated(*args, **kwargs):
        result = func(*args, **kwargs)
        return get_mutation_result(result)

    mutated.__name__=func.__name__
    mutated.__doc__=func.__doc__
    return mutated


class MutateInResult(object):
    pass