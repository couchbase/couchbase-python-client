from couchbase_core.bucket import Bucket as CoreBucket
from .collection import Collection as SDK3Collection, CollectionOptions, Collection
from .options import OptionBlock, forward_args
from .result import *


class BucketOptions(OptionBlock):
    pass


class IViewResult(IResult):
    def __init__(self, sdk2_result # type: couchbase_core.ViewResult
                ):
        pass


class ViewOptions(object):
    pass


class Bucket(object):
    _bucket=None  # type: CoreBucket

    @overload
    def __init__(self,
                 connection_string,  # type: str
                 name=None,
                 *options  # type: BucketOptions
                 ):
        # type: (...)->None
        pass

    def __init__(self,
                 connection_string,  # type: str
                 name=None,
                 *options,
                 **kwargs
                ):
        # type: (...)->None
        self._name=name
        self._bucket=CoreBucket(connection_string, **forward_args(kwargs, *options))

    @property
    def name(self):
        # type: (...)->str
        return self._name
    from .collection import Scope

    def scope(self,
              scope_name  # type: str
              ):
        # type: (...)->Scope
        from couchbase import Scope

        return Scope(self, scope_name)

    def default_collection(self,
                           options=None  # type: CollectionOptions
                           ):
        # type: (...)->Collection
        return Collection(self)

    def collection(self,
                   collection_name,  # type: str
                   options=None  # type: CollectionOptions
                   ):
        # type: (...)->Collection
        return Collection(self, collection_name)

    def view_query(self,
                   design_doc,  # type: str
                   view_name,  # type: str
                   *view_options # type: ViewOptions
                   ):
        # type: (...)->IViewResult
        cb=self._bucket # type: CoreBucket
        res=cb.query(design_doc, view_name, **forward_args(None,*view_options))
        return IViewResult(res)

    def views(self):
        # type: (...)->IViewManager
        pass

    def ping(self,
             options=None  # type: PingOptions
             ):
        # type: (...)->IPingResult
        pass