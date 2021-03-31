#
# Copyright 2019, Couchbase, Inc.
# All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from enum import Enum

from couchbase.options import forward_args
from couchbase_core._pyport import *
from couchbase_core.client import Client
from couchbase.exceptions import HTTPException, ErrorMapper, DictMatcher
from couchbase.options import OptionBlockTimeOut
import couchbase_core._libcouchbase as _LCB
import json


class DesignDocumentNamespace(Enum):
    PRODUCTION = False
    DEVELOPMENT = True

    # TODO: put the _design/ in here too?  Ponder it.
    def prefix(self, ddocname):
        return Client._mk_devmode(ddocname, self.value)

    @classmethod
    def unprefix(cls, name):
        for prefix in ('_design/', 'dev_'):
            name = name[name.startswith(prefix) and len(prefix):]
        return name



class DesignDocumentNotFoundException(HTTPException):
    pass


class ViewErrorHandler(ErrorMapper):
    @staticmethod
    def mapping():
        # type (...) -> Mapping[CBErrorType, Mapping[Any,CBErrorType]]
        return {HTTPException: {'not_found': DesignDocumentNotFoundException}}


class GetDesignDocumentOptions(OptionBlockTimeOut):
    pass


class GetAllDesignDocumentsOptions(OptionBlockTimeOut):
    pass


class UpsertDesignDocumentOptions(OptionBlockTimeOut):
    pass

class DropDesignDocumentOptions(OptionBlockTimeOut):
    pass

class PublishDesignDocumentOptions(OptionBlockTimeOut):
    pass


@ViewErrorHandler.wrap
class ViewIndexManager(object):
    def __init__(self, bucket, admin_bucket, bucketname):
        self._bucketname = bucketname
        self._admin = admin_bucket
        self._bucket = bucket

    def _http_request(self, admin_port=True, **kwargs):
        request_type = _LCB.LCB_HTTP_TYPE_MANAGEMENT if admin_port else _LCB.LCB_HTTP_TYPE_VIEW
        kwargs['type'] = request_type
        kwargs['content_type'] = 'application/json'
        kwargs['response_format'] = _LCB.FMT_JSON
        kwargs['method'] = kwargs.get('method', _LCB.LCB_HTTP_METHOD_GET)
        if admin_port:
            return self._admin._http_request(**kwargs)
        return self._bucket._http_request(**kwargs)

    def get_design_document(self,  # type: ViewIndexManager
                            design_doc_name,  # type: str
                            namespace,  # type: DesignDocumentNamespace
                            *options,   # type: GetDesignDocumentOptions
                            **kwargs
                            ):
        # type: (...)->DesignDocument
        """
        Fetches a design document from the server if it exists.

        :param str design_doc_name: the name of the design document.
        :param DesignDocumentNamespace namespace: PRODUCTION if the user is requesting a document from the production namespace
        or DEVELOPMENT if from the development namespace.
        :param GetDesignDocumentOptions options:  Options to use when requesting design document.
        :param Any kwargs: Override corresponding value in options.
        :return: An instance of DesignDocument.

        :raises: DesignDocumentNotFoundException
        """
        args = forward_args(kwargs, *options)
        name = namespace.prefix(design_doc_name)
        args['path'] = "_design/" + name

        response = self._http_request(False, **args)
        return DesignDocument.from_json(name, **response.value)

    def get_all_design_documents(self,          # type: ViewIndexManager
                                 namespace,     # type: DesignDocumentNamespace
                                 *options,      # type: GetAllDesignDocumentsOptions
                                 **kwargs):
        # type: (...) -> Iterable[DesignDocument]
        """
        Fetches all design documents from the server.

        :param DesignDocumentNamespace namespace: indicates whether the user wants to get production documents (PRODUCTION) or development documents (DEVELOPMENT).
        :param GetAllDesignDocumentsOptions options: Options for get all design documents request.
        :param Any kwargs: Override corresponding value in options.
        :return: An iterable of DesignDocument.
        """

        args = forward_args(kwargs, *options)
        args['path'] = "pools/default/buckets/{bucketname}/ddocs".format(bucketname=self._bucketname)
        response = self._http_request(**args).value

        def matches(row):
            return namespace == DesignDocumentNamespace(row['doc']['meta']['id'].startswith("_design/dev_"))

        rows = [r for r in response['rows'] if matches(r)]
        return list(map(lambda x: DesignDocument.from_json(x['doc']['meta']['id'], **x['doc']['json']), rows))

    def upsert_design_document(self,                # type: ViewIndexManager
                               design_doc_data,     # type: DesignDocument
                               namespace,           # type: DesignDocumentNamespace
                               *options,            # type: UpsertDesignDocumentOptions
                               **kwargs):
        # type: (...) -> None
        """
        Updates, or inserts, a design document.

        :param DesignDocument design_doc_data: the data to use to create the design document
        :param DesignDocumentNamespace namespace: indicates whether the user wants to upsert the document to the
               production namespace (PRODUCTION) or development namespace (DEVELOPMENT).
        :param UpsertDesignDocumentOptions options: Options for request to upsert design doc.
        :param Any kwargs: Override corresponding value in options.
        :return:
        """
        name = namespace.prefix(design_doc_data.name)
        ddoc = json.dumps(design_doc_data.as_dict(namespace))

        args = forward_args(kwargs, *options)
        args['path'] = "_design/{name}".format(name=name, bucketname=self._bucketname)
        args['method'] = _LCB.LCB_HTTP_METHOD_PUT
        args['post_data'] = ddoc
        self._http_request(False, **args)

    def drop_design_document(self,              # type: ViewIndexManager
                             design_doc_name,   # type: str
                             namespace,         # type: DesignDocumentNamespace
                             *options,          # type: DropDesignDocumentOptions
                             **kwargs):
        # type: (...) -> None
        """
        Removes a design document.

        :param str design_doc_name: the name of the design document.
        :param DesignDocumentNamespace namespace: indicates whether the name refers to a production document (PRODUCTION) or a development document (DEVELOPMENT).
        :param DropDesignDocumentOptions options: Options for the drop request.
        :param Any kwargs: Override corresponding value in options.
        :raises: DesignDocumentNotFoundException
        :raises: InvalidArgumentsException
        """
        args = forward_args(kwargs, *options)
        name = namespace.prefix(design_doc_name)
        args['method'] = _LCB.LCB_HTTP_METHOD_DELETE
        args['path'] = "_design/{}".format(name)

        self._http_request(False, **args)

    def publish_design_document(self,               # type: ViewIndexManager
                                design_doc_name,    # type: str
                                *options,           # type: PublishDesignDocumentOptions
                                **kwargs):
        # type: (...) -> None

        """
        Publishes a design document. This method is equivalent to getting a document from the development namespace and upserting it to the production namespace.

        :param design_doc_name: the name of the development design document.
        :param PublishDesignDocumentOptions options: Options for the publish design documents request.
        :param Any kwargs: Override corresponding value in options.
        :raises: DesignDocumentNotFoundException (http 404)
        :raises: InvalidArgumentsException
        """
        # NOTE - we can't use forward args as it will convert the timedelta for the timeout, and then
        # later that will confuse things when the functions we call also call forward args, so we must
        # construct an options block no matter what

        doc = self.get_design_document(design_doc_name, DesignDocumentNamespace.DEVELOPMENT, *options, **kwargs)
        self.upsert_design_document(doc, DesignDocumentNamespace.PRODUCTION, *options, **kwargs)


class View(object):
    def __init__(self,
                 map,           # type: str
                 reduce=None    # type: str
                ):
        # type: (...) -> View
        self._map = map
        self._reduce = reduce

    @property
    def map(self):
        return self._map

    @property
    def reduce(self):
        return self._reduce

    def as_dict(self):
        # type: (...) -> Dict[str, Any]
        return {k: v for k, v in  {"map": self._map, "reduce": self._reduce }.items() if v}

    def to_json(self):
        # type: (...) -> str
        return json.dumps(self.as_dict())

    @classmethod
    def from_json(cls, json_view):
        # type: (...) -> View
        return cls(json.loads(json_view))


class DesignDocument(object):
    def __init__(self,
                 name,      # type: str
                 views      # dict[str, View]
                 ):
        # type: (...) -> DesignDocument
        self._name = DesignDocumentNamespace.unprefix(name)
        self._views = views

    @property
    def name(self):
        # type(...) -> str
        return self._name

    @property
    def views(self):
        # type: (...) -> Dict[str,View]
        return self._views

    def as_dict(self, namespace):
        # type: (...) -> Dict[str, Any]
        return {
            '_id': "_design/{}".format(namespace.prefix(self._name)),
            'language': 'javascript',
            'views': dict({key: value.as_dict() for key, value in self.views.items()})
        }

    def add_view(self,
                 name,  # type: str
                 view   # type: View
                 ):
        # type: (...) -> DesignDocument
        self.views[name] = view
        return self

    def get_view(self,
                 name   # type: str
                 ):
        # type: (...) -> View
        return self._views.get(name, None)

    @classmethod
    def from_json(cls, name, **kwargs):
        # type: (...) -> DesignDocument
        views = kwargs.get('views', dict())
        views = dict({key: View(**value) for key, value in views.items()})
        return cls(name, views)





