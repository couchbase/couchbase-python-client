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

import re
from enum import Enum

from attr import ib as attrib, s as attrs, asdict
from attr.validators import instance_of as io, deep_mapping as dm

from couchbase.management.generic import GenericManager
from couchbase.options import Duration, timeout_forward_args as forward_args
from couchbase_core import JSON
from couchbase_core._pyport import *
from couchbase_core.bucketmanager import BucketManager
from couchbase_core.client import Client
from couchbase_core.exceptions import HTTPError, ErrorMapper, DictMatcher


class DesignDocumentNamespace(Enum):
    PRODUCTION = False
    DEVELOPMENT = True

    def prefix(self, ddocname):
        return Client._mk_devmode(ddocname, self.value)


class DesignDocumentNotFoundException(HTTPError):
    pass


class ViewErrorHandler(ErrorMapper):
    @staticmethod
    def mapping():
        # type (...) -> Mapping[CBErrorType, Mapping[Any,CBErrorType]]
        return {HTTPError:
                    {DictMatcher(error=re.compile('not_found')): DesignDocumentNotFoundException}
                }


@ViewErrorHandler.wrap
class ViewIndexManager(object):
    def __init__(self, corebucket, bucketname):
        self._parent=corebucket
        self._bucketname = bucketname
        self.bm = BucketManager(corebucket, bucketname)

    def get_design_document(self,  # type: ViewIndexManager
                            design_doc_name,  # type: str
                            namespace,  # type: DesignDocumentNamespace
                            timeout = None,  # type: Duration
                            **options):
        # type: (...)->DesignDocument
        """
        Fetches a design document from the server if it exists.

        :param str design_doc_name: the name of the design document.
        :param DesignDocumentNamespace namespace: PRODUCTION if the user is requesting a document from the production namespace
        or DEVELOPMENT if from the development namespace.
        :param options:
        :param Duration timeout: the time allowed for the operation to be terminated. This is controlled by the client.
        :return: An instance of DesignDocument.

        :raises: DesignDocumentNotFoundException
        """

        response = self.bm.design_get(design_doc_name, namespace.value)
        return self._json_to_ddoc(response.value)

    @staticmethod
    def _json_to_ddoc(
            response  # type: JSON
    ):
        # type: (...)->DesignDocument
        return DesignDocument.from_json(**response)

    def get_all_design_documents(self,  # type: ViewIndexManager
                                 namespace,  # type: DesignDocumentNamespace
                                 *options,
                                 **kwargs):
        # type: (...)->Iterable[DesignDocument]
        """
        Fetches all design documents from the server.

        :param DesignDocumentNamespace namespace: indicates whether the user wants to get production documents (PRODUCTION) or development documents (DEVELOPMENT).
        :param Duration timeout: the time allowed for the operation to be terminated. This is controlled by the client.
        :return: An iterable of DesignDocument.
        """

        path = "{bucketname}/ddocs".format(bucketname=self._bucketname)

        response = self.bm.design_list()

        return list(map(lambda x: self._json_to_ddoc(x['doc']['json']), response))

    def upsert_design_document(self,  # type: ViewIndexManager
                               design_doc_data,  # type: DesignDocument
                               namespace,  # type: DesignDocumentNamespace
                               *options,
                               **kwargs):
        # type: (...)->None
        """
        Updates, or inserts, a design document.

        :param DesignDocument design_doc_data: the data to use to create the design document
        :param DesignDocumentNamespace namespace: indicates whether the user wants to upsert the document to the
               production namespace (PRODUCTION) or development namespace (DEVELOPMENT).
        :return:
        """

        self.bm.design_create(design_doc_data.name, design_doc_data.asdict(), namespace.value, True)


    @overload
    def drop_design_document(self,  # type: ViewIndexManager
                             design_doc_name,  # type: str
                             namespace,  # type: DesignDocumentNamespace
                             timeout=None  # type: Duration
                             ):
        pass

    def drop_design_document(self,  # type: ViewIndexManager
                             design_doc_name,  # type: str
                             namespace,  # type: DesignDocumentNamespace
                             *options,
                             **kwargs):
        # type: (...)->None
        """
        Removes a design document.

        :param design_doc_name: the name of the design document.
        :param namespace: indicates whether the name refers to a production document (PRODUCTION) or a development document (DEVELOPMENT).
        :param timeout:  the time allowed for the operation to be terminated. This is controlled by the client.

        :raises: DesignDocumentNotFoundException (http 404)
        :raises: InvalidArgumentsException
        """

        self.bm.design_delete(design_doc_name, namespace.value, True)

    @overload
    def publish_design_document(self,  # type: ViewIndexManager
                                design_doc_name,  # type: str
                                timeout=None,  # type: Duration
                                syncwait=None  # type: Duration
                                ):
        pass

    def publish_design_document(self,  # type: ViewIndexManager
                                design_doc_name,  # type: str
                                **kwargs):

        """
        Publishes a design document. This method is equivalent to getting a document from the development namespace and upserting it to the production namespace.

        :param design_doc_name: the name of the development design document.
        :param timeout:  the time allowed for the operation to be terminated. This is controlled by the client.

        :raises: DesignDocumentNotFoundException (http 404)
        :raises: InvalidArgumentsException
        """
        final_opts = forward_args(kwargs)
        self.bm.design_publish(design_doc_name, **final_opts)

@attrs
class JSONAttrs(Protocol):
    def asdict(self):
        return asdict(self)


@attrs
class View(JSONAttrs):
    name = attrib(validator=io(str))  # type: str
    reduce = attrib(validator=io(str))  # type: str


@attrs
class DesignDocument(JSONAttrs, Protocol):
    name = attrib(validator=io(str))  # type: str
    views = attrib(validator=dm(io(str), io(View), None))  # type: Mapping[str,View]
    language = attrib(default="javascript", validator=io(str))  # type: str
    @classmethod
    def from_json(cls, **kwargs):
        result=DesignDocument("", {}, "")
        for k, v in kwargs.items():
            setattr(result, k, v)
        return result
