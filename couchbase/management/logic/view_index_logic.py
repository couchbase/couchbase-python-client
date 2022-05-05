#  Copyright 2016-2022. Couchbase, Inc.
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

import json
from enum import Enum
from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Iterable,
                    Optional)

from couchbase.exceptions import (DesignDocumentNotFoundException,
                                  InvalidArgumentException,
                                  RateLimitedException)
from couchbase.options import forward_args
from couchbase.pycbc_core import (management_operation,
                                  mgmt_operations,
                                  view_index_mgmt_operations)

if TYPE_CHECKING:
    from couchbase.management.options import (DropDesignDocumentOptions,
                                              GetAllDesignDocumentsOptions,
                                              GetDesignDocumentOptions,
                                              UpsertDesignDocumentOptions)


class ViewIndexManagerLogic:

    _ERROR_MAPPING = {r'not_found': DesignDocumentNotFoundException,
                      r'.*Limit\(s\) exceeded\s+\[.*\].*': RateLimitedException}

    def __init__(self, connection, bucket_name):
        self._connection = connection
        self._bucket_name = bucket_name

    def get_design_document(self,
                            design_doc_name,  # type: str
                            namespace,  # type: DesignDocumentNamespace
                            *options,   # type: GetDesignDocumentOptions
                            **kwargs
                            ) -> Optional[DesignDocument]:
        if not design_doc_name:
            raise InvalidArgumentException("Expected design document name to not be None")

        if not namespace:
            raise InvalidArgumentException("Expected design document namespace to not be None")

        final_args = forward_args(kwargs, *options)
        op_args = {
            'bucket_name': self._bucket_name,
            'document_name': design_doc_name,
            'namespace': namespace.to_str()
        }

        if final_args.get("client_context_id", None) is not None:
            op_args["client_context_id"] = final_args.get("client_context_id")

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.VIEW_INDEX.value,
            "op_type": view_index_mgmt_operations.GET_INDEX.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def get_all_design_documents(self,
                                 namespace,     # type: DesignDocumentNamespace
                                 *options,      # type: GetAllDesignDocumentsOptions
                                 **kwargs) -> Optional[Iterable[DesignDocument]]:
        if not namespace:
            raise InvalidArgumentException("Expected design document namespace to not be None")

        final_args = forward_args(kwargs, *options)
        op_args = {
            'bucket_name': self._bucket_name,
            'namespace': namespace.to_str()
        }

        if final_args.get("client_context_id", None) is not None:
            op_args["client_context_id"] = final_args.get("client_context_id")

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.VIEW_INDEX.value,
            "op_type": view_index_mgmt_operations.GET_ALL_INDEXES.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def upsert_design_document(self,
                               design_doc_data,     # type: DesignDocument
                               namespace,           # type: DesignDocumentNamespace
                               *options,            # type: UpsertDesignDocumentOptions
                               **kwargs) -> None:
        if not design_doc_data:
            raise InvalidArgumentException("Expected design document to not be None")

        if not namespace:
            raise InvalidArgumentException("Expected design document namespace to not be None")

        final_args = forward_args(kwargs, *options)
        op_args = {
            'bucket_name': self._bucket_name,
            'design_docucment': design_doc_data.as_dict(namespace)
        }

        if final_args.get("client_context_id", None) is not None:
            op_args["client_context_id"] = final_args.get("client_context_id")

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.VIEW_INDEX.value,
            "op_type": view_index_mgmt_operations.UPSERT_INDEX.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)

    def drop_design_document(self,
                             design_doc_name,   # type: str
                             namespace,         # type: DesignDocumentNamespace
                             *options,          # type: DropDesignDocumentOptions
                             **kwargs) -> None:
        if not design_doc_name:
            raise InvalidArgumentException("Expected design document name to not be None")

        if not namespace:
            raise InvalidArgumentException("Expected design document namespace to not be None")

        final_args = forward_args(kwargs, *options)
        op_args = {
            'bucket_name': self._bucket_name,
            'document_name': design_doc_name,
            'namespace': namespace.to_str()
        }

        if final_args.get("client_context_id", None) is not None:
            op_args["client_context_id"] = final_args.get("client_context_id")

        mgmt_kwargs = {
            "conn": self._connection,
            "mgmt_op": mgmt_operations.VIEW_INDEX.value,
            "op_type": view_index_mgmt_operations.DROP_INDEX.value,
            "op_args": op_args
        }

        callback = kwargs.pop('callback', None)
        if callback:
            mgmt_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            mgmt_kwargs['errback'] = errback

        if final_args.get("timeout", None) is not None:
            mgmt_kwargs["timeout"] = final_args.get("timeout")

        return management_operation(**mgmt_kwargs)


class DesignDocumentNamespace(Enum):
    PRODUCTION = False
    DEVELOPMENT = True

    # def prefix(self, ddocname):
    #     if ddocname.startswith('dev_') or not self.value:
    #         return ddocname
    #     return 'dev_' + ddocname

    def to_str(self):
        return 'development' if self.value else 'production'

    @classmethod
    def unprefix(cls, name):
        for prefix in ('_design/', 'dev_'):
            name = name[name.startswith(prefix) and len(prefix):]
        return name

    @classmethod
    def from_str(cls, value):
        if value == 'production':
            return cls.PRODUCTION
        else:
            return cls.DEVELOPMENT


class View:
    def __init__(self,
                 map,           # type: str
                 reduce=None    # type: Optional[str]
                 ) -> None:
        self._map = map
        self._reduce = reduce

    @property
    def map(self) -> str:
        return self._map

    @property
    def reduce(self) -> Optional[str]:
        return self._reduce

    def as_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in {"map": self._map,
                                  "reduce": self._reduce}.items() if v}

    def to_json(self) -> str:
        return json.dumps(self.as_dict())

    @classmethod
    def from_json(cls, json_view) -> View:
        return cls(json.loads(json_view))


class DesignDocument(object):
    def __init__(self,
                 name,      # type: str
                 views,      # Dict[str, View]
                 namespace=None,  # type: Optional[DesignDocumentNamespace]
                 rev=None       # type: Optional[str]
                 ) -> None:
        self._name = DesignDocumentNamespace.unprefix(name)
        self._views = views
        self._rev = rev
        self._namespace = namespace

    @property
    def name(self) -> str:
        return self._name

    @property
    def views(self) -> Dict[str, View]:
        return self._views

    @property
    def rev(self) -> Optional[str]:
        return self._rev

    @property
    def namespace(self) -> Optional[DesignDocumentNamespace]:
        return self._namespace

    def as_dict(self,
                namespace  # type: DesignDocumentNamespace
                ) -> Dict[str, Any]:
        output = {
            'name': self._name
        }
        if namespace is not None:
            output['namespace'] = namespace.to_str()
        output['views'] = dict({key: value.as_dict() for key, value in self.views.items()})

        if self.rev:
            output['rev'] = self.rev

        return output

    def add_view(self,
                 name,  # type: str
                 view   # type: View
                 ) -> DesignDocument:
        self.views[name] = view
        return self

    def get_view(self,
                 name   # type: str
                 ) -> View:
        return self._views.get(name, None)

    @classmethod
    def from_json(cls, raw_json  # type: Dict[str, Any]
                  ) -> DesignDocument:
        name = raw_json.get('name')
        rev = raw_json.get('rev', None)
        ns = DesignDocumentNamespace.from_str(raw_json.get('namespace', None))
        views = raw_json.get('views', dict())
        views = dict({key: View(**value) for key, value in views.items()})
        return cls(name, views, namespace=ns, rev=rev)

    def __repr__(self):
        output = self.as_dict(self.namespace)
        return f'DesignDocument({output})'
