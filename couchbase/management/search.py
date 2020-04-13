from ..options import OptionBlockTimeOut, forward_args
from couchbase.management.admin import Admin, METHMAP
from typing import *
from .generic import GenericManager
from couchbase.exceptions import ErrorMapper, HTTPException, NotSupportedException, \
    InvalidArgumentException, SearchIndexNotFoundException
import couchbase_core._libcouchbase as LCB
import json


class SearchIndexErrorHandler(ErrorMapper):
  @staticmethod
  def mapping():
      # type (...)->Mapping[str, CBErrorType]
      return {HTTPException: {'.*index not found': SearchIndexNotFoundException,
                          'Page not found': NotSupportedException}}


@SearchIndexErrorHandler.wrap
class SearchIndexManager(GenericManager):
    def __init__(self, admin_bucket):
        super(SearchIndexManager, self).__init__(admin_bucket)

    def _http_request(self, **kwargs):
        #  TODO: maybe there is a more general way of making this
        # call?  Ponder
        # the kwargs can override the defaults
        imeth = None
        method = kwargs.get('method', 'GET')
        if not method in METHMAP:
            raise InvalidArgumentException("Unknown HTTP Method", method)

        imeth = METHMAP[method]
        return self._admin_bucket._http_request(
            type=LCB.LCB_HTTP_TYPE_SEARCH,
            path=kwargs['path'],
            method=imeth,
            content_type=kwargs.get('content_type', 'application/json'),
            post_data=kwargs.get('content', None),
            response_format=LCB.FMT_JSON,
            timeout=kwargs.get('timeout', None))

    def get_index(self,       # type: SearchIndexManager
                  index_name, # type: str
                  *options,   # type: GetSearchIndexOptions
                  **kwargs    # type: Any
                  ):
        # type: (...)-> SearchIndex
        """
        Fetches an index from the server if it exists.

        param str index_name: Name of the index to get.
        param GetSearchIndexOptions: options to use when getting index.
        param Any kwargs: overrides corresponding value in options

        :return: a :class:`~.SearchIndex` object.
        :raise:
          :exc:`~.SearchIndexNotFoundException`
            if the index was not found.
          :exc:`~.InvalidArgumentsException`
            if the arguments were not understood
          :exc:`~.CouchbaseException`
            for various server errors
        Uri
        GET http://localhost:8094/api/index/<name>"""
        if not index_name:
            raise InvalidArgumentException("expected index_name to not be empty")

        return SearchIndex.from_server(
            **self._http_request(
                path="api/index/{}".format(index_name),
                method='GET',
                **forward_args(kwargs, *options)).value["indexDef"]
        )

    def get_all_indexes(self,     # type: SearchIndexManager
                        *options, # type: GetAllIndexesOptions
                        **kwargs  # type: Any
                        ):
        # type: (...) -> list[SearchIndex]
        """
        Get all indexes
        :param GetAllIndexesOptions options: options for getting all indexes
        :param Any kwargs: override corresponding value in options
        :return: A list of :class: `~.SearchIndex` objects
        :raise:
      :exc: `~.CouchbaseException`
            for various server errors
        Uri
        GET http://localhost:8094/api/index
        """
        result = self._http_request(path='api/index', **forward_args(kwargs, *options)).value
        retval = list()
        for r in result["indexDefs"]["indexDefs"].values():
            retval.append(SearchIndex.from_server(**r))
        return retval

    def upsert_index(self,      # type: SearchIndexManager
                     index,     # type: SearchIndex
                     *options,  # type: UpsertSearchIndexOptions
                     **kwargs   # type: Any
                     ):
        # type: (...) -> None
        """
        Creates or updates an index.
        :param SearchIndex index: Index to upsert.
        :param UpsertSearchIndexOptions options: options to upsert index.
        :param Any kwargs: override corresponding value in options
        :raise:
          :exc: `~.InvalidArgumentsException`
            if the arguments were invalid
          :exc: `~.CouchbaseException`
            for various server errors
        Uri
        PUT http://localhost:8094/api/index/<index_name>"""
        if not index:
            raise InvalidArgumentException("expected index to not be None")
        else:
            if not index.is_valid():
                raise InvalidArgumentException("Index must have name, source set")
        try:
            self._http_request(
                path="api/index/{}".format(index.name),
                method="PUT",
                content=json.dumps(index.as_dict()),
                **forward_args(kwargs, *options))
        except HTTPException as h:
            error = getattr(getattr(h, 'objextra', None), 'value', {}).get('error', "")
            if not "index with the same name already exists" in error:
                raise

    def drop_index(self,       # type: SearchIndexManager
                   index_name, # type: str
                   *options,   # type: DropSearchIndexOptions
                   **kwargs    # type: Any
                   ):
        # type: (...)  -> None
        """
        Drop an index.
        :param str index_name: Name of index to drop.
        :param DropSearchIndexOptions options: options for dropping index.
        :param Any kwargs: override corresponding option in options
        :raise:
          :exc: `~.InvalidArgumentsException`
            if the arguments were invalid
          :exc: `~.CouchbaseException`
            for various server errors

        Uri
        DELETE http://localhost:8094/api/index/{index_name}
        """
        if not index_name:
            raise InvalidArgumentException("expected an index_name")

        self._http_request(
            path="api/index/{}".format(index_name),
            method='DELETE',
            **forward_args(kwargs, *options))

    def get_indexed_documents_count(self,       # type: SearchIndexManager
                                    index_name, # type: str
                                    *options,   # type: GetSearchIndexedDocumentsCountOptions
                                    **kwargs    # type: Any
                                    ):
        """
        Get a count of the documents indexed by the given index.

        :param str index_name: Name of index to get indexed document count.
        :param GetIndexedDocumentsSearchIndexOptions options: Options for geting the indexed document count.
        :param Any kwargs: Override corresponding value in options.
        :return: A :class: `int`, the count of documents indexed by the index.
        :raise:
          :exc: `~.InvalidArgumentsException`
            if the arguments were invalid
          :exc: `~.CouchbaseException`
            for various server errors
        Uri
        GET http://localhost:8094/api/index/{index_name}/count
        """
        if not index_name:
            raise InvalidArgumentException("expected an index_name")

        return self._http_request(
            path="api/index/{}/count".format(index_name),
            **forward_args(kwargs, *options)).value['count']

    def pause_ingest(self,  # type: SearchIndexManager
                     index_name,  # type: str
                     *options,  # type: PauseIngestSearchIndexOptions
                     **kwargs  # type: Any
                     ):
        """
        Pause the ingestion of documents for an index.
        :param str index_name: Name of index to pause.
        :param PauseIngestSearchIndexOptions options: Options for pausing ingestion of index.
        :param Any kwargs: Override corresponding value in options.
        :return: None
        :raise:
          :exc: `~.InvalidArgumentsException`
            if the arguments were invalid
          :exc: `~.CouchbaseException`
            for various server errors
        Uri
        POST http://localhost:8094/api/index/{index_name}/ingestControl/pause
        """

        if not index_name:
            raise InvalidArgumentException("expected an index_name")

        self._http_request(
            path="api/index/{}/ingestControl/pause".format(index_name),
            method="POST",
            **forward_args(kwargs, *options))

    def resume_ingest(self,  # type: SearchIndexManager
                      index_name,  # type: str
                      *options,  # type: ResumeIngestSearchIndexOptions
                      **kwargs  # type: Any
                      ):
        """
        Resume the ingestion of documents for an index.
        :param str index_name: Name of index to resume.
        :param ResumeIngestSearchIndexOptions options: Options for resuming ingestion of index.
        :param Any kwargs: Override corresponding value in options.
        :return: None
        :raise:
          :exc: `~.InvalidArgumentsException`
            if the arguments were invalid
          :exc: `~.CouchbaseException`
            for various server errors
        Uri
        POST http://localhost:8094/api/index/{index_name}/ingestControl/resume
        """

        if not index_name:
            raise InvalidArgumentException("expected an index_name")

        self._http_request(
            path="api/index/{}/ingestControl/resume".format(index_name),
            method="POST",
            **forward_args(kwargs, *options))

    def allow_querying(self,  # type: SearchIndexManager
                       index_name,  # type: str
                       *options,  # type: AllowQueryingSearchIndexOptions
                       **kwargs  # type: Any
                       ):
        """
        Allow querying against an index.
        :param str index_name: Name of index to allow querying.
        :param AllowQueryingSearchIndexOptions options: options for allowing querying.
        :param Any kwargs: Override corresponding value in options.
        :return: None
        :raise:
          :exc: `~.SearchIndexNotFoundException`
            if the index doesn't exist
          :exc: `~.InvalidArgumentsException`
            if the arguments were invalid
          :exc: `~.CouchbaseException`
            for various server errors
        Uri
        POST http://localhost:8094/api/index/{index_name}/queryControl/allow
        """
        if not index_name:
            raise InvalidArgumentException("expected an index_name")

        self._http_request(
            path='api/index/{}/queryControl/allow'.format(index_name),
            method='POST',
            **forward_args(kwargs, *options))

    def disallow_querying(self,  # type: SearchIndexManager
                          index_name,  # type: str
                          *options,  # type: DisallowQueryingSearchIndexOptions
                          **kwargs  # type: Any
                          ):
        """
        Allow querying against an index.
        :param str index_name: Name of index to allow querying.
        :param AllowQueryingSearchIndexOptions options: options for allowing querying.
        :param Any kwargs: Override corresponding value in options.
        :return: None
        :raise:
          :exc: `~.SearchIndexNotFoundException`
            if the index doesn't exist
          :exc: `~.InvalidArgumentsException`
            if the arguments were invalid
          :exc: `~.CouchbaseException`
            for various server errors
        Uri
        POST http://localhost:8094/api/index/{index_name}/queryControl/allow
        """
        if not index_name:
            raise InvalidArgumentException("expected an index_name")

        self._http_request(
            path='api/index/{}/queryControl/allow'.format(index_name),
            method='POST',
            **forward_args(kwargs, *options))

    def freeze_plan(self,  # type: SearchIndexManager
                    index_name,  # type: str
                    *options,  # type: FreezePlanSearchIndexOptions
                    **kwargs  # type: Any
                    ):
        """
        Freeze the assignment of index partitions to nodes.
        :param str index_name: Name of index to freeze.
        :param FreezePlanSearchIndexOptions options: Options for freezing index.
        :param kwargs Any: Override corresponding value in options.
        :return: None
        :raise:
          :exc: `~.SearchIndexNotFoundException`
            if the index doesn't exist
          :exc: `~.InvalidArgumentsException`
            if the arguments were invalid
          :exc: `~.CouchbaseException`
            for various server errors
        Uri
        POST http://localhost:8094/api/index/{index_name}/planFreezeControl/freeze
        """
        if not index_name:
            raise InvalidArgumentException("expected an index_name")

        self._http_request(
            path='api/index/{}/planFreezeControl/freeze'.format(index_name),
            method='POST',
            **forward_args(kwargs, *options))

    def unfreeze_plan(self,  # type: SearchIndexManager
                      index_name,  # type: str
                      *options,  # type: UnfreezePlanSearchIndexOptions
                      **kwargs  # type: Any
                      ):
        """
        Unfreeze the assignment of index partitions to nodes.
        :param str index_name: Name of index to freeze.
        :param UnfreezePlanSearchIndexOptions options: Options for freezing index.
        :param kwargs Any: Override corresponding value in options.
        :return: None
        :raise:
          :exc: `~.SearchIndexNotFoundException`
            if the index doesn't exist
          :exc: `~.InvalidArgumentsException`
            if the arguments were invalid
          :exc: `~.CouchbaseException`
            for various server errors
        Uri
        POST http://localhost:8094/api/index/{index_name}/planFreezeControl/unfreeze
        """
        if not index_name:
            raise InvalidArgumentException("expected an index_name")

        self._http_request(
            path='api/index/{}/planFreezeControl/unfreeze'.format(index_name),
            method='POST',
            **forward_args(kwargs, *options))

    def analyze_document(self,  # type: SearchIndexManager
                         index_name,  # type: str
                         document,  # type: Any
                         *options,  # type: AnalyzeDocumentSearchIndexOptions
                         **kwargs  # type: Any
                         ):
        """
        Shows how a given document will be analyzed against a specific index
        :param str index_name: Index to use.
        :param Any document: Document to analyze.
        :param AnalyzeDocumentSearchIndexOptions options: Options for analyzing document.
        :param Any kwargs: Override corresponding value in options.
        :return: dict
        :raise:
          :exc: `~.SearchIndexNotFoundException`
            if the index doesn't exist
          :exc: `~.InvalidArgumentsException`
            if the arguments were invalid
          :exc: `~.CouchbaseException`
            for various server errors
        Uri
        POST http://localhost:8094/api/index/{index_name}/analyzeDoc
        """
        if not index_name:
            raise InvalidArgumentException("expected an index_name")
        if not document:
            raise InvalidArgumentException("expected a document to analyze")
        try:
            jsonDoc = json.dumps(document)
        except:
            raise InvalidArgumentException("cannot convert doc to json to analyze")
        return self._http_request(
            path="api/index/{}/analyzeDoc".format(index_name),
            method='POST',
            content=jsonDoc,
            **forward_args(kwargs, *options)).value


# for now lets just have this be a wrapper over a dict...
class SearchIndex(dict):
    def __init__(self,  # type: SearchIndex
                 name=None,  # type: str
                 idx_type=None,  # type: str
                 source_name=None,  # type: str
                 uuid=None,  # type: str
                 params=None,  # type: dict
                 source_uuid=None,  # type: str
                 source_params=None,  # type: dict
                 source_type=None,  # type: str
                 plan_params=None  # type: dict
                 ):
        # we need to make the keys camelCase...
        self['name'] = name
        self['sourceName'] = source_name
        self['type'] = idx_type if idx_type else 'fulltext-index'
        self['uuid'] = uuid
        self['params'] = params
        self['sourceUUID'] = source_uuid
        self['sourceType'] = source_type if source_type else 'couchbase'
        self['planParams'] = plan_params

    @classmethod
    def from_server(cls,  # type: SearchIndex
                    **raw_info  # type: dict
                    ):
        retval = cls()
        retval.update(raw_info)
        return retval

    def as_dict(self):
        return {k: v for k, v in self.items() if v}

    def is_valid(self):
        idx = self.as_dict()
        return bool(idx['name']) and bool(idx['type']) and bool(idx['sourceType'])

    @property
    def name(self):
        return self['name']

    @property
    def type(self):
        return self['type']

    @property
    def source_name(self):
        return self['sourceName']

    @property
    def uuid(self):
        return self['uuid']

    @property
    def params(self):
        return self['params']

    @property
    def source_uuid(self):
        return self['sourceUUID']

    @property
    def source_params(self):
        return self['sourceParams']

    @property
    def source_type(self):
        return self['sourceType']

    @property
    def plan_params(self):
        return self['planParams']


class GetSearchIndexOptions(OptionBlockTimeOut):
    pass


class GetAllSearchIndexesOptions(OptionBlockTimeOut):
    pass


class UpsertSearchIndexOptions(OptionBlockTimeOut):
    pass


class DropSearchIndexOptions(OptionBlockTimeOut):
    pass


class GetIndexedSearchIndexOptions(OptionBlockTimeOut):
    pass


class PauseIngestSearchIndexOptions(OptionBlockTimeOut):
    pass


class ResumeIngestSearchIndexOptions(OptionBlockTimeOut):
    pass


class AllowQueryingSearchIndexOptions(OptionBlockTimeOut):
    pass


class DisallowQueryingSearchIndexOptions(OptionBlockTimeOut):
    pass


class FreezePlanSearchIndexOptions(OptionBlockTimeOut):
    pass


class UnfreezePlanSearchIndexOptions(OptionBlockTimeOut):
    pass
