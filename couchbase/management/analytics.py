# -*- coding:utf-8 -*-
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

from datetime import timedelta
from typing import *

from couchbase.management.generic import GenericManager
from couchbase.options import OptionBlockTimeOut, forward_args
from couchbase.analytics import (AnalyticsLinkType, AnalyticsOptions,
                                 AnalyticsResult, AnalyticsDataset, AnalyticsIndex,
                                 AnalyticsLink, CouchbaseRemoteAnalyticsLink,
                                 S3ExternalAnalyticsLink, AzureBlobExternalAnalyticsLink)
import couchbase_core._libcouchbase as _LCB
from couchbase_core import ulp, mk_formstr
from couchbase.exceptions import (CouchbaseException, DataverseNotFoundException, NotSupportedException,
                                  InvalidArgumentException, ErrorMapper, HTTPException,
                                  AnalyticsLinkExistsException, AnalyticsLinkNotFoundException)
from couchbase.management.admin import Admin, METHMAP


class BaseAnalyticsIndexManagerOptions(OptionBlockTimeOut):
    # valid AnalyticsOptions keys
    OPTION_KEYS = ["timeout", "readonly", "scan_consistency", "client_context_id",  "priority",
                   "positional_parameters", "named_parameters", "raw"]

    def to_analytics_options(self, **kwargs):
        final_opts = {**self, **kwargs}
        return AnalyticsOptions(**{k: v for k, v in final_opts.items() if k in self.OPTION_KEYS})


class GetPendingMutationsOptions(BaseAnalyticsIndexManagerOptions):
    pass


class DisconnectLinkOptions(BaseAnalyticsIndexManagerOptions):
    @overload
    def __init__(self,
                 timeout=None,              # type: timedelta
                 dataverse_name='Default',  # type: str
                 link_name='Local'          # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        super(DisconnectLinkOptions, self).__init__(**kwargs)

    @property
    def dataverse_name(self):
        return self.get('dataverse_name', 'Default')

    @property
    def link_name(self):
        return self.get("link_name", 'Local')


class ConnectLinkOptions(BaseAnalyticsIndexManagerOptions):
    @overload
    def __init__(self,
                 timeout=None,              # type: timedelta
                 dataverse_name='Default',  # type: str
                 link_name='Local',         # type: str
                 force=False                # type: bool
                 ):
        pass

    def __init__(self, **kwargs):
        super(ConnectLinkOptions, self).__init__(**kwargs)

    @property
    def dataverse_name(self):
        return self.get('dataverse_name', 'Default')

    @property
    def link_name(self):
        return self.get('link_name', "Local")

    @property
    def force(self):
        return self.get("force", False)


class DropAnalyticsIndexOptions(BaseAnalyticsIndexManagerOptions):
    @overload
    def __init__(self,
                 timeout=None,                  # type: timedelta
                 ignore_if_not_exists=False,    # type: bool
                 dataverse_name='Default'       # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        super(DropAnalyticsIndexOptions, self).__init__(**kwargs)

    @property
    def dataverse_name(self):
        return self.get('dataverse_name', 'Default')

    @property
    def ignore_if_not_exists(self):
        return self.get('ignore_if_not_exists', False)


class GetAllAnalyticsIndexesOptions(BaseAnalyticsIndexManagerOptions):
    pass


class CreateAnalyticsIndexOptions(BaseAnalyticsIndexManagerOptions):
    @overload
    def __init__(self,
                 timeout=None,              # type: timedelta
                 ignore_if_exists=False,    # type: bool
                 dataverse_name='Default',  # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        super(CreateAnalyticsIndexOptions, self).__init__(**kwargs)

    @property
    def ignore_if_exists(self):
        return self.get('ignore_if_exists', False)

    @property
    def dataverse_name(self):
        return self.get('dataverse_name', 'Default')


class DropDatasetOptions(BaseAnalyticsIndexManagerOptions):
    @overload
    def __init__(self,
                 timeout=None,                  # type: timedelta
                 ignore_if_not_exists=False,    # type: bool
                 dataverse_name=None,           # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        super(DropDatasetOptions, self).__init__(**kwargs)

    @property
    def ignore_if_not_exists(self):
        return self.get('ignore_if_not_exists', False)

    @property
    def dataverse_name(self):
        return self.get('dataverse_name', 'Default')


class GetAllDatasetsOptions(BaseAnalyticsIndexManagerOptions):
    pass


class CreateDataverseOptions(BaseAnalyticsIndexManagerOptions):
    @overload
    def __init__(self,
                 timeout=None,              # type: timedelta
                 ignore_if_exists=False     # type: bool
                 ):
        pass

    def __init__(self, **kwargs):
        super(CreateDataverseOptions, self).__init__(**kwargs)

    @property
    def ignore_if_exists(self):
        return self.get('ignore_if_exists', False)


class DropDataverseOptions(BaseAnalyticsIndexManagerOptions):
    @overload
    def __init__(self,
                 timeout=None,                  # type: timedelta
                 ignore_if_not_exists=False     # type: bool
                 ):
        pass

    def __init__(self, **kwargs):
        super(DropDataverseOptions, self).__init__(**kwargs)

    @property
    def ignore_if_not_exists(self):
        return self.get('ignore_if_not_exists', False)


class CreateDatasetOptions(BaseAnalyticsIndexManagerOptions):
    @overload
    def __init__(self,
                 timeout=None,                  # type: timedelta
                 ignore_if_exists=False,        # type: bool
                 condition=None,                # type: str
                 dataverse_name='Default'       # type: str
                 ):
        pass

    def __init__(self, **kwargs):
        super(CreateDatasetOptions, self).__init__(**kwargs)

    @property
    def ignore_if_exists(self):
        # type: (...) -> bool
        return self.get('ignore_if_exists', False)

    @property
    def condition(self):
        # type: (...) -> str
        return self.get('condition', "")

    @property
    def dataverse_name(self):
        # type: (...) -> str
        return self.get('dataverse_name', 'Default')


class CreateLinkAnalyticsOptions(OptionBlockTimeOut):
    pass


class ReplaceLinkAnalyticsOptions(OptionBlockTimeOut):
    pass


class DropLinkAnalyticsOptions(OptionBlockTimeOut):
    pass


class GetLinksAnalyticsOptions(OptionBlockTimeOut):
    @overload
    def __init__(self,
                 timeout=None,          # type: timedelta
                 dataverse_name=None,   # type: str
                 name=None,             # type: str
                 link_type=None,        # type: AnalyticsLinkType
                 ):
        pass

    def __init__(self, **kwargs):
        super(GetLinksAnalyticsOptions, self).__init__(**kwargs)

    @property
    def dataverse_name(self):
        # type: (...) -> str
        return self.get('dataverse_name', None)

    @property
    def name(self):
        # type: (...) -> str
        return self.get('name', None)

    @property
    def link_type(self):
        # type: (...) -> AnalyticsLinkType
        return self.get('link_type', None)


class AnalyticsIndexErrorHandler(ErrorMapper):
    @staticmethod
    def mapping():
        # type (...)->Mapping[str, CBErrorType]
        return {HTTPException: {'24055.*already exists': AnalyticsLinkExistsException,
                                '24006.*does not exist': AnalyticsLinkNotFoundException,
                                '24034.*Cannot find': DataverseNotFoundException}}


class AnalyticsIndexManager(GenericManager):
    def __init__(self,         # type: "AnalyticsIndexManager"
                 cluster,       # type: "Cluster"
                 admin_bucket  # type: "Admin"
                 ):
        """Analytics Manager

        :param admin_bucket: Admin bucket
        """
        super(AnalyticsIndexManager, self).__init__(admin_bucket)
        self._cluster = cluster

    @staticmethod
    def _to_analytics_options(option    # type: BaseAnalyticsIndexManagerOptions
                              ):
        return option.to_analytics_options() if option else AnalyticsOptions()

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
            type=_LCB.LCB_HTTP_TYPE_ANALYTICS,
            path=kwargs['path'],
            method=imeth,
            content_type=kwargs.get(
                'content_type', 'application/x-www-form-urlencoded'),
            post_data=kwargs.get('content', None),
            response_format=_LCB.FMT_JSON,
            timeout=kwargs.get('timeout', None))

    def _scrub_dataverse_name(self, dataverse_name):
        tokens = dataverse_name.split("/")
        return "`" + "`.`".join(tokens) + "`"

    def create_dataverse(self,
                         dataverse_name,    # type: str
                         options=None,      # type: CreateDataverseOptions
                         **kwargs
                         ):
        # type: (...) -> None
        """

        :param str dataverse_name: Name of the dataverse to create.
        :param CreateDataverseOptions options: Options for dataverse creation.
        :param Any kwargs: Override corresponding value in options.
        :return: None
        :raises DataverseAlreadyExistsException
        :raises InvalidArgumentsException
        :raises CouchbaseException
        """
        if not options:
            options = CreateDataverseOptions()
        ignore = options.ignore_if_exists or kwargs.get(
            "ignore_if_exists", False)
        if_not_exists_clause = "IF NOT EXISTS"
        query = "CREATE DATAVERSE {} {};".format(self._scrub_dataverse_name(
            dataverse_name), if_not_exists_clause if ignore else "")
        #print("create_dataverse query: {}".format(query))
        self._cluster.analytics_query(
            query, AnalyticsIndexManager._to_analytics_options(options)).rows()

    def drop_dataverse(self,
                       dataverse_name,  # type: str
                       options=None,    # type: DropDataverseOptions
                       **kwargs
                       ):
        # type: (...) -> None
        if not options:
            options = DropDataverseOptions()
        ignore = options.ignore_if_not_exists or kwargs.get(
            "ignore_if_not_exists", False)
        if_exists_clause = "IF EXISTS"
        query = "DROP DATAVERSE {} {};".format(self._scrub_dataverse_name(
            dataverse_name), if_exists_clause if ignore else "")
        #print("drop dataverse query: {}".format(query))
        self._cluster.analytics_query(
            query, AnalyticsIndexManager._to_analytics_options(options)).rows()

    def create_dataset(self,
                       dataset_name,    # type: str
                       bucket_name,     # type: str
                       options=None,    # type: CreateDatasetOptions
                       **kwargs):
        # type: (...) -> None
        if not options:
            options = CreateDatasetOptions()
        ignore = kwargs.get('ignore_if_exists', options.ignore_if_exists)
        dataverse_name = kwargs.get('dataverse_name', options.dataverse_name)
        if_not_exists_clause = "IF NOT EXISTS"
        where_clause = kwargs.get('condition', options.condition)
        if where_clause:
            where_clause = "WHERE {}".format(where_clause)
        query = "USE {}; CREATE DATASET {} `{}` ON `{}` {};" .format(self._scrub_dataverse_name(dataverse_name),
                                                                     if_not_exists_clause if ignore else "",
                                                                     dataset_name,
                                                                     bucket_name,
                                                                     where_clause,
                                                                     )
        #print("create_dataset n1ql: {}".format(query))
        self._cluster.analytics_query(
            query, AnalyticsIndexManager._to_analytics_options(options)).rows()

    def drop_dataset(self,
                     dataset_name,  # type: str
                     options=None,  # type: DropDatasetOptions
                     **kwargs
                     ):
        # type: (...) -> None
        """
        Drop a dataset.
        :param str dataset_name: Name of dataset to drop.
        :param DropDatasetOptions options: Options for the drop request.
        :param Any kwargs: Override corresponding value in options.
        :return: None
        """
        if not options:
            options = DropDatasetOptions()
        dataverse_name = kwargs.get('dataverse_name', options.dataverse_name)
        ignore = kwargs.get('ignore_if_not_exists',
                            options.ignore_if_not_exists)
        if_exists_clause = ""
        if ignore:
            if_exists_clause = "IF EXISTS"

        query = "USE {}; DROP DATASET `{}` {};".format(
            self._scrub_dataverse_name(dataverse_name), dataset_name, if_exists_clause)
        self._cluster.analytics_query(
            query, options.to_analytics_options()).rows()

    def get_all_datasets(self,
                         options=None   # type: GetAllDatasetsOptions
                         ):
        # type: (...) -> Iterable[AnalyticsDataset]
        """
        Get all the datasets in the cluster.  Note we don't return the Metadata dataset, but we do return the Default
        dataset.
        :return Iterable[AnalyticsDataset]: The datasets, in an iterable.
        """
        if not options:
            options = GetAllDatasetsOptions()
        query = 'SELECT d.* FROM Metadata.`Dataset` d WHERE d.DataverseName <> "Metadata"'
        result = self._cluster.analytics_query(
            query, options.to_analytics_options())
        return_val = []
        for r in result.rows():
            return_val.append(AnalyticsDataset(**r))
        return return_val

    def create_index(self,
                     index_name,    # type: str
                     dataset_name,  # type: str
                     fields,        # type:Dict[str, AnalyticsDataType]
                     options=None,  # type: CreateAnalyticsIndexOptions
                     **kwargs
                     ):
        # type: (...) -> None
        """
        Create Index on a dataset, over a set of fields.

        :param str index_name: Name for the index
        :param str dataset_name: Name of dataset to use for the index
        :param Dict[str, str] fields: Dict containing the name of the field (as the key) and the type of the
        field (as the value)
        :param CreateAnalyticsOptions options: Options for creating the index.
        :param kwargs: Override corresponding value in options.
        :return: None
        """
        if not options:
            options = CreateAnalyticsIndexOptions()
        ignore = kwargs.get('ignore_if_exists', options.ignore_if_exists)
        dataverse_name = kwargs.get('dataverse_name', options.dataverse_name)
        if_not_exists_clause = ''
        if ignore:
            if_not_exists_clause = "IF NOT EXISTS"

        fields_clause = ", "
        fields_clause = fields_clause.join(
            ["{}: {}".format(k, v.value) for k, v in fields.items()])
        statement = "CREATE INDEX `{}` {} ON {}.`{}` ({});".format(index_name, if_not_exists_clause, self._scrub_dataverse_name(dataverse_name),
                                                                   dataset_name, fields_clause)
        #print("create index statement: {}".format(statement))
        self._cluster.analytics_query(
            statement, options.to_analytics_options()).rows()

    def drop_index(self,
                   index_name,      # type: str
                   dataset_name,    # type: str
                   options=None,    # type: DropAnalyticsIndexOptions
                   **kwargs):
        # type: (...) -> None
        """
        Drop specified index.
        :param str index_name: Name of index to drop.
        :param str dataset_name: Name of the dataset this index was created on.
        :param DropAnalyticsIndexOptions options: Options for dropping index.
        :param Any kwargs: Override corresponding value in options.
        :return: None.
        """
        if not options:
            options = DropAnalyticsIndexOptions()
        dataverse_name = kwargs.get('dataverse_name', options.dataverse_name)
        ignore = kwargs.get('ignore_if_not_exists',
                            options.ignore_if_not_exists)
        if_exists_clause = ""
        if ignore:
            if_exists_clause = "IF EXISTS"
        statement = 'DROP INDEX {} {}.`{}`.`{}`'.format(
            if_exists_clause, self._scrub_dataverse_name(dataverse_name), dataset_name, index_name)
        self._cluster.analytics_query(
            statement, options.to_analytics_options(**kwargs)).rows()

    def get_all_indexes(self,
                        options=None,   # type: GetAllAnalyticsIndexesOptions
                        **kwargs
                        ):
        # (...) -> Iterable[AnalyticsIndex]
        """
        Get all analytics indexes in the cluster.
        :param GetAllAnalyticsIndexesOptions options: Options for getting all analytics indexes.
        :param Any kwargs: Override corresponding value in options.
        :return:
        """
        if not options:
            options = GetAllAnalyticsIndexesOptions()
        statement = 'SELECT * FROM Metadata.`Index` WHERE DataverseName <> "Metadata";'
        result = self._cluster.analytics_query(
            statement, options.to_analytics_options())
        return_val = []
        for r in result.rows():
            return_val.append(AnalyticsIndex(**r))
        return return_val

    def connect_link(self,
                     options=None,  # type: ConnectLinkOptions
                     **kwargs
                     ):
        # type: (...) -> None
        """
        Connect a link.
        :param ConnectLinkOptions options: Options to connect a link.
        :param Any kwargs: Override corresponding value in options.
        :return:
        """
        if not options:
            options = ConnectLinkOptions()

        dataverse_name = kwargs.get('dataverse_name', options.dataverse_name)
        link_name = kwargs.get('link_name', options.link_name)
        force = kwargs.get('force', options.force)
        force_clause = ""
        if force:
            force_clause = "WITH force: true"
        statement = 'USE {}; CONNECT LINK {} {};'.format(
            self._scrub_dataverse_name(dataverse_name), link_name, force_clause)
        self._cluster.analytics_query(
            statement, options.to_analytics_options(**kwargs)).rows()

    def disconnect_link(self,
                        options=None,   # type: DisconnectLinkOptions
                        **kwargs
                        ):
        # type: (...) -> None
        """
        Disconnect a link.
        :param DisconnectLinkOptions options: Options to disconnect a link.
        :param Any kwargs: Override corresponding value in options.
        :return:
        """
        if not options:
            options = DisconnectLinkOptions()

        dataverse_name = kwargs.get('dataverse_name', options.dataverse_name)
        link_name = kwargs.get('link_name', options.link_name)
        statement = 'USE {}; DISCONNECT LINK {};'.format(
            self._scrub_dataverse_name(dataverse_name), link_name)
        self._cluster.analytics_query(
            statement, options.to_analytics_options(**kwargs)).rows()

    def get_pending_mutations(self,
                              options=None,     # type: GetPendingMutationsOptions
                              **kwargs
                              ):
        # type: (...) -> Dict[string, int]
        if not options:
            options = GetPendingMutationsOptions()

        try:
            return self._cluster._admin._http_request(type=_LCB.LCB_HTTP_TYPE_ANALYTICS,
                                                      method=_LCB.LCB_HTTP_METHOD_GET,
                                                      path="analytics/node/agg/stats/remaining"
                                                      ).value
        except CouchbaseException as e:
            extra = getattr(e, 'objextra', None)
            if extra:
                if int(getattr(extra, 'http_status', None)) == 404:
                    raise NotSupportedException(
                        "get pending mutations not supported")
            raise e

    @AnalyticsIndexErrorHandler.mgmt_exc_wrap
    def create_link(
        self,  # type: "AnalyticsIndexManager"
        link,  # type: "AnalyticsLink"
        *options,     # type: CreateLinkAnalyticsOptions
        **kwargs
    ):
        """Creates a new analytics link

        :param link: the link to create
        :param options: CreateLinkAnalyticsOptions to create a link.
        :param kwargs: Override corresponding value in options.

        :raises: AnalyticsLinkExistsException
        :raises: DataverseNotFoundException
        :raises: InvalidArgumentException
        """

        link.validate()

        if "/" in link.dataverse_name():
            path = "/analytics/link/{}/{}".format(
                ulp.quote(link.dataverse_name(), safe=''), link.name())
        else:
            path = "/analytics/link"

        self._http_request(
            path=path,
            method="POST",
            content=link.form_encode(),
            **forward_args(kwargs, *options))

    @AnalyticsIndexErrorHandler.mgmt_exc_wrap
    def replace_link(
        self,  # type: "AnalyticsIndexManager"
        link,  # type: "AnalyticsLink"
        *options,     # type: ReplaceLinkAnalyticsOptions
        **kwargs
    ):
        """Replaces an existing analytics link

        :param link: the link to replace
        :param options: CreateLinkAnalyticsOptions to create a link.
        :param kwargs: Override corresponding value in options.

        :raises: AnalyticsLinkNotFoundException
        :raises: DataverseNotFoundException
        :raises: InvalidArgumentException
        """

        link.validate()

        if "/" in link.dataverse_name():
            path = "/analytics/link/{}/{}".format(
                ulp.quote(link.dataverse_name(), safe=''), link.name())
        else:
            path = "/analytics/link"

        self._http_request(
            path=path,
            method="PUT",
            content=link.form_encode(),
            **forward_args(kwargs, *options))

    @AnalyticsIndexErrorHandler.mgmt_exc_wrap
    def drop_link(
        self,  # type: "AnalyticsIndexManager"
        link_name,  # type: str
        dataverse_name,     # type: str
        *options,  # type: DropLinkAnalyticsOptions
        **kwargs
    ):
        """Drops an existing analytics link from provided dataverse

        :param link_name: The name of the link to drop
        :param dataverse_name: The name of the dataverse in which the link belongs
        :param options: DropLinkAnalyticsOptions to create a link.
        :param kwargs: Override corresponding value in options.

        :raises: AnalyticsLinkNotFoundException
        :raises: DataverseNotFoundException
        """
        content = None
        if "/" in dataverse_name:
            path = "/analytics/link/{}/{}".format(
                ulp.quote(dataverse_name, safe=''), link_name)
        else:
            path = "/analytics/link"
            content = mk_formstr({
                "dataverse": dataverse_name,
                "name": link_name,
            })

        self._http_request(
            path=path,
            method="DELETE",
            content=content,
            **forward_args(kwargs, *options))

    @AnalyticsIndexErrorHandler.mgmt_exc_wrap
    def get_links(
        self,  # type: "AnalyticsIndexManager"
        *options,  # type: GetLinksAnalyticsOptions
        **kwargs
    ) -> List[AnalyticsLink]:
        """Gets existing analytics links

        :param options: GetLinksAnalyticsOptions to create a link.
        :param kwargs: Override corresponding value in options.

        :raises: DataverseNotFoundException
        :raises: InvalidArgumentException
        """

        path = "analytics/link"

        final_args = forward_args(kwargs, *options)
        link_type = final_args.pop("link_type", None)
        link_name = final_args.pop("name", None)
        dataverse_name = final_args.pop("dataverse_name", None)

        if dataverse_name is not None:
            if "/" in dataverse_name:
                path += "/{}".format(ulp.quote(dataverse_name, safe=''))
                if link_name is not None:
                    path += "/{}".format(link_name)
            else:
                path += "?dataverse={}".format(dataverse_name)
                if link_name is not None:
                    path += "&name={}".format(link_name)

        else:
            if link_name is not None:
                raise InvalidArgumentException(
                    "Both the link name and the dataverse name must be set.")

        if link_type is not None:
            path += "?type={}".format(link_type.value)

        links = self._http_request(
            path=path,
            method="GET",
            **final_args).value

        analytics_links = []
        for link in links:
            if link["type"] == AnalyticsLinkType.CouchbaseRemote.value:
                analytics_links.append(
                    CouchbaseRemoteAnalyticsLink.link_from_server_json(link))
            elif link["type"] == AnalyticsLinkType.S3External.value:
                analytics_links.append(
                    S3ExternalAnalyticsLink.link_from_server_json(link))
            if link["type"] == AnalyticsLinkType.AzureBlobExternal.value:
                analytics_links.append(
                    AzureBlobExternalAnalyticsLink.link_from_server_json(link))

        return analytics_links
