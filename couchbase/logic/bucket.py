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

from typing import (TYPE_CHECKING,
                    Any,
                    Dict,
                    Optional)

from couchbase.diagnostics import ServiceType
from couchbase.exceptions import InvalidArgumentException
from couchbase.options import forward_args
from couchbase.pycbc_core import (diagnostics_operation,
                                  open_or_close_bucket,
                                  operations)
from couchbase.result import PingResult
from couchbase.serializer import Serializer
from couchbase.transcoder import Transcoder

if TYPE_CHECKING:
    from couchbase.options import PingOptions


class BucketLogic:
    def __init__(self, cluster, bucket_name):
        self._cluster = cluster
        self._connection = cluster.connection
        self._bucket_name = bucket_name
        self._connected = False

    @property
    def connection(self):
        """
        **INTERNAL**
        """
        return self._connection

    @property
    def default_transcoder(self) -> Optional[Transcoder]:
        return self._cluster.default_transcoder

    @property
    def default_serializer(self) -> Optional[Serializer]:
        return self._cluster.default_serializer

    @property
    def connected(self) -> bool:
        """
            bool: Indicator on if the bucket has been connected or not.
        """
        return self._connected

    @property
    def name(self):
        """
            str: The name of this :class:`~.Bucket` instance.
        """
        return self._bucket_name

    def _open_or_close_bucket(self, open_bucket=True, **kwargs):
        if not self._connection:
            raise RuntimeError("No cluster connection")

        bucket_kwargs = {
            "open_bucket": 1 if open_bucket is True else 0
        }

        callback = kwargs.pop('callback', None)
        if callback:
            bucket_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            bucket_kwargs['errback'] = errback

        return open_or_close_bucket(self._connection, self._bucket_name, **bucket_kwargs)

    def _set_connected(self, value):
        self._connected = value

    def _destroy_connection(self):
        del self._cluster
        del self._connection

    def ping(self,
             *opts,  # type: PingOptions
             **kwargs  # type: Dict[str,Any]
             ) -> Optional[PingResult]:

        ping_kwargs = {
            'conn': self._connection,
            'op_type': operations.PING.value
        }

        callback = kwargs.pop('callback', None)
        if callback:
            ping_kwargs['callback'] = callback

        errback = kwargs.pop('errback', None)
        if errback:
            ping_kwargs['errback'] = errback

        final_args = forward_args(kwargs, *opts)
        service_types = final_args.get("service_types", None)
        if not service_types:
            service_types = list(
                map(lambda st: st.value, [ServiceType(st.value) for st in ServiceType]))

        if not isinstance(service_types, list):
            raise InvalidArgumentException("Service types must be a list/set.")

        service_types = list(map(lambda st: st.value if isinstance(st, ServiceType) else st, service_types))
        final_args["service_types"] = service_types
        # TODO: tracing
        # final_args.pop("span", None)

        ping_kwargs.update(final_args)
        return diagnostics_operation(**ping_kwargs)
