#
# Copyright 2018, Couchbase, Inc.
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

from typing import Callable, Any, Union

from couchbase_core.client import Client
from couchbase_core.analytics import AnalyticsQuery
from couchbase_core import JSON
import uuid
from couchbase.exceptions import InvalidArgumentException

IdGenerator = Callable[[JSON], str]
DataConverter = Callable[[JSON], Any]
Query = Union[AnalyticsQuery, str]


class BucketOperator:
    def __init__(self, verb):
        self.verb = verb

    def __call__(self, bucket, *args, **kwargs):
        return self.verb(bucket, *args, **kwargs)


class BucketOperators:
    INSERT = BucketOperator(lambda x, k, v: x.insert(k, v))
    UPSERT = BucketOperator(lambda x, k, v: x.upsert(k, v))
    REPLACE = BucketOperator(lambda x, k, v: x.replace(k, v))


class AnalyticsIngester:
    id_generator = None  # type: IdGenerator
    data_converter = None  # type: DataConverter
    operation = None  # type: BucketOperator

    def __init__(self, id_generator=None, data_converter=lambda x: x, operation=BucketOperators.UPSERT):
        # type: (IdGenerator, DataConverter, BucketOperator) -> None
        """
        Initialise ingester.

        :param DataConverter data_converter: Single parameter Callable which takes a JSON
            input and returns a transformed JSON output.
        :param IdGenerator id_generator: Callable that takes a JSON input and
            returns an ID string
        :param BucketOperator operation: Callable that takes a bucket object, a key and a
            value and applies the key and value to the bucket (e.g. upsert/insert/replace)
        """
        if not isinstance(operation, BucketOperator):
            raise InvalidArgumentException("Operation is not a BucketOperator")

        if operation == BucketOperators.REPLACE and not id_generator:
            raise InvalidArgumentException("Replace cannot use default ID generator.")

        self.id_generator = id_generator or (lambda x: str(uuid.uuid4()))
        self.data_converter = data_converter
        self.operation = operation

    def __call__(self, bucket, query, host=None, ignore_ingest_error=False, *args, **kwargs):
        # type: (Client, Query, str, bool, *Any, **Any) -> None
        """
        Run an analytics query, pass the results through the data converter, and the results of that
        into the id_generator, then apply the bucket operator to the bucket using the id generator
        result as the key, and the data converter result as the value.

        :param bucket: bucket to run query on
        :param query: analytics query to run
        :param host: host to run it on
        :param ignore_ingest_error: whether to suppress any exceptions raised during processing
        :param args: positional args for analytics query
        :param kwargs: named args for analytics query
        """
        request = bucket.analytics_query(query, host, *args, **kwargs)
        operation = self.operation or getattr(type(bucket), 'upsert', None)
        try:
            for item in request:
                try:
                    converted_data = self.data_converter(item)
                    operation(bucket, self.id_generator(converted_data), converted_data)
                except:
                    if not ignore_ingest_error:
                        raise
        except:
            if not ignore_ingest_error:
                raise
