#
# Copyright 2013, Couchbase, Inc.
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

from couchbase_core.asynchronous.bucket import AsyncClientFactory as CoreAsyncBucketFactory
from couchbase_core.asynchronous.view import AsyncViewBase
from couchbase_core.exceptions import ArgumentError
from couchbase_v2.bucket import Bucket
from couchbase_core._pyport import with_metaclass


class AsyncBucket(CoreAsyncBucketFactory.gen_async_client(Bucket)):
    def __init__(self, *args, **kwargs):
        super(AsyncBucket,self).__init__(*args,**kwargs)

    def query(self, *args, **kwargs):
        """
        Reimplemented from base class.

        This method does not add additional functionality of the
        base class' :meth:`~couchbase_v2.bucket.Bucket.query` method (all the
        functionality is encapsulated in the view class anyway). However it
        does require one additional keyword argument

        :param class itercls: A class used for instantiating the view
          object. This should be a subclass of
          :class:`~couchbase_v2.asynchronous.view.AsyncViewBase`.
        """
        itercls=kwargs.get('itercls', None)
        if not issubclass(itercls, AsyncViewBase):
            raise ArgumentError.pyexc("itercls was {} must be defined "
                                      "and must be derived from AsyncViewBase".format(itercls))

        return super(AsyncBucket, self).query(*args, **kwargs)

    def endure(self, key, *args, **kwargs):
        res = super(AsyncBucket, self).endure_multi([key], *args, **kwargs)
        res._set_single()
        return res
