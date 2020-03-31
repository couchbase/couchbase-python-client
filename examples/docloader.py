# Copyright 2015, Couchbase, Inc.
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

# This file attempts to mimic the basic behavior of cbdocloader
from __future__ import print_function
from zipfile import ZipFile
import logging

from time import sleep
from itertools import izip_longest
from os.path import basename

import requests

# Uncomment to use the cffi module (useful with pypy)
# import couchbase_ffi

from couchbase_core.user_constants import FMT_JSON, FMT_BYTES
from couchbase_core.transcoder import Transcoder
from couchbase_v2.bucket import Bucket
from couchbase_v2.exceptions import CouchbaseNetworkException, CouchbaseTransientException


class DocLoader(object):
    def __init__(self, bucket, srcfile, username, password, quota, cluster,
                 create_bucket=False):
        self.bucket = bucket
        self.srcfile = srcfile
        self.username = username
        self.password = password
        self.quota = quota
        self.cluster = cluster
        self.should_create = create_bucket
        self.logger = logging.getLogger('docloader')
        self._retries = {}

        self._zf = ZipFile(self.srcfile, 'r')
        self._client = None
        self._htsess = requests.Session()
        self._htsess.auth = (self.username, self.password)

        if not self.bucket:
            self.bucket = basename(srcfile).replace('.zip', '')

        self.logger.info('Using bucket %s', self.bucket)

    @property
    def cluster_prefix(self):
        return 'http://{0}/pools/default'.format(self.cluster)

    @property
    def bucket_spec(self):
        return 'http://{0}/{1}'.format(self.cluster, self.bucket)

    def prepare_bucket(self):
        """
        Resets and creates the destination bucket (
            only called if --create is true).
        :return:
        """
        self.logger.info('Deleting old bucket first')
        del_url = '{0}/buckets/{1}'.format(self.cluster_prefix, self.bucket)
        r = self._htsess.delete(del_url)

        try:
            r.raise_for_status()
        except:
            self.logger.exception("Couldn't delete bucket")

        cr_url = '{0}/buckets'.format(self.cluster_prefix)
        data = {
            'name': self.bucket,
            'ramQuotaMB': '{0}'.format(self.quota),
            'bucketType': 'couchbase',
            'authType': 'sasl',
            'saslPassword': '',
            'replicaNumber': '0'
        }
        r = self._htsess.post(cr_url, data)
        r.raise_for_status()

    def make_client(self):
        cb = None
        while not cb:
            # Try to connect via HTTP
            try:
                cb = Bucket(self.bucket_spec)
                cb.transcoder = AlwaysJsonTranscoder()
                cb.stats()
                cb.timeout = 7.5
            except CouchbaseNetworkException as e:
                self.logger.exception(
                    'Got error while connecting. Sleeping for a bit')
                sleep(1)

        self._client = cb

    def process_one_batch(self, curnames):
        ret = {}
        for name in curnames:
            if not name:
                continue

            comps = name.split('/')
            if len(comps) != 3:
                continue

            pfx, dirname = comps[0:2]
            if dirname != 'docs':
                self.logger.warn('Skipping {0} (not a document)'.format(name))
                continue
            else:
                docname = comps[-1].replace('.json', '')
                if not docname:
                    self.logger.warn('No document path for {0}'.format(name))
                    continue

            fp = None
            try:
                fp = self._zf.open(name, mode='r')
                ret[docname] = fp.read()
            except:
                self.logger.error("Couldn't load %s (%s)", name, docname)
                raise
            finally:
                if fp:
                    fp.close()
        return ret

    def run_batch(self, kvs):
        if not kvs:
            return

        try:
            self._client.upsert_multi(kvs)
        except CouchbaseTransientException as e:
            self.logger.info('Items have failed. Placing into retry queue: %r', e)
            for k, v in e.all_results.items():
                if not v.success:
                    self._retries[k] = kvs[k]

    def flush_retries(self):
        while self._retries:
            self.logger.info('Retrying %d items', len(self._retries))
            tmp = self._retries
            self._retries = {}
            self.run_batch(tmp)

    def start_load(self):
        docnames = self._zf.namelist()
        self.logger.info('Will load %d docs', len(docnames))

        for curnames in grouper(docnames, 1000):
            self.flush_retries()
            curdocs = self.process_one_batch(curnames)
            self.run_batch(curdocs)

        self.flush_retries()

    def run(self):
        r = self._htsess.get(
            '{0}/buckets/{1}'.format(self.cluster_prefix, self.bucket))

        if self.should_create:
            self.logger.info('Recreating bucket as requested')
            self.prepare_bucket()
        elif r.status_code == 404:
            self.logger.info('Bucket does not exist. Creating')
            self.prepare_bucket()
        elif r.status_code == 200:
            pass
        else:
            r.raise_for_status()

        self.make_client()
        self.start_load()


# The data we want to load is actually bytes, but we want it to be JSON.
# therefore a custom transcoder is needed which blindly sets the appropriate
# flags
class AlwaysJsonTranscoder(Transcoder):
    def encode_value(self, value, flags):
        value, flags = super(AlwaysJsonTranscoder,
                             self).encode_value(value, FMT_BYTES)
        flags = FMT_JSON
        return value, flags


# Direct from Python.org
def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


if __name__ == '__main__':
    import sys
    import couchbase_v2
    from argparse import ArgumentParser

    ap = ArgumentParser()
    ap.add_argument('-f', '--file',
                    help='Path to zip file', required=True)
    ap.add_argument('--force-create',
                    help='Always recreate the bucket', action='store_true')
    ap.add_argument('-n', '--node', help='Address for cluster',
                    default='127.0.0.1:8091')
    ap.add_argument('-u', '--username',
                    help='Administrative username', default='Administrator')
    ap.add_argument('-p', '--password',
                    help='Administrative password', default="123456")
    ap.add_argument('-s', '--size',
                    help='RAM quota size for the bucket if created',
                    type=int, default=100)
    ap.add_argument('-b', '--bucket',
                    help=('Name of destination bucket '
                          '(determined from filename if not provided)'))
    ap.add_argument('-v', '--verbose',
                    help='Verbosity of logging', action='count')

    options = ap.parse_args()
    if options.verbose:
        lvl = logging.DEBUG
    else:
        lvl = logging.INFO

    logging.basicConfig(
        stream=sys.stderr, level=lvl,
        format='%(created)f - %(name)s - %(levelname)s - %(message)s')

    couchbase_v2.enable_logging()
    loader = DocLoader(
        bucket=options.bucket, srcfile=options.file, username=options.username,
        password=options.password, quota=options.size, cluster=options.node,
        create_bucket=options.force_create)
    loader.run()
