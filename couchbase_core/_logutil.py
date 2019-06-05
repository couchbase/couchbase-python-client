#
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
#

import logging
LEVEL_MAP = (
    logging.DEBUG, # TRACE
    logging.DEBUG, # DEBUG
    logging.INFO, # INFO
    logging.WARN, # WARN
    logging.ERROR, # Error
    logging.FATAL # Fatal
)


def pylog_log_handler(**kwargs):
    pylevel = LEVEL_MAP[kwargs.pop('level')]
    logger_name = 'couchbase' + '.' + kwargs['subsys']
    msg = '[{id}] {message} (L:{c_src[1]})'.format(**kwargs)
    logging.getLogger(logger_name).log(pylevel, msg)


def configure(val):
    from couchbase_core.bucket import Bucket
    import couchbase_core._libcouchbase
    if val:
        couchbase_core._libcouchbase.lcb_logging(pylog_log_handler)
        initmsg = 'Initializing Couchbase logging. lcb_version={0}'.format(
            Bucket.lcb_version())
        logging.getLogger('couchbase').info(initmsg)
    else:
        couchbase_core._libcouchbase.lcb_logging(None)

