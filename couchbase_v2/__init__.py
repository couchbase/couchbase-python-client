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

import sys

import couchbase_v2.exceptions_shim as exceptions
sys.modules[__name__ + '.exceptions'] = exceptions

import couchbase_core.n1ql as n1ql
sys.modules[__name__ + '.n1ql'] = n1ql

import couchbase_core.analytics as analytics
sys.modules[__name__ + '.analytics'] = analytics

import couchbase_core.connstr as connstr
sys.modules[__name__ + '.connstr'] = connstr

import couchbase_core.crypto as crypto
sys.modules[__name__ + '.crypto'] = crypto

import couchbase_core.items as items
sys.modules[__name__ + '.items'] = items

import couchbase_core.fulltext as fulltext
sys.modules[__name__ + '.fulltext'] = fulltext

import couchbase_core.experimental as experimental
sys.modules[__name__ + '.experimental'] = experimental

import couchbase_core.result as result
sys.modules[__name__ + '.result'] = result

import couchbase_core.transcoder as transcoder
sys.modules[__name__ + '.transcoder'] = transcoder

import couchbase_core.subdocument as subdocument
sys.modules[__name__ + '.subdocument'] = subdocument

import couchbase_core.user_constants as user_constants
sys.modules[__name__ + '.user_constants'] = user_constants

import couchbase_core.auth_domain as auth_domain
sys.modules[__name__ + '.auth_domain'] = auth_domain

import couchbase_core.mutation_state as mutation_state
sys.modules[__name__ + '.mutation_state'] = mutation_state

import couchbase_core.analytics_ingester as analytics_ingester
sys.modules[__name__ + '.analytics_ingester'] = analytics_ingester
