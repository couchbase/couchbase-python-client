#  Copyright 2016-2024. Couchbase, Inc.
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

"""Keyspace names and handler source for the eventing function management suites.

Api-independent: names and JavaScript, no SDK call anywhere, so both environment modules and
both suite trees read the same values.

**The suites provision their own bucket.**  A keyspace the suite does not create is an
environment prerequisite stated nowhere: on a cluster where nobody set it up the suites do not
skip, they fail inside the first upsert with a manager error.  Creating the bucket puts the
prerequisite in code, and dropping it afterwards leaves the cluster as it was found.

**One bucket per suite, not two.**  Eventing requires the metadata keyspace to differ from the
source keyspace, which two buckets satisfy and so, measured on 7.6.10, does a metadata keyspace
naming a *collection* of the source bucket, upsert through deploy through undeploy through
drop.  So each suite takes one bucket and puts its metadata in a named collection inside it.

**The two suites take different buckets**, and the reason is xdist rather than eventing.
Under the default `--dist load` two classes can be spread across workers and their
class-scoped fixtures overlap in wall-clock time; one bucket name shared between them would
then be created by one suite and dropped by the other mid-run.  Everything here is also
worker-suffixed, one level up from that, because an eventing function created through the
*cluster* manager is cluster-global: two workers running the same test would otherwise
upsert and drop one shared function underneath each other.  Fixed names for either would make
the suites unsafe to run in parallel with themselves.
"""

from __future__ import annotations

import os

EVENTING_BUCKET_NAME = 'test-eventing'
SCOPE_EVENTING_BUCKET_NAME = 'test-scope-eventing'

# One scope holding two collections, provisioned inside whichever bucket the suite owns.
# The cluster-level suite streams the bucket itself and uses `evt-meta` for metadata, with
# `evt-src` there for the one test that names a scope- and collection-qualified source.  The
# scope-level suite manages functions *through* `evt-scope` and both of its keyspaces live in
# it, which is what makes those functions addressable through that scope and nowhere else.
# Metadata and source sharing a scope is fine; what eventing requires is that the two
# keyspaces differ, measured on 7.6.10 through deploy and undeploy.
FUNCTION_SCOPE_NAME = 'evt-scope'
SOURCE_COLLECTION_NAME = 'evt-src'
META_COLLECTION_NAME = 'evt-meta'

# A handler that compiles, deploys, and does nothing observable.  Nothing in either suite
# writes to the source keyspace, so OnUpdate and OnDelete are never called; all this has to
# be is JavaScript the eventing service accepts.
SIMPLE_EVT_CODE = ('function OnUpdate(doc, meta) {\n    log("Doc created/updated", meta.id);\n}'
                   '\n\nfunction OnDelete(meta, options) {\n    log("Doc deleted/expired", meta.id);\n}')

# A handler the service rejects at upsert: `func` is not a keyword and OnDelete is missing.
INVALID_EVT_CODE = 'func OnUpdate(doc, meta) {\n    log("Doc created/updated", meta.id);\n}\n\n'

BUCKET_BINDING_ALIAS = 'evtFunc'
CONSTANT_BINDING_ALIASES = ('testConstant', 'testConstant1')
CONSTANT_BINDING_LITERALS = ('1234', '"another test value"')

# The four URL bindings test_url_bindings sends, one per authentication type.  The hostnames
# are never contacted: a binding is stored with the function definition and resolved only
# when the handler calls through it, which nothing here does.
URL_BINDING_HOSTNAMES = ('http://localhost:5000',
                         'http://localhost:5001',
                         'http://localhost:5002',
                         'http://localhost:5003')
URL_BINDING_ALIASES = ('urlBinding1', 'urlBinding2', 'urlBinding3', 'urlBinding4')
URL_BINDING_USERNAME = 'username'
URL_BINDING_PASSWORD = 'password'  # nosec
URL_BINDING_BEARER_KEY = 'IThinkTheBearerTokenIsSupposedToGoHere'  # nosec

MISSING_FUNCTION_NAME = 'not-a-function'


def _worker_suffix() -> str:
    """`-gw0` under xdist, empty otherwise.

    Read from the environment rather than from cb_env because the suites name these before an
    environment exists.
    """
    worker_id = os.environ.get('PYTEST_XDIST_WORKER', '')
    return f'-{worker_id}' if worker_id else ''


def eventing_bucket_name() -> str:
    """The bucket the cluster-level suite creates and drops, one per xdist worker."""
    return f'{EVENTING_BUCKET_NAME}{_worker_suffix()}'


def scope_eventing_bucket_name() -> str:
    """The bucket the scope-level suite creates and drops, one per xdist worker."""
    return f'{SCOPE_EVENTING_BUCKET_NAME}{_worker_suffix()}'


def function_name(*parts: str) -> str:
    """A worker-unique function name, distinguished from its siblings by *parts*."""
    tail = ''.join(f'-{part}' for part in parts if part)
    return f'test-evt-func{tail}{_worker_suffix()}'


def function_version(server_version_full: str) -> str:
    """The `evt-<server version>` string the server stamps its own functions with."""
    return 'evt-{}'.format(server_version_full.replace('enterprise', 'ee').replace('community', 'ce'))
