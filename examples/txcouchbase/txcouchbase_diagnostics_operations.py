#  Copyright 2016-2026. Couchbase, Inc.
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

from datetime import timedelta

# this is new with Python SDK 4.0, it needs to be imported prior to
# importing the twisted reactor
import txcouchbase  # nopep8 # isort:skip # noqa: E402, F401

from twisted.internet import defer, reactor

from txcouchbase.auth import PasswordAuthenticator
from txcouchbase.cluster import Cluster
from txcouchbase.diagnostics import PingState, ServiceType
from txcouchbase.exceptions import UnAmbiguousTimeoutException
from txcouchbase.options import PingOptions, WaitUntilReadyOptions


@defer.inlineCallbacks
def ok(cluster):
    result = yield cluster.ping()
    for _, reports in result.endpoints.items():
        for report in reports:
            if not report.state == PingState.OK:
                return False
    return True


@defer.inlineCallbacks
def main():
    # create a cluster object
    cluster = Cluster('couchbase://localhost',
                      authenticator=PasswordAuthenticator('Administrator', 'password'))

    # @TODO: if connection fails, this will hang
    yield cluster.on_connect()

    cluster_ready = False
    try:
        yield cluster.wait_until_ready(timedelta(seconds=3),
                                       WaitUntilReadyOptions(service_types=[ServiceType.KeyValue, ServiceType.Query]))
        cluster_ready = True
    except UnAmbiguousTimeoutException as ex:
        print('Cluster not ready in time: {}'.format(ex))

    if cluster_ready is False:
        quit()

    # For Server versions 6.5 or later you do not need to open a bucket here
    bucket = cluster.bucket("beer-sample")
    yield bucket.on_connect()

    ping_result = yield cluster.ping()

    for endpoint, reports in ping_result.endpoints.items():
        for report in reports:
            print(
                "{0}: {1} took {2}".format(
                    endpoint.value,
                    report.remote,
                    report.latency))

    ping_result = yield cluster.ping()
    print(ping_result.as_json())

    cluster_ok = yield ok(cluster)
    print("Cluster is okay? {}".format(cluster_ok))

    ping_result = yield cluster.ping(PingOptions(service_types=[ServiceType.Query]))
    print(ping_result.as_json())

    diag_result = yield cluster.diagnostics()
    print(diag_result.as_json())

    print("Cluster state: {}".format(diag_result.state))

    reactor.stop()  # type: ignore[attr-defined]


if __name__ == "__main__":
    main()
    reactor.run()  # type: ignore[attr-defined]
