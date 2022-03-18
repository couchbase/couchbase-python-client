# this is new with Python SDK 4.0, it needs to be imported prior to
# importing the twisted reactor
from datetime import timedelta

from twisted.internet import defer, reactor

import txcouchbase
from couchbase.auth import PasswordAuthenticator

# **DEPRECATED**, use: from couchbase.options import PingOptions
from couchbase.bucket import PingOptions
from couchbase.diagnostics import PingState, ServiceType
from couchbase.exceptions import UnAmbiguousTimeoutException
from couchbase.options import WaitUntilReadyOptions
from txcouchbase.cluster import Cluster


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

    reactor.stop()


if __name__ == "__main__":
    main()
    reactor.run()
