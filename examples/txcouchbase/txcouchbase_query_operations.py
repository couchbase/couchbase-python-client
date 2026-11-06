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

import uuid

# this is new with Python SDK 4.0, it needs to be imported prior to
# importing the twisted reactor
import txcouchbase  # nopep8 # isort:skip # noqa: E402, F401

from twisted.internet import defer, reactor

from txcouchbase.auth import PasswordAuthenticator
from txcouchbase.cluster import Cluster
from txcouchbase.exceptions import ParsingFailedException
from txcouchbase.mutation_state import MutationState
from txcouchbase.n1ql import QueryScanConsistency
from txcouchbase.options import ClusterOptions, QueryOptions


@defer.inlineCallbacks
def main():

    # create a cluster object
    cluster = Cluster('couchbase://localhost',
                      ClusterOptions(PasswordAuthenticator('Administrator', 'password')))

    # @TODO: if connection fails, this will hang
    yield cluster.on_connect()
    # create a bucket object
    bucket = cluster.bucket('travel-sample')
    yield bucket.on_connect()

    collection = bucket.default_collection()

    # basic query
    try:
        result = yield cluster.query(
            "SELECT * FROM `travel-sample` LIMIT 10;", QueryOptions(metrics=True))

        for row in result.rows():
            print(f'Found row: {row}')

        # The metadata, and the metrics in it, arrive once every row has been read.
        metadata = result.metadata()
        metrics = metadata.metrics() if metadata is not None else None
        if metrics is not None:
            print(f'Query execution time: {metrics.execution_time()}')

    except ParsingFailedException as ex:
        import traceback
        traceback.print_exc()

    # positional params
    q_str = "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`=$1 LIMIT 10"
    result = yield cluster.query(q_str, "hotel")
    rows = [r for r in result]

    # positional params via QueryOptions
    result = yield cluster.query(q_str, QueryOptions(positional_parameters=["hotel"]))
    rows = [r for r in result]

    # named params
    q_str = "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`=$doc_type LIMIT 10"
    result = yield cluster.query(q_str, doc_type='hotel')
    rows = [r for r in result]

    # name params via QueryOptions
    result = yield cluster.query(q_str, QueryOptions(named_parameters={'doc_type': 'hotel'}))
    rows = [r for r in result]

    # iterate over result/rows
    q_str = "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`='airline' LIMIT 10"
    result = yield cluster.query(q_str)

    # iterate over rows
    for row in result:
        # each row is an serialized JSON object
        name = row["name"]
        callsign = row["callsign"]
        print(f'Airline name: {name}, callsign: {callsign}')

    # query metrics
    result = yield cluster.query("SELECT 1=1", QueryOptions(metrics=True))
    rows = [r for r in result]

    metadata = result.metadata()
    metrics = metadata.metrics() if metadata is not None else None
    if metrics is not None:
        print("Execution time: {}".format(metrics.execution_time()))

    # print scan consistency
    result = yield cluster.query(
        "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`='airline' LIMIT 10",
        QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS))
    rows = [r for r in result]

    # Read your own writes
    new_airline = {
        "callsign": None,
        "country": "United States",
        "iata": "TX",
        "icao": "TX99",
        "id": 123456789,
        "name": "Howdy Airlines",
        "type": "airline"
    }

    res = yield collection.upsert(
        "airline_{}".format(new_airline["id"]), new_airline)

    ms = MutationState(res)

    result = yield cluster.query(
        "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`='airline' LIMIT 10",
        QueryOptions(consistent_with=ms))
    rows = [r for r in result]

    # client context id
    result = yield cluster.query(
        "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`='hotel' LIMIT 10",
        QueryOptions(client_context_id="user-44{}".format(uuid.uuid4())))
    rows = [r for r in result]

    # read only
    result = yield cluster.query(
        "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`='hotel' LIMIT 10",
        QueryOptions(read_only=True))
    rows = [r for r in result]

    reactor.stop()  # type: ignore[attr-defined]


if __name__ == "__main__":
    main()
    reactor.run()  # type: ignore[attr-defined]
