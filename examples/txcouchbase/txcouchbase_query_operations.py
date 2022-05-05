import uuid

# this is new with Python SDK 4.0, it needs to be imported prior to
# importing the twisted reactor
import txcouchbase  # nopep8 # isort:skip # noqa: E402, F401

from twisted.internet import defer, reactor

from couchbase.auth import PasswordAuthenticator

# **DEPRECATED**, import ALL options from `couchbase.options`
from couchbase.cluster import (ClusterOptions,
                               QueryOptions,
                               QueryScanConsistency)
from couchbase.exceptions import ParsingFailedException
from couchbase.mutation_state import MutationState
from txcouchbase.cluster import Cluster

# use this to not have deprecation warnings
# from couchbase.options import ClusterOptions, QueryOptions
# from couchbase.n1ql import QueryScanConsistency


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

        metrics = result.metadata().metrics()
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

    print("Execution time: {}".format(
        result.metadata().metrics().execution_time()))

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

    reactor.stop()


if __name__ == "__main__":
    main()
    reactor.run()
