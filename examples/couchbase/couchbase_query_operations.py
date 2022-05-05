import uuid

from acouchbase.cluster import Cluster, get_event_loop
from couchbase.auth import PasswordAuthenticator

# **DEPRECATED**, import ALL options from `couchbase.options`
from couchbase.cluster import (ClusterOptions,
                               QueryOptions,
                               QueryScanConsistency)
from couchbase.exceptions import ParsingFailedException
from couchbase.mutation_state import MutationState

# use this to not have deprecation warnings
# from couchbase.options import ClusterOptions, QueryOptions
# from couchbase.n1ql import QueryScanConsistency


async def main():
    cluster = await Cluster.connect('couchbase://localhost',
                                    ClusterOptions(PasswordAuthenticator('Administrator', 'password')))
    bucket = cluster.bucket("travel-sample")
    collection = bucket.default_collection()

    # basic query
    try:
        result = cluster.query(
            "SELECT * FROM `travel-sample` LIMIT 10;", QueryOptions(metrics=True))

        async for row in result.rows():
            print(f'Found row: {row}')

        metrics = result.metadata().metrics()
        print(f'Query execution time: {metrics.execution_time()}')

    except ParsingFailedException as ex:
        import traceback
        traceback.print_exc()

    # positional params
    q_str = "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`=$1 LIMIT 10"
    result = cluster.query(q_str, "hotel")
    rows = [r async for r in result]

    # positional params via QueryOptions
    result = cluster.query(q_str, QueryOptions(positional_parameters=["hotel"]))
    rows = [r async for r in result]

    # named params
    q_str = "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`=$doc_type LIMIT 10"
    result = cluster.query(q_str, doc_type='hotel')
    rows = [r async for r in result]

    # name params via QueryOptions
    result = cluster.query(q_str, QueryOptions(named_parameters={'doc_type': 'hotel'}))
    rows = [r async for r in result]

    # iterate over result/rows
    q_str = "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`='airline' LIMIT 10"
    result = cluster.query(q_str)

    # iterate over rows
    async for row in result:
        # each row is an serialized JSON object
        name = row["name"]
        callsign = row["callsign"]
        print(f'Airline name: {name}, callsign: {callsign}')

    # query metrics
    result = cluster.query("SELECT 1=1", QueryOptions(metrics=True))
    await result.execute()

    print("Execution time: {}".format(
        result.metadata().metrics().execution_time()))

    # print scan consistency
    result = cluster.query(
        "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`='airline' LIMIT 10",
        QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS))
    rows = [r async for r in result]

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

    res = await collection.upsert(
        "airline_{}".format(new_airline["id"]), new_airline)

    ms = MutationState(res)

    result = cluster.query(
        "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`='airline' LIMIT 10",
        QueryOptions(consistent_with=ms))
    rows = [r async for r in result]

    # client context id
    result = cluster.query(
        "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`='hotel' LIMIT 10",
        QueryOptions(client_context_id="user-44{}".format(uuid.uuid4())))
    rows = [r async for r in result]

    # read only
    result = cluster.query(
        "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`='hotel' LIMIT 10",
        QueryOptions(read_only=True))
    rows = [r async for r in result]

    agent_scope = bucket.scope("inventory")

    result = agent_scope.query(
        "SELECT a.* FROM `airline` a WHERE a.country=$country LIMIT 10",
        country='France')
    rows = [r async for r in result]

if __name__ == "__main__":
    loop = get_event_loop()
    loop.run_until_complete(main())
