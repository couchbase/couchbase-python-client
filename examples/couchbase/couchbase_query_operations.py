import uuid

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import ParsingFailedException
from couchbase.mutation_state import MutationState
from couchbase.n1ql import QueryScanConsistency
from couchbase.options import ClusterOptions, QueryOptions


def main():
    cluster = Cluster.connect('couchbase://localhost',
                              ClusterOptions(PasswordAuthenticator('Administrator', 'password')))
    bucket = cluster.bucket("travel-sample")
    collection = bucket.default_collection()

    # basic query
    try:
        result = cluster.query(
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
    result = cluster.query(q_str, "hotel")
    rows = [r for r in result]

    # positional params via QueryOptions
    result = cluster.query(q_str, QueryOptions(positional_parameters=["hotel"]))
    rows = [r for r in result]

    # named params
    q_str = "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`=$doc_type LIMIT 10"
    result = cluster.query(q_str, doc_type='hotel')
    rows = [r for r in result]

    # name params via QueryOptions
    result = cluster.query(q_str, QueryOptions(named_parameters={'doc_type': 'hotel'}))
    rows = [r for r in result]

    # iterate over result/rows
    q_str = "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`='airline' LIMIT 10"
    result = cluster.query(q_str)

    # iterate over rows
    for row in result:
        # each row is an serialized JSON object
        name = row["name"]
        callsign = row["callsign"]
        print(f'Airline name: {name}, callsign: {callsign}')

    # query metrics
    result = cluster.query("SELECT 1=1", QueryOptions(metrics=True))
    # ignore results
    result.execute()

    metadata = result.metadata()
    metrics = metadata.metrics() if metadata is not None else None
    if metrics is not None:
        print("Execution time: {}".format(metrics.execution_time()))

    # print scan consistency
    result = cluster.query(
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

    res = collection.upsert("airline_{}".format(new_airline["id"]), new_airline)

    ms = MutationState(res)

    result = cluster.query(
        "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`='airline' LIMIT 10",
        QueryOptions(consistent_with=ms))
    rows = [r for r in result]

    # client context id
    result = cluster.query(
        "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`='hotel' LIMIT 10",
        QueryOptions(client_context_id="user-44{}".format(uuid.uuid4())))
    rows = [r for r in result]

    # read only
    result = cluster.query(
        "SELECT ts.* FROM `travel-sample` ts WHERE ts.`type`='hotel' LIMIT 10",
        QueryOptions(read_only=True))
    rows = [r for r in result]

    agent_scope = bucket.scope("inventory")

    result = agent_scope.query(
        "SELECT a.* FROM `airline` a WHERE a.country=$country LIMIT 10",
        country='France')
    rows = [r for r in result]


if __name__ == "__main__":
    main()
