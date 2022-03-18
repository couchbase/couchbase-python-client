from datetime import timedelta

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.diagnostics import ServiceType
from couchbase.exceptions import ParsingFailedException
from couchbase.options import (AnalyticsOptions,
                               ClusterOptions,
                               QueryOptions,
                               UpsertOptions,
                               WaitUntilReadyOptions)

cluster = Cluster("couchbase://127.0.0.1", ClusterOptions(PasswordAuthenticator("Administrator", "password")))
bucket = cluster.bucket("default")

cluster.wait_until_ready(timedelta(seconds=3),
                         WaitUntilReadyOptions(service_types=[ServiceType.KeyValue, ServiceType.Query]))

print(bucket.name)

coll = bucket.default_collection()
r = coll.upsert("foo", {"some": "thing"}, UpsertOptions(timeout=timedelta(seconds=5)))
print(r)
print(r.mutation_token)
r = coll.get("foo")
print(r)

# change this to a bucket that doesn't exist
# should get a KeyspaceNotFound, get a ParsingException...
# n1ql = 'SELECT * FROM `beer-sample` LIMIT 10;'
# try:
#     query = cluster.query(n1ql, QueryOptions(metrics=True))
#     for r in query.rows():
#         print(f'Found row: {r}')
#     metadata = query.metadata()
#     metrics = metadata.metrics()
#     print(metrics)
# except ParsingFailedException as ex:
#     print('Caught parsing failure.')

# n1ql = 'NOT N1QL!'
# try:
#     query = cluster.query(n1ql, QueryOptions(metrics=True))
#     for r in query.rows():
#         print(f'Found row: {r}')
# except ParsingFailedException as ex:
#     print('Caught parsing failure.')

# result = cluster.analytics_query(f'SELECT * FROM `test-dataset` WHERE `type` = $type LIMIT 1',
#                                             AnalyticsOptions(named_parameters={'type': 'airline'}))

# for row in result.rows():
#     print(row)

result = coll.binary().append('bc_tests_utf8', 'foo')

# am = cluster.analytics_indexes()
# empty_dataverse_name = 'empty/dataverse'
# #await am.create_dataverse(empty_dataverse_name)
# test_dataset = 'test-dataset'
# am.create_dataset(test_dataset, 'default')

cluster.close()
