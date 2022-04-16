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
# r = coll.upsert("foo", {"some": "thing"}, UpsertOptions(timeout=timedelta(seconds=5)))
# print(r)
# print(r.mutation_token)
# r = coll.get("foo")
# print(r)
key = 'new_list'
# coll.list_append(key, 1)
# coll.list_prepend(key, 2)
# coll.list_append(key, 3)

# r = coll.list_get(key, 0)
# print(r)
# r = coll.list_remove(key, 0)
# coll.list_set(key, 0, 5)
# res = coll.get(key)
# c = coll.list_size('bad-key')
# print(f'list size: {c}')
# print(res.content_as[list])

# new
cb_list = coll.list(key)
cb_list.append(1)
cb_list.append(2)
cb_list.append(3)
cb_list.append(4)

print(cb_list.index_of(5))

for v in cb_list:
    print(v)

for v in cb_list.for_each():
    print(v)

res = coll.get(key)
print(res.content_as[list])

coll.remove(key)

cluster.close()