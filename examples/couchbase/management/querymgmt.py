from datetime import timedelta

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import (QueryIndexAlreadyExistsException,
                                  QueryIndexNotFoundException,
                                  WatchQueryIndexTimeoutException)

# **DEPRECATED**, import ALL management options from `couchbase.management.options`
# from couchbase.management.queries import (CreatePrimaryQueryIndexOptions,
#                                           CreateQueryIndexOptions,
#                                           DropQueryIndexOptions,
#                                           WatchQueryIndexOptions)
from couchbase.management.options import (CreatePrimaryQueryIndexOptions,
                                          CreateQueryIndexOptions,
                                          DropQueryIndexOptions,
                                          WatchQueryIndexOptions)

cluster = Cluster(
    'couchbase://localhost',
    authenticator=PasswordAuthenticator(
        'Administrator',
        'password'))

bucket_name = 'beer-sample'

ixm = cluster.query_indexes()
try:
    # expect exception as the travel-sample bucket should already come w/ a primary index
    ixm.create_primary_index(bucket_name)
except QueryIndexAlreadyExistsException:
    print('Primary index already exists')

# can create an index, and ignore if it already exists
ixm.create_primary_index(bucket_name, CreatePrimaryQueryIndexOptions(ignore_if_exists=True))


n1ql = '''
SELECT brewery.name,
       beer.beerCount,
       beer.brewery_id
FROM `beer-sample` AS brewery
    JOIN (
    SELECT brewery_id,
           COUNT(1) AS beerCount
    FROM `beer-sample`
    WHERE `type` = 'beer'
    GROUP BY brewery_id) AS beer ON beer.brewery_id = META(brewery).id
WHERE brewery.`type` = 'brewery'
LIMIT 10;
'''

# lets create some secondary indexes that let the above query run
# w/o needing to do a primary index scan
beer_sample_type_idx = 'beer_sample_type'
fields = ['type']
ixm.create_index(bucket_name, beer_sample_type_idx, fields,
                 CreateQueryIndexOptions(deferred=True, timeout=timedelta(seconds=120)))

beer_sample_brewery_id_idx = 'beer_sample_type_brewery_id'
ixm.create_index(bucket_name, beer_sample_brewery_id_idx, ('type', 'brewery_id'),
                 CreateQueryIndexOptions(deferred=True, timeout=timedelta(seconds=120)))

# get all the indexes for a bucket
indexes = ixm.get_all_indexes(bucket_name)

# we only care about the indexes that are deferred
ix_names = list(map(lambda i: i.name, [idx for idx in indexes if idx.state == 'deferred']))

print(f"Building indexes: {', '.join(ix_names)}")
# build the deferred indexes
ixm.build_deferred_indexes(bucket_name)

# lets wait until the indexes are built
wait_seconds = 30
for _ in range(3):
    try:
        ixm.watch_indexes(bucket_name,
                          ix_names,
                          WatchQueryIndexOptions(timeout=timedelta(seconds=wait_seconds)))
        print('Indexes {} built!')
    except WatchQueryIndexTimeoutException:
        print(f"Indexes not build within {wait_seconds} seconds...")

# now lets do query!
query_res = cluster.query(n1ql)
for row in query_res.rows():
    print('Query row: {}'.format(row))

# time to drop the indexes
ixm.drop_index(bucket_name, beer_sample_type_idx)
ixm.drop_index(bucket_name, beer_sample_brewery_id_idx)

try:
    # Should get an exception if the index has already been dropped
    ixm.drop_index(bucket_name, beer_sample_brewery_id_idx)
except QueryIndexNotFoundException:
    print('Query index already dropped')

# can drop an index, and ignore if it doesn't exists
ixm.drop_index(bucket_name, beer_sample_brewery_id_idx, DropQueryIndexOptions(ignore_if_not_exists=True))

cluster.close()
