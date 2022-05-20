from datetime import timedelta

from acouchbase.cluster import Cluster, get_event_loop
from couchbase.auth import PasswordAuthenticator
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


async def main():

    cluster = Cluster(
        'couchbase://localhost',
        authenticator=PasswordAuthenticator(
            'Administrator',
            'password'))

    await cluster.on_connect()

    bucket_name = 'beer-sample'

    ixm = cluster.query_indexes()
    try:
        # expect exception as the travel-sample bucket should already come w/ a primary index
        await ixm.create_primary_index(bucket_name)
    except QueryIndexAlreadyExistsException:
        print('Primary index already exists')

    # can create an index, and ignore if it already exists
    await ixm.create_primary_index(bucket_name, CreatePrimaryQueryIndexOptions(ignore_if_exists=True))

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
    await ixm.create_index(bucket_name,
                           beer_sample_type_idx,
                           fields,
                           CreateQueryIndexOptions(deferred=True,
                                                   timeout=timedelta(seconds=120)))

    beer_sample_brewery_id_idx = 'beer_sample_type_brewery_id'
    await ixm.create_index(bucket_name,
                           beer_sample_brewery_id_idx,
                           ('type', 'brewery_id'),
                           CreateQueryIndexOptions(deferred=True,
                                                   timeout=timedelta(seconds=120)))

    # get all the indexes for a bucket
    indexes = await ixm.get_all_indexes(bucket_name)

    # we only care about the indexes that are deferred
    ix_names = list(map(lambda i: i.name, [idx for idx in indexes if idx.state == 'deferred']))

    print(f"Building indexes: {', '.join(ix_names)}")
    # build the deferred indexes
    await ixm.build_deferred_indexes(bucket_name)

    # lets wait until the indexes are built
    wait_seconds = 30
    for _ in range(3):
        try:
            await ixm.watch_indexes(bucket_name,
                                    ix_names,
                                    WatchQueryIndexOptions(timeout=timedelta(seconds=wait_seconds)))
            print('Indexes {} built!')
        except WatchQueryIndexTimeoutException:
            print(f"Indexes not build within {wait_seconds} seconds...")

    # now lets do query!
    query_res = cluster.query(n1ql)
    async for row in query_res.rows():
        print('Query row: {}'.format(row))

    # time to drop the indexes
    await ixm.drop_index(bucket_name, beer_sample_type_idx)
    await ixm.drop_index(bucket_name, beer_sample_brewery_id_idx)

    try:
        # Should get an exception if the index has already been dropped
        await ixm.drop_index(bucket_name, beer_sample_brewery_id_idx)
    except QueryIndexNotFoundException:
        print('Query index already dropped')

    # can drop an index, and ignore if it doesn't exists
    await ixm.drop_index(bucket_name, beer_sample_brewery_id_idx, DropQueryIndexOptions(ignore_if_not_exists=True))


if __name__ == "__main__":
    loop = get_event_loop()
    loop.run_until_complete(main())
