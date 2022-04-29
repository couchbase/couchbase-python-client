import asyncio
import time
from datetime import timedelta
from random import random
from uuid import uuid4

from acouchbase.cluster import Cluster
from acouchbase.management.queries import QueryIndexManager
from acouchbase.transactions import Transactions
from couchbase.auth import PasswordAuthenticator
from couchbase.diagnostics import ServiceType
from couchbase.durability import DurabilityLevel, ServerDurability
from couchbase.exceptions import CouchbaseException, ParsingFailedException
from couchbase.options import (ClusterOptions,
                               QueryOptions,
                               TransactionConfig,
                               UpsertOptions,
                               WaitUntilReadyOptions)


async def do_query(cluster):
    n1ql = 'SELECT * FROM `travel-sample` LIMIT 10;'
    # n1ql = 'NOT N1QL!'
    query = cluster.query(n1ql, QueryOptions(metrics=True))
    async for r in query.rows():
        print(f'Found row: {r}')


async def do_upsert(coll, key, value):
    r = await coll.upsert(key, value)
    await asyncio.sleep(random())
    print(f'Got result - cas: {r.cas}')


async def do_get(coll, key):
    r = await coll.get(key)
    await asyncio.sleep(random())
    print(f'Got result: {r.content_as[dict]}')


async def do_lots_of_kv_things(coll):
    data = {f'test-key-{i}': {'what': 'test doc', 'key': f'test-key-{i}'} for i in range(10)}
    all_the_things = [do_upsert(coll, k, v) for k, v in data.items()]
    all_the_things.extend([do_get(coll, k) for k in data.keys()])
    await asyncio.gather(*all_the_things)


async def write_and_read(key, value):
    cluster = Cluster('couchbase://127.0.0.1',
                      ClusterOptions(PasswordAuthenticator('Administrator', 'password')), enable_mutation_tokens=False)
    # await cluster.on_connect()

    await cluster.wait_until_ready(timedelta(seconds=3),
                                   WaitUntilReadyOptions(service_types=[ServiceType.KeyValue, ServiceType.Query]))
    cb = cluster.bucket('default')
    await cb.on_connect()
    cb_coll = cb.default_collection()
    # await cb_coll.upsert(key, value)

    durability = ServerDurability(level=DurabilityLevel.NONE)
    r = await cb_coll.upsert(key, value,
                             UpsertOptions(durability=durability, timeout=timedelta(seconds=20)))
    print(r.mutation_token())
    result = await cb_coll.get(key)

    # change this to a bucket that doesn't exist
    # should get a KeyspaceNotFound, get a ParsingException...
    #n1ql = 'SELECT * FROM `beer-sample` WHERE `type`=$1 LIMIT 10;'
    # n1ql = 'SELECT * FROM `beer-sample` WHERE `type`=$type LIMIT 10;'
    # try:
    #     #query = cluster.query(n1ql, QueryOptions(metrics=True), positional_parameters=["beer"])
    #     #query = cluster.query(n1ql, QueryOptions(metrics=True, positional_parameters=["beer"]))
    #     #query = cluster.query(n1ql, QueryOptions(metrics=True), named_parameters={"type":"beer"})
    #     query = cluster.query(n1ql, QueryOptions(metrics=True, named_parameters={"type": "beer"}))
    #     async for r in query.rows():
    #         print(f'Found row: {r}')
    #     metadata = query.metadata()
    #     metrics = metadata.metrics()
    #     print(metrics)
    # except ParsingFailedException as ex:
    #     print('Caught parsing failure.')
    #
    # n1ql = 'NOT N1QL!'
    # try:
    #     query = cluster.query(n1ql, QueryOptions(metrics=True))
    #     async for r in query.rows():
    #         print(f'Found row: {r}')
    # except ParsingFailedException as ex:
    #     print('Caught parsing failure.')
    #
    # test for async, need to also uncomment the following
    #   in acouchbase/n1ql.py > _get_next_row():
    #       await asyncio.sleep(random())
    # await asyncio.gather(*[do_lots_of_kv_things(cb_coll), do_query(cluster)])

    # ixm = cluster.query_indexes()
    # ixname = 'ix2'
    # fields = ('fld1', 'fld2')
    # await ixm.create_index(cb.name, ixname,
    #                         fields=fields, timeout=timedelta(seconds=120))
    # n1ql = "SELECT {1}, {2} from `{0}` where {1}=1 and {2}=2 limit 1".format(
    #     cb.name, *fields)
    # await cluster.query(n1ql).execute()

    txns = Transactions(cluster, TransactionConfig())

    async def txn_logic(ctx):
        try:
            print(f'txn_logic got ctx {ctx}')
            futures = []
            ids = []
            for i in range(10):
                ids.append(str(uuid4()))
                futures.append(ctx.insert(cb_coll, ids[-1], f'"I have an id of {ids[-1]}"'))
            results = await asyncio.gather(*futures)
            futures.clear()
            for res in results:
                print(f'inserts: result id={res.id}, cas={res.cas}, val={res.content_as[str]}')

            async def get_and_replace(key):
                get_res = await ctx.get(cb_coll, key)
                print(f'get_result for {key} returns ')
                return await ctx.replace(get_res, f'"I got replaced, my id still is {key}"')

            async def get_and_remove(key):
                get_res = await ctx.get(cb_coll, key)
                return await ctx.remove(get_res)

            async def do_queries(ids):
                quoted_ids = ",".join(map(lambda x: f'"{x}"', ids))

                query_result = await ctx.query(f"SELECT * FROM `default` USE KEYS [{quoted_ids}]")
                rows = []
                async for row in query_result.rows():
                    print(f'got row {row}')
                    rows.append(row)
                return rows

            for k in ids:
                futures.append(get_and_replace(k))
            results = await asyncio.gather(*futures)
            futures.clear()
            for res in results:
                print(f'replaces: result id={res.id}, cas={res.cas}, val={res.content_as[str]}')
            # do one query, getting all the docs we inserted
            rows = await do_queries(ids)
            print(f'rows after awaiting do_queries: {rows}')
            for k in ids:
                futures.append(get_and_remove(k))
            await asyncio.gather(*futures)
            print(f'removed {", ".join(ids)}')
            #raise RuntimeError("some random crap happened")
        except Exception as e:
            print(f'got exception {e}')
            raise e

    x = await txns.run(txn_logic)
    print(f'txns.run returned {x}')
    txns.close()
    await cluster.close()
    return result

loop = asyncio.get_event_loop()
rv = loop.run_until_complete(write_and_read('foo', 'bar'))
print(rv.content_as[str])
