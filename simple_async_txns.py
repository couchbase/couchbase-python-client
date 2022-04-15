import asyncio
from acouchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions, TransactionConfig
from couchbase.exceptions import CouchbaseException
from datetime import timedelta
from uuid import uuid4
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from acouchbase.transactions import AttemptContext

opts = ClusterOptions(authenticator=PasswordAuthenticator("Administrator", "password"),
                      transaction_config=TransactionConfig(expiration_time=timedelta(seconds=2)))


async def wait_for_all(tasks):
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            raise r
    return results


async def run():
    my_cluster = Cluster("couchbase://localhost", opts)
    await my_cluster.on_connect()

    bucket = my_cluster.bucket("default")
    await bucket.on_connect()

    coll = bucket.default_collection()

    key1 = str(uuid4())
    key2 = str(uuid4())

    tasks = [coll.insert(key1, {"value": 10}), coll.insert(key2, {"value": 0})]
    results = await wait_for_all(tasks)
    print(f'insert results: {results}')

    async def txn_logic(ctx  # type: AttemptContext
                        ):
        t = [ctx.get(coll, key1), ctx.get(coll, key2)]
        res = await wait_for_all(t)
        print(f'get results: {results}')
        doc1 = res[0]
        doc2 = res[1]
        doc1_content = doc1.content_as[dict]
        doc2_content = doc2.content_as[dict]
        print(f'doc1:{doc1}, doc2: {doc2}')
        print(f'doc1_content: {doc1_content}, doc2_content: {doc2_content}')
        if doc1_content["value"] > 0:
            doc1_content["value"] = doc1_content["value"] - 1
            doc2_content["value"] = doc2_content["value"] + 1
            t = [ctx.replace(doc1, doc1_content), ctx.replace(doc2, doc2_content)]
            await wait_for_all(t)
        else:
            raise RuntimeError("doc1 is exhausted")

    ok = True
    while ok:
        try:
            print(f'txn_result: {await my_cluster.transactions.run(txn_logic)}')
        except CouchbaseException as e:
            print(f'txn raised exception: {e}')
            ok = False

    tasks = [coll.get(key1), coll.get(key2)]
    results = await wait_for_all(tasks)
    print(f'after txns, we have {results}')
    await my_cluster.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(run())
print('done')
