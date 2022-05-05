import logging
from datetime import timedelta
from sys import stdout
from typing import TYPE_CHECKING
from uuid import uuid4

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import TransactionFailed
from couchbase.options import ClusterOptions, TransactionConfig

if TYPE_CHECKING:
    from couchbase.transactions import AttemptContext

logging.basicConfig(stream=stdout, level=logging.DEBUG, format='%(asctime)s|%(levelname)s|%(name)s| %(message)s')

opts = ClusterOptions(authenticator=PasswordAuthenticator("Administrator", "password"),
                      transaction_config=TransactionConfig(expiration_time=timedelta(seconds=2)))

my_cluster = Cluster("couchbase://localhost", opts)

coll = my_cluster.bucket("default").default_collection()

key1 = str(uuid4())
key2 = str(uuid4())

coll.insert(key1, {"value": 10})
coll.insert(key2, {"value": 0})


def txn_logic(ctx  # type: AttemptContext
              ):
    doc1 = ctx.get(coll, key1)
    doc2 = ctx.get(coll, key2)
    doc1_content = doc1.content_as[dict]
    doc2_content = doc2.content_as[dict]
    if doc1_content["value"] > 0:
        doc1_content["value"] = doc1_content["value"] - 1
        doc2_content["value"] = doc2_content["value"] + 1
        ctx.replace(doc1, doc1_content)
        ctx.replace(doc2, doc2_content)
    else:
        raise RuntimeError("doc1 is exhausted")


ok = True
logging.info(f'doc 1 starts off as: {coll.get(key1).content_as[dict]}')
logging.info(f'doc 2 starts off as: {coll.get(key2).content_as[dict]}')
while ok:
    try:
        logging.info(f'txn_result: {my_cluster.transactions.run(txn_logic)}')
    except TransactionFailed as e:
        logging.info(f'got {e}')
        ok = False

logging.info(f'doc 1 is now: {coll.get(key1).content_as[dict]}')
logging.info(f'doc 2 is now: {coll.get(key2).content_as[dict]}')
