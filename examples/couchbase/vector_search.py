import json
import os
import pathlib
from time import sleep
from typing import List, Optional
from uuid import uuid4

import couchbase.search as search
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.collection import Collection
from couchbase.exceptions import DocumentNotFoundException, QueryIndexAlreadyExistsException
from couchbase.management.search import SearchIndex
from couchbase.options import ClusterOptions, SearchOptions
from couchbase.vector_search import VectorQuery, VectorSearch

# NOTE:  These paths should be updated accordingly if going to use.  The paths are currently setup for running w/in
#        the couchbase-python-client root directory.
#

TEST_INDEX_NAME = 'test-search-vector-index'
TEST_COLL_INDEX_NAME = 'test-search-vector-coll-index'
TEST_PATH = os.path.join(pathlib.Path(__file__).parent,
                         'tests',
                         'test_cases')
TEST_INDEX_PATH = os.path.join(TEST_PATH,
                               f'{TEST_INDEX_NAME}-params.json')
TEST_COLL_INDEX_PATH = os.path.join(TEST_PATH,
                                    f'{TEST_COLL_INDEX_NAME}-params.json')
TEST_VECTOR_SEARCH_DOCS_PATH = os.path.join(TEST_PATH,
                                            'test-vector-search-docs.json')
TEST_SEARCH_VECTOR_PATH = os.path.join(TEST_PATH,
                                       'test-search-vector.json')


def check_doc_count(sixm,
                    idx_name: str,
                    min_count: int,
                    retries: Optional[int] = 20,
                    delay: Optional[int] = 30
                    ) -> bool:

    indexed_docs = 0
    no_docs_cutoff = 300
    for i in range(retries):
        # if no docs after waiting for a period of time, exit
        if indexed_docs == 0 and i * delay >= no_docs_cutoff:
            return 0
        indexed_docs = sixm.get_indexed_documents_count(idx_name)
        if indexed_docs >= min_count:
            break
        print(f'Found {indexed_docs} indexed docs, waiting a bit...')
        sleep(delay)

    return indexed_docs


def load_search_idx(cluster: Cluster) -> None:
    sixm = cluster.search_indexes()
    params_json = None
    with open(TEST_INDEX_PATH) as params_file:
        input = params_file.read()
        params_json = json.loads(input)

    if params_json is None:
        print('Unabled to read search index params')
        return

    idx = SearchIndex(name=TEST_INDEX_NAME,
                      idx_type='fulltext-index',
                      source_name='default',
                      source_type='couchbase',
                      params=params_json)
    sixm.upsert_index(idx)
    indexed_docs = check_doc_count(sixm, TEST_INDEX_NAME, 20, retries=10, delay=3)
    print(f'Have {indexed_docs} indexed documents.')


def drop_search_idx(cluster: Cluster) -> None:
    sixm = cluster.search_indexes()
    sixm.drop_index(TEST_INDEX_NAME)


def load_test_vector():
    vector = None
    with open(TEST_SEARCH_VECTOR_PATH) as vector_file:
        vector = json.loads(vector_file.read())

    return vector


def read_docs():
    with open(TEST_VECTOR_SEARCH_DOCS_PATH) as input:
        while line := input.readline():
            yield json.loads(line)


def load_docs(root: str, collection: Collection) -> List[str]:
    idx = 0
    keys = []
    for doc in read_docs():
        key = f'{root}_{idx}'
        # the search index expects a type field w/ vector as the value
        doc['type'] = 'vector'
        collection.upsert(key, doc)
        keys.append(key)
        idx += 1
    print(f'loaded {len(keys)} docs.')
    return keys


def remove_docs(keys: List[str], collection: Collection) -> None:
    for key in keys:
        try:
            collection.remove(key)
        except DocumentNotFoundException:
            pass


def setup(cluster: Cluster, collection: Collection) -> List[str]:
    root = str(uuid4())[:8]
    keys = load_docs(root, collection)
    try:
        load_search_idx(cluster)
    except QueryIndexAlreadyExistsException:
        pass

    return keys


def teardown(cluster: Cluster, collection: Collection, keys: List[str]) -> None:
    remove_docs(keys, collection)
    drop_search_idx(cluster)


def main():
    auth = PasswordAuthenticator('Administrator', 'password')
    opts = ClusterOptions(auth)
    cluster = Cluster.connect('couchbase://localhost', opts)
    bucket = cluster.bucket('default')
    collection = bucket.default_collection()

    # NOTE:  validate paths (see note on line 17) if going to use setup/teardown functionality
    keys = setup(cluster, collection)
    vector = load_test_vector()

    search_req = search.SearchRequest.create(search.MatchAllQuery()).with_vector_search(
        VectorSearch.from_vector_query(VectorQuery('vector_field', vector)))
    search_iter = cluster.search(TEST_INDEX_NAME, search_req, SearchOptions(limit=2))
    for row in search_iter.rows():
        print(f'row: {row}')

    print(f'Metatdata: {search_iter.metadata()}')

    # NOTE: only use in conjunction w/ setup() method
    teardown(cluster, collection, keys)


if __name__ == '__main__':
    main()
