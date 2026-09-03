#  Copyright 2016-2024. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""View names, the design document's map function, and the key arithmetic that mirrors it.

Api-independent: all three environment modules and all three generated suite trees read the
same names, the same map function and the same expectations.

**The map function and the expectations have to live in one file, and that is the whole reason
this module exists.**  The view partitions the seeded vehicles into four keys by slicing the
8-character batch id, one slice per decade of document ids, and every row count and every
ordering the suite asserts is a restatement of that same slicing.  Held apart, the two drift
silently: the fixture is JavaScript nobody type-checks and the expectations are Python nobody
connects to it, so a change to either passes review on its own.  PYCBC-1898 is what that costs.
It was reported as an intermittent CI failure in three tests at once, and the cause was a third
copy of the same arithmetic, in the data provider, that did not guarantee what the other two
assumed: two of the four slices could be equal, which merges two decades under one key.

The bucket name carries the xdist worker id because a bucket is cluster-global: two workers
running this suite would otherwise create and drop one shared bucket underneath each other.  The
design document name is worker-suffixed too, which is belt-and-braces given the bucket already
is, and costs one f-string.
"""

from __future__ import annotations

import os

VIEW_BUCKET_NAME = 'test-view-bucket'
DESIGN_DOC_NAME = 'test-ddoc'
VIEW_NAME = 'test-view'

# Documents the views suites seed into their own bucket.  The decade boundaries in the map
# function below are literal, so this is not a free knob: 50 is what makes the four view keys
# hold 10, 10, 10 and 20 documents, and every expected count in the suite follows from that
# shape.  view_key_size() is what the suite reads, so a change here needs no change there.
VIEW_DOCS = 50

# The slice width the map function uses, and the number of slices an 8-character batch id
# yields.  Named rather than spelled 2 and 4 in four places, because the data provider's
# guarantee (`_batch_id_with_distinct_view_keys`) is stated in terms of exactly these.
_KEY_WIDTH = 2
_NUM_KEYS = 4

# The design document's map function.
#
# One emit per vehicle document, keyed by a 2-character slice of the batch id chosen by the
# document's own index: ids 0-9 take the first slice, 10-19 the second, 20-29 the third and
# everything from 30 up the fourth.  The value is a trimmed copy of the document, which is what
# lets a row assertion check `row.value['batch']` without a second fetch.
#
# One branch, not two.  The views environment seeds vehicles and only vehicles, so a second
# arm emitting dealership documents would never run, and a map function with an unreachable
# arm invites a later reader to assume dealerships are covered here.
VIEW_MAP_FUNCTION = """function(doc, meta) {
  if (doc.type !== "vehicle" || !doc.batch) {
    return;
  }
  const value = { batch: doc.batch, id: doc.id, type: doc.type };
  const idx = parseInt(doc.id.split('::')[1]);
  if (idx < 10) {
    emit(doc.batch.slice(0, 2), value);
  } else if (idx < 20) {
    emit(doc.batch.slice(2, 4), value);
  } else if (idx < 30) {
    emit(doc.batch.slice(4, 6), value);
  } else {
    emit(doc.batch.slice(-2), value);
  }
}
"""


# ---------------------------------------------------------------------------
# The management suites
# ---------------------------------------------------------------------------
#
# viewmgmt_t manages design documents and never queries one, so it needs none of the machinery
# above: no bucket of its own, no corpus, no wait for the view to catch up.  Its documents go on
# the shared test bucket, and a design document nothing queries is inert there.
#
# The names are distinct because the management suite drops and re-creates its document on
# nearly every test, so a querying suite sharing the name would find its document missing
# mid-run.  The two also sit on different buckets, and neither reason is worth relying on alone.

MGMT_DESIGN_DOC_NAME = 'test-mgmt-ddoc'
MGMT_VIEW_NAME = 'test-mgmt-view'
MGMT_SECONDARY_VIEW_NAME = 'test-mgmt-view-alt'

# Two map functions, and the only thing that matters about them is that they are *different*.
# Nothing executes either one: the management suite asserts that what upsert sent is what get
# returns, so the bodies are round-trip payloads and the pair is what lets a test tell one
# stored document from another.  They are deliberately trivial rather than a copy of
# VIEW_MAP_FUNCTION, which would read as though this suite queried something.
MGMT_MAP_FUNCTION = 'function (doc, meta) { emit(meta.id, null); }'
MGMT_SECONDARY_MAP_FUNCTION = 'function (doc, meta) { emit(meta.id, doc.type); }'


def _worker_suffix() -> str:
    """`-gw0` under xdist, empty otherwise.

    Read from the environment rather than from cb_env because the suites name these before an
    environment exists.
    """
    worker_id = os.environ.get('PYTEST_XDIST_WORKER', '')
    return f'-{worker_id}' if worker_id else ''


def view_bucket_name() -> str:
    """The bucket the views suites create, seed, query and drop, one per xdist worker."""
    return f'{VIEW_BUCKET_NAME}{_worker_suffix()}'


def design_doc_name() -> str:
    """The design document holding the one view the suites query."""
    return f'{DESIGN_DOC_NAME}{_worker_suffix()}'


def mgmt_design_doc_name() -> str:
    """The design document the management suites upsert, publish and drop, one per worker."""
    return f'{MGMT_DESIGN_DOC_NAME}{_worker_suffix()}'


def view_keys(batch_id: str) -> list[str]:
    """The four view keys the map function emits for a batch, in slice order.

    Slice order, not sorted order, and the difference is load bearing: the *n*th slice is the
    key for the *n*th decade of document ids, which is what docids_for_key() inverts.  The view
    itself returns rows in key order, so a suite comparing against a document ordering has to
    sort these first; every caller that does says so at the call site.

    Raises rather than returning duplicates.  DataProvider draws a batch id whose four slices
    are distinct precisely so this cannot happen (PYCBC-1898), and a run where it happens anyway
    invalidates every expectation below, so it should stop here rather than surface later as an
    unexplained row-count diff.
    """
    keys = [batch_id[i:i + _KEY_WIDTH] for i in range(0, _KEY_WIDTH * _NUM_KEYS, _KEY_WIDTH)]
    if len(set(keys)) != _NUM_KEYS:
        raise ValueError(f'batch id {batch_id} yields duplicate view keys {keys}; '
                         'the seeded corpus cannot support the views suites')
    return keys


def view_key_size(batch_id: str, key: str, num_docs: int = VIEW_DOCS) -> int:
    """How many documents the map function emits under `key`."""
    return len(docids_for_key(batch_id, key, num_docs))


def docids_for_key(batch_id: str, key: str, num_docs: int = VIEW_DOCS) -> list[str]:
    """The document ids the map function emits under `key`, in the order the view returns them.

    The inverse of the map function's decade test, and the reason it is written from the key's
    *position* rather than from a range table: position is what the JavaScript branches on.

    Lexicographic order is the view's order for these ids, because every index in a decade has
    the same number of digits.  It stops being true above 99 documents, which is why num_docs is
    a parameter with the corpus size as its default rather than something read from a config.
    """
    key_idx = view_keys(batch_id).index(key)
    start = key_idx * 10
    stop = num_docs if key_idx == _NUM_KEYS - 1 else start + 10
    return [f'{batch_id}::{i}' for i in range(start, stop)]


def docids_in_view_order(batch_id: str,
                         num_docs: int = VIEW_DOCS,
                         *,
                         descending: bool = False) -> list[str]:
    """Every emitted document id, in the order a view query returns them.

    Rows come back sorted by key and then by document id, so this sorts the keys and concatenates
    each key's ids.  ViewOrdering.DESCENDING reverses both levels, not just the outer one, which
    is the part a hand-written expectation gets wrong.
    """
    ordered_keys = sorted(view_keys(batch_id), reverse=descending)
    return [docid
            for key in ordered_keys
            for docid in sorted(docids_for_key(batch_id, key, num_docs), reverse=descending)]
