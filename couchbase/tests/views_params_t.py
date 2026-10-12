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

from __future__ import annotations

import json
from datetime import timedelta
from typing import (Any,
                    Dict,
                    List,
                    Tuple)

import pytest

from couchbase._utils import JSONType
from couchbase.exceptions import InvalidArgumentException
from couchbase.management.views import DesignDocumentNamespace
from couchbase.options import ViewOptions
from couchbase.serializer import DefaultJsonSerializer
from couchbase.views import (ViewErrorMode,
                             ViewOrdering,
                             ViewQuery,
                             ViewScanConsistency)

# Labels rather than claims about a cluster: nothing here connects, and all three appear in
# the request verbatim.
BUCKET_NAME = 'default'
DDOC_NAME = 'test-ddoc'
VIEW_NAME = 'test-view'


def _encoded(options: ViewOptions) -> Dict[str, Any]:
    # Copied rather than returned as-is because `as_encodable()` hands back the query's own
    # params dict, and returning a live internal dict is how a test ends up asserting about
    # a value it wrote itself.
    query = ViewQuery.create_view_query_object(BUCKET_NAME, DDOC_NAME, VIEW_NAME, options)
    return dict(query.as_encodable())


def _expected(**overrides: Any) -> Dict[str, Any]:
    # The three names are unconditional: a view query cannot be addressed without all of
    # them, so unlike the search and query suites nothing defaults into the base.
    return {'bucket_name': BUCKET_NAME,
            'document_name': DDOC_NAME,
            'view_name': VIEW_NAME,
            **overrides}


class ViewsParamsUnitTests:
    # No cb_env and no TEST_MANIFEST: every test builds a ViewQuery and reads the dict it
    # would hand the core.  Not generated either, since `couchbase.views.ViewQuery` is the
    # only definition and neither async tree carries its own views or options module.
    #
    # The SDK renames four options on the way out (`endkey` to `end_key`, `endkey_docid` to
    # `end_key_doc_id`, and the two `startkey` counterparts), and `namespace` becomes `ns`.
    # Everything a view sends as a key is JSON, so each key is serialized individually.

    def test_params_base(self) -> None:
        assert _encoded(ViewOptions()) == _expected()

    def test_params_client_context_id(self) -> None:
        assert _encoded(ViewOptions(client_context_id='test-context-id')) == _expected(
            client_context_id='test-context-id')

    def test_params_debug(self) -> None:
        assert _encoded(ViewOptions(debug=True)) == _expected(debug=True)

    def test_params_endkey(self) -> None:
        # A compound key is a list, and reaches the request as the JSON text of that list.
        key = ['101 Coffee Shop', 'landmark_11769']
        assert _encoded(ViewOptions(endkey=key)) == _expected(end_key=json.dumps(key))

    def test_params_endkey_docid(self) -> None:
        # The doc id half of a compound bound is a plain string, not JSON.
        assert _encoded(ViewOptions(endkey_docid='landmark_11769')) == _expected(
            end_key_doc_id='landmark_11769')

    def test_params_group(self) -> None:
        assert _encoded(ViewOptions(group=True)) == _expected(group=True)

    def test_params_group_level(self) -> None:
        assert _encoded(ViewOptions(group_level=10)) == _expected(group_level=10)

    def test_params_inclusive_end(self) -> None:
        assert _encoded(ViewOptions(inclusive_end=True)) == _expected(inclusive_end=True)

    def test_params_key(self) -> None:
        # A str key is still JSON, so it arrives quoted rather than bare.
        assert _encoded(ViewOptions(key='test-key')) == _expected(key=json.dumps('test-key'))

    def test_params_keys(self) -> None:
        # Each key is encoded on its own, so this is a list of JSON strings and not the JSON
        # of a list, the same shape search uses for its sort specs.  The local carries the
        # full union because `ViewOptions.keys` is `Optional[List[JSONType]]` and `List` is
        # invariant, so the natural `keys=['a', 'b']` is an arg-type error; `Sequence` is the
        # fix and it belongs in the SDK.
        keys: List[JSONType] = ['test-key1', 'test-key2']
        assert _encoded(ViewOptions(keys=keys)) == _expected(keys=[json.dumps(k) for k in keys])

    def test_params_limit(self) -> None:
        assert _encoded(ViewOptions(limit=10)) == _expected(limit=10)

    def test_params_namespace(self) -> None:
        opts = ViewOptions(namespace=DesignDocumentNamespace.DEVELOPMENT)
        query = ViewQuery.create_view_query_object(BUCKET_NAME, DDOC_NAME, VIEW_NAME, opts)

        assert dict(query.as_encodable()) == _expected(
            ns=DesignDocumentNamespace.DEVELOPMENT.to_str())
        assert query.namespace == DesignDocumentNamespace.DEVELOPMENT

    def test_params_on_error(self) -> None:
        opts = ViewOptions(on_error=ViewErrorMode.CONTINUE)
        query = ViewQuery.create_view_query_object(BUCKET_NAME, DDOC_NAME, VIEW_NAME, opts)

        assert dict(query.as_encodable()) == _expected(on_error=ViewErrorMode.CONTINUE.to_str())
        assert query.on_error == ViewErrorMode.CONTINUE

    def test_params_order(self) -> None:
        opts = ViewOptions(order=ViewOrdering.ASCENDING)
        query = ViewQuery.create_view_query_object(BUCKET_NAME, DDOC_NAME, VIEW_NAME, opts)

        assert dict(query.as_encodable()) == _expected(order=ViewOrdering.ASCENDING.to_str())
        assert query.order == ViewOrdering.ASCENDING

    def test_params_reduce(self) -> None:
        assert _encoded(ViewOptions(reduce=True)) == _expected(reduce=True)

    def test_params_scan_consistency(self) -> None:
        opts = ViewOptions(scan_consistency=ViewScanConsistency.REQUEST_PLUS)
        query = ViewQuery.create_view_query_object(BUCKET_NAME, DDOC_NAME, VIEW_NAME, opts)

        assert dict(query.as_encodable()) == _expected(
            scan_consistency=ViewScanConsistency.REQUEST_PLUS.to_str())
        assert query.consistency == ViewScanConsistency.REQUEST_PLUS

    def test_params_serializer(self) -> None:
        # The serializer rides along as the object itself: it is consumed when rows are
        # decoded, so it is never serialized into the request.
        #
        # @FINDING(PYCBC-1873): `ViewOptions` accepts `serializer` at runtime, as the
        # assertion below proves, but its `__init__` overload does not declare it and the
        # class docstring does not document it.  mypy honours the lone overload rather than
        # the `**kwargs` implementation, so the option is also a call error.
        serializer = DefaultJsonSerializer()
        assert _encoded(ViewOptions(serializer=serializer)) == _expected(  # type: ignore[call-overload]  # PYCBC-1873
            serializer=serializer)

    def test_params_skip(self) -> None:
        assert _encoded(ViewOptions(skip=10)) == _expected(skip=10)

    def test_params_startkey(self) -> None:
        key = ['101 Coffee Shop', 'landmark_11769']
        assert _encoded(ViewOptions(startkey=key)) == _expected(start_key=json.dumps(key))

    def test_params_startkey_docid(self) -> None:
        assert _encoded(ViewOptions(startkey_docid='landmark_11769')) == _expected(
            start_key_doc_id='landmark_11769')

    def test_params_timeout(self) -> None:
        # A timedelta, a whole number of seconds, and a fractional one: the three input forms,
        # all converging on the same millisecond count, which is the unit the binding reads.
        timeouts: List[Tuple[Any, int]] = [(timedelta(seconds=20), 20000),
                                           (20, 20000),
                                           (25.5, 25500)]
        for timeout, expected_ms in timeouts:
            assert _encoded(ViewOptions(timeout=timeout)) == _expected(timeout=expected_ms)

    def test_params_timeout_getter(self) -> None:
        # Regression guard (PYCBC-1894): the request carries milliseconds and the property
        # reports seconds, so the getter converts rather than handing back what is stored.  The
        # absent case has to stay None rather than becoming zero.
        query = ViewQuery.create_view_query_object(BUCKET_NAME, DDOC_NAME, VIEW_NAME,
                                                   ViewOptions(timeout=timedelta(seconds=20)))
        assert query.timeout == 20

        query = ViewQuery.create_view_query_object(BUCKET_NAME, DDOC_NAME, VIEW_NAME, ViewOptions())
        assert query.timeout is None

    def test_params_uninterpretable_enum_option(self) -> None:
        # set_option writes into params without verification, so a value none of the three
        # getters can turn into an enum member survives until it is read back.  Each handles
        # None and a str and has nothing left for anything else, from a signature declaring a
        # non-optional enum.
        options = (('scan_consistency', 'consistency'), ('order', 'order'), ('on_error', 'on_error'))
        for option, prop in options:
            query = ViewQuery.create_view_query_object(BUCKET_NAME, DDOC_NAME, VIEW_NAME)
            query.set_option(option, 5)
            with pytest.raises(InvalidArgumentException):
                getattr(query, prop)

        # The str branch of the consistency getter delegates to from_str, so an unrecognised
        # string raises from the enum rather than from the query.
        with pytest.raises(InvalidArgumentException):
            ViewScanConsistency.from_str('not_a_scan_consistency')
