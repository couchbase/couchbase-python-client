#  Copyright 2016-2026. Couchbase, Inc.
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

from typing import Any

import pytest

from couchbase.management.logic.view_index_mgmt_types import (DesignDocument,
                                                              DesignDocumentNamespace,
                                                              View)

MAP_FN = 'function(doc, meta) { emit(meta.id, null); }'
DDOC_NAME = 'test-ddoc'


def _design_document(**overrides: Any) -> DesignDocument:
    return DesignDocument.from_json({'name': DDOC_NAME,
                                     'namespace': 'production',
                                     'views': {'by_id': {'map': MAP_FN, 'reduce': '_count',
                                                         'name': 'by_id'}},
                                     **overrides})


class ViewTypesUnitTests:
    # No cb_env and no TEST_MANIFEST: every test feeds a server-shaped dict to a `from_json`
    # classmethod, and `couchbase.management.logic.view_index_mgmt_types` holds the only
    # definitions.  Not generated for that reason.
    #
    # Decoding is the half `viewmgmt_t` cannot reach: it builds `View(...)` objects to hand to
    # management calls, which exercises the constructor and `as_dict()` on the way out.  What
    # comes back is shaped differently in three places, one per group of tests below.

    def test_view_from_json(self) -> None:
        view = View.from_json({'map': MAP_FN, 'reduce': '_count'})

        assert view.map == MAP_FN
        assert view.reduce == '_count'

    def test_view_from_json_carrying_name(self) -> None:
        # The core puts a name in every view dict it builds and `from_json` splats the whole
        # dict at the constructor, so the parameter has to exist.  It is not stored: a view's
        # name is its key in the design document, and writing it back is the caller's call.
        view = View.from_json({'map': MAP_FN, 'name': 'by_id'})

        assert view.map == MAP_FN
        assert view.reduce is None
        assert view.as_dict() == {'map': MAP_FN}
        assert view.as_dict(name='by_id') == {'map': MAP_FN, 'name': 'by_id'}

    def test_view_from_json_rejects_an_unknown_key(self) -> None:
        # The splat again: an unrecognised field from a newer server reaches the constructor
        # as a keyword and fails there rather than being ignored.
        with pytest.raises(TypeError):
            View.from_json({'map': MAP_FN, 'bogus': 1})

    def test_design_document_from_json(self) -> None:
        ddoc = _design_document()

        assert ddoc.name == DDOC_NAME
        assert ddoc.namespace is DesignDocumentNamespace.PRODUCTION
        assert ddoc.get_view('by_id').map == MAP_FN
        assert ddoc.get_view('by_id').reduce == '_count'

    def test_design_document_name_is_stored_unprefixed(self) -> None:
        # The server stores the name with a `_design/` prefix and, for a development document,
        # a `dev_` one on top.  Both come off, in that order, so a caller can pass the name it
        # read straight back in.
        assert _design_document(name=f'_design/{DDOC_NAME}').name == DDOC_NAME
        assert _design_document(name=f'dev_{DDOC_NAME}').name == DDOC_NAME
        assert _design_document(name=f'_design/dev_{DDOC_NAME}').name == DDOC_NAME

    def test_get_view_answers_none_for_an_unknown_view(self) -> None:
        # A dict lookup with a None default rather than a raise, so the caller gets an
        # AttributeError one line later instead of DesignDocumentNotFoundException here.
        assert _design_document().get_view('no-such-view') is None

    @pytest.mark.parametrize('namespace, expected',
                             [('production', DesignDocumentNamespace.PRODUCTION),
                              ('development', DesignDocumentNamespace.DEVELOPMENT),
                              ('Production', DesignDocumentNamespace.DEVELOPMENT),
                              (None, DesignDocumentNamespace.DEVELOPMENT)])
    def test_namespace_decodes_production_by_name_and_everything_else_as_development(
            self,
            namespace: str | None,
            expected: DesignDocumentNamespace) -> None:
        # `from_str` tests for the one string 'production' and answers DEVELOPMENT for
        # everything else, absent and mis-cased included, so a namespace the SDK could not
        # read is indistinguishable from one that really is development.
        assert _design_document(namespace=namespace).namespace is expected

    def test_namespace_round_trips_under_a_different_key(self) -> None:
        # Read from `namespace`, written to `ns`, and the namespace to write is handed to
        # `as_dict` rather than taken from the document, so nothing stops the two disagreeing.
        ddoc = _design_document()

        assert ddoc.namespace is DesignDocumentNamespace.PRODUCTION
        assert ddoc.as_dict(DesignDocumentNamespace.PRODUCTION)['ns'] == 'production'
        assert ddoc.as_dict(DesignDocumentNamespace.DEVELOPMENT)['ns'] == 'development'
