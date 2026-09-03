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

"""The TEST_MANIFEST check every suite class runs against itself.

A suite declares the tests it is meant to hold; this fails the class when the two disagree.
The check is what makes a generated mirror trustworthy: a test the generator drops, or one
added to a mirror by hand, shows up as a manifest mismatch rather than as a suite that
quietly runs fewer tests than its source.
"""

from __future__ import annotations

from typing import ClassVar, Protocol

import pytest


class HasTestManifest(Protocol):
    """Anything that declares a manifest, which is all validate_test_manifest reads.

    `SuiteRequirements` satisfies this structurally.  The wider type is for the cluster-free
    unit classes, which declare a manifest but deliberately do not inherit `SuiteRequirements`:
    the autouse feature and protocol gates read their requirements off every class in the tree,
    and a class with no environment has nothing for them to check.
    """

    TEST_MANIFEST: ClassVar[list[str]]


def _is_test_method(cls: type, name: str) -> bool:
    """True for real test methods; excludes fixtures (e.g. a same-named test_* fixture)."""
    if not name.startswith('test') or name.startswith('__'):
        return False
    attr = getattr(cls, name, None)
    return callable(attr) and not hasattr(attr, '_pytestfixturefunction')


def validate_test_manifest(suite_cls: type[HasTestManifest], concrete_cls: type) -> None:
    """Fail unless concrete_cls's test methods exactly match suite_cls.TEST_MANIFEST.

    Call from an autouse fixture:
        @pytest.fixture(scope='class', autouse=True)
        def manifest_validated(self):
            validate_test_manifest(SuiteClass, self.__class__)
    """
    method_list = [name for name in dir(concrete_cls) if _is_test_method(concrete_cls, name)]
    mismatched = set(suite_cls.TEST_MANIFEST).symmetric_difference(method_list)
    if mismatched:
        pytest.fail(f'Test manifest not validated.  Missing/extra tests: {mismatched}.')
