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

import pickle
import traceback

import pytest

import couchbase.exceptions as E
from couchbase.exceptions import CouchbaseException


def _exception_classes() -> list[type[CouchbaseException]]:
    # `dir()` returns names, and three of them are aliases assigned at the bottom of the
    # module, so iterating names constructs three classes twice.  Deduplicating by identity
    # is also what makes the alias claim testable separately, below.
    seen: dict[int, type[CouchbaseException]] = {}
    for name in dir(E):
        obj = getattr(E, name)
        if isinstance(obj, type) and issubclass(obj, CouchbaseException) and obj is not CouchbaseException:
            seen[id(obj)] = obj
    return sorted(seen.values(), key=lambda c: c.__name__)


class CouchbaseExceptionUnitTests:
    # No TEST_MANIFEST: there is no `acouchbase.exceptions` or `txcouchbase.exceptions` to be
    # in parity with, and all three apis raise and catch these same classes.  That is why
    # this file is absent from the generator's suite table rather than carrying an exclude.

    MESSAGE = 'This is a test message.'

    @pytest.fixture(scope='class', name='cb_exceptions')
    def get_couchbase_exceptions(self) -> list[type[CouchbaseException]]:
        return _exception_classes()

    def test_couchbase_exception_base(self) -> None:
        base = CouchbaseException(message=self.MESSAGE)
        assert isinstance(base, Exception)
        assert isinstance(base, CouchbaseException)
        assert base.message == self.MESSAGE
        # The base renders its detail list bare; every subclass wraps that in its own name.
        assert str(base) == f'<message={self.MESSAGE}>'

    def test_exceptions_create_only_message(self, cb_exceptions: list[type[CouchbaseException]]) -> None:
        assert len(cb_exceptions) > 50, 'the module namespace scan found almost nothing, so it is broken'
        for ex in cb_exceptions:
            ex_str = str(ex(self.MESSAGE))
            assert ex_str.startswith(f'{ex.__name__}('), ex_str
            assert f'message={self.MESSAGE}' in ex_str, ex_str

    def test_deprecated_aliases_name_the_class_they_replace(self) -> None:
        # Assigned rather than subclassed, so `except CASMismatchException` and
        # `except CasMismatchException` catch the same object.  Nothing warns on use of the
        # old spelling, which is what keeps them safe to leave in place.
        assert E.CASMismatchException is E.CasMismatchException
        assert E.SubdocCantInsertValueException is E.InvalidValueException
        assert E.SubdocPathMismatchException is E.PathMismatchException

    def test_every_class_names_the_public_module(self) -> None:
        classes = [getattr(E, name) for name in E.__all__ if isinstance(getattr(E, name), type)]
        assert len(classes) > 100, 'the __all__ scan found almost nothing, so it is broken'
        assert {cls.__module__ for cls in classes} == {'couchbase.exceptions'}

    def test_traceback_names_the_public_module(self) -> None:
        ex = E.DocumentNotFoundException(self.MESSAGE)
        line = traceback.format_exception_only(type(ex), ex)[-1]
        assert line.startswith('couchbase.exceptions.DocumentNotFoundException: '), line

    def test_pickle_names_the_public_module(self, cb_exceptions: list[type[CouchbaseException]]) -> None:
        # A pickle records the class path, so one written here has to load on a release where the
        # classes are only reachable as `couchbase.exceptions`.
        for ex in cb_exceptions:
            payload = pickle.dumps(ex(self.MESSAGE))
            assert b'couchbase.common' not in payload, ex
            assert type(pickle.loads(payload)) is ex

    def test_couchbase_exception_positional_message(self) -> None:
        # Regression guard (PYCBC-1926): the base declares `base` first and every one of its 93
        # subclasses declares `message` first, so a bare string used to land in `_base` and
        # __repr__ then called .err() on it.  str() raised AttributeError rather than reporting
        # anything, and __str__ delegates to __repr__, so print and traceback rendering went with
        # it.  The base now routes a leading string the way its subclasses always have.
        ex = CouchbaseException(self.MESSAGE)
        assert ex.message == self.MESSAGE
        assert str(ex) == f'<message={self.MESSAGE}>'
        # The core-driven path is the one this must not disturb: ErrorMapper hands the real error
        # object in by keyword, and that still populates `_base` rather than the message.
        assert CouchbaseException(message=self.MESSAGE).message == self.MESSAGE
