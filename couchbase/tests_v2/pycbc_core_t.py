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

import gc
import json
import os
import subprocess
import sys
import sysconfig
from typing import Any, NamedTuple

import pytest

import couchbase
from couchbase.logic.pycbc_core import _core


class _CoreType(NamedTuple):
    # One row of the type table below, and one parametrization of most tests here.
    name: str
    # Py_TPFLAGS_BASETYPE.  Asserted against the type itself by the last test in this file.
    subclassable: bool
    ctor_kwargs: dict[str, Any]
    # Instances to create and destroy when sampling the type's refcount.
    cycles: int


_CYCLES = 25

_CORE_TYPES = [
    # Constructing a connection spawns the IO thread pool, so keep the instance count small.
    _CoreType('pycbc_connection', False, {}, 3),
    _CoreType('pycbc_exception', True, {}, _CYCLES),
    _CoreType('pycbc_hdr_histogram',
              True,
              {'lowest_discernible_value': 1, 'highest_trackable_value': 100000, 'significant_figures': 3},
              _CYCLES),
    _CoreType('pycbc_kv_request', True, {}, _CYCLES),
    _CoreType('pycbc_logger', True, {}, _CYCLES),
    _CoreType('pycbc_result', False, {}, _CYCLES),
    _CoreType('pycbc_scan_iterator', False, {}, _CYCLES),
    _CoreType('pycbc_streamed_result', False, {}, _CYCLES),
    _CoreType('transaction_config', True, {}, _CYCLES),
    _CoreType('transaction_get_multi_result', True, {}, _CYCLES),
    _CoreType('transaction_get_result', True, {}, _CYCLES),
    _CoreType('transaction_options', True, {}, _CYCLES),
    _CoreType('transaction_query_options', True, {}, _CYCLES),
]

# `transaction_operations` is an enum.Enum built during module init rather than one of our
# PyType_Spec types, but it is still a heap type `_core` exports.
_NON_SPEC_TYPES = frozenset(['transaction_operations'])

# Members the constructor is expected to initialize.  Not derived from `_core.pyi`:
# `pycbc_kv_request` declares Optional members that tp_new deliberately leaves NULL for the
# request builder to fill in.
_INITIALIZED_MEMBERS = [
    ('pycbc_exception', ('core_span', 'start_time', 'end_time')),
    ('pycbc_result', ('core_span', 'start_time', 'end_time')),
    ('pycbc_streamed_result', ('core_span', 'start_time', 'end_time')),
]

_PACKAGE_PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(couchbase.__file__)))

_SUBCLASS_SCRIPT = """
import json
import sys

from couchbase.logic.pycbc_core import _core

base = getattr(_core, sys.argv[1])
ctor_kwargs = json.loads(sys.argv[2])


class _Subclass(base):
    pass


for _ in range(50):
    _Subclass(**ctor_kwargs)
"""


# Several `_core` types deref internal C++ pointers from __repr__, which is undefined on a
# default-constructed instance, so no test may put a bare instance in an assert or a pytest id.
def _core_type_id(core_type: _CoreType) -> str:
    return core_type.name


def _member_init_id(param: Any) -> str:
    return str(param[0]) if isinstance(param, tuple) else str(param)


class PycbcCoreTypeUnitTests:
    # Object-model invariants for the `_core` heap types, which are refcounted by their
    # instances and subclassable.  Both properties introduce defects that show up only on
    # teardown, so no api-level suite can observe them: the objects behave correctly and leak
    # or corrupt memory on the way out.  Runs in both the stable-ABI and the full C-API build.
    #
    # No cb_env and no TEST_MANIFEST, and not generated: `_core` is one shared library that
    # all three trees import, so a generated copy would sample the same refcounts.

    # Samples the type object's refcount, not the instance's.  A static type is never increfed
    # by its instances, but PyType_GenericAlloc takes a reference on a heap type that
    # tp_dealloc owes back, and skipping it is invisible from Python apart from this number.
    @pytest.mark.parametrize('core_type', _CORE_TYPES, ids=_core_type_id)
    def test_type_refcount_is_stable(self, core_type: _CoreType) -> None:
        if sysconfig.get_config_var('Py_GIL_DISABLED'):
            pytest.skip('Deferred refcounting makes the refcount delta meaningless on a free-threaded build.')

        core_type_obj = getattr(_core, core_type.name)

        def cycle() -> None:
            for _ in range(core_type.cycles):
                # Discarded immediately, which is the point: it must release the reference
                # PyType_GenericAlloc took on its type.
                core_type_obj(**core_type.ctor_kwargs)

        cycle()  # Warm-up, so first-instance caching is not counted against the sample.
        # These types are not GC-tracked today, so the collect only guards a future one.
        gc.collect()
        refcount_before = sys.getrefcount(core_type_obj)
        cycle()
        gc.collect()
        refcount_after = sys.getrefcount(core_type_obj)

        assert refcount_after == refcount_before, (f'{core_type.name} refcount grew by '
                                                   f'{refcount_after - refcount_before} over '
                                                   f'{core_type.cycles} instances.')

    # A Python subclass sets Py_TPFLAGS_HAVE_GC, putting a PyGC_Head in front of the instance,
    # so a tp_dealloc freeing with PyObject_Free instead of the type's own Py_tp_free slot
    # hands the allocator a pointer into the middle of its own block.  Untagged, that free
    # usually succeeds.
    @pytest.mark.parametrize('core_type', [t for t in _CORE_TYPES if t.subclassable], ids=_core_type_id)
    def test_subclass_alloc_and_free(self, core_type: _CoreType) -> None:
        # PYTHONMALLOC only takes effect at interpreter startup and a detected corruption
        # aborts, so this has to be a child process read by exit code.
        env = dict(os.environ)
        env['PYTHONMALLOC'] = 'debug'
        # Keep the child on the parent's couchbase package, whatever the working directory.
        env['PYTHONPATH'] = os.pathsep.join(filter(None, [_PACKAGE_PARENT_DIR, os.environ.get('PYTHONPATH')]))
        proc = subprocess.run([sys.executable,
                               '-c',
                               _SUBCLASS_SCRIPT,
                               core_type.name,
                               json.dumps(core_type.ctor_kwargs)],
                              env=env,
                              capture_output=True,
                              text=True,
                              timeout=120)
        assert proc.returncode == 0, (f'Subclassing {core_type.name} exited with {proc.returncode} '
                                      f'under PYTHONMALLOC=debug.\n{proc.stderr}')

    @pytest.mark.parametrize('type_name, members', _INITIALIZED_MEMBERS, ids=_member_init_id)
    def test_constructor_initializes_members(self, type_name: str, members: tuple[str, ...]) -> None:
        instance = getattr(_core, type_name)()
        for member in members:
            # These are Py_T_OBJECT_EX, so a lost initializer turns an Optional attribute into
            # an AttributeError rather than a None.
            assert getattr(instance, member) is None, f'{type_name}.{member} is not None.'

    # _CORE_TYPES drives every test above, so a type added to `_core` without a row would be
    # silently untested rather than a failure.
    def test_type_table_covers_every_core_type(self) -> None:
        exported = set(name for name, obj in vars(_core).items() if isinstance(obj, type))
        assert exported - _NON_SPEC_TYPES == set(t.name for t in _CORE_TYPES)

    # Guards the subclass test's filter.  A row wrongly marked True fails there loudly, but
    # one wrongly marked False drops the type from that sweep with no failure anywhere.
    def test_type_table_records_subclassability(self) -> None:
        for core_type in _CORE_TYPES:
            core_type_obj = getattr(_core, core_type.name)
            # Derived by subclassing rather than by reading __flags__, which would hardcode a
            # CPython bit.
            try:
                type('_Subclass', (core_type_obj,), {})
                subclassable = True
            except TypeError:
                subclassable = False
            assert subclassable is core_type.subclassable, (f'{core_type.name} subclassable='
                                                            f'{subclassable}, table says '
                                                            f'{core_type.subclassable}.')
