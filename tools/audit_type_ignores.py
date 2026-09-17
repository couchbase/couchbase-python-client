"""Audit every ``# type: ignore`` in the tests_v2 trees.

The test trees allow suppressions under a narrow rule, and this enforces it:

1. **Never bare.**  ``# type: ignore`` with no code silences everything on that line
   forever, including the next, unrelated error to appear there.  The code names the
   error the suppression is about, and a comma-separated list names several, each held
   to rules 2 and 3 on its own.  ``enable_error_code`` in ``[tool.mypy]`` holds the same
   line and names the code that belongs there, which reading the text cannot do.
2. **Temporary suppressions carry a PYCBC key on the comment itself**, so the ticket that
   will retire them is readable at the site::

       cb_env.collection.unlock(key, res.cas)  # type: ignore[arg-type]  # PYCBC-1877

   ``warn_unused_ignores`` in ``[tool.mypy]`` then fails the build once the key's fix
   lands, which matters because that fix is made in the SDK tree by someone with no
   reason to open a test file.
3. **Permanent suppressions carry no key**, because there is no ticket to name.  The test
   is whether any future change would make the suppression unnecessary, not which error
   code it happens to be.  Two shapes qualify: ``[override]``, which is what running one
   suite against three api styles costs, and the entries in ``STRUCTURAL`` below.

``STRUCTURAL`` is an allowlist and not a pattern on purpose.  Every key-free suppression
that is not an ``[override]`` has to be added here by hand, which is the point at which
someone has to justify it.

Prefer a ``cast`` to a suppression wherever one expresses the claim.  ``cast(M, m)``
asserts a single conversion and leaves every other error on the line live; an ignore does
not.

Run directly, or via the ``audit-type-ignores`` pre-commit hook::

    python tools/audit_type_ignores.py

Exits 1, naming each offending site, when the rule is broken, and 2 when a tree it
audits does not exist.  Tests::

    python -m pytest tools/tests -q
"""

from __future__ import annotations

import pathlib
import re
import sys
from typing import (List,
                    Set,
                    Tuple)

# The trees audited, relative to the repo root.  The same four paths as ``files`` in
# ``[tool.mypy]``, and a tree renamed in one has to be renamed in the other.
ROOTS = (
    'tests_v2',
    'couchbase/tests_v2',
    'acouchbase/tests_v2',
    'txcouchbase/tests_v2',
)

# (path, error code) pairs allowed to carry no PYCBC key.  Add an entry only when no
# future change could retire the suppression, and comment why.
STRUCTURAL: Set[Tuple[str, str]] = {
    # Twisted's reactor is installed at runtime and typed as a module, so callLater is
    # invisible to the checker.  Not our code and not our stub, so there is no ticket to
    # point at.  A cast does not work either: zope declares interface methods without
    # self, so mypy reads IReactorTime.callLater's `delay` as the receiver and every
    # argument shifts by one.
    ('tests_v2/test_utils.py', 'attr-defined'),
    # datastructures_t iterates a CouchbaseMap inside pytest.raises(TypeError), because the
    # map deliberately implements neither __iter__ nor __aiter__: keys(), values() and
    # items() are its interface.  mypy is right about the line, and there is no ticket to
    # make it wrong.  The suppression is also the better half of the check, since
    # warn_unused_ignores fails the build if CouchbaseMap ever grows an __iter__.
    ('couchbase/tests_v2/datastructures_t.py', 'attr-defined'),
    # bucketmgmt_bucket_settings_t passes name=None to BucketSettings inside
    # pytest.raises(InvalidArgumentException), because rejecting it is the behaviour under
    # test.  mypy is right about the line and no ticket will ever make it wrong; a widened
    # annotation here would be the defect.  A cast is not the alternative it usually is,
    # since cast(str, None) asserts exactly what the test exists to disprove.
    ('couchbase/tests_v2/bucketmgmt_bucket_settings_t.py', 'call-overload'),
    # bucket_t and cluster_t pass a bare ServiceType where PingOptions wants an iterable of
    # them, inside pytest.raises(InvalidArgumentException), because rejecting the scalar is
    # the behaviour under test.  Same shape as the BucketSettings(name=None) entry above:
    # widening the annotation to accept it would be the defect, and a cast would assert the
    # very thing the test disproves.
    ('couchbase/tests_v2/bucket_t.py', 'call-overload'),
    ('couchbase/tests_v2/cluster_t.py', 'call-overload'),
    # The inverse of the two entries above, and the reason it is permanent is the same.
    # PingOptions.service_types is annotated homogeneous, Iterable[ServiceType] or
    # Iterable[str], because that is what callers should write.  The request builders convert
    # per element, so a mixed iterable works, and test_ping_mixed_services guards that it keeps
    # working.  Widening the annotation to promise the mixed form is the change nobody should
    # make, so no ticket retires this; PYCBC-1913 is where the homogeneous shape was chosen.
    ('couchbase/tests_v2/bucket_t.py', 'arg-type'),
    ('couchbase/tests_v2/cluster_t.py', 'arg-type'),
    # connection_t's three _fail suites hand the constructors and the profile registry the
    # wrong type on purpose, because rejecting it is the behaviour under test: a dict where
    # CertificateAuthenticator wants three named paths, None and {} in place of each path, a
    # plain dict where ClusterOptions wants a typed options object, and a class that has the
    # apply() the registry will call but is not a ConfigProfile.  Same shape as the
    # BucketSettings(name=None) entry above, and permanent for the same reason: widening any
    # of these annotations to accept the input would be the defect, and a cast would assert
    # exactly what the test exists to disprove.
    ('couchbase/tests_v2/connection_t.py', 'arg-type'),
    ('couchbase/tests_v2/connection_t.py', 'call-overload'),
    # tracing_kv_multi_t hands a legacy parent span to two multi option classes, because the
    # tracer the suite's cluster was built with is what produces the span: under
    # TracingType.Legacy there is no other kind to hand over, which is also the position a
    # caller who configured a deprecated tracer is in.  The multi classes annotate parent_span
    # as RequestSpan because that is what callers should write, while maybe_get_parent_span
    # accepts the legacy protocol with a deprecation warning, so the combination works and this
    # suite is what guards that it keeps working.  Same shape as the PingOptions.service_types
    # entry above: widening the annotation to promise the legacy form is the change nobody
    # should make, so no ticket retires this; PYCBC-1922 is where the narrow shape was chosen.
    ('couchbase/tests_v2/tracing_kv_multi_t.py', 'arg-type'),
    # The two search request builder suites pass a SearchRequest to build_search_query_request
    # inside pytest.raises(InvalidArgumentException), because refusing it is the behaviour
    # under test: search() takes a request and search_query() takes a query, and the refusal
    # is what tells the two entry points apart.  Same shape as the BucketSettings(name=None)
    # entry above, and permanent for the same reason: widening the annotation to accept a
    # request would erase the distinction, and a cast would assert exactly what the test
    # exists to disprove.
    ('couchbase/tests_v2/cluster_req_builder_t.py', 'arg-type'),
    ('couchbase/tests_v2/scope_req_builder_t.py', 'arg-type'),
    # collection_req_builder_t hands _get_scan_type the tag string it would have emitted for a
    # RangeScan, inside pytest.raises(InvalidArgumentException).  Same shape again, and the
    # near miss is the point: a caller who read the wire format rather than the api reaches
    # the else branch, and widening the annotation to accept a str would remove it.
    ('couchbase/tests_v2/collection_req_builder_t.py', 'arg-type'),
}

# Directory names skipped anywhere under ROOTS.
#
# ``_generated`` holds generated copies of the sync suites, so every suppression in it is
# already audited at the line it was authored on. Auditing both would double the count and
# report the copy's line number, which points at a file no one can edit: the fix is only
# ever made in the sync source, and arrives here by regenerating.
SKIP_DIRS = frozenset({'_generated'})

# Error codes that never need a key, whatever file they appear in.
KEYLESS_CODES = frozenset({'override'})

ANY_IGNORE = re.compile(r'#\s*type:\s*ignore')
CODED_IGNORE = re.compile(r'#\s*type:\s*ignore\[([a-z-]+(?:\s*,\s*[a-z-]+)*)\]')
KEY = re.compile(r'PYCBC-\d+')


class MissingRoots(Exception):
    """Paths in ROOTS that are not directories."""

    def __init__(self, missing: List[str]) -> None:
        super().__init__(', '.join(missing))
        self.missing = missing


class Site:
    """One ``# type: ignore`` occurrence."""

    def __init__(self, path: str, line: int, codes: Tuple[str, ...], key: str | None) -> None:
        self.path = path
        self.line = line
        # Empty for a bare ignore.
        self.codes = codes
        self.key = key

    def __str__(self) -> str:
        where = f'{self.path}:{self.line}'
        return f'{where:<50}[{", ".join(self.codes) or "BARE"}]  {self.key or ""}'.rstrip()


def collect(roots: Tuple[str, ...]) -> List[Site]:
    """Every ignore in *roots*, in path order.

    Done by reading files rather than by shelling out to grep: a long line truncated in a
    pipeline turns a well-formed ignore into an apparent bare one, and that reads as a
    violation of rule 1 that is not there.

    A root that is not a directory raises.  Walking past one audits a smaller population
    and exits 0, which is what a conforming tree also does, so the rename that ROOTS missed
    takes the check away and says nothing.
    """
    missing = [r for r in roots if not pathlib.Path(r).is_dir()]
    if missing:
        raise MissingRoots(missing)
    sites: List[Site] = []
    for root in roots:
        for path in sorted(pathlib.Path(root).rglob('*.py')):
            if SKIP_DIRS.intersection(path.parts):
                continue
            for num, text in enumerate(path.read_text().splitlines(), 1):
                if not ANY_IGNORE.search(text):
                    continue
                coded = CODED_IGNORE.search(text)
                keyed = KEY.search(text)
                codes = tuple(c.strip() for c in coded.group(1).split(',')) if coded else ()
                sites.append(Site(str(path), num, codes, keyed.group(0) if keyed else None))
    return sites


def violations(sites: List[Site]) -> List[Tuple[Site, str]]:
    """The sites that break the rule, each with the reason."""
    found: List[Tuple[Site, str]] = []
    for site in sites:
        if not site.codes:
            found.append((site, 'bare: needs a specific error code'))
        elif site.key is None and any(c not in KEYLESS_CODES and (site.path, c) not in STRUCTURAL
                                      for c in site.codes):
            found.append((site, 'needs a PYCBC key, or a STRUCTURAL entry saying why not'))
    return found


def main() -> int:
    try:
        sites = collect(ROOTS)
    except MissingRoots as ex:
        # 2 rather than 1, following mypy: the audit did not run, which is a different
        # thing to report than a tree that failed it.
        print(f'FAIL: {len(ex.missing)} audited tree(s) do not exist, so nothing was '
              'checked:', file=sys.stderr)
        for root in ex.missing:
            print(f'    {root}', file=sys.stderr)
        print('Run from the repo root.  If a tree was renamed, ROOTS and `files` in '
              '[tool.mypy] both name it.', file=sys.stderr)
        return 2

    keyed = [s for s in sites if s.key]
    keyless = [s for s in sites if not s.key]

    print(f'keyed ({len(keyed)}) - each waits on the SDK fix its ticket names:')
    for site in keyed:
        print(f'    {site}')
    print(f'\nkey-free ({len(keyless)}) - permanent, no ticket will retire these:')
    for site in keyless:
        print(f'    {site}')

    bad = violations(sites)
    if not bad:
        print(f'\nOK: {len(sites)} suppressions, all conforming.')
        return 0

    print(f'\nFAIL: {len(bad)} suppression(s) break the rule:', file=sys.stderr)
    for site, why in bad:
        print(f'    {site.path}:{site.line}  {why}', file=sys.stderr)
    return 1


if __name__ == '__main__':
    sys.exit(main())
