"""Hold the three public APIs to the same surface, with every difference listed and explained.

``couchbase``, ``acouchbase`` and ``txcouchbase`` are the same SDK three ways, so a public member on a
facade in one is expected on its counterpart in the other two, with the same parameters and the same
documented contract.  Each facade class in ``FACADES`` is imported with its counterparts and their
public members compared, inherited ones included.  A difference is a gap, named by the qualified name
it is about.  Three gaps are about existence:

* a counterpart class that does not exist: ``txcouchbase.management.eventing.EventingFunctionManager``
* a member the sync facade has and a counterpart lacks: ``txcouchbase.collection.Collection.scan``
* a member a counterpart has and the sync facade lacks: ``acouchbase.cluster.AsyncCluster.on_connect``

and five are about a member both sides have, written ``<member>(<aspect>)``:

* ``signature``: the parameters differ in name, kind or default.
* ``doc``: the sync member has a docstring and the counterpart has none.
* ``args``: the names under ``Args:`` differ, in order.
* ``raises``: the set of exceptions under ``Raises:`` differs.
* ``returns``: the type under ``Returns:`` differs.

One more is about a single facade rather than a pair, and is checked on every API, the sync one
included:

* ``positional-only``: a parameter sits before a ``/``, so it cannot be passed by name.  The options
  object is an ordinary parameter, so ``drop_bucket(bucket_name='b')`` binds and the names before
  it stay part of the call contract.

The docstring aspects compare what the API promises, not how it delivers it, so before comparing
``Async`` is dropped from a class name, a ``:class:`` role becomes its last dotted component, and
``Awaitable[T]`` or ``Deferred[T]`` under ``Returns:`` becomes ``T``.  A bare ``Awaitable`` or
``Deferred``, and a missing ``Returns:``, both read as ``None``.  A section is parsed as Sphinx's
napoleon does: an entry starts at the section's indent and its continuation lines sit deeper, so a
continuation line written at the entry's indent reads as an entry of its own and shows up as a
difference.  A member the sync facade leaves undocumented is not compared on its docstring.

Every gap has to match a line of ``api_parity_exemptions.txt``, which reads
``<qualified name or fnmatch pattern>: <why>``.  The reason is required.  The check fails on:

1. **A gap no exemption matches.**  Add the counterpart or make the two agree, or add an exemption
   saying why they differ.
2. **An exemption that matches no gap.**  The difference it explained is gone, so the line goes too,
   and the file is always the exact list of what differs.

Run directly, or via the ``check-api-parity`` pre-commit hook::

    python tools/check_api_parity.py           # check
    python tools/check_api_parity.py --list    # every gap, with the exemption that covers it

Exits 1 when the APIs and the exemptions disagree.  Tests::

    python -m pytest tools/tests -q
"""

from __future__ import annotations

import fnmatch
import importlib
import inspect
import pathlib
import re
import sys
from collections.abc import Iterable
from typing import NamedTuple

EXEMPTIONS = pathlib.Path(__file__).with_name('api_parity_exemptions.txt')

_MANAGERS = ('analytics.AnalyticsIndexManager', 'buckets.BucketManager', 'collections.CollectionManager',
             'eventing.EventingFunctionManager', 'eventing.ScopeEventingFunctionManager',
             'queries.QueryIndexManager', 'queries.CollectionQueryIndexManager', 'search.SearchIndexManager',
             'search.ScopeSearchIndexManager', 'users.UserManager', 'views.ViewIndexManager')
_DATASTRUCTURES = ('CouchbaseList', 'CouchbaseMap', 'CouchbaseSet', 'CouchbaseQueue')
_TRANSACTIONS = ('Transactions', 'AttemptContext')

# (sync, acouchbase, txcouchbase), by qualified name.
FACADES: tuple[tuple[str, str, str], ...] = (
    ('couchbase.cluster.Cluster', 'acouchbase.cluster.AsyncCluster', 'txcouchbase.cluster.Cluster'),
    ('couchbase.bucket.Bucket', 'acouchbase.bucket.AsyncBucket', 'txcouchbase.bucket.Bucket'),
    ('couchbase.scope.Scope', 'acouchbase.scope.AsyncScope', 'txcouchbase.scope.Scope'),
    ('couchbase.collection.Collection', 'acouchbase.collection.AsyncCollection',
     'txcouchbase.collection.Collection'),
    ('couchbase.binary_collection.BinaryCollection', 'acouchbase.binary_collection.BinaryCollection',
     'txcouchbase.binary_collection.BinaryCollection'),
    *((f'couchbase.management.{m}', f'acouchbase.management.{m}', f'txcouchbase.management.{m}')
      for m in _MANAGERS),
    *((f'couchbase.datastructures.{d}', f'acouchbase.datastructures.{d}', f'txcouchbase.datastructures.{d}')
      for d in _DATASTRUCTURES),
    *((f'couchbase.transactions.transactions.{t}', f'acouchbase.transactions.transactions.{t}',
       f'txcouchbase.transactions.transactions.{t}') for t in _TRANSACTIONS),
)


_ROLE = re.compile(r':\w+:`~?([\w.]+)`')
_ASYNC = re.compile(r'\bAsync(?=[A-Z])')
_WRAPPED = re.compile(r'`?(?:Awaitable|Deferred)`?(?:\[(.*)\])?')
_SECTIONS = ('Args', 'Raises', 'Returns')
_POSITIONAL_ONLY = '(positional-only)'


def _advice(gap: str) -> str:
    if gap.endswith(_POSITIONAL_ONLY):
        return ('a public parameter cannot be passed by name and no exemption covers it; remove the `/`, '
                'or add an exemption saying why it is there')
    if gap.endswith(')'):
        return ('the counterparts disagree and no exemption covers it; make them agree, or add an exemption '
                'saying why they differ')
    return ('exists on one side only and no exemption covers it; add the counterpart, or an exemption '
            'saying why it is absent')


class Exemption(NamedTuple):
    pattern: str
    reason: str


def resolve(qualname: str) -> type | None:
    """The class ``qualname`` names, or None when its module or the class does not exist."""
    module, _, name = qualname.rpartition('.')
    try:
        found = getattr(importlib.import_module(module), name, None)
    except ModuleNotFoundError as exc:
        # Only the counterpart module being absent is a gap.  Anything else it fails to import is
        # a broken tree, and is raised rather than reported as a missing class.
        if exc.name is None or not module.startswith(exc.name):
            raise
        return None
    return found if isinstance(found, type) else None


def public_members(cls: type) -> set[str]:
    return {name for klass in cls.__mro__ if klass is not object for name in vars(klass)
            if not name.startswith('_')}


def function(cls: type, name: str) -> object:
    """What ``cls.name`` runs: a property's getter, or the function under a static or class method."""
    member = next(vars(klass)[name] for klass in cls.__mro__ if name in vars(klass))
    if isinstance(member, property):
        return member.fget
    return getattr(member, '__func__', member)


def signature(func: object) -> list[tuple[str, str, str]] | None:
    try:
        params = inspect.signature(func).parameters.values()  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return [(p.name, p.kind.name, repr(p.default)) for p in params if p.name != 'self']


def _normalize(text: str) -> str:
    text = _ROLE.sub(lambda m: m.group(1).rpartition('.')[2], text)
    return _ASYNC.sub('', ' '.join(text.split()))


def sections(doc: str | None) -> dict[str, list[str]] | None:
    """The entries under each of ``Args:``, ``Raises:`` and ``Returns:``, or None with no docstring."""
    if not doc or not doc.strip():
        return None
    found: dict[str, list[str]] = {}
    current: list[str] | None = None
    indent = 0
    for line in inspect.cleandoc(doc).splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        depth = len(line) - len(line.lstrip())
        if depth == 0:
            heading = stripped[:-1] if stripped.endswith(':') else None
            current = found.setdefault(heading, []) if heading in _SECTIONS else None
            indent = 0
        elif current is not None:
            if not indent:
                indent = depth
            if depth <= indent:
                current.append(_normalize(stripped))
    return found


def _head(entry: str) -> str:
    """An entry up to the colon that ends its name or type, which is the first one followed by a space."""
    return re.split(r':(?:\s|$)', entry, maxsplit=1)[0].strip()


def documented(found: dict[str, list[str]]) -> dict[str, object]:
    """What the sections promise, in the form the three APIs are compared on."""
    returns = _head(found['Returns'][0]) if found.get('Returns') else 'None'
    wrapped = _WRAPPED.fullmatch(returns)
    if wrapped:
        returns = wrapped.group(1) or 'None'
    return {'args': [re.split(r'[\s(:]', e.lstrip('*'), maxsplit=1)[0] for e in found.get('Args', [])],
            'raises': sorted({_head(e) for e in found.get('Raises', [])}),
            'returns': returns}


def aspects(sync: type, counterpart: type, name: str) -> list[str]:
    """The aspects on which two facades' member ``name`` disagree."""
    ours, theirs = function(sync, name), function(counterpart, name)
    # A data attribute has no docstring of its own; reading one would compare its type's.
    if isinstance(ours, type) or not callable(ours):
        return []
    found = ['signature'] if signature(ours) != signature(theirs) else []
    our_doc = sections(getattr(ours, '__doc__', None))
    if our_doc is None:
        return found
    their_doc = sections(getattr(theirs, '__doc__', None))
    if their_doc is None:
        return [*found, 'doc']
    ours_promised, theirs_promised = documented(our_doc), documented(their_doc)
    return found + [a for a in ('args', 'raises', 'returns') if ours_promised[a] != theirs_promised[a]]


def positional_only(qualname: str, cls: type) -> list[str]:
    """``<qualname>.<member>(positional-only)`` for each public method of ``cls`` with a parameter
    before a ``/``."""
    found = []
    for name in sorted(public_members(cls)):
        func = function(cls, name)
        if isinstance(func, type) or not callable(func):
            continue
        if any(kind == 'POSITIONAL_ONLY' for _, kind, _ in signature(func) or ()):
            found.append(f'{qualname}.{name}{_POSITIONAL_ONLY}')
    return found


def gaps(facades: Iterable[tuple[str, ...]]) -> list[str]:
    """Every qualified name that exists on one side of a facade triple and not the other, and every
    member both sides have that they disagree on, as ``<member>(<aspect>)``, and every member of
    any of them with a positional-only parameter."""
    found: list[str] = []
    for sync_name, *counterparts in facades:
        sync = resolve(sync_name)
        if sync is None:
            raise LookupError(f'{sync_name} is in FACADES and does not exist')
        found += positional_only(sync_name, sync)
        sync_members = public_members(sync)
        for name in counterparts:
            counterpart = resolve(name)
            if counterpart is None:
                found.append(name)
                continue
            members = public_members(counterpart)
            found += [f'{name}.{m}' for m in sorted(sync_members - members)]
            found += [f'{name}.{m}' for m in sorted(members - sync_members)]
            found += [f'{name}.{m}({a})' for m in sorted(sync_members & members) for a in aspects(sync, counterpart, m)]
            found += positional_only(name, counterpart)
    return found


def read_exemptions(path: pathlib.Path) -> list[Exemption]:
    exemptions = []
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip() or line.startswith('#'):
            continue
        pattern, _, reason = line.partition(':')
        if not reason.strip():
            raise ValueError(f'{path.name}:{number}: an exemption needs a reason after the colon')
        exemptions.append(Exemption(pattern.strip(), reason.strip()))
    return exemptions


def covering(gap: str, exemptions: Iterable[Exemption]) -> Exemption | None:
    return next((e for e in exemptions if fnmatch.fnmatchcase(gap, e.pattern)), None)


def compare(found: Iterable[str], exemptions: list[Exemption]) -> list[str]:
    found = list(found)
    uncovered = [g for g in found if covering(g, exemptions) is None]
    unused = [e.pattern for e in exemptions if not any(fnmatch.fnmatchcase(g, e.pattern) for g in found)]
    return ([f'{g}: {_advice(g)}' for g in uncovered]
            + [f'{p}: the exemption matches no difference; remove it' for p in unused])


def main(argv: list[str]) -> int:
    root = pathlib.Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))
    found = gaps(FACADES)
    exemptions = read_exemptions(EXEMPTIONS)
    if '--list' in argv:
        for gap in found:
            exemption = covering(gap, exemptions)
            print(f'{gap}: {exemption.reason if exemption else "NOT EXEMPT"}')
        return 0
    problems = compare(found, exemptions)
    if not problems:
        print(f'OK: {len(FACADES)} facades agree across the three APIs, '
              f'with {len(found)} difference(s) under {len(exemptions)} exemption(s).')
        return 0
    print(f'FAIL: {len(problems)} disagreement(s) with {EXEMPTIONS.name}:', file=sys.stderr)
    for problem in problems:
        print(f'    {problem}', file=sys.stderr)
    return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
