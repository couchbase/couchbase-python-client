"""Rewrite a sync suite in couchbase/tests_v2 into its acouchbase or txcouchbase form.

Two choices here are load-bearing.

**1. Analyse with `ast`, rewrite the source text.**  The obvious shape, parse -> mutate ->
`ast.unparse`, cannot be used: `ast` discards comments, and the suites carry comments that
must survive verbatim.  A `@FINDING(PYCBC-nnnn)` block names the SDK defect the test below
it works around, and the Apache header is a licensing requirement.  `ast.unparse` also renormalises every string
quote, line break and parenthesis, which turns reviewing a generated diff into reading a
full-file rewrite.  So the AST is used only to *locate* edits, by (line, col), and the edits
are applied to the original text.  Formatting and comments are preserved by construction,
because untouched bytes are never re-emitted.

**2. Unresolved sites are reported, never guessed.**  See resolver.py.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass, field
from types import ModuleType

from tools.testgen.resolver import (PURE_HELPER_NAMESPACES,
                                    Kind,
                                    Resolver)

# ---------------------------------------------------------------------------
# Identifier substitutions
# ---------------------------------------------------------------------------

ACB_NAME_MAP = {
    'SyncTestEnvironment': 'AsyncTestEnvironment',
    'TestEnvironment': 'AsyncTestEnvironment',
    'build_environment': 'build_async_environment',
    'teardown_environment': 'teardown_async_environment',
    'cb_base_env': 'acb_base_env',
    'cb_base_txn_env': 'acb_base_txn_env',
    # Managers.  A suite declares MANAGERS = [TestEnvironmentQueryIndexManager] and then
    # asks for it back with cb_env.get_manager(TestEnvironmentQueryIndexManager); the
    # registry is keyed by the class object, so rewriting the name in both places keeps
    # them consistent and needs no special case in the environment.
    'TestEnvironmentQueryIndexManager': 'AsyncTestEnvironmentQueryIndexManager',
    'TestEnvironmentCollectionQueryIndexManager': 'AsyncTestEnvironmentCollectionQueryIndexManager',
    'TestEnvironmentAnalyticsIndexManager': 'AsyncTestEnvironmentAnalyticsIndexManager',
    'TestEnvironmentBucketManager': 'AsyncTestEnvironmentBucketManager',
    'TestEnvironmentCollectionManager': 'AsyncTestEnvironmentCollectionManager',
    'TestEnvironmentCollectionMgmtManager': 'AsyncTestEnvironmentCollectionMgmtManager',
    'TestEnvironmentSearchIndexManager': 'AsyncTestEnvironmentSearchIndexManager',
    'TestEnvironmentScopeSearchIndexManager': 'AsyncTestEnvironmentScopeSearchIndexManager',
    'TestEnvironmentEventingFunctionManager': 'AsyncTestEnvironmentEventingFunctionManager',
    'TestEnvironmentScopeEventingFunctionManager': 'AsyncTestEnvironmentScopeEventingFunctionManager',
    'TestEnvironmentUserManager': 'AsyncTestEnvironmentUserManager',
    'TestEnvironmentViewIndexManager': 'AsyncTestEnvironmentViewIndexManager',
    # Query, analytics, search and views environments.
    'QueryEnvironment': 'AsyncQueryEnvironment',
    'AnalyticsEnvironment': 'AsyncAnalyticsEnvironment',
    'SearchEnvironment': 'AsyncSearchEnvironment',
    'ViewsEnvironment': 'AsyncViewsEnvironment',
    'EventingEnvironment': 'AsyncEventingEnvironment',
    'QueryIndexManagementEnvironment': 'AsyncQueryIndexManagementEnvironment',
    'CollectionQueryIndexManagementEnvironment': 'AsyncCollectionQueryIndexManagementEnvironment',
}

TXCB_NAME_MAP = {
    'SyncTestEnvironment': 'TwistedTestEnvironment',
    'TestEnvironment': 'TwistedTestEnvironment',
    'build_environment': 'build_twisted_environment',
    'teardown_environment': 'teardown_twisted_environment',
    'cb_base_env': 'txcb_base_env',
    'cb_base_txn_env': 'txcb_base_txn_env',
    'TestEnvironmentQueryIndexManager': 'TwistedTestEnvironmentQueryIndexManager',
    # Maps to a class that deliberately does not exist, and that is the point.  txcouchbase
    # has no collection-level query index manager, so a suite retargeted here without
    # excluding its collection-level class rewrites this name, the resolver cannot find the
    # class, and it lands under MISSING API-SPECIFIC MANAGER CLASSES at --write time.  Delete
    # the entry and the sync name passes through instead, which resolves, and the failure
    # moves back to a real cluster.  See the note at the end of managers/query_index.py.
    'TestEnvironmentCollectionQueryIndexManager': 'TwistedTestEnvironmentCollectionQueryIndexManager',
    'TestEnvironmentAnalyticsIndexManager': 'TwistedTestEnvironmentAnalyticsIndexManager',
    'TestEnvironmentBucketManager': 'TwistedTestEnvironmentBucketManager',
    'TestEnvironmentCollectionManager': 'TwistedTestEnvironmentCollectionManager',
    'TestEnvironmentCollectionMgmtManager': 'TwistedTestEnvironmentCollectionMgmtManager',
    'TestEnvironmentSearchIndexManager': 'TwistedTestEnvironmentSearchIndexManager',
    'TestEnvironmentScopeSearchIndexManager': 'TwistedTestEnvironmentScopeSearchIndexManager',
    'TestEnvironmentUserManager': 'TwistedTestEnvironmentUserManager',
    'TestEnvironmentViewIndexManager': 'TwistedTestEnvironmentViewIndexManager',
    'AnalyticsEnvironment': 'TwistedAnalyticsEnvironment',
    'SearchEnvironment': 'TwistedSearchEnvironment',
    'ViewsEnvironment': 'TwistedViewsEnvironment',
    'QueryEnvironment': 'TwistedQueryEnvironment',
    'QueryIndexManagementEnvironment': 'TwistedQueryIndexManagementEnvironment',
    'CollectionQueryIndexManagementEnvironment': 'TwistedCollectionQueryIndexManagementEnvironment',
}

# Module paths that have an api-specific counterpart.  Everything else in `couchbase.*`
# (options, exceptions, result types, subdocument) is shared and must NOT be rewritten:
# acouchbase re-exports some of it and not the rest, so a blanket prefix swap produces
# imports that do not exist.
API_MODULES = {
    'couchbase.cluster', 'couchbase.bucket', 'couchbase.scope', 'couchbase.collection',
    'couchbase.binary_collection', 'couchbase.datastructures', 'couchbase.transactions',
    'couchbase.kv_range_scan', 'couchbase.management.buckets',
    'couchbase.management.queries', 'couchbase.management.collections',
    'couchbase.management.users', 'couchbase.management.views',
    'couchbase.management.search',
    'couchbase.management.analytics',
}

# Deliberately not in the set above: couchbase.management.eventing.  Its acouchbase
# counterpart re-exports three of the module's names alongside the two manager classes, and
# a suite that imports the settings, keyspace, binding and enum types by name would rewrite
# to a module holding none of them.  Nothing needs the rewrite either: the eventing suites
# reach both managers through the framework's own manager classes, which ACB_NAME_MAP
# rewrites, and every other name they import is api-independent.

# Root of the shared test-framework package.  Every module under it must be declared in
# ENV_MODULE_MAP for every api, identity included; see the blocker in rewrite_imports().
ENV_PACKAGE = 'tests_v2.environments'

ENV_MODULE_MAP = {
    'acouchbase': {
        # Identity entry, and it is load-bearing.  The managers module does not move, but
        # the *names* imported from it do (TestEnvironmentBucketManager ->
        # AsyncTestEnvironmentBucketManager).  Without an entry here rewrite_imports()
        # skips the statement entirely, the body still gets the rewritten name, and the
        # generated file raises NameError at import, once per rewritten name.
        'tests_v2.environments.managers': 'tests_v2.environments.managers',
        'tests_v2.environments.test_environment': 'tests_v2.environments.async_environment',
        'tests_v2.environments.query_environment': 'tests_v2.environments.async_query_environment',
        'tests_v2.environments.query_index_mgmt_environment':
            'tests_v2.environments.async_query_index_mgmt_environment',
        'tests_v2.environments.analytics_environment':
            'tests_v2.environments.async_analytics_environment',
        'tests_v2.environments.search_environment': 'tests_v2.environments.async_search_environment',
        'tests_v2.environments.views_environment': 'tests_v2.environments.async_views_environment',
        'tests_v2.environments.eventing_environment': 'tests_v2.environments.async_eventing_environment',
        # The first environment module that maps to itself, and it is not an oversight.  Every
        # other one here has an asyncio twin because it drives the SDK; this one issues
        # management REST calls and nothing else, so there is no api-specific form of it to
        # name.  Still an entry rather than an omission, for the reason stated above the
        # managers line: omission is the blocker.
        'tests_v2.environments.rate_limit_environment': 'tests_v2.environments.rate_limit_environment',
        # Api-independent: they build names, index definitions and view-key arithmetic out of
        # strings, with no SDK call anywhere in either, so all three trees import the same
        # module.  Identity entries rather than omissions, because omission is the blocker
        # below.
        'tests_v2.search_index_defs': 'tests_v2.search_index_defs',
        'tests_v2.view_defs': 'tests_v2.view_defs',
        'tests_v2.eventing_defs': 'tests_v2.eventing_defs',
        # Identity, and for a different reason than the three above: these two *are*
        # environment modules, and they are still shared.  Nothing in either awaits.  A meter
        # and a tracer are both configured through the shared couchbase.options.ClusterOptions
        # all three apis take, and both are read back off
        # cluster._impl.observability_instruments, which all three hold under that name.  So
        # there is no async or twisted counterpart to name, and writing one would be three
        # copies of the same handful of statements.
        'tests_v2.environments.metrics_environment': 'tests_v2.environments.metrics_environment',
        'tests_v2.environments.tracing_environment': 'tests_v2.environments.tracing_environment',
    },
    # No eventing entries here, and the omission is deliberate.  txcouchbase has no eventing
    # surface to wrap, so there is no twisted eventing environment to name and no txcouchbase
    # suite that imports one.  If a suite is ever retargeted here it hits the blocker below,
    # which is the loud failure this map is shaped to produce.
    'txcouchbase': {
        'tests_v2.environments.managers': 'tests_v2.environments.managers',
        'tests_v2.environments.test_environment': 'tests_v2.environments.twisted_environment',
        'tests_v2.environments.analytics_environment':
            'tests_v2.environments.twisted_analytics_environment',
        'tests_v2.environments.query_environment': 'tests_v2.environments.twisted_query_environment',
        'tests_v2.environments.query_index_mgmt_environment':
            'tests_v2.environments.twisted_query_index_mgmt_environment',
        'tests_v2.environments.search_environment': 'tests_v2.environments.twisted_search_environment',
        'tests_v2.environments.views_environment': 'tests_v2.environments.twisted_views_environment',
        'tests_v2.search_index_defs': 'tests_v2.search_index_defs',
        'tests_v2.view_defs': 'tests_v2.view_defs',
        # Identity, for the reason given at the acouchbase entries above.
        'tests_v2.environments.metrics_environment': 'tests_v2.environments.metrics_environment',
        'tests_v2.environments.tracing_environment': 'tests_v2.environments.tracing_environment',
    },
}


@dataclass
class Edit:
    """A single text replacement, addressed by 0-based line and column."""
    line: int
    col: int
    end_line: int
    end_col: int
    text: str
    why: str

    def key(self) -> tuple[int, int]:
        return (self.line, self.col)


@dataclass
class Report:
    """What the transformer could not do, and why."""
    unresolved: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    long_lines: list[str] = field(default_factory=list)
    # Per-test outcome.  The four buckets are not cosmetic: "async and still has an
    # unresolved call" is the only one that is actively dangerous, because the emitted
    # test looks finished and drops a coroutine at runtime.
    tests_clean: list[str] = field(default_factory=list)
    tests_incomplete: list[str] = field(default_factory=list)
    sync_tests_blocked: list[str] = field(default_factory=list)
    sync_tests_genuine: list[str] = field(default_factory=list)
    # Deliberately not generated for this api, via @pytest.mark.testgen_exclude.  Separate
    # from the four analysis buckets because it is an authoring decision, not a finding:
    # counting it as "genuinely sync" would hide a shrinking generated suite.
    tests_excluded: list[str] = field(default_factory=list)
    # The same mark applied to a whole class.  Kept apart from tests_excluded so the
    # per-test totals stay comparable across runs: a class exclusion removes tests that
    # were never analysed, and folding them in would make the test count move for a reason
    # that has nothing to do with resolution.
    classes_excluded: list[str] = field(default_factory=list)


@dataclass
class Ctx:
    """Per-function analysis context.

    `scope` maps a local name to the async class it holds.  `fns` maps a nested def's
    name to its stable key, so a call to `txn_logic()` finds *this* method's closure and
    not one of the 35 others in the same file that share the name.  `getattr_bound` maps
    a local the source bound through `getattr` to the line it was bound on, which is the
    only thing that tells a call through that local apart from any other bare-name call.
    """
    scope: dict[str, str]
    fns: dict[str, str]
    owner: str
    getattr_bound: dict[str, int]


# Receiver roots worth reporting when they cannot be typed.  A bare `pytest.raises()` or
# `timedelta(...)` is not interesting; an untyped `qixm.create_index()` is the whole
# question.  Anything bound from cb_env.get_manager() lands here.
TRACKED_ROOTS = {'cb_env', 'qixm', 'bm', 'ctx', 'coll', 'cluster', 'scope', 'bucket',
                 'mgr', 'ixm', 'cm'}


def is_fixture_decorator(dec: ast.expr) -> bool:
    """True only for `@pytest.fixture(...)` / `@pytest_asyncio.fixture(...)`.

    The obvious test, `'fixture' in ast.unparse(dec)`, also matches
    `@pytest.mark.usefixtures('check_x_supported')`.  Every test in bucketmgmt_t and
    querymgmt_t carries one of those, so a substring test files all of them as fixtures and
    drops them from the report entirely: a silent undercount, and the kind that reads as a
    clean result.
    """
    target = dec.func if isinstance(dec, ast.Call) else dec
    return ast.unparse(target) in ('pytest.fixture', 'pytest_asyncio.fixture')


def testgen_excluded_apis(dec: ast.expr) -> set[str]:
    """The api names listed in `@pytest.mark.testgen_exclude('acouchbase', ...)`.

    A mark rather than a comment so it is greppable, registered in pyproject.toml, and
    visible to pytest tooling; the sync suite ignores it at runtime.
    """
    if not isinstance(dec, ast.Call):
        return set()
    if ast.unparse(dec.func) != 'pytest.mark.testgen_exclude':
        return set()
    return {a.value for a in dec.args if isinstance(a, ast.Constant) and isinstance(a.value, str)}


def dotted(node: ast.AST) -> str:
    parts: list[str] = []
    cur: ast.AST = node
    while isinstance(cur, ast.Attribute):
        parts.append(cur.attr)
        cur = cur.value
    if isinstance(cur, ast.Name):
        parts.append(cur.id)
    else:
        return ''
    return '.'.join(reversed(parts))


class SuiteTransformer:
    """Builds the edit list for one api variant of one source suite.

    Subclassed once per api.  Everything that differs between acouchbase and txcouchbase is
    either a table (`name_map`, and the resolver's `Model`) or one of four hooks:
    `_bridge`, `finish_function`, `propagates`, and `bridge_needs_import`.  Nothing else in
    this class knows which api it is serving.

    `propagates` is the deepest of the four and the least obvious.  Under acouchbase, a
    helper containing an awaited call *becomes* a coroutine, so every call to it also needs
    awaiting, and the analysis is a fixpoint that spreads outward from the SDK.  Under
    txcouchbase nothing spreads: run_in_reactor_thread() blocks, so a helper that uses it is
    still an ordinary function and its callers are unaffected.  Running the acouchbase
    propagation on a Twisted suite would wrap calls to plain Python helpers, and each one
    would dispatch that helper to the reactor thread, where its own internal
    run_in_reactor_thread() deadlocks against the reactor it is already running on.
    """

    api = ''
    name_map: dict[str, str] = {}
    # See the class docstring.  True for acouchbase, False for txcouchbase.
    propagates = True

    def __init__(self, src: str, path: str, resolver: Resolver, report: Report) -> None:
        self.model = resolver.model
        # How a report line names this file.  The api has to be on it: both variants
        # of one suite report against the same source path and line numbers, so
        # without the prefix an acouchbase finding and a txcouchbase finding at the
        # same site collapse into one line and the per-file counts merge the two.
        self.rpath = f'{self.api}:{path}'
        self.env_class = resolver.model.env_class
        self.src = src
        self.path = path
        self.lines = src.splitlines(keepends=True)
        self.tree = ast.parse(src)
        self.res = resolver
        self.report = report
        self.edits: list[Edit] = []
        # (id(node), text) of every _insert(), so a re-analysis pass does not repeat one.
        self.inserted: set[tuple[int, str]] = set()
        # fn_key -> True once the function is known to need `async def`
        self.async_fns: dict[str, bool] = {}
        self.changed_this_pass = False
        self.needs_pytest_asyncio = False
        self.needs_async_generator = False
        self.parents: dict[int, ast.AST] = {}
        # method names on the class currently being analysed that are known to be
        # coroutines.  Reset per class by transform_class().
        self.async_methods: set[str] = set()
        # fn_key of every function containing at least one call the resolver could not
        # answer.  Used to separate "genuinely synchronous" from "blocked on missing
        # infrastructure", which are very different things to report.
        self.blocked_fns: set[str] = set()
        self._seen_unresolved: set[str] = set()
        # class name -> the coroutine method names it exposes to its subclasses
        self.class_async_methods: dict[str, set[str]] = {}
        # 0-based [start, end) line spans removed by testgen_exclude, and the Edits that
        # remove them.  Kept apart so the cleanup pass can tell a deletion from an edit
        # that merely happens to sit inside one.
        self.deleted_spans: list[tuple[int, int]] = []
        self.deletion_edits: list[Edit] = []

    # -- type environment ---------------------------------------------------

    def receiver_type(self, node: ast.AST, scope: dict[str, str]) -> str | None:
        """Best-effort type name for the receiver of an attribute access."""
        if isinstance(node, ast.Name):
            return self.name_type(node, scope)
        if isinstance(node, ast.Attribute):
            return self.attribute_type(node, scope)
        if isinstance(node, ast.Call):
            return self.call_type(node, scope)
        return None

    def name_type(self, node: ast.Name, scope: dict[str, str]) -> str | None:
        local = scope.get(node.id)
        if local is not None:
            return local
        # A bare class name used as a receiver: `TestEnvironment.try_n_times(...)`, which
        # the name map has already turned into AsyncTestEnvironment, and whose async form
        # IS a coroutine.  collection_t alone reaches this on 32 sites, and treating the
        # Name as untypeable drops every one of them silently.
        mapped = self.name_map.get(node.id, node.id)
        if mapped in self.res.classes or mapped in PURE_HELPER_NAMESPACES:
            return mapped
        return None

    def attribute_type(self, node: ast.Attribute, scope: dict[str, str]) -> str | None:
        base = self.receiver_type(node.value, scope)
        if base == self.env_class:
            return self.model.env_attr_types.get(node.attr)
        # A pure namespace stays pure through a nested attribute.  `SD.get(...)` resolved
        # already because its receiver is the bare name, but `SD.MutationMacro.cas()` asks
        # return_type('SD', 'MutationMacro') first, gets None, and the whole call is then
        # reported unresolved.  Nothing reachable under a declared-pure namespace touches
        # the SDK, which is the property PURE_HELPER_NAMESPACES asserts, so the nesting
        # depth cannot change the answer.  Without this, subdoc_t's five macro tests read
        # as INCOMPLETE in both mirrors on the strength of SD.MutationMacro.cas() and
        # SD.LookupInMacro.document().
        if base in PURE_HELPER_NAMESPACES:
            return base
        if base is not None:
            return self.res.return_type(base, node.attr)
        return None

    def call_type(self, node: ast.Call, scope: dict[str, str]) -> str | None:
        f = node.func
        if isinstance(f, ast.Name):
            return self.res.function_return_type(self.name_map.get(f.id, f.id))
        if not isinstance(f, ast.Attribute):
            return None
        # `Cluster.connect(...)` names its own type through the Name, which local scope
        # knows nothing about.
        if isinstance(f.value, ast.Name):
            ctor = self.model.static_ctor_types.get((f.value.id, f.attr))
            if ctor:
                return ctor
        base = self.receiver_type(f.value, scope)
        return self.res.return_type(base, f.attr) if base else None

    # -- edit emission ------------------------------------------------------

    def _insert(self, node: ast.AST, text: str, why: str) -> None:
        """Queue `text` at the start of `node`, once per node however many passes ask.

        Keyed by node rather than by position: in `a().b().c`, both calls start at the same
        column and each needs its own `(await `.
        """
        if (id(node), text) in self.inserted:
            return
        self.inserted.add((id(node), text))
        self.edits.append(Edit(node.lineno - 1, node.col_offset, node.lineno - 1,
                               node.col_offset, text, why))
        self.changed_this_pass = True

    def _cut(self, after: ast.AST, through: ast.AST, why: str) -> None:
        """Delete from the end of `after` to the end of `through`, separators included.

        Dropping a trailing element from a bracketed list, where replacing the list itself
        would re-render everything between the brackets and lose the source's own wrapping.
        """
        e = Edit((after.end_lineno or after.lineno) - 1, after.end_col_offset or 0,
                 (through.end_lineno or through.lineno) - 1, through.end_col_offset or 0,
                 '', why)
        if any(x.key() == e.key() for x in self.edits):
            return
        self.edits.append(e)
        self.changed_this_pass = True

    def _replace(self, node: ast.AST, text: str, why: str) -> None:
        """Replace a node's whole span, discarding any edit already queued inside it.

        Edits at the text level stop being independent as soon as one spans another.
        Applying both in reverse order writes the inner one first, then overwrites from the
        outer start to an end column the inner edit has already moved, which truncates the
        outer span and can leave a file that does not parse.  An outer replacement renders
        whatever it contains, so anything within it is dropped rather than applied.

        Nothing in the corpus overlaps today, because the one rewrite that would, the return
        annotation, edits in place to keep the source's wrapping.  The guard stays because
        what it prevents is a file that does not parse, and it covers the next whole-node
        replacement to span a mapped name without anyone having to notice.
        """
        e = Edit(node.lineno - 1, node.col_offset,
                 (node.end_lineno or node.lineno) - 1, node.end_col_offset or node.col_offset,
                 text, why)
        if any(x.key() == e.key() for x in self.edits):
            return

        def within(x: Edit) -> bool:
            return ((x.line, x.col) >= (e.line, e.col)
                    and (x.end_line, x.end_col) <= (e.end_line, e.end_col))

        self.edits = [x for x in self.edits if not within(x)]
        self.edits.append(e)
        self.changed_this_pass = True

    def _needs_parens(self, call: ast.Call) -> bool:
        """`await x.get(k).content_as[dict]` parses as `await (x.get(k).content_as[dict])`,
        which awaits the wrong thing.  Parenthesise whenever the awaited call is the base
        of a further attribute access, subscript or call."""
        p = self.parents.get(id(call))
        if isinstance(p, ast.Attribute) and p.value is call:
            return True
        if isinstance(p, ast.Subscript) and p.value is call:
            return True
        if isinstance(p, ast.Call) and p.func is call:
            return True
        return False

    def _bridge(self, call: ast.Call, why: str) -> None:
        if self._needs_parens(call):
            self._insert(call, '(await ', why + ' [parenthesised]')
            end_line = (call.end_lineno or call.lineno) - 1
            end_col = call.end_col_offset or 0
            e = Edit(end_line, end_col, end_line, end_col, ')', why + ' [close paren]')
            if not any(x.key() == e.key() and x.text == ')' for x in self.edits):
                self.edits.append(e)
        else:
            self._insert(call, 'await ', why)

    # -- scoped walk --------------------------------------------------------

    def build_parents(self) -> dict[int, ast.AST]:
        parents: dict[int, ast.AST] = {}
        for node in ast.walk(self.tree):
            for child in ast.iter_child_nodes(node):
                parents[id(child)] = node
        return parents

    def run(self) -> None:
        self.parents = self.build_parents()
        self.rewrite_imports()
        self.rewrite_names()
        for cls in [n for n in self.tree.body if isinstance(n, ast.ClassDef)]:
            if any(self.api in testgen_excluded_apis(d) for d in cls.decorator_list):
                self.drop_class(cls)
                continue
            self.transform_class(cls)
        self.drop_edits_in_deleted_spans()

    def drop_edits_in_deleted_spans(self) -> None:
        """Discard edits that fall inside a method removed by testgen_exclude.

        rewrite_names() and rewrite_imports() run over the whole tree before any class is
        transformed, so by the time a method is dropped it may already have queued name
        substitutions inside it.  Applying both would write into lines that the deletion
        then blanks, which at best wastes the edit and at worst reorders a span boundary.
        """
        if not self.deleted_spans:
            return
        keep = {id(e) for e in self.deletion_edits}
        self.edits = [e for e in self.edits
                      if id(e) in keep
                      or not any(start <= e.line < end for start, end in self.deleted_spans)]

    def drop_method(self, fn: ast.FunctionDef, cls: ast.ClassDef) -> None:
        """Remove a whole method: its explanatory comment, its decorators, its body, and
        the blank line that separated it from the next one."""
        start = min([d.lineno for d in fn.decorator_list] + [fn.lineno]) - 1
        # Reach back over a comment block sitting directly above the decorators.  It
        # explains the method being removed, so leaving it behind attaches the rationale
        # for an excluded test to whichever test happens to follow it.
        while start > 0 and self.lines[start - 1].lstrip().startswith('#'):
            start -= 1
        end = fn.end_lineno or fn.lineno
        while end < len(self.lines) and self.lines[end].strip() == '':
            end += 1
        e = Edit(start, 0, end, 0, '', f'excluded from {self.api} by testgen_exclude')
        self.deleted_spans.append((start, end))
        self.deletion_edits.append(e)
        self.edits.append(e)
        self.report.tests_excluded.append(
            f'{self.rpath}:{fn.lineno}: {cls.name}.{fn.name}')

    def drop_class(self, cls: ast.ClassDef) -> None:
        """Remove a whole class the mark says this api has nothing to test.

        The per-test form is not a substitute here.  Excluding every method of a suite class
        one at a time leaves the class behind with an empty TEST_MANIFEST, plus its concrete
        subclass and that subclass's cb_env fixture: dead code in the generated file, and a
        `[]` manifest that validate_test_manifest passes vacuously.  datastructures_t is what
        forced this.  Its deprecated suite calls collection.list_append / map_add / set_add /
        queue_push, which exist only on the sync Collection, so for acouchbase the honest
        output is no class at all.

        Both the suite class and its concrete subclass carry their own mark.  Inferring the
        second from the first would mean resolving bases here, and a wrong inference deletes
        a class that was meant to ship; stating it twice costs two lines and cannot misfire.
        """
        start = min([d.lineno for d in cls.decorator_list] + [cls.lineno]) - 1
        while start > 0 and self.lines[start - 1].lstrip().startswith('#'):
            start -= 1
        end = cls.end_lineno or cls.lineno
        while end < len(self.lines) and self.lines[end].strip() == '':
            end += 1
        e = Edit(start, 0, end, 0, '', f'class excluded from {self.api} by testgen_exclude')
        self.deleted_spans.append((start, end))
        self.deletion_edits.append(e)
        self.edits.append(e)
        tests = sorted(fn.name for fn in cls.body
                       if isinstance(fn, ast.FunctionDef) and fn.name.startswith('test_'))
        self.report.classes_excluded.append(
            f'{self.rpath}:{cls.lineno}: {cls.name}'
            + (f' ({len(tests)} tests: {", ".join(tests)})' if tests else ' (no tests)'))

    def prune_test_manifest(self, cls: ast.ClassDef, names: set[str]) -> None:
        """Drop the excluded names from the class's TEST_MANIFEST.

        Not optional bookkeeping: validate_test_manifest() compares the manifest against
        the concrete class with symmetric_difference, so a manifest still naming a method
        that is no longer emitted fails the whole class at setup.  That check is also what
        makes this mechanism safe, since forgetting the pruning is loud rather than silent.
        """
        for stmt in cls.body:
            if not isinstance(stmt, ast.Assign):
                continue
            if not any(isinstance(t, ast.Name) and t.id == 'TEST_MANIFEST' for t in stmt.targets):
                continue
            if not isinstance(stmt.value, ast.List):
                continue
            for elt in stmt.value.elts:
                if not (isinstance(elt, ast.Constant) and elt.value in names):
                    continue
                line = self.lines[elt.lineno - 1].strip()
                # One entry per line is the manifest's house style, and a whole-line delete
                # is only correct while that holds.  If it ever stops holding, say so
                # instead of silently deleting a neighbouring entry.
                if line not in (f"'{elt.value}',", f'"{elt.value}",'):
                    self.report.notes.append(
                        f'{self.rpath}:{elt.lineno}: TEST_MANIFEST entry {elt.value!r} shares '
                        f'its line; pruned by hand or the manifest will not validate')
                    continue
                self.edits.append(Edit(elt.lineno - 1, 0, elt.lineno, 0, '',
                                       f'TEST_MANIFEST entry for excluded {elt.value}'))

    def transform_class(self, cls: ast.ClassDef) -> None:
        """Fixpoint over the whole class, not over one method at a time.

        kv_range_scan_t is what forced this.  Eighteen of its tests call
        `self._validate_result(res, ...)`, a plain helper method on the suite class whose
        body iterates the scan result.  The helper only becomes a coroutine once the
        iteration is known to be async, and the eighteen call sites only need `await` once
        the helper is a coroutine.  A per-method fixpoint sees neither: it finishes each
        test before it has ever looked at the helper.  So the unit of iteration is the
        class, and `self.<method>()` participates in exactly the same propagation as a
        nested closure.
        """
        self.seed_async_methods(cls)
        methods = self.live_methods(cls)
        self.propagate(cls, methods)
        self.apply_class_uniformity(cls, methods)

        for fn in methods:
            # A fixture is identified by its decorator, never by its name.  kv_range_scan_t
            # has three class-scoped fixtures called test_id, test_ids and
            # test_mutation_state; classifying by prefix counted all three as tests.  Worth
            # noting on its own: drop the decorator from any of them and pytest collects it.
            # See is_fixture_decorator() for why the decorator test is not a substring.
            is_fixture = any(is_fixture_decorator(d) for d in fn.decorator_list)
            if fn.name.startswith('test_') and not is_fixture:
                self.bucket_test(fn, cls)
            if self.async_fns.get(self.fn_key(fn)):
                self.finish_function(fn, is_fixture)

        self.class_async_methods[cls.name] = set(self.async_methods)

    def seed_async_methods(self, cls: ast.ClassDef) -> None:
        """Start from what the in-file base classes already resolved to.

        These suites are always a mixin suite class plus one or more concrete `*Tests`
        classes that inherit it, and the concrete class's cb_env fixture calls helpers
        defined on the mixin: kv_range_scan_t's RangeScanTests fixture calls
        self._purge_temp_docs(), which lives on RangeScanTestSuite.  Resetting per class
        loses that, and the emitted fixture drops a coroutine during teardown, where
        nothing fails and the collection is simply never purged.  Classes are visited in
        source order, so a base is always already resolved by the time a subclass is
        reached.
        """
        self.async_methods = set()
        for base in cls.bases:
            self.async_methods |= self.class_async_methods.get(ast.unparse(base), set())

    def live_methods(self, cls: ast.ClassDef) -> list[ast.FunctionDef]:
        """Drop the methods this api excludes, and return the ones left to analyse.

        Excluded methods are removed before analysis, not after, so an exclusion is a
        mechanism rather than a note.  query_t's test_query_in_thread is why: it reads as
        having no async content, so nothing in the analysis withholds it, and its body
        hands cb_env.assert_rows, a coroutine, to a worker thread that never awaits it.
        """
        excluded = [fn for fn in cls.body
                    if isinstance(fn, ast.FunctionDef)
                    and any(self.api in testgen_excluded_apis(d) for d in fn.decorator_list)]
        for fn in excluded:
            self.drop_method(fn, cls)
        if excluded:
            self.prune_test_manifest(cls, {fn.name for fn in excluded})

        dropped = {id(fn) for fn in excluded}
        return [fn for fn in cls.body
                if isinstance(fn, ast.FunctionDef) and id(fn) not in dropped]

    def propagate(self, cls: ast.ClassDef, methods: list[ast.FunctionDef]) -> None:
        """Re-analyse the class until a pass learns nothing new."""
        keys = {fn.name: self.fn_key(fn) for fn in methods}
        for _ in range(8):
            self.changed_this_pass = False
            for fn in methods:
                self.analyse_function(fn, cls)
            for name, key in keys.items():
                if self.async_fns.get(key) and name not in self.async_methods:
                    self.async_methods.add(name)
                    self.changed_this_pass = True
            if not self.changed_this_pass:
                break

    def apply_class_uniformity(self, cls: ast.ClassDef,
                               methods: list[ast.FunctionDef]) -> None:
        """Emit every test in an async class as a coroutine, not only those that await.

        Three kv_range_scan tests contain no await at all: they assert that scan() rejects
        a bad argument, which it does lazily before any I/O.  A strictly minimal rule leaves
        those three `def` in a class whose every other test is `async def`, where a lone
        `def` reads as an oversight rather than as a finding.  So the unit is the class:
        every test in a class that touches the SDK is a coroutine, and a class that touches
        nothing, TransactionOptionsUnitTests and RangeScanRequestBuilderUnitTests, stays
        synchronous throughout.  The per-test analysis is still what decides, it just
        decides for the class; nothing is guessed.

        Not applied under txcouchbase, and the asymmetry is real rather than an oversight.
        The acouchbase rule exists because `async def` is visible in the signature, so a
        lone `def` in an otherwise-async class is a discrepancy a reader can see.  A Twisted
        test carries no signature marker at all:
        bridging is per call site, so a test with nothing to bridge is simply a test with
        nothing to bridge, and there is nothing to make uniform.
        """
        if not self.propagates:
            return
        if not any(self.async_fns.get(self.fn_key(fn)) for fn in methods):
            return

        tests = [fn for fn in methods
                 if fn.name.startswith('test_')
                 and not any(is_fixture_decorator(d) for d in fn.decorator_list)]
        for fn in tests:
            if self.async_fns.get(self.fn_key(fn)):
                continue
            self.report.notes.append(
                f'{self.rpath}:{fn.lineno}: {cls.name}.{fn.name} has no await of its '
                f'own; emitted async because its class does')
            self.async_fns[self.fn_key(fn)] = True

    def bucket_test(self, fn: ast.FunctionDef, cls: ast.ClassDef) -> None:
        """File one test under the outcome its analysis reached."""
        where = f'{self.rpath}:{fn.lineno}: {cls.name}.{fn.name}'
        is_async = bool(self.async_fns.get(self.fn_key(fn)))
        is_blocked = self.fn_key(fn) in self.blocked_fns

        if is_async and is_blocked:
            self.report.tests_incomplete.append(where)
        elif is_async:
            self.report.tests_clean.append(where)
        elif is_blocked:
            self.report.sync_tests_blocked.append(where)
        else:
            # Genuinely nothing to await.  Worth a look every time: in query_t this is
            # test_query_in_thread, which drives a threading.Thread and has no async
            # analogue at all.  Emitting `async def` would hide that.
            self.report.sync_tests_genuine.append(where)

    def analyse_function(self, fn: ast.FunctionDef, cls: ast.ClassDef) -> None:
        base_scope: dict[str, str] = {}
        for arg in fn.args.args:
            ann = ast.unparse(arg.annotation).strip('\'"') if arg.annotation else ''
            mapped = self.model.param_annotation_types.get(ann)
            if mapped:
                base_scope[arg.arg] = mapped
            elif arg.arg == 'cb_env':
                base_scope[arg.arg] = self.env_class

        ctx = Ctx(scope=base_scope, fns={}, owner=self.fn_key(fn), getattr_bound={})
        self.visit_body(fn.body, ctx)

    @staticmethod
    def fn_key(fn: ast.AST) -> str:
        return f'{getattr(fn, "name", "?")}@{getattr(fn, "lineno", 0)}'

    # -- statements ---------------------------------------------------------

    def visit_body(self, body: list[ast.stmt], ctx: Ctx) -> None:
        for stmt in body:
            self.visit_stmt(stmt, ctx)

    def visit_stmt(self, stmt: ast.stmt, ctx: Ctx) -> None:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            self.visit_nested_def(stmt, ctx)
            return

        if isinstance(stmt, ast.For):
            self.visit_for(stmt, ctx)

        self.visit_children(stmt, ctx)

        if isinstance(stmt, (ast.Assign, ast.AnnAssign)):
            self.bind_assignment(stmt, ctx)

    def visit_for(self, stmt: ast.For, ctx: Ctx) -> None:
        """Make a loop `async for` when what it iterates is asynchronous under this api."""
        if not isinstance(stmt.iter, ast.Call):
            # `for x in result:` over a bound result object.  This is the preferred source
            # shape, and it is also the one a call-based rule misses entirely.
            t = self.receiver_type(stmt.iter, ctx.scope)
            if t in self.model.iterable_types:
                self._insert(stmt, 'async ', f'{t} is an async iterable under {self.api}')
                self.mark_async(ctx.owner)
            return

        # `for x in <call>()` where the call yields an async iterable.
        r = self.resolve_call(stmt.iter, ctx)
        if r is not None and r.kind is Kind.ASYNC_ITER:
            self._insert(stmt, 'async ', f'async iteration: {r.why}')
            self.mark_async(ctx.owner)
        else:
            self.check_wrapped_async_iter(stmt.iter, ctx)

    def visit_children(self, stmt: ast.stmt, ctx: Ctx) -> None:
        for child in ast.iter_child_nodes(stmt):
            if isinstance(child, ast.stmt):
                self.visit_stmt(child, ctx)
            elif isinstance(child, ast.expr):
                self.visit_expr(child, ctx)
            elif isinstance(child, ast.withitem):
                self.visit_expr(child.context_expr, ctx)
            elif isinstance(child, ast.ExceptHandler):
                if child.type is not None:
                    self.visit_expr(child.type, ctx)
                self.visit_body(child.body, ctx)

    def visit_nested_def(self, fn: ast.AST, ctx: Ctx) -> None:
        key = self.fn_key(fn)
        ctx.fns[fn.name] = key
        inner = Ctx(scope=dict(ctx.scope), fns=ctx.fns, owner=key,
                    getattr_bound=dict(ctx.getattr_bound))
        for arg in fn.args.args:
            ann = ast.unparse(arg.annotation) if arg.annotation else ''
            mapped = self.model.param_annotation_types.get(ann.strip("'\""))
            if mapped:
                inner.scope[arg.arg] = mapped
        self.visit_body(fn.body, inner)
        if self.async_fns.get(key) and isinstance(fn, ast.FunctionDef):
            self.finish_function(fn, is_fixture=False)

    def bind_assignment(self, stmt: ast.stmt, ctx: Ctx) -> None:
        value = stmt.value
        if value is None:
            return
        self.bind_getattr(stmt, value, ctx)
        t = self.manager_binding(value, ctx) or self.receiver_type(value, ctx.scope)
        # An annotated local names its own type, and that is the only handle available when
        # the value comes back from something generic.  usermgmt_t is the case: its
        # change-password test binds `cluster: Cluster = TestEnvironment.try_n_times(10, 1,
        # Cluster.connect, ...)`, because a freshly created user takes a moment to become
        # usable and the connect has to be retried.  try_n_times returns Any, so without this
        # `cluster` is untyped, and an untyped receiver resolves SYNC by omission: the
        # generated acouchbase test called `cluster.users().change_password(...)` unawaited and
        # passed, with the coroutine-never-awaited warning as the only sign.  Same table the
        # parameter annotations use, so a local says the same thing a parameter would.
        if t is None and isinstance(stmt, ast.AnnAssign) and stmt.annotation is not None:
            ann = ast.unparse(stmt.annotation).strip('\'"')
            t = self.model.param_annotation_types.get(ann)
        if t is None:
            return
        targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
        for tgt in targets:
            if isinstance(tgt, ast.Name):
                ctx.scope[tgt.id] = t

    def bind_getattr(self, stmt: ast.stmt, value: ast.expr, ctx: Ctx) -> None:
        """Remember a local bound through `getattr`, so a later call on it can be reported.

        `operation = getattr(cb_env.collection.binary(), op_name.value)` and then
        `operation(key, b'XXX')`.  The receiver was typeable and the local is not, and an
        untyped receiver resolves sync by omission, so the call is emitted unbridged: under
        acouchbase it throws a coroutine away, under txcouchbase it enters the reactor from
        the wrong thread.  Neither shows up where the cause is.

        The call site alone cannot carry this.  It is a bare name, which is the path every
        plain function call in every suite takes.  The binding is what narrows it, and it is
        one statement away.
        """
        if not isinstance(value, ast.Call) or not isinstance(value.func, ast.Name):
            return
        if value.func.id != 'getattr':
            return
        targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
        for tgt in targets:
            if isinstance(tgt, ast.Name):
                ctx.getattr_bound[tgt.id] = value.lineno

    def manager_binding(self, value: ast.expr, ctx: Ctx) -> str | None:
        """`qixm = cb_env.get_manager(TestEnvironmentQueryIndexManager)` names its own type
        in the argument.  Binding it turns 172 anonymous "unresolved receiver" lines in
        querymgmt_t into one statement about one missing class."""
        if not isinstance(value, ast.Call) or not isinstance(value.func, ast.Attribute):
            return None
        if value.func.attr != 'get_manager':
            return None
        if self.receiver_type(value.func.value, ctx.scope) != self.env_class:
            return None
        if value.args and isinstance(value.args[0], ast.Name):
            # Map the name first.  The *source* says TestEnvironmentQueryIndexManager and
            # the generated file says AsyncTestEnvironmentQueryIndexManager; looking up the
            # source name finds the sync wrapper, whose methods are not coroutines, so the
            # resolver confidently answers SYNC for all 157 of them: a wrong answer
            # rather than a reported miss, which is the failure this tool is shaped to
            # avoid.
            return f'manager:{self.name_map.get(value.args[0].id, value.args[0].id)}'
        return None

    # -- expressions --------------------------------------------------------

    def visit_expr(self, root: ast.expr, ctx: Ctx) -> None:
        """Walk an expression tree without descending into nested function bodies."""
        stack: list[ast.AST] = [root]
        while stack:
            node = stack.pop()
            if isinstance(node, ast.Call):
                self.handle_call(node, ctx)
            elif isinstance(node, (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)):
                # A comprehension over an async iterable needs `async for` inside the
                # brackets, which this transformer does not emit.  Unlike a statement-level
                # `for`, even the bare-call shape is unhandled here, so both are reported.
                for gen in node.generators:
                    self.check_wrapped_async_iter(gen.iter, ctx, bare_is_ok=False)
            for child in ast.iter_child_nodes(node):
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
                    continue
                stack.append(child)

    def resolve_call(self, call: ast.Call, ctx: Ctx):
        f = call.func
        if isinstance(f, ast.Attribute):
            cls_name = self.receiver_type(f.value, ctx.scope)
            if cls_name is None:
                return None
            return self.res.method_kind(cls_name, f.attr)
        return None

    def handle_call(self, call: ast.Call, ctx: Ctx) -> None:
        f = call.func
        if isinstance(f, ast.Name):
            self.handle_bare_call(call, f, ctx)
        elif isinstance(f, ast.Attribute):
            self.handle_attribute_call(call, f, ctx)

    def handle_bare_call(self, call: ast.Call, f: ast.Name, ctx: Ctx) -> None:
        """A call to a sibling closure already known to be a coroutine, or to a module-level
        helper that is `async def` in the api-specific environment module.

        Anything else arriving here goes unreported unless the source bound the name through
        `getattr`, which is the one case narrow enough to say something about.
        """
        bound_at = ctx.getattr_bound.get(f.id)
        if bound_at is not None:
            self.note_unresolved(
                f'{self.rpath}:{call.lineno}: `{f.id}()` is a getattr binding from line '
                f'{bound_at}, so nothing can type it; name the operation instead', ctx)
            return

        key = ctx.fns.get(f.id)
        if key and self.propagates and self.async_fns.get(key):
            self._bridge(call, f'{f.id}() is an async closure')
            self.mark_async(ctx.owner)
            return
        if f.id in ctx.fns or f.id in ctx.scope:
            return

        r = self.res.function_kind(self.name_map.get(f.id, f.id))
        if r.kind is Kind.AWAIT:
            self._bridge(call, r.why)
            self.mark_async(ctx.owner)

    def handle_attribute_call(self, call: ast.Call, f: ast.Attribute, ctx: Ctx) -> None:
        if self.handle_named_receiver(call, f, ctx):
            return

        cls_name = self.receiver_type(f.value, ctx.scope)
        if cls_name is None:
            self.report_unresolved_receiver(call, f, ctx)
            return

        r = self.res.method_kind(cls_name, f.attr)
        if r.kind is Kind.AWAIT:
            self._bridge(call, r.why)
            self.mark_async(ctx.owner)
        elif r.kind is Kind.UNKNOWN:
            self.note_unresolved(f'{self.rpath}:{call.lineno}: {dotted(f)}() -> {r.why}', ctx)

    def handle_named_receiver(self, call: ast.Call, f: ast.Attribute, ctx: Ctx) -> bool:
        """The two receivers decided by name alone.  True if this call is settled here.

        `self._validate_result(...)` is a sibling method of the same suite class, whose
        asyncness the class-level fixpoint in transform_class() has already decided.  A
        static constructor such as `Cluster.connect(...)` is a coroutine on the async class,
        and names its own type through a Name that local scope knows nothing about.
        """
        if not isinstance(f.value, ast.Name):
            return False

        if f.value.id == 'self':
            if self.propagates and f.attr in self.async_methods:
                self._bridge(call, f'self.{f.attr}() is a coroutine method of this suite')
                self.mark_async(ctx.owner)
            return True

        if (f.value.id, f.attr) in self.model.static_ctor_types:
            self._bridge(call, f'{f.value.id}.{f.attr}() is a coroutine on the async class')
            self.mark_async(ctx.owner)
            return True
        return False

    def report_unresolved_receiver(self, call: ast.Call, f: ast.Attribute, ctx: Ctx) -> None:
        """Report a receiver whose type nothing could establish.

        A bare CapWords receiver is reported too, not just tracked locals.  A class used as
        a receiver is exactly the shape that hid AsyncQueryEnvironment: not a local, not in
        TRACKED_ROOTS, so neither resolved nor reported.  Over-reporting here costs a line
        in the report; under-reporting costs a silently unawaited coroutine, which is what
        the "report, never guess" rule exists to prevent.
        """
        chain = dotted(f)
        root = chain.split('.')[0] if chain else ''
        looks_like_class = bool(root) and root[:1].isupper()
        if chain and (root in ctx.scope or root in TRACKED_ROOTS or looks_like_class):
            self.note_unresolved(
                f'{self.rpath}:{call.lineno}: unresolved receiver `{chain}()`', ctx)

    def check_wrapped_async_iter(self, it: ast.expr, ctx: Ctx, bare_is_ok: bool = True) -> None:
        """Report an async iterable that is being consumed by something other than a plain
        `for`, which is the one shape this transformer can rewrite.

        `for idx, r in enumerate(res.rows())` in kv_range_scan_t is the case that forced
        this.  rows() resolves to ASYNC_ITER perfectly well, but ASYNC_ITER matches neither
        the AWAIT nor the UNKNOWN branch of handle_call(), so the site was resolved,
        unrewritable, and completely unreported: the emitted loop stayed synchronous and
        died on the real server with `'AsyncRangeScanRequest' object is not iterable`.

        There is deliberately no attempt to rewrite it.  `enumerate`, `zip`, `list` and
        `sorted` all consume an iterator eagerly and none of them accept an async one, so
        the fix belongs in the source suite (hoist a counter, or `async for` into a list
        first), not in a generator heuristic that would have to reproduce each builtin's
        semantics.  Report it and let a human write the two lines.
        """
        for sub in ast.walk(it):
            if not isinstance(sub, ast.Call):
                continue
            if bare_is_ok and sub is it:
                continue
            r = self.resolve_call(sub, ctx)
            if r is not None and r.kind is Kind.ASYNC_ITER:
                self.note_unresolved(
                    f'{self.rpath}:{it.lineno}: `{ast.unparse(sub)}` is an async iterable '
                    f'consumed by `{ast.unparse(it)}`; rewrite the source to iterate it '
                    f'directly', ctx)
                return

    def note_unresolved(self, message: str, ctx: Ctx) -> None:
        """Record one unresolved site.  The class-level fixpoint re-walks every method up
        to eight times, so appending blindly multiplies each site by the number of passes
        it survived and the report stops being countable."""
        if message not in self._seen_unresolved:
            self._seen_unresolved.add(message)
            self.report.unresolved.append(message)
        self.blocked_fns.add(ctx.owner)

    def mark_async(self, key: str) -> None:
        if not self.async_fns.get(key):
            self.async_fns[key] = True
            self.changed_this_pass = True

    # -- def / decorator edits ----------------------------------------------

    def finish_function(self, fn: ast.AST, is_fixture: bool) -> None:
        """Whatever the api needs at the `def` line of a function that bridges a call.

        Under acouchbase that is the whole `async def` / marker / fixture / annotation
        story below.  Under txcouchbase it is nothing at all, which is why this is a hook
        and not an `if`.
        """
        self.make_async_def(fn, is_fixture)

    def make_async_def(self, fn: ast.AST, is_fixture: bool) -> None:
        line = fn.lineno - 1
        idx = self.lines[line].index('def ')
        e = Edit(line, idx, line, idx, 'async ', 'coroutine body')
        if not any(x.key() == e.key() and x.text == 'async ' for x in self.edits):
            self.edits.append(e)
        # Fixture first.  kv_range_scan_t's class-scoped fixtures are called test_id,
        # test_ids and test_mutation_state, so a name-first branch sends them down the
        # `@pytest.mark.asyncio` path and never swaps `@pytest.fixture` for
        # `@pytest_asyncio.fixture`.  pytest-asyncio then warns "requested async
        # @pytest.fixture ... in strict mode", hands the test an unawaited coroutine, and
        # says it will become an error in a future release.
        if is_fixture:
            self.swap_fixture_decorator(fn)
            self.rewrite_generator_annotation(fn)
        elif getattr(fn, 'name', '').startswith('test_'):
            self.add_asyncio_marker(fn)

    def add_asyncio_marker(self, fn: ast.AST) -> None:
        """pytest-asyncio runs in strict mode here: pyproject.toml sets
        asyncio_default_*_loop_scope but never asyncio_mode, and the plugin defaults to
        strict.  So every async test needs the marker.  It is also inert under auto mode,
        so it is emitted unconditionally rather than read from a config value the generator
        has no reliable way to see."""
        decs = getattr(fn, 'decorator_list', [])
        anchor = decs[0] if decs else fn
        line = anchor.lineno - 1
        col = self.lines[line].index('@') if decs else fn.col_offset
        indent = ' ' * col
        e = Edit(line, col, line, col, f'@pytest.mark.asyncio\n{indent}', 'strict-mode asyncio marker')
        if not any(x.key() == e.key() and 'asyncio' in x.text for x in self.edits):
            self.edits.append(e)

    def rewrite_generator_annotation(self, fn: ast.AST) -> None:
        """`-> Generator[X, None, None]` becomes `-> AsyncGenerator[X, None]`.

        An async generator's return annotation is not the sync one with `async` in front
        of the def: AsyncGenerator takes two parameters, not three.  mypy says so directly,
        "the return type of an async generator function should be AsyncGenerator".
        Nothing at runtime notices, which is exactly why a type checker has to.

        Two edits in place rather than one re-rendering of the whole subscript, so an
        annotation the source wrapped stays wrapped.  Re-rendering puts it back on one
        line, where the api name substitution can carry it past the column limit and the
        only fix offered to the author is to shorten a source line that was already inside
        the budget.  The yield type is left to rewrite_names() for the same reason.
        """
        ann = getattr(fn, 'returns', None)
        if not isinstance(ann, ast.Subscript):
            return
        if dotted(ann.value) not in ('Generator', 'collections.abc.Generator'):
            return
        elts = ann.slice.elts if isinstance(ann.slice, ast.Tuple) else [ann.slice]
        self._replace(ann.value, 'AsyncGenerator', 'async generator annotation')
        if len(elts) > 2:
            self._cut(elts[1], elts[-1], 'AsyncGenerator takes two parameters')
        self.needs_async_generator = True

    def swap_fixture_decorator(self, fn: ast.AST) -> None:
        for d in getattr(fn, 'decorator_list', []):
            txt = ast.unparse(d)
            if txt.startswith('pytest.fixture'):
                self._replace(d, 'pytest_asyncio.' + txt[len('pytest.'):], 'async fixture')
                self.needs_pytest_asyncio = True

    # -- module-level rewrites ----------------------------------------------

    def rewrite_names(self) -> None:
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Name) and node.id in self.name_map:
                self._replace(node, self.name_map[node.id], 'api name substitution')
            elif isinstance(node, ast.arg) and node.arg in self.name_map:
                # Fixture parameter names (cb_base_env -> acb_base_env) are `arg` nodes,
                # not `Name` nodes, and are how pytest resolves the fixture.  Missing
                # these produces a file that imports cleanly and errors at collection.
                e = Edit(node.lineno - 1, node.col_offset, node.lineno - 1,
                         node.col_offset + len(node.arg), self.name_map[node.arg],
                         'fixture parameter substitution')
                if not any(x.key() == e.key() for x in self.edits):
                    self.edits.append(e)

    def rewrite_imports(self) -> None:
        """Re-resolve each imported *name* against the api-specific module.

        The obvious implementation, swap `couchbase.` for `acouchbase.` in the module
        path, is wrong.  A module such
        as `couchbase.management.buckets` does have an acouchbase counterpart, but the
        counterpart re-exports only the manager classes; the shared enums (BucketType,
        StorageBackend, ConflictResolutionType) stay behind in the sync module.  The same
        holds for `couchbase.kv_range_scan` (RangeScan, ScanTerm, PrefixScan, SamplingScan)
        and `couchbase.transactions` (TransactionGetMultiMode).  A per-module rewrite emits
        a file that parses, imports an existing module, and then fails at collection with
        ImportError on a name that module never had.

        So the unit of rewriting is the name, and one source import can become two.
        """
        # ast.walk, not self.tree.body.  query_t's test_query_timeout imports
        # couchbase.auth / couchbase.cluster / couchbase.options inside the function body,
        # to build a second cluster with its own timeout options.  A top-level-only sweep
        # leaves `from couchbase.cluster import Cluster` in place, and the transform then
        # emits `await Cluster.connect(...)` against the synchronous class.  Only mypy sees
        # it, "Incompatible types in await (actual type Cluster)": that test skips on
        # GoCAVES, so the mock run cannot reach it.
        for node in ast.walk(self.tree):
            if isinstance(node, ast.ImportFrom) and node.module is not None:
                self.rewrite_import(node, node.module)

    def rewrite_import(self, node: ast.ImportFrom, mod: str) -> None:
        """Re-point one `from <mod> import ...`, splitting it if the names disagree."""
        target = self.import_target(mod)
        if target is None:
            self.report_unmapped(node, mod)
            return

        target_mod = self.load_module(node, mod, target)
        if target_mod is None:
            return

        moved, stayed = self.split_names(node, target_mod)
        if not moved:
            return

        stmts = [self.render_import(target, moved)]
        if stayed:
            stmts.append(self.render_import(mod, stayed))
            self.report.notes.append(
                f'{self.rpath}:{node.lineno}: split `from {mod} import ...`, '
                f'{len(stayed)} name(s) have no {self.api} counterpart: '
                f'{", ".join(stayed)}')

        line = self.lines[node.lineno - 1]
        indent = line[:len(line) - len(line.lstrip())]
        self.edits.append(Edit(node.lineno - 1, node.col_offset,
                               (node.end_lineno or node.lineno) - 1,
                               node.end_col_offset or 0,
                               f'\n{indent}'.join(stmts), 'import re-resolution'))

    def import_target(self, mod: str) -> str | None:
        """The api-specific module this import's names should be looked for in."""
        target = ENV_MODULE_MAP[self.api].get(mod)
        if target is not None:
            return target
        if mod in API_MODULES:
            return mod.replace('couchbase.', f'{self.api}.', 1)
        if mod == 'couchbase':
            return self.api
        return None

    def report_unmapped(self, node: ast.ImportFrom, mod: str) -> None:
        """A shared-framework module with no entry for this api is a blocker.

        ENV_MODULE_MAP is meant to be an *exhaustive* declaration for the
        tests_v2.environments package: an api-specific module names its counterpart, a
        genuinely shared one names itself.  Silence means the question was never asked.

        Falling through does not leave the file unchanged, which is what makes it
        dangerous.  The name maps are consulted separately, so the suite keeps the *sync*
        environment class while its managers are rewritten to the api-specific ones, and
        the two then disagree about the registry key.  Retargeting query_t to txcouchbase
        produced exactly that: a generated suite registering
        TwistedTestEnvironmentQueryIndexManager and then calling the sync
        QueryEnvironment.setup_query_indexes(), which asks the registry for
        TestEnvironmentQueryIndexManager and gets ENV_DEGRADED.

        It reported 0 blockers, passed the mock run, and failed on the first real cluster,
        because setup_query_indexes() opens with a QueryIndexManagement feature check that
        skips the whole class on GoCAVES.  A mock run cannot reach the defect, so the
        generator has to.
        """
        if mod != ENV_PACKAGE and not mod.startswith(f'{ENV_PACKAGE}.'):
            return
        self.report.blockers.append(
            f'{self.rpath}:{node.lineno}: `{mod}` has no ENV_MODULE_MAP entry for '
            f'{self.api} (needed for `from {mod} import '
            f'{", ".join(a.name for a in node.names)}`).  Add the api-specific '
            f'counterpart module, or an identity entry if it is genuinely shared.')

    def load_module(self, node: ast.ImportFrom, mod: str, target: str) -> ModuleType | None:
        """Import `target`, or report why the file this import lands in could not work."""
        import importlib

        if not self.module_exists(target):
            self.report.blockers.append(
                f'{self.rpath}:{node.lineno}: `{target}` does not exist '
                f'(needed for `from {mod} import ...`)')
            return None
        try:
            return importlib.import_module(target)
        except Exception as ex:                       # pragma: no cover - reported
            self.report.blockers.append(
                f'{self.rpath}:{node.lineno}: `{target}` fails to import: {ex}')
            return None

    def split_names(self, node: ast.ImportFrom,
                    target_mod: ModuleType) -> tuple[list[str], list[str]]:
        """This import's names, split into those the api module has and those it does not."""
        moved: list[str] = []
        stayed: list[str] = []
        for alias in node.names:
            name = self.name_map.get(alias.name, alias.name)
            rendered = f'{name} as {alias.asname}' if alias.asname else name
            bucket = moved if hasattr(target_mod, name) else stayed
            # SyncTestEnvironment and TestEnvironment both map onto AsyncTestEnvironment,
            # so a suite importing both emits the name twice.  Legal Python, flagged by
            # flake8 as F811, and confusing to read.
            if rendered not in bucket:
                bucket.append(rendered)
        return moved, stayed

    @staticmethod
    def render_import(module: str, names: list[str]) -> str:
        """Emit `from m import a, b` on one line and stop there.

        The transformer does not format.  pre-commit runs autopep8, then flake8, then
        isort with the repo's own arguments, so wrapping and import ordering are already
        somebody else's job and reimplementing them here would only produce a second
        opinion that has to be kept in step with the first.  What autopep8 deliberately
        will not fix is E501, which is why the report carries long_lines.
        """
        return f'from {module} import ' + ', '.join(names)

    @staticmethod
    def module_exists(dotted_mod: str) -> bool:
        import importlib.util
        try:
            return importlib.util.find_spec(dotted_mod) is not None
        except (ImportError, ValueError):
            return False


class AsyncTransformer(SuiteTransformer):
    """couchbase/tests_v2 -> acouchbase/tests_v2.  The base class is this transform."""

    api = 'acouchbase'
    name_map = ACB_NAME_MAP
    propagates = True


class TwistedTransformer(SuiteTransformer):
    """couchbase/tests_v2 -> txcouchbase/tests_v2.

    Not the async transform with different names in it.  Three things are structurally
    different, and each one is a place the async version would have produced a file that
    parses, imports and then misbehaves:

    **The bridge is an expression rewrite, not an insertion.**  `await x.get(k)` adds a
    keyword in front of an expression that is otherwise untouched.  `run_in_reactor_thread(
    x.get, k)` takes the call apart: the callable becomes the first argument and the call's
    own parentheses become the wrapper's.  Two edits do it without ever re-emitting the
    argument text, which is what keeps comments and line breaks inside a long argument list
    exactly where the author put them.

    **Nothing propagates.**  See SuiteTransformer.propagates.

    **The retry helpers need a dispatch decision.**  try_n_times() takes a callable, and
    under Twisted it defaults to running that callable on the reactor thread.  That is right
    for a bare SDK method and wrong for everything else, so the generator has to decide,
    per call site, and say so when it cannot.  See check_retry_dispatch().
    """

    api = 'txcouchbase'
    name_map = TXCB_NAME_MAP
    propagates = False

    BRIDGE = 'run_in_reactor_thread'
    RETRY_HELPERS = ('try_n_times', 'try_n_times_till_exception')

    def __init__(self, src: str, path: str, resolver: Resolver, report: Report) -> None:
        super().__init__(src, path, resolver, report)
        # True once at least one call site has been wrapped, so run.py knows whether the
        # generated file needs the import.  bucketmgmt_t is the case that makes this worth
        # tracking rather than assuming: every one of its SDK calls goes through a manager
        # wrapper that blocks internally, so it bridges nothing and importing the helper
        # would be an unused import.
        self.needs_rit = False

    # -- the bridge ---------------------------------------------------------

    def _open_paren(self, call: ast.Call) -> tuple[int, int] | None:
        """0-based (line, col) of the `(` that opens this call's argument list.

        Scanned forward from the end of the callable expression rather than assumed to be
        the next character, because `cb.get (key)` and a callable that ends at a line break
        are both legal.
        """
        fn = call.func
        line = (fn.end_lineno or fn.lineno) - 1
        col = fn.end_col_offset or 0
        while line < len(self.lines):
            idx = self.lines[line].find('(', col)
            if idx != -1:
                return line, idx
            line += 1
            col = 0
        return None

    def _bridge(self, call: ast.Call, why: str) -> None:
        """`recv.method(a, b)` -> `run_in_reactor_thread(recv.method, a, b)`.

        Two edits, and the arguments are never touched:

          1. insert `run_in_reactor_thread(` at the start of the call
          2. replace the call's own `(` with `, ` when it has arguments, or delete it when
             it does not

        The call's closing `)` then closes the wrapper, which is why there is no third edit
        and no end-of-call bookkeeping.  It is also why this needs no equivalent of the
        async side's _needs_parens(): `run_in_reactor_thread(cb.get, k).content_as[dict]`
        binds correctly on its own, whereas `await cb.get(k).content_as[dict]` awaits the
        subscript.  The wrapping form is simply safer here.
        """
        pos = self._open_paren(call)
        if pos is None:
            self.report.blockers.append(
                f'{self.rpath}:{call.lineno}: cannot find the opening paren of '
                f'`{ast.unparse(call.func)}(...)`; not wrapped')
            return
        line, col = pos
        self._insert(call, f'{self.BRIDGE}(', why)
        if not (call.args or call.keywords):
            sep = ''
        elif self.lines[line][col + 1:].strip() == '':
            # The `(` ended its line, so the arguments start on the next one.  A `, ` here
            # would leave a trailing space that flake8 reports as W291; autopep8 strips it,
            # but the transformer should not be emitting it in the first place.
            sep = ','
        else:
            sep = ', '
        e = Edit(line, col, line, col + 1, sep, f'{why} [callable/argument split]')
        if not any(x.key() == e.key() for x in self.edits):
            self.edits.append(e)
            self.changed_this_pass = True
        self.needs_rit = True

    def finish_function(self, fn: ast.AST, is_fixture: bool) -> None:
        """Nothing happens at the `def` line.

        run_in_reactor_thread() blocks, so a Twisted test that drives the SDK is an
        ordinary synchronous function: no `async def`, no @pytest.mark.asyncio, no
        pytest_asyncio.fixture, and `Generator[...]` stays `Generator[...]` rather than
        becoming AsyncGenerator.  Every one of those would be wrong here, and each would
        have been inherited silently from the base class without this override.
        """

    # -- the retry-helper dispatch decision ---------------------------------

    def handle_call(self, call: ast.Call, ctx: Ctx) -> None:
        self.check_retry_dispatch(call, ctx)
        super().handle_call(call, ctx)

    def check_retry_dispatch(self, call: ast.Call, ctx: Ctx) -> None:
        """Add `is_deferred=False` where try_n_times must not use the reactor thread.

        TwistedTestEnvironment.try_n_times(n, s, func, *args) defaults to calling `func` via
        run_in_reactor_thread, which is correct only when `func` is a bare SDK method whose
        return value is a Deferred.  For anything else, a local closure, an environment
        method, a manager wrapper, the callable is ordinary blocking Python that calls
        run_in_reactor_thread itself, and dispatching it to the reactor thread makes it
        block on the reactor it is already running on.  That is a deadlock, not an error:
        the suite hangs until the CI job times out, with no traceback and no failing test.

        Nothing about this has an acouchbase counterpart.  There, `await try_n_times(...)`
        with an `async def` callable is the only shape there is.

        The existing framework code already follows this rule by hand, which is the
        confirmation that it is the right one: twisted_environment.py passes
        `is_deferred=False` at all six of its own call sites, every one of them a manager
        wrapper (coll_mgr.setup_named_collection, coll_mgr.teardown_named_collection,
        bm.get_bucket, bm.drop_bucket) or an environment method (self.load_data,
        self.purge_data).
        """
        f = call.func
        if not isinstance(f, ast.Attribute) or f.attr not in self.RETRY_HELPERS:
            return
        if self.receiver_type(f.value, ctx.scope) != self.env_class:
            return
        if any(k.arg == 'is_deferred' for k in call.keywords):
            return
        if len(call.args) < 3:
            self.report.blockers.append(
                f'{self.rpath}:{call.lineno}: {f.attr}() called with fewer than three '
                f'positional arguments; cannot tell which one is the callable')
            return

        target = call.args[2]
        deferred = self.callable_is_deferred(target, ctx)
        if deferred is True:
            return                    # the default dispatch is already right
        if deferred is None:
            self.note_unresolved(
                f'{self.rpath}:{call.lineno}: cannot resolve `{ast.unparse(target)}` passed '
                f'to {f.attr}(); left dispatching to the reactor thread, which deadlocks '
                f'if it is blocking Python', ctx)
            return
        self.inject_is_deferred_false(call, f'{ast.unparse(target)} is blocking')

    def callable_is_deferred(self, node: ast.expr, ctx: Ctx) -> bool | None:
        """True if `node` names something that returns a Deferred, False if it names
        blocking Python, None if the generator cannot tell."""
        if isinstance(node, ast.Attribute):
            cls_name = self.receiver_type(node.value, ctx.scope)
            if cls_name is None:
                return None
            r = self.res.method_kind(cls_name, node.attr)
            if r.kind is Kind.AWAIT:
                return True
            if r.kind is Kind.SYNC:
                return False
            return None
        if isinstance(node, ast.Name):
            # A closure defined in this method: ordinary Python by construction.  Both real
            # cases are this shape, cas_matches in collection_t and get_bucket_expiry_equal
            # in bucketmgmt_t, and both call the SDK inside, so both would deadlock.
            if node.id in ctx.fns:
                return False
            name = self.name_map.get(node.id, node.id)
            r = self.res.function_kind(name)
            if r.kind is Kind.AWAIT:
                return True
            # Membership is asked separately, and on purpose.  function_kind() answers SYNC
            # for any name it cannot find, so trusting it here would give a confident wrong
            # answer for a bare name that is neither a local closure nor a known
            # module-level helper.  Same catch-all trap as RESULT_TYPES.
            return False if self.res.knows_function(name) else None
        return None

    def inject_is_deferred_false(self, call: ast.Call, why: str) -> None:
        end_line = (call.end_lineno or call.lineno) - 1
        end_col = (call.end_col_offset or 1) - 1        # the closing `)`
        sep = '' if self._last_nonspace(end_line, end_col) == ',' else ','
        e = Edit(end_line, end_col, end_line, end_col,
                 f'{sep} is_deferred=False', f'reactor dispatch: {why}')
        if not any(x.key() == e.key() and 'is_deferred' in x.text for x in self.edits):
            self.edits.append(e)
            self.changed_this_pass = True

    def _last_nonspace(self, line: int, col: int) -> str:
        """The last non-whitespace character before (line, col), scanning back over line
        breaks.  A trailing comma in the source would otherwise produce `f(a,, kw=...)`."""
        while line >= 0:
            seg = self.lines[line][:col].rstrip()
            if seg:
                return seg[-1]
            line -= 1
            col = len(self.lines[line]) if line >= 0 else 0
        return ''
