# testgen: generating the acouchbase and txcouchbase test suites

Each suite under `couchbase/tests/` is the single hand-written source for its
acouchbase and txcouchbase counterparts. `tools/testgen` rewrites that source into
both, and the two generated trees are committed:

```
couchbase/tests/<suite>_t.py          the source, hand-written
acouchbase/tests/_generated/          generated, tracked, do not edit
txcouchbase/tests/_generated/         generated, tracked, do not edit
```

The transform is not a reformat. Under acouchbase the SDK surface is `async def`,
so the bridge is a keyword and the rewrite spreads outward: a helper that gains an
`await` becomes a coroutine and every caller has to await it too. Under txcouchbase
no method is `async def`; the surface returns a `Deferred` and the bridge is a
blocking call onto the reactor thread, which propagates to nothing. One source
spelling covers both because the bridge absorbs the difference.

## Usage

Run from the repository root, in an environment where `couchbase` is importable.
The resolver imports `tests.environments` and inspects the real SDK classes, so
the C extension has to be built. The tool's own dependencies are in
`tools/testgen/tools_requirements.txt`; none of them ship in the wheel.

```
# validate the suite table and show what it generates
python -m tools.testgen config
# generate in memory and print what resolved, writing nothing
python -m tools.testgen report
# regenerate both trees
python -m tools.testgen generate --write
# fail if the trees on disk are not what the current sources generate
python -m tools.testgen check
```

`--api acouchbase` or `--api txcouchbase` limits any of them to one tree.
`--config` points at a different suite table; the default is
`tools/testgen/config/testgen.yaml`. `report --verbose` additionally names every
test in the buckets that need no decision.

Every command prints a counter naming the suite it is on, erased when it finishes,
and suppressed when stderr is not a terminal.

`generate` writes nothing at all if the run produced blockers. `check` exits
non-zero on any difference and names it. Both exit 0 and say so when clean.

## A source change is not finished until the generator has run

A test lives in three API trees and only one of them is hand-written, so adding,
changing or removing a sync test leaves two stale copies on disk. Every gate in the
repository stays green while they sit there: the stale files parse, type-check, pass
the hooks, and run. So:

```
python -m tools.testgen generate --write
git add acouchbase/tests/_generated txcouchbase/tests/_generated
```

goes in the same commit as the source edit, and `check` is what proves it happened.
Both output paths are marked `linguist-generated` in `.gitattributes`, so a
regeneration collapses in review instead of burying the source change under it.

`check` names three kinds of difference. **stale** and **missing** are fixed by
regenerating. **orphaned**, a generated file no source produces any more, has to be
deleted by hand, and is why removing a suite needs the same attention as adding one.

`check` is deliberately not a pre-commit hook: it needs the built extension, which
would mean `language: system`. It belongs in CI, where the extension is already
built, and that is the only place the 48 generated files are checked against their
sources automatically.

## Reading the report

`report` regenerates everything in memory and writes nothing, which makes it the
cheap thing to run while writing a suite. Its per-test buckets are the point, not
its totals:

| bucket | meaning |
|---|---|
| CLEAN | bridges at least one call, nothing unresolved |
| **INCOMPLETE** | **bridges calls, and still holds an unresolved one** |
| BLOCKED | bridges nothing, every candidate call was unresolved |
| GENUINELY SYNC | nothing to bridge at all |
| EXCLUDED | a `testgen_exclude` mark, an authoring decision |

**INCOMPLETE is the bucket to act on.** BLOCKED is loud and usually harmless; an
INCOMPLETE test runs and passes while asserting nothing. Under acouchbase the
unresolved call returns a coroutine nothing awaits, so the operation never runs.
Under txcouchbase it returns a `Deferred` nothing waits on, so the operation is
submitted and the test races it, which passes intermittently and reads as a flaky
server.

Read the buckets per API, never merged: the same test can land in different buckets
under acouchbase and txcouchbase, correctly. Every line carries an API prefix for
that reason, since the two variants of a suite report against the same source path
and line. Unresolved sites are grouped by cause rather than by line, so one missing
manager class is one line rather than 172.

By default the report names the tests in INCOMPLETE and BLOCKED and only counts the
rest: GENUINELY SYNC alone is 219 names, and scrolling past them to reach the ten
that need a decision is the wrong way round. `--verbose` names them all.

### LINES OVER 120 COLS

Bridging a sync call only ever makes the line longer, so a source line that fits the
repository's 120-column limit can exceed it once rewritten. flake8 never flags the
source line, because the source line is fine, which makes this the one finding an
author cannot see from the source.

Budgets, from the measured worst-case growth per line:

- **107 columns** for a suite that targets acouchbase, worst observed growth +13.
- **90 columns** if it also targets txcouchbase, worst observed growth +30.

`await ` is six columns in front of an untouched expression, while
`run_in_reactor_thread(...)` names the callable and its arguments separately and
costs 23 on its own, compounding with a `TestEnvironment` rename on the same line.
Treat 90 as a working budget rather than a proof: two bridged calls on one line
would cost 46, and no current line does that.

autopep8 and isort run as a post-pass with the pre-commit settings, and they do not
close the gap: autopep8 will not break a subscript or a long call, so an over-long
generated line reaches disk and the report is the only place it surfaces. A wrapped
return annotation keeps its wrap, which is the only lever available when a signature
will not fit.

## Writing a sync suite the generator can transform

The transform decides whether a call needs bridging from **the type of its
receiver**, resolved statically. It never guesses: a receiver it cannot type is
reported and left alone. Nearly every rule below exists because breaking it produced
a generated test that collected, ran, and passed while asserting nothing.

- **Annotate every parameter carrying an SDK or environment object.** Test, fixture,
  nested closure and helper parameters all count. The annotation on `cb_env` becomes
  `AsyncTestEnvironment` or `TwistedTestEnvironment`, and it types the receiver for
  the whole body, so one unannotated helper parameter leaves every call site in that
  helper untransformed.
- **Bind SDK objects in a form that can be typed.** Method-call chains resolve
  through a table of return types; a return type missing from it produces an untyped
  local, and every call on that local is reported. Add the entry rather than working
  around it.
- **Name the operation under test; never reach it through `getattr`.** A local bound
  from `getattr(...)` has no type, and an untyped bare name resolves sync by
  omission, so the generated test calls a coroutine function and throws the coroutine
  away. Write the named tests.
- **Bind a manager to a local before calling it.** `mgr = cb_env.get_manager(X)` then
  `mgr.method()`, never `cb_env.get_manager(X).method()`. The receiver of the inline
  form is itself a call, so the manager call is left alone while other calls in the
  class still make the test a coroutine: a test the report calls CLEAN that creates a
  coroutine and discards it.
- **Iterate a result directly; never wrap it.** `for row in result.rows():` becomes
  an `async for`. `enumerate(result.rows())` cannot, since `enumerate` consumes its
  argument eagerly and has no async counterpart, and neither can `zip`, `list`,
  `sorted` or `sum`. Count by hand, incrementing an `int` rather than reading `len()`
  of the list you are appending to.
- **Do not reach into private SDK internals.** `result` is typed, `result._request`
  is not, so the call after it reports as an unresolved receiver. Suppressing that by
  method name would suppress `collection.get(key)` too. A test that needs to assert
  on request parameters is a signal the SDK should expose them.
- **Do not bind a non-SDK value to an SDK-sounding name.**
  `bucket = bm.get_bucket(...)` is a `BucketSettings`, and `bucket.get('num_replicas')`
  is a mapping lookup that reports as an unresolved SDK call. Name it
  `bucket_settings`.
- **Import names from `couchbase.*` and let the generator move them.** The unit of
  rewriting is the name, not the module, because the async modules re-export only
  some names. Function-body imports are rewritten too. Do not write the async
  spelling yourself.
- **One `TEST_MANIFEST` entry per line.** The exclusion and sidecar mechanisms edit
  the manifest as whole-line deletes and inserts, so an entry sharing a line is
  reported and skipped, and the class then fails at setup.
- **Identify a fixture by its decorator, and never name a fixture `test_*`.** The
  transform matches the decorator exactly, and pytest collects a fixture that has
  lost its decorator as a test.
- **Annotate a list literal read before its first `append`.** `rows: list[Any] = []`.
  mypy defers inference for `x = []` until the first `append`, and reading the name
  before then breaks the deferral, raising the error in a file nobody edited.
- **Never name a module in a `monkeypatch.setattr` string literal.** Use the object
  form, `monkeypatch.setattr(sys.modules[type(obj).__module__], 'Name', double)`. The
  transform rewrites imports, not string contents, and it must not start: a generator
  guessing which strings are module paths would eventually rewrite a document key.
  The dotted form is correct in the sync file and silently wrong in every copy made
  from it, patching one module while the code under test resolves the name in
  another. The double is never constructed, the real class is, and the test passes.
  Nothing reports this one.
- **Put the query call inside `pytest.raises`, not above it.** A lazily-executed
  query raises when the rows are iterated under sync and acouchbase, and at call time
  under Twisted. Put both the call and the iteration inside the block and all three
  APIs agree; nothing is weakened, since `pytest.raises` asserts that some statement
  in the block raises. Both forms transform cleanly and only one works, so nothing
  reports it.
- **Never write a prose `pytest.skip()`.** Skips go through `tests.skips`, one
  function per category, so that expected skips can be counted from the JUnit XML
  rather than read as sentences.
- **Never sleep then assert.** Use `cb_assert_eventually` when you are waiting for
  the product to do what the test claims, and `cb_env.wait_until` when you are
  waiting for the environment to become what the suite was promised: a timeout in the
  first is a test failure, and in the second a degraded environment.

### One more, for a suite that also targets txcouchbase

**Pass `try_n_times` a callable the resolver can classify, and do not write your own
retry helper.**

`TwistedTestEnvironment.try_n_times(n, s, func, *args)` defaults to running `func` on
the reactor thread, which is right only when `func` is a bare Deferred-returning SDK
method. A local closure, an environment method or a manager wrapper is ordinary
blocking Python that bridges internally, and dispatching it to the reactor means it
blocks on the reactor it is already running on. That deadlocks, and the deadlock does
not fail: the suite hangs until the CI job times out, with no traceback and nothing
naming the call.

So the generator resolves the third positional argument and injects
`is_deferred=False` when the callable is not an SDK method. What this asks of an
author is only that the callable is a plain name or attribute path, which the
annotation rules already make resolvable. A lambda, a `functools.partial`, or an
attribute on an untypeable receiver resolves to "cannot tell", and an unresolvable
callable is reported *and left dispatching to the reactor*, because the generator
cannot know that changing the default is safe either. Unlike every other report here,
that one describes a live deadlock rather than an incomplete rewrite. Read it.

**Never write `is_deferred=False` in the sync source.** It looks like the obvious fix
and it is only a keyword the Twisted helper understands. The sync and acouchbase
environments forward `**kwargs` into `func(*args, **kwargs)`, so the keyword is passed
to the callable itself and raises `TypeError`, once per retry. The keyword belongs
only in generated output, which is why the generator injects it.

The generator recognizes exactly two retry helpers, `try_n_times` and
`try_n_times_till_exception`. A new one of your own that accepts an SDK callable gets
no dispatch analysis, so nothing is injected and nothing is reported. If a suite
genuinely needs one, teach the generator about it in the same change.

## What the generator will not do

Each of these is a place an author might reasonably expect help and will not get it.

- **It will not guess.** An unresolved receiver is reported and left untouched. A
  wrong `await` and a missing one are both worse than a report.
- **It will not report a call through a bare, untyped name.** A *receiver* it cannot
  type is reported; a *name* it cannot type is not, because the same code path
  carries every plain function call in every suite. The one exception is a name the
  source bound through `getattr`, which is narrow enough to say something about.
- **It will not rewrite an async iterable out of a sync consumer**, and it will not
  wrap a line to fit.
- **It will not invent an async counterpart.** A manager class, environment method or
  module with no async version is a blocker naming exactly what is missing, and
  nothing is written.
- **It will not decide per-test whether a class is async** (acouchbase only).
  Asyncness is decided per class: if any method in the class touches the SDK, every
  test in it is emitted as a coroutine. A strictly minimal rule would leave a lone
  `def` in an otherwise-async class, which reads as an oversight rather than as a
  finding. Each such test is reported as a note.
- **It will not propagate under txcouchbase.** The bridge blocks, so the wave stops at
  the SDK call. Applying the acouchbase propagation here would dispatch plain Python
  helpers to the reactor thread and deadlock.
- **It will not touch any decorator except two, and neither under txcouchbase.** It
  inserts `@pytest.mark.asyncio` on async tests and swaps `@pytest.fixture` for
  `@pytest_asyncio.fixture` on async fixtures. Everything else is carried through
  untouched, which is how the `usefixtures` markers survive.

## The suite table

`config/testgen.yaml` decides what is generated and where it lands. No code in
`tools/testgen` knows the name of any suite, service or SDK feature.

```yaml
source_dir: couchbase/tests

output:
  acouchbase: acouchbase/tests/_generated
  txcouchbase: txcouchbase/tests/_generated

defaults:
  targets: [acouchbase, txcouchbase]

suites:
  - source: couchbase/tests/collection_t.py

  # No Twisted KV range scan support: TxCollection has no scan() and there is no
  # txcouchbase.kv_range_scan module.
  - source: couchbase/tests/kv_range_scan_t.py
    targets: [acouchbase]

not_generated:
  <reason>:
    - couchbase/tests/<suite>_t.py
```

Every narrowed `targets` list is a capability fact about the SDK, stated at the entry,
and reading them is the fastest way to see where the three API trees genuinely differ.
`not_generated` accounts for every remaining source under `source_dir`, so a new suite
that is neither generated nor explained fails validation rather than being silently
skipped.

Validation is the point of `config`, and the mistake it exists to catch is the quiet
one: `target:` written for `targets:` reads as "no override", so the suite generates
for an API that cannot run it.

## Two mechanisms that are not in the table

1. **`testgen_exclude`**, a mark carried by a test or a class.
2. **A sidecar**, a hand-written file merged into one API's generated output.

**`testgen_exclude`** drops a test or a class, and its `TEST_MANIFEST` entry, for the
named APIs, before analysis rather than after:

```python
@pytest.mark.testgen_exclude('acouchbase', 'txcouchbase',
                             reason='drives a threading.Thread; no async analogue')
def test_query_in_thread(self, cb_env: SyncTestEnvironment) -> None:
```

Excluded tests get their own bucket in the report, kept apart from the analysis
buckets, because an exclusion is a decision and not a finding. Counting one as
"genuinely sync" would let a generated suite shrink without anyone noticing.

**A sidecar** is a hand-written file merged verbatim into the generated output, for a
test that asserts something only one API has and so has no sync form to be derived
from. It is never transformed. The class name in the sidecar is the merge target, not
the file name, and a name already present on the generated class is a blocker rather
than a silent redefinition. If it declares its own `TEST_MANIFEST` that manifest has
to match its own methods exactly, and the merger adds those names to the target
class's manifest. Write it in the idiom of its own API: an acouchbase sidecar is
`async def` and marked `@pytest.mark.asyncio`, a txcouchbase sidecar is plain `def`
calling `run_in_reactor_thread` by hand.

The question that decides a sidecar is whether there is a generated file to merge
into. A suite with a sync source that generates nothing gets a standalone hand-written
file in the API tree instead.

**Before writing either, ask whether the test should be in all three APIs.** If it
should, put it in the sync source and all three get it. A sidecar is earned only by
asserting something that exists solely under one API.

## Why the AST is used to locate edits and not to rewrite them

The obvious shape, parse then mutate then `ast.unparse`, cannot be used here. `ast`
discards comments, and these suites carry comments that have to survive verbatim: the
Apache header is a licensing requirement, and the blocks recording which SDK defect a
workaround exists for are the reason the workaround is readable. `ast.unparse` also
renormalises every quote, line break and parenthesis, which turns reviewing a
generated diff into reading a full-file rewrite.

So the AST is used only to locate edits, by line and column, and the edits are applied
to the original source text. Untouched bytes are never re-emitted, so formatting and
comments are preserved by construction.

## Tests

```
python -m pytest tools/testgen/tests -q
```

They cover the parts with no observable behaviour of their own: suite-table
validation, the formatter post-pass, the transform's edit application, and the three
kinds of difference `check` reports. The end-to-end check is `check` itself against
the committed trees.
