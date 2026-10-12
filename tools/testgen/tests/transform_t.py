import ast

from tools.testgen.apply import apply_edits
from tools.testgen.resolver import Resolver
from tools.testgen.transform import (AsyncTransformer,
                                     Report,
                                     SuiteTransformer,
                                     TwistedTransformer)


def _generated(src: str, transformer: type[SuiteTransformer]) -> str:
    """One suite through the whole transform, as the generator runs it."""
    tr = transformer(src, 'x_t.py', Resolver(transformer.api), Report())
    tr.run()
    return apply_edits(src, tr.edits)


UPSERT = ('class Thing:\n\n'
          '    def test_upsert(self, cb_env: SyncTestEnvironment) -> None:\n'
          "        res = cb_env.collection.upsert('key', {})\n"
          '        assert res.cas is not None\n')


def test_an_sdk_call_is_awaited_and_the_test_holding_it_becomes_a_coroutine() -> None:
    """What the tool is for, on the acouchbase side.

    An unbridged call returns a coroutine nothing runs, so the operation never reaches the
    cluster and the assertion below it measures a result of an upsert that never happened.
    The test passes.
    """
    out = _generated(UPSERT, AsyncTransformer)

    assert '    @pytest.mark.asyncio\n    async def test_upsert(' in out
    assert "        res = await cb_env.collection.upsert('key', {})\n" in out


def test_the_same_call_is_wrapped_for_the_reactor_and_its_test_stays_synchronous() -> None:
    """The txcouchbase side, which is not the acouchbase one with other names in it.

    The callable and its arguments come apart, and the `def` line is untouched:
    run_in_reactor_thread() blocks, so nothing here is a coroutine.  An `async def` would
    inherit the async side's answer to a question Twisted does not ask.
    """
    out = _generated(UPSERT, TwistedTransformer)

    assert '    def test_upsert(self, cb_env: TwistedTestEnvironment) -> None:\n' in out
    assert "        res = run_in_reactor_thread(cb_env.collection.upsert, 'key', {})\n" in out
    assert 'async' not in out and 'await' not in out


def test_an_awaited_call_that_is_subscripted_is_parenthesised() -> None:
    """`await cb.get(k).content_as[dict]` awaits the subscript rather than the call.

    It parses and it runs, and what it raises is an AttributeError against a coroutine,
    which names neither the operation nor the line that is wrong.  The wrapping bridge
    binds correctly without help, and the second assertion is what keeps a later reader
    from making the two symmetric.
    """
    src = ('class Thing:\n\n'
           '    def test_get(self, cb_env: SyncTestEnvironment) -> None:\n'
           "        body = cb_env.collection.get('key').content_as[dict]\n")

    assert ("(await cb_env.collection.get('key')).content_as[dict]"
            in _generated(src, AsyncTransformer))
    assert ("run_in_reactor_thread(cb_env.collection.get, 'key').content_as[dict]"
            in _generated(src, TwistedTransformer))


def test_each_awaited_call_in_a_chain_gets_its_own_parentheses() -> None:
    """Both calls start at the same column, so one `(await ` for the position is one short.

    The second `)` still lands, which leaves a file that does not parse.
    """
    src = ('from couchbase.cluster import Cluster\n\n'
           'class Thing:\n\n'
           '    def test_ping(self, cb_env: SyncTestEnvironment) -> None:\n'
           "        rid = Cluster.connect('couchbase://x').ping().id\n")

    out = _generated(src, AsyncTransformer)

    assert "(await (await Cluster.connect('couchbase://x')).ping()).id" in out
    ast.parse(out)


def test_a_renamed_call_is_renamed_inside_its_parentheses() -> None:
    """The rename and the `(await ` share a start column, and the rename's span has to be
    written before the insertion moves it."""
    src = ('from tests.environments.test_environment import build_environment\n\n'
           'class Thing:\n\n'
           '    def test_x(self, cb_base_env) -> None:\n'
           '        c = build_environment(cb_base_env, Thing).cluster\n')

    out = _generated(src, AsyncTransformer)

    assert '(await build_async_environment(acb_base_env, Thing)).cluster' in out


def test_a_split_import_in_a_function_keeps_its_indentation() -> None:
    """BucketType has no acouchbase counterpart, so the import becomes two."""
    src = ('class Thing:\n\n'
           '    def test_x(self) -> None:\n'
           '        from couchbase.management.buckets import BucketSettings, BucketType\n'
           '        assert BucketSettings and BucketType\n')

    out = _generated(src, AsyncTransformer)

    assert ('        from acouchbase.management.buckets import BucketSettings\n'
            '        from couchbase.management.buckets import BucketType\n') in out
    ast.parse(out)


HELPER = ('class Thing:\n\n'
          '    def _store(self, cb_env: SyncTestEnvironment) -> None:\n'
          "        cb_env.collection.upsert('key', {})\n\n"
          '    def test_store(self, cb_env: SyncTestEnvironment) -> None:\n'
          '        self._store(cb_env)\n')


def test_awaiting_spreads_outward_from_the_sdk_call() -> None:
    """A helper holding an awaited call is itself a coroutine, so its callers await too.

    Stopping at the helper leaves `self._store(cb_env)` returning a coroutine nothing runs.
    Same silent no-op as an unbridged call, one level further from the line that caused it.
    """
    out = _generated(HELPER, AsyncTransformer)

    assert '    async def _store(self, cb_env: AsyncTestEnvironment) -> None:\n' in out
    assert '        await self._store(cb_env)\n' in out


def test_wrapping_spreads_to_nothing() -> None:
    """The same source under Twisted, where propagating would deadlock.

    run_in_reactor_thread() blocks, so `_store` stays an ordinary function.  Wrapping the
    call to it would dispatch it to the reactor thread, where its own
    run_in_reactor_thread() waits on the reactor it is already running on: no traceback and
    no failing test, just a suite that hangs until the job times out.
    """
    out = _generated(HELPER, TwistedTransformer)

    assert '    def _store(self, cb_env: TwistedTestEnvironment) -> None:\n' in out
    assert '        self._store(cb_env)\n' in out
    assert out.count('run_in_reactor_thread(') == 1


def test_a_blocking_callable_passed_to_try_n_times_is_marked_not_deferred() -> None:
    """try_n_times() dispatches to the reactor thread, which suits an SDK method alone.

    Any other callable is ordinary blocking Python that reaches run_in_reactor_thread by
    itself, and running it on the reactor thread waits on the reactor it is already on.
    Nothing raises; the suite hangs.  So the decision is per call site, and the two sources
    below differ only in the callable they hand over.
    """
    sdk = ('class Thing:\n\n'
           '    def test_retry(self, cb_env: SyncTestEnvironment) -> None:\n'
           "        cb_env.try_n_times(3, 1, cb_env.collection.get, 'key')\n")
    env = ('class Thing:\n\n'
           '    def test_retry(self, cb_env: SyncTestEnvironment) -> None:\n'
           '        cb_env.try_n_times(3, 1, cb_env.load_data)\n')

    assert ("        cb_env.try_n_times(3, 1, cb_env.collection.get, 'key')\n"
            in _generated(sdk, TwistedTransformer))
    assert ('        cb_env.try_n_times(3, 1, cb_env.load_data, is_deferred=False)\n'
            in _generated(env, TwistedTransformer))


def _annotation_rewritten(src: str) -> str:
    """The generator-annotation rule and the name substitution, on the first method.

    Both, because the point of the rule editing in place rather than re-rendering is that
    the substitution inside the annotation survives it.
    """
    tr = AsyncTransformer(src, 'x_t.py', Resolver('acouchbase'), Report())
    cls = tr.tree.body[0]
    assert isinstance(cls, ast.ClassDef)
    tr.rewrite_generator_annotation(cls.body[0])
    tr.rewrite_names()
    return apply_edits(src, tr.edits)


def test_a_one_line_annotation_gains_async_and_loses_its_third_parameter() -> None:
    out = _annotation_rewritten(
        'class Thing:\n\n'
        '    def cb_env(self, cb_base_env: SyncTestEnvironment) -> Generator[SyncTestEnvironment, None, None]:\n'
        '        yield cb_base_env\n')

    assert ('    def cb_env(self, acb_base_env: AsyncTestEnvironment)'
            ' -> AsyncGenerator[AsyncTestEnvironment, None]:\n') in out


def test_a_wrapped_annotation_keeps_its_wrapping() -> None:
    """The transform only ever makes a line longer, so the column budget is tight.

    Re-rendering the whole subscript put an annotation the author had wrapped back onto one
    line, where the api name substitution can carry it past the limit; the report then asks
    for a shorter source line that was already inside the budget.
    """
    out = _annotation_rewritten(
        'class Thing:\n\n'
        '    def cb_env(self,\n'
        '               cb_base_env: SyncTestEnvironment\n'
        '               ) -> Generator[\n'
        '                   SyncTestEnvironment, None, None]:\n'
        '        yield cb_base_env\n')

    assert '               ) -> AsyncGenerator[\n' in out
    assert '                   AsyncTestEnvironment, None]:\n' in out


def _unresolved(src: str) -> list[str]:
    report = Report()
    AsyncTransformer(src, 'x_t.py', Resolver('acouchbase'), report).run()
    return report.unresolved


def test_a_call_through_a_getattr_binding_is_reported() -> None:
    """A typeable receiver, an untyped local, and a call that was invisible everywhere.

    The receiver is typeable and the local it is bound to is not, so the call is emitted
    unbridged and neither the source nor the report says anything about it.
    """
    rows = _unresolved(
        'class Thing:\n\n'
        '    def test_op(self, cb_env: SyncTestEnvironment) -> None:\n'
        "        operation = getattr(cb_env.collection, 'upsert')\n"
        "        operation('key', {})\n")

    assert len(rows) == 1
    assert '`operation()` is a getattr binding from line 4' in rows[0]
    assert 'x_t.py:5' in rows[0]


def test_a_local_bound_from_anything_else_is_not_reported() -> None:
    """Every plain function call in every suite takes the same path, so the binding is the
    whole of what narrows this to something worth reporting."""
    assert _unresolved(
        'class Thing:\n\n'
        '    def test_op(self, cb_env: SyncTestEnvironment) -> None:\n'
        "        operation = pick_operation('upsert')\n"
        "        operation('key', {})\n") == []
