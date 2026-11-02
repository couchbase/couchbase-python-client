import pathlib
import sys
import types

import pytest

from tools.check_api_parity import (EXEMPTIONS,
                                    FACADES,
                                    Exemption,
                                    aspects,
                                    compare,
                                    documented,
                                    gaps,
                                    main,
                                    positional_only,
                                    public_members,
                                    read_exemptions,
                                    resolve,
                                    sections)


class _Base:
    def inherited(self) -> None: ...


class _Sync(_Base):
    def shared(self) -> None: ...
    def sync_only(self) -> None: ...
    def _private(self) -> None: ...

    @property
    def prop(self) -> int:
        return 0


class _Async(_Base):
    def shared(self) -> None: ...
    def async_only(self) -> None: ...

    @property
    def prop(self) -> int:
        return 0


@pytest.fixture
def fake_api(monkeypatch: pytest.MonkeyPatch) -> str:
    module = types.ModuleType('_parity_fake')
    module.Sync = _Sync  # type: ignore[attr-defined]
    module.Async = _Async  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, '_parity_fake', module)
    return '_parity_fake'


def test_public_members_include_inherited_and_properties_but_not_private() -> None:
    assert public_members(_Sync) == {'inherited', 'shared', 'sync_only', 'prop'}


def test_resolve_a_missing_module_or_class_is_none(fake_api: str) -> None:
    assert resolve(f'{fake_api}.Sync') is _Sync
    assert resolve(f'{fake_api}.Nope') is None
    assert resolve('_parity_no_such_module.Sync') is None


def test_gaps_name_both_directions_and_absent_classes(fake_api: str) -> None:
    found = gaps([(f'{fake_api}.Sync', f'{fake_api}.Async', f'{fake_api}.Missing')])
    assert found == [f'{fake_api}.Async.sync_only', f'{fake_api}.Async.async_only', f'{fake_api}.Missing']


def test_a_sync_facade_that_does_not_exist_is_an_error(fake_api: str) -> None:
    with pytest.raises(LookupError):
        gaps([(f'{fake_api}.Gone', f'{fake_api}.Async')])


def test_an_uncovered_gap_and_an_unused_exemption_both_fail() -> None:
    exemptions = [Exemption('a.B.*_multi', 'sync only'), Exemption('a.B.stale', 'gone')]
    problems = compare(['a.B.get_multi', 'a.B.scan'], exemptions)
    assert len(problems) == 2
    assert problems[0].startswith('a.B.scan: exists on one side only')
    assert problems[1].startswith('a.B.stale: the exemption matches no difference')


def test_every_gap_covered_and_every_exemption_used_is_clean() -> None:
    assert compare(['a.B.get_multi', 'a.B.upsert_multi'], [Exemption('a.B.*_multi', 'sync only')]) == []


def test_an_exemption_without_a_reason_is_refused(tmp_path: pathlib.Path) -> None:
    path = tmp_path / 'exemptions.txt'
    path.write_text('# comment\n\na.B.c: why\na.B.d:\n')
    with pytest.raises(ValueError, match=':4:'):
        read_exemptions(path)


def test_every_sync_facade_exists() -> None:
    assert all(resolve(sync) is not None for sync, *_ in FACADES)


def test_the_tree_agrees_with_the_exemptions() -> None:
    assert EXEMPTIONS.exists()
    assert main([]) == 0


_DOC = """Gets a thing.

    Args:
        key (str): The key.
        *opts (:class:`~couchbase.options.GetOptions`): Options,
            continued on a deeper line.

    Returns:
        :class:`~couchbase.result.GetResult`: The result.

    Raises:
        :class:`~couchbase.exceptions.DocumentNotFoundException`: If it is absent.
        TimeoutError: If it is slow.

    Examples:
        Not compared::

            get('key')
    """


def test_sections_read_entries_at_the_section_indent_only() -> None:
    found = sections(_DOC)
    assert found is not None
    assert documented(found) == {'args': ['key', 'opts'],
                                 'raises': ['DocumentNotFoundException', 'TimeoutError'],
                                 'returns': 'GetResult'}


def test_a_continuation_at_the_entry_indent_reads_as_an_entry() -> None:
    found = sections(_DOC.replace('            continued', '        continued'))
    assert found is not None
    assert documented(found)['args'] == ['key', 'opts', 'continued']


def test_no_docstring_is_none_and_a_docstring_without_sections_is_empty() -> None:
    assert sections(None) is None
    assert sections('   ') is None
    assert sections('Just a summary.') == {}


@pytest.mark.parametrize('returns, expected', [
    ('Awaitable[:class:`~couchbase.result.GetResult`]: A future.', 'GetResult'),
    ('Deferred[:class:`~acouchbase.result.AsyncQueryResult`]: A deferred.', 'QueryResult'),
    (':class:`~acouchbase.bucket.AsyncBucket`: A bucket.', 'Bucket'),
    ('`Deferred`: An empty deferred.', 'None'),
    ('Awaitable: An empty future.', 'None'),
])
def test_returns_drop_the_async_wrapper_and_prefix(returns: str, expected: str) -> None:
    found = sections(f'Summary.\n\nReturns:\n    {returns}\n')
    assert found is not None
    assert documented(found)['returns'] == expected


def test_a_missing_returns_reads_as_none() -> None:
    assert documented({})['returns'] == 'None'


class _Documented:
    def op(self, key: str, flag: bool = False) -> None:
        """Does it.

        Args:
            key (str): The key.
            flag (bool): The flag.

        Raises:
            ValueError: If it is bad.
        """

    def undocumented(self) -> None: ...


class _Agrees:
    async def op(self, key: str, flag: bool = False) -> None:
        """Does it, asynchronously.

        Args:
            key (str): The key.
            flag (bool): The flag.

        Returns:
            Awaitable[None]: A future.

        Raises:
            ValueError: If it is bad.
        """

    def undocumented(self, extra: int) -> None:
        """Documented here, which is not compared when the sync side is not."""


class _Disagrees:
    def op(self, key: str, flag: bool = True) -> None:
        """Does it.

        Args:
            key (str): The key.

        Returns:
            int: A number.
        """

    def undocumented(self) -> None: ...


class _Undocumented:
    def op(self, key: str, flag: bool = False) -> None: ...

    def undocumented(self) -> None: ...


def test_aspects_agree_through_async_and_skip_an_undocumented_sync_member() -> None:
    assert aspects(_Documented, _Agrees, 'op') == []
    assert aspects(_Documented, _Agrees, 'undocumented') == ['signature']


def test_aspects_name_each_disagreement() -> None:
    assert aspects(_Documented, _Disagrees, 'op') == ['signature', 'args', 'raises', 'returns']


def test_a_counterpart_without_a_docstring_is_a_doc_gap() -> None:
    assert aspects(_Documented, _Undocumented, 'op') == ['doc']


def test_gaps_name_aspects_on_shared_members(monkeypatch: pytest.MonkeyPatch) -> None:
    module = types.ModuleType('_parity_doc_fake')
    module.Documented = _Documented  # type: ignore[attr-defined]
    module.Disagrees = _Disagrees  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, '_parity_doc_fake', module)
    found = gaps([('_parity_doc_fake.Documented', '_parity_doc_fake.Disagrees')])
    assert found == [f'_parity_doc_fake.Disagrees.op({a})' for a in ('signature', 'args', 'raises', 'returns')]


class _PositionalOnly:
    def op(self, name: str, options: object = None, /, **kwargs: object) -> None: ...

    def ordinary(self, name: str, options: object = None, **kwargs: object) -> None: ...

    @property
    def prop(self) -> int:
        return 0


def test_a_parameter_before_a_slash_is_a_gap_and_an_ordinary_one_is_not() -> None:
    assert positional_only('a.B', _PositionalOnly) == ['a.B.op(positional-only)']


def test_positional_only_is_checked_on_the_sync_facade_and_every_counterpart(monkeypatch: pytest.MonkeyPatch) -> None:
    module = types.ModuleType('_parity_slash_fake')
    module.Sync = _PositionalOnly  # type: ignore[attr-defined]
    module.Async = _PositionalOnly  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, '_parity_slash_fake', module)
    found = gaps([('_parity_slash_fake.Sync', '_parity_slash_fake.Async')])
    assert found == ['_parity_slash_fake.Sync.op(positional-only)', '_parity_slash_fake.Async.op(positional-only)']


def test_an_uncovered_positional_only_gap_says_to_remove_the_slash() -> None:
    problems = compare(['a.B.op(positional-only)'], [])
    assert problems == ['a.B.op(positional-only): a public parameter cannot be passed by name and no exemption '
                        'covers it; remove the `/`, or add an exemption saying why it is there']
