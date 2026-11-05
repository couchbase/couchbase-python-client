import collections
import dataclasses
import pathlib
import re

import pytest

from tools.check_public_api import (INVENTORY,
                                    Entry,
                                    read_counterparts,
                                    read_inventory)
from tools.docgen import render
from tools.docgen.layout import (LAYOUT,
                                 Group,
                                 Layout,
                                 LayoutError,
                                 Page,
                                 Replacement,
                                 Section,
                                 default_slug,
                                 load,
                                 problems)
from tools.docgen.render import DEPRECATED, Reference

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
SECTIONS = {key: Section(key, '') for key in ('common', 'couchbase', 'acouchbase', 'txcouchbase')}
INVENTORY_ = {
    'couchbase.cluster': Entry(('Cluster',), (), True),
    'couchbase.options': Entry(('GetOptions', 'UpsertOptions'), ('forward_args',), True),
    'acouchbase.cluster': Entry(('AsyncCluster',), (), True),
    'txcouchbase.cluster': Entry(('TxCluster',), (), True),
}
COUNTERPARTS = {
    'couchbase.cluster': {'acouchbase': 'own', 'txcouchbase': 'own'},
    'couchbase.options': {'acouchbase': 'shim', 'txcouchbase': 'shim'},
}


def layout(common=None, apis=None, exclude=None, replacements=None) -> Layout:
    return Layout('docs', 'reference', SECTIONS,
                  tuple(common if common is not None else [Page('Options', 'options', ('couchbase.options',))]),
                  tuple(apis if apis is not None else [Page('Cluster', 'cluster', ('couchbase.cluster',))]),
                  exclude or {}, replacements or {})


@pytest.mark.parametrize('module, slug', [
    ('couchbase', 'package'),
    ('couchbase.cluster', 'cluster'),
    ('couchbase.management.buckets', 'management/buckets'),
])
def test_default_slug(module, slug):
    assert default_slug(module) == slug


def test_a_consistent_layout_has_no_problems():
    assert problems(layout(), INVENTORY_, COUNTERPARTS) == []


def test_an_unplaced_module_is_reported():
    found = problems(layout(common=[]), INVENTORY_, COUNTERPARTS)
    assert found == ['couchbase.options: docs: true, and the layout does not place it']


def test_a_module_placed_twice_is_reported():
    twice = [Page('Options', 'options', ('couchbase.options',)), Page('Again', 'again', ('couchbase.options',))]
    assert problems(layout(common=twice), INVENTORY_, COUNTERPARTS) == [
        'couchbase.options: placed in common and again in common']


def test_a_module_an_api_writes_cannot_be_common():
    found = problems(layout(common=[Page('Options', 'options', ('couchbase.options',)),
                                    Page('Cluster', 'cluster', ('couchbase.cluster',))], apis=[]),
                     INVENTORY_, COUNTERPARTS)
    assert found == ["couchbase.cluster: in common, but ['acouchbase', 'txcouchbase'] write their own; "
                     'place it under apis']


def test_a_module_no_api_writes_cannot_be_under_apis():
    found = problems(layout(common=[], apis=[Page('Cluster', 'cluster', ('couchbase.cluster',)),
                                             Page('Options', 'options', ('couchbase.options',))]),
                     INVENTORY_, COUNTERPARTS)
    assert found == ['couchbase.options: under apis, but no other API writes its own; place it in common']


def test_an_undocumented_module_cannot_be_placed():
    inventory = {**INVENTORY_, 'couchbase.views': Entry(('View',), ())}
    common = [Page('Options', 'options', ('couchbase.options',)), Page('Views', 'views', ('couchbase.views',))]
    assert problems(layout(common=common), inventory, COUNTERPARTS) == [
        'couchbase.views: placed in common, and not a couchbase module with docs: true']


def test_a_slug_used_twice_is_reported():
    inventory = {**INVENTORY_, 'couchbase.views': Entry(('View',), (), True)}
    counterparts = {**COUNTERPARTS, 'couchbase.views': {'acouchbase': 'shim', 'txcouchbase': 'shim'}}
    common = [Page('Options', 'options', ('couchbase.options',)), Page('Views', 'options', ('couchbase.views',))]
    assert problems(layout(common=common), inventory, counterparts) == ["common: slug 'options' is used twice"]


@pytest.mark.parametrize('groups, expected', [
    ((Group('KV', ('GetOptions',)), Group('Writes', ('UpsertOptions',))), []),
    ((Group('KV', ('GetOptions',)),), ["page 'Options': ['UpsertOptions'] in no group"]),
    ((Group('KV', ('GetOptions', 'UpsertOptions')), Group('Writes', ('UpsertOptions',))),
     ["page 'Options': ['UpsertOptions'] in more than one group"]),
    ((Group('KV', ('GetOptions', 'UpsertOptions', 'Nope')),),
     ["page 'Options': groups name ['Nope'], which couchbase.options does not publish"]),
])
def test_groups_hold_each_public_name_once(groups, expected):
    common = [Page('Options', 'options', ('couchbase.options',), groups)]
    assert problems(layout(common=common), INVENTORY_, COUNTERPARTS) == expected


def test_groups_belong_only_on_common_pages():
    apis = [Page('Cluster', 'cluster', ('couchbase.cluster',), (Group('All', ('Cluster',)),))]
    assert problems(layout(apis=apis), INVENTORY_, COUNTERPARTS) == [
        "page 'Cluster': groups belong on a common page of one module, where every API reads the same names"]


def test_exclude_members_names_a_documented_object():
    found = problems(layout(exclude={'couchbase.cluster.Nope': ('a',), 'couchbase.cluster.Cluster': ('b', 'a')}),
                     INVENTORY_, COUNTERPARTS)
    assert found == ['exclude_members: couchbase.cluster.Cluster is not sorted, or names a member twice',
                     'exclude_members: couchbase.cluster.Nope is not a public name of a documented couchbase module']


@pytest.mark.parametrize('replacements, expected', [
    ({'couchbase.options.forward_args': Replacement(None, 'internal')}, []),
    ({'couchbase.options.forward_args': Replacement('couchbase.options.GetOptions')}, []),
    ({'couchbase.options.GetOptions': Replacement(None, 'internal')},
     ['replacements: couchbase.options.GetOptions is not a compat name']),
    ({'couchbase.options.forward_args': Replacement('couchbase.options.Nope')},
     ['replacements: couchbase.options.forward_args uses couchbase.options.Nope, which is not a public name']),
    ({'couchbase.options.forward_args': Replacement(None)},
     ['replacements: couchbase.options.forward_args has none, and no reason']),
])
def test_replacements_name_a_compat_name_and_a_public_one(replacements, expected):
    assert problems(layout(replacements=replacements), INVENTORY_, COUNTERPARTS) == expected


@pytest.mark.parametrize('text, complaint', [
    ('docs: docs\n', 'missing keys'),
    ('docs: docs\noutput: r\nsections: {}\ncommon: []\napis: []\nextra: 1\n', 'unknown keys'),
])
def test_load_rejects_a_malformed_layout(tmp_path, text, complaint):
    path = tmp_path / 'layout.yaml'
    path.write_text(text)
    with pytest.raises(LayoutError, match=complaint):
        load(path)


@pytest.mark.parametrize('value', ['{use: a, none: b}', '{instead: a}', 'a'])
def test_load_rejects_a_malformed_replacement(tmp_path, value):
    sections = ''.join(f'  {k}: {{title: T, intro: I}}\n' for k in SECTIONS)
    path = tmp_path / 'layout.yaml'
    path.write_text(f'docs: docs\noutput: r\nsections:\n{sections}common: []\napis: []\n'
                    f'replacements:\n  couchbase.options.forward_args: {value}\n')
    with pytest.raises(LayoutError, match='use: <path> or none: <reason>'):
        load(path)


def test_a_page_takes_modules_or_pages_and_not_both(tmp_path):
    sections = ''.join(f'  {k}: {{title: T, intro: I}}\n' for k in SECTIONS)
    path = tmp_path / 'layout.yaml'
    path.write_text(f'docs: docs\noutput: r\nsections:\n{sections}common:\n'
                    '  - {title: T, modules: [couchbase.options], pages: []}\napis: []\n')
    with pytest.raises(LayoutError, match='either modules or pages'):
        load(path)


@pytest.fixture(scope='module')
def reference():
    inventory = read_inventory(INVENTORY)
    counterparts = read_counterparts(INVENTORY)
    reference_layout = load(LAYOUT)
    assert problems(reference_layout, inventory, counterparts) == []
    return Reference(ROOT, reference_layout, inventory, counterparts)


@pytest.fixture(scope='module')
def pages(reference):
    return reference.render(), reference.inventory


def test_every_compat_name_has_a_replacement_given_only_where_needed(reference):
    assert reference.replacement_problems() == []


def test_the_deprecated_paths_table_has_a_row_per_compat_name(pages):
    rendered, inventory = pages
    rows = re.findall(r'^   \* - ``(\S+)``$', rendered[f'reference/{DEPRECATED}'], re.MULTILINE)
    assert rows == sorted(f'{m}.{n}' for m, e in inventory.items() for n in e.compat)


def test_a_replacement_the_generator_finds_by_itself_is_reported(reference):
    path = 'couchbase.cluster.ClusterOptions'
    given = dataclasses.replace(reference.layout,
                                replacements={**reference.layout.replacements, path: Replacement(None, 'x')})
    found = Reference(ROOT, given, reference.inventory, reference.counterparts).replacement_problems()
    assert found == [f'replacements: {path} resolves to couchbase.options.ClusterOptions; remove it']


def test_no_documented_name_is_marked_internal(reference):
    assert reference.internal_problems() == []


def test_a_documented_name_marked_internal_is_reported(reference, monkeypatch):
    def marked():
        """**INTERNAL** Called by the SDK."""

    def unmarked():
        """Called by applications, and mentions **INTERNAL** later on."""

    objects = {'couchbase.marked': marked, 'couchbase.unmarked': unmarked, 'couchbase.data': '**INTERNAL**'}
    monkeypatch.setattr(render, '_resolve', objects.get)
    given = Reference(ROOT, reference.layout, reference.inventory, reference.counterparts)
    given.canonical = {(target, ''): target for target in objects}
    assert [p.split(':')[0] for p in given.internal_problems()] == ['couchbase.marked']


def test_every_documented_name_is_described_once_or_linked(pages):
    """Each object is described on exactly one page, and every other page publishing it links there."""
    rendered, inventory = pages
    described = collections.Counter()
    linked = set()
    for text in rendered.values():
        module = None
        for line in text.splitlines():
            if m := re.match(r'\.\. module:: (\S+)$', line):
                module = m.group(1)
            elif m := re.match(r'\.\. auto\w+:: (\S+)$', line):
                described[f'{module}.{m.group(1)}'] += 1
            elif m := re.match(r'\* :py:obj:`(\S+)`$', line):
                linked.add((module, m.group(1)))
    assert [t for t, n in described.items() if n > 1] == []
    missing = [f'{module}.{name}' for module, entry in inventory.items() if entry.docs for name in entry.public
               if f'{module}.{name}' not in described
               and not any(m == module and t.endswith(f'.{name}') for m, t in linked)]
    assert missing == []
    assert all(target in described for _, target in linked)


def test_a_compat_name_the_generator_cannot_place_needs_a_replacement(reference):
    path = 'couchbase.options.forward_args'
    given = dataclasses.replace(reference.layout,
                                replacements={k: v for k, v in reference.layout.replacements.items() if k != path})
    found = Reference(ROOT, given, reference.inventory, reference.counterparts).replacement_problems()
    assert found == [f'replacements: {path} has no documented counterpart; say what replaces it']
