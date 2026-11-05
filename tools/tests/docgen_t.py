import collections
import pathlib
import re

import pytest

from tools.check_public_api import (INVENTORY,
                                    Entry,
                                    read_counterparts,
                                    read_inventory)
from tools.docgen.layout import (LAYOUT,
                                 Group,
                                 Layout,
                                 LayoutError,
                                 Page,
                                 Section,
                                 default_slug,
                                 load,
                                 problems)
from tools.docgen.render import Reference

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
SECTIONS = {key: Section(key, '') for key in ('common', 'couchbase', 'acouchbase', 'txcouchbase')}
INVENTORY_ = {
    'couchbase.cluster': Entry(('Cluster',), (), True),
    'couchbase.options': Entry(('GetOptions', 'UpsertOptions'), (), True),
    'acouchbase.cluster': Entry(('AsyncCluster',), (), True),
    'txcouchbase.cluster': Entry(('TxCluster',), (), True),
}
COUNTERPARTS = {
    'couchbase.cluster': {'acouchbase': 'own', 'txcouchbase': 'own'},
    'couchbase.options': {'acouchbase': 'shim', 'txcouchbase': 'shim'},
}


def layout(common=None, apis=None, exclude=None) -> Layout:
    return Layout('docs', 'reference', SECTIONS,
                  tuple(common if common is not None else [Page('Options', 'options', ('couchbase.options',))]),
                  tuple(apis if apis is not None else [Page('Cluster', 'cluster', ('couchbase.cluster',))]),
                  exclude or {})


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


@pytest.mark.parametrize('text, complaint', [
    ('docs: docs\n', 'missing keys'),
    ('docs: docs\noutput: r\nsections: {}\ncommon: []\napis: []\nextra: 1\n', 'unknown keys'),
])
def test_load_rejects_a_malformed_layout(tmp_path, text, complaint):
    path = tmp_path / 'layout.yaml'
    path.write_text(text)
    with pytest.raises(LayoutError, match=complaint):
        load(path)


def test_a_page_takes_modules_or_pages_and_not_both(tmp_path):
    sections = ''.join(f'  {k}: {{title: T, intro: I}}\n' for k in SECTIONS)
    path = tmp_path / 'layout.yaml'
    path.write_text(f'docs: docs\noutput: r\nsections:\n{sections}common:\n'
                    '  - {title: T, modules: [couchbase.options], pages: []}\napis: []\n')
    with pytest.raises(LayoutError, match='either modules or pages'):
        load(path)


@pytest.fixture(scope='module')
def pages():
    inventory = read_inventory(INVENTORY)
    counterparts = read_counterparts(INVENTORY)
    reference_layout = load(LAYOUT)
    assert problems(reference_layout, inventory, counterparts) == []
    return Reference(ROOT, reference_layout, inventory, counterparts).render(), inventory


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
