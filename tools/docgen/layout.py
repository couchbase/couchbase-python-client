"""The layout of the API reference: which page each documented module gets, in what order.

``tools/public_api.yaml`` says which modules are documented, ``docs: true``, and what each API has at
a ``couchbase`` path.  The layout adds only what the inventory cannot: a title and an order for each
page, the pages that gather others, the groups a long page is split into, the members a page leaves
out, and what replaces a deprecated import path the generator cannot work out for itself.  Every
placement is checked against the inventory, so a module cannot be documented twice, or not at all,
and a page cannot claim a section its counterparts contradict.
"""

from __future__ import annotations

import dataclasses
import pathlib
from collections.abc import Iterator, Set
from typing import Any

import yaml

from tools.check_public_api import (APIS,
                                    OWN,
                                    Entry)

LAYOUT = pathlib.Path(__file__).parent / 'config' / 'layout.yaml'
SYNC = 'couchbase'
COMMON = 'common'
SECTIONS = (COMMON, SYNC, *APIS)


class LayoutError(ValueError):
    pass


@dataclasses.dataclass(frozen=True)
class Group:
    title: str
    names: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class Page:
    """A reference page: one or more modules, or, when ``pages`` is set, an index of other pages.

    ``slug`` is the page's path inside its section, without the suffix.
    """
    title: str
    slug: str
    modules: tuple[str, ...] = ()
    groups: tuple[Group, ...] = ()
    pages: tuple[Page, ...] = ()

    def walk(self) -> Iterator[Page]:
        yield self
        for page in self.pages:
            yield from page.walk()


@dataclasses.dataclass(frozen=True)
class Section:
    title: str
    intro: str


@dataclasses.dataclass(frozen=True)
class Layout:
    """``docs`` is the Sphinx source directory, and ``output`` the generated tree inside it."""
    docs: str
    output: str
    sections: dict[str, Section]
    common: tuple[Page, ...]
    apis: tuple[Page, ...]
    exclude_members: dict[str, tuple[str, ...]]

    def pages(self, pages: tuple[Page, ...]) -> Iterator[Page]:
        for page in pages:
            yield from page.walk()


def _keys(where: str, raw: dict[str, Any], required: Set[str], optional: Set[str] = frozenset()) -> None:
    if not isinstance(raw, dict):
        raise LayoutError(f'{where}: expected a mapping, found {raw!r}')
    missing = sorted(required - set(raw))
    unknown = sorted(set(raw) - required - optional)
    if missing or unknown:
        raise LayoutError(f'{where}: missing keys {missing}, unknown keys {unknown}')


def default_slug(module: str) -> str:
    """``couchbase.management.buckets`` is ``management/buckets``, and the package itself ``package``."""
    rest = module[len(SYNC):].lstrip('.')
    return rest.replace('.', '/') or 'package'


def _page(where: str, raw: dict[str, Any]) -> Page:
    _keys(where, raw, {'title'}, {'slug', 'modules', 'groups', 'pages'})
    title = raw['title']
    where = f'{where} {title!r}'
    if ('modules' in raw) == ('pages' in raw):
        raise LayoutError(f'{where}: give either modules or pages')
    if 'pages' in raw:
        if 'groups' in raw:
            raise LayoutError(f'{where}: an index of pages has no groups')
        slug = raw.get('slug') or title.lower().replace(' ', '_')
        return Page(title, slug, pages=tuple(_page(where, p) for p in raw['pages']))
    modules = tuple(raw['modules'])
    if not modules:
        raise LayoutError(f'{where}: modules is empty')
    groups = []
    for group in raw.get('groups') or ():
        _keys(f'{where} group', group, {'title', 'names'})
        groups.append(Group(group['title'], tuple(group['names'])))
    return Page(title, raw.get('slug') or default_slug(modules[0]), modules, tuple(groups))


def load(path: pathlib.Path = LAYOUT) -> Layout:
    raw = yaml.safe_load(path.read_text())
    _keys(path.name, raw, {'docs', 'output', 'sections', 'common', 'apis'},
          {'exclude_members'})
    _keys(f'{path.name} sections', raw['sections'], set(SECTIONS))
    sections = {}
    for key in SECTIONS:
        _keys(f'{path.name} sections.{key}', raw['sections'][key], {'title', 'intro'})
        sections[key] = Section(raw['sections'][key]['title'], raw['sections'][key]['intro'].strip())
    excluded = {target: tuple(names) for target, names in (raw.get('exclude_members') or {}).items()}
    return Layout(raw['docs'], raw['output'], sections,
                  tuple(_page(f'{path.name} common', p) for p in raw['common']),
                  tuple(_page(f'{path.name} apis', p) for p in raw['apis']),
                  excluded)


def problems(layout: Layout, inventory: dict[str, Entry], counterparts: dict[str, dict[str, str]]) -> list[str]:
    """Where the layout and the inventory disagree.

    Every documented ``couchbase`` module is placed exactly once.  A module the other APIs only re-export
    goes in the common reference, and one that any API writes its own version of goes in the API
    sections, where each API renders its own module.  A long page is split into groups that between
    them hold each of its public names once.
    """
    found = []
    documented = {m for m, e in inventory.items() if e.docs and m.split('.')[0] == SYNC}
    placed: dict[str, str] = {}
    for where, pages in ((COMMON, layout.common), ('apis', layout.apis)):
        slugs: set[str] = set()
        for page in layout.pages(pages):
            if page.slug in slugs:
                found.append(f'{where}: slug {page.slug!r} is used twice')
            slugs.add(page.slug)
            for module in page.modules:
                found += _placement_problems(module, where, placed, documented, counterparts)
            found += _group_problems(page, inventory, where == COMMON)
    found += [f'{m}: docs: true, and the layout does not place it' for m in sorted(documented - set(placed))]
    for target, names in sorted(layout.exclude_members.items()):
        module, _, name = target.rpartition('.')
        if module not in documented or name not in inventory[module].public:
            found.append(f'exclude_members: {target} is not a public name of a documented couchbase module')
        if list(names) != sorted(set(names)):
            found.append(f'exclude_members: {target} is not sorted, or names a member twice')
    return found


def _placement_problems(module: str, where: str, placed: dict[str, str], documented: set[str],
                        counterparts: dict[str, dict[str, str]]) -> list[str]:
    """Place ``module`` in ``where``, recording it in ``placed``, and say what is wrong with that."""
    if module in placed:
        return [f'{module}: placed in {placed[module]} and again in {where}']
    placed[module] = where
    if module not in documented:
        return [f'{module}: placed in {where}, and not a couchbase module with docs: true']
    own = [api for api, kind in counterparts.get(module, {}).items() if kind == OWN]
    if where == COMMON and own:
        return [f'{module}: in common, but {own} write their own; place it under apis']
    if where != COMMON and not own:
        return [f'{module}: under apis, but no other API writes its own; place it in common']
    return []


def _group_problems(page: Page, inventory: dict[str, Entry], common: bool) -> list[str]:
    if not page.groups:
        return []
    where = f'page {page.title!r}'
    if not common or len(page.modules) != 1:
        return [f'{where}: groups belong on a common page of one module, where every API reads the same names']
    entry = inventory.get(page.modules[0])
    if entry is None:
        return []
    grouped = [n for g in page.groups for n in g.names]
    found = []
    twice = sorted({n for n in grouped if grouped.count(n) > 1})
    if twice:
        found.append(f'{where}: {twice} in more than one group')
    unknown = sorted(set(grouped) - set(entry.public))
    if unknown:
        found.append(f'{where}: groups name {unknown}, which {page.modules[0]} does not publish')
    missing = sorted(set(entry.public) - set(grouped))
    if missing:
        found.append(f'{where}: {missing} in no group')
    return found
