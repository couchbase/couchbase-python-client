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

"""Render the API reference pages the layout places.

Each page names its objects and leaves their text to autodoc, so a page changes only when the inventory
or the layout does, never when a docstring does.

An object is described once, on the first page that publishes it, taking the synchronous API's pages
first, then the common reference, then the asyncio and Twisted APIs.  So ``acouchbase.result``
describes its own results and links to ``couchbase.result`` for the ones it shares.  Two names are the
same object when they lead to one definition, following re-exports, so this needs no native binding.

It also renders the table of deprecated import paths that the migration guide includes: each compat name,
and what to import instead, as the inventory says.  And, for the docs build, the map from each documented
object to the compat names it replaces, from which ``docs/old_import_paths.py`` warns on the object's own
entry.

A documented name cannot open its docstring with ``**INTERNAL**``.  The docs build leaves such members
out, so on a public name the marker contradicts the inventory, which says it is public.
"""

from __future__ import annotations

import collections
import dataclasses
import enum
import importlib
import inspect
import json
import pathlib
import typing

from jinja2 import (Environment,
                    FileSystemLoader,
                    StrictUndefined)

from tools.check_public_api import (APIS,
                                    BINDING,
                                    INVENTORY,
                                    OWN,
                                    PACKAGES,
                                    SHIM,
                                    Entry,
                                    Tree,
                                    twin)
from tools.docgen.layout import (COMMON,
                                 SYNC,
                                 Layout,
                                 Page)

TEMPLATES = pathlib.Path(__file__).parent / 'templates'
# Heading underlines, by depth below the page title.
RULES = '=-~^'
# Included by the migration guide, so it is not a page of its own.
DEPRECATED = 'deprecated_paths.inc'
OLD_PATHS = 'old_import_paths.json'
INTERNAL = '**INTERNAL**'


@dataclasses.dataclass(frozen=True)
class Item:
    name: str
    directive: str
    options: tuple[str, ...] = ()


@dataclasses.dataclass(frozen=True)
class Block:
    """The items of one module on a page, under group titles when the layout splits it."""
    module: str
    groups: tuple[tuple[str, tuple[Item, ...]], ...]
    also: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class Link:
    title: str
    docname: str


@dataclasses.dataclass(frozen=True)
class Row:
    path: str
    instead: str


class Reference:
    """Every file of the reference, as a path below the docs directory and its text."""

    def __init__(self, root: pathlib.Path, layout: Layout, inventory: dict[str, Entry],
                 counterparts: dict[str, dict[str, str]]) -> None:
        self.layout = layout
        self.inventory = inventory
        self.counterparts = counterparts
        self.tree = Tree(root)
        self.env = Environment(loader=FileSystemLoader(str(TEMPLATES)), undefined=StrictUndefined,
                               keep_trailing_newline=True, trim_blocks=True, lstrip_blocks=True)
        self.canonical: dict[tuple[str, str], str] = {}
        self._described: set[int] | None = None
        for section, pages in ((SYNC, layout.apis), (COMMON, layout.common),
                               *((api, layout.apis) for api in APIS)):
            for page in layout.pages(pages):
                for module in self._own(section, page):
                    for name in inventory[module].public:
                        self.canonical.setdefault(self.key(module, name), f'{module}.{name}')

    def docname(self, section: str, page: Page) -> str:
        slug = f'{page.slug}/index' if page.pages else page.slug
        return f'{self.layout.output}/{section}/{slug}'

    def section_docname(self, section: str) -> str:
        return f'{self.layout.output}/{section}/index'

    def key(self, module: str, name: str) -> tuple[str, str]:
        """What a name leads to, so that two names for one object have one key.

        A name the SDK does not define is served from the native binding.
        """
        return self.tree.defined_in(module, name) or BINDING, name

    def _own(self, section: str, page: Page) -> list[str]:
        """The modules ``section`` renders on ``page``, at its own paths."""
        if section in (SYNC, COMMON):
            return list(page.modules)
        return [twin(section, m) for m in page.modules if self.counterparts[m][section] == OWN]

    def _shared(self, section: str, page: Page) -> bool:
        """Whether ``section`` re-exports a module ``page`` renders for the synchronous API."""
        return section in APIS and any(self.counterparts[m][section] == SHIM for m in page.modules)

    def render(self) -> dict[str, str]:
        files = {f'{docname}.rst': text for docname, text in self._pages().items()}
        files[f'{self.layout.output}/{DEPRECATED}'] = self.env.get_template('deprecated.rst.jinja2').render(
            rows=self._deprecated())
        files[f'{self.layout.output}/{OLD_PATHS}'] = json.dumps(self._old_paths(), indent=2, sort_keys=True) + '\n'
        return files

    def internal_problems(self) -> list[str]:
        """The documented names whose docstring says they are internal."""
        return [f'{target}: documented, and its docstring opens with {INTERNAL}; drop the marker, '
                f'or take it out of public in {INVENTORY.name}'
                for target in sorted(set(self.canonical.values())) if _is_internal(_resolve(target))]

    def _compat(self) -> list[str]:
        return sorted(f'{module}.{name}' for module, entry in self.inventory.items() for name in entry.compat)

    def _found(self, module: str, name: str) -> str | None:
        """Where the documented object that replaces a compat name is described, if it is."""
        use = self.inventory[module].compat[name].use
        if use is None:
            return None
        return self.canonical.get(self.key(*use.rsplit('.', 1)))

    def _old_paths(self) -> dict[str, list[str]]:
        """Each documented object that replaces a compat name, and those compat names."""
        found: dict[str, list[str]] = collections.defaultdict(list)
        for path in self._compat():
            module, _, name = path.rpartition('.')
            target = self._found(module, name)
            if target:
                found[target].append(path)
        return dict(found)

    def _deprecated(self) -> list[Row]:
        rows = []
        for path in self._compat():
            module, _, name = path.rpartition('.')
            compat = self.inventory[module].compat[name]
            found = self._found(module, name)
            if found:
                instead = f':py:obj:`{found}`'
            elif compat.use:
                instead = f'``{compat.use}``'
            else:
                instead = f'Nothing: {compat.reason}'
            rows.append(Row(path, instead))
        return rows

    def _pages(self) -> dict[str, str]:
        out: dict[str, str] = {}
        for page in self.layout.common:
            self._render_page(COMMON, page, out)
        children = [self.docname(COMMON, p) for p in self.layout.common]
        out[self.section_docname(COMMON)] = self._index(self.layout.sections[COMMON].title,
                                                        self.layout.sections[COMMON].intro, children, [])
        for section in (SYNC, *APIS):
            children, shared = self._render_pages(section, self.layout.apis, out)
            out[self.section_docname(section)] = self._index(self.layout.sections[section].title,
                                                             self.layout.sections[section].intro, children, shared)
        return out

    def _render_pages(self, section: str, pages: tuple[Page, ...],
                      out: dict[str, str]) -> tuple[list[str], list[Link]]:
        """Render ``pages`` for ``section``: the docnames rendered, and the synchronous pages it shares."""
        children: list[str] = []
        shared: list[Link] = []
        for page in pages:
            rendered, links = self._render_page(section, page, out)
            if rendered:
                children.append(self.docname(section, page))
            shared += links
        return children, shared

    def _render_page(self, section: str, page: Page, out: dict[str, str]) -> tuple[bool, list[Link]]:
        """Render ``page`` for ``section`` into ``out``.

        Returns whether it rendered, and the synchronous pages of ``page`` that ``section`` shares and
        the caller has to link, which an index that renders links itself.
        """
        if page.pages:
            children, shared = self._render_pages(section, page.pages, out)
            if not children:
                return False, shared
            out[self.docname(section, page)] = self._index(page.title, '', children, shared)
            return True, []
        shared = [Link(page.title, self.docname(SYNC, page))] if self._shared(section, page) else []
        modules = self._own(section, page)
        if not modules:
            return False, shared
        blocks = [self._block(module, page) for module in modules]
        out[self.docname(section, page)] = self.env.get_template('page.rst.jinja2').render(
            title=page.title, blocks=blocks, rules=RULES)
        return True, shared

    def _index(self, title: str, intro: str, children: list[str], shared: list[Link]) -> str:
        return self.env.get_template('index.rst.jinja2').render(
            title=title, intro=intro, children=children, shared=shared,
            sync_title=self.layout.sections[SYNC].title, rules=RULES)

    def _block(self, module: str, page: Page) -> Block:
        public = self.inventory[module].public
        groups = page.groups or ()
        ordered = [(g.title, g.names) for g in groups] if groups else [('', public)]
        also: list[str] = []
        rendered = []
        for title, names in ordered:
            items = []
            for name in names:
                target = self.canonical[self.key(module, name)]
                if target != f'{module}.{name}':
                    also.append(target)
                    continue
                items.append(self._item(module, name))
            rendered.append((title, tuple(items)))
        return Block(module, tuple(rendered), tuple(also))

    def _item(self, module: str, name: str) -> Item:
        excluded = self.layout.exclude_members.get(f'{module}.{name}', ())
        exclude = (f':exclude-members: {", ".join(excluded)}',) if excluded else ()
        obj = _resolve(f'{module}.{name}')
        if obj is None:
            # Served by the native binding, which only ever serves functions.
            return Item(name, 'autofunction')
        if typing.get_origin(obj) is not None:
            # A type alias such as ``Mapping[str, X]``.  On 3.10 a subscripted ``collections.abc``
            # class passes ``isclass``, and on later versions it is still callable.
            return Item(name, 'autodata')
        if inspect.isclass(obj):
            if issubclass(obj, enum.Enum):
                return Item(name, 'autoclass', (':members:', ':undoc-members:', *exclude))
            inherited = (':inherited-members:',) if self._hidden_base(obj) else ()
            if issubclass(obj, BaseException):
                return Item(name, 'autoexception', (':members:', *inherited, *exclude))
            return Item(name, 'autoclass', (':members:', *inherited, *exclude))
        if callable(obj):
            return Item(name, 'autofunction')
        return Item(name, 'autodata')

    def _hidden_base(self, cls: type) -> bool:
        """Whether ``cls`` inherits public members from an SDK base no page describes.

        A private base, such as the one the synchronous and asyncio query results share, has no page,
        so its members are documented on each class that inherits them.
        """
        if self._described is None:
            self._described = {id(obj) for obj in map(_resolve, self.canonical.values()) if inspect.isclass(obj)}
        return any(base.__module__.split('.')[0] in PACKAGES
                   and id(base) not in self._described
                   and any(not k.startswith('_') for k in vars(base))
                   for base in cls.__mro__[1:])


def _is_internal(obj: object) -> bool:
    if not (inspect.isclass(obj) or inspect.isroutine(obj)):
        return False
    doc = obj.__doc__
    return isinstance(doc, str) and doc.lstrip().startswith(INTERNAL)


def _resolve(target: str) -> object:
    """The object at a dotted ``module.name``, or None where only the native binding serves it."""
    module, _, name = target.rpartition('.')
    try:
        return getattr(importlib.import_module(module), name)
    except AttributeError:
        return None
    except ImportError as exc:
        # Any other import failure, such as Twisted missing, would otherwise render every name in the
        # module as a binding function, and `check` would report the pages stale rather than the cause.
        if not (exc.name or '').startswith(BINDING):
            raise
        return None
