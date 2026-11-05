# docgen: generating the API reference pages

The API reference under `docs/reference/` is generated, and the generated pages are committed:

```
tools/public_api.yaml           which modules are documented (`docs: true`), what each API has at a path,
                                and what replaces each compat name
tools/docgen/config/layout.yaml each page's title and order, the groups a long page splits into, members left out
docs/reference/                 generated, tracked, do not edit
docs/*.rst                      the guide and the front page, hand-written
```

A page names its objects and leaves their text to autodoc, so it changes only when the inventory or
the layout does. Editing a docstring never needs a regeneration.

## Usage

Run from the repository root, in an environment where `couchbase`, `acouchbase` and `txcouchbase`
import. `tools/docgen/tools_requirements.txt` installs everything the tool needs, Twisted, OpenTelemetry
and Sphinx included. A module that fails to import is an error, not a stale page. `config`, `generate`
and `check` do not need the native binding; `build` does, because autodoc imports the functions it
serves.

```
# validate the layout against the inventory and list the pages it generates
python -m tools.docgen config
# write the pages, removing any the layout no longer generates
python -m tools.docgen generate --write
# fail if the pages on disk are not what the layout generates
python -m tools.docgen check
```

`check` runs as the `docgen` pre-commit hook. To build the HTML, failing on any warning:

```
# runs check first; --out keeps the HTML, which otherwise goes to a temporary directory
python -m tools.docgen build --out docs/_build
```

The build is nitpicky: every cross-reference has to resolve, in docstrings and in signatures. It reads
the Python, Twisted, typing_extensions and OpenTelemetry inventories over the network.
`docs/type_checking_names.py` makes signatures resolvable: it gives autodoc the names each module
imports only under `TYPE_CHECKING`, and links the few names that still have no target. What it cannot
link, a private name or an unpublished type alias, is listed in `nitpick_ignore` in `docs/conf.py`.

## How a module finds its page

The inventory's `counterparts` says, for each `couchbase` module, whether `acouchbase` and
`txcouchbase` write their own version (`own`), re-export it (`shim`), or have none. The layout places
every documented `couchbase` module once, in one of two lists, and is checked against that:

- **`common`**: no API writes its own version, so every API publishes the same objects. Options,
  exceptions and the search queries are here. Each is described once.
- **`apis`**: some API writes its own version, as with `Cluster`. Each API section renders its own
  module at its own path, links to the synchronous page for a module it re-exports, and leaves out
  one it does not have.

An object is described once, on the first page that publishes it, taking the synchronous API first,
then the common reference, then asyncio and Twisted. Every other page that publishes it lists it as
a link. Two names are the same object when they lead to one definition, following re-exports, as
`tools/check_public_api.py` reads them.

## Adding a documented module

Flag it `docs: true` in `tools/public_api.yaml`, place it in `layout.yaml`, then run
`generate --write`. `check` fails until all three agree, and names what is missing.

A new public name in a module with `groups` has to go in one of them; `check` names it.

## The deprecated import paths

`docs/reference/deprecated_paths.inc` is the table of every compat name in the inventory and what to
import instead, included by `docs/migration.rst`. It is an include rather than a page, so it has no
title and no place in a toctree.

What replaces each compat name is in its inventory entry, the same one the SDK's import warning is
rendered from, so the table and the warning cannot disagree.

The compat names whose replacement is documented are warned about on the replacement's own entry too.
`docs/reference/old_import_paths.json` maps each documented object to them, and
`docs/old_import_paths.py` puts a warning naming them after the object's summary, so a reader who
imports it from an old path learns that on the page they are reading. Nothing in a docstring says
where to import from.
