"""The one place this tool renders output.

rich is used for what is tabular and only ever read on a terminal.  It is deliberately
not used for the report's per-test listings: those lines are `<api>:<path>:<lineno>`,
and they are grepped, clicked in an editor and pasted into a review, all of which a
table cell that truncates a path mid-way takes away.

`console` drops colour on its own when the stream is not a terminal, so redirected
output stays plain without a flag.
"""

from __future__ import annotations

import sys
import textwrap
from typing import Any, Sequence

from rich import box
from rich.console import Console, RenderableType
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# Redirected, rich would fall back to 80 columns and start folding paths that fit
# perfectly well in a file.  120 is the repository's own line limit, and pinning it also
# makes redirected output the same on every machine.
console = Console(highlight=False, width=None if sys.stdout.isatty() else 120)


def line(text: str = '') -> None:
    """One line, exactly as given.

    `soft_wrap` is what keeps a long path on one line rather than folded across two,
    which would break the grep the listings exist for.
    """
    console.print(text, markup=False, highlight=False, soft_wrap=True)


def show(renderable: RenderableType) -> None:
    """Print a renderable, or say so when a table has no rows.

    An empty rich table still draws its header and underline, which reads as two lines of
    output about nothing.
    """
    if getattr(renderable, 'row_count', None) == 0:
        line('  none')
        return
    console.print(renderable)


def wrapped(text: str) -> None:
    """One item that is prose, folded at the console width under a hanging indent.

    The listings are printed with `line` instead, unwrapped: they are
    `<api>:<path>:<lineno>` and grep has to find the whole thing on one line.  A note is a
    sentence, and one that runs to 400 columns is worse unwrapped than folded.  The
    hanging indent is what keeps the location at the start of the first line visible once
    the sentence behind it takes three.
    """
    for chunk in textwrap.wrap(text, width=console.width, initial_indent='  ',
                               subsequent_indent='      ', break_long_words=False,
                               break_on_hyphens=False):
        line(chunk)


def rule(title: str, style: str = 'bold') -> None:
    console.print()
    console.rule(Text(title, style=style), align='left', characters='=')


def plain_table(*columns: tuple[str, dict[str, Any]]) -> Table:
    """A table with a header underline and no frame.

    Anything heavier draws a box around output that is as often piped into a file as it
    is read on screen.
    """
    table = Table(box=box.SIMPLE_HEAD, pad_edge=False, show_edge=False, header_style='dim')
    for header, opts in columns:
        # `fold` rather than the default `ellipsis`: a truncated path is not a path, and
        # these columns hold the paths and call names the reader is meant to act on.
        table.add_column(header, **{'overflow': 'fold', **opts})
    return table


def panel(body: str, title: str, style: str) -> None:
    console.print(Panel(body, title=title, title_align='left', border_style=style,
                        padding=(0, 1)))


def counted(title: str, items: Sequence[str]) -> str:
    return f'{title}  ({len(set(items))} unique / {len(items)} total)'
