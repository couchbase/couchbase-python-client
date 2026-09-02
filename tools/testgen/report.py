"""Format what a generation pass found.

The buckets, not the counts, are the point.  A test that bridges some of its calls and
leaves one unresolved is the outcome worth acting on, and it is invisible in a total.
"""

from __future__ import annotations

import os
from typing import Iterable, Sequence

from rich.text import Text

from tools.testgen.console import (counted,
                                   line,
                                   panel,
                                   plain_table,
                                   rule,
                                   show,
                                   wrapped)
from tools.testgen.generate import RunResult

INCOMPLETE_MEANS = (
    'Those files look finished and are not.  acouchbase: the test is a coroutine, and an '
    'unresolved call inside it returns a coroutine that nothing awaits, so the operation '
    'never runs and the assertion that follows measures nothing.  txcouchbase: the '
    'unresolved call returns a Deferred that nothing waits on, so the operation IS '
    'submitted and the test races it.  That is worse to diagnose, because it passes '
    'intermittently.')

# Which buckets get their per-suite spread printed without --verbose.  CLEAN alone spreads
# across 48 api/suite pairs, and a reader who has to scroll past it to reach INCOMPLETE is
# being charged for the one bucket that needs no decision.
ACT_ON = ('INCOMPLETE', 'BLOCKED')


def print_report(result: RunResult, verbose: bool = False) -> None:
    _print_generated(result)
    _section('BLOCKERS: infrastructure the generated file needs and that does not exist',
             result.report.blockers)
    _print_unresolved(result, verbose)
    _print_resolver_state(result)
    _print_outcomes(result, verbose)


def _section(title: str, items: Sequence[str], prose: bool = False) -> None:
    rule(counted(title, items))
    if not items:
        line('  none')
        return
    for it in sorted(set(items)):
        wrapped(it) if prose else line(f'  {it}')


def _folded(title: str, items: Sequence[str], verbose: bool) -> None:
    """A listing that is real output but is almost never why anyone ran `report`.

    The count stays visible either way; it is the 200-odd names behind it that push the
    two buckets worth acting on off the top of the scrollback.
    """
    if verbose:
        _section(title, items)
        return
    rule(counted(title, items))
    line('  not listed; pass --verbose')


def _print_generated(result: RunResult) -> None:
    rule('GENERATED')
    table = plain_table(('suite', {}),
                        ('bridged call sites', {'justify': 'right'}))
    for k, v in sorted(result.bridged.items()):
        table.add_row(k, str(v))
    show(table)


def _print_unresolved(result: RunResult, verbose: bool) -> None:
    """Group unresolved sites by cause rather than by line.

    One missing manager class is one decision.  Printing it 172 times says nothing the
    first line did not.
    """
    grouped: dict[str, list[str]] = {}
    for line_ in result.report.unresolved:
        _, _, rest = line_.partition(': ')
        call, _, why = rest.partition(' -> ')
        grouped.setdefault(why or 'unresolved receiver', []).append(call.strip())
    total = sum(len(v) for v in grouped.values())
    rule(f'UNRESOLVED CALL SITES, BY CAUSE  ({len(grouped)} causes / {total} sites)')
    table = plain_table(('sites', {'justify': 'right', 'no_wrap': True}),
                        ('cause', {}),
                        ('distinct calls', {'ratio': 1}))
    for why, calls in sorted(grouped.items(), key=lambda kv: -len(kv[1])):
        distinct = sorted({_strip(c, why) for c in calls})
        shown = distinct if verbose else distinct[:6]
        rest = '' if len(shown) == len(distinct) else f' (+{len(distinct) - len(shown)} more)'
        table.add_row(str(len(calls)), why, f'{len(distinct)}: ' + ', '.join(shown) + rest)
    show(table)


def _strip(call: str, why: str) -> str:
    """Drop the cause from a call that repeats it: the column heading already says it."""
    return call[len(why):].strip() if call.startswith(why) else call


def _print_resolver_state(result: RunResult) -> None:
    missing = {(api, m): a for api, r in result.resolvers.items()
               for m, a in r.missing_managers.items()}
    rule(f'MISSING API-SPECIFIC MANAGER CLASSES  ({len(missing)})')
    table = plain_table(('class', {'no_wrap': True}),
                        ('methods', {'justify': 'right'}),
                        ('called', {'ratio': 1}))
    for (api, mgr), attrs in sorted(missing.items()):
        table.add_row(f'{api}/{mgr}', str(len(attrs)), ', '.join(sorted(attrs)))
    show(table)

    inherited = sorted({f'{api}: {m}' for api, r in result.resolvers.items()
                        for m in r.inherited_sync})
    _section('INHERITED-SYNC ENV METHODS REACHED FROM GENERATED SUITES', inherited)


def _buckets(result: RunResult) -> list[tuple[str, str, list[str], str]]:
    rep = result.report
    return [
        ('CLEAN', 'bridges at least one call, nothing unresolved', rep.tests_clean, 'green'),
        ('INCOMPLETE', 'bridges calls, but still holds an unresolved one',
         rep.tests_incomplete, 'bold red'),
        ('BLOCKED', 'bridges nothing, every candidate call is unresolved',
         rep.sync_tests_blocked, 'red'),
        ('GENUINELY SYNC', 'nothing to bridge at all', rep.sync_tests_genuine, 'cyan'),
        ('EXCLUDED', 'testgen_exclude: not generated for this api', rep.tests_excluded, 'dim'),
    ]


def _spread(items: Iterable[str]) -> str:
    """Count a bucket's members per api/suite pair.

    Report lines are `<api>:<source path>:<lineno>: ...`, and both leading fields are
    needed: one suite generates for two APIs and the two outcomes are frequently
    different.
    """
    by_file: dict[str, int] = {}
    for item in items:
        api, _, rest = item.partition(':')
        f = f'{api}/{os.path.basename(rest.split(":")[0])}'
        by_file[f] = by_file.get(f, 0) + 1
    return ', '.join(f'{f} {n}' for f, n in sorted(by_file.items()))


def _print_outcomes(result: RunResult, verbose: bool) -> None:
    buckets = _buckets(result)
    total = sum(len(v) for _, _, v, _ in buckets)
    pairs = {item.split(':', 2)[0] + ':' + os.path.basename(item.split(':')[1])
             for _, _, items, _ in buckets for item in items}
    rule(f'PER-TEST OUTCOME  ({total} tests across {len(pairs)} api/suite pairs)')
    table = plain_table(('tests', {'justify': 'right', 'no_wrap': True}),
                        ('bucket', {'no_wrap': True}),
                        ('meaning', {'ratio': 1}))
    for name, meaning, items, style in buckets:
        cell = Text(meaning)
        spread = _spread(items)
        if spread and (verbose or name in ACT_ON):
            cell.append(f'\n{spread}', style='dim')
        table.add_row(Text(str(len(items)), style=style), Text(name, style=style), cell)
    show(table)
    panel(INCOMPLETE_MEANS, 'INCOMPLETE is the bucket that matters', 'red')

    # Name the members of the two buckets a reader is told to act on.  Printing counts
    # alone for INCOMPLETE and BLOCKED, while naming every GENUINELY SYNC and EXCLUDED
    # test, has it backwards.
    rep = result.report
    _section('TESTS INCOMPLETE: bridged, but still holding an unresolved call',
             rep.tests_incomplete)
    _section('TESTS BLOCKED: nothing bridged, every candidate call unresolved',
             rep.sync_tests_blocked)
    _section('LINES OVER 120 COLS', rep.long_lines)
    _section('RESOLVER MISSES',
             [f'{api}: {m}' for api, r in result.resolvers.items() for m in r.misses])
    _section('NOTES', rep.notes, prose=True)
    _folded('TESTS WITH NOTHING TO BRIDGE', rep.sync_tests_genuine, verbose)
    _folded('TESTS EXCLUDED BY testgen_exclude', rep.tests_excluded, verbose)
    _folded('CLASSES EXCLUDED BY testgen_exclude', rep.classes_excluded, verbose)
