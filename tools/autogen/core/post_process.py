"""Post-processing for generated binding files.

The generator renders unformatted output, so the committed files are whatever the repo's
pre-commit fixers turn that output into.  Formatting therefore has to run before we can
answer "did this file actually change?" - which is what lets us drop the files whose only
delta is the per-run header metadata.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path
from typing import (List,
                    Optional,
                    Tuple)

# Generated-On changes every run and Content-Hash is derived from the body, so neither line
# says anything about whether the generated output actually changed.
RUN_METADATA_RE = re.compile(r'^[ \t]*(?://|#)[ \t]*(?:Generated-On|Content-Hash):.*$\n?', re.MULTILINE)


def strip_run_metadata(content: str) -> str:
    return RUN_METADATA_RE.sub('', content)


def run_pre_commit(paths: List[Path], repo_root: Path) -> Tuple[bool, str]:
    """Run the repo's pre-commit fixers over ``paths`` only.

    Returns (ran, output).  Scoped with --files so unrelated files are never touched and
    unrelated lint failures never block generation.
    """
    if shutil.which('pre-commit') is None:
        return False, 'pre-commit not found on PATH'

    rel_paths = [str(p.relative_to(repo_root)) for p in paths]
    # Fixer hooks report failure on the run that rewrites a file, so a second pass is what
    # tells us whether anything is genuinely wrong.
    output = ''
    for _ in range(2):
        completed = subprocess.run(['pre-commit', 'run', '--files', *rel_paths],
                                   cwd=repo_root,
                                   capture_output=True,
                                   text=True)
        output = completed.stdout + completed.stderr
        if completed.returncode == 0:
            break

    return True, output


def _git_head_content(path: Path, repo_root: Path) -> Optional[str]:
    rel_path = path.relative_to(repo_root)
    completed = subprocess.run(['git', 'show', f'HEAD:{rel_path}'],
                               cwd=repo_root,
                               capture_output=True,
                               text=True)
    if completed.returncode != 0:
        return None

    return completed.stdout


def restore_unchanged(paths: List[Path], repo_root: Path) -> Tuple[List[Path], List[Path]]:
    """Restore generated files whose only delta against HEAD is the run metadata.

    This is safe by construction: a file is only restored when its content is byte-identical
    to HEAD once the metadata lines are removed, so genuine changes - including hand-edits -
    can never be discarded.  Returns (restored, changed).
    """
    restored = []
    changed = []
    for path in paths:
        head_content = _git_head_content(path, repo_root)
        if head_content is None:
            # Untracked or not in HEAD, so there is no baseline to compare against.
            changed.append(path)
            continue

        with open(path, 'r') as f:
            current = f.read()

        if strip_run_metadata(current) == strip_run_metadata(head_content):
            restored.append(path)
        else:
            changed.append(path)

    if restored:
        rel_paths = [str(p.relative_to(repo_root)) for p in restored]
        subprocess.run(['git', 'checkout', 'HEAD', '--', *rel_paths],
                       cwd=repo_root,
                       capture_output=True,
                       text=True,
                       check=True)

    return restored, changed
