from __future__ import annotations

import collections
import concurrent.futures
import json
import os.path
import re
import sqlite3
import subprocess
from typing import NamedTuple

FIRST_COMMIT = 'b1767a6a76ee31f8c63a37ae1dfb9eca82172edf'
LAST_COMMIT = 'b6083b163df0984becf8596976f60c9a30d41532'
SRC = os.path.abspath('../sentry')

SKIPPED = frozenset((
    '134912850861e3e105992027aa18288ccc260ffe',
    '433839ba0381b148e658609f51ab8b4ff32cf638',
    '622827e76320cbfe541b320f109bd04209a8d00d',
    'af0b3e8e036899f781797b4db35e80bb9d6e36dd',
))

ERROR_RE = re.compile(r'^([^:]+):[0-9]+: error:.*  \[([^]]+)\]$')


class Info(NamedTuple):
    python: str
    mypy: str
    author: str
    commit_time: int


def _info(cid: str) -> Info:
    with open(os.path.join('data', cid, 'info.json')) as f:
        return Info(**json.load(f))


def _errors(cid: str) -> tuple[
    str,
    collections.Counter[str],
    collections.Counter[str],
]:
    by_file: collections.Counter[str] = collections.Counter()
    by_code: collections.Counter[str] = collections.Counter()
    with open(os.path.join('data', cid, 'mypy-out')) as f:
        for line in f:
            match = ERROR_RE.match(line)
            if match is not None:
                by_file[match[1]] += 1
                by_code[match[2]] += 1

    return cid, by_file, by_code


def main() -> int:
    out = subprocess.check_output((
        'git', '-C', SRC,
        'log', '--format=%H', '--reverse',
        f'{FIRST_COMMIT}..{LAST_COMMIT}', '--',
        '*.py', '*.pyi', 'requirements*.txt', 'pyproject.toml',
        '.python-version',
    ))
    commit_ids = [line.decode() for line in out.splitlines()]
    commit_ids.insert(0, FIRST_COMMIT)
    commit_ids = [cid for cid in commit_ids if cid not in SKIPPED]

    try:
        os.remove('db.db')
    except OSError:
        pass

    with sqlite3.connect('db.db') as db:
        db.execute(
            'CREATE TABLE commits (hash, python, mypy, author, commit_time);',
        )
        db.execute('CREATE TABLE by_file (hash, file, count)')
        db.execute('CREATE TABLE by_code (hash, code, count)')

        for cid in commit_ids:
            db.execute(
                'INSERT INTO commits VALUES (?, ?, ?, ?, ?)',
                (cid, *_info(cid)),
            )

        with concurrent.futures.ProcessPoolExecutor(8) as exe:
            for cid, by_file, by_code in exe.map(_errors, commit_ids):
                db.executemany(
                    'INSERT INTO by_file VALUES (?, ?, ?)',
                    [(cid, fname, count) for fname, count in by_file.items()],
                )
                db.executemany(
                    'INSERT INTO by_code VALUES (?, ?, ?)',
                    [(cid, code, count) for code, count in by_code.items()],
                )

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
