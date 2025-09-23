from __future__ import annotations

import argparse
import io
import json
import os
import queue
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import uuid
import zipfile
from typing import NamedTuple

SRC = os.path.abspath('../sentry')
DATA = os.path.abspath('data')
CACHE = os.path.abspath('cache')
VENDOR = os.path.abspath('vendor')
FIRST_COMMIT = 'b1767a6a76ee31f8c63a37ae1dfb9eca82172edf'
LAST_COMMIT = 'b6083b163df0984becf8596976f60c9a30d41532'


def _determine_commits() -> list[str]:
    out = subprocess.check_output((
        'git', '-C', SRC,
        'log', '--format=%H', '--reverse',
        f'{FIRST_COMMIT}..{LAST_COMMIT}', '--', '*.py',
    ))
    commit_ids = [line.decode() for line in out.splitlines()]
    # ok git :)
    commit_ids.insert(0, FIRST_COMMIT)

    completed = set()
    for maybe_done in os.listdir(DATA):
        if (
                len(maybe_done) == 40 and
                all(
                    os.path.exists(os.path.join(DATA, maybe_done, fn))
                    for fn in ('info.json', 'mypy-out')
                )
        ):
            completed.add(maybe_done)
        else:
            shutil.rmtree(os.path.join(DATA, maybe_done))

    todo = [cid for cid in commit_ids if cid not in completed]
    print(f'skipping {len(commit_ids) - len(todo)} already done!')
    return todo


PROG = '''\
pip install \
    --cache-dir /cache/pip \
    --disable-pip-version-check \
    --quiet \
    --root-user-action=ignore \
    uv==0.8.19

uv venv /.venv --quiet --no-managed-python -p $(which python)
export PATH=/.venv/bin:$PATH VIRTUAL_ENV=/.venv

cd /src
if [ -f requirements-dev-frozen.txt ]; then
    uv pip install -r requirements-dev-frozen.txt --cache-dir /cache/uv --quiet
else
    uv sync --active --frozen --quiet --cache-dir /cache/uv
fi

python /vendor/fast_editable.py >& /dev/null

sentry init

pip freeze | grep '^mypy==' > /data/mypy-version

! python -m tools.mypy_helpers.mypy_without_ignores > /data/mypy-out
'''


def _threaded_worker(q: queue.Queue[str]) -> None:
    while True:
        try:
            cid = q.get(timeout=.5)
        except queue.Empty:
            return

        ver_cmd = ('git', '-C', SRC, 'show', f'{cid}:.python-version')
        version = subprocess.check_output(ver_cmd).decode().strip()
        ver, _ = version.rsplit('.', 1)

        with tempfile.TemporaryDirectory() as tmpdir:
            src = os.path.join(tmpdir, 'src')
            data = os.path.join(tmpdir, 'data')
            os.makedirs(data)

            subprocess.check_call((
                'git', 'clone', '--quiet', '--shared', '--no-checkout',
                SRC, src,
            ))
            subprocess.check_call(('git', '-C', src, 'checkout', '-q', cid))

            info_out = subprocess.check_output((
                'git', '-C', src,
                'show', '--no-patch', '--format=%an <%ae>\t%ct', 'HEAD',
            )).strip().decode()
            author, ct_s = info_out.split('\t')

            subprocess.check_call((
                'podman', 'run', '--rm',
                '-v', f'{VENDOR}:/vendor:ro',
                '-v', f'{CACHE}:/cache:rw',
                '-v', f'{data}:/data:rw',
                '-v', f'{src}:/src:rw',
                f'python:{ver}-slim',
                'bash', '-euc', PROG,
            ))

            with open(os.path.join(data, 'mypy-version')) as f:
                mypy_version = f.read().strip()

            with tempfile.TemporaryDirectory(dir=DATA, delete=False) as tdir:
                info = {
                    'python': ver,
                    'mypy': mypy_version,
                    'author': author,
                    'commit_time': int(ct_s),
                }
                info_json = os.path.join(tdir, 'info.json')
                with open(info_json, 'w') as f:
                    json.dump(info, f)

                shutil.copy(os.path.join(data, 'mypy-out'), tdir)

                os.rename(tdir, os.path.join(DATA, cid))


class SSH(NamedTuple):
    host: str
    jobs: int

    @classmethod
    def parse(cls, s: str) -> SSH:
        host, jobs_s = s.rsplit(',', 1)
        return cls(host, int(jobs_s))


def _ssh_worker(q: queue.Queue[str], ssh: SSH) -> None:
    while True:
        items = []
        for _ in range(ssh.jobs):
            try:
                items.append(q.get(block=False))
            except queue.Empty:
                break
        if not items:
            return

        rm_cmd = ('ssh', ssh.host, 'rm -rf ~/workspace/sentry-mypy-stats/data')
        subprocess.check_call(rm_cmd)

        pyver = f'python{sys.version_info.major}.{sys.version_info.minor}'
        subprocess.check_call((
            'ssh', ssh.host,
            f'cd ~/workspace/sentry-mypy-stats && '
            f'{pyver} -m main --jobs {ssh.jobs} {" ".join(items)}',
        ))

        with tempfile.TemporaryDirectory(dir=DATA) as tmpdir:
            subprocess.check_call((
                'scp', '-r', '-q',
                f'{ssh.host}:workspace/sentry-mypy-stats/data', tmpdir,
            ))
            data = os.path.join(tmpdir, 'data')
            for name in os.listdir(data):
                os.rename(
                    os.path.join(data, name),
                    os.path.join(DATA, name),
                )


def _gha_worker(q: queue.Queue[str]) -> None:
    with open(os.path.expanduser('~/.github-auth.json')) as f:
        token = json.load(f)['token']

    while True:
        items = []
        for _ in range(16):
            try:
                items.append(q.get(block=False))
            except queue.Empty:
                break
        if not items:
            return

        aid = str(uuid.uuid4())
        data = {
            'ref': 'main',
            'inputs': {'artifact': aid, 'shas': ' '.join(items)},
        }
        headers = {'Authorization': f'Bearer {token}'}

        req = urllib.request.Request(
            'https://api.github.com/repos/asottile/sentry-mypy-stats/actions/workflows/run.yml/dispatches',  # noqa: E501
            method='POST',
            data=json.dumps(data).encode(),
            headers=headers,
        )
        urllib.request.urlopen(req).close()

        time.sleep(120)

        while True:
            req = urllib.request.Request(
                f'https://api.github.com/repos/asottile/sentry-mypy-stats/actions/artifacts?name={aid}',  # noqa: E501
                headers=headers,
            )
            artifacts_resp = json.load(urllib.request.urlopen(req))
            if artifacts_resp['artifacts']:
                break
            else:
                time.sleep(2)

        artifact, = artifacts_resp['artifacts']
        req = urllib.request.Request(artifact['archive_download_url'])
        for k, v in headers.items():
            req.add_unredirected_header(k, v)
        contents = urllib.request.urlopen(req).read()

        with tempfile.TemporaryDirectory(dir=DATA) as tmpdir:
            zipf = zipfile.ZipFile(io.BytesIO(contents))
            zipf.extractall(tmpdir)

            for name in os.listdir(tmpdir):
                os.rename(
                    os.path.join(tmpdir, name),
                    os.path.join(DATA, name),
                )

        req = urllib.request.Request(
            artifact['url'],
            method='DELETE',
            headers=headers,
        )
        urllib.request.urlopen(req).close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobs', type=int, default=os.cpu_count() or 8)
    parser.add_argument('--ssh', type=SSH.parse, action='append', default=[])
    parser.add_argument('--gha-jobs', type=int, default=0)
    parser.add_argument('cid', nargs='*', default=[])
    args = parser.parse_args()

    os.makedirs(DATA, exist_ok=True)
    os.makedirs(CACHE, exist_ok=True)

    os.makedirs(VENDOR, exist_ok=True)
    with open(os.path.join(VENDOR, 'fast_editable.py'), 'w') as f:
        cmd = ('git', '-C', SRC, 'show', 'cef27b49ea4:tools/fast_editable.py')
        subprocess.check_call(cmd, stdout=f)

    if args.cid:
        todo = args.cid
    else:
        todo = _determine_commits()

    q: queue.Queue[str] = queue.Queue()
    for cid in todo:
        q.put(cid)

    def _clear_queue(*a: object) -> None:
        print('USR1: clearing queue and exiting...')
        while True:
            try:
                q.get(timeout=.1)
            except queue.Empty:
                break
        raise SystemExit(1)

    signal.signal(signal.SIGUSR1, _clear_queue)

    threads = []

    for _ in range(args.jobs):
        t = threading.Thread(target=_threaded_worker, args=(q,))
        threads.append(t)
        t.start()

    for info in args.ssh:
        t = threading.Thread(target=_ssh_worker, args=(q, info))
        threads.append(t)
        t.start()

    for _ in range(args.gha_jobs):
        t = threading.Thread(target=_gha_worker, args=(q,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
