'''Compact comment-parent index (Parquet + DuckDB) for reconstructing comment threads.

Usage:
    python -m otacon.thread_index build  --input DIR --index DIR [--subreddits FILE] [--time_from 2010-01] [--time_to 2021-09] [--workers 4]
    python -m otacon.thread_index merge  --index DIR
    python -m otacon.thread_index lookup --index DIR ID [ID ...]
'''

import argparse
import os
import re
from multiprocessing import Pool
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from otacon.pushshift_handling import read_redditfile

BATCH_ROWS = 1_000_000
SCHEMA = pa.schema([
    ('id', pa.int64()),
    ('parent_id', pa.int64()),
    ('parent_is_submission', pa.bool_()),
    ('link_id', pa.int64()),
    ('created_utc', pa.uint32()),
    ('subreddit', pa.string()),
])
FILE_RE = re.compile(r'RC_(\d{4})-(\d{2})\.zst$')


def to_int(base36_id):
    return int(base36_id, 36) if base36_id else None


def to_base36(n: int) -> str:
    digits = '0123456789abcdefghijklmnopqrstuvwxyz'
    out = ''
    while n:
        n, r = divmod(n, 36)
        out = digits[r] + out
    return out or '0'


def strip_prefix(fullname):
    return fullname.split('_', 1)[-1] if fullname else None


def load_subreddits(path):
    with open(path, encoding='utf-8') as f:
        return {line.strip().lower() for line in f if line.strip()}


def index_month(args):
    src, dst, subreddit_file = args
    if os.path.exists(dst):
        return dst, 0
    wanted = load_subreddits(subreddit_file) if subreddit_file else None
    tmp = dst + '.tmp'
    cols = {name: [] for name in SCHEMA.names}
    rows = 0

    def flush(writer):
        writer.write_table(pa.table(cols, schema=SCHEMA))
        for v in cols.values():
            v.clear()

    with pq.ParquetWriter(tmp, SCHEMA, compression='zstd', compression_level=9) as writer:
        for c in read_redditfile(src):
            cid = c.get('id')
            if not cid:
                continue
            if wanted is not None and (c.get('subreddit') or '').lower() not in wanted:
                continue
            parent = c.get('parent_id') or ''
            cols['id'].append(to_int(cid))
            cols['parent_id'].append(to_int(strip_prefix(parent)))
            cols['parent_is_submission'].append(parent.startswith('t3_'))
            cols['link_id'].append(to_int(strip_prefix(c.get('link_id'))))
            cols['created_utc'].append(int(float(c.get('created_utc') or 0)))
            cols['subreddit'].append(c.get('subreddit'))
            rows += 1
            if len(cols['id']) >= BATCH_ROWS:
                flush(writer)
        if cols['id']:
            flush(writer)
    os.replace(tmp, dst)
    return dst, rows


def build(input_dir, index_dir, time_from, time_to, workers, subreddit_file=None):
    parts = Path(index_dir) / 'parts'
    parts.mkdir(parents=True, exist_ok=True)
    jobs = []
    for f in sorted(Path(input_dir).iterdir()):
        m = FILE_RE.match(f.name)
        if m and time_from <= f'{m[1]}-{m[2]}' <= time_to:
            jobs.append((str(f), str(parts / f'RC_{m[1]}-{m[2]}.parquet'), subreddit_file))
    with Pool(workers) as pool:
        for dst, rows in pool.imap_unordered(index_month, jobs):
            print(f'{Path(dst).name}: {rows:,} rows' if rows else f'{Path(dst).name}: skipped/done', flush=True)


def merge(index_dir):
    idx = Path(index_dir)
    con = duckdb.connect()
    con.execute(f"SET temp_directory='{idx / 'tmp'}'")
    for name, order in (('by_id', 'id'), ('by_parent', 'parent_is_submission, parent_id')):
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{idx}/parts/*.parquet') ORDER BY {order}) "
            f"TO '{idx}/{name}.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
        )
        print(f'wrote {name}.parquet', flush=True)


class ThreadIndex:
    def __init__(self, index_dir):
        idx = Path(index_dir)
        self.con = duckdb.connect()
        self.by_id = f"read_parquet('{idx}/by_id.parquet')"
        self.by_parent = f"read_parquet('{idx}/by_parent.parquet')"

    def _fetch(self, source, where_col, ids, extra=''):
        ph = ','.join('?' * len(ids))
        return self.con.execute(
            f'SELECT id, parent_id, parent_is_submission, link_id, created_utc, subreddit '
            f'FROM {source} WHERE {where_col} IN ({ph}) {extra}', list(ids)
        ).fetchall()

    def thread(self, comment_ids):
        """Return ({comment_id: row} for all ancestors and descendants, set of submission ids)."""
        found = {}
        submissions = set()

        frontier = {to_int(c) for c in comment_ids}
        while frontier:  # ancestors
            rows = self._fetch(self.by_id, 'id', frontier)
            frontier = set()
            for r in rows:
                if r[0] in found:
                    continue
                found[r[0]] = r
                if r[3] is not None:
                    submissions.add(r[3])
                if not r[2] and r[1] not in found:
                    frontier.add(r[1])

        frontier = {to_int(c) for c in comment_ids}
        while frontier:  # descendants
            rows = self._fetch(self.by_parent, 'parent_id', frontier, 'AND NOT parent_is_submission')
            frontier = set()
            for r in rows:
                if r[0] not in found:
                    found[r[0]] = r
                    frontier.add(r[0])

        return {to_base36(k): v for k, v in found.items()}, {to_base36(s) for s in submissions}


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest='cmd', required=True)
    b = sub.add_parser('build')
    b.add_argument('--input', required=True)
    b.add_argument('--index', required=True)
    b.add_argument('--subreddits', help='text file with one subreddit name per line')
    b.add_argument('--time_from', default='2010-01')
    b.add_argument('--time_to', default='2021-09')
    b.add_argument('--workers', type=int, default=4)
    m = sub.add_parser('merge')
    m.add_argument('--index', required=True)
    l = sub.add_parser('lookup')
    l.add_argument('--index', required=True)
    l.add_argument('ids', nargs='+')
    a = p.parse_args()

    if a.cmd == 'build':
        build(a.input, a.index, a.time_from, a.time_to, a.workers, a.subreddits)
    elif a.cmd == 'merge':
        merge(a.index)
    else:
        comments, submissions = ThreadIndex(a.index).thread(a.ids)
        print('submissions:', sorted(submissions))
        for cid, r in sorted(comments.items(), key=lambda kv: kv[1][4]):
            print(cid, r)


if __name__ == '__main__':
    main()
