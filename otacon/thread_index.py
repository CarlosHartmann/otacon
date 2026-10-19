'''Compact comment-parent index (Parquet + DuckDB) for reconstructing comment threads.

Usage:
    python -m otacon.thread_index build  --input DIR --index DIR [--subreddits FILE] [--time_from 2010-01] [--time_to 2021-09] [--workers 4]
    python -m otacon.thread_index merge  --index DIR [--subreddits FILE] [--temp-dir DIR] [--max-temp-size 800GiB] [--chunks 20] [--memory-limit 5GB] [--threads N]
    python -m otacon.thread_index lookup --index DIR ID [ID ...]
'''

import argparse
import json
import os
import re
import shutil
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


def merge_bounds(con, parts):
    """Key ranges to split the sorts on: one group for by_id, one per parent_is_submission value for by_parent.
    Parent ranges start at a low quantile: a few replies to very old comments would otherwise stretch the range and
    crowd nearly all rows into the top chunks. Keys below the start land in chunk 0."""
    lo, hi = con.execute(f"SELECT min(id), max(id) FROM {parts}").fetchone()
    groups = {'by_id': [(None, lo, hi)], 'by_parent': []}
    for is_sub, plo, phi in con.execute(
        f"SELECT parent_is_submission, approx_quantile(parent_id, 0.0001), max(parent_id) FROM {parts} "
        f"GROUP BY 1 ORDER BY 1"
    ).fetchall():
        groups['by_parent'].append((is_sub, plo, phi))
    return groups


COPY_OPTS = '(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)'
COLUMNS = ', '.join(SCHEMA.names)


def chunk_range(lo, hi, chunks, i):
    width = hi - lo + 1
    return lo + width * i // chunks, lo + width * (i + 1) // chunks


def write_chunk(con, select, dst, name):
    part_tmp = dst.with_suffix('.parquet.tmp')
    con.execute(f"COPY ({select}) TO '{part_tmp}' {COPY_OPTS}")
    os.replace(part_tmp, dst)
    print(f'wrote {name}/{dst.name}', flush=True)


def merge(index_dir, subreddit_file=None, max_temp_size='800GiB', temp_dir=None, chunks=20, memory_limit=None,
          threads=None):
    """Write by_id/ and by_parent/ as sorted Parquet chunks split by key range, so each sort spills only ~1/chunks
    of the data. Finished chunks are skipped, so an interrupted merge resumes where it stopped.

    by_id chunks are read straight from the parts: ids rise with time, so Parquet stats skip all but a month or two.
    parent_id stats span nearly everything (submission and comment parents are mixed), so by_parent first splits
    the rows into per-chunk buckets under the temp dir in one unsorted pass, then sorts each bucket."""
    idx = Path(index_dir)
    config = {'max_temp_directory_size': max_temp_size}
    if memory_limit:
        config['memory_limit'] = memory_limit
    if threads:
        config['threads'] = threads
    con = duckdb.connect(config=config)
    tmp = Path(temp_dir) if temp_dir else idx / 'tmp'
    tmp.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{tmp}'")
    print(f'temp_directory: {tmp}', flush=True)
    print(f'max_temp_directory_size: {max_temp_size}', flush=True)
    print(f"memory_limit: {con.execute('SELECT current_setting(?)', ['memory_limit']).fetchone()[0]}", flush=True)
    print(f"threads: {con.execute('SELECT current_setting(?)', ['threads']).fetchone()[0]}", flush=True)
    print(f'chunks: {chunks}', flush=True)
    parts = f"read_parquet('{idx}/parts/*.parquet')"
    filter_clause = ''
    if subreddit_file:
        wanted = pa.table({'subreddit': sorted(load_subreddits(subreddit_file))})
        con.register('wanted_subreddits', wanted)
        filter_clause = 'AND lower(subreddit) IN (SELECT subreddit FROM wanted_subreddits)'

    bounds_file = idx / 'merge_bounds.json'
    if bounds_file.exists():
        groups = json.loads(bounds_file.read_text())
    else:
        print('scanning key ranges', flush=True)
        groups = merge_bounds(con, parts)
    if groups.setdefault('chunks', chunks) != chunks:  # finished chunk files would not line up with new ranges
        raise SystemExit(f"{bounds_file} is for --chunks {groups['chunks']}, not {chunks}")
    bounds_file.write_text(json.dumps(groups))

    by_id = idx / 'by_id'
    by_id.mkdir(exist_ok=True)
    (_, lo, hi), = groups['by_id']
    for i in range(chunks):
        dst = by_id / f'part-{i:03d}.parquet'
        if not dst.exists():
            a, b = chunk_range(lo, hi, chunks, i)
            write_chunk(con, f'SELECT * FROM {parts} WHERE id >= {a} AND id < {b} {filter_clause} ORDER BY id',
                        dst, 'by_id')
    print('finished by_id', flush=True)

    by_parent = idx / 'by_parent'
    by_parent.mkdir(exist_ok=True)
    targets = [(int(is_sub), i, by_parent / f'part-{int(is_sub)}-{i:03d}.parquet')
               for is_sub, _, _ in groups['by_parent'] for i in range(chunks)]
    if any(not dst.exists() for *_, dst in targets):
        buckets = tmp / 'by_parent_buckets'
        if not buckets.exists():
            print('splitting by_parent into buckets', flush=True)
            bucket_expr = 'CASE ' + ' '.join(
                f'WHEN parent_is_submission = {str(is_sub).lower()} THEN greatest(0, (parent_id - {plo}) * {chunks} // {phi - plo + 1})'
                for is_sub, plo, phi in groups['by_parent']) + ' END'
            buckets_tmp = tmp / 'by_parent_buckets.tmp'
            if buckets_tmp.exists():
                shutil.rmtree(buckets_tmp)
            con.execute(
                f"COPY (SELECT {COLUMNS}, parent_is_submission::INT AS grp, {bucket_expr} AS bucket FROM {parts} "
                f"WHERE true {filter_clause}) TO '{buckets_tmp}' "
                f"(FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (grp, bucket))"
            )
            os.replace(buckets_tmp, buckets)
        for grp, i, dst in targets:
            if dst.exists():
                continue
            src = buckets / f'grp={grp}' / f'bucket={i}'
            if src.exists():
                write_chunk(con, f"SELECT {COLUMNS} FROM read_parquet('{src}/*.parquet', hive_partitioning = false) "
                                 f"ORDER BY parent_id", dst, 'by_parent')
            else:  # empty key range
                write_chunk(con, f'SELECT * FROM {parts} WHERE false', dst, 'by_parent')
        shutil.rmtree(buckets)
    print('finished by_parent', flush=True)


class ThreadIndex:
    def __init__(self, index_dir):
        idx = Path(index_dir)
        self.con = duckdb.connect()
        self.by_id = self._source(idx, 'by_id')
        self.by_parent = self._source(idx, 'by_parent')

    @staticmethod
    def _source(idx, name):
        path = idx / name / '*.parquet' if (idx / name).is_dir() else idx / f'{name}.parquet'
        return f"read_parquet('{path}')"

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
    m.add_argument('--subreddits', help='text file with one subreddit name per line')
    m.add_argument('--max-temp-size', default='800GiB')
    m.add_argument('--temp-dir', help='DuckDB spill directory (default: <index>/tmp)')
    m.add_argument('--chunks', type=int, default=20, help='key-range chunks per sort (more = less spill per chunk)')
    m.add_argument('--threads', type=int, help='DuckDB worker threads (default: one per core)')
    m.add_argument('--memory-limit', help="DuckDB memory limit, e.g. '5GB' (default: DuckDB's 80%% of RAM)")
    l = sub.add_parser('lookup')
    l.add_argument('--index', required=True)
    l.add_argument('ids', nargs='+')
    a = p.parse_args()

    if a.cmd == 'build':
        build(a.input, a.index, a.time_from, a.time_to, a.workers, a.subreddits)
    elif a.cmd == 'merge':
        merge(a.index, a.subreddits, a.max_temp_size, a.temp_dir, a.chunks, a.memory_limit, a.threads)
    else:
        comments, submissions = ThreadIndex(a.index).thread(a.ids)
        print('submissions:', sorted(submissions))
        for cid, r in sorted(comments.items(), key=lambda kv: kv[1][4]):
            print(cid, r)


if __name__ == '__main__':
    main()
