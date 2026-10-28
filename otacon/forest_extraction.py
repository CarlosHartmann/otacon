'''Extract the comments and submissions of the shadow_moses forest recon from the Pushshift dumps.

The order is past_commands/forest_text_extraction/ORDER.md. Two passes, one output file per dump file:
1. RS: every RS file against all submission ids; also records each found submission's author.
2. RC: every RC file against its month's comment ids (and its neighbours' lists, in case a comment
   sits in the next or previous file), and against the submitters of all listed threads: comments
   whose author is the submission's author. It needs the complete submitter map, so it starts once
   every RS file is done.

Lines are written byte for byte. They are matched on their raw bytes first (link_id, "id" values,
author) and only those candidates are parsed as JSON, which then decides. Lines that do not look
like a compact JSON object are parsed in full.

Usage:
    python -m otacon.forest_extraction run     --ids DIR --comments-input DIR --submissions-input DIR --output DIR [--workers 2] [--only NAME ...]
    python -m otacon.forest_extraction summary --ids DIR --output DIR
'''

import argparse
import fcntl
import json
import os
import queue
import re
import shutil
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from datetime import datetime, timedelta
from multiprocessing import get_context
from pathlib import Path

import zstandard

RS_MONTHS = ('2005-06', '2021-09')
RC_MONTHS = ('2005-12', '2021-09')
READ_SIZE = 8 << 20  # large reads keep the USB disk sequential when two workers share it
PIECE_SIZE = 16 << 20  # decompressed bytes handed over per piece
HEARTBEAT = 4 << 30  # compressed bytes between progress lines within a file
ID_RE = re.compile(rb'"id":"([0-9a-z]+)"')
LINK_KEY = b'"link_id":"t3_'
NOT_AN_AUTHOR = {'', '[deleted]'}
PAUSE_NAME = '.pause_requested'
REPORT_COLUMNS = ('file', 'input_bytes', 'lines', 'expected', 'found', 'missing', 'neighbour_ids_found',
                  'submitter_comments', 'duplicates', 'undecodable', 'full_parses', 'written',
                  'output_bytes', 'seconds', 'submitter_map', 'finished')

# Set in each worker by the pool initializer.
_wanted = None  # RS pass: frozenset of submission ids
_links = None  # RC pass: submission id -> its author, b'' if unknown or deleted


def log(message):
    print(f'{datetime.now():%Y-%m-%d %H:%M:%S} {message}', flush=True)


def _duration(seconds):
    return str(timedelta(seconds=round(seconds)))


def month_range(first, last):
    year, month = map(int, first.split('-'))
    while f'{year:04d}-{month:02d}' <= last:
        yield f'{year:04d}-{month:02d}'
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)


def dump_files(directory, prefix, months):
    '''{name: path} of the PREFIX_YYYY-MM.zst files in the month range, in month order.'''
    found = {}
    for month in month_range(*months):
        path = Path(directory) / f'{prefix}_{month}.zst'
        if path.is_file():
            found[f'{prefix}_{month}'] = path
    return found


class _FrameCheck:
    '''Passes a zstd file's bytes through and follows its frame and block headers on the way.

    zstandard's stream reader stops silently where a truncated file ends; complete() tells whether
    the bytes read so far end between frames.
    '''

    def __init__(self, fh):
        self.fh, self.frames, self.checksum, self.skip = fh, 0, False, 0
        self._next_frame()

    def _next_frame(self):
        self.state, self.head, self.need = 'magic', b'', 4

    def complete(self):
        return self.frames > 0 and self.state == 'magic' and not self.head and not self.skip

    def read(self, size):
        data = self.fh.read(size)
        pos, end = 0, len(data)
        while pos < end:
            if self.skip:
                step = min(self.skip, end - pos)
                self.skip -= step
                pos += step
                continue
            take = self.need - len(self.head)
            self.head += data[pos:pos + take]
            pos += take
            if len(self.head) == self.need:
                self._header()
        return data

    def _header(self):
        head = self.head
        if self.state == 'magic':
            magic = int.from_bytes(head, 'little')
            if magic == 0xFD2FB528:
                self.state, self.need = 'frame', 5
            elif magic & 0xFFFFFFF0 == 0x184D2A50:  # skippable frame: 4-byte size, then its content
                self.state, self.need = 'skippable', 8
            else:
                raise ValueError(f'not a zstd frame: magic {magic:#x}')
        elif self.state == 'skippable':
            self._next_frame()
            self.skip = int.from_bytes(head[4:], 'little')
        elif self.state == 'frame':
            descriptor = head[4]
            single_segment = descriptor >> 5 & 1
            size = (5 + (not single_segment) + (0, 1, 2, 4)[descriptor & 3]
                    + (single_segment, 2, 4, 8)[descriptor >> 6])
            if len(head) < size:
                self.need = size
            else:
                self.frames += 1
                self.checksum = bool(descriptor & 4)
                self.state, self.head, self.need = 'block', b'', 3
        elif self.state == 'block':
            header = int.from_bytes(head, 'little')
            kind = header >> 1 & 3
            if kind == 3:
                raise ValueError('reserved zstd block type')
            self.head, self.skip = b'', 1 if kind == 1 else header >> 3  # an RLE block holds one byte
            if header & 1 and self.checksum:  # last block of the frame
                self.state, self.need = 'checksum', 4
            elif header & 1:
                self._next_frame()
        else:  # checksum
            self._next_frame()


def read_chunks(path):
    '''Yield (decompressed piece, compressed bytes read so far) for a zstd file, across frames.

    Decompression runs in a thread (zstandard's stream reader releases the GIL), so it overlaps
    with the matching. Raises if the file ends inside a frame.
    '''
    pieces = queue.Queue(maxsize=4)
    stop = threading.Event()

    def decompress():
        try:
            with open(path, 'rb', buffering=0) as fh:
                check = _FrameCheck(fh)
                reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(
                    check, read_size=READ_SIZE, read_across_frames=True)
                while (piece := reader.read(PIECE_SIZE)) and not stop.is_set():
                    pieces.put((piece, fh.tell()))
                if not stop.is_set() and (fh.read(1) or not check.complete()):
                    raise EOFError(f'{path} does not end where a zstd frame ends (truncated?)')
            pieces.put(None)
        except BaseException as error:
            pieces.put(error)

    thread = threading.Thread(target=decompress, daemon=True)
    thread.start()
    try:
        while (item := pieces.get()) is not None:
            if isinstance(item, BaseException):
                raise item
            yield item
    finally:
        stop.set()
        while thread.is_alive():  # unblock a pending put so the thread (and its window) goes away
            try:
                pieces.get(timeout=0.1)
            except queue.Empty:
                pass
        thread.join()


def read_lines(path, label):
    '''Yield the lines of a zstd file as bytes, without the newline; log progress every HEARTBEAT.'''
    size, start, beat, rest = os.path.getsize(path), time.time(), HEARTBEAT, b''
    for piece, position in read_chunks(path):
        lines = piece.split(b'\n')
        lines[0] = rest + lines[0]
        rest = lines.pop()
        yield from lines
        if position >= beat:
            beat += HEARTBEAT
            log(f'  {label}: {position / 1e9:.1f} of {size / 1e9:.1f} GB read, '
                f'{position / 1e6 / (time.time() - start):.1f} MB/s')
    if rest:
        yield rest


def _parse(line):
    '''The line as a dict, or None if it is not a JSON object.'''
    try:
        obj = json.loads(line)
    except (ValueError, RecursionError):
        return None
    return obj if isinstance(obj, dict) else None


def _read_ids(path):
    with open(path, 'rb') as f:
        return f.read().split()


def _write_atomic(path, data):
    tmp = path.with_name(f'.{path.name}.tmp')
    tmp.write_bytes(data)
    os.replace(tmp, path)


def _id_lines(ids):
    return b''.join(i + b'\n' for i in sorted(ids, key=lambda i: int(i, 36)))


class _Output:
    '''One dump file's outputs: a zstd data file and its side files, committed together.

    The data file is renamed into place last, so its existence means the dump file is done.
    '''

    def __init__(self, out_dir, kind, name):
        self.out_dir, self.name = out_dir, name
        self.path = out_dir / kind / f'{name}.ndjson.zst'
        self.tmp = self.path.with_name(f'.{self.path.name}.tmp')
        self.side = {}
        for stale in (out_dir / 'duplicates' / f'{name}.txt', out_dir / 'neighbours' / f'{name}.tsv'):
            stale.unlink(missing_ok=True)  # from an earlier run of this file
        self.fh = open(self.tmp, 'wb')
        self.writer = zstandard.ZstdCompressor(level=3, write_checksum=True).stream_writer(self.fh, closefd=False)
        self.written = 0

    def write(self, line):
        self.writer.write(line)
        self.writer.write(b'\n')
        self.written += 1

    def commit(self, stats):
        self.writer.close()
        self.fh.flush()
        os.fsync(self.fh.fileno())
        self.fh.close()
        for path, data in self.side.items():
            path.parent.mkdir(exist_ok=True)
            _write_atomic(path, data)
        stats['written'] = self.written
        stats['output_bytes'] = self.tmp.stat().st_size
        stats['finished'] = datetime.now().isoformat(timespec='seconds')
        _write_atomic(self.out_dir / 'stats' / f'{self.name}.json', json.dumps(stats, indent=1).encode())
        os.replace(self.tmp, self.path)


def _init_rs(ids_dir):
    global _wanted
    _wanted = frozenset(_read_ids(Path(ids_dir) / 'submission_ids.txt'))


def scan_rs(job):
    '''Write the listed submissions of one RS file and record their authors.'''
    name, path, out_dir = job
    start, wanted = time.time(), _wanted
    authors, duplicates = {}, []
    lines = undecodable = full_parses = 0
    out = _Output(out_dir, 'RS', name)
    for line in read_lines(path, name):
        if not line:
            continue
        lines += 1
        ids = ID_RE.findall(line) if line[0] == 123 and line[-1] == 125 else None  # {...}
        if ids:
            if wanted.isdisjoint(ids):
                continue
        else:
            full_parses += 1
        obj = _parse(line)
        if obj is None:
            undecodable += 1
            continue
        sid = obj.get('id')
        if not isinstance(sid, str) or (sid := sid.encode()) not in wanted:
            continue  # e.g. a crosspost quoting a listed submission
        if sid in authors:
            duplicates.append(sid)
            continue
        out.write(line)
        authors[sid] = obj.get('author') or ''
    out.side[out_dir / 'submitters' / f'{name}.tsv'] = ''.join(
        f'{sid.decode()}\t{author}\n' for sid, author in authors.items()).encode()
    if duplicates:
        out.side[out_dir / 'duplicates' / f'{name}.txt'] = _id_lines(duplicates)
    stats = dict(file=name, input_bytes=path.stat().st_size, lines=lines, expected=None, found=len(authors),
                 missing=None, neighbour_ids_found=None, submitter_comments=None, duplicates=len(duplicates),
                 undecodable=undecodable, full_parses=full_parses, seconds=round(time.time() - start, 1),
                 submitter_map=None)
    out.commit(stats)
    return stats


def load_links(ids_dir, map_files):
    '''Submission id -> author bytes for every listed submission, b'' where none is usable.

    map_files are TSVs whose first two columns are submission id and author.
    '''
    links = dict.fromkeys(_read_ids(Path(ids_dir) / 'submission_ids.txt'), b'')
    shared = {}  # one bytes object per author
    for map_file in map_files:
        with open(map_file, 'rb') as f:
            for row in f:
                sid, author = row.rstrip(b'\n').split(b'\t')[:2]
                if sid in links and not links[sid] and author.decode() not in NOT_AN_AUTHOR:
                    links[sid] = shared.setdefault(author, author)
    return links


def _init_rc(ids_dir, map_files):
    global _links
    _links = load_links(ids_dir, map_files)


def scan_rc(job):
    '''Write the listed comments and the submitters' comments of one RC file.'''
    name, path, out_dir, own_list, neighbour_lists, map_state = job
    start, links = time.time(), _links
    listed = {}  # comment id -> name of the list it is on
    for list_name, list_path in neighbour_lists + [(name, own_list)]:
        if list_path is not None:
            listed.update(dict.fromkeys(_read_ids(list_path), list_name))
    listed_ids = listed.keys()
    expected = sum(1 for list_name in listed.values() if list_name == name)
    seen, duplicates, neighbour_found = set(), [], []
    lines = undecodable = full_parses = found = submitter_comments = 0
    out = _Output(out_dir, 'RC', name)
    for line in read_lines(path, name):
        if not line:
            continue
        lines += 1
        i = line.find(LINK_KEY) if line[0] == 123 and line[-1] == 125 else -1  # {...}
        if i >= 0:
            i += len(LINK_KEY)
            author = links.get(line[i:line.find(b'"', i)])
            if author is None:
                continue  # not one of the listed threads
            if listed_ids.isdisjoint(ID_RE.findall(line)) and not (author and b'"author":"' + author + b'"' in line):
                continue
        else:
            full_parses += 1
        obj = _parse(line)
        if obj is None:
            undecodable += 1
            continue
        cid, link = obj.get('id'), obj.get('link_id')
        if not isinstance(cid, str) or not isinstance(link, str) or not link.startswith('t3_'):
            continue
        cid = cid.encode()
        list_name = listed.get(cid)
        submitter = links.get(link[3:].encode()) or b''
        by_submitter = bool(submitter) and obj.get('author') == submitter.decode()
        if list_name is None and not by_submitter:
            continue
        if cid in seen:
            duplicates.append(cid)
            continue
        seen.add(cid)
        out.write(line)
        if list_name == name:
            found += 1
        elif list_name is not None:
            neighbour_found.append((list_name, cid))
        else:
            submitter_comments += 1
    own = [cid for cid, list_name in listed.items() if list_name == name]
    missing = [cid for cid in own if cid not in seen]
    if own_list is not None:
        out.side[out_dir / 'missing' / f'{name}.txt'] = _id_lines(missing)
    if neighbour_found:
        out.side[out_dir / 'neighbours' / f'{name}.tsv'] = ''.join(
            f'{list_name}\t{cid.decode()}\n' for list_name, cid in sorted(neighbour_found)).encode()
    if duplicates:
        out.side[out_dir / 'duplicates' / f'{name}.txt'] = _id_lines(duplicates)
    stats = dict(file=name, input_bytes=path.stat().st_size, lines=lines, expected=expected, found=found,
                 missing=len(missing), neighbour_ids_found=len(neighbour_found),
                 submitter_comments=submitter_comments, duplicates=len(duplicates), undecodable=undecodable,
                 full_parses=full_parses, seconds=round(time.time() - start, 1), submitter_map=map_state)
    out.commit(stats)
    return stats


def read_stats(out_dir):
    return {p.stem: json.loads(p.read_text()) for p in sorted((out_dir / 'stats').glob('R[CS]_*.json'))}


def write_report(out_dir):
    '''report.tsv: one row per finished dump file, from stats/.'''
    rows = ['\t'.join(REPORT_COLUMNS)]
    for stats in read_stats(out_dir).values():
        rows.append('\t'.join('' if stats.get(c) is None else str(stats[c]) for c in REPORT_COLUMNS))
    _write_atomic(out_dir / 'report.tsv', ('\n'.join(rows) + '\n').encode())


def write_submitter_map(out_dir, ids_dir, rs_names):
    '''submitter_map.tsv (submission id, author, RS file) and missing/submissions.txt, from submitters/.

    A submission found in several RS files keeps the first copy with a usable author, else the first.
    '''
    chosen, copies = {}, 0
    for name in rs_names:
        with open(out_dir / 'submitters' / f'{name}.tsv', encoding='utf-8') as f:
            for row in f:
                sid, author = row.rstrip('\n').split('\t')
                if sid in chosen:
                    copies += 1
                    if chosen[sid][0] in NOT_AN_AUTHOR and author not in NOT_AN_AUTHOR:
                        chosen[sid] = (author, name)
                else:
                    chosen[sid] = (author, name)
    wanted = [i.decode() for i in _read_ids(Path(ids_dir) / 'submission_ids.txt')]
    missing = [sid for sid in wanted if sid not in chosen]
    _write_atomic(out_dir / 'missing' / 'submissions.txt', ''.join(f'{sid}\n' for sid in missing).encode())
    _write_atomic(out_dir / 'submitter_map.tsv', ('submission_id\tauthor\tdump_file\n' + ''.join(
        f'{sid}\t{author}\t{name}\n' for sid, (author, name) in sorted(chosen.items(), key=lambda kv: int(kv[0], 36))
    )).encode())
    usable = sum(author not in NOT_AN_AUTHOR for author, _ in chosen.values())
    log(f'Submitter map: {len(chosen):,} of {len(wanted):,} submissions found ({len(missing):,} missing), '
        f'{usable:,} with a usable author, {copies:,} copies in further RS files.')


class Progress:
    def __init__(self, sizes, done):
        self.sizes, self.done = sizes, set(done)
        self.total = sum(sizes.values())
        self.start, self.run_bytes = time.time(), 0

    def file_done(self, stats):
        name = stats['file']
        self.done.add(name)
        self.run_bytes += self.sizes[name]
        done_bytes = sum(self.sizes[n] for n in self.done)
        elapsed = time.time() - self.start
        rate = self.run_bytes / elapsed if elapsed else 0
        counts = f'found {stats["found"]:,}' + (f'/{stats["expected"]:,}' if stats['expected'] is not None else '')
        if stats['submitter_comments'] is not None:
            counts += f', submitter {stats["submitter_comments"]:,}'
        for key in ('duplicates', 'undecodable', 'neighbour_ids_found'):
            if stats[key]:
                counts += f', {key} {stats[key]:,}'
        message = (f'done {name} ({stats["input_bytes"] / 1e9:.2f} GB in {_duration(stats["seconds"])}; {counts}) | '
                   f'{len(self.done)}/{len(self.sizes)} files, {done_bytes / 1e9:,.1f}/{self.total / 1e9:,.1f} GB, '
                   f'{rate / 1e6:.1f} MB/s, elapsed {_duration(elapsed)}')
        if rate and done_bytes < self.total:
            eta = (self.total - done_bytes) / rate
            message += f', ETA {datetime.now() + timedelta(seconds=eta):%a %d %b %H:%M} (in {_duration(eta)})'
        log(message)


def _run_pass(jobs, worker, initializer, initargs, workers, out_dir, progress):
    '''Run the jobs on a pool, a few at a time so a pause request takes effect. False if paused.'''
    pause_file = out_dir / PAUSE_NAME
    todo, pending, failure = list(jobs), {}, None
    with ProcessPoolExecutor(max_workers=min(workers, len(todo)), mp_context=get_context('spawn'),
                             initializer=initializer, initargs=initargs) as pool:
        while todo or pending:
            while todo and len(pending) < workers and failure is None and not pause_file.exists():
                job = todo.pop(0)
                log(f'start {job[0]} ({job[1].stat().st_size / 1e9:.2f} GB)')
                pending[pool.submit(worker, job)] = job
            if not pending:
                break
            finished, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in finished:
                job = pending.pop(future)
                try:
                    stats = future.result()
                except Exception as error:
                    log(f'FAILED {job[0]}: {error!r}')
                    failure = failure or error
                    continue
                progress.file_done(stats)
                write_report(out_dir)
    if failure is not None:
        raise failure
    if todo:
        log(f'Paused with {len(todo)} files of this pass left; rerun to resume.')
        return False
    return True


def _rc_done(out_dir, name, map_complete):
    if not (out_dir / 'RC' / f'{name}.ndjson.zst').exists():
        return False
    stats = json.loads((out_dir / 'stats' / f'{name}.json').read_text())
    return stats['submitter_map'] == 'complete' or not map_complete  # redo test files once the map is complete


def run(args):
    out_dir, ids_dir = args.output, args.ids
    for directory in (args.comments_input, args.submissions_input, ids_dir):
        if not directory.is_dir():
            sys.exit(f'Not reachable: {directory}')
    for sub in ('RS', 'RC', 'stats', 'missing', 'submitters'):
        (out_dir / sub).mkdir(parents=True, exist_ok=True)
    lock = open(out_dir / '.lock', 'w')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit(f'Another run is using {out_dir}.')
    free = shutil.disk_usage(out_dir).free
    if free < args.min_free_gb * 2**30:
        sys.exit(f'Only {free / 2**30:,.0f} GiB free on {out_dir} (--min-free-gb {args.min_free_gb}).')

    all_rs = dump_files(args.submissions_input, 'RS', RS_MONTHS)
    all_rc = dump_files(args.comments_input, 'RC', RC_MONTHS)
    for prefix, months, found in (('RS', RS_MONTHS, all_rs), ('RC', RC_MONTHS, all_rc)):
        absent = [f'{prefix}_{m}' for m in month_range(*months) if f'{prefix}_{m}' not in found]
        if absent:
            more = f' and {len(absent) - 5} more' if len(absent) > 5 else ''
            log(f'WARNING: no dump file for {", ".join(absent[:5])}{more}')
    rs, rc = dict(all_rs), dict(all_rc)
    if args.only:
        unknown = set(args.only) - set(rs) - set(rc)
        if unknown:
            sys.exit(f'Not a dump file in range: {", ".join(sorted(unknown))}')
        rs = {n: p for n, p in rs.items() if n in args.only}
        rc = {n: p for n, p in rc.items() if n in args.only}

    rs_done = {n for n in all_rs if (out_dir / 'RS' / f'{n}.ndjson.zst').exists()}
    map_will_be_complete = rs_done | set(rs) == set(all_rs)  # once this run's RS pass is through
    sizes = {n: p.stat().st_size for n, p in {**rs, **rc}.items()}
    progress = Progress(sizes, [n for n in rs if n in rs_done]
                        + [n for n in rc if _rc_done(out_dir, n, map_will_be_complete)])
    log(f'{len(sizes)} dump files, {progress.total / 1e9:,.1f} GB; {len(progress.done)} already done; '
        f'workers: {args.workers}; output {out_dir}')

    rs_jobs = [(n, p, out_dir) for n, p in rs.items() if n not in rs_done]
    if rs_jobs:
        log(f'RS pass: {len(rs_jobs)} files')
        if not _run_pass(rs_jobs, scan_rs, _init_rs, (str(ids_dir),), args.workers, out_dir, progress):
            return
        rs_done |= {job[0] for job in rs_jobs}
    map_complete = rs_done == set(all_rs)
    if map_complete:
        if not (out_dir / 'submitter_map.tsv').exists() or rs_jobs:
            write_submitter_map(out_dir, ids_dir, list(all_rs))
        map_files, map_state = [out_dir / 'submitter_map.tsv'], 'complete'
    else:
        map_files = [out_dir / 'submitters' / f'{n}.tsv' for n in all_rs if n in rs_done]
        map_state = f'partial ({len(map_files)} of {len(all_rs)} RS files)'
        log(f'WARNING: submitter map is {map_state}; the RC outputs will be redone once it is complete.')

    rc_jobs = []
    for name, path in rc.items():
        if _rc_done(out_dir, name, map_complete):
            continue
        month = name[3:]
        neighbours = []
        for other in (_shift(month, -1), _shift(month, 1)):
            other_list = ids_dir / 'comment_ids' / f'RC_{other}.txt'
            if other_list.exists():
                neighbours.append((f'RC_{other}', other_list))
        own_list = ids_dir / 'comment_ids' / f'{name}.txt'
        rc_jobs.append((name, path, out_dir, own_list if own_list.exists() else None, neighbours, map_state))
    if rc_jobs:
        log(f'RC pass: {len(rc_jobs)} files, submitter map {map_state}')
        if not _run_pass(rc_jobs, scan_rc, _init_rc, (str(ids_dir), map_files), args.workers, out_dir, progress):
            return
    write_report(out_dir)
    log(f'All {len(sizes)} selected dump files are done.')


def _shift(month, delta):
    year, m = map(int, month.split('-'))
    m += delta
    year, m = year + (m - 1) // 12, (m - 1) % 12 + 1
    return f'{year:04d}-{m:02d}'


def summary(args):
    '''Print the totals and write missing_comments.tsv (listed ids found in no RC file).'''
    out_dir, ids_dir = args.output, args.ids
    stats = read_stats(out_dir)
    rs = [s for n, s in stats.items() if n.startswith('RS')]
    rc = [s for n, s in stats.items() if n.startswith('RC')]
    neighbour_found = set()
    for path in (out_dir / 'neighbours').glob('RC_*.tsv'):
        neighbour_found.update(tuple(row.split('\t')) for row in path.read_text().splitlines())
    missing = []
    for path in sorted((out_dir / 'missing').glob('RC_*.txt')):
        missing += [(path.stem, cid) for cid in path.read_text().split() if (path.stem, cid) not in neighbour_found]
    _write_atomic(out_dir / 'missing_comments.tsv',
                  ('list_file\tcomment_id\n' + ''.join(f'{n}\t{c}\n' for n, c in missing)).encode())

    def total(rows, key):
        return sum(r[key] or 0 for r in rows)

    expected_subs = len(_read_ids(ids_dir / 'submission_ids.txt'))
    expected_comments = sum(int(line.split('\t')[1]) for line in (ids_dir / 'manifest.tsv').read_text().splitlines()[1:]
                            if line.startswith('comment_ids/'))
    missing_subs = out_dir / 'missing' / 'submissions.txt'
    print(f'RS files done: {len(rs)}; RC files done: {len(rc)}')
    print(f'Lines read: {total(rs, "lines"):,} RS, {total(rc, "lines"):,} RC; '
          f'input {total(rs, "input_bytes") / 1e9:,.1f} GB RS, {total(rc, "input_bytes") / 1e9:,.1f} GB RC')
    print(f'Submissions: {expected_subs:,} expected, {total(rs, "found"):,} found in RS files (copies included)'
          + (f', {len(missing_subs.read_text().split()):,} missing' if missing_subs.exists() else ''))
    print(f'Comments: {expected_comments:,} expected, {total(rc, "found"):,} found in their own RC file, '
          f'{len(neighbour_found):,} in a neighbouring one, {len(missing):,} missing (missing_comments.tsv)')
    print(f'Submitter comments kept (not listed): {total(rc, "submitter_comments"):,}')
    print(f'Duplicates skipped: {total(rs, "duplicates"):,} RS, {total(rc, "duplicates"):,} RC; '
          f'undecodable lines: {total(rs, "undecodable"):,} RS, {total(rc, "undecodable"):,} RC; '
          f'lines parsed in full: {total(rs, "full_parses"):,} RS, {total(rc, "full_parses"):,} RC')
    print(f'Output: {total(rs, "output_bytes") / 1e9:,.2f} GB RS, {total(rc, "output_bytes") / 1e9:,.2f} GB RC (zstd)')
    print(f'Worker time: {_duration(total(rs, "seconds"))} RS, {_duration(total(rc, "seconds"))} RC')
    if stats:
        print(f'First file finished {min(s["finished"] for s in stats.values())}, '
              f'last {max(s["finished"] for s in stats.values())}')
    partial = sorted(s['file'] for s in rc if s['submitter_map'] != 'complete')
    if partial:
        print(f'RC files done with a partial submitter map: {", ".join(partial)}')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('run', 'summary'):
        p = sub.add_parser(name)
        p.add_argument('--ids', type=Path, required=True, help='the order\'s ids/ folder')
        p.add_argument('--output', type=Path, required=True)
        if name == 'run':
            p.add_argument('--comments-input', type=Path, required=True)
            p.add_argument('--submissions-input', type=Path, required=True)
            p.add_argument('--workers', type=int, default=2)
            p.add_argument('--min-free-gb', type=float, default=100, help='free space required on the output disk')
            p.add_argument('--only', nargs='+', metavar='NAME', help='only these dump files, e.g. RS_2012-03 RC_2012-03')
    args = parser.parse_args()
    if args.command == 'run':
        run(args)
    else:
        summary(args)


if __name__ == '__main__':
    main()
