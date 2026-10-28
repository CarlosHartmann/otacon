import pytest
import argparse

import os
import re
import datetime
import json
import shutil
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from otacon.argument_handling import handle_args
from otacon.main import extract
import otacon.main as main_module
import otacon.history_runner as history_runner

from otacon.data_types import valid_date
from otacon.data_types import dir_path
from otacon.prep_input import within_timeframe
from otacon.main import filter
from otacon.prep_input import fetch_data_timeframe
from otacon.main import inside_quote
from otacon.data_types import comment_regex

def test_valid_date():
    assert valid_date('2010-01') == (2010, 1)
    assert valid_date('2019-02') == (2019, 2)
    assert valid_date('2019-2') == (2019, 2)
    
    with pytest.raises(argparse.ArgumentTypeError):
        assert valid_date('2010-12-01') != (2010, 12)
    with pytest.raises(argparse.ArgumentTypeError):    
        assert valid_date('2010-13') != (2010, 13)
    with pytest.raises(argparse.ArgumentTypeError):    
        assert valid_date('2010-00') != (2010, 0)
    with pytest.raises(argparse.ArgumentTypeError):    
        assert valid_date('201-10') != (201, 10)


def test_dir_path():
    assert dir_path('/Users/chartman') == '/Users/chartman'

    with pytest.raises(NotADirectoryError):
        assert dir_path('/Some/Kind/Of/Clown/Path') != '/Some/Kind/Of/Clown/Path'


def test_within_timeframe():
    assert within_timeframe('RC_2010-03.zst', (2010, 1), (2010, 6)) == True
    assert within_timeframe('RC_2010-03.zst', (2010, 1), (2010, 3)) == True
    assert within_timeframe('RC_2010-03.zst', (2010, 3), (2010, 6)) == True
    assert within_timeframe('RC_2010-03', (2010, 3), (2010, 3)) == True
    assert within_timeframe('RC_2011-03', (2010, 5), (2012, 1)) == True
    assert within_timeframe('RC_2011-03', (2010, 1), (2012, 5)) == True
    assert within_timeframe('RC_2011-03', (2001, 12), (2011, 3)) == True


    assert within_timeframe('RC_2009-03', (2010, 1), (2010, 6)) == False
    assert within_timeframe('RC_2020-03', (2010, 1), (2010, 6)) == False

#def test_filter():
#    assert filter({'body':'Gosh darn it, anyway!'}, popularity_threshold=None) == (False, None)
#    assert filter({'body': 'Fuck fuck fuck fuck fuck fuck fuck!'}, popularity_threshold=None) == (True, 'offensive language')
#    assert filter({'body':"Bleep bloop, I'm a bot!"}, popularity_threshold=None) == (True, 'non-human generated')

# This one only works if there is a data dir and you want to make sure that the timeframe corresponds to the data that you think you have
# def test_fetch_data_timeframe():
#    assert fetch_data_timeframe("data") == ((2010, 1), (2012, 8))

def test_inside_quote():
    text = '''Lookie here:
    
    &gt; It was me who ate the pop tart.
    
    They said it themself!'''
    r = re.search('pop tart', text)
    span1 = (r.start(), r.end())

    r = re.search('themself', text)
    span2 = (r.start(), r.end())

    assert inside_quote(text, span1) == True
    assert inside_quote(text, span2) == False

def test_comment_regex():
    string = '(?i)^naja'

    assert comment_regex(string) == '^(?i)(^>.+\n\n)*naja'


def test_name_file(monkeypatch, tmp_path):
    names_file = tmp_path / 'names.txt'
    names_file.write_text('Alice\nBob\n', encoding='utf-8')
    monkeypatch.setattr('sys.argv', [
        'otacon', '--input', str(tmp_path), '--output', str(tmp_path),
        '--time_from', '2010-01', '--time_to', '2010-01', '--src', 'user',
        '--name-file', str(names_file),
    ])

    args = handle_args()

    assert args.name == {'alice', 'bob'}


def test_extract_submission_preserves_post_metadata():
    entry = {
        'id': 'abc123',
        'author': 'example_user',
        'created_utc': 1609459200,
        'selftext': 'A full submission body',
        'subreddit': 'example',
        'score': 4,
        'permalink': '/r/example/comments/abc123/title/',
    }
    output = StringIO()
    args = SimpleNamespace(searchmode='subs', return_all=True, firstmatch=False)

    extract(args, {'index': 0, 'entry': entry}, None, False, output, None)

    result = json.loads(output.getvalue())
    assert result['selftext'] == entry['selftext']
    assert result['_otacon_type'] == 'post'
    assert result['_otacon_permalink'] == 'https://www.reddit.com/r/example/comments/abc123/title/'


def test_process_timeframe_can_reset_deduplication_monthly(monkeypatch):
    seen_hash_counts = []

    def record_month(month, args, outfile, reviewfile):
        seen_hash_counts.append(len(main_module.hash_set))
        main_module.hash_set.add(month.encode())

    try:
        main_module.hash_set.clear()
        monkeypatch.setattr(main_module, 'process_month', record_month)
        args = SimpleNamespace(reset_dedup_monthly=True)
        main_module.process_timeframe(args, ['2010-01', '2010-02'], None, None)
        assert seen_hash_counts == [0, 0]
    finally:
        main_module.hash_set.clear()


def test_filter_then_extract_skips_filters_when_requested(monkeypatch):
    extracted = []
    output = StringIO()
    review = StringIO()

    def unexpected_filter(entry, popularity):
        raise AssertionError('filter should not run when dont_filter is set')

    def record_extract(args, entry, regex, include_quoted, outfile, filter_reason):
        extracted.append((outfile, filter_reason))

    monkeypatch.setattr(main_module, 'filter', unexpected_filter)
    monkeypatch.setattr(main_module, 'extract', record_extract)
    args = SimpleNamespace(dont_filter=True, popularity=None)

    main_module.filter_then_extract({'entry': {}}, None, args, False, output, review)

    assert extracted == [(output, None)]


def test_filter_then_extract_applies_filters_by_default(monkeypatch):
    extracted = []
    output = StringIO()
    review = StringIO()

    monkeypatch.setattr(main_module, 'filter', lambda entry, popularity: (True, 'AutoModerator'))
    monkeypatch.setattr(
        main_module,
        'extract',
        lambda args, entry, regex, include_quoted, outfile, filter_reason: extracted.append(
            (outfile, filter_reason)
        ),
    )
    args = SimpleNamespace(dont_filter=False, popularity=None)

    main_module.filter_then_extract({'entry': {}}, None, args, False, output, review)

    assert extracted == [(review, 'AutoModerator')]


def test_relevant_ignores_record_without_author():
    args = SimpleNamespace(
        name={'alice'},
        src='user',
        case_sensitive=False,
    )

    assert main_module.relevant({}, args) is False


def test_history_runner_offloads_paired_month_with_cutoff_and_no_filter(monkeypatch, tmp_path):
    comments = tmp_path / 'comments'
    submissions = tmp_path / 'submissions'
    output_root = tmp_path / 'output'
    remote_root = tmp_path / 'remote'
    comments.mkdir()
    submissions.mkdir()
    output_root.mkdir()
    remote_root.mkdir()

    comment = {
        'author': 'AutoModerator', 'created_utc': 1670000000, 'id': 'comment1',
        'body': 'A comment', 'subreddit': 'example', 'score': 1,
        'author_flair_text': None, 'link_id': 't3_post1', 'parent_id': 't3_post1',
    }
    submission = {
        'author': 'alice', 'created_utc': 1670000000, 'id': 'post1',
        'selftext': 'A submission', 'subreddit': 'example', 'score': 2,
        'author_flair_text': None, 'permalink': '/r/example/comments/post1/title/',
    }
    (comments / 'RC_2022-11').write_text(json.dumps(comment) + '\n', encoding='utf-8')
    (comments / 'RC_2022-12').write_text(json.dumps(comment) + '\n', encoding='utf-8')
    (comments / 'RC_2023-01').write_text(json.dumps(comment) + '\n', encoding='utf-8')
    (submissions / 'RS_2022-11').write_text(json.dumps(submission) + '\n', encoding='utf-8')
    (submissions / 'RS_2022-12').write_text(json.dumps(submission) + '\n', encoding='utf-8')
    (submissions / 'RS_2023-01').write_text(json.dumps(submission) + '\n', encoding='utf-8')
    user_file = tmp_path / 'users.txt'
    user_file.write_text('AutoModerator\nalice\n', encoding='utf-8')

    moves = []
    events = []

    def fake_rclone(arguments, capture_output=False):
        listing = '\n'.join(path.name for path in remote_root.iterdir())
        return SimpleNamespace(returncode=0, stdout=listing)

    class FakeOffload:
        def __init__(self, pairs, month):
            self.pairs = pairs
            self.month = month
            self.args = ['fake-rclone']
            self.finished = False

        def wait(self):
            if not self.finished:
                events.append(('offload-finish', self.month))
                moves.extend(str(destination) for _, destination in self.pairs)
                for source, destination in self.pairs:
                    source_path = Path(str(source).rstrip('/'))
                    destination_path = Path(destination)
                    destination_path.mkdir(parents=True, exist_ok=True)
                    if source_path.is_dir():
                        for child in list(source_path.iterdir()):
                            shutil.move(str(child), str(destination_path / child.name))
                    else:
                        shutil.move(str(source_path), str(destination_path / source_path.name))
                self.finished = True
            return 0

    def fake_start_move_pairs(pairs):
        marker = next(str(source) for source, _ in pairs if '_otacon_history_' in str(source))
        month = re.search(r'_otacon_history_(\d{4}-\d{2})_', marker).group(1)
        events.append(('offload-start', month))
        return FakeOffload(pairs, month)

    original_process_month = main_module.process_month

    def track_process_month(month, *args):
        events.append(('scan', month[-7:]))
        return original_process_month(month, *args)

    monkeypatch.setattr(history_runner, '_run_rclone', fake_rclone)
    monkeypatch.setattr(history_runner, '_start_move_pairs', fake_start_move_pairs)
    monkeypatch.setattr(main_module, 'process_month', track_process_month)
    config = SimpleNamespace(
        comments_input=comments,
        submissions_input=submissions,
        user_file=user_file,
        output_root=output_root,
        remote_root=str(remote_root),
        state_file=output_root / '.completed',
        pause_file=output_root / '.pause_requested',
        user_list_id='test-list',
        time_to='2022-12',
        worker_index=0,
        worker_count=1,
        offload_command=None,
    )

    history_runner._run(config)

    comment_outputs = list((remote_root / 'comments').glob('*.jsonl'))
    submission_outputs = list((remote_root / 'submissions').glob('*.jsonl'))
    comment_results = [json.loads(path.read_text(encoding='utf-8')) for path in comment_outputs]
    submission_results = [json.loads(path.read_text(encoding='utf-8')) for path in submission_outputs]
    assert len(comment_results) == len(submission_results) == 2
    assert all(result['author'] == 'AutoModerator' for result in comment_results)
    assert all(result['_otacon_type'] == 'comment' for result in comment_results)
    assert all(result['selftext'] == 'A submission' for result in submission_results)
    assert all(result['_otacon_type'] == 'post' for result in submission_results)
    assert events.index(('scan', '2022-12')) < events.index(('offload-finish', '2022-11'))
    assert moves == [
        str(remote_root / 'submissions'), str(remote_root / 'comments'), str(remote_root),
        str(remote_root / 'submissions'), str(remote_root / 'comments'), str(remote_root),
    ]
    assert config.state_file.read_text(encoding='utf-8').splitlines() == ['2022-11', '2022-12']

def test_thread_index_chunked_merge_is_sorted_filtered_and_resumable(tmp_path):
    import random
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    from otacon import thread_index

    rng = random.Random(0)
    (tmp_path / 'parts').mkdir()
    rows, next_id = [], 1000
    for month in ('2010-01', '2010-02', '2010-03'):
        cols = {f.name: [] for f in thread_index.SCHEMA}
        for _ in range(500):
            on_sub = not rows or rng.random() < 0.3
            parent = rng.randrange(1, 300) if on_sub else rng.choice(rows)[0]
            row = (next_id, parent, on_sub, 1, next_id, rng.choice(['AskReddit', 'pics', 'dropped']))
            rows.append(row)
            for f, v in zip(thread_index.SCHEMA, row):
                cols[f.name].append(v)
            next_id += rng.randrange(1, 5)
        pq.write_table(pa.table(cols, schema=thread_index.SCHEMA), tmp_path / 'parts' / f'{month}.parquet')
    ancient_reply = (next_id, 1, False, 1, next_id, 'pics')  # below the quantile start of the parent range
    rows.append(ancient_reply)
    pq.write_table(pa.table({f.name: [v] for f, v in zip(thread_index.SCHEMA, ancient_reply)}, schema=thread_index.SCHEMA),
                   tmp_path / 'parts' / '2010-04.parquet')
    subs = tmp_path / 'subs.txt'
    subs.write_text('askreddit\npics\n')
    kept = [r for r in rows if r[5] != 'dropped']
    bounds = thread_index.merge_bounds(duckdb.connect(), f"read_parquet('{tmp_path}/parts/*.parquet')")
    assert bounds['by_parent'][0][0] is False
    bounds['by_parent'][0] = (False, 500, bounds['by_parent'][0][2])  # as if the quantile start skipped the outlier
    (tmp_path / 'merge_bounds.json').write_text(json.dumps(bounds))

    thread_index.merge(tmp_path, subs, '1GiB', tmp_path / 'spill', chunks=4, memory_limit='1GB', threads=2)
    first_chunk = tmp_path / 'by_parent' / 'part-0-000.parquet'
    first_chunk.unlink()  # simulate an interruption after the other chunks finished
    thread_index.merge(tmp_path, subs, '1GiB', tmp_path / 'spill', chunks=4)
    with pytest.raises(SystemExit):  # chunk count is pinned by merge_bounds.json
        thread_index.merge(tmp_path, subs, '1GiB', tmp_path / 'spill', chunks=5)

    def read(name):
        files = sorted((tmp_path / name).glob('*.parquet'))
        return [tuple(r) for f in files for r in duckdb.sql(f"SELECT * FROM '{f}'").fetchall()]

    assert len(list((tmp_path / 'by_parent').glob('*.parquet'))) == 8
    assert read('by_id') == sorted(kept, key=lambda r: r[0])
    by_parent = read('by_parent')
    assert [(r[2], r[1]) for r in by_parent] == sorted((r[2], r[1]) for r in kept)
    assert sorted(by_parent) == sorted(kept)

    target = next(r for r in kept if not r[2])
    comments, _ = thread_index.ThreadIndex(tmp_path).thread([thread_index.to_base36(target[0])])
    assert thread_index.to_base36(target[1]) in comments or target[1] not in {r[0] for r in kept}


def test_forest_extraction_keeps_lines_verbatim_checks_candidates_and_resumes(tmp_path, capsys):
    import zstandard
    from otacon import forest_extraction as fe

    comments, submissions, ids, out = (tmp_path / d for d in ('comments', 'submissions', 'ids', 'out'))
    for d in (comments, submissions, ids / 'comment_ids'):
        d.mkdir(parents=True)

    def line(spaced=False, **fields):
        return json.dumps(fields, separators=None if spaced else (',', ':')).encode()

    def dump(path, lines):
        path.write_bytes(zstandard.ZstdCompressor().compress(b'\n'.join(lines) + b'\n'))

    alice, deleted = line(id='aaaaa', author='Alice', title='t'), line(id='bbbbb', author='[deleted]')
    carol = line(spaced=True, id='ccccc', author='Carol')  # not compact: parsed in full
    crosspost = line(id='zzzzz', author='Zed', crosspost_parent_list=[{'id': 'aaaaa', 'author': 'Alice'}])
    dump(submissions / 'RS_2009-12.zst', [carol])
    dump(submissions / 'RS_2010-01.zst', [line(id='eeeee', author='Alice'), alice, crosspost, deleted,
                                          line(id='aaaaa', author='Alice', title='again'), b'{"id":"aaaaa","tit'])
    dump(submissions / 'RS_2010-02.zst', [line(id='fffff', author='Alice')])
    (ids / 'submission_ids.txt').write_text('aaaaa\nbbbbb\nccccc\nddddd\n')
    (ids / 'comment_ids' / 'RC_2010-01.txt').write_text('c000001\nc000002\nc000003\nc000009\n')
    (ids / 'comment_ids' / 'RC_2010-02.txt').write_text('c000010\n')
    (ids / 'manifest.tsv').write_text('file\tids\ncomment_ids/RC_2010-01.txt\t4\ncomment_ids/RC_2010-02.txt\t1\n'
                                      'submission_ids.txt\t4\n')

    def comment(cid, author, link, spaced=False, **extra):
        return line(spaced, id=cid, author=author, link_id=f't3_{link}', **extra)

    old_by_carol = comment('c00000x', 'Carol', 'ccccc')
    dump(comments / 'RC_2009-12.zst', [comment('c00000y', 'Dave', 'ccccc'), old_by_carol])
    listed, by_alice, listed_by_alice = (comment('c000001', 'Bob', 'aaaaa'), comment('c000004', 'Alice', 'aaaaa'),
                                         comment('c000009', 'Alice', 'aaaaa'))
    dump(comments / 'RC_2010-01.zst', [
        listed, by_alice, listed_by_alice, listed,
        comment('c000005', 'alice', 'aaaaa'),  # authors compare case-sensitively
        comment('c000006', 'Alice', 'eeeee'),  # not a listed thread
        comment('c000007', '[deleted]', 'bbbbb'),
        comment('c00000a', 'Bob', 'aaaaa', all_awardings=[{'id': 'c000002'}]),  # nested id of a listed comment
    ])
    neighbour, own, spaced_by_alice = (comment('c000003', 'Bob', 'aaaaa'), comment('c000010', 'Eve', 'bbbbb'),
                                       comment('c00000b', 'Alice', 'aaaaa', spaced=True))
    dump(comments / 'RC_2010-02.zst', [neighbour, own, spaced_by_alice])

    def run(only=None):
        fe.run(SimpleNamespace(ids=ids, output=out, comments_input=comments, submissions_input=submissions,
                               workers=1, only=only, min_free_gb=0))

    def lines_of(name):
        data = (out / name[:2] / f'{name}.ndjson.zst').read_bytes()
        return zstandard.ZstdDecompressor().decompressobj().decompress(data).splitlines()

    def stats(name):
        return json.loads((out / 'stats' / f'{name}.json').read_text())

    run(['RS_2010-01', 'RC_2010-01'])  # a test run: the submitter map has 1 of 3 RS files
    assert lines_of('RS_2010-01') == [alice, deleted]
    assert stats('RS_2010-01') | {'duplicates': 1, 'undecodable': 1, 'found': 2} == stats('RS_2010-01')
    assert stats('RC_2010-01')['submitter_map'] == 'partial (1 of 3 RS files)'
    rs_finished = stats('RS_2010-01')['finished']

    (out / fe.PAUSE_NAME).touch()
    run()
    assert not (out / 'RS' / 'RS_2009-12.ndjson.zst').exists()
    (out / fe.PAUSE_NAME).unlink()

    run()
    assert stats('RS_2010-01')['finished'] == rs_finished  # not redone
    assert lines_of('RS_2009-12') == [carol] and lines_of('RS_2010-02') == []
    assert (out / 'submitter_map.tsv').read_text() == ('submission_id\tauthor\tdump_file\naaaaa\tAlice\tRS_2010-01\n'
                                                      'bbbbb\t[deleted]\tRS_2010-01\nccccc\tCarol\tRS_2009-12\n')
    assert (out / 'missing' / 'submissions.txt').read_text() == 'ddddd\n'
    assert lines_of('RC_2009-12') == [old_by_carol]
    assert lines_of('RC_2010-01') == [listed, by_alice, listed_by_alice]
    assert lines_of('RC_2010-02') == [neighbour, own, spaced_by_alice]
    rc1, rc2 = stats('RC_2010-01'), stats('RC_2010-02')
    assert rc1['submitter_map'] == 'complete'  # redone with the complete map
    assert (rc1['expected'], rc1['found'], rc1['missing'], rc1['submitter_comments'], rc1['duplicates']) == (4, 2, 2, 1, 1)
    assert (rc2['found'], rc2['neighbour_ids_found'], rc2['submitter_comments'], rc2['full_parses']) == (1, 1, 1, 1)
    assert (out / 'missing' / 'RC_2010-01.txt').read_text() == 'c000002\nc000003\n'
    assert len((out / 'report.tsv').read_text().splitlines()) == 7

    mtime = (out / 'RC' / 'RC_2010-02.ndjson.zst').stat().st_mtime_ns
    run()
    assert (out / 'RC' / 'RC_2010-02.ndjson.zst').stat().st_mtime_ns == mtime
    fe.summary(SimpleNamespace(ids=ids, output=out))
    assert '1 in a neighbouring one, 1 missing' in capsys.readouterr().out
    assert (out / 'missing_comments.tsv').read_text() == 'list_file\tcomment_id\nRC_2010-01\tc000002\n'

    frame = zstandard.ZstdCompressor().compress(b'{"a":1}\n' * 1000)
    (tmp_path / 'two_frames.zst').write_bytes(frame + frame)
    assert sum(len(piece) for piece, _ in fe.read_chunks(tmp_path / 'two_frames.zst')) == 16000
    (tmp_path / 'truncated.zst').write_bytes(frame[:-10])
    with pytest.raises(EOFError):
        list(fe.read_chunks(tmp_path / 'truncated.zst'))
