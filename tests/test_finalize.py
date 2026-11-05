import json
import os
import sys

import pytest
from zstandard import ZstdCompressor

import otacon.main as otacon_main


def _write_month(directory, month, entries):
    raw = ''.join(json.dumps(e) + '\n' for e in entries)
    with open(os.path.join(directory, f'RC_{month}.zst'), 'wb') as f:
        f.write(ZstdCompressor().compress(raw.encode('utf-8')))


def _entry(i, author, body, ts):
    return {'id': f'c{i}', 'author': author, 'body': body, 'subreddit': 'test', 'score': 1,
            'author_flair_text': None, 'created_utc': ts, 'link_id': 't3_x', 'parent_id': 't3_x'}


@pytest.fixture
def dataset(tmp_path):
    comments = tmp_path / 'comments'
    comments.mkdir()
    _write_month(comments, '2010-01', [_entry(1, 'Smogshaik', 'hello', 1262304000),
                                       _entry(2, 'other', 'hi', 1262304001),
                                       _entry(3, 'Smogshaik', "beep, I'm a bot", 1262304002)])
    _write_month(comments, '2010-02', [_entry(4, 'smogshaik', 'again', 1264982400)])
    out = tmp_path / 'out'
    out.mkdir()
    (out / 'unrelated_results_2009-01.jsonl').write_text('{"keep": "me"}\n')
    return comments, out


def _run(monkeypatch, comments, out, *extra):
    otacon_main.hash_set.clear()
    monkeypatch.setattr(sys, 'argv', ['otacon', '--input', str(comments), '--output', str(out),
                                      '--src', 'user', '--name', 'Smogshaik', '--return_all', '--no_stats', *extra])
    otacon_main.main()


def _ids(path):
    return [json.loads(line)['id'] for line in open(path, encoding='utf-8')]


def test_cleanup_renames_only_this_runs_files(monkeypatch, dataset):
    comments, out = dataset
    _run(monkeypatch, comments, out)
    files = sorted(os.listdir(out))

    # the unrelated file is untouched
    assert 'unrelated_results_2009-01.jsonl' in files
    assert (out / 'unrelated_results_2009-01.jsonl').read_text() == '{"keep": "me"}\n'

    results = [f for f in files if f.startswith('comment_extraction') and '_filtered-out_' not in f]
    review = [f for f in files if '_filtered-out_' in f]
    assert len(results) == 1 and len(review) == 1
    # named after the whole timeframe, not just the first month
    assert '_from_2010-1_up_to_2010-2_' in results[0] and 'RC_2010-01' not in results[0]
    assert review[0] == results[0][:-len('.jsonl')] + '_filtered-out_matches.jsonl'

    assert _ids(out / results[0]) == ['c1', 'c4']
    assert _ids(out / review[0]) == ['c3']


def test_no_cleanup_keeps_original_names(monkeypatch, dataset):
    comments, out = dataset
    _run(monkeypatch, comments, out, '--no_cleanup')
    results = [f for f in os.listdir(out) if f.startswith('comment_extraction') and '_filtered-out_' not in f]
    assert len(results) == 1 and results[0].endswith('_RC_2010-01.zst.jsonl')
    assert _ids(out / results[0]) == ['c1', 'c4']


def test_cleanup_csv_output_is_not_rewritten(monkeypatch, dataset):
    comments, out = dataset
    _run(monkeypatch, comments, out)  # warm-up with JSONL so both formats share a directory
    otacon_main.hash_set.clear()
    monkeypatch.setattr(sys, 'argv', ['otacon', '--input', str(comments), '--output', str(out),
                                      '--src', 'user', '--name', 'Smogshaik', '--no_stats'])
    otacon_main.main()
    csvs = [f for f in os.listdir(out) if f.endswith('.csv') and '_filtered-out_' not in f]
    assert len(csvs) == 1
    lines = open(out / csvs[0], encoding='utf-8').read().splitlines()
    # header + 2 rows, with the columns exactly as extract() wrote them (no duplicated type/year/month)
    assert lines[0].split(';')[:4] == ['type', 'year', 'month', 'id']
    assert len(lines) == 3 and lines[1].startswith('comment;2010;1;c1;')
    # the JSONL results of the first run are still there
    assert len([f for f in os.listdir(out) if f.endswith('.jsonl')]) == 3


def test_csv_review_file_keeps_header_across_months(monkeypatch, tmp_path):
    comments = tmp_path / 'comments'
    comments.mkdir()
    _write_month(comments, '2010-01', [_entry(1, 'Smogshaik', 'hello', 1262304000)])  # nothing filtered
    _write_month(comments, '2010-02', [_entry(2, 'Smogshaik', "i am a bot", 1264982400)])
    out = tmp_path / 'out'
    out.mkdir()
    otacon_main.hash_set.clear()
    monkeypatch.setattr(sys, 'argv', ['otacon', '--input', str(comments), '--output', str(out),
                                      '--src', 'user', '--name', 'Smogshaik', '--no_stats'])
    otacon_main.main()
    review = [f for f in os.listdir(out) if '_filtered-out_' in f]
    assert len(review) == 1
    lines = open(out / review[0], encoding='utf-8').read().splitlines()
    assert lines[0].startswith('type;year;month;id;') and len(lines) == 2 and ';c2;' in lines[1]
