import json
import random

import pytest
from zstandard import ZstdCompressor

from otacon.pushshift_handling import (
    read_redditfile,
    read_lines_zst_prefiltered,
    _encode_needles,
)


def _make_entries():
    rng = random.Random(0)
    authors = ['Smogshaik', 'smogshaik', 'SMOGSHAIK', 'someone_else', 'smog', 'shaik', 'xSmogshaikx', '[deleted]']
    entries = []
    for i in range(500):
        author = rng.choice(authors)
        body = rng.choice([
            'plain text',
            'mentions u/Smogshaik in the body',
            'ünïcödé ✓ 漢字 ' * rng.randint(0, 5),
            'line\nbreak inside "quotes"',
        ])
        entries.append({'id': f'c{i}', 'author': author, 'body': body, 'subreddit': 'test'})
    return entries


def _write_zst(path, entries, trailing_newline=True):
    raw = '\n'.join(json.dumps(e, ensure_ascii=False) for e in entries)
    if trailing_newline:
        raw += '\n'
    with open(path, 'wb') as f:
        f.write(ZstdCompressor().compress(raw.encode('utf-8')))


def _matching_ids(entries_iter, name):
    return [e['id'] for e in entries_iter if e['author'].lower() == name]


@pytest.mark.parametrize('trailing_newline', [True, False])
def test_prefiltered_zst_matches_stock_reader(tmp_path, trailing_newline):
    entries = _make_entries()
    path = str(tmp_path / 'RC_2010-01.zst')
    _write_zst(path, entries, trailing_newline)

    expected = [e['id'] for e in entries if e['author'].lower() == 'smogshaik']
    got = _matching_ids(read_redditfile(path, prefilter_names={'smogshaik'}), 'smogshaik')
    assert got == expected

    # the stock reader agrees, except that it drops a final line lacking a trailing newline
    stock = _matching_ids(read_redditfile(path), 'smogshaik')
    if trailing_newline or entries[-1]['author'].lower() != 'smogshaik':
        assert stock == expected


@pytest.mark.parametrize('chunk_size', [1, 7, 64, 333, 4096])
def test_prefiltered_zst_chunk_boundaries(tmp_path, chunk_size):
    entries = _make_entries()
    path = str(tmp_path / 'RC_2010-01.zst')
    _write_zst(path, entries)

    needles = _encode_needles({'smogshaik'}, case_sensitive=False)
    lines = list(read_lines_zst_prefiltered(path, needles, case_sensitive=False, chunk_size=chunk_size))
    parsed = [json.loads(l) for l in lines]

    # every yielded line is a complete, parseable entry containing the name, in file order, without duplicates
    expected = [e for e in entries if 'smogshaik' in json.dumps(e, ensure_ascii=False).lower()]
    assert parsed == expected


def test_prefiltered_case_sensitive(tmp_path):
    entries = _make_entries()
    path = str(tmp_path / 'RC_2010-01.zst')
    _write_zst(path, entries)

    got = [e['id'] for e in read_redditfile(path, prefilter_names={'Smogshaik'}, case_sensitive=True) if e['author'] == 'Smogshaik']
    assert got == [e['id'] for e in entries if e['author'] == 'Smogshaik']


def test_prefiltered_plain_file(tmp_path):
    entries = _make_entries()
    path = str(tmp_path / 'RC_2010-01')
    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(json.dumps(e, ensure_ascii=False) for e in entries) + '\n')

    got = _matching_ids(read_redditfile(path, prefilter_names={'smogshaik'}), 'smogshaik')
    assert got == [e['id'] for e in entries if e['author'].lower() == 'smogshaik']
