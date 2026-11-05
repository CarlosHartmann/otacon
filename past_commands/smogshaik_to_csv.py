"""Merge per-month otacon --return_all JSONL parts into one JSONL + one ;-separated CSV per dataset."""
import csv, glob, json, os, sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from otacon.main import filter as soft_filter

root = sys.argv[1]
COMMON = ['type', 'year', 'month', 'id', 'text', 'subreddit', 'score', 'user', 'flairtext', 'timestamp_utc', 'permalink']
EXTRA = {'comments': ['parent_id', 'link_id'], 'submissions': ['title', 'url']}

for ds in ['comments', 'submissions']:
    entries = {}
    for path in sorted(glob.glob(os.path.join(root, 'parts', ds, '*', '*.jsonl'))):
        # filtered-out files hold entries flagged by otacon's soft filter; they are Smogshaik's too
        with open(path, encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    e = json.loads(line)
                    entries.setdefault(e['id'], e)  # drop duplicates across months
    rows = sorted(entries.values(), key=lambda e: int(float(e['created_utc'])))
    with open(os.path.join(root, f'Smogshaik_{ds}.jsonl'), 'w', encoding='utf-8') as f:
        for e in rows:
            f.write(json.dumps(e, ensure_ascii=False) + '\n')
    with open(os.path.join(root, f'Smogshaik_{ds}.csv'), 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        w.writerow(COMMON + EXTRA[ds] + ['filter_reason'])
        for e in rows:
            w.writerow([e.get('_otacon_type'), e.get('_otacon_year'), e.get('_otacon_month'), e['id'],
                        e.get('body') if ds == 'comments' else e.get('selftext'),
                        e.get('subreddit'), e.get('score'), e.get('author'), e.get('author_flair_text'),
                        int(float(e['created_utc'])), e.get('_otacon_permalink')]
                       + [e.get(k) for k in EXTRA[ds]]
                       + [soft_filter(e, None)[1]])  # recomputed so all months are flagged consistently
    print(f'{ds}: {len(rows)} entries')
