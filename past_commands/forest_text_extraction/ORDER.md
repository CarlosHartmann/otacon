# Order: text extraction for the shadow_moses "forest recon"

From the shadow_moses agent (repo `/Users/uni/Documents/GitHub/shadow_moses`, step `0-assets/raw_data/main/8-full-contexts/programmatic/`), 2026-10-28. Please build and run an extraction script in otacon that takes the original JSON lines of a known set of comments and submissions out of the Pushshift dumps, in one pass over the dumps.

## Background

shadow_moses studies how Reddit users refer to "OP". From the thread index (`/Volumes/rdisk2/redditdata/index`, built by otacon), it has built 3,006,374 "discussions" (comment trees around comments that mention "OP") in 2,514,433 threads. The index has structure only, so their texts, authors and flairs still have to come from the dumps. The ids are final; nothing else needs to be derived from them.

## What to extract

1. **Submissions**: the 2,514,433 ids in `ids/submission_ids.txt`, from `RS_2005-06` to `RS_2021-09` (347 GB). The index has no submission times, so the month of a submission is unknown. Every RS file must be checked against the whole set.
2. **Comments**: the 17,861,694 ids in `ids/comment_ids/RC_YYYY-MM.txt`, from `RC_2010-01` to `RC_2021-09` (1.08 TB). They are split by the UTC month of `created_utc`, which is the dump file holding each comment, so each RC file only needs its own list (130,000 ids on average, at most a few hundred thousand).
3. **The submitters' comments**: every comment in one of those 2,514,433 threads whose `author` is the submission's `author`, wherever it is in the thread and whether or not it is in the id lists. They show whether the thread knew OP's gender.
   - This needs the submission authors, so the RS pass must come first.
   - Exclude `[deleted]` and empty authors. Compare authors exactly, case-sensitive.
   - Thread membership comes from the comment's `link_id`: `t3_` plus the submission id.
   - Here the RC pass also covers `RC_2005-12` to `RC_2009-12` (2.5 GB), for old threads.

All ids are base 36 without prefix, one per line, sorted, matching the dumps' `id` field. `ids/manifest.tsv` gives the expected count per file.

## Output

- Location: `/Volumes/dissertationhorse/OP_forest_texts/` (2.1 TB free). Expected size about 40 GB uncompressed, much less as zstd.
- **Keep each matching line verbatim**: the original JSON, all fields, nothing parsed out or dropped. Flairs live in `author_flair_text` and, from 2019, in `author_flair_richtext`; `is_submitter` exists from 2018.
- One output file per dump file, e.g. `RS/RS_2015-06.ndjson.zst` and `RC/RC_2015-06.ndjson.zst`. If a comment is both a listed id and a submitter's comment, write it once.
- Remove duplicate ids within a file (forest_recon_index.md Q4: none were found in samples, but possible).
- Per dump file, a small report (e.g. one line in a TSV) with:
  - the ids expected, the ids found and the ids missing; write the missing ids to a file;
  - the submitter comments kept;
  - the duplicates skipped;
  - lines that could not be decoded;
  - the running time.
- The submitter map as a file: submission id → author, from the RS pass.
- At the end, a short `REPORT.md` in this folder with totals, missing counts, timings and any surprises, for the shadow_moses agent.

## How to run it

- **Do not start the full run while the sayer-histories extraction is running.** It must not be stopped, restarted or throttled, and it is already short of memory. Wait until the user says it has finished. Short tests on one or two small months are fine before then, but ask the user first.
- **Memory:** the computer has 16 GB, and last night it swapped heavily when processes together went above it. Keep the whole run, all workers together, under about 6 GB.
  - Use compact id sets. 2.5M submission ids as Python strings take roughly 200–300 MB per process; sorted numpy int64 arrays (`int(id, 36)`) with `searchsorted` take 20 MB.
  - Give each RC worker only its month's list.
- **Speed:** reading is CPU-bound (decompression and JSON) at about 7 MB/s per process (forest_recon_index.md Q6), so several workers per pass pay off up to what the USB disk delivers.
  - With one worker the whole job is about 1.43 TB, roughly 57 h; with 4 workers about 15 h.
  - Matching can be done on the raw line before full JSON parsing, e.g. a quick look at `"id":"..."`, if that is safe.
- **Resumable:** write each output file under a temporary name and rename it when complete. Skip finished files on a rerun.
- **Progress:** log after every dump file: files done, GB done/total, rate, elapsed time, ETA with clock time. Run detached (`nohup caffeinate -ims taskpolicy -b ... & disown`). The user wants to check progress at any time.
- **Test first:** run on one small and one large month (e.g. `RC_2012-03` and `RC_2020-06`, plus their RS files). Report the rate, memory and found/expected counts to the user. Only then start the full run, once sayer has finished and the user agrees.

## Rules (the user's)

- Ask before stopping any process or deleting any data.
- Do not rebuild the index.
- Do not commit the user's uncommitted work in otacon. `ids/` is gitignored in this folder (153 MB); do not commit it.
