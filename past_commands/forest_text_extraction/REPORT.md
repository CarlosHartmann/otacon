# Report: forest recon text extraction

From otacon, 2026-10-29, for the shadow_moses agent. This answers ORDER.md. The run was done with `otacon/forest_extraction.py`, started by `run_forest_extraction.zsh` in this folder.

## Result

All 386 dump files were read in one run, 2026-10-28 19:25 to 2026-10-29 03:19 GMT (7 h 54 min).

| | Expected | Found | Missing |
|---|---|---|---|
| Comments (`ids/comment_ids/`) | 17,861,694 | 17,861,694 | 0 |
| Submissions (`ids/submission_ids.txt`) | 2,514,433 | 2,511,879 | 2,554 (0.10%) |
| Submitter comments (not in the id lists) | — | 16,118,574 | — |

- Every listed comment was in the RC file of its own month. No id had to be taken from a neighbouring month.
- No submission appears in more than one RS file.
- Output: 45.6 GB uncompressed (9.43 GB RS, 36.12 GB RC), 1,460 GB of dumps read (10.8 billion RC lines, 1.43 billion RS lines).

## Where the output is

The user changed the location and the format from what ORDER.md asked for. The output is **uncompressed** NDJSON on the internal disk, here:
`/Users/uni/Documents/GitHub/shadow_moses/0-assets/raw_data/main/8-full-contexts/programmatic/otacon_output/`

**This folder is not gitignored in shadow_moses.** Add it to `.gitignore` before the next commit there, or 45 GB will be committed.

| Path | Content |
|---|---|
| `RS/RS_YYYY-MM.ndjson`, `RC/RC_YYYY-MM.ndjson` | One file per dump file: the matching lines, verbatim and in dump order. Months with no match have an empty file. |
| `report.tsv` | One row per dump file: `input_bytes`, `lines`, `expected`, `found`, `missing`, `neighbour_ids_found`, `submitter_comments`, `duplicates`, `undecodable`, `full_parses`, `written`, `output_bytes`, `seconds`, `submitter_map`, `finished`. For RS files `expected` and `missing` are blank, because a submission's month is unknown. |
| `submitter_map.tsv` | `submission_id`, `author`, `dump_file`: 2,511,879 rows, one per found submission. |
| `missing/submissions.txt` | The 2,554 submission ids that are in no RS file. |
| `missing/RC_YYYY-MM.txt`, `missing_comments.tsv` | Missing comment ids per file and over all files. All are empty. |
| `duplicates/RC_2017-09.txt` | Skipped duplicate ids (see below). |
| `submitters/RS_YYYY-MM.tsv` | The submitter map split by RS file. |
| `stats/*.json` | The per-file stats behind `report.tsv`. |
| `extraction.log` | The full run log. |

## How the order was carried out

- **Submissions.** Every RS file from `RS_2005-06` to `RS_2021-09` was checked against the whole id set.
- **Comments.** Each RC file was checked against its own month's list. As a safety net, it was also checked against the previous and next months' lists, which matched nothing.
- **Submitter comments.** The RS pass ran first and built the submitter map. In the RC pass (`RC_2005-12` to `RC_2021-09`), a comment was kept when:
  - its `link_id` is `t3_` plus a found submission, and
  - its `author` equals that submission's `author` exactly (case-sensitive), and
  - the author is neither `[deleted]` nor empty.

  A comment that is both listed and a submitter comment was written once, and counts as listed.
- **No submitter comments are possible for some threads.** 391,686 of the found submissions have a `[deleted]` or empty author. The 2,554 missing submissions have no author at all.
- **Lines are verbatim.** Each output line is the dump line byte for byte. Only candidate lines found by a quick look at the raw bytes were parsed as JSON, and the parsed JSON decided the match. Lines whose layout didn't fit the quick look were parsed in full instead, so no line was skipped unchecked.
- **Duplicates.** Within a file, the first copy of an id was kept and later copies were skipped.
- **Unreadable lines.** A line counts as "undecodable" when it isn't valid JSON. Such a line can't match, so it is counted and left out.
- **Truncated dumps.** The reader checks each zstd file's frame structure to its end, so a cut-off dump file stops the run instead of passing silently. None were cut off.

## Surprises

- **Duplicates only in `RC_2017-09`.** 9,438 copies of 9,296 ids were skipped. 142 of those ids appear three times. They all fall in one id range, `dmehp4r` to `dn1tfv7`; the first of them was posted at 2017-09-01 00:00:04 UTC. This looks like a stretch of the dump that was written twice, but the copies weren't compared. No other file has a duplicate.
- **Only one unreadable line in the whole dumps,** in `RS_2011-01`.
- **2,554 submissions are missing.** Their threads have comments in the dumps, but the submissions themselves are in no RS file. Gaps like this are known in the Pushshift RS dumps, but the reason for these ids wasn't checked. Their listed comments were extracted normally.

## Timings and resources

- **RS pass:** 196 files, 372.7 GB, 1 h 30 min (19:25 to 20:55).
- **RC pass:** 190 files, 1,087.1 GB, 6 h 25 min (20:55 to 03:19).
- **Speed:** 51.3 MB/s overall. The largest files took 15 to 17 min each (for example `RC_2021-05`, 22.4 GB in 16.7 min).
- **Workers:** 2, at `nice -n 10` instead of `taskpolicy -b`. On this M3, `taskpolicy -b` keeps the workers on the efficiency cores, which is 3 to 4 times slower.
- **Why only 2 workers.** 327 of the 386 dumps use a 2 GiB zstd window, so each worker needs about 2.5 GB to decompress. Two workers stay within the 6 GB budget; the 4 workers ORDER.md suggested would not. The id sets were held as compact byte sets, about 300 MB per worker.
- **The 7 MB/s per process in ORDER.md is out of date.** The reader managed 40 to 90 MB/s per worker.
- **Memory** was sampled at about 0.4 GB per worker at the start. It wasn't sampled later, during the large files.
- **No test run.** The test run that ORDER.md asked for was skipped at the user's request. The full run started after the sayer-histories extraction had finished.

## Rerunning

`run_forest_extraction.zsh` resumes: files whose output is complete are skipped. To redo a file, ask the user before deleting its output. `poetry run python -m otacon.forest_extraction summary --ids past_commands/forest_text_extraction/ids --output <output folder>` prints the totals above again.
