#!/usr/bin/env zsh
#
# Resumable wrapper around `otacon.main` for the "all_theys_local" extraction.
#
# WHY THIS EXISTS:
# otacon.main processes months sequentially inside a single Python process, and
# keeps a file handle open into the input directory while a month is being read.
# That makes it unsafe to SIGSTOP the process and eject the drive mid-run: the
# volume can't unmount cleanly while a file descriptor is open, and resuming
# after a forced eject would hit I/O errors.
#
# This script instead runs ONE month per subprocess call (via --time_from/--time_to
# both set to the same month), records completed months in a state file, and only
# checks for a pause request in between months -- a point where no file on the
# external drive is open. This makes eject-and-resume fully safe, without touching
# the otacon package itself.
#
# Months are processed newest-first (--reverse_order), since the most recent
# months in this dataset are the largest and slowest to process. This front-loads
# the bulky months into the initial ~65h uninterrupted window, leaving the
# smaller/faster months for after the first pause.
#
# USAGE:
#   ./run_resumable_theys.sh
#
# TO PAUSE (safe to eject the drive afterwards):
#   touch "$OUTPUT_DIR/.pause_requested"
#   # wait for the script to print "Pause requested..." and exit on its own,
#   # i.e. until the CURRENT month finishes (do not eject before that).
#
# TO RESUME (after reconnecting the drive):
#   ./run_resumable_theys.sh
#   # it automatically clears the pause flag and continues from the next
#   # not-yet-completed month.

set -euo pipefail

# ---- configuration (mirrors past_commands/all_theys_local.txt) ----
REPO_DIR="/Users/uni/Documents/GitHub/otacon"
INPUT_DIR="/Volumes/rdisk2/redditdata/comments"
OUTPUT_DIR="/Volumes/rdisk2/redditdata/output/project3/all_theys"
TIME_TO="2021-10"
COMMENT_REGEX="(?:(?<=^)|(?<=\s)|(?<=\W))(?:thei|they|them|their|theirn)(?:'?(?:re|ve|d|ll|s|self|selves|selfs|selve|selvs|selv))*(?=\$|(?=\s)|(?=\W))"

STATE_FILE="$OUTPUT_DIR/.completed_months"
PAUSE_FILE="$OUTPUT_DIR/.pause_requested"
MERGED_NAME="all_theys_merged.jsonl"

mkdir -p "$OUTPUT_DIR"
touch "$STATE_FILE"
rm -f "$PAUSE_FILE"   # clear any stale pause request on a fresh/resumed run

cd "$REPO_DIR"

if [[ ! -d "$INPUT_DIR" ]]; then
    echo "ERROR: input directory $INPUT_DIR is not reachable. Is the external HDD connected?"
    exit 1
fi

# all month files/dirs present in the input dir, sorted newest-first (mirrors --reverse_order:
# the huge recent months get processed first, so the 65h budget clears them before pausing)
months=("${(@f)$(ls "$INPUT_DIR" | grep -E '^(RC|RS)_[0-9]{4}-[0-9]{2}' | sort -r)}")

for month in "${months[@]}"; do
    m=$(echo "$month" | grep -oE '[0-9]{4}-[0-9]{2}')

    # respect the original --time_to cutoff
    if [[ "$m" > "$TIME_TO" ]]; then
        continue
    fi

    # skip months already completed in a previous run
    if grep -qxF "$month" "$STATE_FILE"; then
        continue
    fi

    # safe checkpoint: only checked between months, never mid-file
    if [[ -e "$PAUSE_FILE" ]]; then
        echo "Pause requested. Stopping cleanly after month $(tail -n1 "$STATE_FILE" 2>/dev/null || echo '(none yet)')."
        echo "It is now safe to eject the drive. Rerun this script to resume."
        exit 0
    fi

    if [[ ! -e "$INPUT_DIR/$month" ]]; then
        echo "ERROR: expected input file $INPUT_DIR/$month is missing. Is the drive connected?"
        exit 1
    fi

    echo "=== Processing $month ==="
    poetry run python -m otacon.main \
        --input "$INPUT_DIR" \
        --output "$OUTPUT_DIR" \
        --time_from "$m" \
        --time_to "$m" \
        --commentregex "$COMMENT_REGEX" \
        --no_stats \
        --return_all \
        --reverse_order \
        --no_cleanup

    echo "$month" >> "$STATE_FILE"
done

echo "All months processed. Merging per-month output files into $MERGED_NAME ..."
poetry run python -c "
from otacon.finalize import cleanup
cleanup('$OUTPUT_DIR', extraction_name='$MERGED_NAME')
"
echo "Done."
