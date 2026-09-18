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
#
# OFFLOADING TO NAS:
# Each month's output is moved (not copied) to the NAS via rclone-custom as soon
# as that month finishes, freeing the local drive before the next month starts.
# rclone-custom is a zsh function defined in ~/.zshrc, so that file is sourced
# below before enabling `set -e`, so a harmless startup hiccup there can't abort
# this script.

# load the user's zsh config so the rclone-custom function is available
source ~/.zshrc

set -euo pipefail

if ! typeset -f rclone-custom > /dev/null; then
    echo "ERROR: rclone-custom function not found after sourcing ~/.zshrc."
    exit 1
fi

# ---- configuration (mirrors past_commands/all_theys_local.txt) ----
REPO_DIR="/Users/uni/Documents/GitHub/otacon"
INPUT_DIR="/Volumes/rdisk2/redditdata/comments"
OUTPUT_DIR="/Volumes/rdisk2/redditdata/output/project3/all_theys"
TIME_TO="2021-10"
COMMENT_REGEX="(?:(?<=^)|(?<=\s)|(?<=\W))(?:thei|they|them|their|theirn)(?:'?(?:re|ve|d|ll|s|self|selves|selfs|selve|selvs|selv))*(?=\$|(?=\s)|(?=\W))"
REMOTE_DEST="almazen:reddit_data/output/project3/all_theys"

STATE_FILE="$OUTPUT_DIR/.completed_months"
PAUSE_FILE="$OUTPUT_DIR/.pause_requested"

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

    # snapshot before running so we can identify exactly which files this month produced
    before_files=("${(@f)$(ls -1 "$OUTPUT_DIR" 2>/dev/null)}")

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

    after_files=("${(@f)$(ls -1 "$OUTPUT_DIR" 2>/dev/null)}")
    new_files=("${(@)after_files:|before_files}")

    echo "=== Offloading $month output to NAS ==="
    for f in "${new_files[@]}"; do
        # skip bookkeeping files (state/pause flags), only ship actual result files
        [[ "$f" == .* ]] && continue
        rclone-custom move "$OUTPUT_DIR/$f" "$REMOTE_DEST"
    done

    # only mark the month complete once its output has been safely moved off-drive
    echo "$month" >> "$STATE_FILE"
done

echo "All months processed and offloaded to $REMOTE_DEST."
echo "Note: each month's output was moved off individually, so no local merge step runs here."
echo "Merge/concatenate the per-month .jsonl files on the NAS side if a single combined file is needed."
