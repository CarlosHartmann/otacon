#!/usr/bin/env zsh
# Extract the forest recon comments and submissions (ORDER.md) from the dumps on rdisk2 with
# otacon/forest_extraction.py. Starts itself detached and appends to $OUTPUT_ROOT/extraction.log;
# an email goes out when the run finishes, pauses or fails.
# Run it again to resume: finished dump files are skipped.
# To pause, touch $OUTPUT_ROOT/.pause_requested: the workers finish their current files, then the
# run exits. The next start removes the request.
#
# Usage: run_forest_extraction.zsh [--output DIR] [--workers N] [--only RS_2012-03 RC_2012-03 ...]
#                                  [--allow-with-sayer]
# Progress: tail -n 3 $OUTPUT_ROOT/extraction.log
# FOREST_POLICY sets the priority wrapper, default "nice -n 10". ORDER.md asked for "taskpolicy -b",
# but on the M3 that keeps the workers on the efficiency cores at about a third of the speed.

set -euo pipefail

REPO_DIR="/Users/$(whoami)/Documents/GitHub/otacon"
IDS_DIR="$REPO_DIR/past_commands/forest_text_extraction/ids"
COMMENTS_INPUT="/Volumes/rdisk2/redditdata/comments"
SUBMISSIONS_INPUT="/Volumes/rdisk2/redditdata/submissions"
OUTPUT_ROOT="/Volumes/dissertationhorse/OP_forest_texts"
WORKERS=2
POLICY=(${=FOREST_POLICY:-nice -n 10})
NOTIFY_TO="vscode_notifs@smog.sh"
NOTIFY_FROM="uni@Carlos-Has-MacBook-Air.local"

foreground=0 allow_with_sayer=0
typeset -a only
while (( $# )); do
    case "$1" in
        --output) OUTPUT_ROOT="$2"; shift 2 ;;
        --workers) WORKERS="$2"; shift 2 ;;
        --allow-with-sayer) allow_with_sayer=1; shift ;;
        --foreground) foreground=1; shift ;;
        --only) shift; while (( $# )) && [[ "$1" != --* ]]; do only+=("$1"); shift; done ;;
        *) echo "Unknown argument: $1"; exit 2 ;;
    esac
done
LOG="$OUTPUT_ROOT/extraction.log"
PAUSE_FILE="$OUTPUT_ROOT/.pause_requested"

if (( ! foreground )); then
    for required_path in "$COMMENTS_INPUT" "$SUBMISSIONS_INPUT" "$IDS_DIR" "${OUTPUT_ROOT:h}"; do
        if [[ ! -d "$required_path" ]]; then
            echo "ERROR: $required_path is not reachable. Is the disk connected?"
            exit 1
        fi
    done
    if (( ! allow_with_sayer )) && pgrep -f otacon.history_runner > /dev/null; then
        echo "ERROR: the sayer-histories extraction is still running (ORDER.md: wait for it)."
        exit 1
    fi
    if pgrep -f "otacon.forest_extraction run .*--output $OUTPUT_ROOT( |\$)" > /dev/null; then
        echo "ERROR: a forest extraction into $OUTPUT_ROOT is already running."
        exit 1
    fi
    mkdir -p "$OUTPUT_ROOT"
    rm -f "$PAUSE_FILE"
    forward=(--output "$OUTPUT_ROOT" --workers "$WORKERS")
    (( ${#only} )) && forward+=(--only "${only[@]}")
    nohup caffeinate -ims "${POLICY[@]}" zsh "$0" --foreground "${forward[@]}" >> "$LOG" 2>&1 &!
    echo "Started (pid $!, ${POLICY[*]}, $WORKERS workers). Log: $LOG"
    exit 0
fi

notify() {
    printf 'From: %s\nTo: %s\nSubject: %s\n\n%s\n' "$NOTIFY_FROM" "$NOTIFY_TO" "$1" "$2" \
        | /usr/sbin/sendmail -t -f "$NOTIFY_FROM" || echo "WARNING: notification failed." >&2
}

cd "$REPO_DIR"
echo "=== $(date '+%Y-%m-%d %H:%M:%S') start: ${POLICY[*]}, $WORKERS workers${only:+, only $only} ==="
set +e
poetry run python -m otacon.forest_extraction run --ids "$IDS_DIR" \
    --comments-input "$COMMENTS_INPUT" --submissions-input "$SUBMISSIONS_INPUT" \
    --output "$OUTPUT_ROOT" --workers "$WORKERS" ${only:+--only} "${only[@]}"
run_status=$?
set -e
if (( run_status != 0 )); then
    notify "otacon forest texts: FAILED (exit $run_status)" "$(tail -n 20 "$LOG")
Re-run $0 to resume."
elif [[ -e "$PAUSE_FILE" ]]; then
    notify "otacon forest texts: paused" "$(tail -n 3 "$LOG")"
else
    notify "otacon forest texts: finished" "$(poetry run python -m otacon.forest_extraction summary \
        --ids "$IDS_DIR" --output "$OUTPUT_ROOT" 2>&1)"
fi
exit $run_status
