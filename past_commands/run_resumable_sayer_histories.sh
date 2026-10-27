#!/usr/bin/env zsh
# Extract the listed users' monthly comment and submission histories with parallel workers.
# Each finished month is compressed, copied to the NAS and archived on dissertationhorse
# (see offload_sayer_month.zsh). Run again to resume from the completion markers.
# To pause safely, touch "$OUTPUT_ROOT/.pause_requested"; every worker finishes and
# offloads its current month, then exits.

source ~/.zshrc

set -euo pipefail

if ! typeset -f rclone-custom > /dev/null; then
    echo "ERROR: rclone-custom function not found after sourcing ~/.zshrc."
    exit 1
fi

REPO_DIR="/Users/$(whoami)/Documents/GitHub/otacon"
DATA_ROOT="/Volumes/rdisk2/redditdata"
COMMENTS_INPUT="$DATA_ROOT/comments"
SUBMISSIONS_INPUT="$DATA_ROOT/submissions"
USER_FILE="/Users/uni/Documents/GitHub/shadow_moses/0-assets/raw_data/main/7-unique-usernames-and-subreddits/unique_usernames.txt"
USER_LIST_ID="$(shasum -a 256 "$USER_FILE" | cut -c1-12)"
# 2005-12..2021-05 were archived uncompressed on /Volumes/Limbo/reddit-OPsayerhistories.
OUTPUT_ROOT="/Volumes/dissertationhorse/OP_sayer_histories_work"
LOCAL_ROOT="/Volumes/dissertationhorse/OP_sayer_histories"
OFFLOAD_COMMAND="$REPO_DIR/past_commands/offload_sayer_month.zsh"
WORKER_COUNT=3
TIME_TO="2022-12"

STATE_FILE="$OUTPUT_ROOT/.completed_months_through_${TIME_TO}_users-$USER_LIST_ID"
PAUSE_FILE="$OUTPUT_ROOT/.pause_requested"

mkdir -p "$OUTPUT_ROOT" "$LOCAL_ROOT"
rm -f "$PAUSE_FILE"

cd "$REPO_DIR"

for required_path in "$COMMENTS_INPUT" "$SUBMISSIONS_INPUT" "$LOCAL_ROOT"; do
    if [[ ! -d "$required_path" ]]; then
        echo "ERROR: directory $required_path is not reachable. Is the external HDD connected?"
        exit 1
    fi
done
if [[ ! -r "$USER_FILE" ]]; then
    echo "ERROR: username file $USER_FILE is not readable."
    exit 1
fi

NOTIFY_TO="vscode_notifs@smog.sh"
NOTIFY_FROM="uni@Carlos-Has-MacBook-Air.local"

notify() {
    printf 'From: %s\nTo: %s\nSubject: %s\n\n%s\n' "$NOTIFY_FROM" "$NOTIFY_TO" "$1" "$2" \
        | /usr/sbin/sendmail -t -f "$NOTIFY_FROM" || echo "WARNING: notification failed." >&2
}

zmodload zsh/datetime
ETA_SCRIPT="$REPO_DIR/past_commands/sayer_histories_eta.zsh"
run_start=$EPOCHSECONDS

# A year is complete once all of its months are recorded, whichever workers did them.
year_complete() {
    local year=$1 month
    for month in {01..12}; do
        grep -qx "$year-$month" "$STATE_FILE" 2>/dev/null || return 1
    done
}

run_worker() {
    local worker_index=$1 line year runner_status
    set +e
    poetry run python -m otacon.history_runner \
        --comments-input "$COMMENTS_INPUT" \
        --submissions-input "$SUBMISSIONS_INPUT" \
        --user-file "$USER_FILE" \
        --output-root "$OUTPUT_ROOT" \
        --remote-root "$LOCAL_ROOT" \
        --state-file "$STATE_FILE" \
        --pause-file "$PAUSE_FILE" \
        --user-list-id "$USER_LIST_ID" \
        --worker-index "$worker_index" \
        --worker-count "$WORKER_COUNT" \
        --offload-command "$OFFLOAD_COMMAND" \
        --time-to "$TIME_TO" 2>&1 | while IFS= read -r line; do
        print -r -- "$line"
        if [[ "$line" =~ 'Offloaded and recorded ([0-9]{4})-[0-9]{2} ' ]]; then
            year="${match[1]}"
            # noclobber makes the flag file a once-only claim across workers.
            if year_complete "$year" && ( set -o noclobber; : > "$OUTPUT_ROOT/.notified_$year" ) 2>/dev/null; then
                notify "otacon sayer histories: $year complete" "$line
$(zsh "$ETA_SCRIPT" "$STATE_FILE" "$LOCAL_ROOT" "$run_start" 2>&1)
$(df -h "$LOCAL_ROOT" | tail -1)"
            fi
        elif [[ "$line" == LOW\ SPACE:* ]]; then
            notify "otacon sayer histories: worker $worker_index stopped, low disk space" "$line"
        fi
    done
    runner_status=${pipestatus[1]}
    if (( runner_status != 0 )); then
        notify "otacon sayer histories: worker $worker_index FAILED (exit $runner_status)" \
            "See $OUTPUT_ROOT/worker_$worker_index.log. Re-run the script to resume from the markers in $LOCAL_ROOT."
    fi
    return $runner_status
}

typeset -a worker_pids
for worker_index in {0..$(( WORKER_COUNT - 1 ))}; do
    run_worker $worker_index > "$OUTPUT_ROOT/worker_$worker_index.log" 2>&1 &
    worker_pids+=($!)
    echo "Started worker $worker_index (pid $!), log $OUTPUT_ROOT/worker_$worker_index.log"
done

overall_status=0
for pid in $worker_pids; do
    wait $pid || overall_status=$?
done
echo "All workers finished (status $overall_status)."
exit $overall_status
