#!/usr/bin/env zsh
# Extract all comments, then all submissions, keeping the username set loaded
# for each full-range otacon process. Run again to resume between these phases.
# To pause safely, touch "$OUTPUT_ROOT/.pause_requested" and wait for the
# current phase to finish and offload.

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
OUTPUT_ROOT="$DATA_ROOT/output/project3/OP_sayer_histories"
REMOTE_ROOT="almazen:reddit_data/output/project3/OP_sayer_histories"

STATE_FILE="$OUTPUT_ROOT/.completed_months_through_2022-12"
PAUSE_FILE="$OUTPUT_ROOT/.pause_requested"

mkdir -p "$OUTPUT_ROOT"
rm -f "$PAUSE_FILE"

cd "$REPO_DIR"

for required_path in "$COMMENTS_INPUT" "$SUBMISSIONS_INPUT"; do
    if [[ ! -d "$required_path" ]]; then
        echo "ERROR: input directory $required_path is not reachable. Is the external HDD connected?"
        exit 1
    fi
done
if [[ ! -r "$USER_FILE" ]]; then
    echo "ERROR: username file $USER_FILE is not readable."
    exit 1
fi

exec poetry run python -m otacon.history_runner \
    --comments-input "$COMMENTS_INPUT" \
    --submissions-input "$SUBMISSIONS_INPUT" \
    --user-file "$USER_FILE" \
    --output-root "$OUTPUT_ROOT" \
    --remote-root "$REMOTE_ROOT" \
    --state-file "$STATE_FILE" \
    --pause-file "$PAUSE_FILE" \
    --time-to 2022-12