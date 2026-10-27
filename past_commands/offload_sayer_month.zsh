#!/usr/bin/env zsh
# Offload one finished history month: compress its outputs with zstd, copy them to the NAS
# (rclone verifies hashes), then move them into the local archive on the same volume.
# The marker goes last, so a marker anywhere means that month's data is complete there.
# Usage: offload_sayer_month.zsh WORK_DIR MARKER
# Leaves WORK_DIR and its stream subdirectories empty for the runner to remove.

source ~/.zshrc >/dev/null 2>&1
set -euo pipefail
typeset -f rclone-custom >/dev/null || exit 127

NAS_ROOT="${SAYER_NAS_ROOT:-almazen:reddit_data/output/project3/OP_sayer_histories_users-7288fa75135a}"
LOCAL_ROOT="${SAYER_LOCAL_ROOT:-/Volumes/dissertationhorse/OP_sayer_histories}"
work_dir="$1" marker="$2"

for stream in submissions comments; do
    [[ -d "$work_dir/$stream" ]] || continue
    for output_file in "$work_dir/$stream"/*.jsonl(N); do
        zstd -q -3 -T2 --rm "$output_file" -o "$output_file.zst"
    done
    rclone-custom copy "$work_dir/$stream/" "$NAS_ROOT/$stream" --stats 0
    mkdir -p "$LOCAL_ROOT/$stream"
    for compressed_file in "$work_dir/$stream"/*.zst(N); do
        mv "$compressed_file" "$LOCAL_ROOT/$stream/"
    done
done

rclone-custom copy "$marker" "$NAS_ROOT" --stats 0
mv "$marker" "$LOCAL_ROOT/"
