#!/usr/bin/env zsh
# Back up the uncompressed 2005-12..2021-05 sayer histories on Limbo to the NAS as .zst,
# oldest month first. Each file is compressed into a staging dir on dissertationhorse and
# rclone-moved to the NAS (hash-verified); compression of the next file overlaps the upload.
# Files whose uncompressed copy is already on the NAS with the same size are skipped.
# Limbo is only read. Run again to resume: finished files are listed in $DONE_FILE.

source ~/.zshrc >/dev/null 2>&1
set -euo pipefail
typeset -f rclone-custom >/dev/null || exit 127
zmodload zsh/stat

SOURCE_ROOT="/Volumes/Limbo/reddit-OPsayerhistories"
NAS_ROOT="almazen:reddit_data/output/project3/OP_sayer_histories_users-7288fa75135a"
STAGING="/Volumes/dissertationhorse/limbo_backup_staging"
DONE_FILE="/Volumes/dissertationhorse/limbo_backup_done.txt"
NOTIFY_TO="vscode_notifs@smog.sh"
NOTIFY_FROM="uni@Carlos-Has-MacBook-Air.local"

notify() {
    printf 'From: %s\nTo: %s\nSubject: %s\n\n%s\n' "$NOTIFY_FROM" "$NOTIFY_TO" "$1" "$2" \
        | /usr/sbin/sendmail -t -f "$NOTIFY_FROM" || echo "WARNING: notification failed." >&2
}
trap 'notify "otacon Limbo backup: FAILED" "See the backup log. Re-run the script to resume."' ZERR

touch "$DONE_FILE"
typeset -A done_files nas_sizes
for entry in ${(f)"$(<$DONE_FILE)"}; do done_files[$entry]=1; done
for stream in comments submissions; do
    for entry in ${(f)"$(rclone-custom lsf --format sp --separator '|' "$NAS_ROOT/$stream" 2>/dev/null | grep -a '|')"}; do
        nas_sizes[$stream/${entry#*|}]=${entry%%|*}
    done
done

# Oldest month first, submissions before comments.
typeset -a sources
sources=(${(f)"$(
    cd "$SOURCE_ROOT"
    for f in submissions/*.jsonl comments/*.jsonl; do
        [[ "$f" =~ '_R[CS]_([0-9]{4}-[0-9]{2})\.zst_' ]] && print -r -- "${match[1]} ${f%%/*} $f"
    done | sort -k1,1 -k2,2r | cut -d' ' -f3-
)"})

upload_pid=""
upload_name=""
wait_upload() {
    [[ -n "$upload_pid" ]] || return 0
    wait $upload_pid
    print -r -- "$upload_name" >> "$DONE_FILE"
    echo "Uploaded $upload_name"
    upload_pid=""
}

for relative in $sources; do
    stream="${relative%%/*}"
    if (( ${+done_files[$relative]} )); then
        continue
    fi
    if [[ "${nas_sizes[$relative]:-}" == "$(zstat +size "$SOURCE_ROOT/$relative")" ]]; then
        echo "Skipping $relative (uncompressed copy already on NAS)"
        print -r -- "$relative" >> "$DONE_FILE"
        continue
    fi
    mkdir -p "$STAGING/$stream"
    staged="$STAGING/$stream/${relative:t}.zst"
    # The dot-prefixed temp name is excluded by rclone-custom until it is complete.
    # Retry transient USB read errors from Limbo before giving up.
    for attempt in 1 2 3; do
        zstd -q -3 -T2 -f "$SOURCE_ROOT/$relative" -o "$STAGING/$stream/.${relative:t}.zst.tmp" && break
        echo "zstd attempt $attempt failed for $relative"
        (( attempt < 3 )) || false
        sleep 30
    done
    mv "$STAGING/$stream/.${relative:t}.zst.tmp" "$staged"
    wait_upload
    rclone-custom move "$staged" "$NAS_ROOT/$stream" --stats 0 &
    upload_pid=$!
    upload_name="$relative"
done
wait_upload

# Markers last, so a marker on the NAS means that month's data is complete there.
for marker in "$SOURCE_ROOT"/_otacon_history_*_complete; do
    rclone-custom copy "$marker" "$NAS_ROOT" --stats 0
done
echo "Limbo backup complete."
notify "otacon Limbo backup: complete" "All of $SOURCE_ROOT is on $NAS_ROOT (compressed .zst; 52 early files uncompressed)."
