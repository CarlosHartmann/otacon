#!/usr/bin/env zsh
# ETA and output-size projection for the yearly sayer-history emails.
# Usage: sayer_histories_eta.zsh STATE_FILE LOCAL_ROOT RUN_START_EPOCH
# Read fresh at every call, so it can be edited while the launcher runs.

zmodload zsh/stat zsh/datetime

COMMENTS_INPUT="/Volumes/rdisk2/redditdata/comments"
SUBMISSIONS_INPUT="/Volumes/rdisk2/redditdata/submissions"
state_file="$1" local_root="$2" run_start="$3"

# Compressed input bytes per month.
typeset -A month_bytes
for input_file in "$COMMENTS_INPUT"/RC_*.zst(N) "$SUBMISSIONS_INPUT"/RS_*.zst(N); do
    month="${${input_file:t}[4,10]}"
    [[ "$month" > 2022-12 ]] && continue
    (( month_bytes[$month] += $(zstat +size "$input_file") ))
done

typeset -A recorded
for month in ${(f)"$(<$state_file)"}; do recorded[$month]=1; done

# Months archived on LOCAL_ROOT, and those finished since this launch, by their markers.
done_bytes=0 archived_input=0
for marker in "$local_root"/_otacon_history_*_complete(N); do
    month="${${marker:t}[17,23]}"
    (( archived_input += month_bytes[$month] ))
    (( $(zstat +mtime "$marker") >= run_start )) && (( done_bytes += month_bytes[$month] ))
done
remaining_bytes=0
for month in ${(k)month_bytes}; do
    (( ${+recorded[$month]} )) || (( remaining_bytes += month_bytes[$month] ))
done

elapsed=$(( EPOCHSECONDS - run_start ))
if (( done_bytes > 0 && elapsed > 0 )); then
    eta=$(( remaining_bytes * elapsed / done_bytes ))
    printf 'Rate this run (all workers): %.1f MB/s of compressed input over %.1f h.\n' $(( done_bytes / 1e6 / elapsed )) $(( elapsed / 3600.0 ))
    printf 'Remaining input: %.0f GB (incl. months in progress) -> ETA ~%.1f days (%s), assuming the same rate.\n' \
        $(( remaining_bytes / 1e9 )) $(( eta / 86400.0 )) "$(strftime '%a %d %b %H:%M' $(( EPOCHSECONDS + eta )))"
fi

out_bytes=$(( $(du -sk "$local_root" | cut -f1) * 1024 ))
free_bytes=$(( $(df -k "$local_root" | awk 'NR==2{print $4}') * 1024 ))
if (( archived_input > 0 )); then
    ratio=$(( out_bytes * 1.0 / archived_input ))
    projected=$(( ratio * remaining_bytes ))
    margin=$(( free_bytes - projected ))
    printf 'Compressed output on %s: %.0f GB (%.2fx compressed input).\n' "$local_root" $(( out_bytes / 1e9 )) $ratio
    printf 'Projected total there: ~%.0f GB (%.0f GB still to come).\n' $(( (out_bytes + projected) / 1e9 )) $(( projected / 1e9 ))
    printf 'Free now: %.0f GB -> %s ~%.0f GB.\n' $(( free_bytes / 1e9 )) \
        "$( (( margin >= 0 )) && print 'fits, spare' || print 'SHORT by')" $(( (margin >= 0 ? margin : -margin) / 1e9 ))
fi
