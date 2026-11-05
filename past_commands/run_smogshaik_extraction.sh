#!/bin/bash
# Extract all comments + submissions by Smogshaik from rdisk2 with otacon --prefilter,
# 6 months in parallel (largest files first), then build combined JSONL + CSV files.
# Resumable: rerun this script and completed months are skipped.
DATA="/Volumes/rdisk2/redditdata"
ROOT="$DATA/output/Smogshaik"
REPO="/Users/$(whoami)/Documents/GitHub/otacon"
JOBS=6
[ -d "$DATA/comments" ] || { echo "rdisk2 not mounted"; exit 1; }
mkdir -p "$ROOT/logs"; rm -f "$ROOT/failed_months.txt"
echo "$(date '+%F %T') start"
stat -f '%z %N' "$DATA"/comments/RC_*.zst "$DATA"/submissions/RS_*.zst | sort -rn | cut -d' ' -f2- \
  | xargs -P "$JOBS" -n 1 "$REPO/past_commands/smogshaik_job.sh"
if [ -s "$ROOT/failed_months.txt" ]; then
  echo "$(date '+%F %T') some months failed, rerun this script:"; cat "$ROOT/failed_months.txt"; exit 1
fi
echo "$(date '+%F %T') all months done, converting"
cd "$REPO" && poetry run python past_commands/smogshaik_to_csv.py "$ROOT"
echo "$(date '+%F %T') finished"
