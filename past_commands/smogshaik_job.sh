#!/bin/bash
# One otacon run for one month file; records success in the state file (resumable).
f="$1"
ROOT="/Volumes/rdisk2/redditdata/output/Smogshaik"
REPO="/Users/$(whoami)/Documents/GitHub/otacon"
STATE="$ROOT/.completed_months"
ds=$(basename "$(dirname "$f")")
m=$(basename "$f" .zst); ym=${m#R?_}
grep -qxF "$ds/$m" "$STATE" 2>/dev/null && exit 0
out="$ROOT/parts/$ds/$m"; rm -rf "$out"; mkdir -p "$out" "$ROOT/logs"
cd "$REPO" || exit 1
start=$(date +%s)
if poetry run python -m otacon.main --input "$(dirname "$f")" --output "$out" \
     --time_from "$ym" --time_to "$ym" --src user --name Smogshaik \
     --prefilter --return_all --no_stats --no_cleanup > "$ROOT/logs/${ds}_$m.log" 2>&1; then
  echo "$ds/$m" >> "$STATE"
  echo "$(date '+%F %T') done $ds/$m in $(( $(date +%s) - start ))s, $(cat "$out"/*.jsonl 2>/dev/null | wc -l | tr -d ' ') hits"
else
  echo "$ds/$m" >> "$ROOT/failed_months.txt"
  echo "$(date '+%F %T') FAILED $ds/$m (see logs/${ds}_$m.log)"
fi
