#!/usr/bin/env bash
# Split a big .sh into 12 parts:
# - first 11 lines are shared header
# - body consists of 1122 groups, each 4 lines
# - split into 12 chunks as evenly as possible (first remainder chunks get 1 extra group)
set -euo pipefail

input="${1:-big.sh}"      # input file (default: big.sh)
chunks="${2:-12}"         # number of output parts (default: 12)
header_lines="${HEADER_LINES:-11}"   # shared header lines
group_lines="${GROUP_LINES:-4}"      # lines per group

# Basic checks
total_lines=$(wc -l < "$input" | tr -d ' ')
if (( total_lines < header_lines )); then
  echo "Error: total lines ($total_lines) < header ($header_lines)" >&2
  exit 1
fi

# Derive body metrics
body_lines=$(( total_lines - header_lines ))
if (( body_lines % group_lines != 0 )); then
  echo "Error: body lines ($body_lines) not a multiple of group_lines ($group_lines)" >&2
  exit 1
fi
groups=$(( body_lines / group_lines ))
if (( chunks <= 0 )); then
  echo "Error: chunks must be > 0" >&2
  exit 1
fi
base=$(( groups / chunks ))     # minimum groups per chunk
rem=$(( groups % chunks ))      # first 'rem' chunks get one extra group

echo "Total lines: $total_lines"
echo "Header lines: $header_lines"
echo "Body lines: $body_lines (= $groups groups × $group_lines lines)"
echo "Chunks: $chunks  -> base=$base groups/chunk, remainder=$rem"

# Prepare temp files
head -n "$header_lines" "$input" > .header.tmp
tail -n +"$((header_lines+1))" "$input" > .body.tmp

# Split loop
current_start=1   # 1-based line index within body.tmp
for i in $(seq 1 "$chunks"); do
  g=$base
  if (( i <= rem )); then g=$((g+1)); fi   # distribute remainder
  lines_this=$(( g * group_lines ))
  if (( lines_this == 0 )); then
    # still emit a file with just the header
    out=$(printf "part_%02d.sh" "$i")
    cat .header.tmp > "$out"
    chmod +x "$out"
    echo "Wrote $out (groups=0, lines=header only)"
    continue
  fi
  start=$current_start
  end=$(( current_start + lines_this - 1 ))

  out=$(printf "part_%02d.sh" "$i")
  cat .header.tmp > "$out"
  sed -n "${start},${end}p" .body.tmp >> "$out"
  chmod +x "$out"

  echo "Wrote $out (groups=$g, body-lines=$lines_this, body-range=${start}-${end})"
  current_start=$(( end + 1 ))
done

rm -f .header.tmp .body.tmp
