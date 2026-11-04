#!/usr/bin/env bash
# Runs the same benchmark on several commits and prints one line per repetition.
# Each commit is built in its own worktree, so a comparison measures that exact
# code and not a flag that might change something else.
#
#   scripts/compare.sh <rate> <duration> <repetitions> <tag>...
#
# Keep the machine otherwise idle: the numbers are only comparable if it is.
set -euo pipefail
rate="$1"; duration="$2"; repetitions="$3"; shift 3
root="$(cd "$(dirname "$0")/.." && pwd)"
base="$root/.run/compare"
mkdir -p "$base"
for tag in "$@"; do
  tree="$base/${tag//\//-}"
  [ -d "$tree" ] || git -C "$root" worktree add -q --detach "$tree" "$tag"
  (cd "$tree" && go build -o bin/ ./cmd/...)
  # The tree needs the scripts of its own commit, so it runs from there.
  (cd "$tree" && ./bin/harness bench --rate "$rate" --duration "$duration" --warmup 5s \
    --workers 1 --concurrency 64 --max-in-flight 512 --repetitions "$repetitions" \
    --drain-deadline 30s --name "$tag" --out "$base/out" 2>&1 | sed "s|^|$tag |" | grep ' rep ')
done
