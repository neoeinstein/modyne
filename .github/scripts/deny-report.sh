#!/usr/bin/env bash
# Runs cargo deny for the given checks, lists every finding in the log and
# the job summary, and fails when any finding is an error.
# Usage: deny-report.sh <check>...   (e.g. bans licenses sources)
set -euo pipefail
. "$(dirname "$0")/deny-lib.sh"

root="$(git rev-parse --show-toplevel)"
json="$(deny_json "$root" "$@")"
rows="$(deny_rows <<<"$json")"
errors="$(awk -F'\t' '$1 == "error"' <<<"$rows")"

{
  echo "## cargo deny: $*"
  echo
  if [ -n "$errors" ]; then
    echo "**Failed:** $(wc -l <<<"$errors" | tr -d ' ') error finding(s)."
  else
    echo "**Passed:** no errors."
  fi
  echo
  echo "### By kind"
  echo
  kind_counts <<<"$rows"
  echo
  echo "### Findings"
  echo
  rows_table <<<"$rows"
} | summary

if [ -n "$rows" ]; then
  echo "Findings (severity, kind, advisory, crate, description):"
  echo "$rows"
fi
[ -z "$errors" ]
