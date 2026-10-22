#!/usr/bin/env bash
# Fails when HEAD's Cargo.lock has failing advisories that the base commit's
# Cargo.lock doesn't: advisories a pull request introduces, not ones newly
# published against dependencies already on main (advisories.yml reports
# those). Both runs use HEAD's deny.toml. The job summary lists new and
# existing findings by kind. Usage: new-advisories.sh <base-sha>
set -euo pipefail
. "$(dirname "$0")/deny-lib.sh"

base="$1"
root="$(git rev-parse --show-toplevel)"
[ -f "$root/deny.toml" ] && export DENY_CONFIG="$root/deny.toml"

if git diff --quiet "$base" HEAD -- Cargo.lock; then
  message="Cargo.lock unchanged; this change can't introduce an advisory."
  echo "$message"
  printf '## cargo deny: new advisories\n\n%s\n' "$message" | summary
  exit 0
fi

scratch="$(mktemp -d)"
trap 'git worktree remove --force "$scratch/base" 2>/dev/null || true; rm -rf "$scratch"' EXIT

deny_json "$root" advisories | deny_rows > "$scratch/head"
if git cat-file -e "$base:Cargo.lock" 2>/dev/null; then
  git worktree add --quiet --detach "$scratch/base" "$base"
  deny_json "$scratch/base" advisories | deny_rows > "$scratch/base.txt"
else
  : > "$scratch/base.txt"
fi

# A finding is identified by its advisory and crate version; severity,
# kind and description come from the head run.
key() { awk -F'\t' '{ print $3 "\t" $4 }' "$1" | sort -u; }
new_keys="$(comm -23 <(key "$scratch/head") <(key "$scratch/base.txt"))"
new="$(awk -F'\t' 'NR == FNR { k[$0]; next } ($3 "\t" $4) in k' <(printf '%s\n' "$new_keys") "$scratch/head")"
existing="$(awk -F'\t' 'NR == FNR { k[$0]; next } !(($3 "\t" $4) in k)' <(printf '%s\n' "$new_keys") "$scratch/head")"
new_errors="$(awk -F'\t' '$1 == "error"' <<<"$new")"

{
  echo "## cargo deny: new advisories"
  echo
  if [ -n "$new_errors" ]; then
    echo "**Failed:** this change introduces $(wc -l <<<"$new_errors" | tr -d ' ') failing advisory finding(s)."
  else
    echo "**Passed:** this change introduces no failing advisories."
  fi
  echo
  echo "### Introduced by this change"
  echo
  rows_table <<<"$new"
  echo
  echo "### Already on the base branch"
  echo
  echo "These don't fail this change; the daily advisories run reports them."
  echo
  rows_table <<<"$existing"
  echo
  echo "### By kind"
  echo
  echo "Introduced:"
  echo
  kind_counts <<<"$new"
  echo
  echo "Existing:"
  echo
  kind_counts <<<"$existing"
} | summary

if [ -n "$new_errors" ]; then
  echo "This change introduces advisories:"
  echo "$new_errors"
  exit 1
fi
echo "No new advisories (existing ones are reported by advisories.yml)."
