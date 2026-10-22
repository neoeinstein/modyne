# Shared helpers for the cargo-deny scripts; source, don't run.

# Runs cargo deny in directory $1 for checks $2..., with --locked so cargo
# never re-resolves the lockfile, and prints its JSON diagnostics. Uses
# $DENY_CONFIG when set. Fails closed: if cargo deny didn't run to its
# summary line, nothing it printed can be trusted.
deny_json() {
  local dir="$1" out
  shift
  local config=()
  [ -n "${DENY_CONFIG:-}" ] && config=(--config "$DENY_CONFIG")
  out="$(cd "$dir" && cargo deny --locked ${config[@]+"${config[@]}"} --format json check "$@" 2>&1 >/dev/null || true)"
  # Cargo's progress lines share stderr; skip anything that isn't JSON.
  if ! jq -R -e 'fromjson? | select(.type == "summary")' <<<"$out" >/dev/null; then
    echo "cargo deny didn't complete in $dir:" >&2
    echo "$out" >&2
    return 2
  fi
  printf '%s\n' "$out"
}

# Reads deny_json output; prints one tab-separated row per finding and
# affected crate: severity, kind, advisory id (or -), crate@version (or -),
# description.
deny_rows() {
  jq -R -r 'fromjson? | select(.type == "diagnostic")
    | .fields as $f
    | ([$f.graphs[]?.Krate | "\(.name)@\(.version)"] | if length == 0 then ["-"] else . end)[] as $crate
    | [$f.severity, $f.code, ($f.advisory.id // "-"), $crate,
       (($f.advisory.title // $f.message) | gsub("[\t\n|]"; " "))]
    | @tsv' | sort -u
}

# Prints rows (stdin) as a Markdown table, errors first.
rows_table() {
  local rows
  rows="$(sort -t $'\t' -k1,1 -k2,2 -k3,3)"
  if [ -z "$rows" ]; then
    echo "_None._"
    return
  fi
  echo "| Severity | Kind | Advisory | Crate | Description |"
  echo "|---|---|---|---|---|"
  awk -F'\t' '{ printf "| %s | %s | %s | `%s` | %s |\n", $1, $2, $3, $4, $5 }' <<<"$rows"
}

# Prints a count of rows (stdin) per kind and severity as a Markdown table.
kind_counts() {
  local rows
  rows="$(cat)"
  if [ -z "$rows" ]; then
    echo "_None._"
    return
  fi
  echo "| Kind | Severity | Count |"
  echo "|---|---|---|"
  cut -f1,2 <<<"$rows" | sort | uniq -c |
    awk '{ printf "| %s | %s | %d |\n", $3, $2, $1 }'
}

# Appends stdin to the job summary when running in GitHub Actions.
summary() {
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    cat >> "$GITHUB_STEP_SUMMARY"
  else
    cat
  fi
}
