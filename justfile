# varve -- local agent-session record lake (Parquet + DuckDB).
# Data home: ~/.local/share/varve (never in this repo; VARVE_DATA overrides).
# Zero resident processes: every target is invoke-and-exit. DuckDB is single-
# writer -- ingest/backfill are the SOLE writers; `just sql` opens read-only.

# Recipe args reach recipes as "$@" with quoting intact ({{ARGS}} interpolation
# would re-split quoted SQL at the shell).
set positional-arguments

data_home := env_var_or_default("VARVE_DATA", env_var("HOME") + "/.local/share/varve")
repo      := justfile_directory()
lake      := data_home + "/lake"
state     := data_home + "/state"
# DuckDB otherwise claims 80% of RAM, every core, and spill up to 90% of free disk.
mine_memory_limit := env_var_or_default("VARVE_MINE_MEMORY_LIMIT", "4GB")
duckdb_threads    := env_var_or_default("VARVE_DUCKDB_THREADS", "4")
duckdb_max_spill  := env_var_or_default("VARVE_DUCKDB_MAX_SPILL", "20GB")
manifest  := state + "/manifest.parquet"
db        := data_home + "/varve.duckdb"
# W7 adjudication ledger: OPERATOR DATA, never in the repo (public-instrument rule).
# Defaults to the data home (survives a lake rebuild -- backfill clears lake/ only);
# VARVE_ADJUDICATIONS overrides. `mine` handles an absent file gracefully.
adjudications := env_var_or_default("VARVE_ADJUDICATIONS", data_home + "/adjudications.jsonl")
# W7 adjudication worksheet: OPERATOR DATA (model-suggested labels + role-level rationale),
# never in the repo (public-instrument rule). Defaults to the data home; VARVE_WORKSHEET
# overrides. Advisory input to `just review`; absence is non-fatal.
worksheet := env_var_or_default("VARVE_WORKSHEET", data_home + "/adjudication_worksheet.jsonl")
# Attribution rules: OPERATOR DATA (the cwd globs name real namespaces), never in
# the repo (public-instrument rule). Defaults to the data home; VARVE_ATTRIBUTION
# overrides. Required: _pipeline fails loudly when absent (silent generic rules
# would mis-attribute every session) -- bootstrap by copying
# adapters/attribution.example.csv there and editing.
attribution := env_var_or_default("VARVE_ATTRIBUTION", data_home + "/attribution.csv")

# Per-runtime source roots. Each has an env override mirroring VARVE_CC_ROOT so a
# frozen snapshot or alternate location can be pointed at for tests. A missing
# root skips that runtime with a note (exit 0) -- see _pipeline.
#
# Claude Code: the glob MUST recurse -- subagent traffic lives in
# <session_id>/subagents/agent-*.jsonl and is otherwise silently dropped.
cc_root     := env_var_or_default("VARVE_CC_ROOT", env_var("HOME") + "/.claude/projects")
cc_source   := cc_root + "/**/*.jsonl"
# pi: append-only JSONL, one dir per dir-encoded cwd, recursive glob.
pi_root     := env_var_or_default("VARVE_PI_ROOT", env_var("HOME") + "/.pi/agent/sessions")
pi_source   := pi_root + "/**/*.jsonl"
# opencode: a single SQLite db (WAL). Read via a read-only APFS clone snapshot so
# the live store is never written or locked (see _pipeline).
opencode_db := env_var_or_default("VARVE_OPENCODE_DB", env_var("HOME") + "/.local/share/opencode/opencode.db")

duckdb := "mise exec -- duckdb"

default:
    @just --list

# Full historical backfill: all Claude Code history -> Parquet partitions.
# Clears the lake first (append-only history means no stale partitions, but a
# clean full rebuild is the contract for `backfill`).
backfill:
    rm -rf "{{lake}}"
    @just _pipeline backfill

# Incremental, idempotent ingest. Rewrites only the date partitions whose source
# files changed (per-file size+mtime high-water mark). Handles live-session
# appends: a grown JSONL re-derives its whole session -> appended rows counted
# exactly once. No changes -> no writes -> identical row counts.
ingest:
    @just _pipeline ingest

# Shared pipeline. mode = backfill | ingest. All runtimes flow through one
# manifest-driven incremental model: each present runtime's adapter derives its
# _<rt>_* staging, sql/stage.sql unions them into stg_*, sql/affected.sql marks
# the changed date partitions, and one COPY per table writes the affected
# (runtime, date) partitions. A missing source root SKIPS that runtime with a
# note (exit 0). opencode is read via a read-only APFS clone so the live db is
# never written or locked.
_pipeline mode:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -f "{{attribution}}" ]; then
      echo "varve: no attribution rules at {{attribution}}" >&2
      echo "       copy adapters/attribution.example.csv there and edit (VARVE_ATTRIBUTION overrides)" >&2
      exit 1
    fi
    mkdir -p "{{lake}}" "{{state}}"
    # Manifest bootstrap: empty (schema-only) parquet on first run.
    if [ ! -f "{{manifest}}" ]; then
      {{duckdb}} -c "COPY (SELECT ''::VARCHAR path, 0::BIGINT size, 0::BIGINT mtime WHERE 1=0) TO '{{manifest}}' (FORMAT parquet);"
    fi
    # Current file inventory = high-water mark (path | size-bytes | mtime-epoch),
    # spanning every present runtime. The manifest is refreshed from this, so one
    # manifest covers all runtimes.
    #
    # `stat` is not portable: BSD (macOS) wants `-f '%N|%z|%m'`; GNU (Linux, the
    # CI runner) has no `-f FORMAT` at all -- `-f` there means "filesystem status"
    # instead, so the BSD invocation fails hard on Linux. Detect the flavor once
    # via `stat --version` (GNU understands it and exits 0; BSD errors on the
    # unknown long option) and pick the matching format string. Both formats are
    # semantically identical -- name, size in bytes (st_size), mtime as decimal
    # seconds since epoch -- so the manifest's (path, size, mtime) high-water mark
    # means exactly the same thing on either platform; this is a syntax fix, not
    # a behavior change.
    if stat --version >/dev/null 2>&1; then
      stat_fmt=(--format='%n|%s|%Y')
    else
      stat_fmt=(-f '%N|%z|%m')
    fi
    inv="$(mktemp -t varve_inv.XXXXXX)"
    printf 'path|size|mtime\n' > "$inv"
    adapters=()
    oc_snap=""
    # ---- Claude Code (JSONL glob; only load the adapter if files exist so
    #      read_json is never handed an empty glob -- same guard as pi) ----
    if [ -d "{{cc_root}}" ] && find "{{cc_root}}" -name '*.jsonl' -type f -print -quit | grep -q .; then
      find "{{cc_root}}" -name '*.jsonl' -type f -exec stat "${stat_fmt[@]}" {} + >> "$inv"
      adapters+=(-f "{{repo}}/adapters/claude_code.sql")
    else
      echo "varve: skip claude-code -- no sessions at {{cc_root}}" >&2
    fi
    # ---- pi (JSONL glob; only load the adapter if files exist so read_json is
    #      never handed an empty glob) ----
    if [ -d "{{pi_root}}" ] && find "{{pi_root}}" -name '*.jsonl' -type f -print -quit | grep -q .; then
      find "{{pi_root}}" -name '*.jsonl' -type f -exec stat "${stat_fmt[@]}" {} + >> "$inv"
      adapters+=(-f "{{repo}}/adapters/pi.sql")
    else
      echo "varve: skip pi -- no sessions at {{pi_root}}" >&2
    fi
    # ---- opencode (SQLite; read-only clone snapshot) ----
    if [ -f "{{opencode_db}}" ]; then
      # Manifest keys on the LIVE db path (only .db; -wal/-shm mtimes drift
      # externally and would cause false "changed"). A change to the db marks all
      # opencode date partitions for rewrite (opencode is one file).
      stat "${stat_fmt[@]}" "{{opencode_db}}" >> "$inv"
      # APFS copy-on-write clone (cp -c: instant, no extra space) so ATTACH
      # READ_ONLY can neither write nor lock the operator's live opencode db.
      # Plain cp is the non-APFS fallback. Clone -wal/-shm too for WAL consistency.
      oc_snap="$(mktemp -d -t varve_ocsnap.XXXXXX)"
      cp -c "{{opencode_db}}" "$oc_snap/opencode.db" 2>/dev/null || cp "{{opencode_db}}" "$oc_snap/opencode.db"
      for ext in -wal -shm; do
        if [ -f "{{opencode_db}}$ext" ]; then
          cp -c "{{opencode_db}}$ext" "$oc_snap/opencode.db$ext" 2>/dev/null || cp "{{opencode_db}}$ext" "$oc_snap/opencode.db$ext"
        fi
      done
      # Defensive fallback for historical/partial stores: ensure both v1 and v2 contract tables
      # exist in the disposable clone before ATTACH READ_ONLY.
      if command -v sqlite3 >/dev/null 2>&1; then
        sqlite3 "$oc_snap/opencode.db" \
          "CREATE TABLE IF NOT EXISTS session (id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT, version TEXT, agent TEXT, model TEXT, time_created INTEGER);" \
          "CREATE TABLE IF NOT EXISTS session_v2 (id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT, version TEXT, agent TEXT, model TEXT, time_created INTEGER);" \
          "CREATE TABLE IF NOT EXISTS message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);" \
          "CREATE TABLE IF NOT EXISTS session_message (id TEXT PRIMARY KEY, session_id TEXT, type TEXT, time_created INTEGER, data TEXT);" \
          "CREATE TABLE IF NOT EXISTS part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT);" 2>/dev/null || true
      fi
      # ATTACH needs a literal path (no getvariable), so inject it here where the
      # snapshot path is known -- before opencode.sql, after the CC adapter.
      adapters+=(-c "INSTALL sqlite; LOAD sqlite; ATTACH '$oc_snap/opencode.db' AS oc (TYPE sqlite, READ_ONLY);" -f "{{repo}}/adapters/opencode.sql")
      echo "varve: opencode read-only clone -> $oc_snap" >&2
    else
      echo "varve: skip opencode -- no db at {{opencode_db}}" >&2
    fi
    # Derive + write affected partitions + refresh manifest, one DuckDB session.
    # Large-corpus safety: an in-memory session cannot spill without a
    # temp_directory, and insertion order buys nothing here (every COPY is
    # partitioned; readers sort) -- without both, a first backfill over a
    # multi-GB corpus exhausts the memory limit and dies.
    mkdir -p "{{state}}"; spill="$(mktemp -d "{{state}}/duckdb_tmp.XXXXXX")"
    trap 'rm -rf "$spill"' EXIT
    {{duckdb}} :memory: \
      -c "SET preserve_insertion_order=false; SET threads={{duckdb_threads}}; SET max_temp_directory_size='{{duckdb_max_spill}}'; SET temp_directory='$spill';" \
      -c "SET VARIABLE cc_source='{{cc_source}}'; SET VARIABLE pi_source='{{pi_source}}'; SET VARIABLE opencode_db='{{opencode_db}}'; SET VARIABLE repo_dir='{{repo}}'; SET VARIABLE attribution='{{attribution}}'; SET VARIABLE mode='{{mode}}'; SET VARIABLE inventory='$inv'; SET VARIABLE manifest='{{manifest}}';" \
      -f "{{repo}}/sql/prelude.sql" \
      ${adapters[@]+"${adapters[@]}"} \
      -f "{{repo}}/sql/stage.sql" \
      -f "{{repo}}/sql/affected.sql" \
      -c "COPY (SELECT * FROM stg_sessions   WHERE date IN (SELECT date FROM _affected)) TO '{{lake}}/sessions'   (FORMAT parquet, PARTITION_BY (runtime, date), FILENAME_PATTERN 'part', OVERWRITE_OR_IGNORE, COMPRESSION zstd);" \
      -c "COPY (SELECT * FROM stg_messages   WHERE date IN (SELECT date FROM _affected)) TO '{{lake}}/messages'   (FORMAT parquet, PARTITION_BY (runtime, date), FILENAME_PATTERN 'part', OVERWRITE_OR_IGNORE, COMPRESSION zstd);" \
      -c "COPY (SELECT * FROM stg_api_calls  WHERE date IN (SELECT date FROM _affected)) TO '{{lake}}/api_calls'  (FORMAT parquet, PARTITION_BY (runtime, date), FILENAME_PATTERN 'part', OVERWRITE_OR_IGNORE, COMPRESSION zstd);" \
      -c "COPY (SELECT * FROM stg_tool_calls WHERE date IN (SELECT date FROM _affected)) TO '{{lake}}/tool_calls' (FORMAT parquet, PARTITION_BY (runtime, date), FILENAME_PATTERN 'part', OVERWRITE_OR_IGNORE, COMPRESSION zstd);" \
      -c "COPY (SELECT path, size, mtime FROM _inventory) TO '{{manifest}}' (FORMAT parquet, OVERWRITE_OR_IGNORE);"
    rm -f "$inv"
    [ -n "$oc_snap" ] && rm -rf "$oc_snap" || true
    just _views
    just mine

# (Re)create the read-side views in varve.duckdb over the Parquet lake.
_views:
    {{duckdb}} "{{db}}" -c "SET VARIABLE lake='{{lake}}';" -f "{{repo}}/sql/views.sql"

# W7 correction mining. Rebuilds correction_candidates (four detectors, precision-first),
# operator_messages (G-recall-floor denominator: metadata-only, flagged = also a candidate
# locus) and promotes adjudicated candidates into correction_events -- all PURELY from the
# lake, so the rebuild is deterministic (byte-identical on an unchanged lake; no manifest
# interaction). _pipeline calls this AFTER the core tables + views are written; ingest/
# backfill remain the sole lake writers. The adjudication ledger is operator data outside
# the repo (see `adjudications`); when absent, an empty temp file is substituted so the
# read never errors -> zero promoted events. COPY targets are literals here (COPY TO cannot
# take getvariable), matching how _pipeline writes the core tables.
mine:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -d "{{lake}}/messages" ]; then
      echo "varve: skip mine -- no core tables at {{lake}}" >&2; exit 0
    fi
    adj="{{adjudications}}"
    tmp=""
    if [ ! -f "$adj" ]; then tmp="$(mktemp -t varve_adj.XXXXXX)"; adj="$tmp"; fi
    # Full-rebuild semantics need a clean target: these three are pure derivations
    # of the lake, and OVERWRITE_OR_IGNORE only replaces partitions the new run
    # writes -- a (runtime, date) partition that becomes EMPTY after a detector
    # change would otherwise survive as stale rows (observed when detector v2
    # landed: one v1 row lingered in a partition v2 no longer writes).
    rm -rf "{{lake}}/correction_candidates" "{{lake}}/operator_messages" "{{lake}}/correction_events"
    mkdir -p "{{state}}"; spill="$(mktemp -d "{{state}}/duckdb_tmp.XXXXXX")"
    trap 'rm -rf "$spill"' EXIT
    {{duckdb}} :memory: \
      -c "SET memory_limit='{{mine_memory_limit}}'; SET preserve_insertion_order=false; SET threads={{duckdb_threads}}; SET max_temp_directory_size='{{duckdb_max_spill}}'; SET temp_directory='$spill';" \
      -c "SET VARIABLE lake='{{lake}}'; SET VARIABLE repo_dir='{{repo}}'; SET VARIABLE adjudications='$adj';" \
      -f "{{repo}}/adapters/corrections.sql" \
      -c "COPY (SELECT * FROM stg_correction_candidates ORDER BY runtime, date, candidate_id) TO '{{lake}}/correction_candidates' (FORMAT parquet, PARTITION_BY (runtime, date), FILENAME_PATTERN 'part', OVERWRITE_OR_IGNORE, COMPRESSION zstd);" \
      -c "COPY (SELECT * FROM stg_operator_messages ORDER BY runtime, date, message_uuid) TO '{{lake}}/operator_messages' (FORMAT parquet, PARTITION_BY (runtime, date), FILENAME_PATTERN 'part', OVERWRITE_OR_IGNORE, COMPRESSION zstd);" \
      -c "COPY (SELECT * FROM stg_correction_events ORDER BY channel, date, event_id) TO '{{lake}}/correction_events' (FORMAT parquet, PARTITION_BY (channel, date), FILENAME_PATTERN 'part', OVERWRITE_OR_IGNORE, COMPRESSION zstd);"
    [ -n "$tmp" ] && rm -f "$tmp" || true
    # (Re)create the read-side W7 views over the just-written tables. CREATE VIEW binds its
    # glob eagerly, so EVERY table gets a view only once its dir holds parquet:
    # correction_events is empty until the first adjudication round, and a corpus that
    # trips zero detectors (or has no user messages at all) leaves candidates/
    # operator_messages fileless too -- an empty COPY writes no partition files.
    ev="DROP VIEW IF EXISTS correction_events;"
    if find "{{lake}}/correction_events" -name '*.parquet' -print -quit 2>/dev/null | grep -q .; then
      ev="CREATE OR REPLACE VIEW correction_events AS SELECT * FROM read_parquet('{{lake}}/correction_events/**/*.parquet', hive_partitioning=true, union_by_name=true);"
    fi
    cands="DROP VIEW IF EXISTS correction_candidates;"
    if find "{{lake}}/correction_candidates" -name '*.parquet' -print -quit 2>/dev/null | grep -q .; then
      cands="CREATE OR REPLACE VIEW correction_candidates AS SELECT * FROM read_parquet('{{lake}}/correction_candidates/**/*.parquet', hive_partitioning=true, union_by_name=true);"
    fi
    om="DROP VIEW IF EXISTS operator_messages;"
    if find "{{lake}}/operator_messages" -name '*.parquet' -print -quit 2>/dev/null | grep -q .; then
      om="CREATE OR REPLACE VIEW operator_messages AS SELECT * FROM read_parquet('{{lake}}/operator_messages/**/*.parquet', hive_partitioning=true, union_by_name=true);"
    fi
    {{duckdb}} "{{db}}" -c "$cands $om $ev"

# W7 adjudication REVIEW document (RFC-0002). Renders the operator's review sitting to
# <data_home>/review_round1.md (override with `out=`): one section per flagged correction
# candidate carrying the FULL prior draft + FULL corrective message + the model's advisory
# suggestion from the worksheet (round-0 finding: context_judgment is largely un-inferable
# from a 240-char excerpt, so the review dereferences the full loci), then the recall-floor
# sample (unflagged operator messages) as a second part. Candidates/loci already in the
# adjudications ledger are SKIPPED, so the doc shrinks as the operator works through it
# (idempotent re-render). CONFIDENTIALITY: the ONLY output is the markdown under the data
# home; adapters/review.sql holds zero transcript content, loci are the `lake:` form, and
# nothing is printed to a terminal. The worksheet is advisory operator data outside the repo
# (see `worksheet`); absence is non-fatal (a non-matching sentinel primes read_json_auto).
# The adjudication ledger is handled like `mine` (empty temp file when absent). COPY target
# is a literal here (COPY TO cannot take getvariable), matching how `mine` writes.
review out=(data_home + "/review_round1.md"):
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -d "{{lake}}/correction_candidates" ]; then
      echo "varve: skip review -- no correction_candidates at {{lake}} (run just mine first)" >&2; exit 0
    fi
    adj="{{adjudications}}"; adjtmp=""
    if [ ! -f "$adj" ]; then adjtmp="$(mktemp -t varve_adj.XXXXXX)"; adj="$adjtmp"; fi
    ws="{{worksheet}}"; wstmp=""
    if [ ! -s "$ws" ]; then
      # Absent/empty worksheet: prime read_json_auto with a non-matching sentinel row so the
      # advisory LEFT JOIN yields nothing (worksheet absence is non-fatal per RFC-0002 review).
      wstmp="$(mktemp -t varve_ws.XXXXXX)"
      printf '%s\n' '{"candidate_id":"__no_worksheet__","detector":"","suggested_verdict":"","suggested_context_judgment":"","rationale":"","suggested_by":"","model_id":"","suggested_at":""}' > "$wstmp"
      ws="$wstmp"
    fi
    mkdir -p "$(dirname "{{out}}")"
    {{duckdb}} :memory: \
      -c "SET VARIABLE lake='{{lake}}'; SET VARIABLE adjudications='$adj'; SET VARIABLE worksheet='$ws';" \
      -f "{{repo}}/adapters/review.sql" \
      -c "COPY (SELECT md FROM stg_review_document ORDER BY part, ord2, ord3) TO '{{out}}' (FORMAT csv, HEADER false, QUOTE '', ESCAPE '', DELIMITER E'\t');"
    [ -n "$adjtmp" ] && rm -f "$adjtmp" || true
    [ -n "$wstmp" ] && rm -f "$wstmp" || true
    echo "varve: wrote review -> {{out}}" >&2

# W7 durability-test input (RFC-0002): the intervention timeline for "does baking a
# correction into a rule stop it recurring" -- recovered from a RULEBOOK's own git
# history, no new capture surface. GENERIC: rulebook_dir is any git repo the operator
# points this at (their voicing/rules directory, e.g.); nothing here names a real path.
# Read-only -- shells `git log`, never writes to rulebook_dir; safe against a dirty
# working tree. Idempotent -- full rewrite of interventions.csv every run.
# Writes <data_home>/state/interventions.csv:
#   intervention_ref : commit sha            bake_date : ISO committer date (%cI)
#   rule_path        : one row per file touched (one row per commit x file)
#   subject          : commit subject line
# Usage: just interventions ~/path/to/rulebook-repo
interventions rulebook_dir:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "{{state}}"
    out="{{state}}/interventions.csv"
    tmp="$(mktemp -t varve_interventions.XXXXXX)"
    printf 'intervention_ref,bake_date,rule_path,subject\n' > "$tmp"
    git -C "$1" log --format='%H%x09%cI%x09%s' --name-only --reverse | awk -F'\t' '
      NF == 3 { href = $1; date = $2; subj = $3; next }
      NF == 1 && $1 != "" {
        s = subj; gsub(/"/, "\"\"", s)
        p = $1;   gsub(/"/, "\"\"", p)
        printf "%s,%s,\"%s\",\"%s\"\n", href, date, p, s
      }
    ' >> "$tmp"
    mv "$tmp" "$out"
    echo "varve: wrote $(($(wc -l < "$out") - 1)) row(s) -> $out" >&2

# Open DuckDB READ-ONLY with lake views attached. Pass args, e.g.
#   just sql -c "SELECT count(*) FROM api_calls;"
#   just sql -f queries/burn_by_day.sql
sql *ARGS:
    {{duckdb}} -readonly "{{db}}" -c "SET VARIABLE lake='{{lake}}';" "$@"

# Burn by estate/campaign/model/day (W1 acceptance query).
burn:
    @{{duckdb}} -readonly "{{db}}" -c "SET VARIABLE lake='{{lake}}';" -f "{{repo}}/queries/burn_by_day.sql"

# varve per-model token+cost totals for reconciliation vs ccusage.
recon:
    @{{duckdb}} -readonly "{{db}}" -c "SET VARIABLE lake='{{lake}}';" -f "{{repo}}/queries/reconcile_ccusage.sql"

# pi inline-cost vs seed-computed-cost delta (empty until a pi corpus exists).
recon-pi:
    @{{duckdb}} -readonly "{{db}}" -c "SET VARIABLE lake='{{lake}}'; SET VARIABLE repo='{{repo}}';" -f "{{repo}}/queries/reconcile_pi_cost.sql"

# Year in review: a local-time activity heatmap plus year totals (ADR-0007).
#   just stats [--year N] [--runtime claude|pi|opencode|all] [--metric prompts|steps|tokens]
# VARVE_TODAY (YYYY-MM-DD) pins "today" for the current-year cut.
stats *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail
    year="$(date +%Y)"; runtime=all; metric=prompts
    while [ $# -gt 0 ]; do
      case "$1" in
        --year|--runtime|--metric)
          [ $# -ge 2 ] || { echo "varve stats: $1 needs a value" >&2; exit 2; }
          printf -v "${1#--}" '%s' "$2"; shift 2 ;;
        --year=*|--runtime=*|--metric=*)
          key="${1%%=*}"; printf -v "${key#--}" '%s' "${1#*=}"; shift ;;
        *) echo "varve stats: unknown argument $1" >&2; exit 2 ;;
      esac
    done
    [[ "$year" =~ ^[0-9]{4}$ ]] || { echo "varve stats: --year takes a four-digit year" >&2; exit 2; }
    case "$runtime" in claude|pi|opencode|all) ;; *) echo "varve stats: --runtime takes claude|pi|opencode|all" >&2; exit 2 ;; esac
    case "$metric" in prompts|steps|tokens) ;; *) echo "varve stats: --metric takes prompts|steps|tokens" >&2; exit 2 ;; esac
    today="${VARVE_TODAY:-$(date +%F)}"
    {{duckdb}} -readonly "{{db}}" -noheader -list \
      -c "SET VARIABLE lake='{{lake}}'; SET VARIABLE year=$year; SET VARIABLE runtime='$runtime'; SET VARIABLE metric='$metric'; SET VARIABLE today=DATE '$today';" \
      -c ".output /dev/null" -f "{{repo}}/queries/stats_year.sql" \
      -c ".output" -f "{{repo}}/sql/stats_render.sql"

# Report lake size; tripwire at ~100 GB (PRD D4: keep-everything is cheap
# at this scale, the watch is the control).
disk:
    @du -sh {{data_home}} 2>/dev/null || echo "no lake yet at {{data_home}}"
    @kb="$(du -sk {{data_home}} 2>/dev/null | cut -f1)"; if [ "${kb:-0}" -ge 104857600 ]; then echo "varve disk: TRIPWIRE -- data home exceeds ~100 GB (PRD D4 disk watch)"; fi

# CI/local lint: syntax-validate the justfile itself. `--evaluate` parses every
# recipe body and evaluates the top-level variable assignments, so a broken
# interpolation or a malformed recipe declaration fails here (verified: an
# unclosed `{{` and a stray `:` both fail `--evaluate` with a parser error).
# Deliberately NOT `just --fmt --check`: this file's aligned `:=` blocks are a
# readability choice, and fmt's canonical reformatting would flatten them --
# that is a style opinion, not a defect, so it stays off the merge gate.
# mise.toml resolving is validated for free: every CI job's `mise install`
# fails outright on an unresolvable duckdb/just pin before any recipe runs.
lint:
    just --evaluate > /dev/null
    @echo "varve lint: justfile OK"

# CI/local test: the real assertion behind `lint`. Runs the full pipeline
# (backfill -> mine -> views) against a synthetic three-runtime fixture
# (tests/fixtures/), isolated under a scratch VARVE_DATA so it never touches
# the operator's real lake, then executes every file in queries/ plus `just
# review` against the result. This exercises every statement in adapters/*.sql
# and sql/*.sql for real against DuckDB (the actual engine, not a generic SQL
# linter) -- a broken statement fails the run, not just a parse-only check.
test:
    #!/usr/bin/env bash
    set -euo pipefail
    tmp="$(mktemp -d -t varve_test.XXXXXX)"
    trap 'rm -rf "$tmp"' EXIT
    export VARVE_DATA="$tmp/data"
    export VARVE_CC_ROOT="{{repo}}/tests/fixtures/cc"
    export VARVE_PI_ROOT="{{repo}}/tests/fixtures/pi"
    export VARVE_OPENCODE_DB="$tmp/opencode.db"
    export VARVE_ATTRIBUTION="{{repo}}/adapters/attribution.example.csv"
    export VARVE_ADJUDICATIONS="$tmp/no_adjudications.jsonl"
    export VARVE_WORKSHEET="$tmp/no_worksheet.jsonl"
    export TZ=UTC

    echo "varve test: seeding synthetic opencode fixture db" >&2
    {{duckdb}} :memory: \
      -c "INSTALL sqlite; LOAD sqlite; ATTACH '$VARVE_OPENCODE_DB' AS oc (TYPE sqlite);" \
      -f "{{repo}}/tests/fixtures/opencode_seed.sql"

    echo "varve test: full backfill over the synthetic fixture" >&2
    just backfill

    echo "varve test: asserting every runtime actually produced rows (a" >&2
    echo "  silently-skipped adapter would otherwise pass unnoticed)" >&2
    for rt in claude-code pi opencode; do
      n="$(just sql -noheader -list -c "SELECT count(*) FROM sessions WHERE runtime='$rt';")"
      echo "  $rt: $n session(s)" >&2
      if [ "${n:-0}" -lt 1 ]; then
        echo "FAIL: $rt produced zero sessions in the fixture backfill" >&2
        exit 1
      fi
    done
    v1_n="$(just sql -noheader -list -c "SELECT count(*) FROM sessions WHERE session_id='oc-session-1';")"
    v2_n="$(just sql -noheader -list -c "SELECT count(*) FROM sessions WHERE session_id='oc-session-v2';")"
    v2_tools="$(just sql -noheader -list -c "SELECT count(*) FROM tool_calls WHERE session_id='oc-session-v2';")"
    if [ "${v1_n:-0}" -ne 1 ] || [ "${v2_n:-0}" -ne 1 ] || [ "${v2_tools:-0}" -ne 1 ]; then
      echo "FAIL: opencode v1/v2 session or tool assertion failed: v1=$v1_n, v2=$v2_n, v2_tools=$v2_tools" >&2
      exit 1
    fi
    cand="$(just sql -noheader -list -c "SELECT count(*) FROM correction_candidates;")"
    if [ "${cand:-0}" -lt 1 ]; then
      echo "FAIL: correction_candidates is empty -- the fixture no longer trips" >&2
      echo "      a W7 detector (mine's correction_candidates view creation" >&2
      echo "      requires at least one row; see justfile _views comment)" >&2
      exit 1
    fi
    # session-copy.jsonl repeats lines of session.jsonl under a later-starting,
    # lower-sorting session id; the copy must count once, in the earliest-started session.
    m="message_uuid = '33333333-3333-3333-3333-333333333333'"
    owners="$(just sql -noheader -list -c "SELECT (SELECT string_agg(DISTINCT session_id, ',') FROM correction_candidates WHERE $m) || ' ' || (SELECT string_agg(session_id, ',' ORDER BY session_id) FROM operator_messages WHERE $m);")"
    if [ "$owners" != "test-session-1 test-session-1" ]; then
      echo "FAIL: copied message attributed to '$owners' (candidates operator_messages)," >&2
      echo "      want test-session-1 once in each" >&2
      exit 1
    fi

    echo "varve test: executing every queries/*.sql against the fixture" >&2
    fail=0
    for f in "{{repo}}"/queries/*.sql; do
      echo "  -- $f" >&2
      if ! just sql -c "SET VARIABLE repo='{{repo}}';" -f "$f" > /dev/null; then
        echo "FAIL: $f did not execute cleanly against DuckDB" >&2
        fail=1
      fi
    done

    echo "varve test: just review (adapters/review.sql)" >&2
    just review "$tmp/review.md" > /dev/null

    echo "varve test: just stats (snapshot, year-boundary streak, empty year, runtime filter)" >&2
    export VARVE_TODAY=2026-10-13
    if ! just stats --year 2026 | diff -u "{{repo}}/tests/snapshots/stats_2026.txt" - >&2; then
      echo "FAIL: just stats --year 2026 differs from tests/snapshots/stats_2026.txt" >&2
      fail=1
    fi
    stats_tail() {
      local want="$1"; shift
      local got; got="$(just stats "$@" | tail -3)"
      if [ "$got" != "$want" ]; then
        echo "FAIL: just stats $* ended with:" >&2; printf '%s\n' "$got" >&2
        fail=1
      fi
    }
    stats_tail $'1 session · 0 subagents\n2 prompts · 3 steps · 4.7k tokens · $0.00\n100.0% tool success · 2 active days · best streak 2 days' --year 2025
    stats_tail $'0 sessions · 0 subagents\n0 prompts · 0 steps · 0 tokens · $0.00\nn/a tool success · 0 active days · best streak 0 days' --year 2024
    stats_tail $'1 session · 0 subagents\n1 prompt · 1 step · 150 tokens · $0.00\n100.0% tool success · 1 active day · best streak 1 day' --year 2026 --runtime pi

    exit $fail
