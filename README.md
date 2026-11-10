![hero](hero.svg)

# varve

> [!NOTE]
> **Fork Notice**: Do not clone or use this fork as a divergent codebase. Following upstream Varve's modular architecture redesign, the core engine remains pristine at [andrewdunndev/varve](https://github.com/andrewdunndev/varve) (canonical at [dunn.dev/varve](https://gitlab.com/dunn.dev/varve)). Use upstream Varve as the core lake engine combined with the standalone companion extensions repository: [**varve-coviant-extensions**](https://github.com/knightb8/varve-coviant-extensions), which provides plug-and-play runtime adapters (Google Antigravity, Mistral Vibe, GitHub Copilot, OpenCode dual-schema v1+v2) and operator tools designed to load via `$VARVE_DATA/adapters/` with zero core modifications.

[![pipeline](https://gitlab.com/dunn.dev/varve/badges/main/pipeline.svg)](https://gitlab.com/dunn.dev/varve/-/pipelines)
[![license](https://img.shields.io/badge/license-MIT-green)](LICENSE)
![built with GitLab](https://img.shields.io/badge/built_with-GitLab-FC6D26?logo=gitlab)

**Status: built for and used by its author, maintained occasionally.** CI tests every change. It has no support, roadmap or releases.

We'd like to hear how you use it: open an issue on GitLab or GitHub. Core fixes are welcome as merge requests. We're moving toward a small, solid set of composable primitives. New runtimes and mines live in your own data home; the ones in this repo are examples.

A varve is one annual sediment layer in a glacial lake. Scientists read the
sequence to reconstruct past climate. varve is that instrument for coding-agent
sessions: a local lake of everything your agents did, in readable layers, mined
for signal.

It normalizes the on-disk session records of Claude Code, pi and opencode into
Parquet partitions and queries them with DuckDB. Files are canonical; the engine
is disposable. It runs no daemon, opens no port and sends nothing off the
machine. One exception: the opencode adapter's `INSTALL sqlite` downloads the
extension from DuckDB's extension host when it is not already installed; no
session data goes with it.

The core answers efficiency questions from history: cost, cache use, dispatch
shape. Layers over the same four tables ask whether corrections land once and
hold, when context engineering has saturated for a team, and whether the record
can serve as simulation evidence and leak-free benchmark material.

## Architecture

`just sync` copies sources into a mirror. `just build` rebuilds the lake from it, checks it, swaps it in, then runs the mines (ADR-0008 to ADR-0010).

![The runtime stores are copied into the mirror, built into a new lake, checked, swapped in and mined; the data home sits apart from the repo.](docs/img/system.svg)

Sync never opens the engine.

Three primitives: files, SQL files, one runner (`bin/duck`). Everything extensible is a SQL file, run in a fixed order: adapters, checks, swap, mines.

| Kind | Reads | Rows become |
|------|-------|-------------|
| adapter | the mirror | the four tables |
| check | the tables | failures |
| mine | the tables | a saved table |
| query | the tables | printed output |

![Each SQL file kind, what it reads and writes, when it runs and where it is looked up; a new runtime is a row and an adapter in the data home; a contributed one also adds a fixture.](docs/img/files.svg)

A new runtime is a row in `runtimes.toml` and an adapter, both in the data home.

![The per-file decision in bin/sync and the replace set that moves a database and its journal together.](docs/img/sync.svg)

A copy only ever grows, except the replace set, which is swapped as a unit.

![One build as a sequence: a failed runtime keeps its published partitions, a failed check before the swap leaves the lake untouched, a rename swaps the new lake in.](docs/img/build.svg)

A failed runtime keeps its published partitions.

![The four tables with their columns grouped by meaning, the joins between them, and the rules every adapter keeps.](docs/img/record.svg)

Ids come from the source and unknown is NULL.

![Every SQL run passes through bin/duck: the lock, the caps, the watch loop, the exit codes and the one failure line.](docs/img/envelope.svg)

One wrapper bounds every DuckDB process.

## Layout

| Path | What |
|------|------|
| `adapters/` | One normalizer per runtime (Claude Code, pi, opencode), plus example pricing and attribution tables |
| `sql/` | Shared adapter prelude and the written meaning of every column (`record.sql`) |
| `queries/` | The versioned SQL query library |
| `mining/` | Example mines (one detector per file), example patterns and taxonomy, review rendering, year-in-review stats and their queries |
| `docs/` | Goals (`prd.md`), ADRs (`adr/`), the correction-mining method (`rfc/`), query index |
| `checks/` | Checks that run on every build |
| `tests/` | Synthetic three-runtime fixtures and expected output for `just test` |
| `bin/` | The runner (`duck`) and the scripts behind `sync`, `build`, `check`, `forget` and `mine` |
| `justfile` | `sync`, `build`, `check`, `forget`, `mine`, `review`, `interventions`, `stats`, `sql`, `lint`, `test`, `disk` |

`mise install` provisions the pinned `duckdb`, `just` and `yq` from `mise.toml`. A `duckdb` or `yq` on `PATH` is used only when it reports the pinned version.

To start, copy the examples into the data home, delete the `runtimes.toml` rows for tools you do not use, then sync and build. `just` must be on `PATH`, for example through `mise activate`.

```
mise install
d=~/.local/share/varve; mkdir -p "$d"
for f in runtimes.example.toml adapters/attribution.example.toml adapters/pricing.example.toml; do
  cp "$f" "$d/$(basename "$f" .example.toml).toml"
done
just sync && just build
```

## Write your own adapter

The three adapters are examples. To add a runtime, copy the closest one to `<data home>/adapters/`, edit it, and add a row to `<data home>/runtimes.toml` naming it. Then `just sync && just build`. Every build grades the lake against `sql/record.sql` (`bin/check`), so a wrong column fails the build. The repo needs no change. The one compatibility rule: a column's meaning never changes, and a new meaning gets a new name.

## Mining

The repo's `mining/` holds example mines, one detector per file. To use them, copy
`mining/[0-9]*.sql` into `mining/` in your data home. Copy `patterns.example.toml`
and `divergence_taxonomy.example.toml` to the data-home root (next to `runtimes.toml`, not in `mining/`) without the `.example`, and tune the
patterns. `just mine` runs your files in filename order. Each saves one table under
the lake: `NN-<table>[.<part>].sql` leaves its rows in `mine_out` and lands in
`<table>/<part>.parquet`. A mine's version is the hash of its file. A failing mine
keeps its last good file and the others still run. `just review` renders the
adjudication document from the mined tables.

Lake data never lives in this repo. It lands in `~/.local/share/varve/`, and so
does your own data: attribution rules, adjudications, worksheets (ADR-0004). The
instrument is public; the readings are not.

## Contributing

Open an issue on GitLab or GitHub to tell us how you use varve. Core fixes are welcome as merge requests. GitLab is canonical; GitHub is a push mirror. New runtimes live in your data home, with no repo change (see [Write your own adapter](#write-your-own-adapter)).
