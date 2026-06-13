# Project Context and Architecture: Local Data Lakehouse

## Overview
I am a Senior Data Engineer building an end-to-end local Data Lakehouse to learn the modern single-node data stack.

Do **NOT** use Apache Spark for this project. The entire architecture is designed around single-node, vectorized execution using open-table formats.

## Tech Stack
* **Python:** 3.12 (pinned — best wheel coverage for Polars/DuckDB/Dagster as of 2026)
* **Extract & Load (EL):** Python and Polars
* **Transform (T):** dbt with the `dbt-duckdb` adapter
* **Query Engine:** DuckDB (persistent file `data/lakehouse.duckdb` — see Implementation Notes #2)
* **Orchestration:** Dagster (`dagster-dbt` + `dagster-polars[deltalake]`)
* **Storage Formats:**
  * **Raw layer:** Delta Lake (Polars writes via `PolarsDeltaIOManager`) — transactional ingest, schema evolution, time travel
  * **Staging & marts:** Parquet via dbt-duckdb's `external` materialization — canonical dbt-duckdb write path, no Python-model boilerplate
  * **Rationale:** no open table format has clean write support across BOTH Polars and dbt-duckdb. This hybrid plays to each tool's strengths and mirrors the dominant production pattern at small/medium scale: transactional bronze + columnar silver/gold.
* **Visualization:** [Evidence.dev](https://evidence.dev) — code-first BI. Markdown + SQL files become a static HTML site. Connects to `lakehouse.duckdb`. Lives in `evidence/`. Run with `cd evidence && npm install && npm run dev`.
* **Package Management:** `uv` (Astral) for Python; `npm` for the Evidence layer
* **Deployment:** Docker (`docker-compose`) for the Python pipeline. Evidence runs separately as a Node dev server (no Docker container needed — it builds to static files).

## Initial Dataset
**Formula 1** via the **Jolpica API** at `https://api.jolpi.ca/ergast/f1` — a community-maintained drop-in mirror of the now-deprecated Ergast API. Same URL paths, same JSON shape, so any Ergast docs still apply.

Currently ingesting one season (2024): races, drivers, results. Lap-by-lap timings were de-scoped because per-race pagination overwhelmed Jolpica's free tier. They can be added later (see Implementation Notes #5 for the constraint).

## Pipeline Architecture & Data Flow

### 1. Extract and Load (Polars + Dagster)
* Hit the Jolpica REST API with `httpx` from inside Dagster Software-Defined Assets (`@asset`).
* Polars handles all the DataFrame shaping — `pl.DataFrame(records)` keeps nested objects as struct columns; flattening happens in staging.
* **Storage:** Write to Delta Lake in `data/raw/`. The `PolarsDeltaIOManager` resource handles all filesystem ops — `@asset` functions never touch the disk directly.
* A pagination helper (`_fetch_paginated`) handles Jolpica's offset paging + retries on transient timeouts (Jolpica's free tier occasionally stalls on heavier endpoints).

### 2. Transform (dbt + DuckDB)
* **Read** Delta tables from `data/raw/` via dbt-duckdb's `delta` plugin. Each Delta source is declared in `models/staging/sources.yml` with `plugin: delta` and a per-table `delta_table_path` (see Implementation Notes #4 and #5).
* **Compute** in DuckDB. Useful idioms in our SQL:
  * `TRY_CAST` for fault-tolerant casts (e.g., `position = "R"` for retired drivers → NULL).
  * Bracket syntax for struct field access (`Driver['driverId']`).
  * DuckDB-specific aggregates: `QUALIFY` (window-based row-picking), `MODE`, `COUNT_IF`, `MEDIAN`.
* **Write** staging/marts via dbt-duckdb's `external` materialization → single Parquet files at `data/staging/<model>.parquet` and `data/marts/<model>.parquet`. Each model's SQL sets `location` in its `{{ config(...) }}` block (can't set at project level — `this.name` isn't available at parse time).
* No partitioning yet — single files are fine at this volume. Hive partitioning is a future enhancement once we add multiple seasons.

### 3. Orchestration (Dagster)
* Polars assets and dbt models share one asset graph via `dagster-dbt`'s `@dbt_assets` decorator (see `pipelines/definitions.py`).
* A custom `DagsterDbtTranslator` strips the source-group prefix so dbt source `source('raw', 'raw_races')` resolves to the same Dagster AssetKey as the Polars `raw_races` asset — unified lineage, one node per logical dataset.
* **Concurrency rule:** never let two assets write to the same table path simultaneously. Dagster's DAG enforces upstream→downstream ordering. For dbt-side concurrency, see Implementation Notes #1.

## Implementation Notes (Hard-won gotchas)

Each of these cost real debugging time. Future-you (or future-Claude): read this section before touching `profiles.yml` or the source declarations.

1. **`threads: 1` in `profiles.yml` is mandatory.** dbt-duckdb's `delta` plugin can't safely register multiple Delta sources concurrently — each worker thread tries to `CREATE SCHEMA "raw"` and they collide in DuckDB's catalog (`TransactionContext write-write conflict`). Single-threaded execution adds <1s to a full build at this scale.

2. **Persistent DuckDB file required (NOT `:memory:`).** dbt-duckdb's `external` materialization writes Parquet AND creates a DuckDB VIEW over it. The view only exists for the current dbt invocation. With `:memory:`, a selective build like `dbt build --select mart_x` fails because the view for upstream `stg_y` was never recreated. We use `path: data/lakehouse.duckdb` so views survive across invocations. The file is dbt engine state — not lakehouse data — and is gitignored via `data/`.

3. **First materialization must be full ("Materialize all").** Even with persistent DuckDB, the very first dbt run needs every model so all views get created. After that, partial materializations from Dagster's UI work fine.

4. **`PolarsDeltaIOManager` appends `.delta` to directory names.** The Delta table for asset `raw_races` lives at `data/raw/raw_races.delta/`, not `data/raw/raw_races/`. The dbt source's `delta_table_path` must include the suffix.

5. **dbt source declaration for Delta is plugin-specific.** Not just `external_location` with `formatter: oldstyle` — that's for reading Parquet/CSV. Delta sources need `plugin: delta` AND `delta_table_path: <full path>`. The plugin doesn't auto-substitute `{name}`, so each source table spells out its own path.

6. **Evidence install needs `--force`, not `--legacy-peer-deps`.** Evidence's transitive deps have version conflicts under strict peer-dep resolution (Node 18+, npm 7+). `--legacy-peer-deps` skips peer deps entirely → `@sveltejs/vite-plugin-svelte` gets dropped → dev server can't start. `--force` accepts conflicts but still installs peer deps. The package.json mirrors Evidence's official template (all 11 connectors listed) — only DuckDB is actively used, but the template's versions are known to install together.

7. **Evidence uses port 3001** (not its default 3000) to avoid colliding with Dagster's web UI. Set via `--port 3001` in the `dev` script in `evidence/package.json`. If Dagster is stopped you can reclaim 3000, but coexistence is the common case.

8. **`filename` in Evidence's source `connection.yaml` resolves relative to the connection file's directory** — NOT the evidence/ project root. From `evidence/sources/lakehouse/connection.yaml`, three `../`s get you to project root, then `data/lakehouse.duckdb`.

9. **Bump Docker's `/dev/shm` to at least 2 GB.** Docker's default is 64 MB, which is too small for Polars/Delta operations that memory-map files. Symptom: `SIGBUS` (signal 7) inside Dagster's `raw_*` or `f1_dbt_assets` steps. Fix is one line in `docker-compose.yml`: `shm_size: 2gb`.

10. **Evidence's "queryable tables" are NOT direct table references — they're source-query files.** Writing `FROM lakehouse.foo` in a dashboard requires `evidence/sources/lakehouse/foo.sql` to exist (typically `SELECT * FROM foo`). Evidence runs that query during `npm run sources`, caches the result, and dashboard queries hit the cache. Without the source file, you get `Catalog Error: Table with name foo does not exist`.

11. **Open the Evidence DuckDB connection in `READ_ONLY` mode.** Set `access_mode: READ_ONLY` under `options:` in `connection.yaml`. Otherwise Evidence takes a write lock that blocks dbt — on macOS Docker, manifests as SIGBUS rather than a clean lock error. READ_ONLY makes Evidence + Dagster-in-Docker coexist safely.

## Current Asset Inventory

| Layer | Asset | Grain | Origin |
|---|---|---|---|
| Bronze | `raw_races` | one row per race | Jolpica `/2024/races.json` |
| Bronze | `raw_drivers` | one row per driver | Jolpica `/2024/drivers.json` |
| Bronze | `raw_results` | one row per (race, driver) | Jolpica `/2024/results.json` |
| Silver | `stg_races` | one row per race (typed, flattened) | dbt: refs `raw_races` |
| Silver | `stg_drivers` | one row per driver | dbt: refs `raw_drivers` |
| Silver | `stg_results` | one row per (race, driver) | dbt: refs `raw_results` |
| Gold | `mart_country_race_summary` | one row per country | dbt: refs `stg_races` |
| Gold | `mart_driver_standings` | one row per driver | dbt: refs `stg_drivers` + `stg_results` |

## Infrastructure & Packaging
* The pipeline must be containerized using a single Docker image. (Dockerfile + docker-compose.yml exist but the build hasn't been validated end-to-end yet.)
* We are strictly using **`uv`** for Python package management. Containerizing with `uv` ensures deterministic, lightning-fast builds whether developing on a Windows laptop or an Apple Silicon M4 desktop, eliminating architecture-specific wheel compilation bottlenecks for Polars and DuckDB's Rust/C++ extensions.
* Use `docker-compose` for local orchestration.
* **Bind mounts:** map host directories into the container so state survives container restarts and is inspectable from the IDE:
  * `./data:/opt/dagster/app/data` — the Delta+Parquet lakehouse
  * `./.dagster_home:/opt/dagster/dagster_home` — Dagster's run/event/asset storage (set `DAGSTER_HOME` env var to this path inside the container)
  * `./pipelines` and `./dbt_project` — source dirs bind-mounted in dev so edits hot-reload without rebuilding the image

## Target Directory Structure
```text
project_root/
├── data/                                  # Bind-mounted lakehouse (gitignored)
│   ├── raw/                               # Bronze (Delta — *.delta/ directories)
│   │   ├── raw_races.delta/
│   │   ├── raw_drivers.delta/
│   │   └── raw_results.delta/
│   ├── staging/                           # Silver (single Parquet files)
│   │   ├── stg_races.parquet
│   │   ├── stg_drivers.parquet
│   │   └── stg_results.parquet
│   ├── marts/                             # Gold (single Parquet files)
│   │   ├── mart_country_race_summary.parquet
│   │   └── mart_driver_standings.parquet
│   └── lakehouse.duckdb                   # dbt's persistent view catalog (Implementation Note #2)
├── pipelines/                             # Dagster project
│   ├── assets/
│   │   └── raw.py                         # All Polars extract assets + paginated HTTP helper
│   └── definitions.py                     # Dagster + dbt wiring (assets, IO managers, DbtCliResource)
├── dbt_project/                           # dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml                # Delta source declarations (plugin: delta + delta_table_path)
│   │   │   ├── stg_races.sql
│   │   │   ├── stg_drivers.sql
│   │   │   └── stg_results.sql
│   │   └── marts/
│   │       ├── schema.yml                 # Docs + tests for marts
│   │       ├── mart_country_race_summary.sql
│   │       └── mart_driver_standings.sql
│   ├── profiles.yml                       # dbt-duckdb config (persistent file, threads=1, delta plugin)
│   └── dbt_project.yml
├── notebooks/
│   └── 01_explore.ipynb                   # VS Code / JupyterLab scratchpad — bronze, silver, gold tours
├── evidence/                              # Code-first BI dashboards (Markdown + SQL)
│   ├── sources/lakehouse/connection.yaml  # DuckDB connection → ../data/lakehouse.duckdb
│   ├── pages/
│   │   ├── index.md                       # Overview dashboard
│   │   ├── drivers.md                     # Championship standings
│   │   └── countries.md                   # Geographic summary
│   └── package.json                       # npm deps (Evidence core + DuckDB connector)
├── .dagster_home/                         # Dagster instance state (bind mounted, gitignored)
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml                         # Managed by uv (Python 3.12)
├── uv.lock
├── README.md                              # Human-facing: setup, how to run, troubleshooting
├── LEARN.md                               # Beginner walkthrough + tool explainers + homework exercise
└── CLAUDE.md                              # This file: architecture & intent for Claude
```

## Documentation
Three docs, three audiences:
* **CLAUDE.md** (this file) — architecture, decisions, intent, gotchas. For future Claude sessions. Update as decisions evolve.
* **README.md** — human-facing setup steps, how to run, troubleshooting. The "I just want to run it" reference.
* **LEARN.md** — beginner-friendly walkthrough of what we built and why, plain-English tool explainers with doc links, and a homework exercise. The "I want to understand this and extend it on my own" reference.
