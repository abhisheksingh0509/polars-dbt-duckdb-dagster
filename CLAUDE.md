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
* **Read** Delta tables from `data/raw/<dataset>/` via dbt-duckdb's `delta` plugin. Each Delta source is declared in `models/<dataset>/staging/sources.yml` (source group named after the dataset) with `plugin: delta` and a per-table `delta_table_path` (see Implementation Notes #4 and #5).
* **Compute** in DuckDB. Useful idioms in our SQL:
  * `TRY_CAST` for fault-tolerant casts (e.g., `position = "R"` for retired drivers → NULL).
  * Bracket syntax for struct field access (`Driver['driverId']`).
  * DuckDB-specific aggregates: `QUALIFY` (window-based row-picking), `MODE`, `COUNT_IF`, `MEDIAN`.
* **Write** staging/marts via dbt-duckdb's `external` materialization → single Parquet files at `data/staging/<dataset>/<model>.parquet` and `data/marts/<dataset>/<model>.parquet`. Each model sets `location` via the `dataset_location(layer)` macro (`macros/dataset_location.sql`), which derives the dataset from the model's folder — no hardcoded paths per model (can't set at project level — `this.name` isn't available at parse time; see Implementation Notes #12).
* No partitioning yet — single files are fine at this volume. Hive partitioning is a future enhancement once we add multiple seasons.

### 3. Orchestration (Dagster)
* Polars assets and dbt models share one asset graph via `dagster-dbt`'s `@dbt_assets` decorator (see `pipelines/definitions.py`).
* A custom `DagsterDbtTranslator` maps each dbt source onto its dataset-prefixed bronze asset key — the dbt source group is the dataset name, so `source('f1', 'raw_races')` resolves to the same Dagster AssetKey `["f1","raw_races"]` as the Polars asset — unified lineage, one node per logical dataset.
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

12. **Don't name a macro `external_location` — dbt-duckdb already defines one.** Our per-dataset Parquet path macro lives in `macros/dataset_location.sql` and is called `dataset_location(layer)`. It was originally named `external_location`, which silently **shadowed** dbt-duckdb's built-in `external_location` macro (the `external` materialization calls it internally with 2 args). Symptom: `macro 'dbt_macro__external_location' takes not more than 1 argument(s)` at build time, raised from inside `materialization_external_duckdb`. The macro derives the dataset from `model.fqn[1]` (our layout is `models/<dataset>/<layer>/`), so models call `dataset_location('staging' | 'marts')` with no hardcoded dataset name.

## Stack vs Dataset (the framework split)

The codebase is split into a reusable **stack** (the engine) and one-or-more **datasets**
(the payloads). The stack is domain-blind; a dataset is everything specific to one data
source. Adding a dataset (e.g. NYC taxi) is additive — you don't touch the stack.

* **Stack** (`pipelines/stack/`): the engine.
  * `extractors.py` — `Extractor` protocol + `RestApiExtractor`. An extractor's only job
    is "get records from somewhere." A future `FileExtractor` (CSV/Parquet/S3) drops in
    here without touching anything else.
  * `specs.py` — `SourceSpec(name, extractor, shape=None, group="raw")`. The bronze
    contract a dataset declares per raw table. Principle: **config for shape, named Python
    function (`shape`) as the escape hatch for per-source logic** — no in-house DSL.
  * `raw_assets.py` — `build_raw_assets(dataset, specs)` turns each `SourceSpec` into a
    Dagster bronze `@asset`, keyed `[dataset, name]`.
  * `dbt.py` — `LakehouseDbtTranslator`, maps dbt sources to the bronze asset keys.
* **Dataset** (`pipelines/datasets/<name>/sources.py`): exports `DATASET` (the name) and
  `SOURCES` (a `list[SourceSpec]`). Plus its dbt models under
  `dbt_project/models/<name>/` and Evidence pages under `evidence/pages/<name>/`.
* **Assembler** (`pipelines/definitions.py`): thin. Imports each dataset, calls
  `build_raw_assets(...)`, wires the IO manager + dbt. Register a dataset by adding it to
  the `DATASETS` list.

**Namespacing trick (the linchpin):** bronze assets get `key_prefix=[dataset]`, so asset
key `["f1","raw_races"]`. `PolarsDeltaIOManager` is a `UPathIOManager` (writes to
`base_dir / *asset_key.path`), so the prefix alone lands the Delta table at
`data/raw/f1/raw_races.delta/` — no per-dataset IO manager. The dbt source **group name
is the dataset name** (`source('f1', 'raw_races')`), which the translator maps to the
same `["f1","raw_races"]` key → unified lineage. dbt models set `location` via the
`dataset_location(layer)` macro, which derives the dataset from the model's folder and
returns `data/<layer>/<dataset>/<model>.parquet` — no per-model hardcoded paths.

**To add a dataset:** (1) `pipelines/datasets/<name>/sources.py` with `DATASET` +
`SOURCES`; (2) add it to `DATASETS` in `definitions.py`; (3) `dbt_project/models/<name>/`
(staging + marts + a `sources.yml` whose source group is named `<name>`); (4) add a
`models/<name>/` block in `dbt_project.yml`; (5) `evidence/pages/<name>/` + a link on the
Evidence hub. The stack stays untouched.

## Current Asset Inventory

Bronze asset keys are namespaced by dataset (`f1/raw_races`); dbt models keep their plain
names but materialize under per-dataset paths.

| Layer | Asset | Grain | Origin |
|---|---|---|---|
| Bronze | `f1/raw_races` | one row per race | Jolpica `/2024/races.json` |
| Bronze | `f1/raw_drivers` | one row per driver | Jolpica `/2024/drivers.json` |
| Bronze | `f1/raw_results` | one row per (race, driver) | Jolpica `/2024/results.json` |
| Silver | `stg_races` | one row per race (typed, flattened) | dbt: refs `f1/raw_races` |
| Silver | `stg_drivers` | one row per driver | dbt: refs `f1/raw_drivers` |
| Silver | `stg_results` | one row per (race, driver) | dbt: refs `f1/raw_results` |
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
│   ├── raw/                               # Bronze (Delta — *.delta/ directories), per dataset
│   │   └── f1/
│   │       ├── raw_races.delta/
│   │       ├── raw_drivers.delta/
│   │       └── raw_results.delta/
│   ├── staging/                           # Silver (single Parquet files), per dataset
│   │   └── f1/
│   │       ├── stg_races.parquet
│   │       ├── stg_drivers.parquet
│   │       └── stg_results.parquet
│   ├── marts/                             # Gold (single Parquet files), per dataset
│   │   └── f1/
│   │       ├── mart_country_race_summary.parquet
│   │       └── mart_driver_standings.parquet
│   └── lakehouse.duckdb                   # dbt's persistent view catalog (Implementation Note #2)
├── pipelines/                             # Dagster project
│   ├── stack/                             # THE ENGINE — domain-agnostic, reusable
│   │   ├── extractors.py                  # Extractor protocol + RestApiExtractor
│   │   ├── specs.py                       # SourceSpec (the bronze contract)
│   │   ├── raw_assets.py                  # build_raw_assets(dataset, specs) → bronze assets
│   │   └── dbt.py                         # LakehouseDbtTranslator (source → bronze asset key)
│   ├── datasets/                          # THE PAYLOADS — one self-contained bundle per dataset
│   │   └── f1/
│   │       └── sources.py                 # DATASET, SOURCES (+ shape_results escape hatch)
│   └── definitions.py                     # Thin assembler: stack + datasets → `defs`
├── dbt_project/                           # dbt project
│   ├── models/
│   │   └── f1/                            # models namespaced by dataset
│   │       ├── staging/
│   │       │   ├── sources.yml            # Delta source group named "f1" (plugin: delta + delta_table_path)
│   │       │   ├── stg_races.sql
│   │       │   ├── stg_drivers.sql
│   │       │   └── stg_results.sql
│   │       └── marts/
│   │           ├── schema.yml             # Docs + tests for marts
│   │           ├── mart_country_race_summary.sql
│   │           └── mart_driver_standings.sql
│   ├── macros/
│   │   └── dataset_location.sql           # location macro: data/<layer>/<dataset>/<model>.parquet (Note #12)
│   ├── profiles.yml                       # dbt-duckdb config (persistent file, threads=1, delta plugin)
│   └── dbt_project.yml
├── notebooks/
│   └── 01_explore.ipynb                   # VS Code / JupyterLab scratchpad — bronze, silver, gold tours
├── evidence/                              # Code-first BI dashboards (Markdown + SQL)
│   ├── sources/lakehouse/connection.yaml  # DuckDB connection → ../data/lakehouse.duckdb
│   ├── pages/
│   │   ├── index.md                       # Hub: links to each dataset
│   │   └── f1/                            # F1 dashboards, namespaced by dataset
│   │       ├── index.md                   # F1 overview dashboard
│   │       ├── drivers.md                 # Championship standings
│   │       └── countries.md               # Geographic summary
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
