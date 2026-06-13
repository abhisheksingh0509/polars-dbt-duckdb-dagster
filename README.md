# Local Data Lakehouse: F1 Edition

A single-node, vectorized data pipeline that ingests Formula 1 racing data and transforms it through a bronze → silver → gold lakehouse — **without Spark or any cluster**.

This is a **learning project** for the modern open-source data stack. If you have a Spark background, this README maps every new tool back to a Spark concept you already know.

> **New to this codebase?** Read **[LEARN.md](LEARN.md)** first. It walks through what we built, explains each tool in plain English with doc links, and ends with a homework assignment to cement what you've learned. This README focuses on *how to run it*; LEARN.md focuses on *what's going on*.

---

## TL;DR — what's happening here?

```
                    ┌──────────────────────────────────────────────────┐
                    │  Jolpica API (Formula 1 racing data, public REST)│
                    │  drop-in mirror of the deprecated Ergast API     │
                    └────────────────────┬─────────────────────────────┘
                                         │  HTTP GET
                                         ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │                         Dagster Orchestrator                       │
   │  (declarative DAG of "assets" — physical data products, not tasks) │
   │                                                                    │
   │   ┌──────────────┐         ┌─────────────┐        ┌─────────────┐ │
   │   │   Polars     │  ───▶   │     dbt     │  ───▶  │  Marts      │ │
   │   │ (extract +   │         │  + DuckDB   │        │ (questions  │ │
   │   │  shape)      │         │ (transform) │        │  answered)  │ │
   │   └──────┬───────┘         └──────┬──────┘        └──────┬──────┘ │
   └──────────┼─────────────────────── ┼────────────────────── ┼───────┘
              ▼                        ▼                       ▼
         data/raw/                data/staging/            data/marts/
       (Delta Lake)              (Parquet files)         (Parquet files)
        — bronze —                 — silver —              — gold —
```

Everything runs **in one Docker container on your laptop**. No JVM, no cluster, no cloud. Data lives in plain files on disk that you can inspect with any tool.

---

## Why this stack? (For folks coming from Spark)

| Tool | Role in this project | If you know Spark, think of it as... |
|---|---|---|
| **Polars** | Extract data, shape DataFrames | Spark DataFrames — but single-node, in Rust, and ~10× lower overhead per row. Same lazy/eager mental model. |
| **DuckDB** | SQL query engine for transforms | Spark SQL **without the cluster**. Runs in-process (like SQLite for analytics). Vectorized, columnar, zero JVM. |
| **dbt** | SQL transformation framework | What you'd build *on top of* Spark SQL if you wanted versioned models, dependency graphs, tests, and lineage. Tool-agnostic — here it drives DuckDB. |
| **Dagster** | Orchestration | Airflow's modern replacement. Key shift: orchestrates **assets** (the data products themselves) rather than just tasks. The DAG is *of data*, not *of jobs*. |
| **Delta Lake** | Open table format for raw layer | Same Delta you know from Databricks — but here written purely from Python via `deltalake-rs`. No Spark needed to produce it. |
| **Parquet** | Storage format for staging + marts | The same columnar file format Spark uses. Universally readable. (We use single files per model; partitioning is a future enhancement once we have multiple seasons.) |
| **uv** | Python package manager | A faster, deterministic `pip` + `poetry` replacement. Written in Rust by Astral. Resolves dependencies in seconds instead of minutes. |
| **Docker Compose** | Local container orchestration | Containerizes the whole pipeline so it runs identically on Mac, Windows (WSL), or Linux. |

---

## The medallion architecture (bronze → silver → gold)

This is a standard lakehouse pattern. Each layer has a clear purpose:

### Bronze: `data/raw/` (Delta Lake)
- **What:** Raw data, freshly extracted, almost no transformation. Source-of-truth.
- **Format:** Delta Lake — transactional, supports schema evolution, supports time travel.
- **Writer:** Polars, via Dagster's `PolarsDeltaIOManager`.
- **Why Delta here:** You want ACID guarantees on ingest. If a Polars asset fails halfway, you don't end up with a partially-written table. Delta also lets you query *as of a previous version*, which is gold for debugging.

### Silver: `data/staging/` (single Parquet files)
- **What:** Cleaned, conformed, typed. Joinable. One file per source entity: `stg_races.parquet`, `stg_drivers.parquet`, `stg_results.parquet`.
- **Format:** Parquet, one file per model.
- **Writer:** dbt models using dbt-duckdb's `external` materialization.
- **Why Parquet here:** This is the canonical dbt-duckdb path. Writing Delta from dbt requires custom Python plumbing for limited gain. Plain Parquet is universally readable and works perfectly at this volume.
- **Why no partitioning yet:** We only have one season — Hive partitioning would be premature. When we add multiple seasons, we'd switch to `data/staging/stg_races/season=YYYY/` layout.

### Gold: `data/marts/` (single Parquet files)
- **What:** Business-facing answers. Wide tables ready for BI / analysis.
- **Files:** `mart_country_race_summary.parquet` (one row per country), `mart_driver_standings.parquet` (one row per driver — points, wins, podiums, DNFs).
- **Format:** Same as silver — single Parquet files.
- **Writer:** dbt mart models built on top of staging via `{{ ref('stg_xxx') }}`.

> **Why mix formats?** No single open table format has clean *write* support in both Polars **and** dbt-duckdb today. Delta is excellent from Polars, weak from dbt-duckdb. Iceberg is the reverse. This hybrid plays to each tool's strengths and mirrors how most production lakehouses at small/medium scale are actually built.

---

## Data flow, step by step

1. **Dagster materializes a Polars asset** (e.g., `raw_races`).
   - The asset calls the **Jolpica API**, builds a Polars DataFrame, returns it.
   - The `PolarsDeltaIOManager` automatically writes it to `data/raw/raw_races.delta/` as a Delta table. No file-handling boilerplate in your code. (The `.delta` suffix is added by the IO manager as a format marker.)

2. **dbt sources read those Delta tables.**
   - In `dbt_project/models/staging/sources.yml`, each Delta directory is declared as a source with `plugin: delta` and an explicit `delta_table_path`.
   - The `dbt-duckdb` `delta` plugin makes DuckDB read these natively (via the deltalake-rs Python library).

3. **dbt staging models clean and conform.**
   - Standard `SELECT ... FROM {{ source('raw', 'raw_races') }}` — pure SQL.
   - Materialized as `external` Parquet at `data/staging/stg_races.parquet` (single file per model).

4. **dbt mart models answer business questions.**
   - Join staging tables, aggregate, use window functions.
   - Materialized as `external` Parquet at `data/marts/<mart>.parquet`.

5. **Dagster's `dagster-dbt` integration imports the dbt DAG.**
   - Every dbt model becomes a Dagster asset.
   - You see the *entire* pipeline (Polars + dbt) as one unified asset graph in the Dagster UI.

---

## Project structure

```text
project_root/
├── data/                        # The lakehouse — bind-mounted from container, inspectable from your IDE
│   ├── raw/                     # Bronze: Delta tables (Polars writes — *.delta/ directories)
│   ├── staging/                 # Silver: single Parquet files per model (dbt writes)
│   ├── marts/                   # Gold: single Parquet files per model (dbt writes)
│   └── lakehouse.duckdb         # dbt's persistent view catalog (engine state — see Troubleshooting)
│
├── pipelines/                   # Dagster project — Python-side orchestration
│   ├── assets/raw.py            # Polars extract assets + paginated HTTP helper
│   └── definitions.py           # Wires assets + IO managers + dbt project into Dagster's Definitions
│
├── dbt_project/                 # dbt project — SQL transforms
│   ├── models/
│   │   ├── staging/             # sources.yml + stg_*.sql (clean & conform raw Delta tables)
│   │   └── marts/               # schema.yml + mart_*.sql (business questions)
│   ├── profiles.yml             # dbt-duckdb config (persistent file, threads=1, delta plugin)
│   └── dbt_project.yml
│
├── notebooks/01_explore.ipynb   # VS Code / JupyterLab scratchpad — bronze, silver, gold tours
│
├── .dagster_home/               # Dagster's instance storage — bind-mounted, gitignored
│
├── Dockerfile                   # Builds the single image (Python 3.12, uv, all deps)
├── docker-compose.yml           # Wires up volumes + ports for local dev
├── pyproject.toml               # Python project metadata + dependencies (managed by uv)
├── uv.lock                      # Locked dependency versions (commit this!)
│
├── README.md                    # This file — how to run it
├── LEARN.md                     # Beginner walkthrough + tool explainers + homework
└── CLAUDE.md                    # Architecture + decisions + gotchas — for Claude Code sessions
```

---

## Getting started

### Prerequisites

You need exactly two things on your laptop:

1. **`uv`** — `curl -LsSf https://astral.sh/uv/install.sh | sh` (or `brew install uv` on macOS)
2. (Optional) **Docker Desktop** — only needed for the containerized run. Everything works locally via uv too.

`uv` manages Python itself, so no separate Python install needed.

### First-time setup (local — fastest feedback loop)

```bash
# 1. Install Python 3.12 + all project deps into a local .venv (~10 seconds)
uv sync

# 2. Start Dagster's dev server
uv run dagster dev -m pipelines.definitions

# 3. Open Dagster's web UI
open http://localhost:3000
```

### Running the pipeline

In the Dagster UI:
1. **First time only — click "Materialize all"** at the top. This is mandatory the first time so dbt creates all of its view definitions in `data/lakehouse.duckdb` (see Troubleshooting #2).
2. After that, materialize any subset you like — Dagster figures out the dbt selector.

From the command line:

```bash
# Run one Polars asset
uv run dagster asset materialize -m pipelines.definitions --select raw_races

# Run a full dbt build (uses the persistent DuckDB file)
cd dbt_project && uv run dbt build --profiles-dir .
```

### Running via Docker (optional)

```bash
docker compose up --build -d
open http://localhost:3000
```

The Docker setup uses bind-mounts for `./data`, `./pipelines`, and `./dbt_project` — so code edits hot-reload without rebuilds.

---

## Exploring your data

The whole point of bind-mounting `./data` is that you can poke at the lakehouse from outside the container. A few ways:

### Query a Delta table from Python

```python
import polars as pl
# Note the .delta suffix — PolarsDeltaIOManager adds it as a format marker.
df = pl.read_delta("data/raw/raw_races.delta")
print(df.head())
```

### Query a Delta table from DuckDB

```sql
-- DuckDB CLI: `duckdb`
INSTALL delta;
LOAD delta;
SELECT * FROM delta_scan('data/raw/raw_races.delta') LIMIT 10;
```

### Query a staging or mart Parquet file

```sql
-- Single Parquet file per model — no glob needed.
SELECT season, COUNT(*) AS race_count
FROM 'data/staging/stg_races.parquet'
GROUP BY season;

-- Or read a mart directly:
SELECT * FROM 'data/marts/mart_driver_standings.parquet'
ORDER BY total_points DESC
LIMIT 5;
```

### Explore via the notebook

Open `notebooks/01_explore.ipynb` in VS Code or JupyterLab — it has pre-built cells touring the bronze (Delta), silver (Parquet), and gold (mart) layers plus a few ad-hoc Polars/DuckDB analyses.

### See dbt's lineage graph

```bash
docker compose exec pipeline dbt docs generate --project-dir dbt_project
docker compose exec pipeline dbt docs serve --project-dir dbt_project
# Open http://localhost:8080
```

### See Dagster's asset graph

Open `http://localhost:3000` while Dagster is running. The "Assets" tab is the entry point — every box is a piece of data, and arrows show what depends on what.

### See the dashboards (Evidence.dev)

Code-first BI — Markdown + SQL files in `evidence/pages/` become a website with charts.

```bash
cd evidence
npm install --force      # first time only — see Troubleshooting #6
npm run dev              # opens http://localhost:3001
```

Three starter dashboards are pre-wired against the marts:
- `/` — Overview (race count, driver count, top 5 standings)
- `/drivers` — Full championship standings + charts
- `/countries` — Geography + map of host countries

Edit any `.md` file in `evidence/pages/` and the dev server hot-reloads. Add a new file → it auto-appears as a new route.

---

## Troubleshooting (gotchas we hit)

These cost real debugging time during the build-out. If you see one of these errors, the fix is already in the config — don't re-debug from scratch.

### 1. `TransactionContext Error: write-write conflict on create with "raw"`

**Cause:** dbt-duckdb's delta plugin can't safely register multiple Delta sources concurrently — each worker thread tries to `CREATE SCHEMA "raw"` and they collide in DuckDB's catalog.

**Fix:** `threads: 1` in `dbt_project/profiles.yml` (already set). Don't increase it.

### 2. `Catalog Error: Table with name stg_xxx does not exist`

**Cause:** dbt's `external` materialization writes Parquet AND creates a DuckDB VIEW over it. With `:memory:` DuckDB, that view doesn't survive across dbt invocations — so a selective build referencing `{{ ref('stg_xxx') }}` fails because the view was never recreated.

**Fix:** Use a persistent DuckDB file (`path: data/lakehouse.duckdb` — already set). AND run **"Materialize all"** at least once before doing selective runs, so every view gets created in the persistent catalog.

If the persistent file ever gets out of sync: `rm data/lakehouse.duckdb` and re-materialize all. The Parquet/Delta data files on disk are the durable lakehouse state — the `.duckdb` file is just dbt's engine state.

### 3. `Invalid table location: ../data/raw/raw_races`

**Cause:** `PolarsDeltaIOManager` appends `.delta` to the asset name when writing. Actual path is `data/raw/raw_races.delta/`, not `data/raw/raw_races/`. Any dbt source's `delta_table_path` config must include the suffix.

**Fix:** Check `dbt_project/models/staging/sources.yml` — all `delta_table_path` entries should end in `.delta`.

### 4. `httpx.ReadTimeout` from Jolpica

**Cause:** Jolpica's free tier occasionally stalls on heavy endpoints (especially `/results.json` with nested data).

**Fix:** `pipelines/assets/raw.py` has retry logic with exponential backoff. If you see this and the retries don't help, just try again later — usually a transient blip.

### 5. dbt run fails with "No dbt_project.yml found"

**Cause:** You ran `dbt` from the wrong directory. dbt looks for `dbt_project.yml` in the current dir.

**Fix:** `cd dbt_project && uv run dbt build --profiles-dir .` — or use the `--project-dir dbt_project` flag.

### 6. Evidence `npm install` fails with peer dep errors

**Cause:** Evidence's transitive deps have version conflicts under npm 7+'s strict peer dep resolution. `--legacy-peer-deps` skips peer deps entirely, which breaks the dev server because `@sveltejs/vite-plugin-svelte` gets dropped.

**Fix:** `npm install --force` instead. Accepts conflicts but still installs peer deps. (After the lock file is generated, plain `npm install` works for subsequent runs.)

### 7. Evidence dashboards aren't loading data

**Cause:** Evidence connects to `data/lakehouse.duckdb`. If that file doesn't exist or has no views, every query errors. The `.duckdb` file is created by dbt — and only contains views after at least one `dbt build` (or "Materialize all" in Dagster).

**Fix:** Run a full materialization first. Then `cd evidence && npm run sources && npm run dev`.

### 8. `SIGBUS` (signal 7) during Dagster materialization in Docker

**Cause:** Two possible causes (often together):
- Docker's default `/dev/shm` is 64 MB — too small for Polars/Delta mmap operations.
- Concurrent access to `lakehouse.duckdb` between Evidence (host) and dbt (container). Without read-only mode on the Evidence side, the writer-vs-writer conflict can manifest as SIGBUS in macOS Docker rather than a clean lock error.

**Fix:** Already applied in `docker-compose.yml` (`shm_size: 2gb`) and `evidence/sources/lakehouse/connection.yaml` (`access_mode: READ_ONLY`). If you ever change these and hit SIGBUS again, restore them.

### 9. Evidence: `Catalog Error: Table with name <foo> does not exist`

**Cause:** Evidence queries reference data via *source query files*, not directly. `FROM lakehouse.mart_x` requires `evidence/sources/lakehouse/mart_x.sql` to exist (Evidence runs that file during `npm run sources` and caches the result).

**Fix:** Create the source `.sql` file (typically `SELECT * FROM <table_name>`), then `npm run sources` to populate the cache. Restart `npm run dev`.

---

## Glossary

**Asset (Dagster)** — A piece of persistent data your pipeline produces. Defined by a Python function decorated with `@asset`. Dagster knows when an asset is stale and re-runs only what's needed. *(Spark analogue: closest thing is a registered DataFrame you'd recompute on demand — but Dagster makes the staleness logic explicit.)*

**Bronze / Silver / Gold** — The three layers of a "medallion" lakehouse architecture. Bronze = raw, Silver = cleaned/conformed, Gold = business-ready. Same idea as "raw / curated / serving" zones.

**Delta Lake** — An open table format. Adds ACID transactions, schema evolution, and time travel to plain Parquet files. The magic lives in a `_delta_log/` JSON directory that records every transaction.

**External materialization (dbt)** — A dbt model that writes its output to a file path (Parquet, CSV, JSON) rather than to a database table. dbt-duckdb supports this natively.

**Hive partitioning** — A convention where partition values are encoded in the directory path: `season=2024/round=1/data.parquet`. Query engines (DuckDB, Polars, Spark) automatically use these as filter predicates.

**IO Manager (Dagster)** — A piece of code that handles "where and how do I save the output of this asset, and load it for the next one." `PolarsDeltaIOManager` is one specifically for Polars DataFrames → Delta tables.

**Lakehouse** — A data architecture that combines the cheap, open-format storage of a *data lake* with the transactional guarantees and schema management of a *data warehouse*. Delta Lake + Parquet on object storage = lakehouse.

**Materialization (dbt)** — How dbt physicalizes a model: as a `view`, `table`, `incremental` model, or here, `external` (file on disk).

**Source (dbt)** — A reference to data that exists *outside* dbt (here: the Delta tables Polars wrote). Declared in YAML so dbt's lineage graph knows where data starts.

---

## Where to learn more

| Tool | Best starting point |
|---|---|
| Polars | [User Guide](https://docs.pola.rs/) — the "Concepts" section in particular |
| DuckDB | [DuckDB SQL tutorial](https://duckdb.org/docs/sql/introduction) |
| Delta Lake (Python) | [delta-rs docs](https://delta-io.github.io/delta-rs/) |
| dbt-duckdb | [dbt-duckdb GitHub README](https://github.com/duckdb/dbt-duckdb) |
| Dagster | [Dagster Essentials tutorial](https://docs.dagster.io/getting-started) |
| uv | [`uv` docs](https://docs.astral.sh/uv/) |

---

## What this project is **not**

- **Not a production template.** Single-node, no auth, no cloud storage, no scheduling beyond what you trigger manually.
- **Not opinionated about your future stack.** This is a tour of one specific corner of the OSS data ecosystem. The same lakehouse patterns translate cleanly to Spark/Iceberg/Snowflake when you graduate to bigger problems.
- **Not Spark-replacement advice.** Spark still wins for jobs that don't fit on one machine. This stack wins when they do — which, with modern hardware, is most of them.
