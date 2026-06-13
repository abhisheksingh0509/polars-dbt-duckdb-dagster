# Learn This Project (Without Hand-Holding)

A standalone walkthrough of what we built and why, written for someone who just inherited this codebase and wants to understand it deeply enough to extend it. By the end you should be able to do the **homework** at the bottom on your own.

The README is the "how do I run it" doc. This is the "what is going on and how do I make it mine" doc.

---

## Part 1 — The 60-second elevator pitch

You built a **single-node Data Lakehouse** on your laptop. It pulls F1 racing data from a public API, lands it as transactional **Delta** files (bronze), cleans and transforms it through **dbt** into **Parquet** files (silver + gold), runs the whole thing as a unified DAG in **Dagster**, and visualizes the gold layer as **Evidence** dashboards.

No Spark. No cluster. No cloud. Everything runs on your Mac. Data lives as plain files you can open with any tool.

This is the "modern data stack, single-node edition" — the same architecture patterns that run trillion-row warehouses, scaled down so you can hold the whole thing in your head.

---

## Part 2 — The journey, in narrative form

The order we built things wasn't accidental. Each step proved one architectural assumption before we layered on the next.

### Step 1: Foundations (uv, pyproject, Docker)
We set up `uv` (Astral's Rust-based Python package manager) as the project's dependency tool, locked Python to **3.12**, and scaffolded a `Dockerfile` + `docker-compose.yml` for containerized runs. **Why first:** every step that follows depends on reproducible installs. With `uv.lock` committed, anyone on any platform gets the exact same dependency tree.

### Step 2: Extract layer (Polars + Dagster + Delta)
We wrote Dagster `@asset` functions in `pipelines/assets/raw.py` that hit the **Jolpica API** (a free, community-maintained mirror of Ergast's defunct F1 API) and return **Polars DataFrames**. Dagster's `PolarsDeltaIOManager` writes those DataFrames as **Delta tables** in `data/raw/<asset>.delta/`. **Why this combo:** Delta gives us ACID transactional writes (no partial files on crash) and time travel. Polars + delta-rs is the only mainstream Python stack that writes Delta without needing Spark.

### Step 3: Transform layer (dbt + DuckDB + Parquet)
We pointed `dbt-duckdb`'s **delta plugin** at the bronze tables (`models/staging/sources.yml`), then wrote staging models in SQL to clean them (`models/staging/stg_*.sql`). Staging output writes back as **Parquet** files via dbt's `external` materialization. Marts (`models/marts/mart_*.sql`) aggregate the staging files into business-facing answers. **Why Parquet here, not Delta:** the dbt-duckdb `delta` plugin only *reads* Delta well. Writing Delta from dbt requires custom Python plumbing. Parquet is the canonical dbt-duckdb output and is universally readable — small/medium production lakehouses mirror this hybrid pattern.

### Step 4: Orchestration glue (dagster-dbt)
We used `dagster-dbt`'s `@dbt_assets` decorator to make every dbt model show up as a Dagster asset. A custom `DagsterDbtTranslator` strips dbt's source-group prefix so dbt's `source('raw', 'raw_races')` resolves to the same Dagster `AssetKey` as the Polars-written asset — so the UI shows one connected graph, not two disconnected ones. **Why this matters:** without it you'd run Polars and dbt as separate processes and lose lineage. With it, Dagster's UI is the single pane of glass.

### Step 5: Containerization (Docker Compose)
We built a single Docker image (using Astral's official `uv` base image) that contains Python 3.12, Polars, DuckDB, dbt, Dagster, and Evidence. Bind-mounts on `./data`, `./.dagster_home`, `./pipelines`, and `./dbt_project` keep state inspectable from the host and make code changes hot-reload without rebuilds. **Why:** packages the entire pipeline into one reproducible artifact. `docker compose up` and someone on a different OS gets the same thing you have.

### Step 6: Visualization layer (Evidence.dev)
We added a code-first BI tool — Evidence — that reads from `data/lakehouse.duckdb` (dbt's persistent view catalog) and renders Markdown + SQL files in `evidence/pages/` as a real interactive website. **Why Evidence specifically:** alternatives like Superset are heavyweight (~2 GB) BI servers with their own metadata databases. Evidence is just Node — the output is static HTML. It matches the "everything is files" philosophy of the rest of the stack: dashboards are version-controlled SQL+Markdown.

### Step 7: Documentation + gotchas
We documented every non-obvious config decision in CLAUDE.md's **Implementation Notes** section. These are things that cost real debugging time — `threads: 1` for the dbt-duckdb delta plugin, persistent DuckDB file, `shm_size: 2gb` for Docker, `access_mode: READ_ONLY` for Evidence's connection. The README's **Troubleshooting** section maps every error message you might see to its fix.

---

## Part 3 — Each tool, in plain English

For every tool, I'll tell you: what it actually does, why we chose it over alternatives, and exactly where to read more.

### [uv](https://docs.astral.sh/uv/) — package manager

**What it is:** A Rust-based replacement for `pip` + `poetry` + `virtualenv`. Manages Python versions, dependencies, and lockfiles in one tool.

**Why it's here:** Dependencies resolve in *seconds*, not minutes. Cross-platform wheel resolution is bulletproof — no more "works on my Mac, breaks on Linux CI." Lockfiles are deterministic.

**Read first:** [Getting started](https://docs.astral.sh/uv/getting-started/) → [Managing projects](https://docs.astral.sh/uv/guides/projects/)

**Mental shortcut for Spark folks:** `uv` ↔ `sbt`/`maven` for the Python world — but actually fast.

---

### [Polars](https://docs.pola.rs/) — DataFrame library

**What it is:** A Rust-based DataFrame engine. Pandas-like API, vectorized columnar execution, lazy + eager modes.

**Why it's here:** We need a DataFrame layer for the extract step (HTTP → JSON → typed columns). Polars is roughly 10× faster than Pandas per row and handles nested JSON structs cleanly via struct columns. Critically, Polars has first-class Delta read/write support via `pl.write_delta()`.

**Read first:** [User Guide → Concepts](https://docs.pola.rs/user-guide/concepts/) → [Expressions](https://docs.pola.rs/user-guide/expressions/)

**Spark shortcut:** Polars DataFrame ↔ Spark DataFrame. Same lazy/eager mental model. Polars's `.collect()` ↔ Spark's `.collect()`/`.toPandas()`. Major difference: Polars uses **expressions** (`pl.col('x').sum()`) rather than column references — slightly different ergonomics, but the same power.

---

### [Delta Lake](https://delta-io.github.io/delta-rs/) (via `deltalake-rs`)

**What it is:** An open *table format* (not a file format). Adds ACID transactions, schema evolution, and time travel to plain Parquet files. The magic lives in a `_delta_log/` JSON directory that records every transaction as an immutable log entry.

**Why it's here:** The bronze layer needs *transactional* ingest — half-written tables on a crash would be brutal to recover from. Delta also gives you "as-of-version" queries which are gold for debugging "why was this row different yesterday?"

**Read first:** [delta-rs Python docs](https://delta-io.github.io/delta-rs/usage/) → especially the [time-travel](https://delta-io.github.io/delta-rs/usage/loading-table/) section.

**Spark shortcut:** It's literally the same Delta you know from Databricks. The difference is **how** it's written. Spark writes Delta via the JVM. `delta-rs` writes it via Rust, callable from Python without a JVM. No Spark needed.

---

### [DuckDB](https://duckdb.org/) — query engine

**What it is:** An embedded analytical OLAP database. SQLite for analytics: in-process, single-file, vectorized, columnar.

**Why it's here:** dbt needs *some* SQL engine. DuckDB runs in-process (no separate server), reads Parquet/CSV/Delta natively, and is roughly competitive with Spark SQL for single-node workloads up to ~100 GB.

**Read first:** [DuckDB tutorial](https://duckdb.org/docs/sql/introduction) → [extensions overview](https://duckdb.org/docs/extensions/overview)

**Spark shortcut:** DuckDB ↔ Spark SQL minus the cluster. SQL dialect is similar to PostgreSQL with a few extensions (`QUALIFY`, `LIST`, struct access via `[]`, etc.).

---

### [dbt](https://docs.getdbt.com/) + [`dbt-duckdb` adapter](https://github.com/duckdb/dbt-duckdb)

**What it is:** A SQL transformation framework. You write `SELECT` statements as `.sql` files; dbt manages the dependency graph, runs them in order, tests assertions, and renders lineage docs. **Adapters** plug it into specific databases — `dbt-duckdb` is the DuckDB adapter.

**Why it's here:** Without dbt, every transformation would be a Python file calling a SQL string. With dbt, transformations are versioned SQL + YAML, get free lineage tracking and free tests, and integrate cleanly with Dagster.

**Read first:** [dbt fundamentals course (free)](https://courses.getdbt.com/courses/fundamentals) → [dbt-duckdb plugins](https://github.com/duckdb/dbt-duckdb#plugins) for the delta integration.

**Spark shortcut:** dbt is what you'd build *on top of* Spark SQL if you wanted versioned models with tests. Tool-agnostic — same dbt project can drive Snowflake, BigQuery, DuckDB, or Spark.

---

### [Dagster](https://docs.dagster.io/) + `dagster-dbt` + `dagster-polars`

**What it is:** A modern orchestrator. Key conceptual shift from Airflow: it orchestrates **assets** (the data products themselves) rather than **tasks** (the jobs that produce them).

**Why it's here:** Single pane of glass for the entire pipeline. Polars assets + dbt models are all just Dagster assets; the UI shows the unified DAG with materialization state per node.

**Read first:** [Dagster Essentials tutorial](https://docs.dagster.io/getting-started) → [Software-Defined Assets concept](https://docs.dagster.io/concepts/assets/software-defined-assets) → [dagster-dbt integration](https://docs.dagster.io/integrations/dbt)

**Spark shortcut:** Dagster ↔ Airflow's modern successor. Same DAG idea, but inverted: the DAG is *of data*, not *of jobs*. A "stale" raw asset propagates staleness downstream automatically.

---

### Docker + docker-compose

**What it is:** Container runtime + multi-container orchestration. Packages your code + its OS + its dependencies into a portable image.

**Why it's here:** Anyone can `docker compose up` and get an identical pipeline running. No "works on my machine."

**Read first:** [Compose file reference](https://docs.docker.com/compose/compose-file/) → [bind mounts](https://docs.docker.com/storage/bind-mounts/) explains why our source code hot-reloads without rebuilds.

---

### [Evidence.dev](https://docs.evidence.dev/)

**What it is:** Code-first BI. Markdown + SQL files become a static HTML site with interactive charts.

**Why it's here:** Dashboards as code (version-controlled) instead of click-built (database-stored). Tiny footprint (no metadata DB needed). Native DuckDB support so it reads our marts directly.

**Read first:** [Quickstart](https://docs.evidence.dev/getting-started/) → [Charts reference](https://docs.evidence.dev/components/charts/) (you'll come back to this constantly when adding new charts).

**Key concept that bit us:** Evidence's "sources" need explicit `.sql` files in `sources/<source>/*.sql`. You don't query the underlying DB directly from a page — you query named source queries that Evidence has cached.

---

### Jupyter (via VS Code's notebook UI)

**What it is:** Interactive Python notebooks. We use them as a scratchpad.

**Why it's here:** When something looks weird in a dashboard or a dbt model, the fastest way to inspect raw data is `pl.read_delta(...)` in a notebook.

**Open it:** Right-click `notebooks/01_explore.ipynb` in VS Code → "Open With..." → Jupyter Notebook (requires the Jupyter extension).

---

## Part 4 — How the pieces actually fit together

```
                               ┌─────────────────────────┐
                               │   Jolpica F1 API        │
                               │   (HTTP / JSON)         │
                               └────────────┬────────────┘
                                            │ httpx.get
                                            ▼
   ┌────────────────────────────────────────────────────────────────────┐
   │                          DAGSTER (orchestrator)                     │
   │                                                                     │
   │  ┌────────────────┐    ┌────────────────┐    ┌─────────────────┐  │
   │  │ Polars @asset  │ ─▶ │ dbt staging    │ ─▶ │ dbt marts       │  │
   │  │ pl.DataFrame   │    │ stg_*.sql      │    │ mart_*.sql      │  │
   │  └────────┬───────┘    └────────┬───────┘    └────────┬────────┘  │
   └───────────┼─────────────────────┼─────────────────────┼───────────┘
               ▼                     ▼                     ▼
       data/raw/*.delta/      data/staging/*.parquet  data/marts/*.parquet
        ┌─────────┐           ┌─────────┐             ┌─────────┐
        │  Delta  │           │ Parquet │             │ Parquet │
        └─────────┘           └─────────┘             └────┬────┘
        BRONZE                SILVER                  GOLD │
                                                           │
                              ┌────────────────────────────┼──┐
                              │      data/lakehouse.duckdb │  │
                              │   (dbt's view catalog)     │  │
                              └────────────────┬───────────┘  │
                                               │              │
                                               ▼              ▼
                                       ┌──────────────────────────┐
                                       │    EVIDENCE.DEV          │
                                       │  Markdown + SQL → HTML   │
                                       └──────────────────────────┘
```

The two things that aren't shown but matter:
- **`lakehouse.duckdb`** isn't a copy of the data — it's just a catalog of *views* pointing at the Parquet files. Tiny file.
- **Evidence's source cache** (`evidence/.evidence/`) is a Parquet copy of the source query results, regenerated by `npm run sources`.

---

## Part 5 — Common operations cheatsheet

These are the day-to-day moves once the pipeline is set up.

### Add a new dbt model
1. Create `dbt_project/models/<staging|marts>/<name>.sql`.
2. Top of file: `{{ config(location = env_var('LAKEHOUSE_DATA_ROOT', '../data') ~ '/<layer>/' ~ this.name ~ '.parquet') }}`
3. Write your `SELECT`. Reference upstream models with `{{ ref('stg_xxx') }}` and sources with `{{ source('raw', 'raw_xxx') }}`.
4. Add an entry to `models/<layer>/schema.yml` for docs + tests (optional but recommended).
5. Reload Dagster's workspace (UI → top-right → Reload), then materialize.

### Add a new raw asset (new data source)
1. Add a new `@asset` function in `pipelines/assets/raw.py`. Use `_fetch_paginated` for HTTP work.
2. Register it in `pipelines/definitions.py`'s `assets=[...]` list.
3. Declare it as a dbt source in `dbt_project/models/staging/sources.yml` (with `plugin: delta` + `delta_table_path`).
4. Materialize it once in Dagster.

### Add an Evidence dashboard
1. Create `evidence/sources/lakehouse/<query_name>.sql` — typically `SELECT * FROM <dbt_model_name>`.
2. `cd evidence && npm run sources` to refresh the cache.
3. Create `evidence/pages/<page>.md`. Use `\`\`\`sql query_name` blocks to query.
4. `npm run dev` — the page appears at `localhost:3001/<page>`.

### When something breaks
First stop: `README.md` → **Troubleshooting** section. We've documented 9 distinct failure modes you might hit, with the exact error messages and fixes.

---

## Part 6 — Your homework

**Build constructor (team) standings end-to-end, then visualize it.**

In F1, constructors (teams like Red Bull, Ferrari, Mercedes) score points based on their drivers' finishes. A "constructor standings" mart aggregates points by team rather than by driver. The constructor data is already in your `stg_results` table — every result row has a `constructor_id` and `constructor_name`. You don't need a new raw asset.

### What you'll build

A new mart `mart_constructor_standings` that has one row per constructor with: total team points, wins (best-finishing driver placed 1st in that race), 1-2 finishes (both drivers podiumed in that race), driver count, etc. Plus a new Evidence page showing it.

### The checklist (file by file)

| File to create or edit | What goes in it | Pattern to copy from |
|---|---|---|
| `dbt_project/models/marts/mart_constructor_standings.sql` | The mart SQL — GROUP BY `constructor_id` and aggregate points/wins/DNFs across the season | `mart_driver_standings.sql` |
| `dbt_project/models/marts/schema.yml` | Add a `mart_constructor_standings` entry with column docs + a `unique` test on `constructor_id` | Existing entries in this file |
| `evidence/sources/lakehouse/mart_constructor_standings.sql` | `SELECT * FROM mart_constructor_standings` | `mart_driver_standings.sql` in the same folder |
| `evidence/pages/constructors.md` | A new dashboard page — DataTable + BarChart of points by team | `drivers.md` |
| `evidence/pages/index.md` | Add a link to `/constructors` in the "Navigate" section | Existing nav links |

### Steps in order

1. Write `mart_constructor_standings.sql`. The trick: a "constructor win" isn't trivially the sum of driver wins — multiple drivers on a team can win different races, and only one driver from a team finishes any given race. The simplest definition is: rows where `finishing_position = 1` grouped by constructor. Use a similar pattern to driver standings.
2. Run `dbt build --select mart_constructor_standings --profiles-dir .` from `dbt_project/` to verify the SQL works.
3. Open the resulting Parquet in your notebook or with DuckDB to sanity-check the numbers (e.g., Red Bull should be near the top in 2024).
4. Add the schema.yml entry.
5. Create the Evidence source `.sql` file.
6. Run `npm run sources` to cache it.
7. Write the `constructors.md` page. Mimic `drivers.md` — DataTable + BarChart should be enough.
8. Link it from the homepage.
9. `npm run dev`, open `localhost:3001/constructors`, confirm it renders.
10. Materialize the full pipeline in Dagster to confirm everything's still green.

### How to know you succeeded

- `dbt build` runs without errors and writes `data/marts/mart_constructor_standings.parquet`
- The new asset shows up in the Dagster UI (after a workspace reload)
- The new dashboard page renders with real data (not "no data" or an error)
- The mart's row count equals the unique constructor count for the season (should be 10 in 2024)

### Stretch goal (if you want more)

Add a new **raw asset** end-to-end. Suggested: `raw_constructors` from `https://api.jolpi.ca/ergast/f1/2024/constructors.json` — fetches constructor metadata (name, nationality, base location). Then a `stg_constructors` staging model that flattens and cleans it. Then JOIN it into your new `mart_constructor_standings` to enrich the team data (e.g., add team nationality, factory location).

That stretch exercise walks you through **every** layer once more, on your own — extract, stage, mart, visualize.

### When you get stuck

- **dbt error?** Read the message — usually clear. Check the `Troubleshooting` section in README.md.
- **Dagster doesn't see the new asset?** Workspace reload (top-right Dagster UI button). If that doesn't work, restart `dagster dev`.
- **Evidence page is blank?** Did you `npm run sources` after creating the source file? Did you spell the source query name the same in the file and the page?
- **Genuinely stuck?** Re-read Part 5 above. Then read the relevant tool's quickstart doc from Part 3. Then ask Claude — but try to figure it out first; you'll learn way more.

Good luck. This homework should take 1–2 hours and will cement the entire stack in your head far more than any reading.
