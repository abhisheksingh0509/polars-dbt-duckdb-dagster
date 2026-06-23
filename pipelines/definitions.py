"""Dagster Definitions: the entry point that wires the whole pipeline together.

Referenced by Dockerfile's CMD: `dagster dev -m pipelines.definitions`
Dagster's CLI imports this module and looks for a top-level `defs` object.

This file is a thin ASSEMBLER. It owns no dataset knowledge — it imports the reusable
engine (pipelines.stack) and each dataset's payload (pipelines.datasets.<name>), then
bolts them together:
  - bronze assets  = build_raw_assets(dataset, SOURCES) per dataset → Delta in data/raw/<ds>/
  - dbt models     = auto-discovered from dbt_project/ → Parquet in data/staging|marts/<ds>/
  - PolarsDeltaIOManager → bridges Polars DataFrames ↔ Delta tables
  - DbtCliResource → lets Dagster invoke the dbt CLI for materializations

Adding a dataset = import its SOURCES and add one build_raw_assets(...) call below.
"""

from pathlib import Path

from dagster import AssetExecutionContext, Definitions
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
from dagster_polars import PolarsDeltaIOManager

from pipelines.datasets import f1
from pipelines.stack import LakehouseDbtTranslator, build_raw_assets

# Resolve paths from this file's location so they work both:
#   - Locally (e.g. /Users/you/.../polars-dbt-duckdb-dagster/data/)
#   - Inside the Docker container (/opt/dagster/app/data/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = PROJECT_ROOT / "data"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"

# Datasets registered in this deployment. Each is a self-contained payload bundle;
# the stack engine turns its SOURCES into namespaced bronze assets.
DATASETS = [f1]

# Make sure the lakehouse zones exist before any materialization tries to write to them.
# DuckDB's COPY TO doesn't auto-create parent directories, and the Delta writer needs the
# raw zone to exist on first run. Each dataset gets its own namespaced subdirectory.
for zone in ("raw", "staging", "marts"):
    for dataset in DATASETS:
        (DATA_ROOT / zone / dataset.DATASET).mkdir(parents=True, exist_ok=True)


# ─── Bronze assets (Polars → Delta) ──────────────────────────────────────────
# One asset per SourceSpec, keyed [dataset, table] so the Delta IO manager lands them
# under data/raw/<dataset>/ automatically (see stack/raw_assets.py).
raw_assets = []
for dataset in DATASETS:
    raw_assets += build_raw_assets(dataset.DATASET, dataset.SOURCES)


# ─── dbt integration ─────────────────────────────────────────────────────────
# DbtProject wraps the dbt project dir and (in dev) regenerates the manifest if stale.
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=LakehouseDbtTranslator(),
)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Every dbt model in dbt_project/models/ becomes a Dagster asset here.

    Uses `dbt build` (not just `dbt run`) so tests run alongside model materialization.
    The translator maps each dbt source onto its dataset-prefixed bronze asset key, so
    Polars assets and dbt models share one unified lineage graph.
    """
    yield from dbt.cli(["build"], context=context).stream()


# ─── Definitions ─────────────────────────────────────────────────────────────

defs = Definitions(
    assets=[*raw_assets, dbt_models],
    resources={
        # IO manager every raw asset uses (io_manager_key="delta_io_manager").
        # base_dir stays data/raw; the asset key prefix [dataset] adds the namespace,
        # so key ["f1","raw_races"] → data/raw/f1/raw_races.delta/
        "delta_io_manager": PolarsDeltaIOManager(
            base_dir=str(DATA_ROOT / "raw"),
            mode="overwrite",
        ),
        # Lets Dagster invoke dbt CLI for materialization + tests. profiles_dir points at
        # the project dir so dbt finds profiles.yml alongside dbt_project.yml.
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
        ),
    },
)
