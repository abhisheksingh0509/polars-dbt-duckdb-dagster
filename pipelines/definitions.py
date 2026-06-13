"""Dagster Definitions: the entry point that wires the whole pipeline together.

Referenced by Dockerfile's CMD: `dagster dev -m pipelines.definitions`
Dagster's CLI imports this module and looks for a top-level `defs` object.

What's wired here:
  - Polars extract assets (from pipelines.assets.*) — write Delta to data/raw/
  - dbt models (auto-discovered from dbt_project/) — write Parquet to data/staging/+marts/
  - PolarsDeltaIOManager → bridges Polars DataFrames ↔ Delta tables
  - DbtCliResource → lets Dagster invoke the dbt CLI for materializations
"""

from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, Definitions
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets
from dagster_polars import PolarsDeltaIOManager

from pipelines.assets import raw

# Resolve paths from this file's location so they work both:
#   - Locally (e.g. /Users/you/.../polars-dbt-duckdb-dagster/data/)
#   - Inside the Docker container (/opt/dagster/app/data/)
# Both contexts have the same layout: <project_root>/pipelines/definitions.py + <project_root>/data/
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = PROJECT_ROOT / "data"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"

# Make sure the lakehouse zones exist before any materialization tries to write to them.
# DuckDB's COPY TO doesn't auto-create parent directories, and the Delta writer needs
# raw/ to exist on first run.
for zone in ("raw", "staging", "marts"):
    (DATA_ROOT / zone).mkdir(parents=True, exist_ok=True)


# ─── dbt integration ─────────────────────────────────────────────────────────

# DbtProject wraps the dbt project directory and (in dev mode) auto-generates the
# manifest via `dbt parse` if it's missing or stale. The manifest is what dagster-dbt
# reads to discover dbt models and turn them into Dagster assets.
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()


class LakehouseDbtTranslator(DagsterDbtTranslator):
    """Map dbt sources to existing Polars asset keys, so lineage is unified.

    Default behavior: dbt source('raw', 'raw_races') → AssetKey(['raw', 'raw_races'])
    But our Polars asset key is just AssetKey(['raw_races']) — no source prefix.

    By stripping the source-group prefix, dbt's view of the raw layer connects to
    the same Dagster asset Polars writes — one node in the UI, not two.
    """

    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            return AssetKey([dbt_resource_props["name"]])
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=LakehouseDbtTranslator(),
)
def f1_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Every dbt model in dbt_project/models/ becomes a Dagster asset here.

    Triggered when you materialize a dbt-backed asset in the Dagster UI.
    Uses `dbt build` (not just `dbt run`) so tests run alongside model materialization.
    """
    yield from dbt.cli(["build"], context=context).stream()


# ─── Definitions ─────────────────────────────────────────────────────────────

defs = Definitions(
    assets=[
        raw.raw_races,
        raw.raw_drivers,
        raw.raw_results,
        f1_dbt_assets,
    ],
    resources={
        # IO manager every raw asset uses (io_manager_key="delta_io_manager").
        # Asset key ["raw_races"] + base_dir → data/raw/raw_races.delta/
        "delta_io_manager": PolarsDeltaIOManager(
            base_dir=str(DATA_ROOT / "raw"),
        ),
        # Lets Dagster invoke dbt CLI for materialization + tests.
        # profiles_dir points at the project dir so dbt finds profiles.yml alongside
        # dbt_project.yml (not the default ~/.dbt/).
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROJECT_DIR),
        ),
    },
)
