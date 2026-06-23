"""build_raw_assets — turn a dataset's SourceSpec list into Dagster bronze assets.

This is the engine that replaces the old hand-written, copy-pasted raw_* assets. One
factory call per dataset; each SourceSpec becomes one @asset that fetches, optionally
shapes, and returns a Polars DataFrame for the Delta IO manager to persist.

Namespacing trick: assets are created with `key_prefix=[dataset]`, e.g. asset key
["f1", "raw_races"]. PolarsDeltaIOManager is a UPathIOManager — it writes to
`base_dir / *asset_key.path` — so the prefix alone lands the Delta table at
`data/raw/f1/raw_races.delta/` with no IO-manager configuration. The dbt translator
(stack/dbt.py) mirrors the same prefix so lineage stays unified.
"""

import polars as pl
from dagster import AssetExecutionContext, AssetsDefinition, asset

from pipelines.stack.specs import SourceSpec


def build_raw_assets(
    dataset: str,
    specs: list[SourceSpec],
    *,
    io_manager_key: str = "delta_io_manager",
) -> list[AssetsDefinition]:
    """Build one bronze asset per SourceSpec, namespaced under `dataset`.

    Args:
        dataset: dataset name used as the asset key prefix (e.g. "f1"). Drives both the
            Dagster lineage namespace and the on-disk Delta path.
        specs: the dataset's bronze sources.
        io_manager_key: which IO manager persists the DataFrames (Delta by default).
    """
    return [_make_asset(dataset, spec, io_manager_key) for spec in specs]


def _make_asset(
    dataset: str, spec: SourceSpec, io_manager_key: str
) -> AssetsDefinition:
    """Build one bronze asset. A dedicated factory so each asset closes over its own
    `spec`/`dataset` (avoids the classic loop-variable late-binding trap)."""

    @asset(
        name=spec.name,
        key_prefix=[dataset],
        group_name=f"{dataset}_{spec.group}",
        io_manager_key=io_manager_key,
    )
    def _raw_asset(context: AssetExecutionContext) -> pl.DataFrame:
        records = spec.extractor.fetch(context)
        if spec.shape is not None:
            records = spec.shape(records)
        context.log.info(f"[{dataset}/{spec.name}] fetched {len(records)} records")
        return pl.DataFrame(records)

    return _raw_asset
