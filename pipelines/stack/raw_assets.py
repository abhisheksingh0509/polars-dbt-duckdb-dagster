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
from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    PartitionsDefinition,
    asset,
)

from pipelines.stack.specs import SourceSpec


def build_raw_assets(
    dataset: str,
    specs: list[SourceSpec],
    *,
    io_manager_key: str = "delta_io_manager",
    partitions_def: PartitionsDefinition | None = None,
) -> list[AssetsDefinition]:
    """Build one bronze asset per SourceSpec, namespaced under `dataset`.

    Args:
        dataset: dataset name used as the asset key prefix (e.g. "f1"). Drives both the
            Dagster lineage namespace and the on-disk Delta path.
        specs: the dataset's bronze sources.
        io_manager_key: which IO manager persists the DataFrames (Delta by default).
        partitions_def: optional PartitionsDefinition applied to every source that
            declares a `partition_column`. With it set, each partition (e.g. a season)
            is stored in the same Delta table partitioned by that column — the Delta IO
            manager replaces only the partition's slice on re-materialization.
    """
    return [
        _make_asset(dataset, spec, io_manager_key, partitions_def) for spec in specs
    ]


def _make_asset(
    dataset: str,
    spec: SourceSpec,
    io_manager_key: str,
    partitions_def: PartitionsDefinition | None,
) -> AssetsDefinition:
    """Build one bronze asset. A dedicated factory so each asset closes over its own
    `spec`/`dataset` (avoids the classic loop-variable late-binding trap)."""

    # Partition the asset only when the dataset supplies a PartitionsDefinition AND the
    # source opts in via partition_column. The "partition_by" metadata tells
    # PolarsDeltaIOManager to store all partitions in one Delta table keyed by that
    # column (predicate-based overwrite per partition).
    partitioned = partitions_def is not None and spec.partition_column is not None

    @asset(
        name=spec.name,
        key_prefix=[dataset],
        group_name=f"{dataset}_{spec.group}",
        io_manager_key=io_manager_key,
        partitions_def=partitions_def if partitioned else None,
        metadata={"partition_by": spec.partition_column} if partitioned else None,
    )
    def _raw_asset(context: AssetExecutionContext) -> pl.DataFrame:
        records = spec.extractor.fetch(context)
        if spec.shape is not None:
            records = spec.shape(records)
        context.log.info(f"[{dataset}/{spec.name}] fetched {len(records)} records")
        df = pl.DataFrame(records)
        if partitioned and context.has_partition_key:
            # Stamp the partition column so its value matches the partition key the IO
            # manager uses to build the per-partition overwrite predicate. Overrides any
            # same-named column from the source so the two can never disagree.
            df = df.with_columns(
                pl.lit(context.partition_key).alias(spec.partition_column)
            )
        return df

    return _raw_asset
