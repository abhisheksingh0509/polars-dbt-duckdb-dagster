"""SourceSpec — the bronze contract a dataset declares for each raw table.

This is the linchpin of the stack/dataset split. A dataset (e.g. datasets/f1/sources.py)
exports a list of these; the engine (`build_raw_assets`) turns each into a Dagster asset
that writes a Delta table. The design principle is **config for shape, named-function
escape hatch for logic**:

  - `extractor` describes HOW to GET the data (pure mechanics, fully reusable).
  - `shape` is an optional Python callable for the messy per-source reshaping that can't
    be expressed declaratively (e.g. exploding a nested array). Most sources need none.

Keeping `shape` a function reference — not more YAML keywords — is what stops this from
slowly becoming a bad in-house transformation DSL.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from pipelines.stack.extractors import Extractor


@dataclass(frozen=True)
class SourceSpec:
    """One bronze source.

    Args:
        name: logical table name (e.g. "raw_races"). Becomes the Dagster asset name; the
            dataset's key prefix (applied in build_raw_assets) namespaces it on disk.
        extractor: how to fetch the records (any Extractor implementation).
        shape: optional post-fetch transform, records -> records. The escape hatch for
            per-source logic. None = land the records as-is.
        group: Dagster asset group name. Defaults to "raw" (the bronze layer).
        partition_column: when the dataset is partitioned (see build_raw_assets'
            partitions_def), the Delta column to partition this source by. The engine
            stamps it with the partition key at write time and hands it to the Delta IO
            manager's `partition_by`, so each partition replaces only its own slice.
            None = unpartitioned (the column is not added).
    """

    name: str
    extractor: Extractor
    shape: Callable[[list[dict]], list[dict]] | None = None
    group: str = "raw"
    partition_column: str | None = None
