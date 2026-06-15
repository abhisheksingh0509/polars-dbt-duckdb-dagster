"""dbt integration helpers (stack-level, domain-agnostic).

Holds the translator that maps dbt sources to the Polars bronze asset keys, so a dbt
source and the Polars asset that produces it resolve to ONE node in Dagster's graph
(unified lineage) instead of two disconnected ones.
"""

from __future__ import annotations

from dagster import AssetKey
from dagster_dbt import DagsterDbtTranslator


class LakehouseDbtTranslator(DagsterDbtTranslator):
    """Map dbt sources onto the namespaced Polars bronze asset keys.

    Polars bronze assets are keyed `[dataset, table]` (e.g. ["f1", "raw_races"]) via the
    key_prefix in build_raw_assets. dbt declares those same tables as sources, but in its
    own grouping (e.g. source('raw', 'raw_races')). We translate the dbt source's name to
    `[dataset, name]` so both sides land on the same AssetKey.

    Convention: the dbt source's parent group name IS the dataset (e.g. a source group
    named "f1" → prefix ["f1"]). This keeps the mapping data-driven — add a dataset by
    naming its dbt source group after it, no code change here.
    """

    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            dataset = dbt_resource_props["source_name"]
            return AssetKey([dataset, dbt_resource_props["name"]])
        return super().get_asset_key(dbt_resource_props)
