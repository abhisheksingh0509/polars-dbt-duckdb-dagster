"""F1 bronze source registry — the F1-specific payload.

Everything F1 lives here: the API base, the season, the per-source endpoints/paths, and
the one bit of reshaping logic the declarative config can't express (exploding results
out of their parent races). The stack engine consumes `SOURCES` and knows nothing about
any of this.

To add another F1 table (e.g. constructors, qualifying), append a SourceSpec — no engine
change. To add a non-API source type, add an Extractor in stack/extractors.py.
"""

from __future__ import annotations

from dagster import StaticPartitionsDefinition

from pipelines.stack import RestApiExtractor, SourceSpec

# Dataset identity — used as the asset key prefix (→ data/raw/f1/...) and the dbt source
# group name (see dbt_project/models/f1/staging/sources.yml).
DATASET = "f1"

ERGAST_BASE = "https://api.jolpi.ca/ergast/f1"

# Seasons to ingest. Each becomes a Dagster partition (selectable / backfillable in the
# UI) and a `season` partition in the bronze Delta tables. Add a year here to ingest it —
# no other code change. The bronze assets stamp `season` = partition key, so re-running
# one season replaces only that season's slice (see stack/raw_assets.py).
SEASONS = ["2023", "2024"]
PARTITIONS_DEF = StaticPartitionsDefinition(SEASONS)


def _explode(races: list[dict], results_key: str) -> list[dict]:
    """Explode a per-race results array into one record per (race, driver).

    The API nests the results (``Results`` for the Grand Prix, ``SprintResults`` for the
    sprint) inside each race; we flatten them and tag each row with the race it belongs to
    (season/round/raceName), which the nesting would otherwise drop — so each row stands
    alone for downstream joins.
    """
    rows: list[dict] = []
    for race in races:
        for result in race.get(results_key, []):
            result["season"] = race["season"]
            result["round"] = race["round"]
            result["raceName"] = race["raceName"]
            rows.append(result)
    return rows


def shape_results(races: list[dict]) -> list[dict]:
    """One record per (Grand Prix, driver) — explodes each race's ``Results``."""
    return _explode(races, "Results")


def shape_sprint_results(races: list[dict]) -> list[dict]:
    """One record per (sprint, driver) — explodes each race's ``SprintResults``.

    Only the ~6 sprint weekends per season carry these, so this is a small table. Sprint
    points are added on top of Grand Prix points to make the championship totals correct.
    """
    return _explode(races, "SprintResults")


# The `{partition}` placeholder in each URL is filled with the season (partition key) at
# fetch time; `partition_column="season"` lands each season in its own Delta partition.
SOURCES = [
    SourceSpec(
        name="raw_races",
        extractor=RestApiExtractor(
            f"{ERGAST_BASE}/{{partition}}/races.json",
            container_path=["RaceTable", "Races"],
        ),
        partition_column="season",
    ),
    SourceSpec(
        name="raw_drivers",
        extractor=RestApiExtractor(
            f"{ERGAST_BASE}/{{partition}}/drivers.json",
            container_path=["DriverTable", "Drivers"],
        ),
        partition_column="season",
    ),
    SourceSpec(
        name="raw_results",
        extractor=RestApiExtractor(
            f"{ERGAST_BASE}/{{partition}}/results.json",
            container_path=["RaceTable", "Races"],
        ),
        shape=shape_results,
        partition_column="season",
    ),
    SourceSpec(
        name="raw_sprint_results",
        extractor=RestApiExtractor(
            f"{ERGAST_BASE}/{{partition}}/sprint.json",
            container_path=["RaceTable", "Races"],
        ),
        shape=shape_sprint_results,
        partition_column="season",
    ),
]
