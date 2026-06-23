"""F1 bronze source registry — the F1-specific payload.

Everything F1 lives here: the API base, the season, the per-source endpoints/paths, and
the one bit of reshaping logic the declarative config can't express (exploding results
out of their parent races). The stack engine consumes `SOURCES` and knows nothing about
any of this.

To add another F1 table (e.g. constructors, qualifying), append a SourceSpec — no engine
change. To add a non-API source type, add an Extractor in stack/extractors.py.
"""

from __future__ import annotations

from pipelines.stack import RestApiExtractor, SourceSpec

# Dataset identity — used as the asset key prefix (→ data/raw/f1/...) and the dbt source
# group name (see dbt_project/models/f1/staging/sources.yml).
DATASET = "f1"

ERGAST_BASE = "https://api.jolpi.ca/ergast/f1"
SEASON = 2024  # TODO: parameterize via Dagster partitions once we add multiple seasons


def shape_results(races: list[dict]) -> list[dict]:
    """Explode race results into one record per (race, driver).

    The API nests Results inside each race; we flatten them and tag each result with the
    race it belongs to (season/round/raceName), which the nesting would otherwise drop —
    so each row stands alone for downstream joins.
    """
    rows: list[dict] = []
    for race in races:
        for result in race.get("Results", []):
            result["season"] = race["season"]
            result["round"] = race["round"]
            result["raceName"] = race["raceName"]
            rows.append(result)
    return rows


SOURCES = [
    SourceSpec(
        name="raw_races",
        extractor=RestApiExtractor(
            f"{ERGAST_BASE}/{SEASON}/races.json",
            container_path=["RaceTable", "Races"],
        ),
    ),
    SourceSpec(
        name="raw_drivers",
        extractor=RestApiExtractor(
            f"{ERGAST_BASE}/{SEASON}/drivers.json",
            container_path=["DriverTable", "Drivers"],
        ),
    ),
    SourceSpec(
        name="raw_results",
        extractor=RestApiExtractor(
            f"{ERGAST_BASE}/{SEASON}/results.json",
            container_path=["RaceTable", "Races"],
        ),
        shape=shape_results,
    ),
]
