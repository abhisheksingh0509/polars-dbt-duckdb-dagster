"""Raw layer assets — extract F1 data from Jolpica, write Delta tables to data/raw/.

Each @asset returns a Polars DataFrame. PolarsDeltaIOManager (wired in definitions.py)
handles the actual Delta write — assets don't touch the filesystem directly.

Data source: api.jolpi.ca — a community-maintained mirror of the original Ergast F1 API
(which was deprecated end of 2024). The URL structure is intentionally identical to
Ergast, so any old Ergast docs/queries you find still apply.
"""

import time

import httpx
import polars as pl
from dagster import AssetExecutionContext, asset

ERGAST_BASE = "https://api.jolpi.ca/ergast/f1"
SEASON = 2024  # TODO: parameterize via Dagster partitions once we add multiple seasons

HTTP_TIMEOUT = 60.0   # Jolpica can be slow assembling nested responses (results, laps)
PAGE_SIZE = 30        # smaller pages = faster server-side assembly = fewer timeouts
REQUEST_PAUSE = 0.25  # ~4 req/s — well under Jolpica's free-tier limit
MAX_RETRIES = 3       # retry on transient timeouts (free tier can stall)
RETRY_BACKOFF = 2.0   # seconds; multiplied by attempt number


def _get_with_retry(url: str, context: AssetExecutionContext) -> httpx.Response:
    """GET with retry on ReadTimeout — Jolpica's free tier occasionally stalls."""
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = httpx.get(url, timeout=HTTP_TIMEOUT)
            response.raise_for_status()
            return response
        except (httpx.ReadTimeout, httpx.ConnectTimeout) as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * attempt
                context.log.warning(
                    f"Timeout on {url} (attempt {attempt}/{MAX_RETRIES}), retrying in {wait}s"
                )
                time.sleep(wait)
    assert last_exc is not None
    raise last_exc


def _fetch_paginated(
    url: str,
    container_path: list[str],
    context: AssetExecutionContext,
) -> list[dict]:
    """Walk Jolpica's offset-based pagination, returning a flat list of records.

    container_path is the JSON path inside MRData where the records live:
      - drivers.json   → ["DriverTable", "Drivers"]
      - races.json     → ["RaceTable", "Races"]
      - results.json   → ["RaceTable", "Races"]  (results nested under each race)
      - laps.json      → ["RaceTable", "Races"]  (laps nested under the race)
    """
    items: list[dict] = []
    offset = 0
    while True:
        response = _get_with_retry(
            f"{url}?limit={PAGE_SIZE}&offset={offset}", context
        )
        payload = response.json()["MRData"]

        # Descend into the container_path to find the actual list
        container = payload
        for key in container_path:
            container = container[key]

        if not container:
            break
        items.extend(container)

        total = int(payload["total"])
        offset += len(container)
        if offset >= total:
            break
        time.sleep(REQUEST_PAUSE)

    return items


@asset(io_manager_key="delta_io_manager", group_name="raw")
def raw_races(context: AssetExecutionContext) -> pl.DataFrame:
    """All F1 races for the season — calendar with circuit metadata."""
    races = _fetch_paginated(
        f"{ERGAST_BASE}/{SEASON}/races.json",
        container_path=["RaceTable", "Races"],
        context=context,
    )
    context.log.info(f"Fetched {len(races)} races for {SEASON}")
    return pl.DataFrame(races)


@asset(io_manager_key="delta_io_manager", group_name="raw")
def raw_drivers(context: AssetExecutionContext) -> pl.DataFrame:
    """All drivers active in the season — name, code, nationality, DOB."""
    drivers = _fetch_paginated(
        f"{ERGAST_BASE}/{SEASON}/drivers.json",
        container_path=["DriverTable", "Drivers"],
        context=context,
    )
    context.log.info(f"Fetched {len(drivers)} drivers for {SEASON}")
    return pl.DataFrame(drivers)


@asset(io_manager_key="delta_io_manager", group_name="raw")
def raw_results(context: AssetExecutionContext) -> pl.DataFrame:
    """Race results for the season — one row per (race, driver) finishing position.

    The API nests results inside races; we explode them and attach season/round so
    each row stands alone for downstream joins.
    """
    races = _fetch_paginated(
        f"{ERGAST_BASE}/{SEASON}/results.json",
        container_path=["RaceTable", "Races"],
        context=context,
    )

    rows: list[dict] = []
    for race in races:
        for result in race.get("Results", []):
            # Tag each result with the race it belongs to (lost by default in the nesting)
            result["season"] = race["season"]
            result["round"] = race["round"]
            result["raceName"] = race["raceName"]
            rows.append(result)

    context.log.info(f"Fetched {len(rows)} results across {len(races)} races")
    return pl.DataFrame(rows)


