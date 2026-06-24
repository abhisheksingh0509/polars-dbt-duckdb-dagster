"""Extractors — the pluggable front-ends that get raw records into the pipeline.

This is STACK code (domain-agnostic). An extractor's only job is "get bytes from
somewhere, hand back a list of records." Once those records land in Delta bronze, the
rest of the lakehouse doesn't know or care where they came from — that's the whole point
of the bronze contract.

Today there's one extractor (`RestApiExtractor`, lifted verbatim from the old
F1-specific raw.py). Adding a `FileExtractor` (CSV/Parquet/S3) later just means writing
another class that satisfies the `Extractor` protocol — no change to the engine or to
any existing dataset.
"""

from __future__ import annotations

import time
from typing import Protocol, runtime_checkable

import httpx
from dagster import AssetExecutionContext


@runtime_checkable
class Extractor(Protocol):
    """The bronze-source contract: anything that can produce a list of records.

    A dataset's SourceSpec (see specs.py) pairs one of these with an optional shaping
    function. `build_raw_assets` calls `fetch()` inside a Dagster asset and wraps the
    result in a Polars DataFrame for the Delta IO manager to persist.
    """

    def fetch(self, context: AssetExecutionContext) -> list[dict]: ...


# ─── REST API extractor ──────────────────────────────────────────────────────
# Defaults tuned for Jolpica's free tier, but nothing here is F1-specific — any
# Ergast-shaped offset-paginated JSON API works. Override per source as needed.

DEFAULT_TIMEOUT = 60.0   # some APIs are slow assembling nested responses
DEFAULT_PAGE_SIZE = 30   # smaller pages = faster server-side assembly = fewer timeouts
DEFAULT_PAUSE = 1.0      # 1 req/s — Jolpica's free tier rate-limits hard at ~4 req/s burst
DEFAULT_MAX_RETRIES = 5  # retry on transient timeouts and 429s
DEFAULT_BACKOFF = 2.0    # seconds; multiplied by attempt number


class RestApiExtractor:
    """Walk an offset-paginated JSON API, returning a flat list of records.

    Args:
        url: full endpoint URL (e.g. "https://api.jolpi.ca/ergast/f1/2024/races.json").
        container_path: JSON path inside the "MRData" envelope where the record list
            lives, e.g. ["RaceTable", "Races"]. The walker descends these keys.
        page_size / pause / timeout / max_retries / backoff: paging + resilience knobs.
        total_key / envelope_key: where the paging-total and the record envelope live;
            defaulted to Ergast's shape but overridable for other APIs.
    """

    def __init__(
        self,
        url: str,
        container_path: list[str],
        *,
        page_size: int = DEFAULT_PAGE_SIZE,
        pause: float = DEFAULT_PAUSE,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff: float = DEFAULT_BACKOFF,
        envelope_key: str = "MRData",
        total_key: str = "total",
    ) -> None:
        self.url = url
        self.container_path = container_path
        self.page_size = page_size
        self.pause = pause
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff
        self.envelope_key = envelope_key
        self.total_key = total_key

    def _get_with_retry(
        self, url: str, context: AssetExecutionContext
    ) -> httpx.Response:
        """GET with retry on timeouts and 429 rate-limit responses."""
        last_exc: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = httpx.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code != 429 or attempt == self.max_retries:
                    raise
                # Respect Retry-After if the server sends it, else exponential backoff
                retry_after = exc.response.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else self.backoff * (2 ** (attempt - 1))
                context.log.warning(
                    f"429 rate-limited on {url} (attempt {attempt}/{self.max_retries}), "
                    f"waiting {wait}s"
                )
                last_exc = exc
                time.sleep(wait)
            except (httpx.ReadTimeout, httpx.ConnectTimeout) as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    wait = self.backoff * attempt
                    context.log.warning(
                        f"Timeout on {url} (attempt {attempt}/{self.max_retries}), "
                        f"retrying in {wait}s"
                    )
                    time.sleep(wait)
        assert last_exc is not None
        raise last_exc

    def fetch(self, context: AssetExecutionContext) -> list[dict]:
        # A `{partition}` placeholder in the URL is substituted with the Dagster
        # partition key at fetch time (e.g. the season "2024"). Datasets that don't
        # partition leave the placeholder out and the URL is used verbatim.
        url = (
            self.url.format(partition=context.partition_key)
            if "{partition}" in self.url
            else self.url
        )

        items: list[dict] = []
        offset = 0
        while True:
            response = self._get_with_retry(
                f"{url}?limit={self.page_size}&offset={offset}", context
            )
            payload = response.json()[self.envelope_key]

            # Descend into the container_path to find the actual record list
            container = payload
            for key in self.container_path:
                container = container[key]

            if not container:
                break
            items.extend(container)

            total = int(payload[self.total_key])
            offset += self.page_size
            if offset >= total:
                break
            time.sleep(self.pause)

        return items
