"""Formula 1 dataset — bronze source declarations.

Data source: api.jolpi.ca — a community-maintained mirror of the original Ergast F1 API
(deprecated end of 2024). URL structure is identical to Ergast, so old Ergast docs apply.
"""

from pipelines.datasets.f1.sources import DATASET, SOURCES

__all__ = ["DATASET", "SOURCES"]
