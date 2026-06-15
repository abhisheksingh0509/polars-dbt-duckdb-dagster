"""The lakehouse stack — domain-agnostic engine.

This package is the reusable machine: it knows how to extract records, build bronze
Dagster assets, and wire dbt into the asset graph. It contains NO knowledge of any
specific dataset (no F1, no NYC). Datasets live in pipelines/datasets/<name>/ and feed
the engine a list of SourceSpecs.
"""

from pipelines.stack.dbt import LakehouseDbtTranslator
from pipelines.stack.extractors import Extractor, RestApiExtractor
from pipelines.stack.raw_assets import build_raw_assets
from pipelines.stack.specs import SourceSpec

__all__ = [
    "Extractor",
    "RestApiExtractor",
    "SourceSpec",
    "build_raw_assets",
    "LakehouseDbtTranslator",
]
