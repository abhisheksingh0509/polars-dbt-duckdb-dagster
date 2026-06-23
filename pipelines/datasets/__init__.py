"""Datasets — the payloads the stack runs.

Each subpackage (datasets/f1/, datasets/nyc_taxi/, ...) is a self-contained bundle: it
declares its bronze sources here, owns its dbt models under dbt_project/models/<name>/,
and its Evidence pages under evidence/pages/<name>/. The stack engine is dataset-blind;
to add a dataset you add a folder here and register its SOURCES in definitions.py.
"""
