{% macro dataset_location(layer) %}
    {#-
        Build the external Parquet path for a model, namespaced by dataset.

        NB: named dataset_location, NOT external_location — dbt-duckdb already defines a
        built-in `external_location` macro that its external materialization calls
        internally; shadowing it breaks the build.

        Returns:  <LAKEHOUSE_DATA_ROOT>/<layer>/<dataset>/<model_name>.parquet

        The dataset is derived from the model's own folder. Our layout is
        models/<dataset>/<layer>/<model>.sql, and dbt's `model.fqn` for such a model
        is [<project>, <dataset>, <layer>, <model>] — so fqn[1] is the dataset. This
        means models never hardcode their dataset name (no copy-paste "/f1/" footgun
        when a second dataset is added). `layer` ('staging' | 'marts') is passed
        explicitly rather than inferred, so deeper subfolders don't break it.

        Mirrors the bronze key-prefix namespacing in pipelines/stack/raw_assets.py.

        Usage (top of a model):
            {{ config(location = external_location('staging')) }}
    -#}
    {%- set dataset = model.fqn[1] -%}
    {%- set data_root = env_var('LAKEHOUSE_DATA_ROOT', '../data') -%}
    {{ return(data_root ~ '/' ~ layer ~ '/' ~ dataset ~ '/' ~ this.name ~ '.parquet') }}
{% endmacro %}
