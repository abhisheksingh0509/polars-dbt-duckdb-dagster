-- Staging model: clean and conform raw_races.
-- Reads:  data/raw/f1/raw_races.delta (Delta, via dbt-duckdb delta plugin)
-- Writes: data/staging/f1/stg_races.parquet
--
-- What this model does:
--   1. Flatten the nested Circuit struct into top-level columns
--   2. Cast string season/round/lat/long into proper numeric types
--   3. Cast the date string to a real DATE
--   4. Rename camelCase JSON fields to snake_case (downstream-friendly)

{{ config(location = dataset_location('staging')) }}

SELECT
    -- Strings from the API → typed columns
    CAST(season AS INTEGER)                     AS season,
    CAST(round AS INTEGER)                      AS round,
    CAST("date" AS DATE)                        AS race_date,
    "time"                                      AS race_time_utc,   -- e.g. "15:00:00Z"

    -- Top-level camelCase → snake_case
    "raceName"                                  AS race_name,
    url                                         AS race_url,

    -- Flatten the nested Circuit struct (bracket syntax preserves DuckDB field-name casing)
    Circuit['circuitId']                        AS circuit_id,
    Circuit['circuitName']                      AS circuit_name,
    Circuit['Location']['locality']             AS circuit_locality,
    Circuit['Location']['country']              AS circuit_country,
    CAST(Circuit['Location']['lat'] AS DOUBLE)  AS circuit_lat,
    CAST(Circuit['Location']['long'] AS DOUBLE) AS circuit_long

FROM {{ source('f1', 'raw_races') }}
