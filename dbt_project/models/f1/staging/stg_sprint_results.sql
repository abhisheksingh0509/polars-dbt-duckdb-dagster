-- Staging model: clean and conform raw_sprint_results.
-- Reads:  data/raw/f1/raw_sprint_results.delta  (Delta, via dbt-duckdb delta plugin)
-- Writes: data/staging/f1/stg_sprint_results.parquet
--
-- Sprint results share the shape of regular results but only the ~6 sprint weekends per
-- season have them. We keep only what the championship marts need: identity + points.
-- Sprint points are folded into the driver/constructor totals (wins/podiums/poles stay
-- Grand-Prix-only — see the marts).

{{ config(location = dataset_location('staging')) }}

SELECT
    CAST(season AS INTEGER)                   AS season,
    CAST(round AS INTEGER)                    AS round,
    "raceName"                                AS race_name,

    -- Identity (driver + team)
    Driver['driverId']                        AS driver_id,
    Constructor['constructorId']              AS constructor_id,
    Constructor['name']                       AS constructor_name,

    -- Points + classification
    position                                  AS position_raw,
    TRY_CAST(position AS INTEGER)             AS finishing_position,
    CAST(points AS DOUBLE)                    AS points,
    CAST(grid AS INTEGER)                     AS grid_position,
    status                                    AS finish_status
FROM {{ source('f1', 'raw_sprint_results') }}
