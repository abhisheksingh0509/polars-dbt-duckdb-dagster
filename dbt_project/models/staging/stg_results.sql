-- Staging model: clean and conform raw_results.
-- Reads:  data/raw/raw_results.delta  (Delta, via dbt-duckdb delta plugin)
-- Writes: data/staging/stg_results.parquet
--
-- What this model does:
--   1. Cast numeric fields (season, round, points, grid, laps)
--   2. Flatten the nested Driver and Constructor structs
--   3. position is "1"/"2"/.../"R" (retired)/"D" (DNS) — keep the raw and add a
--      nullable INTEGER `finishing_position` (NULL when not numeric)
--   4. Pull optional Time.millis and FastestLap fields via TRY_CAST so missing
--      values (DNFs, mid-pack finishes) become NULL instead of erroring

{{ config(
    location = env_var('LAKEHOUSE_DATA_ROOT', '../data') ~ '/staging/' ~ this.name ~ '.parquet'
) }}

SELECT
    CAST(season AS INTEGER)                   AS season,
    CAST(round AS INTEGER)                    AS round,
    "raceName"                                AS race_name,

    -- Identity (driver + team)
    Driver['driverId']                        AS driver_id,
    Constructor['constructorId']              AS constructor_id,
    Constructor['name']                       AS constructor_name,

    -- Numbers
    TRY_CAST(number AS INTEGER)               AS car_number,
    position                                  AS position_raw,
    TRY_CAST(position AS INTEGER)             AS finishing_position,
    CAST(points AS DOUBLE)                    AS points,
    CAST(grid AS INTEGER)                     AS grid_position,
    CAST(laps AS INTEGER)                     AS laps_completed,
    status                                    AS finish_status,

    -- Race-finish time (only populated for lead-lap finishers)
    TRY_CAST(Time['millis'] AS BIGINT)        AS race_time_ms,

    -- Fastest-lap metadata (optional — not always recorded)
    TRY_CAST(FastestLap['rank'] AS INTEGER)   AS fastest_lap_rank,
    TRY_CAST(FastestLap['lap'] AS INTEGER)    AS fastest_lap_number
FROM {{ source('raw', 'raw_results') }}
