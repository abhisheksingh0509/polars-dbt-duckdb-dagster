-- Staging model: clean and conform raw_drivers.
-- Reads:  data/raw/raw_drivers.delta  (Delta, via dbt-duckdb delta plugin)
-- Writes: data/staging/stg_drivers.parquet
--
-- What this model does:
--   1. Rename camelCase JSON fields → snake_case
--   2. Cast date_of_birth string → DATE
--   3. Cast permanent_number (sometimes empty) → nullable INTEGER
--   4. Add a convenience full_name column

{{ config(
    location = env_var('LAKEHOUSE_DATA_ROOT', '../data') ~ '/staging/' ~ this.name ~ '.parquet'
) }}

SELECT
    "driverId"                                       AS driver_id,
    TRY_CAST(NULLIF("permanentNumber", '') AS INTEGER) AS permanent_number,
    code                                             AS driver_code,
    "givenName"                                      AS given_name,
    "familyName"                                     AS family_name,
    "givenName" || ' ' || "familyName"               AS full_name,
    CAST("dateOfBirth" AS DATE)                      AS date_of_birth,
    nationality                                      AS nationality,
    url                                              AS driver_url
FROM {{ source('raw', 'raw_drivers') }}
