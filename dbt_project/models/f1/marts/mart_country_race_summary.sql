-- Mart: F1 races aggregated by country.
-- Reads:  data/staging/f1/stg_races.parquet (via ref('stg_races') below)
-- Writes: data/marts/f1/mart_country_race_summary.parquet
--
-- Business question: "Where does F1 visit most often, and what does the calendar look
-- like geographically?" Useful for sponsorship targeting, regional content planning,
-- understanding the sport's footprint.
--
-- Grain change: stg_races has one row per race; this mart has one row per country —
-- that grain shift is what makes it a "mart" rather than another staging model.

{{ config(location = dataset_location('marts')) }}

SELECT
    circuit_country                          AS country,
    COUNT(*)                                 AS race_count,
    STRING_AGG(DISTINCT circuit_name, ', ')  AS circuits,
    MIN(race_date)                           AS first_race_date,
    MAX(race_date)                           AS latest_race_date,
    AVG(circuit_lat)                         AS centroid_lat,
    AVG(circuit_long)                        AS centroid_long
FROM {{ ref('stg_races') }}
GROUP BY circuit_country
ORDER BY race_count DESC, country
