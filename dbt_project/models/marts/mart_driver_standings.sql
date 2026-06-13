-- Mart: driver championship standings.
-- Reads:  stg_results + stg_drivers
-- Writes: data/marts/mart_driver_standings.parquet
--
-- Business question: "Who's winning the championship — and how do they get there?"
-- Grain: one row per driver. Aggregates across all races loaded in stg_results.

{{ config(
    location = env_var('LAKEHOUSE_DATA_ROOT', '../data') ~ '/marts/' ~ this.name ~ '.parquet'
) }}

WITH results_with_driver_info AS (
    SELECT
        r.driver_id,
        r.constructor_name,
        r.points,
        r.finishing_position,
        r.grid_position,
        d.full_name,
        d.driver_code,
        d.nationality,
        d.date_of_birth
    FROM {{ ref('stg_results') }} r
    LEFT JOIN {{ ref('stg_drivers') }} d USING (driver_id)
)

SELECT
    driver_id,
    -- ANY_VALUE picks one row's value for non-aggregated columns. Safe here because
    -- driver attributes (name, nationality) are constant for a given driver_id.
    ANY_VALUE(full_name)            AS driver_name,
    ANY_VALUE(driver_code)          AS driver_code,
    ANY_VALUE(nationality)          AS nationality,
    ANY_VALUE(date_of_birth)        AS date_of_birth,

    -- Most-frequent constructor for the season (a few drivers switch mid-season)
    MODE(constructor_name)          AS primary_constructor,

    -- Championship numbers
    SUM(points)                                                  AS total_points,
    COUNT(*)                                                     AS races_entered,
    COUNT_IF(finishing_position = 1)                             AS wins,
    COUNT_IF(finishing_position BETWEEN 1 AND 3)                 AS podiums,
    COUNT_IF(finishing_position BETWEEN 1 AND 10)                AS points_finishes,
    COUNT_IF(finishing_position IS NULL)                         AS dnfs,
    COUNT_IF(grid_position = 1)                                  AS pole_positions,

    -- Average finishing position when classified (DNFs excluded)
    AVG(finishing_position)         AS avg_finishing_position
FROM results_with_driver_info
GROUP BY driver_id
ORDER BY total_points DESC, wins DESC
