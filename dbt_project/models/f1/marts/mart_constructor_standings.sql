-- Mart: constructors championship standings.
-- Reads:  stg_results
-- Writes: data/marts/f1/mart_constructor_standings.parquet
--
-- Business question: "Who's winning the championship — and how do they get there?"
-- Grain: one row per constructor. Aggregates across all races loaded in stg_results.

{{ config(location = dataset_location('marts')) }}

SELECT
    -- Most-frequent constructor for the season
    constructor_name,

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
FROM {{ ref('stg_results') }}
GROUP BY constructor_name
ORDER BY total_points DESC, wins DESC