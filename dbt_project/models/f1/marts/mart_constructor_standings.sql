-- Mart: constructors championship standings.
-- Reads:  stg_results + stg_sprint_results
-- Writes: data/marts/f1/mart_constructor_standings.parquet
--
-- Business question: "Who's winning the championship — and how do they get there?"
-- Grain: one row per (constructor, season). Aggregates across all races in a season.
--
-- total_points = Grand Prix points + sprint points. Counting stats (wins/podiums/poles)
-- stay Grand-Prix-only; race_points and sprint_points are exposed separately.

{{ config(location = dataset_location('marts')) }}

WITH constructor_race_stats AS (
    SELECT
        season,
        constructor_name,

        -- Grand Prix points + counting stats
        SUM(points)                                                  AS race_points,
        COUNT(*)                                                     AS races_entered,
        COUNT_IF(finishing_position = 1)                             AS wins,
        COUNT_IF(finishing_position BETWEEN 1 AND 3)                 AS podiums,
        COUNT_IF(finishing_position BETWEEN 1 AND 10)                AS points_finishes,
        COUNT_IF(finishing_position IS NULL)                         AS dnfs,
        COUNT_IF(grid_position = 1)                                  AS pole_positions,

        -- Average finishing position when classified (DNFs excluded)
        AVG(finishing_position)         AS avg_finishing_position
    FROM {{ ref('stg_results') }}
    GROUP BY season, constructor_name
),

-- Sprint points per (constructor, season) — only the ~6 sprint weekends contribute.
constructor_sprint_points AS (
    SELECT
        season,
        constructor_name,
        SUM(points) AS sprint_points
    FROM {{ ref('stg_sprint_results') }}
    GROUP BY season, constructor_name
)

SELECT
    -- Surrogate key for the (constructor, season) grain — schema.yml tests uniqueness here.
    c.season || '-' || c.constructor_name           AS constructor_season_key,
    c.season,
    c.constructor_name,

    -- Championship total = Grand Prix + sprint points
    c.race_points + COALESCE(sp.sprint_points, 0)   AS total_points,
    c.race_points,
    COALESCE(sp.sprint_points, 0)                   AS sprint_points,

    c.races_entered,
    c.wins,
    c.podiums,
    c.points_finishes,
    c.dnfs,
    c.pole_positions,
    c.avg_finishing_position
FROM constructor_race_stats c
LEFT JOIN constructor_sprint_points sp USING (season, constructor_name)
ORDER BY c.season DESC, total_points DESC, c.wins DESC
