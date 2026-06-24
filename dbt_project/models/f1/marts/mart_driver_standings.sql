-- Mart: driver championship standings.
-- Reads:  stg_results + stg_drivers + stg_sprint_results
-- Writes: data/marts/f1/mart_driver_standings.parquet
--
-- Business question: "Who's winning the championship — and how do they get there?"
-- Grain: one row per (driver, season). Aggregates across all races in a season.
--
-- total_points = Grand Prix points + sprint points (the real championship total). The
-- counting stats (wins/podiums/pole_positions) stay Grand-Prix-only — those are the
-- headline numbers people expect; sprint wins/poles aren't folded in. race_points and
-- sprint_points are exposed separately for transparency.

{{ config(location = dataset_location('marts')) }}

WITH results_with_driver_info AS (
    SELECT
        r.season,
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
    -- Join on season too: drivers are ingested per season, so an unqualified
    -- USING(driver_id) would fan out a two-season driver's results.
    LEFT JOIN {{ ref('stg_drivers') }} d
        ON r.driver_id = d.driver_id AND r.season = d.season
),

driver_race_stats AS (
    SELECT
        season,
        driver_id,
        -- ANY_VALUE picks one row's value for non-aggregated columns. Safe here because
        -- driver attributes (name, nationality) are constant for a driver within a season.
        ANY_VALUE(full_name)            AS driver_name,
        ANY_VALUE(driver_code)          AS driver_code,
        ANY_VALUE(nationality)          AS nationality,
        ANY_VALUE(date_of_birth)        AS date_of_birth,

        -- Most-frequent constructor for the season (a few drivers switch mid-season)
        MODE(constructor_name)          AS primary_constructor,

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
    FROM results_with_driver_info
    GROUP BY season, driver_id
),

-- Sprint points per (driver, season) — only the ~6 sprint weekends contribute.
driver_sprint_points AS (
    SELECT
        season,
        driver_id,
        SUM(points) AS sprint_points
    FROM {{ ref('stg_sprint_results') }}
    GROUP BY season, driver_id
)

SELECT
    -- Surrogate key for the (driver, season) grain — dbt-core has no multi-column
    -- unique test, so schema.yml tests uniqueness on this instead.
    s.season || '-' || s.driver_id                  AS driver_season_key,
    s.season,
    s.driver_id,
    s.driver_name,
    s.driver_code,
    s.nationality,
    s.date_of_birth,
    s.primary_constructor,

    -- Championship total = Grand Prix + sprint points
    s.race_points + COALESCE(sp.sprint_points, 0)   AS total_points,
    s.race_points,
    COALESCE(sp.sprint_points, 0)                   AS sprint_points,

    s.races_entered,
    s.wins,
    s.podiums,
    s.points_finishes,
    s.dnfs,
    s.pole_positions,
    s.avg_finishing_position
FROM driver_race_stats s
LEFT JOIN driver_sprint_points sp USING (season, driver_id)
ORDER BY s.season DESC, total_points DESC, s.wins DESC
