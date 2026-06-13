-- Source query: exposes dbt's mart_country_race_summary view as queryable to
-- the Evidence pages. Evidence runs this once at `npm run sources` time,
-- caches the result, and dashboard queries `FROM lakehouse.mart_country_race_summary`
-- resolve to that cache.
SELECT * FROM mart_country_race_summary
