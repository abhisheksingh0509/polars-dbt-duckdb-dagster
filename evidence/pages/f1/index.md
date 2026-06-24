---
title: F1 Lakehouse
---

A code-first BI tour of the local lakehouse. Data flows: **Jolpica API → Polars / Delta (bronze) → dbt + DuckDB (silver, gold) → here**.

<Dropdown name=season title="Season" defaultValue=2024>
  <DropdownOption value=2024 />
  <DropdownOption value=2023 />
</Dropdown>

## {inputs.season.value} at a glance

```sql totals
SELECT
  (SELECT COUNT(*) FROM lakehouse.mart_country_race_summary WHERE season = ${inputs.season.value}) AS countries,
  (SELECT SUM(race_count) FROM lakehouse.mart_country_race_summary WHERE season = ${inputs.season.value}) AS races,
  (SELECT COUNT(*) FROM lakehouse.mart_driver_standings WHERE season = ${inputs.season.value}) AS drivers,
  (SELECT SUM(total_points) FROM lakehouse.mart_driver_standings WHERE season = ${inputs.season.value}) AS total_points
```

<BigValue data={totals} value=countries title="Countries visited" />
<BigValue data={totals} value=races title="Races run" />
<BigValue data={totals} value=drivers title="Drivers" />
<BigValue data={totals} value=total_points title="Total points awarded" />

## Top of the championship

```sql top5
SELECT
  driver_name,
  driver_code,
  primary_constructor AS team,
  total_points,
  wins,
  podiums
FROM lakehouse.mart_driver_standings
WHERE season = ${inputs.season.value}
ORDER BY total_points DESC
LIMIT 5
```

<DataTable data={top5} />

## Navigate

- [**Driver standings**](/f1/drivers) — full table, points distribution, wins vs podiums
- [**Constructor standings**](/f1/constructors) — team championship
- [**Geography**](/f1/countries) — where F1 raced each season

_The season selector above applies to this page. Each linked page has its own selector._
