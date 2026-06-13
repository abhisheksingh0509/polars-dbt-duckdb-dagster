---
title: Driver Standings
---

# 2024 Driver Championship

```sql standings
SELECT
  driver_name,
  driver_code,
  primary_constructor AS team,
  nationality,
  total_points,
  wins,
  podiums,
  points_finishes,
  pole_positions,
  dnfs,
  ROUND(avg_finishing_position, 1) AS avg_finish
FROM lakehouse.mart_driver_standings
ORDER BY total_points DESC
```

## Full standings table

<DataTable data={standings} rows=25>
  <Column id=driver_code title="Code" />
  <Column id=driver_name title="Driver" />
  <Column id=team title="Team" />
  <Column id=nationality title="Nationality" />
  <Column id=total_points title="Points" />
  <Column id=wins title="Wins" />
  <Column id=podiums title="Podiums" />
  <Column id=dnfs title="DNFs" />
</DataTable>

## Points distribution

<BarChart
  data={standings}
  x=driver_code
  y=total_points
  title="Total points by driver"
  sort=false
/>

## Wins vs. Podiums

Bigger bubbles = more total points.

<ScatterPlot
  data={standings}
  x=wins
  y=podiums
  series=team
  size=total_points
  title="Wins vs Podiums (sized by total points)"
/>
