---
title: Constructors Standings
---

# 2024 Constructors Championship

```sql standings
SELECT
  constructor_name,
  races_entered,
  total_points,
  wins,
  podiums,
  points_finishes,
  pole_positions,
  dnfs,
  ROUND(avg_finishing_position, 1) AS avg_finish
FROM lakehouse.mart_constructor_standings
ORDER BY total_points DESC
```

## Full standings table

<DataTable data={standings} rows=25>
  <Column id=constructor_name title="Constructor" />
  <Column id=total_points title="Points" />
  <Column id=wins title="Wins" />
  <Column id=podiums title="Podiums" />
  <Column id=dnfs title="DNFs" />
</DataTable>

## Points distribution

<BarChart
  data={standings}
  x=constructor_name
  y=total_points
  title="Total points by constructor"
  sort=false
/>

## Wins vs. Podiums

Bigger bubbles = more total points.

<ScatterPlot
  data={standings}
  x=wins
  y=podiums
  series=constructor_name
  size=total_points
  title="Wins vs Podiums (sized by total points)"
/>
