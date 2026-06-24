---
title: Driver Standings
---

<Dropdown name=season title="Season" defaultValue=2024>
  <DropdownOption value=2024 />
  <DropdownOption value=2023 />
</Dropdown>

# {inputs.season.value} Driver Championship

```sql standings
SELECT
  driver_name,
  driver_code,
  primary_constructor AS team,
  nationality,
  total_points,
  sprint_points,
  wins,
  podiums,
  points_finishes,
  pole_positions,
  dnfs,
  ROUND(avg_finishing_position, 1) AS avg_finish
FROM lakehouse.mart_driver_standings
WHERE season = ${inputs.season.value}
ORDER BY total_points DESC
```

## Full standings table

<DataTable data={standings} rows=25>
  <Column id=driver_code title="Code" />
  <Column id=driver_name title="Driver" />
  <Column id=team title="Team" />
  <Column id=nationality title="Nationality" />
  <Column id=total_points title="Points" />
  <Column id=sprint_points title="Sprint pts" />
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
