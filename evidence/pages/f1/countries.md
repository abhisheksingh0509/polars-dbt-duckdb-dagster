---
title: Geography
---

# Where F1 Raced in 2024

```sql countries
SELECT
  country,
  race_count,
  circuits,
  first_race_date,
  latest_race_date,
  centroid_lat AS latitude,
  centroid_long AS longitude
FROM lakehouse.mart_country_race_summary
ORDER BY race_count DESC, country
```

## Calendar by country

<DataTable data={countries} rows=25>
  <Column id=country title="Country" />
  <Column id=race_count title="Races" />
  <Column id=circuits title="Circuits" wrap=true />
  <Column id=first_race_date title="First race" />
  <Column id=latest_race_date title="Last race" />
</DataTable>

## Races per country

<BarChart
  data={countries}
  x=country
  y=race_count
  title="Race count by host country"
/>

## Map of circuits

Each point is the centroid of all circuits in a country (helpful for countries with multiple GPs).

<PointMap
  data={countries}
  lat=latitude
  long=longitude
  size=race_count
  pointName=country
  title="F1 host countries"
/>
