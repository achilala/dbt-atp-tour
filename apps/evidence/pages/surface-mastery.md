---
title: Surface Mastery
---

```sql clay_leaders
select Winner as Player, count(*) as Wins
from atp_tour.match_summary
where Surface = 'Clay'
group by Winner
order by Wins desc
limit 15
```

```sql grass_leaders
select Winner as Player, count(*) as Wins
from atp_tour.match_summary
where Surface = 'Grass'
group by Winner
order by Wins desc
limit 15
```

```sql hard_leaders
select Winner as Player, count(*) as Wins
from atp_tour.match_summary
where Surface = 'Hard'
group by Winner
order by Wins desc
limit 15
```

```sql carpet_leaders
select Winner as Player, count(*) as Wins
from atp_tour.match_summary
where Surface = 'Carpet'
group by Winner
order by Wins desc
limit 15
```

```sql surface_heatmap
select
  Winner as Player,
  Surface,
  count(*) as Wins
from atp_tour.match_summary
where Surface != 'NaN'
  and Winner in (
    select Winner from atp_tour.match_summary
    group by Winner order by count(*) desc limit 20
  )
group by Winner, Surface
order by Player, Surface
```

```sql surface_pct
select
  Winner as Player,
  Surface,
  count(*) as Wins,
  round(100.0 * count(*) / sum(count(*)) over (partition by Winner), 1) as Pct
from atp_tour.match_summary
where Surface != 'NaN'
  and Winner in (
    select Winner from atp_tour.match_summary
    group by Winner order by count(*) desc limit 20
  )
group by Winner, Surface
order by Player, Surface
```

## Clay Court Kings

<BarChart data={clay_leaders} x="Player" y="Wins" swapXY=true color="#c2410c"/>

## Grass Court Masters

<BarChart data={grass_leaders} x="Player" y="Wins" swapXY=true color="#16a34a"/>

## Hard Court Dominators

<BarChart data={hard_leaders} x="Player" y="Wins" swapXY=true color="#2563eb"/>

## Indoor Carpet Specialists

<BarChart data={carpet_leaders} x="Player" y="Wins" swapXY=true color="#7c3aed"/>

---

## Win Distribution by Surface — Top 20 Players

How each all-time great splits their wins across surfaces.

<Heatmap
  data={surface_heatmap}
  x="Surface"
  y="Player"
  value="Wins"
  title="Wins by Surface"
/>

## Surface Win Share (%)

<BarChart
  data={surface_pct}
  x="Player"
  y="Pct"
  series="Surface"
  type="stacked100"
  swapXY=true
/>
