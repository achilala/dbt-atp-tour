---
title: ATP Tour Analytics
---

```sql totals
select
  count(*)                    as total_matches,
  count(distinct Winner)      as total_players,
  count(distinct Tournament)  as total_tournaments,
  min(Year)                   as first_year,
  max(Year)                   as last_year
from atp_tour.match_summary
where Surface != 'NaN'
```

```sql matches_by_year
select Year, count(*) as Matches
from atp_tour.match_summary
where Surface != 'NaN'
group by Year
order by Year
```

```sql top_10_winners
select Winner as Player, count(*) as Wins
from atp_tour.match_summary
group by Winner
order by Wins desc
limit 10
```

```sql matches_by_surface
select Surface, count(*) as Matches
from atp_tour.match_summary
where Surface != 'NaN'
group by Surface
order by Matches desc
```

```sql surface_by_year
select Year, Surface, count(*) as Matches
from atp_tour.match_summary
where Surface != 'NaN'
group by Year, Surface
order by Year
```

<BigValue data={totals} value="total_matches"      title="Total Matches"      fmt="num0"/>
<BigValue data={totals} value="total_players"      title="Players"            fmt="num0"/>
<BigValue data={totals} value="total_tournaments"  title="Tournaments"        fmt="num0"/>
<BigValue data={totals} value="first_year"         title="Open Era Since"/>
<BigValue data={totals} value="last_year"          title="Through"/>

---

## Matches Over Time

<LineChart data={matches_by_year} x="Year" y="Matches"/>

## Surface Mix Over Time

<AreaChart data={surface_by_year} x="Year" y="Matches" series="Surface" fillOpacity=0.5/>

## Top 10 All-Time Winners

<BarChart data={top_10_winners} x="Player" y="Wins" swapXY=true/>

## Matches by Surface

<BarChart data={matches_by_surface} x="Surface" y="Matches"/>

---

Explore the full analysis:
- [Surface Mastery](/surface-mastery) — who dominates on clay, grass, and hard courts
- [Rivalries](/rivalries) — head-to-head records between any two players
- [Eras](/eras) — how the game has changed across decades
- [Players](/players) — career stats for every player
- [Tournaments](/tournaments) — tournament history and dynasties
