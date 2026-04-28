---
title: Tournaments
---

```sql tournament_stats
select
  Tournament,
  Surface,
  count(*) as Matches,
  count(distinct Year) as Years_Played,
  count(distinct Winner) as Unique_Winners
from atp_tour.match_summary
where Surface != 'NaN'
group by Tournament, Surface
order by Matches desc
```

```sql dynasty_players
select
  Tournament,
  Winner as Player,
  count(*) as Finals_Won
from atp_tour.match_summary
where Round = 'Final'
group by Tournament, Winner
qualify rank() over (partition by Tournament order by count(*) desc) = 1
order by Finals_Won desc
limit 20
```

```sql most_finals_won
select
  Winner as Player,
  count(*) as Finals_Won,
  count(distinct Tournament) as Tournaments
from atp_tour.match_summary
where Round = 'Final'
group by Winner
order by Finals_Won desc
limit 15
```

```sql finals_by_surface
select
  Surface,
  Winner as Player,
  count(*) as Finals_Won
from atp_tour.match_summary
where Round = 'Final' and Surface != 'NaN'
  and Winner in (
    select Winner from atp_tour.match_summary
    where Round = 'Final'
    group by Winner order by count(*) desc limit 10
  )
group by Surface, Winner
order by Finals_Won desc
```

```sql surface_breakdown
select
  surface as Surface,
  count(*) as Tournaments,
  sum(num_of_matches) as Total_Matches
from atp_tour.tournaments
group by surface
order by Tournaments desc
```

## Most Tournament Titles (Finals Won)

<BarChart data={most_finals_won} x="Player" y="Finals_Won" swapXY=true/>

---

## Finals Won by Surface — Top 10 Players

<BarChart data={finals_by_surface} x="Player" y="Finals_Won" series="Surface" swapXY=true/>

---

## Tournament Dynasties

The most dominant player at each tournament (by finals won).

<DataTable data={dynasty_players} rows=20/>

---

## All Tournaments

<DataTable data={tournament_stats} search=true rows=20/>
