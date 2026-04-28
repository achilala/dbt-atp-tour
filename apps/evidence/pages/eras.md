---
title: Eras of Tennis
---

```sql era_totals
select
  case
    when cast(Year as integer) < 1980 then '1968–1979'
    when cast(Year as integer) < 1990 then '1980–1989'
    when cast(Year as integer) < 2000 then '1990–1999'
    when cast(Year as integer) < 2010 then '2000–2009'
    when cast(Year as integer) < 2020 then '2010–2019'
    else '2020–2024'
  end as Era,
  count(*) as Matches,
  count(distinct Winner) as Players,
  count(distinct Tournament) as Tournaments
from atp_tour.match_summary
where Surface != 'NaN' and cast(Year as integer) >= 1968
group by Era
order by Era
```

```sql era_top_players
select
  case
    when cast(Year as integer) < 1980 then '1968–1979'
    when cast(Year as integer) < 1990 then '1980–1989'
    when cast(Year as integer) < 2000 then '1990–1999'
    when cast(Year as integer) < 2010 then '2000–2009'
    when cast(Year as integer) < 2020 then '2010–2019'
    else '2020–2024'
  end as Era,
  Winner as Player,
  count(*) as Wins
from atp_tour.match_summary
where cast(Year as integer) >= 1968
group by Era, Winner
qualify rank() over (partition by Era order by count(*) desc) <= 8
order by Era, Wins desc
```

```sql surface_by_era
select
  case
    when cast(Year as integer) < 1980 then '1968–1979'
    when cast(Year as integer) < 1990 then '1980–1989'
    when cast(Year as integer) < 2000 then '1990–1999'
    when cast(Year as integer) < 2010 then '2000–2009'
    when cast(Year as integer) < 2020 then '2010–2019'
    else '2020–2024'
  end as Era,
  Surface,
  count(*) as Matches,
  round(100.0 * count(*) / sum(count(*)) over (partition by
    case
      when cast(Year as integer) < 1980 then '1968–1979'
      when cast(Year as integer) < 1990 then '1980–1989'
      when cast(Year as integer) < 2000 then '1990–1999'
      when cast(Year as integer) < 2010 then '2000–2009'
      when cast(Year as integer) < 2020 then '2010–2019'
      else '2020–2024'
    end
  ), 1) as Pct
from atp_tour.match_summary
where Surface != 'NaN' and cast(Year as integer) >= 1968
group by Era, Surface
order by Era, Matches desc
```

```sql aces_by_era
select
  case
    when cast(Year as integer) < 1980 then '1968–1979'
    when cast(Year as integer) < 1990 then '1980–1989'
    when cast(Year as integer) < 2000 then '1990–1999'
    when cast(Year as integer) < 2010 then '2000–2009'
    when cast(Year as integer) < 2020 then '2010–2019'
    else '2020–2024'
  end as Era,
  round(avg(Aces), 2) as Avg_Aces_Per_Match
from atp_tour.match_summary
where cast(Year as integer) >= 1968 and Aces is not null
group by Era
order by Era
```

## Matches per Era

<BarChart data={era_totals} x="Era" y="Matches"/>

<DataTable data={era_totals}/>

---

## Top 8 Winners by Era

<BarChart data={era_top_players} x="Player" y="Wins" series="Era" swapXY=true/>

---

## Surface Mix by Era

How the game's surfaces have shifted — carpet disappeared, hard courts took over.

<BarChart
  data={surface_by_era}
  x="Era"
  y="Pct"
  series="Surface"
  type="stacked100"
  yAxisTitle="% of Matches"
/>

---

## Average Aces Per Match by Era

Serve aggression over time.

<BarChart data={aces_by_era} x="Era" y="Avg_Aces_Per_Match"/>
