---
title: Players
---

```sql top_players
select
  player_name                as Player,
  nationality                as Nationality,
  continent                  as Continent,
  dominant_hand              as Hand,
  height                     as Height,
  age                        as Age,
  num_of_wins                as Wins,
  num_of_losses              as Losses,
  career_wins_vs_losses      as "W/L",
  career_win_ratio           as "Win %"
from atp_tour.players
where num_of_wins >= 50
order by Wins desc
```

```sql wins_by_continent
select continent as Continent, count(*) as Players, sum(num_of_wins) as Wins
from atp_tour.players
where num_of_wins >= 10 and continent is not null
group by Continent
order by Wins desc
```

```sql hand_breakdown
select
  dominant_hand as Hand,
  count(*) as Players,
  sum(num_of_wins) as Wins,
  round(avg(cast(replace(career_win_ratio, '%', '') as float)), 1) as Avg_Win_Pct
from atp_tour.players
where num_of_wins >= 50 and dominant_hand in ('Right', 'Left')
group by Hand
order by Wins desc
```

```sql height_vs_wins
select
  player_name as Player,
  height_in_centimeters as Height_cm,
  num_of_wins as Wins,
  dominant_hand as Hand,
  nationality as Nationality
from atp_tour.players
where num_of_wins >= 100 and height_in_centimeters > 0
order by Wins desc
```

## Career Win Leaders (50+ wins)

<DataTable data={top_players} search=true rows=20/>

---

## Wins by Continent

<BarChart data={wins_by_continent} x="Continent" y="Wins"/>

---

## Right-Handed vs Left-Handed

<DataTable data={hand_breakdown}/>

---

## Height vs Career Wins (100+ win players)

<ScatterPlot
  data={height_vs_wins}
  x="Height_cm"
  y="Wins"
  series="Hand"
  tooltipTitle="Player"
  xAxisTitle="Height (cm)"
  yAxisTitle="Career Wins"
/>
