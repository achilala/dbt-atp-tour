---
title: Head-to-Head Rivalries
---

```sql player_list
select Winner as player_name, count(*) as wins
from atp_tour.match_summary
group by Winner
order by wins desc
limit 200
```

<Dropdown
  name="player1"
  data={player_list}
  value="player_name"
  label="player_name"
  defaultValue="Roger Federer"
  title="Player 1"
/>

<Dropdown
  name="player2"
  data={player_list}
  value="player_name"
  label="player_name"
  defaultValue="Rafael Nadal"
  title="Player 2"
/>

```sql h2h_record
select
  Winner,
  count(*) as Wins
from atp_tour.match_summary
where
  (Winner = '${inputs.player1.value}' and Loser = '${inputs.player2.value}')
  or
  (Winner = '${inputs.player2.value}' and Loser = '${inputs.player1.value}')
group by Winner
order by Wins desc
```

```sql h2h_p1_wins
select count(*) as wins
from atp_tour.match_summary
where Winner = '${inputs.player1.value}' and Loser = '${inputs.player2.value}'
```

```sql h2h_p2_wins
select count(*) as wins
from atp_tour.match_summary
where Winner = '${inputs.player2.value}' and Loser = '${inputs.player1.value}'
```

```sql h2h_by_surface
select
  Surface,
  Winner,
  count(*) as Wins
from atp_tour.match_summary
where Surface != 'NaN'
  and (
    (Winner = '${inputs.player1.value}' and Loser = '${inputs.player2.value}')
    or
    (Winner = '${inputs.player2.value}' and Loser = '${inputs.player1.value}')
  )
group by Surface, Winner
order by Surface
```

```sql h2h_by_round
select
  Round,
  Winner,
  count(*) as Wins
from atp_tour.match_summary
where
  (Winner = '${inputs.player1.value}' and Loser = '${inputs.player2.value}')
  or
  (Winner = '${inputs.player2.value}' and Loser = '${inputs.player1.value}')
group by Round, Winner
order by Wins desc
```

```sql h2h_by_year
select
  Year,
  Winner,
  count(*) as Wins
from atp_tour.match_summary
where
  (Winner = '${inputs.player1.value}' and Loser = '${inputs.player2.value}')
  or
  (Winner = '${inputs.player2.value}' and Loser = '${inputs.player1.value}')
group by Year, Winner
order by Year
```

```sql h2h_matches
select
  Date,
  Tournament,
  Surface,
  Round,
  Winner,
  Score
from atp_tour.match_summary
where
  (Winner = '${inputs.player1.value}' and Loser = '${inputs.player2.value}')
  or
  (Winner = '${inputs.player2.value}' and Loser = '${inputs.player1.value}')
order by Date desc
```

## {inputs.player1.label} vs {inputs.player2.label}

<BigValue data={h2h_p1_wins} value="wins" title="{inputs.player1.label} Wins"/>
<BigValue data={h2h_p2_wins} value="wins" title="{inputs.player2.label} Wins"/>

---

## By Surface

<BarChart data={h2h_by_surface} x="Surface" y="Wins" series="Winner"/>

## By Round

<BarChart data={h2h_by_round} x="Round" y="Wins" series="Winner" swapXY=true/>

## Over Time

<BarChart data={h2h_by_year} x="Year" y="Wins" series="Winner" type="stacked"/>

## Match History

<DataTable data={h2h_matches} rows=20/>
