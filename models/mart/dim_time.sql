{{ config(
    tags=["mart", "dimension"]
  )
}}

with recursive num_of_minutes as (
  select 0 as minute_num

    union all

  select minute_num + 1
    from num_of_minutes
   where minute_num < (60 * 24) - 1
)
, ref_unknown_record as (
	select *
	  from {{ ref('ref_unknown_value') }}
)
, time_series as (
  select minute_num
		    ,to_minutes(minute_num) as this_minute
    from num_of_minutes
)
, unknown_record as (
  select strftime(('1900-01-01 ' || this_minute)::timestamp, '%H%M')::int as dim_time_key
        ,this_minute as the_time
        ,minute_num as num_of_minutes
        ,extract('hour' from this_minute) + 1 as the_hour
        ,extract('minute' from this_minute) + 1 as the_minute
        ,extract('hour' from this_minute) as military_hour
        ,extract('minute' from this_minute) as military_minute
        ,case
            when this_minute < '12:00:00' then 'AM'
            else 'PM'
         end::varchar as am_pm
        ,case
            when this_minute >= '00:00:00' and this_minute < '06:00:00' then 'Night'
            when this_minute >= '06:00:00' and this_minute < '12:00:00' then 'Morning'
            when this_minute >= '12:00:00' and this_minute < '18:00:00' then 'Afternoon'
            else 'Evening'
         end:: varchar as time_of_day
        ,case
            when this_minute >= '13:00:00' then this_minute - interval 12 hours
            else this_minute
         end::varchar as notation_12
        ,this_minute:: varchar as notation_24
    from time_series

  union all

  select unknown_key as dim_time_key
        ,null as the_time
        ,unknown_integer as num_of_minutes
        ,unknown_integer as the_hour
        ,unknown_integer as the_minute
        ,unknown_integer as military_hour
        ,unknown_integer as military_minute
        ,unknown_text as am_pm
        ,unknown_text as time_of_day
        ,unknown_text as notation_12
        ,unknown_text as notation_24
    from ref_unknown_record
)
select *
  from unknown_record