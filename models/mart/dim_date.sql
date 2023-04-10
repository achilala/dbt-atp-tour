{{ config(
    tags=["mart", "dimension"]
  )
}}

with date_spine as (
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="cast('1800-01-01' as date)",
      end_date="cast('2023-12-31' as date)"
    )
  }}
)
, ref_unknown_record as (
	select *
	  from {{ ref('ref_unknown_value') }}
)
, calendar as (
  select strftime(date_day, '%Y%m%d') as dim_date_key
        ,date_day
        ,strftime(date_day, '%d/%m/%Y') as date_ddmmyyy
        ,strftime(date_day, '%A') as day_of_week
        ,strftime(date_day, '%w') as day_of_week_number
        ,strftime(date_day, '%a') as day_of_week_abbr
        ,strftime(date_day, '%d') as day_of_month
        ,strftime(date_day, '%j') as day_of_year
        ,strftime(date_day, '%W') as week_of_year
        ,strftime(date_day, '%m') as month_of_year
        ,strftime(date_day, '%B') as month_name
        ,strftime(date_day, '%b') as month_name_abbr
        ,strftime(date_day, '%Y-%m-01') as first_day_of_month
        ,strftime(date_day, '%Y') as year
    from date_spine
)
, unknown_record as (
    select dim_date_key
          ,date_day
          ,date_ddmmyyy
          ,day_of_week_number
          ,day_of_week
          ,day_of_week_abbr
          ,day_of_month
          ,day_of_year
          ,week_of_year
          ,month_of_year
          ,month_name
          ,month_name_abbr
          ,first_day_of_month
          ,year
      from calendar

    union all
    
    select unknown_key
          ,unknown_null
          ,unknown_text
          ,unknown_integer
          ,unknown_text
          ,unknown_text
          ,unknown_text
          ,unknown_text
          ,unknown_text
          ,unknown_text
          ,unknown_text
          ,unknown_text
          ,unknown_date
          ,unknown_integer
      from ref_unknown_record
)
select *
  from unknown_record