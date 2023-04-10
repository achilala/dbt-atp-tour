{{ config(
    tags=["util"]
  )
}}

with unknown_record_value as (
	select *
	  from {{ ref('unknown_record_values') }}
)
, casting as (
    select cast(unknown_key as varchar) as unknown_key
          ,cast(unknown_text as varchar) as unknown_text
          ,cast(unknown_integer as int) as unknown_integer
          ,cast(unknown_float as float) as unknown_float
          ,cast(unknown_boolean as boolean) as unknown_boolean
          ,cast(unknown_date as date) as unknown_date
          ,null as unknown_null
      from unknown_record_value
)
select *
  from casting