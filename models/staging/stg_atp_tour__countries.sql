{{ config(
    tags=["staging"]
  )
}}

with atp_tour_countries as (
    select *
      from {{ source('atp_tour', 'countries') }}
)
, renamed as (
    select cca3 as iso_country_code
          ,name.common as country_name
          ,demonyms.eng.f as nationality
      from atp_tour_countries
)
, surrogate_keys as (
    select {{ dbt_utils.surrogate_key(['iso_country_code']) }} as country_sk
          ,*
      from renamed
)
select *
  from surrogate_keys