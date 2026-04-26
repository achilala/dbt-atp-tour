{{ config(tags=["staging"]) }}

with source as (
    select *
      from {{ source('atp_tour', 'countries') }}
)
, renamed as (
    select cca3::varchar(3)          as country_iso_code
          ,name.common::varchar(100) as country_name
          ,demonyms.eng.f::varchar(100) as nationality
          ,region::varchar(100)      as continent
          ,subregion::varchar(100)   as region
          ,flag::varchar(10)         as flag
          ,population::int           as population
          ,coalesce(json_array_length(borders), 0)::int as num_of_borders
      from source
)
select {{ dbt_utils.generate_surrogate_key(['country_iso_code']) }} as country_sk
      ,*
  from renamed
