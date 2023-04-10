{{ config(
    tags=["mart", "dimension"]
  )
}}

with matches as (
	select *
	  from {{ ref('stg_atp_tour__matches') }}
)
, ref_unknown_record as (
	select *
	  from {{ ref('ref_unknown_value') }}
)
, tourney as (
	select tourney_sk as dim_tournament_key
		  ,tourney_id
		  ,tourney_name
		  ,tourney_level
		  ,tourney_date
		  ,surface
		  ,draw_size
		  ,count(1) as num_of_matches
	  from matches
	 group by tourney_sk
		  ,tourney_id
		  ,tourney_name
		  ,tourney_level
		  ,tourney_date
		  ,surface
		  ,draw_size
)
, unknown_record as (
	select dim_tournament_key
		  ,tourney_id
		  ,tourney_name
		  ,tourney_level
		  ,tourney_date
		  ,surface
		  ,draw_size
		  ,num_of_matches
	  from tourney

    union all
    
	select unknown_key as dim_tournament_key
		  ,unknown_text as tourney_id
		  ,unknown_text as tourney_name
		  ,unknown_text as tourney_level
		  ,unknown_date as tourney_date
		  ,unknown_text as surface
		  ,unknown_integer as draw_size
		  ,unknown_integer as num_of_matches
	  from ref_unknown_record
)
select *
  from unknown_record