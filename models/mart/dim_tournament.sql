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
	  from {{ ref('ref_unknown_values') }}
)
, tourney as (
	select tournament_sk as dim_tournament_key
		  ,tournament_id
		  ,tournament_name
		  ,tournament_level
		  ,tournament_date
		  ,surface
		  ,draw_size
		  ,count(1) as num_of_matches
	  from matches
	 group by all
)
, unknown_record as (
	select dim_tournament_key
		  ,tournament_id
		  ,tournament_name
		  ,tournament_level
		  ,tournament_date
		  ,surface
		  ,draw_size
		  ,num_of_matches
	  from tourney

    union all
    
	select unknown_key as dim_tournament_key
		  ,unknown_text as tournament_id
		  ,unknown_text as tournament_name
		  ,unknown_text as tournament_level
		  ,unknown_date as tournament_date
		  ,unknown_text as surface
		  ,unknown_integer as draw_size
		  ,unknown_integer as num_of_matches
	  from ref_unknown_record
)
select *
  from unknown_record