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
, matches_2 as (
	select match_sk as dim_match_key
		  ,match_id
		  ,score
		  ,best_of
		  ,best_of_labeled
		  ,round
		  ,winner_entry
		  ,loser_entry
	  from matches
	 group by all
)
, unknown_record as (
	select unknown_key::varchar as dim_match_key
		  ,unknown_text::varchar as match_id
		  ,unknown_text::varchar as score
		  ,unknown_text::varchar as best_of
		  ,unknown_text::varchar as best_of_labeled
		  ,unknown_text::varchar as round
		  ,unknown_text::varchar as winner_entry
		  ,unknown_text::varchar as loser_entry
	  from ref_unknown_record

    union all
    
	select dim_match_key
		  ,match_id
		  ,score
		  ,best_of
		  ,best_of_labeled
		  ,round
		  ,winner_entry
		  ,loser_entry
	  from matches_2
)
select *
  from unknown_record