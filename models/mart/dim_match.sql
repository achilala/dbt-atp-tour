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
, unknown_record as (
	select match_sk as dim_match_key
		  ,match_id
		  ,tourney_id
		  ,score
		  ,best_of
		  ,round
		  ,minutes
		  ,winner_id
		  ,winner_seed
		  ,winner_entry
		  ,winner_name
		  ,winner_hand
		  ,winner_ht
		  ,winner_ioc
		  ,winner_age
		  ,winner_ace
		  ,winner_df
		  ,winner_svpt
		  ,winner_1stin
		  ,winner_1stwon
		  ,winner_2ndwon
		  ,winner_svgms
		  ,winner_bpsaved
		  ,winner_bpfaced
		  ,winner_rank
		  ,winner_rank_points
		  ,loser_id
		  ,loser_seed
		  ,loser_entry
		  ,loser_name
		  ,loser_hand
		  ,loser_ht
		  ,loser_ioc
		  ,loser_age
		  ,loser_ace
		  ,loser_df
		  ,loser_svpt
		  ,loser_1stin
		  ,loser_1stwon
		  ,loser_2ndwon
		  ,loser_svgms
		  ,loser_bpsaved
		  ,loser_bpfaced
		  ,loser_rank
		  ,loser_rank_points
	  from matches

    union all
    
    select unknown_key as dim_match_key
          ,unknown_integer as match_id
		  ,unknown_text as tourney_id
		  ,unknown_text as score
		  ,unknown_integer as best_of
		  ,unknown_text as round
		  ,unknown_text as minutes
		  ,unknown_integer as winner_id
		  ,unknown_text as winner_seed
		  ,unknown_text as winner_entry
		  ,unknown_text as winner_name
		  ,unknown_text as winner_hand
		  ,unknown_integer as winner_ht
		  ,unknown_text as winner_ioc
		  ,unknown_float as winner_age
		  ,unknown_text as winner_ace
		  ,unknown_text as winner_df
		  ,unknown_text as winner_svpt
		  ,unknown_text as winner_1stin
		  ,unknown_text as winner_1stwon
		  ,unknown_text as winner_2ndwon
		  ,unknown_text as winner_svgms
		  ,unknown_text as winner_bpsaved
		  ,unknown_text as winner_bpfaced
		  ,unknown_text as winner_rank
		  ,unknown_text as winner_rank_points
		  ,unknown_integer as loser_id
		  ,unknown_text as loser_seed
		  ,unknown_text as loser_entry
		  ,unknown_text as loser_name
		  ,unknown_text as loser_hand
		  ,unknown_integer as loser_ht
		  ,unknown_text as loser_ioc
		  ,unknown_float as loser_age
		  ,unknown_text as loser_ace
		  ,unknown_text as loser_df
		  ,unknown_text as loser_svpt
		  ,unknown_text as loser_1stin
		  ,unknown_text as loser_1stwon
		  ,unknown_text as loser_2ndwon
		  ,unknown_text as loser_svgms
		  ,unknown_text as loser_bpsaved
		  ,unknown_text as loser_bpfaced
		  ,unknown_text as loser_rank
		  ,unknown_text as loser_rank_points
      from ref_unknown_record
)
select *
  from unknown_record