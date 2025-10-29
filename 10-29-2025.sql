--missing flags for plasma event find

select --string_agg(cast(r.id_run_growth as string))
id_run_growth, end_time,char_block_count_240

from `df-data-and-ai.trilliback.bq_growth_runs_about_block`
join `df-data-and-ai.trilliback.bq_growth_runs` r using(id_run_growth)
left join `df-max.raw_dfdb.run_excursion_flagging` flag on 
where id_run_growth in (27328, 27143, 27798, 27745, 28382, 28233, 28338, 28561, 28707, 28838, 28946, 28504, 28592, 28927,27242,28686,27931,28183,27688,27933,27756,27049,27866,27863,28014,28579,28457,28181,27493,28547,27402,27877,28756,27645,27915,27704,27591,27112,27936,28255,28393,27674,27277)
and r.end_time > current_timestamp() - interval 90 day
and char_block_count_240 > 0
and r.loading_purpose = 'Production'

