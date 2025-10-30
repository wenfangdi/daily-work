--missing flags for plasma event find


select --string_agg(cast(r.id_run_growth as string))
id_run_growth, end_time,char_block_count_240, flag.excursion_types, run_duration, 
(UNIX_SECONDS(ge.timestamp)-UNIX_SECONDS(r.start_time))/3600 as hour_since_start

from `df-data-and-ai.trilliback.bq_growth_runs_about_block`
join `df-data-and-ai.trilliback.bq_growth_runs` r using(id_run_growth)
left JOIN `df-max.raw_dfdb.run_excursion_flagging` flag using(id_run_growth)
LEFT JOIN `df-data-and-ai.df_warehouse.growth_events` ge using(id_run_growth)
where id_run_growth in (28973, 28952, 28937, 28909, 28858, 28842, 28803, 28732, 28703, 28695, 28680, 28666, 28628, 28579, 28547, 28323, 28255, 28183, 28014, 27936, 27877, 27863, 27756, 27704, 27688, 27645, 27493, 27402, 27242, 27112)
and r.end_time > current_timestamp() - interval 90 day

and r.loading_purpose = 'Production'
and excursion_types = 'No Excursions'
and id_run_growth not in (28927, 28592,28504, 28579 ) -- checked and fixed
and char_block_count_240 > 0 
-- and r.run_duration < 680
-- and id_run_growth = 28686
order by end_time desc, hour_since_start desc
