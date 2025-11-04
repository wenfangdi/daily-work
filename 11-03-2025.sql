with growth_with_striation as (
SELECT tri.id_run_growth,tri.end_time, (UNIX_SECONDS(timestamp)-start_time_unix)/3600 as hour_since_start, (end_time_unix -UNIX_SECONDS(timestamp))/3600 hour_til_end,cast(TIMESTAMP_TRUNC(timestamp,Week) as timestamp) as time_trunc,tri.loading_purpose,
CASE WHEN range_incident_power > 0.8 THEN 'incident_power_event'
                WHEN range_chamber_pressure > 1 THEN 'chamber_pressure_event'
                WHEN range_reflected_power > 6800 THEN 'reflected_power_event'
                ELSE 'vsp_counter_event' end as event_type
                 from `df-data-and-ai.df_warehouse.growth_events` ge
join  ( 
select id_run_growth,station_name, UNIX_SECONDS(start_time) as start_time_unix,UNIX_SECONDS(end_time) as end_time_unix, growing_location_name,end_time,
run_duration, loading_purpose from `df-data-and-ai.trilliback.bq_growth_runs`
) tri on tri.id_run_growth = safe_cast(ge.id_run_growth as int64)
where 1=1 -- timestamp BETWEEN '2025-08-05T22:58:01.172Z' AND '2025-11-03T23:58:01.172Z'
and 
((range_incident_power > 0.8 and range_reflected_power > 6800)
   or (range_chamber_pressure > 1))
and UNIX_SECONDS(timestamp) - start_time_unix > 24 *3600
and end_time_unix - UNIX_SECONDS(timestamp) > 0.5 *3600
and range_ppb_n2 < 50 and range_curtain_h2 < 100 and range_process_ch4 < 10 and range_process_h2 < 50 and range_mw_setpoint < 4 and range_pressure_setpoint < 4
order by timestamp asc
),
raw as 
(
select vbit.id_block, tray_name, id_tray, vbit.timestamp time_into_tray, gwt.end_time,
case when gwt.hour_til_end is not null then 'Striated'
else 'Good'
end as theoretical_label,
gwt.hour_til_end,
bd.post_hep_bake_weight,
b.final_clean_vol_cm3,
loading_purpose,
bd.custom_workflow
 from `df-max.raw_dfdb.vw_blocks_in_trays` vbit
 join `df-max.raw_dfdb.block_dimensions` bd using(id_block)
 join `df-data-and-ai.trilliback.bq_growth_block_data` b on bd.latest_id_run_growth = b.id_run_growth and bd.id_block = b.id_block
 left join (select id_run_growth,end_time,max(loading_purpose) as loading_purpose, max(hour_til_end) as hour_til_end from growth_with_striation group by 1,2) gwt on gwt.id_run_growth = bd.latest_id_run_growth
where id_tray in (63462, 63483, 63484, 63535, 63540, 63541, 63565, 63592, 63638, 63639, 63640, 63692, 63735, 63736, 63741, 63744, 63745, 63779, 63807, 63872, 63886, 63890, 63918, 63919, 63892, 63920, 63921, 63922, 63923, 63924, 63925, 63934, 63935, 63936, 63943, 63944, 63945, 63976, 63979, 63992, 64004, 64022, 64023, 64024, 64029, 64030, 64031, 64032, 64039, 64042, 64045, 64055, 64059, 64063, 64064, 64066, 64104, 64217, 64218, 64220, 64057, 64058, 64231, 64229, 64230, 64236, 64237, 64238, 64262, 64256, 64258, 64262, 64274, 64286, 64292, 64375, 64380, 64417, 64373, 64378, 64449, 64451, 64450, 64452, 62430, 62434, 62436, 62443, 62478, 62554, 62493, 62494, 62495, 62555, 62556, 62557, 62558, 62577, 62578, 62579, 62608, 62609, 62610, 62632, 62634, 62649, 62650, 62652, 62660, 62667, 62668, 62671, 62672, 62721, 62724, 62725, 62727, 62767, 62770, 62774, 62777, 62806, 62843, 62898, 62900, 62906, 62911, 63021, 63023, 63024, 63025, 63032, 63033, 63039, 63040, 63041, 63042, 63044, 63030, 63038, 63110, 63130, 63142, 63144, 63145, 63176, 63193, 63243, 63245, 63246, 63239, 63261, 63262, 63263, 63264, 63278, 63279, 63300, 63301, 63382, 63388, 63400, 63401, 63402, 63403, 63404, 63405, 63406, 63407, 63408, 63432, 63434, 63435, 63436, 63437, 63438, 63439, 63440, 63442, 63446, 63447, 63449, 61484, 61420, 61501, 61503, 61521, 61542, 61546, 61549, 61118, 61556, 61558, 61560, 61561, 61382, 61604, 61605, 61559, 61641, 61647, 61669, 61674, 61695, 61720, 61851, 61852, 61860, 61861, 61884, 61892, 61893, 61900, 61682, 61683, 61684, 61685, 61914, 61903, 61909, 61910, 61911, 61924, 61925, 61930, 61931, 61932, 61936, 61941, 61951, 61983, 61999, 62000, 62001, 62016, 62029, 62030, 62031, 62060, 62068, 62072, 62095, 62096, 62102, 62141, 62149, 62161, 61939, 61940, 61942, 61943, 61944, 62194, 62197, 62199, 62284, 62288, 62293, 62294, 62345, 62347, 62348, 62379, 62381, 62383, 62387, 62417, 62418, 62419, 62426, 62427, 60949, 60950, 60636, 60977, 60978, 60979, 60992, 60993, 60951, 60996, 60997, 61028, 61032, 61034, 61049, 61051, 61053, 61054, 61072, 61100, 61104, 61108, 61121, 61122, 61129, 61138, 61137, 61130, 61173, 61174, 61176, 61179, 61189, 61216, 61219, 61221, 61230, 61235, 61220, 61249, 61248, 61259, 61260, 61266, 61337, 61355, 61357, 61372, 61376, 61377, 61379, 61407, 61478, 61430, 61431, 61464, 61469, 61485, 60117, 60121, 60122, 59959, 60125, 61048, 60150, 60173, 60174, 60175, 60184, 60220, 60221, 60225, 60229, 60230, 60246, 60247, 60275, 60277, 60278, 60447, 60448, 60500, 60517, 60518, 60519, 60522, 60524, 60569, 60585, 60586, 60607, 60611, 60646, 60647, 60648, 60649, 60652, 60653, 60655, 60656, 60657, 60723, 60507, 60508, 60739, 60762, 60764, 60765, 60759, 60766, 60757, 60756, 60767, 60758, 60768, 60774, 60775, 60776, 60777, 60778, 60779, 60794, 60795, 60796, 60797, 60850, 60851, 60852, 60853, 60854, 60857, 60858, 60859, 60931, 60932, 59141, 59142, 58974, 59159, 59165, 59168, 59171, 59172, 59175, 59176, 59181, 59001, 59190, 59191, 59197, 59204, 59214, 59234, 59235, 59189, 59247, 59545, 59283, 59608, 59638, 59640, 59641, 59729, 59730, 59731, 59733, 59734, 59736, 59737, 59821, 59824, 59866, 59872, 59873, 59874, 59875, 59877, 59878, 59889, 59890, 59892, 59893, 59894, 59927, 59922, 59926, 59921, 59940, 59941, 59953, 59954, 59955, 59958, 59961, 59968, 59971, 59973, 59978, 60010, 60014, 60012, 60015, 60032, 60033, 60035, 60036, 60077, 60082, 60091, 60092, 60095, 60096, 60099, 60104, 60115)
and vbit.tray_name not like '%PS%'
)

select theoretical_label,
case when end_time < '2024-09-30' then 'Before ECN'
else NULL end
as time_category,
case when hour_til_end < 4 then 'close to end'
else NULL end as time_of_occurance,
case when final_clean_vol_cm3 is null then 'missed post hep step'
else NULL end as post_hep_check,
case when loading_purpose != 'Production' then 'Not Production' else null end as purpose,
case when custom_workflow is not null then 'Flagged but missed' else null end as mistake,
 count(*) from raw
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6
