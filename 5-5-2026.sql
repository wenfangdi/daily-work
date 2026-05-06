with gas_change as (
select id_run, rd.station_name,start_time,id_station,
step_parameter, safe_cast(step_data as float64) as step_data,
lag(safe_cast(step_data as float64)) over(partition by id_station, step_parameter order by id_run) as last_step_data
from `df-data-and-ai.trilliant_warehouse.run_data` rd
where id_process = 671
-- and start_time > '2025-01-01'
),
gas_change_vs_growth as (
select gas_change.id_run, gas_change.station_name as gas_station,gas_change.start_time, gas_change.step_parameter, s.station_name, step_data - last_step_data as change_of_value, step_data,last_step_data,
r.id_run_growth,
r.mean_growth_rate,
sr.source_id_station, sr.target_id_station
from gas_change 
join (select source_id_station, target_id_station from `df-max.raw_dfdb.station_relations` group by 1,2) sr on gas_change.id_station = sr.source_id_station
join `df-max.raw_dfdb.stations` s on sr.target_id_station = s.id_station
join `df-data-and-ai.trilliback.bq_growth_runs` r on r.id_station = s.id_station and gas_change.start_time between r.start_time and r.end_time
where gas_change.station_name = 'DFIWA-PH05-GC01'
and gas_change.start_time > '2025-01-01'
and gas_change.id_run = 4240633
)
select id_run_growth, mean_growth_rate,
sum(change_of_value) as change_of_n2,
string_agg(step_parameter) as changed_parameters,
count(step_parameter) as changed_parameter_count
 from gas_change_vs_growth
group by 1,2
