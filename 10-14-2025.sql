
-- Igor remake block step tracker
WITH seq AS (
  SELECT
    id_block,
    end_time                           AS this_end,
    LEAD(start_time) OVER (
      PARTITION BY id_block
      ORDER BY end_time    
    )                                   AS next_start
  FROM `df-mes.mes_warehouse.block_step_tracker251014`
  where end_time != start_time
  -- where end_time > current_timestamp() - interval 90 day
)
SELECT *, timestamp_diff(next_start, this_end, HOUR) AS time_diff
FROM seq
WHERE next_start IS DISTINCT FROM this_end       -- NULL-safe inequality
and this_end is not null 
and next_start is not null
-- and timestamp_diff(next_start, this_end, HOUR) > 24
ORDER BY id_block, this_end;

--Josiah End time check
BEGIN


FOR row IN ( -- For each run that ended in the last 24 hours, aggregate reactor data points collected once a sec 

  SELECT
    r.id_run_growth,
    r.run_duration,
    r.start_time,
    r.end_time,
    s.data_table,
    r.id_growing_run
  FROM `df-data-and-ai.trilliback.bq_growth_runs` r
  join `df-max.raw_dfdb.stations` s on r.id_station = s.id_station
  WHERE end_time BETWEEN CURRENT_TIMESTAMP() - INTERVAL 24*30 HOUR AND CURRENT_TIMESTAMP()
    AND run_duration > 0
    AND id_run_growth not in (select id_run_growth from `df-data-and-ai.temp.temp_growth_end_time_check`)

)
 DO  -- loop ends at line 417

Insert into `df-data-and-ai.temp.temp_growth_end_time_check`
select row.id_run_growth, row.id_growing_run, row.end_time, max(timestamp) as id_run_end_time

            FROM `dfdb-178217.mfgdata.*` 
            WHERE _table_suffix = row.data_table
              AND timestamp >= row.end_time - interval 2 hour
              AND timestamp < row.end_time + interval 1 hour
              AND id_run = row.id_growing_run
              AND pyrometer.id_value != ''
              AND pyrometer.id_run_growth = CAST(row.id_run_growth AS STRING)
            GROUP BY row.id_run_growth, row.id_growing_run;

END FOR;

-- HWC Andrea found bug

with 
rdh as (
  select * EXCEPT(step_parameter), 
  case when step_parameter in ('Hardware Platform','HardwarePlatform' 'Generation') then 'HardwarePlatform'
when step_parameter like 'Hardware%Platform%' then 'HardwarePlatform'
when step_parameter in ('Microwave','Cabinet Type') then 'Microwave'
when step_parameter in ('Pyro Motor Configuration', 'Pyro motor configuration') then 'Pyro Motor Configuration'
when step_parameter in ('Water Flow Meter', 'Water Flow Sensor') then 'Water Flow Meter'
when step_parameter in ('PLC Configuration','PLC Version') then 'PLC Configuration'
when step_parameter in ('Centering Collar','Coax Collar') then 'Centering Collar'
when step_parameter in ('Camera','Camera Configurations') then 'Camera'
else step_parameter end as step_parameter 
from `df-data-and-ai.hardware_config.hwc_run_data`
),
last_run_per_growth as (select r.id_growing_run, r.id_run_growth,r.station_name,rdh.step_parameter,
                max(rdh.id_run) as last_equipment_run
FROM `df-data-and-ai.trilliback.bq_growth_runs` r
LEFT JOIN rdh ON rdh.id_station=r.id_station AND rdh.id_run<r.id_growing_run and rdh.start_time < r.start_time
-- where r.id_run_growth = 28247
group by 1,2,3,step_name, rdh.step_parameter
)

select 
id_run_growth, id_growing_run, last_run_per_growth.station_name,
MAX(IF(rdh.step_parameter = 'Nozzle', step_data, NULL)) as nozzle,
MAX(IF(rdh.step_parameter = 'Hardlines', step_data, NULL)) as hardlines,
MAX(IF(rdh.step_parameter in ('Microwave'), step_data, NULL)) as microwave,
MAX(IF(rdh.step_parameter = 'Regulator', step_data, NULL)) as regulator,
MAX(IF(rdh.step_parameter in ('HardwarePlatform'), step_data, NULL)) as hardware_platform,
MAX(IF(rdh.step_parameter = 'End Cap', step_data, NULL)) as end_cap,
MAX(IF(rdh.step_parameter in ('Pyro Motor Configuration'), step_data, NULL)) as pyro_motor_configuration,
MAX(IF(rdh.step_parameter = 'Chamber', step_data, NULL)) as chamber,
MAX(IF(rdh.step_parameter = 'Door Knob', step_data, NULL)) as door_knob,
MAX(IF(rdh.step_parameter = 'Turbo', step_data, NULL)) as turbo,
MAX(IF(rdh.step_parameter = 'Clamp Ring', step_data, NULL)) as clamp_ring,
MAX(IF(rdh.step_parameter in ('Water Flow Meter'), step_data, NULL)) as water_flow_meter,
MAX(IF(rdh.step_parameter = 'Inner Coax Coating', step_data, NULL)) as inner_coax_coating,
MAX(IF(rdh.step_parameter = 'Stage coating', step_data, NULL)) as stage_coating,
MAX(IF(rdh.step_parameter = 'Magnetron', step_data, NULL)) as magnetron,
MAX(IF(rdh.step_parameter = 'Backing', step_data, NULL)) as backing,
MAX(IF(rdh.step_parameter = 'Water Supply & Return Bulkhead Fittings', step_data, NULL)) as water_supply_return_bulkhead_fittings,
MAX(IF(rdh.step_parameter in ('PLC Configuration'), step_data, NULL)) as plc_configuration,
MAX(IF(rdh.step_parameter = 'Cooling Plate', step_data, NULL)) as cooling_plate,
MAX(IF(rdh.step_parameter = 'Miter Bend', step_data, NULL)) as miter_bend,
MAX(IF(rdh.step_parameter = 'GFCIs', step_data, NULL)) as gfci,
MAX(IF(rdh.step_parameter = 'Exhaust DP Sensor', step_data, NULL)) as exhaust_dp_sensor,
MAX(IF(rdh.step_parameter in ('Centering Collar'), step_data, NULL)) as centering_collar,
MAX(IF(rdh.step_parameter in ('Camera'), step_data, NULL)) as camera,
MAX(IF(rdh.step_parameter = 'MaxPower', step_data, NULL)) as MaxPower,
MAX(IF(rdh.step_parameter = 'PSU1', step_data, NULL)) as PSU1,
MAX(IF(rdh.step_parameter = 'PSU2', step_data, NULL)) as PSU2,
MAX(IF(rdh.step_parameter = 'PSU3', step_data, NULL)) as PSU3,
MAX(IF(rdh.step_parameter = 'PSU4', step_data, NULL)) as PSU4,
MAX(IF(rdh.step_parameter = 'Octopus', step_data, NULL)) as octopus,
MAX(IF(rdh.step_parameter = 'Octopus O-ring Grease', step_data, NULL)) as octopus_Oring_grease,
MAX(IF(rdh.step_parameter = 'Particulate Filters', step_data, NULL)) as particulate_filters,
MAX(IF(rdh.step_parameter = 'Stage O-ring Grease', step_data, NULL)) as stage_Oring_grease,


from last_run_per_growth 
join rdh on rdh.id_run = last_run_per_growth.last_equipment_run and last_run_per_growth.step_parameter = rdh.step_parameter
-- where id_run_growth = 25844
group by 1,2,3

END


