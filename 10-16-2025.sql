-- Colin - look for charx on 2x2
-- charx started implementation around 10-8, longer dimension wafer runs > 80, 2x2 > 40
select id_block,process_name, max(timestamp) as timestamp, max(id_run) as id_run, max(data) as data from `df-data-and-ai.trilliant_warehouse.vw_block_history`
where process_name in ('New Clear Seed Characterization', 'New Seed Characterization')
and step_parameter = 'Longer Dimension'
and safe_cast(data as float64) > 40
and timestamp > current_timestamp() - interval 30 day
group by 1,2
