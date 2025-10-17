-- Colin - look for charx on 2x2
-- charx started implementation around 10-8, longer dimension wafer runs > 80, 2x2 > 40
select id_block,process_name, max(timestamp) as timestamp, max(id_run) as id_run, max(data) as data from `df-data-and-ai.trilliant_warehouse.vw_block_history`
where process_name in ('New Clear Seed Characterization', 'New Seed Characterization')
and step_parameter = 'Longer Dimension'
and safe_cast(data as float64) > 40
and timestamp > current_timestamp() - interval 30 day
group by 1,2

-- some growth with improper operation
select id_run_growth,growing_location_name, count(id_block), string_agg(cast(id_block as string)) from
(select case when bgr.run_duration >= 690 and bgr.hardware_platform like 'Gen7.5%' then 'Gem' 
    when bgr.run_duration >= 770 and bgr.hardware_platform like 'Gen8%' then 'Gem'
    when bgr.run_duration >= 270 and bgr.run_duration < 350 then 'Plate' end as run_type,
    date(bgr.end_time, 'America/Los_Angeles') as end_date, bgbd.id_block, bgr.run_duration,
    bgbd.new_thickness, bgbd.pre_thickness, bgbd.abs_clean_area_rect_210,
    (bgbd.new_thickness - bgbd.pre_thickness) * bgbd.abs_clean_area_rect_210 as final_clean_vol_cm3,
    bgr.id_run_growth, bgr.growing_location_name
    from `df-data-and-ai`.trilliback.bq_growth_block_data bgbd
    inner join `df-data-and-ai`.trilliback.bq_growth_runs bgr 
    on bgbd.id_run_growth = bgr.id_run_growth 
    where date(bgr.end_time, 'America/Los_Angeles') >= current_date('America/Los_Angeles') - interval 30 day 
    and date(bgr.end_time, 'America/Los_Angeles') < current_date('America/Los_Angeles')
)
where run_type is not null
and final_clean_vol_cm3 is null
and end_date < '2025-10-16'
and growing_location_name = 'Diamond Foundry WA'
group by 1,2
