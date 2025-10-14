-- Check block that been through TDL but not been through core

with tdled as (select id_block, end_time
from `df-mes.mes_warehouse.block_step_tracker`    mh
where mh.flow_name_in = 'TDL-Seed_V0L'
    and mh.end_time BETWEEN timestamp_trunc(current_timestamp() - interval 14+1 day,DAY, "America/Los_Angeles") AND timestamp_trunc(current_timestamp(),DAY, "America/Los_Angeles")
    and mh.step_name in (
    '304020'
    )
),
cored as (
    select id_block, end_time as cored_time
from `df-mes.mes_warehouse.block_step_tracker`    mh
where mh.flow_name_in = 'TDL-Seed_V0L'
    -- and mh.end_time BETWEEN timestamp_trunc(current_timestamp() - interval 14+1 day,DAY, "America/Los_Angeles") AND timestamp_trunc(current_timestamp(),DAY, "America/Los_Angeles")
    and mh.step_name in (
    '302020'
    )
)
select *, timestamp_diff(end_time, cored_time, minute)/60/24 as time_delay from tdled
left join cored using(id_block)


-- Total blocks cored in last 5 day
select   

count(id_block)

    from
    (
    select start_time, end_time, id_block, material_type_last, product_name_last, 
    REGEXP_REPLACE(
    REGEXP_REPLACE(step_name, r'^403', '303'),
    r'^404', '304'
    ) AS step_name,
    REGEXP_REPLACE(
    REGEXP_REPLACE(step_name_next, r'^403', '303'),
    r'^404', '304'
    ) AS step_name_next,
    step_description_in, product_id_next,material_name_in,manufacturer_lot_number_in, flow_name_in 
    from
    df-mes.mes_warehouse.block_step_tracker
    ) mh
    where 1=1 -- mh.flow_name_in in ('TDL-Seed_V0H', 'TDL-Seed_V0B')
    and mh.end_time BETWEEN timestamp_trunc(current_timestamp() - interval 5 day,DAY, "America/Los_Angeles") AND timestamp_trunc(current_timestamp(),DAY, "America/Los_Angeles")
    and mh.step_name in (
    '302020'
    )


-- Coring cycle time

with blocks_to_search as (
    select
    fp.id_block,
    blocks.parent_id_block,
    fp.timestamp as WS_ship_out_time,
    fps.size_family,
    FORMAT_TIMESTAMP('%Y-%U', DATETIME(fp.timestamp, 'America/Los_Angeles') + INTERVAL 1 WEEK) AS WS_ship_out_week,
    max(fps.surface_finish) as PIM_surface_finish,
    max(fps.output_category) as PIM_output_category
    from `df-max.raw_dfdb.final_plates` fp
    join `df-max.raw_dfdb.final_plate_shipments` fps using(id_final_plate_shipment)
    join `df-max.raw_dfdb.blocks` blocks on fp.id_block = blocks.id_block
    where fp.timestamp > timestamp_trunc(current_timestamp() - interval 90 day, day, 'America/Los_Angeles')
    and fp.timestamp <  timestamp_trunc(current_timestamp(), day, 'America/Los_Angeles')
    group by 1,2,3,4
    ),
    record_raw as (
    select
    mh.id_block as processed_block,
    blocks_to_search.id_block as child_plate,
    blocks_to_search.WS_ship_out_time,
    blocks_to_search.parent_id_block,
    case 
    when fs.segment_name = 'Shave/Trim' then
      case when mh.step_name in ('306010') then 'Trim'
      when mh.step_name in ('306006') then 'Shave'
      else 'Shave/Trim Buffer and Char' end
    when fs.segment_name = 'TDL' then
      case when mh.step_name in ('304020') then 'TDL'
      when mh.step_name in ('304030','304040','304050','304060','304062') then 'Split'
      else 'TDL/Split' end
    else fs.segment_name end as segment_name,
    manufacturer_lot_number_in as batch,
    cycle_time/3600 as cycle_hour,
    product_name_in as product_name,
    step_description_in as step_description,
    area_name_in as area_name,
    Concat(step_name,'-',IFNULL(step_description_in, '')) as step
    from `df-mes.mes_warehouse.block_step_tracker` mh
    JOIN blocks_to_search on (mh.id_block = blocks_to_search.id_block or mh.id_block = blocks_to_search.parent_id_block)
    join `df-mes.mes_warehouse.flow_segments_new` fs on fs.segment_id = substring(mh.step_name,1,3)
    where end_time > current_timestamp() - interval 365 day
    and step_name_next is not null
    and cycle_time is not null
    and material_type_in != 'Software Test'
    -- and segment_name in ('Coring', 'TDL','Shave/Trim')
    ),
    cycle_time_per_block as (
    select child_plate, parent_id_block, case when segment_name in ('Coring') then 'Core'
    when segment_name in ('TDL') then 'TDL'
    when segment_name in ('Split') then 'Split'
    when segment_name in ('TDL/Split') then 'TDL/Split'
    when segment_name in ('Trim') then 'Trim'
    when segment_name in ('Shave') then 'Shave'
    when segment_name in ('Shave/Trim Buffer and Char') then 'Trim / Shave'
    end as segment_name, WS_ship_out_time, sum(cycle_hour) as cycle_hour, 
    from record_raw
    group by 1, 2, 3,4
    ),
    segments as 
    (select segment_name, segment_rank from unnest(['Core', 'TDL','Split','TDL/Split', 'Trim', 'Shave','Trim / Shave']) segment_name with offset as segment_rank),
    periods as 
    (select period, period_rank from unnest(['90d', '30d', '7d']) period with offset as period_rank),
    seg_p as 
    (select * from segments join periods on true)

select PERCENTILE_CONT(cycle_hour, 0.5) over (partition by segment_name) from cycle_time_per_block
where segment_name = 'Core'

-- Page made for data checking
https://g.df.com/d/faz4bls/block-mes-history-timeline?folderUid=cebh1a4wxdybka&orgId=1&from=now-90d&to=now&timezone=browser&var-query0=&var-block_list=%201870173&var-id_block=$__all&tab=transformations

-- data for individual coring cycle time
https://drive.google.com/file/d/1L0FW7V3t9W9Ea_46UYoGVx86CuSaAZJB/view

-- Blocks that have been through that new buffer
select id_block 
from `df-mes.mes_warehouse.block_step_tracker`    mh
where mh.flow_name_in = 'TDL-Seed_V0L'
    and mh.end_time BETWEEN timestamp_trunc(current_timestamp() - interval 14+1 day,DAY, "America/Los_Angeles") AND timestamp_trunc(current_timestamp(),DAY, "America/Los_Angeles")
    and mh.step_name in (
    '302005' -- Ingot In
    )
