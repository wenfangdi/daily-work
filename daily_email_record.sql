-- before new flow implementation 
    with throughput_raw as 
    (select   
    '1x1' as master_product,
    end_time,
    -- mh.product_name_last,
    timestamp_trunc(DATETIME(TIMESTAMP(end_time), "America/Los_Angeles"), DAY) as end_date_PST, 
    case 
    when mh.step_name = '301005' then '2-Ingot In'
    when mh.step_name = '302020' then '3-Cored'
    when mh.step_name = '304020' then '4-TDLed'
    when mh.step_name = '304060' then '5-Splitted'
    when mh.step_name = '306010' then '6-Trimmed'
    when mh.step_name = '306006' then '7-Shaved'
    end
    as segment,
    flow_name_in as flow_name,
    1 as out,
    bd.thickness,
    mh.id_block
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
    left join df-max.raw_dfdb.block_dimensions bd
    on mh.id_block = bd.id_block
    left join 
    (select master_product, sub_product_name, sub_product_id as product_id from df-mes.mes_warehouse.master_products 
    where product_group = 'Product Reporting Layer') mp 
    on mh.product_id_next = mp.product_id
    join df-mes.mes_warehouse.flow_segments_new fs 
    on fs.segment_id = substring(mh.step_name,1,3)
    where mh.flow_name_in = 'TDL-Seed_V0L'
    and mh.end_time BETWEEN timestamp_trunc(current_timestamp() - interval 14+1 day,DAY, "America/Los_Angeles") AND timestamp_trunc(current_timestamp(),DAY, "America/Los_Angeles")
    and mh.step_name in (
    '301005', -- Ingot In
    '302020', -- cored
    '304020', -- TDLed use presumed plate count
    '304060', -- splitted
    '306010', -- trimmed
    '306006' -- shaved 
    )
    and end_time is not null
    ),
    throughput as (
    select *,
    case 
    when segment in ('2-Ingot In','3-Cored', '4-TDLed') then floor(thickness/460)*out
    else out
    end as plate_count,
    master_product as plate_size, 
    from throughput_raw
    where end_time BETWEEN timestamp_trunc(current_timestamp() - interval 14+1 day,DAY, "America/Los_Angeles") AND current_timestamp()
    ),
    segments as 
    (select segment from unnest(['2-Ingot In','3-Cored', '4-TDLed', '5-Splitted','6-Trimmed','7-Shaved']) segment),
    periods as 
    (select period from unnest(['14d', '5d', '1d']) period),
    master_products as 
    (select master_product from unnest([ '1x1', '2x2', 'D200']) master_product),
    seg_col as (
    select segment, period, master_product as plate_size 
    from segments
    join periods
    on true
    join master_products
    on true
    ),
    grown as (
    select 
    seed_size_type as seed_type,
    b.id_block,
    b.end_time,
    greatest(IFNULL(floor(new_thickness / 460 - 2),0), 1) as plate_count
    from `df-data-and-ai.trilliback.bq_growth_block_data` b
    join `df-data-and-ai.trilliback.bq_growth_runs_about_block` r using(id_run_growth) 
    where date(b.end_time, 'America/Los_Angeles') >= current_date('America/Los_Angeles') - interval 14 day 
    and date(b.end_time, 'America/Los_Angeles') < current_date('America/Los_Angeles')
    and r.seed_size_type != 'Others'
    ),    
    seg_col_product_output as (
    select seg_col.segment, seg_col.period as col, seg_col.plate_size,
    d.output as output
    from seg_col
    left join 
    (select segment, '14d' as period, plate_size, sum(plate_count) / 14 as output 
    from throughput
    group by segment, plate_size
    UNION ALL
    select segment, '5d' as period, plate_size, sum(plate_count) / 5 as output 
    from throughput
    where end_date_PST BETWEEN timestamp_trunc(datetime(current_timestamp() - interval 5+1 day, "America/Los_Angeles"), DAY) AND current_datetime()
    group by segment, plate_size
    UNION ALL
    select segment, '1d' as period, plate_size, sum(plate_count) /1 as output 
    from throughput
    where end_date_PST BETWEEN timestamp_trunc(datetime(current_timestamp() - interval 1 day, "America/Los_Angeles"), DAY) AND current_datetime()
    group by segment, plate_size
    ) d 
    on d.segment = seg_col.segment 
    and d.plate_size = seg_col.plate_size 
    and d.period = seg_col.period
    UNION ALL
    SELECT '8-Stock/30' as segment, '5d' as period, -- bar location holder no real meaning
    case when master_product = '20x20' THEN 'Fillers'
    when master_product = '33x26' THEN '1x1'
    else 'Fillers' 
    end as plate_size, 
    sum(plate_count) / 30 as output 
    from df-mes.mes_warehouse.WIP_Cache_plate 
    WHERE master_product in ('33x26')
    and flow_name = 'TDL-Seed_V0L'
    group by 1, 2, 3   
    UNION ALL
    select '1-Grown' as segment, periods.period, master_product, output
    from periods 
    join master_products
    on true
    left join    
    (select '14d' as period,
    seed_type,
    sum(plate_count) / 14 as output
    from grown
    where date(end_time, 'America/Los_Angeles') >= current_date('America/Los_Angeles') - interval 14 day 
    and date(end_time, 'America/Los_Angeles') < current_date('America/Los_Angeles')
    group by 1, 2
    UNION ALL
    select '5d' as period,
    seed_type,
    sum(plate_count) / 5 as output
    from grown
    where date(end_time, 'America/Los_Angeles') >= current_date('America/Los_Angeles') - interval 5 day 
    and date(end_time, 'America/Los_Angeles') < current_date('America/Los_Angeles')
    group by 1, 2
    UNION ALL
    select '1d' as period,
    seed_type,
    sum(plate_count)  as output
    from grown
    where date(end_time, 'America/Los_Angeles') >= current_date('America/Los_Angeles') - interval 1 day 
    and date(end_time, 'America/Los_Angeles') < current_date('America/Los_Angeles')
    group by 1, 2) yield
    on periods.period = yield.period
    and master_products.master_product = yield.seed_type    
    )
    select 
    case 
    when segment = '1-Grown' then 'Ingots Grown'
    when segment = '2-Ingot In' then 'Ingots In@BE'
    when segment = '3-Cored' then 'Core'
    when segment = '4-TDLed' then 'TDL'
    when segment = '5-Splitted' then 'Split'
    when segment = '6-Trimmed' then 'Trim'
    when segment = '7-Shaved' then 'Shave'
    when segment = '8-Stock/30' then 'Stock/30'
    end as step, 
    safe_cast(left(segment, 1) as int64) - 1 as step_number,
    col as period,
    coalesce(sum(
        case when plate_size = '1x1' then output
        when plate_size = '2x2' then output * 4
        when plate_size = 'D200' then output * 36
        else output end), 0.01) as yield,
    plate_size,
    from seg_col_product_output
    group by 1, 2, 3, 5
    order by 2, 3, 5;

-- for cycle time

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
      else 'TDL / Split' end
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
    select child_plate, case when segment_name in ('Coring') then 'Core'
    when segment_name in ('TDL') then 'TDL'
    when segment_name in ('Split') then 'Split'
    when segment_name in ('TDL / Split') then 'TDL / Split'
    when segment_name in ('Trim') then 'Trim'
    when segment_name in ('Shave') then 'Shave'
    when segment_name in ('Shave/Trim Buffer and Char') then 'Trim / Shave'
    end as segment_name, WS_ship_out_time, sum(cycle_hour) as cycle_hour, 
    from record_raw
    group by 1, 2, 3
    ),
    segments as 
    (select segment_name, segment_rank from unnest(['Core', 'TDL', 'Split', 'Trim', 'Shave', 'TDL / Split', 'Trim / Shave']) segment_name with offset as segment_rank),
    periods as 
    (select period, period_rank from unnest(['90d', '30d', '7d']) period with offset as period_rank),
    seg_p as 
    (select * from segments join periods on true)
    select distinct seg_p.*, coalesce(t.cycle_hour_median, 0.01) as cycle_hour_median
    from seg_p 
    left join 
    (select segment_name, '90d' as time_interval, -- count(child_plate) as child_count_with_data, avg(cycle_hour) as cycle_hour 
    PERCENTILE_CONT(cycle_hour, 0.5) over (partition by segment_name) as cycle_hour_median,
    from cycle_time_per_block
    where WS_ship_out_time > timestamp_trunc(current_timestamp() - interval 90 day, day, 'America/Los_Angeles')
    UNION ALL
    select segment_name, '30d' as time_interval, -- count(child_plate) as child_count_with_data, avg(cycle_hour) as cycle_hour 
    PERCENTILE_CONT(cycle_hour, 0.5) over (partition by segment_name) as cycle_hour_median,
    from cycle_time_per_block
    where WS_ship_out_time > timestamp_trunc(current_timestamp() - interval 30 day, day, 'America/Los_Angeles')
    UNION ALL
    select segment_name, '7d' as time_interval, -- count(child_plate) as child_count_with_data, avg(cycle_hour) as cycle_hour 
    PERCENTILE_CONT(cycle_hour, 0.5) over (partition by segment_name) as cycle_hour_median,
    from cycle_time_per_block
    where WS_ship_out_time > timestamp_trunc(current_timestamp() - interval 7 day, day, 'America/Los_Angeles')) t
    on seg_p.segment_name = t.segment_name
    and seg_p.period = t.time_interval
    order by segment_rank, period_rank;
