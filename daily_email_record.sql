-- under new flow
with 
    step_table as (
      SELECT '702020' as step_name, '3-Cored' as segment
      UNION ALL
      SELECT '703020', '4-RIM'
      UNION ALL 
      SELECT '704020', '5-TDLed'
      UNION ALL
      SELECT '704060', '6-Splitted' -- breeding
      UNION ALL
      SELECT '704110', '7-Trimmed'
      UNION ALL
      SELECT '704105', '8-Shaved'
    ),
throughput_raw as 
    (select   
    '1x1' as master_product,
    end_time,
    timestamp_trunc(DATETIME(TIMESTAMP(end_time), "America/Los_Angeles"), DAY) as end_date_PST, 
    IFNULL(step_table.segment, '2-Ingot In') as segment,
    flow_name_in as flow_name,
    1 as out,
    bd.thickness,
    mh.id_block
    from
        `df-mes.mes_warehouse.block_step_tracker` mh
    left join step_table on mh.step_name = step_table.step_name
    left join df-max.raw_dfdb.block_dimensions bd
    on mh.id_block = bd.id_block
    where mh.flow_name_in in ('MPF I&D','MPF')
    and mh.end_time BETWEEN timestamp_trunc(current_timestamp() - interval 14+1 day,DAY, "America/Los_Angeles") AND timestamp_trunc(current_timestamp(),DAY, "America/Los_Angeles")
    and ( (mh.step_name_next = '702020') or 
          (mh.step_name in (select step_name from step_table))
        )
       -- and mh.material_type_in != 'Software Test'
    and end_time is not null
    ),
throughput as (
      select *,
      case 
      when segment in ('2-Ingot In','3-Cored', '4-RIM','5-TDLed') then floor(IF(thickness != 0, thickness, 460)/460)*out
      else out
      end as plate_count,
      master_product as plate_size, 
      from throughput_raw
      where end_time BETWEEN timestamp_trunc(current_timestamp() - interval 14+1 day,DAY, "America/Los_Angeles") AND current_timestamp()
    ),
segments as 
    (select segment from step_table),
    periods as 
    (select period from unnest(['14d', '5d', '1d']) period),
    master_products as 
    (select master_product from unnest([ '1x1']) master_product),
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

    select '1-Grown' as segment, periods.period, master_product, output
    from periods 
    join master_products
    on true
    left join  
    (
      select '14d' as period,
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
      group by 1, 2
      ) yield
    on periods.period = yield.period
    and master_products.master_product = yield.seed_type    
    )
select 
segment, 
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
-- for cycle time---------------------------------------------------------------------------------------------------------------------------------------

WITH
fs AS ( -- flow segments (keep array order)
    SELECT step_name, segment_name, pos
    FROM UNNEST([
      STRUCT('702020' AS step_name, 'Core'        AS segment_name),
      ('702030','Core'),
      ('702035','Core'),
      ('703015','RIM'),
      ('703020','RIM'),
      ('704020','SingulationParent'),
      ('704035','SingulationParent'),
      ('704040','SingulationParent'),
      ('704050','SingulationParent'),
      ('704060','SingulationChild'),
      ('704062','SingulationChild'),
      ('704070','SingulationChild'),
      ('704105','Finish'),
      ('704110','Finish'),
      ('704115','Finish')
    ]) WITH OFFSET AS pos
  ),
  sc as ( --segment change, do not make it automatic as there might be alternative flow and steps
    SELECT step_name,step_name_next, segment_name
    FROM UNNEST([
      STRUCT('702035' AS step_name,'703015' as step_name_next, 'Core' AS segment_name),
      ('703020','704020', 'RIM'),
      ('704070','704105', 'SingulationParent'),
      ('704070','704105', 'SingulationChild'),
      ('704115','708015', 'Finish')
    ])
   ),
   blocks_to_search as (
    select
    id_block,sc.segment_name,
    max(end_time) as segment_finish_time
    from `df-mes.mes_warehouse.block_step_tracker`
    join sc using(step_name, step_name_next) 
    where end_time > timestamp_trunc(current_timestamp() - interval 90 day, day, 'America/Los_Angeles')
    and end_time <  timestamp_trunc(current_timestamp(), day, 'America/Los_Angeles')
    group by 1,2
    ),
  record_raw as (
    select
    mh.id_block,
    step_name, step_name_next,end_time,
    fs.segment_name as segment_name,
    blocks_to_search.segment_finish_time,
    manufacturer_lot_number_in as batch,
    cycle_time/3600 as cycle_hour,
    product_name_in as product_name,
    step_description_in as step_description,
    area_name_in as area_name,
    Concat(step_name,'-',IFNULL(step_description_in, '')) as step
    from `df-mes.mes_warehouse.block_step_tracker` mh
    JOIN blocks_to_search on (mh.id_block = blocks_to_search.id_block)
    join fs using(step_name, segment_name)
    where end_time > current_timestamp() - interval 365 day
    and step_name_next is not null
    and cycle_time is not null
    -- and material_type_in != 'Software Test'
    -- and segment_name in ('Coring', 'TDL','Shave/Trim')
    ),
    cycle_time_per_block as (
    select id_block, segment_name, segment_finish_time, sum(cycle_hour) as cycle_hour, 
    from record_raw
    group by 1, 2, 3
    ),
  segments AS ( -- distinct segments + rank by first appearance
    SELECT
      segment_name,
      DENSE_RANK() OVER (ORDER BY MIN(pos)) AS segment_rank
    FROM fs
    GROUP BY segment_name
  ),
  periods as 
    (select period, period_rank from unnest(['90d', '30d', '7d']) period with offset as period_rank),
  seg_p as 
    (select * from segments join periods on true),
output_with_singulation_separated as (
      select distinct seg_p.*, coalesce(t.cycle_hour_median, 0.01) as cycle_hour_median
  from seg_p 
  left join 
  (select segment_name, '90d' as time_interval, -- count(child_plate) as child_count_with_data, avg(cycle_hour) as cycle_hour 
  PERCENTILE_CONT(cycle_hour, 0.5) over (partition by segment_name) as cycle_hour_median,
  from cycle_time_per_block
  where segment_finish_time > timestamp_trunc(current_timestamp() - interval 90 day, day, 'America/Los_Angeles')
  UNION ALL
  select segment_name, '30d' as time_interval, -- count(child_plate) as child_count_with_data, avg(cycle_hour) as cycle_hour 
  PERCENTILE_CONT(cycle_hour, 0.5) over (partition by segment_name) as cycle_hour_median,
  from cycle_time_per_block
  where segment_finish_time > timestamp_trunc(current_timestamp() - interval 30 day, day, 'America/Los_Angeles')
  UNION ALL
  select segment_name, '7d' as time_interval, -- count(child_plate) as child_count_with_data, avg(cycle_hour) as cycle_hour 
  PERCENTILE_CONT(cycle_hour, 0.5) over (partition by segment_name) as cycle_hour_median,
  from cycle_time_per_block
  where segment_finish_time > timestamp_trunc(current_timestamp() - interval 7 day, day, 'America/Los_Angeles')) t
  on seg_p.segment_name = t.segment_name
  and seg_p.period = t.time_interval

  )

-- Last merge two singulation into one
select 
case when segment_name like 'Singulation%' then 'Singulation'
  ELSE segment_name end as segment,
segment_rank,
period,
period_rank,
sum(cycle_hour_median) as cycle_hour_median
from output_with_singulation_separated
group by 1,2,3,4
  order by segment_rank, period_rank

