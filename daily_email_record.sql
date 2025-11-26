with 
  sc as ( --segment change, do not make it automatic as there might be alternative flow and steps
    SELECT step_name,step_name_next, segment_name, unit
    FROM UNNEST([
      STRUCT('702038' AS step_name,'702040' as step_name_next, 'Block Prep' AS segment_name, 'Block' AS unit ),
      ('702040','704020', 'RIM','Block'),
      ('704050','704060', 'Singulation|parent','Block'),
      ('704070','706015', 'Singulation|child','Plate'),
      ('706025','710010','Basic Surfin','Plate'),
      ('710035','711010','Finish','Plate'),
      ('710035','712030','Finish','Plate')
    ])
   ),
numbered_step AS (
  SELECT
    step_name,
    step_name_next,
    segment_name,
    unit,
    DENSE_RANK() OVER (ORDER BY step_name) + 1 AS step_order -- start from 2
  FROM sc
),
throughput_raw as 
    (select   
    '1x1' as master_product,
    numbered_step.unit,
    end_time,
    timestamp_trunc(DATETIME(TIMESTAMP(end_time), "America/Los_Angeles"), DAY) as end_date_PST, 
    numbered_step.segment_name,
    1 as out,
    bd.thickness,
    mh.id_block
    from
        `df-mes.mes_warehouse.block_step_tracker` mh
    join numbered_step using(step_name, step_name_next)
    left join df-max.raw_dfdb.block_dimensions bd
    on mh.id_block = bd.id_block
    where mh.flow_name_in in ('BE Flow')
    and mh.end_time BETWEEN timestamp_trunc(current_timestamp() - interval 14+1 day,DAY, "America/Los_Angeles") AND timestamp_trunc(current_timestamp(),DAY, "America/Los_Angeles")
       and mh.material_type_in != 'Software Test'
       and numbered_step.segment_name != 'Finish'
 
    and mh.id_block not in (1414294,1414293,1497866,1835970,1835971,1546036,1414292,1643538)
    and end_time is not null

  -- add the PIM info

  UNION ALL
    SELECT
    '1x1' as master_product,
    'Plate' as unit,
    fps.timestamp end_time,
    timestamp_trunc(DATETIME(TIMESTAMP(fps.timestamp), "America/Los_Angeles"), DAY) as end_date_PST, 
    'Finish' segment_name,
    1 as out,
    bd.thickness,
    fp.id_block
    FROM `df-max.raw_dfdb.final_plates` fp
    JOIN `df-max.raw_dfdb.final_plate_shipments` fps USING (id_final_plate_shipment)
    JOIN df-max.raw_dfdb.block_dimensions bd USING(id_block)
    WHERE fps.timestamp BETWEEN timestamp_trunc(current_timestamp() - interval 14+1 day,DAY, "America/Los_Angeles") AND timestamp_trunc(current_timestamp(),DAY, "America/Los_Angeles")
    AND size_family = "1x1"
    ),
throughput as (
      select *,
      case 
      when unit = 'Block' then floor(IF(thickness != 0, thickness, 460)/460)*out
      else out
      end as plate_count
      from throughput_raw
      where end_time BETWEEN timestamp_trunc(current_timestamp() - interval 14+1 day,DAY, "America/Los_Angeles") AND current_timestamp()
    ),
segments as 
    (select segment_name,step_order from numbered_step
    group by 1,2
    ),
    periods as 
    (select period from unnest(['14d', '5d', '1d']) period),
    master_products as 
    (select master_product from unnest([ '1x1','2x2','D200']) master_product),
    seg_col as (
    select segment_name,step_order, period, master_product
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
    select seg_col.segment_name,step_order, seg_col.period as col, seg_col.master_product,
    d.output as output
    from seg_col
    left join 
    (
      select segment_name, '14d' as period, master_product, sum(plate_count) / 14 as output 
      from throughput
      group by segment_name, master_product
  UNION ALL
      select segment_name, '5d' as period, master_product, sum(plate_count) / 5 as output 
      from throughput
      where end_date_PST BETWEEN timestamp_trunc(datetime(current_timestamp() - interval 5+1 day, "America/Los_Angeles"), DAY) AND current_datetime()
      group by segment_name, master_product
  UNION ALL
      select segment_name, '1d' as period, master_product, sum(plate_count) /1 as output 
      from throughput
      where end_date_PST BETWEEN timestamp_trunc(datetime(current_timestamp() - interval 1 day, "America/Los_Angeles"), DAY) AND current_datetime()
      group by segment_name, master_product


    ) d 
    on d.segment_name = seg_col.segment_name 
    and d.master_product = seg_col.master_product 
    and d.period = seg_col.period
      -- Stock /30
  UNION ALL
      select 'Stock/30',8 as step_order, '5d' as period,
      '1x1' as master_product,
      sum(plate_count)/30 as output
      from `df-mes.mes_warehouse.WIP_Cache_plate`

  UNION ALL

    select 'Grown' as segment_name,1 as step_order, periods.period, master_product, output
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
split(segment_name, '|')[0] as segment, 
step_order,
col as period,
coalesce(sum(
    case when master_product = '1x1' then output
    when master_product = '2x2' then output * 4
    when master_product = 'D200' then output * 36
    else output end), 0.01) as yield,
master_product,
from seg_col_product_output
where 1=1
and segment_name != 'Singulation|parent'
and col = '5d'
and master_product = '1x1'
group by 1, 2, 3, 5
order by 2, 3, 5;
-- for cycle time---------------------------------------------------------------------------------------------------------------------------------------


WITH
fs AS ( -- flow segments (keep array order)
    SELECT step_name, segment_name, pos
    FROM UNNEST([
  STRUCT('702005' AS step_name, 'Block Prep'      AS segment_name),
  ('702020','Block Prep'),
  ('702030','Block Prep'),
  ('702033','Block Prep'),
  ('702035','Block Prep'),
  ('702038','Block Prep'),
  ('702040','RIM'),

  ('704020','Singulation|parent'),
  ('704035','Singulation|parent'),
  ('704040','Singulation|parent'),
  ('704050','Singulation|parent'),

  ('704060','Singulation|child'),
  ('704066','Singulation|child'),
  ('704070','Singulation|child'),

  ('706015','Basic Surfin'),
  ('706020','Basic Surfin'),
  ('706025','Basic Surfin'),

  ('710010','Finish'),
  ('710020','Finish'),
  ('710030','Finish'),
  ('710035','Finish')

    ]) WITH OFFSET AS pos
  ),
sc as ( --segment change, do not make it automatic as there might be alternative flow and steps
    SELECT step_name,step_name_next, segment_name, unit
    FROM UNNEST([
      STRUCT('702038' AS step_name,'702040' as step_name_next, 'Block Prep' AS segment_name, 'Block' AS unit ),
      ('702040','704020', 'RIM','Block'),
      ('704050','704060', 'Singulation|parent','Plate'),
      ('704070','706015', 'Singulation|child','Plate'),
      ('706025','710010','Basic Surfin','Plate'),
      ('710035','711010','Finish','Plate'),
      ('710035','712030','Finish','Plate')
    ])
   ),
   blocks_to_search as (
    select
    id_block,sc.segment_name,
    max(end_time) as segment_finish_time
    from `df-mes.mes_warehouse.block_step_tracker` mh
    join sc using(step_name, step_name_next) 
    where end_time > timestamp_trunc(current_timestamp() - interval 90 day, day, 'America/Los_Angeles')
    and end_time <  timestamp_trunc(current_timestamp(), day, 'America/Los_Angeles')
    and flow_name_in = 'BE Flow'
    and mh.id_block not in (1414294,1414293,1497866,1835970,1835971,1546036,1414292,1643538)
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
    and mh.flow_name_in = 'BE Flow'
    and material_type_in != 'Software Test'
    ),
    cycle_time_per_block as (
    select id_block, segment_name, segment_finish_time, sum(cycle_hour) as cycle_hour, 
    from record_raw
    group by 1, 2, 3
    ),
converted_cycle_time_per_block as (
select id_block, split(segment_name,'|')[0] as segment_name, max(segment_finish_time) as segment_finish_time, sum(cycle_hour) as cycle_hour from 
  (
      select b.id_block as id_block, cycle_time_per_block.segment_name, cycle_time_per_block.segment_finish_time, cycle_time_per_block.cycle_hour
      from cycle_time_per_block
      join `df-max.raw_dfdb.blocks` b on cycle_time_per_block.id_block = b.parent_id_block

      UNION ALL
      select * from cycle_time_per_block
      where segment_name != 'Singulation|parent'
  )
group by 1,2
),
  segments AS ( -- distinct segments + rank by first appearance
    SELECT
      split(segment_name,'|')[0] as segment_name,
      DENSE_RANK() OVER (ORDER BY MIN(pos)) AS segment_rank
    FROM fs
    GROUP BY 1
  ),
  periods as 
    (select period, period_rank from unnest(['90d', '30d', '7d']) period with offset as period_rank),
  seg_p as 
    (select * from segments join periods on true)


  select distinct seg_p.*, coalesce(t.cycle_hour_avg, 0.01) as cycle_hour_avg,coalesce(t.cycle_hour_max, 0.01) as cycle_hour_max, t.number_of_blocks
  from seg_p 
  left join 
  (
      select segment_name, '90d' as time_interval, -- count(child_plate) as child_count_with_data, avg(cycle_hour) as cycle_hour 
      AVG(cycle_hour) over (partition by segment_name) as cycle_hour_avg,MAX(cycle_hour) OVER (PARTITION BY segment_name)   cycle_hour_max, count(id_block) OVER (PARTITION BY segment_name)  as number_of_blocks
      from converted_cycle_time_per_block
      where segment_finish_time > timestamp_trunc(current_timestamp() - interval 90 day, day, 'America/Los_Angeles')
    UNION ALL
      select segment_name, '30d' as time_interval, -- count(child_plate) as child_count_with_data, avg(cycle_hour) as cycle_hour 
      AVG(cycle_hour) over (partition by segment_name) as cycle_hour_avg,MAX(cycle_hour) OVER (PARTITION BY segment_name)  cycle_hour_max, count(id_block) OVER (PARTITION BY segment_name)  as number_of_blocks
      from converted_cycle_time_per_block
      where segment_finish_time > timestamp_trunc(current_timestamp() - interval 30 day, day, 'America/Los_Angeles')
    UNION ALL
      select segment_name, '7d' as time_interval, -- count(child_plate) as child_count_with_data, avg(cycle_hour) as cycle_hour 
      AVG(cycle_hour) over (partition by segment_name) as cycle_hour_avg,MAX(cycle_hour) OVER (PARTITION BY segment_name)  cycle_hour_max, count(id_block) OVER (PARTITION BY segment_name)  as number_of_blocks
      from converted_cycle_time_per_block
      where segment_finish_time > timestamp_trunc(current_timestamp() - interval 7 day, day, 'America/Los_Angeles')
  ) t
  on seg_p.segment_name = t.segment_name
  and seg_p.period = t.time_interval
order by 4,2
