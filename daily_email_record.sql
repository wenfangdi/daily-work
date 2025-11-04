-- under new flow
with 
    step_table as (
  SELECT '702040' AS step_name, 'Block Prep'      AS segment, 'Block' AS unit UNION ALL
  -- SELECT '702020', 'Block Core',                         'Block' UNION ALL
  -- SELECT '702030', 'Edge Clean',                         'Block' UNION ALL
  -- SELECT '702032', 'Post Core CharX',                    'Block' UNION ALL
  -- SELECT '702035', 'Post Core Apex',                     'Block' UNION ALL
  -- SELECT '702040', 'Block Prep',                          'Block' UNION ALL

  -- SELECT '704020', 'Block TDL',                          'Block' UNION ALL
  -- SELECT '704035', 'Block De-RIM & Clean',               'Block' UNION ALL
  -- SELECT '704040', 'Split Ingress',                      'Block' UNION ALL
  -- SELECT '704050', 'Split Separate',                     'Block' UNION ALL
  -- SELECT '704060', 'Split Seed Breed',                   'Plate' UNION ALL

  -- SELECT '704065', 'Post Split Plate CharX',             'Plate' UNION ALL
  SELECT '704070', 'Singulation',               'Plate' UNION ALL

  -- SELECT '706015', 'Plate Shave',                        'Plate' UNION ALL
  -- SELECT '706020', 'Plate Clean',                        'Plate' UNION ALL
  SELECT '706025', 'Basic Surfin',            'Plate' UNION ALL

  -- SELECT '710010', 'Plate Trim',                         'Plate' UNION ALL
  -- SELECT '710020', 'Post Trim Plate CharX',              'Plate' UNION ALL
  SELECT '710030', 'Finish',               'Plate'

    ),
numbered_step AS (
  SELECT
    step_name,
    segment,
    unit,
    ROW_NUMBER() OVER (ORDER BY step_name) + 1 AS step_order -- start from 2
  FROM step_table
),
throughput_raw as 
    (select   
    '1x1' as master_product,
    numbered_step.unit,
    end_time,
    timestamp_trunc(DATETIME(TIMESTAMP(end_time), "America/Los_Angeles"), DAY) as end_date_PST, 
    IFNULL(numbered_step.segment, 'Block QC') as segment,
    flow_name_in as flow_name,
    1 as out,
    bd.thickness,
    mh.id_block
    from
        `df-mes.mes_warehouse.block_step_tracker` mh
    left join numbered_step on mh.step_name = numbered_step.step_name
    left join df-max.raw_dfdb.block_dimensions bd
    on mh.id_block = bd.id_block
    where mh.flow_name_in in ('BE Flow')
    and mh.end_time BETWEEN timestamp_trunc(current_timestamp() - interval 14+1 day,DAY, "America/Los_Angeles") AND timestamp_trunc(current_timestamp(),DAY, "America/Los_Angeles")
    and 
          (mh.step_name in (select step_name from step_table))
       -- and mh.material_type_in != 'Software Test'
    and end_time is not null
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
    (select segment,step_order from numbered_step
    ),
    periods as 
    (select period from unnest(['14d', '5d', '1d']) period),
    master_products as 
    (select master_product from unnest([ '1x1','2x2','D200']) master_product),
    seg_col as (
    select segment,step_order, period, master_product
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
    select seg_col.segment,step_order, seg_col.period as col, seg_col.master_product,
    d.output as output
    from seg_col
    left join 
    (
      select segment, '14d' as period, master_product, sum(plate_count) / 14 as output 
      from throughput
      group by segment, master_product
  UNION ALL
      select segment, '5d' as period, master_product, sum(plate_count) / 5 as output 
      from throughput
      where end_date_PST BETWEEN timestamp_trunc(datetime(current_timestamp() - interval 5+1 day, "America/Los_Angeles"), DAY) AND current_datetime()
      group by segment, master_product
  UNION ALL
      select segment, '1d' as period, master_product, sum(plate_count) /1 as output 
      from throughput
      where end_date_PST BETWEEN timestamp_trunc(datetime(current_timestamp() - interval 1 day, "America/Los_Angeles"), DAY) AND current_datetime()
      group by segment, master_product
    ) d 
    on d.segment = seg_col.segment 
    and d.master_product = seg_col.master_product 
    and d.period = seg_col.period

    UNION ALL

    select 'Grown' as segment,1 as step_order, periods.period, master_product, output
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
step_order,
col as period,
coalesce(sum(
    case when master_product = '1x1' then output
    when master_product = '2x2' then output * 4
    when master_product = 'D200' then output * 36
    else output end), 0.01) as yield,
master_product,
from seg_col_product_output
-- where col = '14d'
-- and master_product = '1x1'
group by 1, 2, 3, 5
order by 2, 3, 5;
-- for cycle time---------------------------------------------------------------------------------------------------------------------------------------

WITH
fs AS ( -- flow segments (keep array order)
    SELECT step_name, segment_name, pos
    FROM UNNEST([
  STRUCT('702025' AS step_name, 'Pre Core Block QC'      AS segment_name),
  ('702020','Block Core'),
  ('702030','Edge Clean'),
  ('702032','Post Core CharX'),
  ('702035','Post Core Apex'),
  ('702040','Block RIM'),

  ('704020','Block TDL'),
  ('704035','Block De-RIM & Clean'),
  ('704040','Split Ingress'),
  ('704050','Split Separate'),
  ('704060','Split Seed Breed'),

  ('704065','Post Split Plate CharX'),
  ('704070','Post Split Plate FRT'),

  ('706015','Plate Shave'),
  ('706020','Plate Clean'),
  ('706021','Plate Clean Passthrough'),

  ('710010','Plate Trim'),
  ('710020','Post Trim Plate CharX'),
  ('710030','Post Trim Plate Apex')

    ]) WITH OFFSET AS pos
  ),
  -- sc as ( --segment change, do not make it automatic as there might be alternative flow and steps
  --   SELECT step_name,step_name_next, segment_name
  --   FROM UNNEST([
  --     STRUCT('701025' AS step_name, '702020' AS step_name_next, 'Block QC' AS segment_name),
  --     ('702020','702030','Block Core'),
  --     ('702030','702035','Block Clean'),
  --     ('702035','703015','Block Measure'),
  --     ('703020','704020','RIM'),
  --     ('704020','704035','TDL'),
  --     ('704035','704040','Block De-RIM'),
  --     ('704050','704060','Split Ingress'),
  --     ('704062','704070','Split Separate'),
  --     ('704070','704105','Plate Measure'),
  --     ('704105','704110','Shave'),
  --     ('704110','704115','Trim'),
  --     ('708015','708018','Plate QC'),
  --     ('708018','708020', 'Plate Final Clean')
  --   ])
  --  ),
   blocks_to_search as (
    select
    id_block,bst.step_name,
    max(end_time) as segment_finish_time
    from `df-mes.mes_warehouse.block_step_tracker` bst
    where end_time > timestamp_trunc(current_timestamp() - interval 90 day, day, 'America/Los_Angeles')
    and end_time <  timestamp_trunc(current_timestamp(), day, 'America/Los_Angeles')
    and flow_name_in = 'BE Flow'
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
    JOIN blocks_to_search using(id_block, step_name)
    join fs using(step_name)
    where end_time > current_timestamp() - interval 365 day
    and step_name_next is not null
    and cycle_time is not null
    and mh.flow_name_in in ('BE Flow')
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
output_separated as (
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
select * from output_separated
order by segment_rank


