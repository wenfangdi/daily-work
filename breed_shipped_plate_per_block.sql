WITH
Shipped_plates as (
select id_block, min(fp.timestamp) as shipping_time 
  from `df-max.raw_dfdb.final_plates` fp
  join `df-max.raw_dfdb.final_plate_shipments` fps using(id_final_plate_shipment)
where size_family = '1x1'
group by 1
),
last_growth_per_block_1 as (
  select id_block, max(id_run_growth) as id_run_growth, max(r.end_time) as growth_end_time, sum(run_duration) as total_run_duration
  from `df-data-and-ai.trilliback.bq_growth_block_data`
  join `df-data-and-ai.trilliback.bq_growth_runs` r using(id_run_growth)
  group by 1
),
last_growth_per_block as (
  select last_growth_per_block_1.*,
  pre_width*pre_length as parent_area,
  tc.width*tc.height as tray_cell_area
  from last_growth_per_block_1
  join `df-data-and-ai.trilliback.bq_growth_block_data` b using(id_block, id_run_growth)
  join `df-max.raw_dfdb.tray_cells` tc using(id_tray_cell)
)

select 
blocks.parent_id_block,
tray_cell_area,
last_growth_per_block.growth_end_time,
total_run_duration,
count(blocks.id_block) as breeded,
count(Shipped_plates.id_block) as shipped,
max(blocks.timestamp) as breeding_time,
avg(coalesce(pbq.initial_longer_dimension*pbq.initial_shorter_dimension,parent_area)) as parent_area,
avg(bq.initial_longer_dimension*bq.initial_shorter_dimension) as child_avg_area,
avg(sqrt(bq.initial_longer_dimension*bq.initial_shorter_dimension / 
coalesce(pbq.initial_longer_dimension*pbq.initial_shorter_dimension,parent_area)
)) as shrinkage,
avg(coalesce(pbq.initial_longer_dimension*pbq.initial_shorter_dimension,parent_area) / 
tray_cell_area
) as parent_tray_occupation
from `df-max.raw_dfdb.blocks` blocks 
join `df-data-and-ai.trilliant_warehouse.block_dimensions_bq` bq on blocks.id_block = bq.id_block
join last_growth_per_block on last_growth_per_block.id_block= blocks.parent_id_block
left join Shipped_plates on blocks.id_block = Shipped_plates.id_block
left join `df-data-and-ai.trilliant_warehouse.block_dimensions_bq` pbq on blocks.parent_id_block = pbq.id_block
where last_growth_per_block.growth_end_time > '2025-04-01'
group by 1,2,3,4
having shipped > 0
