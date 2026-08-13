declare test_range int64;
set test_range = 90;

with lib as (
  select A as step_name, B as Seg, C as process_name, D as step_parameter, E as material_type, F as flow_name 
  from `df-data-and-ai.google_sheet.WeeklyEmailBEYield_YieldChart`
),
siblings_tab as (
  select parent_id_block, count(b.id_block) siblings, max(bd.mean_thickness) as parent_thickness
  from `df-max.raw_dfdb.blocks` b
  join `df-max.raw_dfdb.block_dimensions` bd on b.parent_id_block = bd.id_block
  group by 1
),
cost_thick_per_child as (
  select parent_id_block, id_block,parent_thickness/siblings as cost_per_parent_thickness
  from siblings_tab
  join `df-max.raw_dfdb.blocks` b using(parent_id_block)
  where siblings < 10
),
finished as (
select bst.id_block, vbh.timestamp,lib.Seg, vbh.step_parameter, safe_cast(vbh.data as float64) data, lib.material_type, 

bdbq.post_growth_clean_vol, bdbq.post_tdl_thickness,
cost_thick_per_child.cost_per_parent_thickness

from `df-mes.mes_warehouse.block_step_tracker` bst
left join `df-data-and-ai.trilliant_warehouse.block_dimensions_bq` bdbq on bst.id_block = bdbq.id_block
left JOIN cost_thick_per_child on bst.id_block = cost_thick_per_child.id_block
join `df-data-and-ai.trilliant_warehouse.vw_block_history` vbh on bst.id_block = vbh.id_block and vbh.timestamp between bst.start_time and bst.end_time
join lib on bst.step_name = lib.step_name and bst.flow_name_in = lib.flow_name and lib.process_name = vbh.process_name and vbh.step_parameter = lib.step_parameter
and bst.end_time > current_timestamp() - interval test_range day
and vbh.timestamp > current_timestamp() - interval test_range day
),
finished_yield_per_piece as (
select id_block, Seg,
min(
case 
  when Seg = 'Prep' then (safe_divide(data,post_growth_clean_vol))
  when Seg = 'RIM&TDL' then (safe_divide(data,cost_per_parent_thickness))
  when Seg = 'Trim&Shave' and step_parameter = 'mean_thickness' then (safe_divide(data,post_tdl_thickness))
 else null end) as weight_loss_yield 
 from finished
-- where Seg = 'RIM&TDL'
group by 1,2
)
select Seg, avg(weight_loss_yield) weight_loss_yield

 from finished_yield_per_piece
 group by 1
