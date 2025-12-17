merge `df-data-and-ai.trilliant_warehouse.out_of_wip_tab` oow
using(
with last_run as (select id_block, max(id_run) id_run from `df-data-and-ai.trilliant_warehouse.vw_block_history`
where step_parameter = 'Longest Dim 13.3x14.8'
group by 1
)
select id_block, max(data) as data from `df-data-and-ai.trilliant_warehouse.vw_block_history`
join last_run using(id_block, id_run)
where step_parameter = 'Longest Dim 13.3x14.8'
group by 1
) t
on oow.id_block = t.id_block
when matched and oow.longest_dim_133_148 is null and safe_cast(t.data as float64) is not null then
update set
oow.longest_dim_133_148 =  safe_cast(t.data as float64)
