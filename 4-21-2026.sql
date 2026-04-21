with block_shipped_in_trilliant as (
  SELECT
            id_run AS transaction_id,
            timestamp AS date_transacted,
            station_name AS vendor,
            id_block,
            -- sn, 
            length,
            width,
            thickness,
            weight,
            tray_name,
            load_order 
        FROM
        -- vw_blocks_in_trays smd_shipment_parcels
            `df-max.raw_dfdb.vw_blocks_in_trays` AS smd_shipment_parcels
        WHERE station_type_name in ('External Slicing Buffer', 'External Slicing Vendor')
        and timestamp > current_timestamp() - interval 90 day
        ORDER BY date_transacted DESC,tray_name, load_order

),
blocks_shipped_in_MES as (
  select 
id_block
from `df-mes.mes_warehouse.block_step_tracker`
where step_name_next = '210102'
group by 1
),
blocks_have_child_origin_SMD as (
  select parent_id_block, max(bd.batch_origin) as batch_origin, max(job_number) as job_number
  from `df-max.raw_dfdb.blocks` blocks
  join `df-max.raw_dfdb.block_dimensions` bd using(id_block)
  where bd.batch_origin like '%SMD%'
  and blocks.timestamp > current_timestamp() - interval 90 day
  group  by 1
)
-- select * from block_shipped_in_trilliant
-- where id_block not in (select id_block from blocks_shipped_in_MES)
-- and vendor not like 'DFISF%'
-- and vendor not like 'ESP%'

select blocks_have_child_origin_SMD.*, vbit.station_name, vbit.location_name, vbit.timestamp
from blocks_have_child_origin_SMD 
join `df-max.raw_dfdb.vw_blocks_in_trays` vbit on vbit.id_block = blocks_have_child_origin_SMD.parent_id_block
where blocks_have_child_origin_SMD.parent_id_block not in (select id_block from blocks_shipped_in_MES)
