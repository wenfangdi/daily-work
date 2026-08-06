with raw_wa as (
  select 
    r.id_run_growth,r.station_name,
    rb.mean_growth_rate,rb.seed_yield_p, rb.block_with_post_product,
    rb.seed_count,rb.block_with_any_clean_vol,rb.block_with_clean_vol_240,rb.clean_vol_240_210_cm3,
    r.end_time,current_timestamp() as current_time, r.run_duration,r.requested_run_duration, r.loading_purpose, r.hardware_platform, r.growing_location_name, 
    r.mean_incident_power, r.mean_chamber_pressure,r.pilot_run,
    stations.fiix_asset_id, 
    NULL as RM_fiix_id, -- placeholder for not checking RM
    case when r.hardware_platform like 'Gen7.5%' then 'Gen7.5'
    when r.hardware_platform like 'Gen8%' then 'Gen8'
    else 'NULL' end as Gen,
    lag(id_run_growth) over(partition by r.station_name, r.loading_purpose order by start_time) as prev_id_run_growth
    from `df-data-and-ai.trilliback.bq_growth_runs` r
    join `df-max.raw_dfdb.stations` stations using(id_station)
    left join `df-data-and-ai.trilliback.bq_growth_runs_about_block` rb using(id_run_growth)
    where 1=1 
    and r.run_duration > 200
    and r.growing_location_name = 'Diamond Foundry WA'
    and r.mean_chamber_pressure between 160 - 0.1 and 160 + 0.1
    and r.pilot_run = 0
),
all_rec_wa as (
  select 
    r.*,pr.clean_vol_240_210_cm3 as prev_clean_vol_240_210_cm3,pr.seed_yield_p as prev_seed_yield_p,
    pr.end_time as prev_growth_end_time,
    timestamp_diff( r.end_time,pr.end_time, day) days_since_last_growth,
    case 
    -- Gen7.5 Gem
      when r.Gen = 'Gen7.5' and r.loading_purpose = 'Production'
        and r.mean_incident_power between 40 and 42.8 
        and r.block_with_any_clean_vol - r.block_with_clean_vol_240 <= 3
        and r.seed_count - r.block_with_any_clean_vol <= 3
      then
        case 
            when 
              safe_divide(r.run_duration, r.requested_run_duration) > 0.95 
              and r.clean_vol_240_210_cm3 < 74.3 and pr.clean_vol_240_210_cm3 < 74.3 
              and timestamp_diff( r.end_time,pr.end_time, day) < 60 then 'Advanced Low Yield'
            when r.mean_incident_power > 41 and r.run_duration > 200 and r.mean_growth_rate < 9.486 then 'Advanced Low Yield'
            when r.run_duration > 200 and r.mean_growth_rate > 10.2967 then 'Advanced Low Yield'
            when safe_divide(r.run_duration, r.requested_run_duration) > 0.95 and r.clean_vol_240_210_cm3 < 74.3 then 'Basic Low Yield'
            else 'NA' end
      --Gen8 Seed
      when r.Gen = 'Gen8' and r.loading_purpose = 'Seed'
        and r.mean_incident_power between 48.2 and 49.9
        and r.seed_count - r.block_with_post_product <= 3
      then
        case 
            when 
              safe_divide(r.run_duration, r.requested_run_duration) > 0.95 
              and r.seed_yield_p < 0.786 and pr.seed_yield_p < 0.786
              and timestamp_diff( r.end_time,pr.end_time, day) < 60 then 'Advanced Low Yield'
            when r.mean_incident_power > 48 and r.run_duration > 200 and r.mean_growth_rate < 8.824 then 'Advanced Low Yield'
            when r.run_duration > 200 and r.mean_growth_rate > 9.467 then 'Advanced Low Yield'
            when safe_divide(r.run_duration, r.requested_run_duration) > 0.95 and r.seed_yield_p < 0.786 then 'Basic Low Yield'
            else 'NA' end
      -- Gen8 Gem
      when r.Gen = 'Gen8' and r.loading_purpose = 'Production'
        and r.mean_incident_power between 48.2 and 49.9
        and r.block_with_any_clean_vol - r.block_with_clean_vol_240 <= 3
        and r.seed_count - r.block_with_any_clean_vol <= 3
      then 
        case 
            when 
              safe_divide(r.run_duration, r.requested_run_duration) > 0.95 
              and r.clean_vol_240_210_cm3 < 92.98 and pr.clean_vol_240_210_cm3 < 92.98
              and timestamp_diff( r.end_time,pr.end_time, day) < 60 then 'Advanced Low Yield'
            when r.mean_incident_power > 48.5 and r.run_duration > 200 and r.mean_growth_rate < 9.198 then 'Advanced Low Yield'
            when r.run_duration > 200 and r.mean_growth_rate > 10.234 then 'Advanced Low Yield'
            when safe_divide(r.run_duration, r.requested_run_duration) > 0.95 and r.clean_vol_240_210_cm3 < 92.98 then 'Basic Low Yield'
            else 'NA' end            
    else 'NA'
    END as reason,
--   Display Reason Explanation
        case 
    -- Gen7.5 Gem
      when r.Gen = 'Gen7.5' and r.loading_purpose = 'Production'
        and r.mean_incident_power between 40 and 42.8 
        and r.block_with_any_clean_vol - r.block_with_clean_vol_240 <= 3
        and r.seed_count - r.block_with_any_clean_vol <= 3
      then
        case 
            when 
              safe_divide(r.run_duration, r.requested_run_duration) > 0.95 
              and r.clean_vol_240_210_cm3 < 74.3 and pr.clean_vol_240_210_cm3 < 74.3 
              and timestamp_diff( r.end_time,pr.end_time, day) < 60 then 'Advanced Low Yield -- low clean vol'
            when r.mean_incident_power > 41 and r.run_duration > 200 and r.mean_growth_rate < 9.486 then 'Advanced Low Yield - low growth rate'
            when r.run_duration > 200 and r.mean_growth_rate > 10.2967 then 'Advanced Low Yield - high growth rate'
            when safe_divide(r.run_duration, r.requested_run_duration) > 0.95 and r.clean_vol_240_210_cm3 < 74.3 then 'Basic Low Yield - low clean vol'
            else 'NA' end
      --Gen8 Seed
      when r.Gen = 'Gen8' and r.loading_purpose = 'Seed'
        and r.mean_incident_power between 48.2 and 49.9
        and r.seed_count - r.block_with_post_product <= 3
      then
        case 
            when 
              safe_divide(r.run_duration, r.requested_run_duration) > 0.95 
              and r.seed_yield_p < 0.786 and pr.seed_yield_p < 0.786
              and timestamp_diff( r.end_time,pr.end_time, day) < 60 then 'Advanced Low Yield'
            when r.mean_incident_power > 48 and r.run_duration > 200 and r.mean_growth_rate < 8.824 then 'Advanced Low Yield - low growth rate'
            when r.run_duration > 200 and r.mean_growth_rate > 9.467 then 'Advanced Low Yield - high growth rate'
            when safe_divide(r.run_duration, r.requested_run_duration) > 0.95 and r.seed_yield_p < 0.786 then 'Basic Low Yield'
            else 'NA' end
      -- Gen8 Gem
      when r.Gen = 'Gen8' and r.loading_purpose = 'Production'
        and r.mean_incident_power between 48.2 and 49.9
        and r.block_with_any_clean_vol - r.block_with_clean_vol_240 <= 3
        and r.seed_count - r.block_with_any_clean_vol <= 3
      then 
        case 
            when 
              safe_divide(r.run_duration, r.requested_run_duration) > 0.95 
              and r.clean_vol_240_210_cm3 < 92.98 and pr.clean_vol_240_210_cm3 < 92.98
              and timestamp_diff( r.end_time,pr.end_time, day) < 60 then 'Advanced Low Yield - low clean vol'
            when r.mean_incident_power > 48.5 and r.run_duration > 200 and r.mean_growth_rate < 9.198 then 'Advanced Low Yield - low growth rate'
            when r.run_duration > 200 and r.mean_growth_rate > 10.234 then 'Advanced Low Yield - high growth rate'
            when safe_divide(r.run_duration, r.requested_run_duration) > 0.95 and r.clean_vol_240_210_cm3 < 92.98 then 'Basic Low Yield - low clean vol'
            else 'NA' end            
    else 'NA'
    END as reason_explanation,

  
case 
      when r.loading_purpose = 'Production'
        and not (r.block_with_any_clean_vol - r.block_with_clean_vol_240 <= 3
        and r.seed_count - r.block_with_any_clean_vol <= 3)
      then 'Charx Incomplete'
    when r.loading_purpose = 'Seed'
        and not (r.seed_count - r.block_with_post_product <= 3)
      then 'Charx Incomplete'
 end 
 as charx_stage
    from raw_wa r
    left join raw_wa pr on r.prev_id_run_growth = pr.id_run_growth
),
-- ------------------------------Spain runs ------------------------------------------
rs as (
  select id_run_growth, max(ChildAssetName) as RM_name, max(ChildAssetID) as RM_fiix_id
  FROM `df-data-and-ai.fiix_warehouse.fiix_reactor_asset_per_growth`
  WHERE ChildAssetName like 'RM%'
  group by 1
),
 raw_esp as (
  select 
    r.id_run_growth,r.station_name,
    rb.mean_growth_rate,rb.seed_yield_p, rb.block_with_post_product,
    rb.seed_count,rb.block_with_any_clean_vol,rb.block_with_clean_vol_240,rb.clean_vol_240_210_cm3,
    r.end_time,current_timestamp() as current_time, r.run_duration,r.requested_run_duration, r.loading_purpose, r.hardware_platform, r.growing_location_name, 
    r.mean_incident_power, r.mean_chamber_pressure,r.pilot_run,
    stations.fiix_asset_id, rs.RM_fiix_id,
    case when r.hardware_platform like 'Gen7.5%' then 'Gen7.5'
    when r.hardware_platform like 'Gen8%' then 'Gen8'
    else 'NULL' end as Gen,
    lag(id_run_growth) over(partition by rs.RM_fiix_id, r.loading_purpose order by start_time) as prev_id_run_growth
    from `df-data-and-ai.trilliback.bq_growth_runs` r
    left join rs using(id_run_growth)
    join `df-max.raw_dfdb.stations` stations using(id_station)
    left join `df-data-and-ai.trilliback.bq_growth_runs_about_block` rb using(id_run_growth)
    where 1=1 
    and r.run_duration > 200
    and r.growing_location_name = 'Diamond Foundry ESP'
    and r.mean_chamber_pressure between 160 - 0.1 and 160 + 0.1
    and r.pilot_run = 0
),
all_rec_esp as (
  select 
    r.*,pr.clean_vol_240_210_cm3 as prev_clean_vol_240_210_cm3,pr.seed_yield_p as prev_seed_yield_p,
    pr.end_time as prev_growth_end_time,
    timestamp_diff( r.end_time,pr.end_time, day) days_since_last_growth,
    case 
    -- Gen7.5 Gem
      when r.Gen = 'Gen7.5' and r.loading_purpose = 'Production'
        and r.mean_incident_power between 40 and 42.8 
        and r.block_with_any_clean_vol - r.block_with_clean_vol_240 <= 3
        and r.seed_count - r.block_with_any_clean_vol <= 3
      then
        case 
            when 
              safe_divide(r.run_duration, r.requested_run_duration) > 0.95 
              and r.clean_vol_240_210_cm3 < 74.3 and pr.clean_vol_240_210_cm3 < 74.3 
              and timestamp_diff( r.end_time,pr.end_time, day) < 60 then 'Advanced Low Yield'
            when r.mean_incident_power > 41 and r.run_duration > 200 and r.mean_growth_rate < 9.486 then 'Advanced Low Yield'
            when r.run_duration > 200 and r.mean_growth_rate > 10.2967 then 'Advanced Low Yield'
            when safe_divide(r.run_duration, r.requested_run_duration) > 0.95 and r.clean_vol_240_210_cm3 < 74.3 then 'Basic Low Yield'
            else 'NA' end         
    else 'NA'
    END as reason,
-- Reason Explanation
    case 
    -- Gen7.5 Gem
      when r.Gen = 'Gen7.5' and r.loading_purpose = 'Production'
        and r.mean_incident_power between 40 and 42.8 
        and r.block_with_any_clean_vol - r.block_with_clean_vol_240 <= 3
        and r.seed_count - r.block_with_any_clean_vol <= 3
      then
        case 
            when 
              safe_divide(r.run_duration, r.requested_run_duration) > 0.95 
              and r.clean_vol_240_210_cm3 < 74.3 and pr.clean_vol_240_210_cm3 < 74.3 
              and timestamp_diff( r.end_time,pr.end_time, day) < 60 then 'Advanced Low Yield - low clean vol'
            when r.mean_incident_power > 41 and r.run_duration > 200 and r.mean_growth_rate < 9.486 then 'Advanced Low Yield - low growth rate'
            when r.run_duration > 200 and r.mean_growth_rate > 10.2967 then 'Advanced Low Yield - high growth rate'
            when safe_divide(r.run_duration, r.requested_run_duration) > 0.95 and r.clean_vol_240_210_cm3 < 74.3 then 'Basic Low Yield - low clean vol'
            else 'NA' end         
    else 'NA'
    END as reason_explanation,

case 
      when r.loading_purpose = 'Production'
        and not (r.block_with_any_clean_vol - r.block_with_clean_vol_240 <= 3
        and r.seed_count - r.block_with_any_clean_vol <= 3)
      then 'Charx Incomplete'
    -- when r.loading_purpose = 'Seed'
    --     and not (r.seed_count - r.block_with_post_product <= 3)
    --   then 'Charx Incomplete'
 end 
 as charx_stage
    from raw_esp r
    left join raw_esp pr on r.prev_id_run_growth = pr.id_run_growth
)
select * from all_rec_wa
UNION ALL
select * from all_rec_esp

-- where end_time > current_timestamp() - interval 3 day
