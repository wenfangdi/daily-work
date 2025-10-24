
-- While adding the custom field to the fiix work order cache
CREATE OR REPLACE TABLE `df-data-and-ai.trilliant_warehouse.fiix_workorders_closed` AS
SELECT
  * EXCEPT(customFieldValues),
  STRUCT(
    customFieldValues.Growth_ID AS Growth_ID,
    customFieldValues.id_tray1 AS id_tray1,
    CAST(NULL AS STRING) AS MFG_GrowthID
  ) AS customFieldValues
FROM `df-data-and-ai.trilliant_warehouse.fiix_workorders_closed`;

-- need to make sure the order of custom field match, bq does not try to order them
