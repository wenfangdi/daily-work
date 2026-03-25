-- insert new report flow

INSERT INTO  `df-data-and-ai.reporting_buffer.WS_throughput_table`
select '1x1-BE-Flow-2MS' as flow_name_in, step_name, step_name_next, Segment_name,unit, step_order, master_product
FROM UNNEST([
  STRUCT(
    '702038' AS step_name,'702040' AS step_name_next,'Block Prep' AS segment_name,'Block' AS unit,2 AS step_order,'1x1' AS master_product),
  STRUCT('702040','704020','RIM','Block',3,'1x1'),
  STRUCT('704041','704060','Singulation|parent','Block',4,'1x1'),
  STRUCT('704042','710020','Singulation|child','Plate',5,'1x1')
]);

INSERT INTO `df-data-and-ai.reporting_buffer.WS_cycle_time_table`
select step_name, segment_name, row_number() over() + 300 as pos, unit, '1x1-BE-Flow-2MS' as flow_name_in, 
from UNNEST([
  STRUCT(
    '702005' AS step_name,'Block Prep' AS segment_name,'Block' AS unit),
  STRUCT('702020','Block Prep','Block'),
  STRUCT('702030','Block Prep','Block'),
  STRUCT('702033','Block Prep','Block'),
  STRUCT('702035','Block Prep','Block'),
  STRUCT('702038','Block Prep','Block'),

  STRUCT('702040','RIM','Block'),

  STRUCT('704020','Singulation|parent','Block'),
  STRUCT('704035','Singulation|parent','Block'),
  STRUCT('704039','Singulation|parent','Block'),
  STRUCT('704041','Singulation|parent','Block'),

  STRUCT('704060','Singulation|child','Plate'),
  STRUCT('704066','Singulation|child','Plate'),
  STRUCT('704070','Singulation|child','Plate'),
  STRUCT('710010','Singulation|child','Plate'),
  STRUCT('710050','Singulation|child','Plate'),
  STRUCT('704042','Singulation|child','Plate'),


  STRUCT('710020','Finish','Plate'),
  STRUCT('710030','Finish','Plate'),
  STRUCT('710035','Finish','Plate'),
  STRUCT('711010','Finish','Plate'),
  STRUCT('712030','Finish','Plate')
])
