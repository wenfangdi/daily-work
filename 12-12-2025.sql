--For Bonnie
insert into `df-data-and-ai.wip_tracker.SOP_doc` (id, timestamp, fe_be, department, team_center_doc_id,status,priority, due_date, link_to_working_copy,file_name, notes)
select 
cast(string_field_0 as int64) as id, 
current_timestamp() - interval 1 day + interval cast(string_field_0 as int64) second as timestamp,
case when string_field_2 like 'Back End %' then 'Back End'
else string_field_2 end
 as fe_be,
string_field_3 as department,  
string_field_4 as team_center_doc_id,
string_field_5 as status,
string_field_6 as priority,
TIMESTAMP(
  SAFE.PARSE_DATE('%m/%d/%y', string_field_7),
  'America/Los_Angeles'
) AS due_date,
string_field_12 as link_to_working_copy,
string_field_13 as file_name,
string_field_18 as notes
from `df-data-and-ai.temp.bonnie_sheet`
where string_field_0 != 'Item'
and string_field_3 is not null


insert into wip_tracker.SOP_reviewer (id, id_user, Role, active)
select  id,users.id_user, 'Owner', true
from (
  select cast(string_field_0 as int64) as id,
  case 
    when string_field_16 = 'Anthony Bolden' then 'Tony Bolden' 
    when string_field_16 = 'Javier Campos Benitez' then 'Javier Benitez'
    when string_field_16 = 'John Pellolio' then 'Johnathan Pellolio'
    when string_field_16 = 'Andrew Stem' then 'Andy Stem'
    when string_field_16 = 'Pablo Cortez' then 'Pablo Cortez Gomez'
    when string_field_16 = 'Chris Fleming' then 'Christopher Fleming'
    when string_field_16 = 'Fred Joucken' then 'Frédéric Joucken'
    when string_field_16 = 'Lada Pryimak' then 'Vladyslava Pryimak'
  else string_field_16 end as string_field_16
  from 
  `df-data-and-ai.temp.bonnie_sheet` 
  where string_field_0 != 'Item'
and string_field_3 is not null
and string_field_16 is not null
and string_field_16 != '?'
) s
left join `df-max.raw_dfdb.users` users on lower(s.string_field_16) = lower(concat(users.first_name, ' ', users.last_name))
where id_user is not null

INSERT INTO wip_tracker.SOP_loc (id, Loc)
select  id,raw.Loc, from 
(
  select Part_Number, Site, Loc from `df-data-and-ai.temp.released_doc`
JOIN UNNEST(SPLIT(Site, ',')) AS loc
)
raw
left join `df-data-and-ai.wip_tracker.SOP_doc` doc on raw.Part_Number = doc.team_center_doc_id
