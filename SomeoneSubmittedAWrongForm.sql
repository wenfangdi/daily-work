--find the wrong record
select runs.*, id_run_data, id_process_step from runs
-- join process_steps using(id_process)
join run_data using(id_run)
where id_run in (5941584)

--there is also a wrong record here
select `ssl`.*, process_name from station_step_latest `ssl`
join process_steps using(id_process_step)
join processes using(id_process)
where id_station = 1135
and id_process = 1352

-- run in trilliant, fix runs table, it uses id_process
update runs
set id_process = 1259
where id_run = 5500789;

-- also fix station_step_latest table, it uses id_process_step
select * from station_step_latest
where id_station = 1386
and id_process_step = 16709
