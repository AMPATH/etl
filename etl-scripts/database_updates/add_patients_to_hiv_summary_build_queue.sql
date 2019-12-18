/*
set @last_update = null;
select max(date_updated) into @last_update from etl.flat_log where table_name=@table_version; */
drop table if exists flat_hiv_summary_build_queue;
create table flat_hiv_summary_build_queue (person_id int, primary key (person_id));
select @_last_update:= {{ params.date }};
replace into flat_hiv_summary_build_queue
(select distinct patient_id 
    from amrs.encounter
    where date_changed > @_last_update
);


replace into flat_hiv_summary_build_queue
(select distinct person_id 
    from etl.flat_obs
    where max_date_created > @_last_update


);


replace into flat_hiv_summary_build_queue
(select distinct person_id
    from etl.flat_lab_obs
    where max_date_created > @_last_update
);

replace into flat_hiv_summary_build_queue
(select distinct person_id
    from etl.flat_orders
    where max_date_created > @_last_update
);
                        
