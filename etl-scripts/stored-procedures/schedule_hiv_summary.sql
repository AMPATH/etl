DELIMITER $$
CREATE  PROCEDURE `schedule_hiv_summary`()
BEGIN

set @_last_update = null;
set @table_version = "flat_hiv_summary_v2.18";

select max(date_updated) into @_last_update from etl.flat_log where table_name=@table_version;

create table if not exists flat_hiv_summary_build_queue (person_id int, primary key (person_id));

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
                        

END$$
DELIMITER ;
