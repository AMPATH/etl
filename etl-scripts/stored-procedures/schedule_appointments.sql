DELIMITER $$
CREATE  PROCEDURE `schedule_appointments`(IN queue_table varchar(100))
BEGIN

set @_last_update = null;
set @table_version = "flat_appointment_v1.1";
set @queue_table = queue_table;

select max(date_updated) into @_last_update from etl.flat_log where table_name=@table_version;

create table if not exists flat_appointment_build_queue (person_id int, primary key (person_id));

SET @dyn_sql=CONCAT('replace into ',@queue_table,' (select distinct person_id from etl.flat_obs where max_date_created > ', @_last_update, ');'); 
PREPARE s1 from @dyn_sql; 
EXECUTE s1; 
DEALLOCATE PREPARE s1;  

SET @dyn_sql=CONCAT('replace into ',@queue_table,' (select distinct person_id from amrs.encounter where date_changed > ', @_last_update, ');'); 
PREPARE s1 from @dyn_sql; 
EXECUTE s1; 
DEALLOCATE PREPARE s1;                       

END$$
DELIMITER ;
