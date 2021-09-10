DELIMITER $$
CREATE  PROCEDURE `etl_kill_idle_processes`()
BEGIN

select CONCAT('Get idle processes ..');

replace into etl.flat_etl_idle_process(
SELECT 
id
FROM INFORMATION_SCHEMA.PROCESSLIST 
WHERE `User` = '' 
AND `Command` = 'Sleep'
AND `time` > 1000 
);


SELECT 
    COUNT(*)
INTO @idle_processes FROM
    etl.flat_etl_idle_process;

while @idle_processes > 0 do

set @iddle_id = null;

SET @dyn_sql=CONCAT('select process_id into @iddle_id from etl.flat_etl_idle_process limit 1;'); 
select CONCAT('Get process_id ..',@dyn_sql);
PREPARE s1 from @dyn_sql; 
EXECUTE s1; 
DEALLOCATE PREPARE s1;

SET @dyn_sql=CONCAT('kill ',@iddle_id,' ;'); 
select CONCAT('killing id ..',@dyn_sql);
PREPARE s1 from @dyn_sql; 
EXECUTE s1; 
DEALLOCATE PREPARE s1;

SET @dyn_sql=CONCAT('delete from etl.flat_etl_idle_process where process_id', '=', @iddle_id,';'); 
select CONCAT('delete from process table ..',@dyn_sql);
PREPARE s1 from @dyn_sql; 
EXECUTE s1; 
DEALLOCATE PREPARE s1;

SET @dyn_sql=CONCAT('select COUNT(*) INTO @idle_processes FROM etl.flat_etl_idle_process;'); 
PREPARE s1 from @dyn_sql; 
EXECUTE s1; 
DEALLOCATE PREPARE s1;


end while;
END$$
DELIMITER ;
