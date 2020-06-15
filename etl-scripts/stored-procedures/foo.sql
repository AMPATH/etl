DELIMITER $$
CREATE  PROCEDURE `foo`(IN queue_number int)
BEGIN
					set @queue_table := concat("flat_hiv_summary_build_queue_",queue_number);
                    set @count = -1;
					SET @dyn_sql=CONCAT('create table if not exists ',@queue_table,' (select * from flat_hiv_summary_build_queue limit 100);'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;                     
                    
                    
                    select concat("Total rows in ",@queue_table,": ",@count);                    
		END$$
DELIMITER ;
