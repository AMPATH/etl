use etl;
DELIMITER $$
CREATE  PROCEDURE `generate_flat_hiv_summary_sync_queue`()
BEGIN
                    set @primary_table := "flat_hiv_summary_v15b";
                    set @queue_table = "";
                    set @total_rows_written = 0;
                    
                    set @start = now();
                    set @table_version = "flat_hiv_summary_v2.25";

                    set session sort_buffer_size=512000000;
                   
                    set @last_date_created = (select max(max_date_created) from etl.flat_obs);

				
                            select 'SYNCING..........................................';
                            set @write_table = "flat_hiv_summary_v15b";
                            set @queue_table = "flat_hiv_summary_sync_queue";
CREATE TABLE IF NOT EXISTS flat_hiv_summary_sync_queue (
    person_id INT PRIMARY KEY
);                            
                            


                            set @last_update = null;
SELECT 
    MAX(date_updated)
INTO @last_update FROM
    etl.flat_log
WHERE
    table_name = @table_version;
    
    select concat('Last Updated :', @last_update);

                            replace into flat_hiv_summary_sync_queue
                            (select distinct patient_id
                                from amrs.encounter
                                where date_changed > @last_update
                            );

                            replace into flat_hiv_summary_sync_queue
                            (select distinct person_id
                                from etl.flat_obs
                                where max_date_created > @last_update
                            );

                            replace into flat_hiv_summary_sync_queue
                            (select distinct person_id
                                from etl.flat_lab_obs
                                where max_date_created > @last_update
                            );

                            replace into flat_hiv_summary_sync_queue
                            (select distinct person_id
                                from etl.flat_orders
                                where max_date_created > @last_update
                            );
                            
                            replace into flat_hiv_summary_sync_queue
                            (select person_id from 
                                amrs.person 
                                where date_voided > @last_update);


                            replace into flat_hiv_summary_sync_queue
                            (select person_id from 
                                amrs.person 
                                where date_changed > @last_update);
                                

                      

                    
                    SET @dyn_sql=CONCAT('delete t1 FROM ',@queue_table,' t1
                            join amrs.person_attribute t2 using (person_id)
                            where t2.person_attribute_type_id=28 and value="true" and voided=0');
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;

                    SET @person_ids_count = 0;
                    SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;

                     SELECT @person_ids_count AS 'num patients to sync';


SELECT 
    CONCAT('Average Cycle Length: ',
            @ave_cycle_length,
            ' second(s)');
                
                 set @end = now();
                 insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');

        END$$
DELIMITER ;
