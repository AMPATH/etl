DELIMITER $$
CREATE  PROCEDURE `generate_flat_appointment_sync_queue`()
BEGIN
					select @start := now();
					SELECT @table_version:='flat_appointment_v1.3';
					set @primary_table := "flat_appointment";
                    set @queue_table = "";
                    set @total_rows_written = 0;


					set session sort_buffer_size=512000000;
					SELECT 
    @last_date_created:=(SELECT 
            MAX(max_date_created)
        FROM
            etl.flat_obs);
            
            
        
                            
					
	
					
							select "SYNCING.......................................";
							set @primary_queue_table = "flat_appointment_sync_queue";
                            set @write_table = concat("flat_appointment");
                            set @queue_table = "flat_appointment_sync_queue";
CREATE TABLE IF NOT EXISTS flat_hiv_summary_sync_queue (
    person_id INT PRIMARY KEY
);
                            
							SELECT 
    @last_update:=(SELECT 
            MAX(date_updated)
        FROM
            etl.flat_log
        WHERE
            table_name = @table_version);

							SELECT 
    @last_update:=IF(@last_update IS NULL,
        (SELECT 
                MAX(e.date_created)
            FROM
                amrs.encounter e
                    JOIN
                etl.flat_appointment USING (encounter_id)),
        @last_update);

							SELECT 
    @last_update:=IF(@last_update,
        @last_update,
        '1900-01-01');
        
         SELECT CONCAT('Last updated : ', @last_update);
                            
                            
                            replace into etl.flat_appointment_sync_queue
							(select distinct patient_id
								from amrs.encounter
								where date_changed > @last_update
							);
                                    
							replace into flat_appointment_sync_queue
							(select distinct person_id
								from etl.flat_obs
								where max_date_created > @last_update
							);

					
						
					CREATE TABLE IF NOT EXISTS flat_appointment_queue (
    person_id INT,
    PRIMARY KEY (person_id)
);
                    
					# Remove test patients
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
                    
                    select CONCAT('Patients to sync : ', @person_ids_count);
                    
                				
                
				SELECT @end:=NOW();
                
				insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');

		END$$
DELIMITER ;
