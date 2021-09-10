DELIMITER $$
CREATE  PROCEDURE `find_missing_hiv_summary_records`()
BEGIN
                    set @primary_table := "flat_hiv_summary_missing_records";
                    set @start = now();
                    set @table_version = "flat_hiv_summary_missing_recordsv1.0";

                    set session sort_buffer_size=512000000;
                    set @last_date_created = (select max(date_created) from etl.flat_hiv_summary_missing_records);

                    
                    
CREATE TABLE IF NOT EXISTS flat_hiv_summary_missing_records (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    person_id INT,
    encounter_id INT,
    encounter_datetime DATETIME,
    encounter_type INT,
    location_id INT,
    PRIMARY KEY encounter_id (encounter_id),
    INDEX person_date (person_id , encounter_datetime),
    INDEX encounter_type (encounter_type),
    INDEX date_created (date_created)
);

SELECT CONCAT('Deleting data from etl.flat_hiv_summary_missing_records');



delete from etl.flat_hiv_summary_missing_records;
                    
                    
								
                        replace into etl.flat_hiv_summary_missing_records
                        (select
                            null as `date_created`,
                            t1.patient_id as `person_id`,
                            t1.encounter_id as `encounter_id`,
                            t1.encounter_datetime as `encounter_datetime`,
                            t1.encounter_type as `encounter_type`,
                            t1.location_id as `location_id`
                            from 
                            amrs.encounter t1
                            left join etl.flat_obs f on (f.encounter_id = t1.encounter_id)
						    left join etl.flat_hiv_summary_v15b fhs on (t1.encounter_id = fhs.encounter_id)
                            left join amrs.person_attribute t2 on (t2.person_id = t1.patient_id AND t2.person_attribute_type_id=28 AND t2.voided = 0)
                            where t1.encounter_type in (1,2,3,4,10,14,15,17,19,22,23,26,32,33,43,47,21,105,106,110,111,112,113,114,116,117,120,127,128,129,138,140,153,154,158, 161,162,163,186,212,214)
							AND NOT f.obs regexp "!!5303=(822|664|1067)!!"  
							AND NOT f.obs regexp "!!9082=9036!!"
                            AND fhs.encounter_id IS NULL
                            AND (t2.value = "false" OR t2.value IS NULL)
                        );
                        
						 SELECT CONCAT('Copying patients to sync queue');
                        
                         replace into etl.flat_hiv_summary_sync_queue(
                         select distinct person_id from etl.flat_hiv_summary_missing_records
                         );
                         
                         
                         

                     
                
                 set @end = now();
                 insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');

        END$$
DELIMITER ;
