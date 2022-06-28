DELIMITER $$
CREATE  PROCEDURE `generate_monthly_covid_extract_report`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int, IN start_date varchar(50))
BEGIN

			set @start = now();
			set @table_version = "monthly_covid_extract_report_v1.0";
			set @last_date_created = (select max(DateCreated) from etl.flat_covid_extract);

            
CREATE TABLE IF NOT EXISTS monthly_covid_extract_report (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
    covid_extract_id BIGINT,
    endDate DATE,
    encounter_id INT,
    person_id INT,
    person_uuid VARCHAR(100),
    birthdate DATE,
    gender VARCHAR(1),
    encounter_date DATE,
    screened_this_month SMALLINT,
    location_id MEDIUMINT,
    location_uuid VARCHAR(100),
    encounter_type INT,
    received_covid_19_vaccine INT,
    date_given_first_dose DATETIME,
    first_dose_vaccine_administered INT,
    date_given_second_dose DATETIME,
    second_dose_vaccine_administered INT,
    vaccination_status_this_month INT,
    vaccine_verification INT,
    vaccine_verification_second_dose INT,
    booster_given INT,
    booster_vaccine INT,
    booster_dose_date DATETIME,
    booster_dose INT,
    booster_dose_verified INT,
    covid_19_test_result INT,
    covid_19_test_date INT,
    patient_status VARCHAR(100),
    hospital_admission INT,
    admission_unit VARCHAR(100),
    missed_appointment_due_to_covid_19 VARCHAR(100),
    covid_19_positive_since_last_visit VARCHAR(100),
    covid_19_test_date_since_last_visit VARCHAR(100),
    patient_status_since_last_visit VARCHAR(100),
    admission_status_since_last_visit VARCHAR(100),
    admission_start_date DATETIME,
    admission_end_date DATETIME,
    admission_unit_since_last_visit DATETIME,
    supplemental_oxygen_received INT,
    ever_covid_19_positive INT,
    ever_hospitalized smallint,
    tracing_final_outcome INT,
    cause_of_death INT,
    PRIMARY KEY covid_extract_id (covid_extract_id),
    INDEX person_enc_date (person_id , encounter_date),
    INDEX person_report_date (person_id , endDate),
    INDEX endDate_location_id (endDate , location_id),
    INDEX date_created_index (date_created)
);
            

			if (query_type = "build") then
					select "BUILDING.......................";
					set @queue_table = concat("monthly_covid_extract_report_build_queue_",queue_number);                    
					SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,'(person_id int primary key) (select * from monthly_covid_extract_report_build_queue limit ', queue_size, ');'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;

					SET @dyn_sql=CONCAT('delete t1 from monthly_covid_extract_report_build_queue t1 join ',@queue_table, ' t2 using (person_id)'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
			end if;

			
            if (query_type = "sync") then
					set @queue_table = "monthly_covid_extract_report_sync_queue";
CREATE TABLE IF NOT EXISTS monthly_covid_extract_report_sync_queue (
    person_id INT PRIMARY KEY
);
                    
					SELECT 
    @last_update:=(SELECT 
            MAX(date_updated)
        FROM
            etl.flat_log
        WHERE
            table_name = @table_version);

					replace into monthly_covid_extract_report_sync_queue
                    (select distinct person_id from etl.flat_covid_extract where DateCreated >= @last_update);
            end if;
                        

			SET @num_ids := 0;
			SET @dyn_sql=CONCAT('select count(*) into @num_ids from ',@queue_table,';'); 
			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1;          
            
            
            SET @person_ids_count = 0;
			SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1;  
            
            
			SET @dyn_sql=CONCAT('delete t1 from monthly_covid_extract_report t1 join ',@queue_table,' t2 using (person_id);'); 
 			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1;  
            
            set @total_time=0;
			set @cycle_number = 0;
                    
			while @person_ids_count > 0 do
			
				set @loop_start_time = now();                        
			
				drop temporary table if exists monthly_covid_extract_report_build_queue__0;
                create temporary table monthly_covid_extract_report_build_queue__0 (person_id int primary key);                

                SET @dyn_sql=CONCAT('insert into monthly_covid_extract_report_build_queue__0 (select * from ',@queue_table,' limit ',cycle_size,');'); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;
                
                
                set @age =null;
                set @status = null;
                
                drop temporary table if exists monthly_covid_extract_report_0;
				create temporary table monthly_covid_extract_report_0
				(select 
					concat(date_format(t1.endDate,"%Y%m"),q.person_id) as covid_extract_id,
					t1.endDate,
                    ce.encounter_id,
					ce.person_id,
					p.uuid as person_uuid,
					p.birthdate,
					p.gender,
					ce.encounter_datetime as encounter_date,
                    CASE
                     WHEN ce.encounter_datetime between date_format(endDate,"%Y-%m-01")  and endDate then 1
                     ELSE NULL
                    END AS 'screened_this_month',
					ce.location_id,
					l.uuid as location_uuid,
					NULL AS encounter_type,
					ce.received_covid_19_vaccine,
					ce.date_given_first_dose,
					ce.first_dose_vaccine_administered,
					ce.date_given_second_dose,
					ce.second_dose_vaccine_administered,
					ce.vaccination_status AS vaccination_status_this_month,
					ce.vaccine_verification,
					ce.vaccine_verification_second_dose,
					ce.booster_given,
					ce.booster_vaccine,
					ce.booster_dose_date,
					ce.booster_dose,
					ce.booster_dose_verified,
					ce.covid_19_test_result,
					ce.covid_19_test_date,
					ce.patient_status,
					ce.hospital_admission,
					ce.admission_unit,
					ce.missed_appointment_due_to_covid_19,
					ce.covid_19_positive_since_last_visit,
					ce.covid_19_test_date_since_last_visit,
					ce.patient_status_since_last_visit,
					ce.admission_status_since_last_visit,
					ce.admission_start_date,
					ce.admission_end_date,
					ce.admission_unit_since_last_visit,
					ce.supplemental_oxygen_received,
					ce.ever_covid_19_positive,
                    ce.ever_hopsitalized as ever_hospitalized,
					ce.tracing_final_outcome,
					ce.cause_of_death
					
					from etl.dates t1
					join etl.flat_covid_extract ce
                    join amrs.location l on (l.location_id = ce.location_id)
					join amrs.person p on (p.person_id = ce.person_id)
					join etl.monthly_covid_extract_report_build_queue__0 q on (q.person_id = ce.person_id)
					where  
                            ce.encounter_datetime < date_add(t1.endDate, interval 1 day)
							and (ce.next_encounter_datetime is null or ce.next_encounter_datetime >= date_add(t1.endDate, interval 1 day) )
							and t1.endDate between start_date and date_add(now(),interval 2 year)
					order by person_id, endDate
				);
                
               

SELECT NOW();
				SELECT 
    COUNT(*) AS num_rows_to_be_inserted
FROM
    monthly_covid_extract_report_0;
	
											
				replace into monthly_covid_extract_report											  
				(select
					NULL AS date_created,
                    covid_extract_id,
					endDate,
                    encounter_id,
					person_id,
					person_uuid,
					birthdate,
					gender,
					encounter_date,
                    screened_this_month,
					location_id,
					location_uuid,
					encounter_type,
					received_covid_19_vaccine,
					date_given_first_dose,
					first_dose_vaccine_administered,
					date_given_second_dose,
					second_dose_vaccine_administered,
					vaccination_status_this_month,
					vaccine_verification,
					vaccine_verification_second_dose,
					booster_given,
					booster_vaccine,
					booster_dose_date,
					booster_dose,
					booster_dose_verified,
					covid_19_test_result,
					covid_19_test_date,
					patient_status,
					hospital_admission,
					admission_unit,
					missed_appointment_due_to_covid_19,
					covid_19_positive_since_last_visit,
					covid_19_test_date_since_last_visit,
					patient_status_since_last_visit,
					admission_status_since_last_visit,
					admission_start_date,
					admission_end_date,
					admission_unit_since_last_visit,
					supplemental_oxygen_received,
					ever_covid_19_positive,
                    ever_hospitalized,
					tracing_final_outcome,
					cause_of_death
					from monthly_covid_extract_report_0 t1
				);
                


				SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join monthly_covid_extract_report_build_queue__0 t2 using (person_id);'); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;  
				
				SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';'); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;  
                
                
				set @cycle_length = timestampdiff(second,@loop_start_time,now());
				set @total_time = @total_time + @cycle_length;
				set @cycle_number = @cycle_number + 1;
				
				set @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);
                
SELECT 
    @num_in_hmrd AS num_in_hmrd,
    @person_ids_count AS num_remaining,
    @cycle_length AS 'Cycle time (s)',
    CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
    @remaining_time AS 'Est time remaining (min)';


			end while;

			if(query_type = "build") then
					SET @dyn_sql=CONCAT('drop table ',@queue_table,';'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
			end if;            

			set @end = now();
			insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
			SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');

        END$$
DELIMITER ;
