DELIMITER $$
CREATE PROCEDURE `generate_monthly_diabetes_and_hypertention_report`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
set @start = now();
			set @table_version = "monthly_diabetes_and_hypertention_report_v1.0";
			set @last_date_created = (select max(date_created) from etl.flat_diabetes_and_hypertention_summary);

DROP TABLE IF EXISTS monthly_diabetes_and_hypertention_report;          
CREATE TABLE IF NOT EXISTS monthly_diabetes_and_hypertention_report (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
    diabetes_and_hypertention_id BIGINT,
    endDate DATE,
    encounter_id INT,
    person_id INT,
    person_uuid VARCHAR(100),
    birthdate DATE,
    gender VARCHAR(1),
    encounter_date DATE,
    location_id INT,
    location_uuid VARCHAR(100),
    facility VARCHAR(200),
    mfl_code INT,
    county VARCHAR(200),
    sub_county VARCHAR(200),
    encounter_type INT,
    cumulative_in_care SMALLINT,
    newly_diagnosed_diabetes_this_month smallint,
    ncd_visit_type int,
    diabetes_status int,
    diabetes_mellitus_type int,
    cumulative_htn_patient SMALLINT,
    has_htn smallint,
    newly_diagnosed_htn_this_month smallint,
    htn_type INT,
    htn_stage INT,
    is_co_morbid SMALLINT,
    newly_diagnosed_co_morbid_this_month smallint,
    co_morbid_type INT,
    stroke_diagnosis SMALLINT,
    ischemic_heart_disease_diagnosis SMALLINT,
    heart_failure_diagnosis SMALLINT,
    has_neuropathies SMALLINT,
    screened_for_diabetic_foot_this_month SMALLINT,
    has_diabetic_foot SMALLINT,
    amputation_due_to_diabetic_foot SMALLINT,
    screened_for_tb_this_month SMALLINT,
    tb_screening_status INT,
    covered_by_shif SMALLINT,
    on_insulin_this_month smallint,
    on_ogla_meds_this_month smallint,
    done_hba1c_this_month smallint,
    hba1c decimal,
    hba1c_date DATE,
    on_exercise smallint,
    on_diet smallint,
    PRIMARY KEY diabetes_and_hypertention_id (diabetes_and_hypertention_id),
    INDEX person_enc_date (person_id , encounter_date),
    INDEX person_report_date (person_id , endDate),
    INDEX endDate_location_id (endDate , location_id),
    INDEX date_created_index (date_created)
);
            

			if (query_type = "build") then
					select "BUILDING.......................";
					set @queue_table = concat("monthly_diabetes_and_hypertention_report_build_queue_",queue_number);                    
					SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,'(person_id int primary key) (select * from monthly_diabetes_and_hypertention_report_build_queue limit ', queue_size, ');'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;

					SET @dyn_sql=CONCAT('delete t1 from monthly_diabetes_and_hypertention_report_build_queue t1 join ',@queue_table, ' t2 using (person_id)'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
			end if;

			
            if (query_type = "sync") then
					set @queue_table = "monthly_diabetes_and_hypertention_report_sync_queue";
CREATE TABLE IF NOT EXISTS monthly_diabetes_and_hypertention_report_sync_queue (
    person_id INT PRIMARY KEY
);
                    
					SELECT 
    @last_update:=(SELECT 
            MAX(date_updated)
        FROM
            etl.flat_log
        WHERE
            table_name = @table_version);

					replace into monthly_diabetes_and_hypertention_report_sync_queue
                    (select distinct person_id from etl.flat_diabetes_and_hypertention_summary where date_created >= @last_update);
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
            
            
			SET @dyn_sql=CONCAT('delete t1 from monthly_diabetes_and_hypertention_report t1 join ',@queue_table,' t2 using (person_id);'); 
 			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1;  
            
            set @total_time=0;
			set @cycle_number = 0;
                    
			while @person_ids_count > 0 do
			
				set @loop_start_time = now();                        
			
				drop temporary table if exists monthly_diabetes_and_hypertention_report_build_queue__0;
                create temporary table monthly_diabetes_and_hypertention_report_build_queue__0 (person_id int primary key);                

                SET @dyn_sql=CONCAT('insert into monthly_diabetes_and_hypertention_report_build_queue__0 (select * from ',@queue_table,' limit ',cycle_size,');'); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;
                
                
                set @age =null;
                set @status = null;
                
                drop  table if exists monthly_diabetes_and_hypertention_report_0;
				create  table monthly_diabetes_and_hypertention_report_0
				(select 
					concat(date_format(t1.endDate,"%Y%m"),q.person_id) as diabetes_and_hypertention_id,
					t1.endDate,
                    fd.encounter_id,
					fd.person_id,
					p.uuid as person_uuid,
					p.birthdate,
					p.gender,
					fd.encounter_datetime as encounter_date,
					fd.location_id,
					l.uuid as location_uuid,
                    l.name as facility,
					la.value_reference as mfl_code,
					l.county_district as county,
					l.state_province as sub_county,
					fd.encounter_type,
                    fd.diabetes_status,
                    1 as cumulative_in_care,
                    case
                      when fd.has_diabetes = 1 AND fd.diagnosis_date between date_format(endDate,"%Y-%m-01")  and endDate then 1
                      else 0
                    end as newly_diagnosed_diabetes_this_month,
                    fd.ncd_visit_type,
                    fd.diabetes_mellitus_type,
                    fd.has_htn as cumulative_htn_patient,
                    fd.has_htn,
                    case
                      when fd.has_htn = 1 AND fd.diagnosis_date between date_format(endDate,"%Y-%m-01")  and endDate then 1
                      else 0
                    end as newly_diagnosed_htn_this_month,
                    fd.htn_type	,
					fd.htn_stage,
					fd.is_co_morbid,
                     case
                      when fd.is_co_morbid = 1 AND fd.co_morbid_type = 1154 AND fd.diagnosis_date between date_format(endDate,"%Y-%m-01")  and endDate then 1
                      else 0
                    end as newly_diagnosed_co_morbid_this_month,
					fd.co_morbid_type,
					fd.stroke_diagnosis,
					fd.ischemic_heart_disease_diagnosis,
					fd.heart_failure_diagnosis,
					fd.has_neuropathies,
                    case
                      when fd.diabetic_foot_screening_date between date_format(endDate,"%Y-%m-01")  and endDate then 1
                      else 0
                    end as screened_for_diabetic_foot_this_month,
					fd.has_diabetic_foot,
					fd.amputation_due_to_diabetic_foot,
                    case
                      when fd.tb_screening_date between date_format(endDate,"%Y-%m-01")  and endDate then 1
                      else 0
                    end as screened_for_tb_this_month,
					fd.tb_screening_status,
					fd.covered_by_shif,
                    fd.on_insulin AS on_insulin_this_month,
                    fd.on_ogla_meds as on_ogla_meds_this_month,
                    case
                     when fd.hba1c_date between date_format(endDate,"%Y-%m-01")  and endDate then 1
					 else 0
                    end as done_hba1c_this_month,
                    fd.hba1c,
                    fd.hba1c_date,
                    fd.on_exercise,
                    fd.on_diet
					from etl.dates t1
					join etl.flat_diabetes_and_hypertention_summary fd
                    join amrs.location l on (l.location_id = fd.location_id)
                    left join amrs.location_attribute la on (la.location_id = l.location_id AND la.attribute_type_id = 2 AND la.voided = 0)
					join amrs.person p on (p.person_id = fd.person_id)
					join etl.monthly_diabetes_and_hypertention_report_build_queue__0 q on (q.person_id = fd.person_id)
					where  
                            fd.encounter_datetime < date_add(t1.endDate, interval 1 day)
							and (fd.next_clinical_encounter_datetime is null or fd.next_clinical_encounter_datetime >= date_add(t1.endDate, interval 1 day) )
							and t1.endDate between '2025-01-01' and date_add(now(),interval 2 year)
                            AND fd.is_clinical_encounter  = 1
					order by person_id, endDate
				);
                
               

SELECT NOW();
				SELECT 
    COUNT(*) AS num_rows_to_be_inserted
FROM
    monthly_diabetes_and_hypertention_report_0;
	
											
				replace into monthly_diabetes_and_hypertention_report											  
				(select
					NULL AS date_created,
                    diabetes_and_hypertention_id,
					endDate,
                    encounter_id,
					person_id,
					person_uuid,
					birthdate,
					gender,
					encounter_date,
					location_id,
					location_uuid,
					facility,
					mfl_code,
					county,
					sub_county,
					encounter_type,
                    cumulative_in_care,
                    newly_diagnosed_diabetes_this_month,
                    ncd_visit_type,
                    diabetes_status,
                    diabetes_mellitus_type,
                    cumulative_htn_patient,
                    has_htn,
                    newly_diagnosed_htn_this_month,
                    htn_type	,
					htn_stage,
					is_co_morbid,
                    newly_diagnosed_co_morbid_this_month,
					co_morbid_type,
					stroke_diagnosis,
					ischemic_heart_disease_diagnosis,
					heart_failure_diagnosis,
					has_neuropathies,
					screened_for_diabetic_foot_this_month,
					has_diabetic_foot,
					amputation_due_to_diabetic_foot,
					screened_for_tb_this_month,
					tb_screening_status,
					covered_by_shif,
                    on_insulin_this_month,
                    on_ogla_meds_this_month,
                    done_hba1c_this_month,
					hba1c,
                    hba1c_date,
					on_exercise,
                    on_diet
					from monthly_diabetes_and_hypertention_report_0 t1
				);
                


				SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join monthly_diabetes_and_hypertention_report_build_queue__0 t2 using (person_id);'); 
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