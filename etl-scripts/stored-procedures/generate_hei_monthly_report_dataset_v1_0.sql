DELIMITER $$
CREATE PROCEDURE `generate_hei_monthly_report_dataset_v1_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int, IN start_date varchar(50))
BEGIN

			set @start = now();
			set @table_version = "hei_monthly_report_dataset_v1.0";

			set @sep = " ## ";
			set @lab_encounter_type = 99999;
			set @death_encounter_type = 31;
            
CREATE TABLE IF NOT EXISTS hei_monthly_report_dataset (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
    elastic_id BIGINT,
    endDate DATE,
    encounter_id INT,
    encounter_type INT,
    person_id INT,
    person_uuid VARCHAR(100),
    birthdate DATE,
    death_date DATE,
    age_in_months DOUBLE,
    gender VARCHAR(1),
    encounter_date DATE,
    rtc_date DATE,
    location_id MEDIUMINT,
    location_uuid VARCHAR(100),
    clinic VARCHAR(250),
    enrolled_date DATE,
    enrolled_this_month BOOLEAN,
    enrollment_location_id mediumint,
    transfer_in_this_month tinyint,
	transfer_in_location_id mediumint,
	transfer_in_date date,
	transfer_out_this_month tinyint,
	transfer_out_location_id mediumint,
	transfer_out_date date,
    initial_pcr_this_month SMALLINT,
    hiv_dna_pcr_date DATETIME,
    second_pcr_this_month smallint,
    third_pcr_this_month smallint,
    initial_antibody_screening_this_month SMALLINT,
    second_antibody_screening_this_month smallint,
    vl_1 INT,
    vl_1_date DATETIME,
    vl_2 INT,
    vl_2_date DATETIME,
    patient_care_status INT,
    retention_status VARCHAR(250),
    hei_outcome_this_month INT,
    infant_feeding_method_this_month INT,
    infection_status_this_month INT,
    mother_alive_this_month SMALLINT,
    mother_alive_on_child_enrollment SMALLINT,
    age INT,
	active_and_eligible_for_ovc tinyint,
	inactive_and_eligible_for_ovc tinyint,
	enrolled_in_ovc_this_month tinyint,
	ovc_non_enrolment_declined tinyint,
	ovc_non_enrolment_out_of_catchment_area tinyint,
    PRIMARY KEY elastic_id (elastic_id),
    INDEX person_enc_date (person_id , encounter_date),
    INDEX person_report_date (person_id , endDate),
    INDEX endDate_location_id (endDate , location_id)
);
            
			if (query_type = "build") then
					select "BUILDING.......................";
					set @queue_table = concat("hei_monthly_report_dataset_build_queue_",queue_number);                    
					SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,'(person_id int primary key) (select * from hei_monthly_report_dataset_build_queue limit ', queue_size, ');'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;
                    
					SET @dyn_sql=CONCAT('delete t1 from hei_monthly_report_dataset_build_queue t1 join ',@queue_table, ' t2 using (person_id)'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
			end if;

			
            if (query_type = "sync") then
					set @queue_table = "hei_monthly_report_dataset_sync_queue";
					CREATE TABLE IF NOT EXISTS hei_monthly_report_dataset_sync_queue (
						person_id INT PRIMARY KEY
					);

					SELECT
						@last_update:=(SELECT
								MAX(date_updated)
							FROM
								etl.flat_log
							WHERE
								table_name = @table_version);

					replace into hei_monthly_report_dataset_sync_queue
                    (select distinct person_id from flat_hei_summary where date_created >= @last_update);
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
            
            SELECT NOW();
            SELECT @person_ids_count;
            SELECT @queue_table;

            
            SET @dyn_sql=CONCAT('delete t1 from hei_monthly_report_dataset t1 join ',@queue_table,' t2 using (person_id);'); 
			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1;  
            
            set @total_time=0;
			set @cycle_number = 0;
                    
			while @person_ids_count > 0 do
			
				set @loop_start_time = now();                        
		
				drop temporary table if exists hei_monthly_report_dataset_build_queue__0;
                create temporary table hei_monthly_report_dataset_build_queue__0 (person_id int primary key);                

                SET @dyn_sql=CONCAT('insert into hei_monthly_report_dataset_build_queue__0 (select * from ',@queue_table,' limit ',cycle_size,');'); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;
                
                set @infant_feeding_six_months := null;
                set @infant_feeding_twelve_months := null;
                set @infant_feeding_this_month := null;
                set @initial_pcr_this_month := null;
                set @hiv_dna_pcr_date:= null;
                set @infection_status_this_month:= null;
                set @retention_status:= null;
                
                drop temporary table if exists hei_monthly_report_dataset_0;
				create temporary table hei_monthly_report_dataset_0
				(select 
					concat(date_format(t1.endDate,"%Y%m"),t2.person_id) as elastic_id,
					t1.endDate,
                    t2.encounter_id,
                    t2.encounter_type,
					t2.person_id,
                    t3.uuid as person_uuid,
					date(birthdate) as birthdate,
                    t3.death_date,
					round(timestampdiff(month,birthdate,t1.endDate),2) as age_in_months,
					t3.gender,
					date(t2.encounter_datetime) as encounter_date,
                    t2.location_id,
                    t2.clinic,
                    t4.uuid as location_uuid,
                    date(t2.date_enrolled) as enrolled_date,
                    t2.rtc_date,
                    case
						when t2.date_enrolled between date_format(t1.endDate,"%Y-%m-01")  and t1.endDate then 1
						else null
					end as enrolled_this_month,
                    t2.enrollment_location_id,
                    case
						when t2.transfer_in_date between date_format(t1.endDate,"%Y-%m-01")  and t1.endDate then 1
						else 0
					end as transfer_in_this_month,
					t2.transfer_in_location_id,
					t2.transfer_in_date,


					case
						when t2.transfer_out_date between date_format(t1.endDate,"%Y-%m-01")  and t1.endDate then 1
						else 0
					end as transfer_out_this_month,
					
					t2.transfer_out_location_id,
					t2.transfer_out_date,
                    case
                        when (t6.hiv_dna_pcr_1_date between date_format(t1.endDate,"%Y-%m-01")  and t1.endDate) then 1
                        else null
                    end as initial_pcr_this_month,
                    t2.hiv_dna_pcr_date,
                    case
                        when (t6.hiv_dna_pcr_2_date between date_format(t1.endDate,"%Y-%m-01")  and t1.endDate)  then 1
                        else null
                    end as second_pcr_this_month,
                    case
                        when (t6.hiv_dna_pcr_3_date between date_format(t1.endDate,"%Y-%m-01")  and t1.endDate) then 1
                        else null
                    end as third_pcr_this_month,
                    case
                        when (t2.antibody_screen_1_date between date_format(t1.endDate,"%Y-%m-01")  and t1.endDate) then 1
                        else null
                    end as initial_antibody_screening_this_month,
                    case
                        when (t2.antibody_screen_2_date between date_format(t1.endDate,"%Y-%m-01")  and t1.endDate)  then 1
                        else null
                    end as second_antibody_screening_this_month,
                    t2.vl_1,
					t2.vl_1_date,
					t2.vl_2,
					t2.vl_2_date,
                    t2.patient_care_status,
                    case
						when date_format(t1.endDate, "%Y-%m-01") > t2.death_date then @retention_status := "dead"
                        when date_format(t1.endDate, "%Y-%m-01") > t2.transfer_out_date then @retention_status := "transfer_out"
						when timestampdiff(day,if(t2.rtc_date,t2.rtc_date,date_add(t2.encounter_datetime, interval 28 day)),t1.endDate) <= 28 then @retention_status := "active"
						when timestampdiff(day,if(t2.rtc_date,t2.rtc_date,date_add(t2.encounter_datetime, interval 28 day)),t1.endDate) between 29 and 90 then @retention_status := "defaulter"
						when timestampdiff(day,if(t2.rtc_date,t2.rtc_date,date_add(t2.encounter_datetime, interval 28 day)),t1.endDate) > 90 then @retention_status := "ltfu"
						else @retention_status := "unknown"
					end as retention_status,
                    t2.hei_outcome as hei_outcome_this_month,
					t2.infant_feeding_method as infant_feeding_method_this_month,
                    case
                      when t2.hiv_dna_pcr_1_date IS NOT NULL then
                        case
							when t2.hiv_dna_pcr_1 = '664' then @infection_status_this_month:= 1
							when t2.hiv_dna_pcr_1 = '703' then @infection_status_this_month:= 2
                            when t2.hiv_dna_pcr_1 = '1118' then @infection_status_this_month:= 3
                            when t2.hiv_dna_pcr_1 = '1138' then @infection_status_this_month:= 4
                            when t2.hiv_dna_pcr_1 = '1304' then @infection_status_this_month:= 5
                            else @infection_status_this_month
						end
                        
                      else @infection_status_this_month
                    
                    end as infection_status_this_month,
                    t2.mother_alive as mother_alive_this_month,
                    t2.mother_alive_on_child_enrollment as mother_alive_on_child_enrollment,
                    case
						when timestampdiff(year,birthdate,t1.endDate) > 0 then @age := round(timestampdiff(year,birthdate,t1.endDate),0)
						else @age :=round(timestampdiff(month,birthdate,t1.endDate)/12,2)
					end as age,
					case
						when @retention_status="active"
                            AND @age<=15
                            then 1
						else 0
					end as active_and_eligible_for_ovc,

					case
						when @retention_status="defaulter" OR @retention_status="ltfu"
                            AND @age<=15
                            then  1
						else 0
					end as inactive_and_eligible_for_ovc,

                    case when t4.in_ovc_this_month
						then 1
                        else 0
					end  as enrolled_in_ovc_this_month,

					case
						when t2.ovc_non_enrolment_reason = 1504
                            then 1
						else 0
					end as ovc_non_enrolment_declined,

					case
						when t2.ovc_non_enrolment_reason = 6834
                            then 1
						else 0
					end as ovc_non_enrolment_out_of_catchment_area
					
					from etl.dates t1
					join etl.flat_hei_summary t2 
					join amrs.person t3 on (t3.person_id = t2.person_id)
                    left join amrs.location t4 on (t4.location_id = t2.person_id)
					join etl.hei_monthly_report_dataset_build_queue__0 t5 ON (t5.person_id=t2.person_id)
					left join etl.flat_hei_summary t6 on (t6.person_id = t2.person_id AND t6.next_encounter_datetime_hiv IS NULL)
                    left join patient_monthly_enrollment t4 on (t2.person_id = t4.person_id and t1.endDate = t4.endDate)

					where  
							DATE(t2.encounter_datetime) <= t1.endDate
                            and (t2.next_clinical_datetime_hiv is null or (t2.next_clinical_datetime_hiv > t1.endDate ))
							and (t2.is_clinical_encounter = 1)
							and t1.endDate between start_date and date_add(now(),interval 1 month)
					order by t2.person_id, t1.endDate
				);
                
                

				set @prev_id = null;
				set @cur_id = null;
				set @cur_status = null;
				set @prev_status = null;

				drop temporary table if exists hei_monthly_report_dataset_1;
				create temporary table hei_monthly_report_dataset_1
				(select
					*,
					@prev_id := @cur_id as prev_id,
					@cur_id := person_id as cur_id
						
					from hei_monthly_report_dataset_0
					order by person_id, endDate desc
				);

				alter table hei_monthly_report_dataset_1 drop prev_id, drop cur_id;

				set @prev_id = null;
				set @cur_id = null;
				set @cur_status = null;
				set @prev_status = null;
                set @cur_arv_meds = null;
				drop temporary table if exists hei_monthly_report_dataset_2;
				create temporary table hei_monthly_report_dataset_2
				(select
					*,
					@prev_id := @cur_id as prev_id,
					@cur_id := person_id as cur_id
					
					from hei_monthly_report_dataset_1
					order by person_id, endDate
				);
                                
SELECT NOW();
				SELECT 
    COUNT(*) AS num_rows_to_be_inserted
FROM
    hei_monthly_report_dataset_2;
	
				#add data to table
				replace into hei_monthly_report_dataset
				(select
					null, #date_created will be automatically set or updated
					elastic_id,
				endDate,
                encounter_id,
                encounter_type,
				person_id,
                person_uuid,
				birthdate,
                death_date,
				age_in_months,
				gender,
				encounter_date,
                rtc_date,
                location_id,
                location_uuid,
                clinic,
                enrolled_date,
                enrolled_this_month,
                enrollment_location_id,
                transfer_in_this_month,
	            transfer_in_location_id,
	            transfer_in_date,
	            transfer_out_this_month,
				transfer_out_location_id,
				transfer_out_date,
                initial_pcr_this_month,
                hiv_dna_pcr_date,
                second_pcr_this_month,
                third_pcr_this_month,
                initial_antibody_screening_this_month,
                second_antibody_screening_this_month,
                vl_1,
				vl_1_date,
				vl_2,
				vl_2_date,
                patient_care_status,
                retention_status,
                hei_outcome_this_month,
                infant_feeding_method_this_month,
                infection_status_this_month,
                mother_alive_this_month,
                mother_alive_on_child_enrollment,
                age,
				active_and_eligible_for_ovc,
				inactive_and_eligible_for_ovc,
				enrolled_in_ovc_this_month,
				ovc_non_enrolment_declined,
				ovc_non_enrolment_out_of_catchment_area
					from hei_monthly_report_dataset_2
				);
                

				SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join hei_monthly_report_dataset_build_queue__0 t2 using (person_id);'); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;  
				

				SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';'); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;  
                
                #select @person_ids_count as num_remaining;
                
				set @cycle_length = timestampdiff(second,@loop_start_time,now());
				#select concat('Cycle time: ',@cycle_length,' seconds');                    
				set @total_time = @total_time + @cycle_length;
				set @cycle_number = @cycle_number + 1;
				
				#select ceil(@person_ids_count / cycle_size) as remaining_cycles;
				set @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);
				SELECT 
    COUNT(*)
INTO @num_in_hmrd FROM
    hei_monthly_report_dataset;
                
SELECT 
    @num_iqn_hmrd AS num_in_hmrd,
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
SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');

        END$$
DELIMITER ;