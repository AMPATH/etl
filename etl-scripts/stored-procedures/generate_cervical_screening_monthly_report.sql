DELIMITER $$
CREATE  PROCEDURE `generate_cervical_screening_monthly_report_v1_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int, IN start_date varchar(50))
BEGIN

			set @start = now();
			set @table_version = "cervical_screening_monthly_report_dataset_v1.0";
			set @last_date_created = (select max(date_created) from etl.flat_cervical_cancer_screening_rc);

            

create table if not exists etl.cervical_screening_monthly_report_dataset (
				monthly_screening_id bigint,
				date_created timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
				endDate date,
                encounter_id int,
				person_id int,
				age double,
				encounter_datetime DATETIME,
                visit_this_month tinyint,
				screening_rtc_date date,
				location_id mediumint,
                reasons_for_current_visit TINYINT,
                screened_this_month_for_cervical_cancer tinyint,
                hiv_status TINYINT,
                primary_care_facility INT,
                screening_method INT,
                via_or_via_vili_test_result int,
                pap_smear_test_result int,
                hpv_test_result int,
                treatment_method int,
                other_treatment_method_non_coded INT,
                primary_care_facility_non_ampath VARCHAR(70),
                primary key encounter_id (monthly_screening_id),
				index person_enc_date (person_id, encounter_datetime),
                index person_report_date (person_id, endDate),
                index person_screened_this_month_for_cervical_cancer (person_id, screened_this_month_for_cervical_cancer),
                index endDate_location_id (endDate, location_id),
                index endDate_primary_care_facility (endDate, primary_care_facility),
                index date_created (date_created)
                
            );
            
           

			if (query_type = "build") then
					select "BUILDING.......................";
					set @queue_table = concat("cervical_screening_monthly_report_dataset_build_queue_",queue_number);                    
					SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,'(person_id int primary key) (select * from cervical_screening_monthly_report_dataset_build_queue limit ', queue_size, ');'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;
                    
                    			
					SET @dyn_sql=CONCAT('delete t1 from cervical_screening_monthly_report_dataset_build_queue t1 join ',@queue_table, ' t2 using (person_id)'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
			end if;
            
            if (query_type = "sync") then
					select "SYNCING.......................";
                    SET @last_update := null;

						SELECT 
							MAX(date_updated)
						INTO @last_update FROM
							etl.flat_log
						WHERE
							table_name = @table_version;	
                            
                                SELECT CONCAT('Last Update :', @last_update);
                                
                                SELECT CONCAT('Adding updated flat_cervical_cancer_screening_rc records to sync queue....');

								replace into etl.cervical_screening_monthly_report_dataset_sync_queue
								(select distinct person_id
								  from etl.flat_cervical_cancer_screening_rc
								  where date_created > @last_update
								);
                                
                                SELECT CONCAT('Adding voided patients to sync queue....');
                                replace into etl.cervical_screening_monthly_report_dataset_sync_queue
								(select person_id from 
								  amrs.person 
								  where date_voided > @last_update);
                                  
                                  SELECT CONCAT('Adding voided screeninng encounters to sync queue....');
                                  
                                  replace into etl.cervical_screening_monthly_report_dataset_sync_queue
									(select patient_id from 
									  amrs.encounter
									  where encounter_type in (69,70,147) and date_voided > @last_update);

      
					set @queue_table = concat("cervical_screening_monthly_report_dataset_sync_queue_",queue_number);                    
					SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,'(person_id int) (select * from cervical_screening_monthly_report_dataset_sync_queue limit ', queue_size, ');'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;
                    
                    			
					SET @dyn_sql=CONCAT('delete t1 from cervical_screening_monthly_report_dataset_sync_queue t1 join ',@queue_table, ' t2 using (person_id)'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
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
            
            
			SET @dyn_sql=CONCAT('delete t1 from etl.cervical_screening_monthly_report_dataset t1 join ',@queue_table,' t2 using (person_id);'); 
			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1;  
            
            set @total_time=0;
			set @cycle_number = 0;
                    
			while @person_ids_count > 0 do
			
				set @loop_start_time = now();                        
				drop temporary table if exists cervical_screening_monthly_report_dataset_build_queue__0;
                create temporary table cervical_screening_monthly_report_dataset_build_queue__0 (person_id int primary key);                

                SET @dyn_sql=CONCAT('insert into cervical_screening_monthly_report_dataset_build_queue__0 (select * from ',@queue_table,' limit ',cycle_size,');'); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;
                
                
                set @age =null;
                set @status = null;
                
                drop temporary table if exists cervical_screening_monthly_report_dataset_interim;
				create temporary table cervical_screening_monthly_report_dataset_interim
				(select 
                   concat(DATE_FORMAT(endDate,"%Y%m"),person_id) as monthly_screening_id,
                   null as date_created,
					endDate,
                    encounter_id,
					person_id,
					age,
					encounter_datetime, 
                    if(DATE_FORMAT(encounter_datetime,'%Y-%m-%d') between date_format(endDate,"%Y-%m-01")  and endDate,1,0) as visit_this_month,
					screening_rtc_date,
                    location_id,
                    reasons_for_current_visit,
                    if(DATE_FORMAT(encounter_datetime,'%Y-%m-%d') between date_format(endDate,"%Y-%m-01")  and endDate,1,0) as screened_this_month_for_cervical_cancer,
					hiv_status,
					primary_care_facility,
					screening_method,
					via_or_via_vili_test_result,
                    pap_smear_test_result,
                    hpv_test_result,
					treatment_method,
					other_treatment_method_non_coded,
					primary_care_facility_non_ampath
					from etl.dates t1
					join etl.flat_cervical_cancer_screening_rc t2 
					join etl.cervical_screening_monthly_report_dataset_build_queue__0 t5 using (person_id)
					where  
                            t2.encounter_datetime < date_add(endDate, interval 1 day)
							and (t2.next_clinical_datetime_cervical_cancer_screening is null or t2.next_clinical_datetime_cervical_cancer_screening >= date_add(t1.endDate, interval 1 day) )
							and t1.endDate between start_date and date_add(now(),interval 2 year)
					order by person_id, endDate
				);
                
               

				

                select now();
				select count(*) as num_rows_to_be_inserted from cervical_screening_monthly_report_dataset_interim;
										  
				replace into cervical_screening_monthly_report_dataset											  
				(select
                 *

					from cervical_screening_monthly_report_dataset_interim t1
				);
                


				SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join cervical_screening_monthly_report_dataset_build_queue__0 t2 using (person_id);'); 
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
				
                
                select @num_in_hmrd as num_in_hmrd,
					@person_ids_count as num_remaining, 
					@cycle_length as 'Cycle time (s)', 
                    ceil(@person_ids_count / cycle_size) as remaining_cycles, 
                    @remaining_time as 'Est time remaining (min)';


			end while;

			-- if(query_type = "build") then
					SET @dyn_sql=CONCAT('drop table ',@queue_table,';'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
			-- end if;            

			set @end = now();
			insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
			select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");

        END$$
DELIMITER ;
