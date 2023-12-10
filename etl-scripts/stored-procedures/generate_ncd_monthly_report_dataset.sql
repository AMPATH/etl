DELIMITER $$
CREATE PROCEDURE `generate_ncd_monthly_report_dataset`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int, IN start_date varchar(50))
BEGIN

			set @start = now();
			set @table_version = "ncd_monthly_report_dataset_v1.4";
			set @last_date_created = (select max(date_created) from etl.flat_hiv_summary_v15b);

			set @sep = " ## ";
			set @lab_encounter_type = 99999;
			set @death_encounter_type = 31;
            

create table if not exists ncd_monthly_report_dataset (
				date_created timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
                elastic_id bigint,
				endDate date,
                encounter_id int,
				person_id int,
                person_uuid varchar(100),
				birthdate date,
				age double,
				gender varchar(1),
				location_id int,
				location_uuid varchar(100),
				encounter_date date,
                visit_this_month tinyint,

				is_hypertensive tinyint,
				htn_state tinyint,

				is_diabetic tinyint,
				dm_state tinyint,

				has_mhd tinyint,
				is_depressive_mhd tinyint,
				is_anxiety_mhd tinyint,
				is_bipolar_and_related_mhd tinyint,
				is_personality_mhd tinyint,
				is_feeding_and_eating_mhd tinyint,
				is_ocd_mhd tinyint,
				
				has_kd tinyint,
				is_ckd tinyint,
				ckd_stage int,

				has_cvd tinyint,
				is_heart_failure_cvd tinyint,
				is_myocardinal_infarction tinyint,

				has_neurological_disorder tinyint,
				has_stroke tinyint,
				is_stroke_haemorrhagic tinyint,
				is_stroke_ischaemic tinyint,
				has_seizure tinyint,
				has_epilepsy tinyint,
				has_convulsive_disorder tinyint,

				has_rheumatologic_disorder tinyint,
				has_arthritis tinyint,
				has_SLE tinyint,


                primary key elastic_id (elastic_id),
				index person_enc_date (person_id, encounter_date),
                index person_report_date (person_id, endDate),
                index endDate_location_id (endDate, location_id),
                index date_created (date_created),
                index status_change (location_id, endDate, status, prev_status)
                
            );
            
            #create table if not exists ncd_monthly_report_dataset_build_queue (person_id int primary key);
			#replace into ncd_monthly_report_dataset_build_queue
			#(select distinct person_id from flat_hiv_summary_v15);

			if (query_type = "build") then
					select "BUILDING.......................";
					set @queue_table = concat("ncd_monthly_report_dataset_build_queue_",queue_number);                    
#set @queue_table = concat("ncd_monthly_report_dataset_build_queue_1");                    				
					#create  table if not exists @queue_table (person_id int, primary key (person_id));
					#SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ncd_monthly_report_dataset_build_queue limit 1000);'); 
					SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,'(person_id int primary key) (select * from ncd_monthly_report_dataset_build_queue limit ', queue_size, ');'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;
                    
                    			#delete t1 from ncd_monthly_report_dataset_build_queue t1 join @queue_table t2 using (person_id)
					SET @dyn_sql=CONCAT('delete t1 from ncd_monthly_report_dataset_build_queue t1 join ',@queue_table, ' t2 using (person_id)'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
			end if;

			
            if (query_type = "sync") then
					set @queue_table = "ncd_monthly_report_dataset_sync_queue";                                
                    create table if not exists ncd_monthly_report_dataset_sync_queue (person_id int primary key);
                    
					select @last_update := (select max(date_updated) from etl.flat_log where table_name=@table_version);

					replace into ncd_monthly_report_dataset_sync_queue
                    (select distinct person_id from flat_hiv_summary_v15b where date_created >= @last_update);
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
            
            
SET @dyn_sql=CONCAT('delete t1 from ncd_monthly_report_dataset t1 join ',@queue_table,' t2 using (person_id);'); 
#            SET @dyn_sql=CONCAT('delete t1 from ncd_monthly_report_dataset t1 join ',@queue_table,' t2 using (person_id);'); 
			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1;  
            
            set @total_time=0;
			set @cycle_number = 0;
                    
			while @person_ids_count > 0 do
			
				set @loop_start_time = now();                        
				#create temporary table ncd_monthly_report_dataset_build_queue_0 (select * from ncd_monthly_report_dataset_build_queue_2 limit 5000); #TODO - change this when data_fetch_size changes

				drop temporary table if exists ncd_monthly_report_dataset_build_queue__0;
                create temporary table ncd_monthly_report_dataset_build_queue__0 (person_id int primary key);                

#SET @dyn_sql=CONCAT('insert into ncd_monthly_report_dataset_build_queue__0 (select * from ncd_monthly_report_dataset_build_queue_1 limit 100);'); 
                SET @dyn_sql=CONCAT('insert into ncd_monthly_report_dataset_build_queue__0 (select * from ',@queue_table,' limit ',cycle_size,');'); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;
                
                
                set @age =null;
                set @status = null;
                
                drop temporary table if exists ncd_monthly_report_dataset_0;
				create temporary table ncd_monthly_report_dataset_0
				(select 
					concat(date_format(endDate,"%Y%m"),person_id) as elastic_id,
					endDate,
                    encounter_id,
					person_id,
                    t3.uuid as person_uuid,
					date(birthdate) as birthdate,
					case
						when timestampdiff(year,birthdate,endDate) > 0 then @age := round(timestampdiff(year,birthdate,endDate),0)
						else @age :=round(timestampdiff(month,birthdate,endDate)/12,2)
					end as age,
					t3.gender,
					date(encounter_datetime) as encounter_date, 
                    
                    if(encounter_datetime between date_format(endDate,"%Y-%m-01")  and endDate,1,0) as visit_this_month,

					date(rtc_date) as rtc_date,
                    timestampdiff(day,rtc_date, endDate) as days_since_rtc_date,

                    if(date(encounter_datetime) = date(prev_clinical_rtc_date_hiv),1,0) as on_schedule,

					case
						when encounter_datetime between date_format(endDate,"%Y-%m-01")  and endDate
								AND date(encounter_datetime) = date(prev_clinical_rtc_date_hiv)
							then 1
						else 0
					end as scheduled_this_month,

					case
						when encounter_datetime between date_format(endDate,"%Y-%m-01")  and endDate
								AND date(encounter_datetime) != date(prev_clinical_rtc_date_hiv)
							then 1
						else 0
					end as unscheduled_this_month,
                    
					case
						when arv_first_regimen_location_id != 9999 
							and arv_first_regimen_start_date between date_format(endDate,"%Y-%m-01")  and endDate then arv_first_regimen_location_id
                        else location_id
                    end as location_id,                  
                    
                    
					encounter_type,
					
					case
						when (comorbidities regexp '903') then 1
						when htn_status = 7285 or htn_status = 7286 then 1
						when (htn_meds is not null) then 1
						else null
					end as is_hypertensive,

					case
						when ((sbp < 130) and (dbp < 80)) then 1 
						when ((sbp >= 130) and (dbp >= 80)) then 2
						when ((sbp is null) or (dbp is null)) then 3
						else NULL
					end as htn_state,

					case
						when (comorbidities regexp '175') then 1
						when dm_status = 7281 or dm_status = 7282 then 1
						when (dm_meds is not null) then 1
						else null
					end as is_diabetic,

					case
						when (hb_a1c >= 7 and hb_a1c <= 8) then 1 
						when (hb_a1c < 7 and hb_a1c > 8) then 2
						when (hb_a1c is null) or (hb_a1c is null) then 3
						else null
					end as dm_state,

					case
						when (comorbidities regexp '10860') then 1
						else null
					end as has_mhd,

					from etl.dates t1
					join etl.flat_hiv_summary_v15b t2 
					join amrs.person t3 using (person_id)
					join etl.ncd_monthly_report_dataset_build_queue__0 t5 using (person_id)
                    

					where  
							#t2.encounter_datetime <= t1.endDate
                            t2.encounter_datetime < date_add(endDate, interval 1 day)
							and (t2.next_clinical_datetime_hiv is null or t2.next_clinical_datetime_hiv >= date_add(t1.endDate, interval 1 day) )
							and t2.is_clinical_encounter=1 
							and t1.endDate between start_date and date_add(now(),interval 2 year)
					order by person_id, endDate
				);
                
               

				set @prev_id = null;
				set @cur_id = null;
				set @cur_status = null;
				set @prev_status = null;
                set @prev_location_id = null;
                set @cur_location_id = null;

				drop temporary table if exists ncd_monthly_report_dataset_1;
				create temporary table ncd_monthly_report_dataset_1
				(select
					*,
					@prev_id := @cur_id as prev_id,
					@cur_id := person_id as cur_id,

					case
						when @prev_id=@cur_id then @prev_location_id := @cur_location_id
                        else @prev_location_id := null
					end as next_location_id,
                    
                    @cur_location_id := location_id as cur_location_id,

					case
						when @prev_id=@cur_id then @prev_status := @cur_status
						else @prev_status := null
					end as next_status,	
					@cur_status := status as cur_status
						
					from ncd_monthly_report_dataset_0
					order by person_id, endDate desc
				);
                                
                select now();
				select count(*) as num_rows_to_be_inserted from ncd_monthly_report_dataset_2;
	
				#add data to table									  
				replace into ncd_monthly_report_dataset											  
				(select
					null, #date_created will be automatically set or updated
					elastic_id,
				endDate,
                encounter_id,
				person_id,
                person_uuid,
				birthdate,
				age,
				gender,
				encounter_date,
                visit_this_month,
				rtc_date,
                days_since_rtc_date,
                on_schedule,
                scheduled_this_month,
                unscheduled_this_month,
                f_18_and_over_this_month,
                prev_location_id,
				location_id,
                t2.uuid as location_uuid,
                next_location_id,
				t2.state_province as clinic_county,
				t2.name as clinic,
				t2.latitude as clinic_latitude,
				t2.longitude as clinic_longitude,
				encounter_type,
				death_date,
				enrollment_date,
				enrolled_this_month,
				transfer_in_this_month,
				transfer_in_location_id,
				transfer_in_date,
				transfer_out_this_month,
				transfer_out_location_id,
				transfer_out_date,
				prev_status,
				status,
				next_status,
                active_in_care_this_month,
                is_pre_art_this_month,
                arv_first_regimen_location_id,
                arv_first_regimen,
				arv_first_regimen_names,
				arv_first_regimen_start_date,
                art_cohort_year,
				art_cohort_month,
				art_cohort_num,
                art_cohort_total_months,
                days_since_starting_arvs,
				started_art_this_month,
                art_revisit_this_month,
                arv_start_date,
                prev_month_arvs,
                prev_month_arvs_names,
                cur_arv_meds,
				cur_arv_meds_names,
                cur_arv_meds_strict,
				cur_arv_line,
                cur_arv_line_strict,
                cur_arv_line_reported,
				on_art_this_month,
                on_original_first_line_this_month,
                on_alt_first_line_this_month,
                on_second_line_or_higher_this_month,
				eligible_for_vl,
                days_since_last_vl,
                net_12_month_cohort_this_month,
				active_on_art_12_month_cohort_this_month,
                has_vl_12_month_cohort,
                vl_suppressed_12_month_cohort,
                has_vl_this_month,
                is_suppressed_this_month,
				vl_1,
				vl_1_date,
				vl_in_past_year,
				vl_2,
				vl_2_date,
                qualifies_for_follow_up_vl,
				got_follow_up_vl,
				got_follow_up_vl_this_month,    
				num_days_to_follow_vl,
				follow_up_vl_suppressed,
				follow_up_vl_suppressed_this_month,
				follow_up_vl_unsuppressed,
				follow_up_vl_unsuppressed_this_month,
                due_for_vl_this_month,
                reason_for_needing_vl_this_month,
                number_of_months_has_needed_vl,
				needs_follow_up_vl,
				had_med_change_this_month,
				had_med_change_this_month_after_2_unsuppressed_vls,
				same_meds_this_month_after_2_unsuppressed_vls,
				tb_screen,
				tb_screening_datetime,
				tb_screening_result,  
                tb_screened_this_visit_this_month,
				tb_screened_active_this_month,
				presumed_tb_positive_this_month,
				tb_tx_start_date,
				started_tb_tx_this_month,
                on_tb_tx_this_month,
                on_tb_tx_and_started_art_this_month,
				pcp_prophylaxis_start_date,
				started_pcp_prophylaxis_this_month,
				on_pcp_prophylaxis_this_month,
				ipt_start_date,
				ipt_stop_date,
				ipt_completion_date,
				started_ipt_this_month,
				on_ipt_this_month,
				completed_ipt_past_12_months,
				pregnant_this_month,
                is_pregnant_and_started_art_this_month,
				delivered_this_month,
				condoms_provided_this_month,
				condoms_provided_since_active,
				started_modern_contraception_this_month,
                modern_contraception_since_active,
                on_modern_contraception_this_month,
                contraceptive_method,
                discordant_status
					from ncd_monthly_report_dataset_2 t1
                    join amrs.location t2 using (location_id)
				);
                


				#delete from @queue_table where person_id in (select person_id from ncd_monthly_report_dataset_build_queue__0);
				SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ncd_monthly_report_dataset_build_queue__0 t2 using (person_id);'); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;  
				
				#select @person_ids_count := (select count(*) from @queue_table);                        
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
				#select concat("Estimated time remaining: ", @remaining_time,' minutes');
                
#                select count(*) into @num_in_hmrd from ncd_monthly_report_dataset;
                
                select @num_in_hmrd as num_in_hmrd,
					@person_ids_count as num_remaining, 
					@cycle_length as 'Cycle time (s)', 
                    ceil(@person_ids_count / cycle_size) as remaining_cycles, 
                    @remaining_time as 'Est time remaining (min)';


			end while;

			if(query_type = "build") then
					SET @dyn_sql=CONCAT('drop table ',@queue_table,';'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
			end if;            

			set @end = now();
            # not sure why we need last date_created, Ive replaced this with @start
			insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
			select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");

        END$$
DELIMITER ;
