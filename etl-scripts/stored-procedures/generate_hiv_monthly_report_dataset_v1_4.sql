use etl;
DELIMITER $$
CREATE  PROCEDURE `generate_hiv_monthly_report_dataset_v1_4`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int, IN start_date varchar(50))
BEGIN

			set @start = now();
			set @table_version = "hiv_monthly_report_dataset_v1.0";
			set @last_date_created = (select max(date_created) from etl.flat_hiv_summary_v15b);

			set @sep = " ## ";
			set @lab_encounter_type = 99999;
			set @death_encounter_type = 31;
            

            #drop table if exists hiv_monthly_report_dataset_v1_3;
create table if not exists hiv_monthly_report_dataset_v1_2 (
#            create table if not exists hiv_monthly_report_dataset (
				date_created timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
                elastic_id bigint,
				endDate date,
                encounter_id int,
				person_id int,
                person_uuid varchar(100),
				birthdate date,
				age double,
				gender varchar(1),
				encounter_date date,
                visit_this_month tinyint,
				rtc_date date,
                days_since_rtc_date int,
                on_schedule tinyint,
                scheduled_this_month tinyint,
                unscheduled_this_month tinyint,
                f_18_and_older_this_month tinyint,                
				prev_location_id mediumint,
				location_id mediumint,
                location_uuid varchar(100),
                next_location_id mediumint,
                clinic_county varchar(250),
				clinic varchar(250),
				clinic_latitude double,
				clinic_longitude double,
				encounter_type int,
				death_date date,
				enrollment_date date,
				enrolled_this_month tinyint,
				transfer_in_this_month tinyint,
				transfer_in_location_id mediumint,
				transfer_in_date date,
				transfer_out_this_month tinyint,
				transfer_out_location_id mediumint,
				transfer_out_date date,
				prev_status varchar(250),
				status varchar(250),
				next_status varchar(250),
                active_in_care_this_month tinyint,
				is_pre_art_this_month tinyint,
                arv_first_regimen_location_id int,
				arv_first_regimen varchar(250),
				arv_first_regimen_names varchar(250),
				arv_first_regimen_start_date date,
				art_cohort_year date,
                art_cohort_month date,
				art_cohort_num smallint,
                art_cohort_total_months smallint,
                days_since_starting_arvs mediumint,
				started_art_this_month tinyint,
                art_revisit_this_month tinyint,
                arv_start_date date,
                prev_month_arvs varchar(250),
                prev_month_arvs_names varchar(250),
                cur_arv_meds varchar(250),
				cur_arv_meds_names varchar(250),
                cur_arv_meds_strict varchar(250),
				cur_arv_line tinyint,
                cur_arv_line_strict tinyint,
                cur_arv_line_reported tinyint,
				on_art_this_month tinyint,
                on_original_first_line_this_month tinyint,
                on_alt_first_line_this_month tinyint,
                on_second_line_or_higher_this_month tinyint,
				eligible_for_vl tinyint,
                days_since_last_vl int,
				net_12_month_cohort_this_month tinyint,
				active_on_art_12_month_cohort_this_month tinyint,
                has_vl_12_month_cohort tinyint,
                vl_suppressed_12_month_cohort tinyint,
                has_vl_this_month tinyint, #patient has had VL in past year and is currently active on ART
                is_suppressed_this_month tinyint, #patient has had VL in past year < 1000 and is currently active on ART
				vl_1 int,
				vl_1_date date,
				vl_in_past_year tinyint,
				vl_2 int,
				vl_2_date date,
                qualifies_for_follow_up_vl tinyint,
				got_follow_up_vl tinyint,
				got_follow_up_vl_this_month tinyint,    
				num_days_to_follow_vl tinyint,
				follow_up_vl_suppressed tinyint,
				follow_up_vl_suppressed_this_month tinyint,
				follow_up_vl_unsuppressed tinyint,
				follow_up_vl_unsuppressed_this_month tinyint,
				due_for_vl_this_month tinyint,
                reason_for_needing_vl_this_month varchar(250),
                number_of_months_has_needed_vl smallint,
				needs_follow_up_vl tinyint,
				had_med_change_this_month tinyint,
				had_med_change_this_month_after_2_unsuppressed_vls tinyint,
				same_meds_this_month_after_2_unsuppressed_vls tinyint,                
				tb_screen tinyint,
				tb_screening_datetime datetime,
				tb_screening_result int,  
                tb_screened_this_visit_this_month tinyint,				
                tb_screened_active_this_month tinyint,
				presumed_tb_positive_this_month tinyint,
				tb_tx_start_date date,
				started_tb_tx_this_month tinyint,
                on_tb_tx_this_month tinyint,                
				on_tb_tx_and_started_art_this_month tinyint,
				pcp_prophylaxis_start_date date,
				started_pcp_prophylaxis_this_month tinyint,
				on_pcp_prophylaxis_this_month tinyint,
				ipt_start_date date,
				ipt_stop_date date,
				ipt_completion_date date,
				started_ipt_this_month tinyint,
				on_ipt_this_month tinyint,
				completed_ipt_past_12_months tinyint,
				pregnant_this_month tinyint,
                is_pregnant_and_started_art_this_month tinyint,                
				delivered_this_month tinyint,
				condoms_provided_this_month tinyint,
				condoms_provided_since_active tinyint,
				started_modern_contraception_this_month tinyint,
                modern_contraception_since_active tinyint,
                on_modern_contraception_this_month tinyint,
                contraceptive_method int,
                discordant_status int,
                is_cross_border_country_this_month int,
				is_cross_border_county_this_month int,
				is_cross_border_this_month int,
                last_cross_boarder_screening_datetime date,
				travelled_outside_last_3_months int,
				travelled_outside_last_6_months int,
				travelled_outside_last_12_months int,
                tb_tx_end_date date,
                tb_tx_stop_date date,
                country_of_residence int,
                active_and_eligible_for_ovc tinyint,
				inactive_and_eligible_for_ovc tinyint,
				enrolled_in_ovc_this_month tinyint,
				ovc_non_enrolment_declined tinyint,
				ovc_non_enrolment_out_of_catchment_area tinyint,
				newly_exited_from_ovc_this_month tinyint,
				exited_from_ovc_this_month tinyint,
                ca_cx_screen tinyint,
				ca_cx_screening_datetime datetime,
				ca_cx_screening_result int,  
                ca_cx_screened_this_visit_this_month tinyint,				
                ca_cx_screened_active_this_month tinyint,
				confirmed_tb_positive_this_month tinyint,
                primary key elastic_id (elastic_id),
				index person_enc_date (person_id, encounter_date),
                index person_report_date (person_id, endDate),
                index endDate_location_id (endDate, location_id),
                index date_created (date_created),
                index status_change (location_id, endDate, status, prev_status)
                
            );
            
            #create table if not exists hiv_monthly_report_dataset_build_queue (person_id int primary key);
			#replace into hiv_monthly_report_dataset_build_queue
			#(select distinct person_id from flat_hiv_summary_v15);

			if (query_type = "build") then
					select "BUILDING.......................";
					set @queue_table = concat("hiv_monthly_report_dataset_build_queue_",queue_number);                    
#set @queue_table = concat("hiv_monthly_report_dataset_build_queue_1");                    				
					#create  table if not exists @queue_table (person_id int, primary key (person_id));
					#SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from hiv_monthly_report_dataset_build_queue limit 1000);'); 
					SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,'(person_id int primary key) (select * from hiv_monthly_report_dataset_build_queue limit ', queue_size, ');'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;
                    
                    			#delete t1 from hiv_monthly_report_dataset_build_queue t1 join @queue_table t2 using (person_id)
					SET @dyn_sql=CONCAT('delete t1 from hiv_monthly_report_dataset_build_queue t1 join ',@queue_table, ' t2 using (person_id)'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
			end if;

			
            if (query_type = "sync") then
					set @queue_table = "hiv_monthly_report_dataset_sync_queue";                                
                    create table if not exists hiv_monthly_report_dataset_sync_queue (person_id int primary key);
                    
					select @last_update := (select max(date_updated) from etl.flat_log where table_name=@table_version);

					replace into hiv_monthly_report_dataset_sync_queue
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
            
            
SET @dyn_sql=CONCAT('delete t1 from hiv_monthly_report_dataset_v1_2 t1 join ',@queue_table,' t2 using (person_id);'); 
#            SET @dyn_sql=CONCAT('delete t1 from hiv_monthly_report_dataset t1 join ',@queue_table,' t2 using (person_id);'); 
			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1;  
            
            set @total_time=0;
			set @cycle_number = 0;
                    
			while @person_ids_count > 0 do
			
				set @loop_start_time = now();                        
				#create temporary table hiv_monthly_report_dataset_build_queue_0 (select * from hiv_monthly_report_dataset_build_queue_2 limit 5000); #TODO - change this when data_fetch_size changes

				drop temporary table if exists hiv_monthly_report_dataset_build_queue__0;
                create temporary table hiv_monthly_report_dataset_build_queue__0 (person_id int primary key);                

#SET @dyn_sql=CONCAT('insert into hiv_monthly_report_dataset_build_queue__0 (select * from hiv_monthly_report_dataset_build_queue_1 limit 100);'); 
                SET @dyn_sql=CONCAT('insert into hiv_monthly_report_dataset_build_queue__0 (select * from ',@queue_table,' limit ',cycle_size,');'); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;
                
                
                set @age =null;
                set @status = null;
                
                drop temporary table if exists hiv_monthly_report_dataset_0;
				create temporary table hiv_monthly_report_dataset_0
				(select 
					concat(date_format(endDate,"%Y%m"),person_id) as elastic_id,
					endDate,
                    encounter_id,
					visit_type,
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
						when encounter_datetime between date_format(endDate,"%Y-%m-01")  and endDate
								AND gender="F"
                                AND @age >= 18
							then 1
						else 0
					end as f_18_and_over_this_month,


					case
						when arv_first_regimen_location_id != 9999 
							and arv_first_regimen_start_date between date_format(endDate,"%Y-%m-01")  and endDate then arv_first_regimen_location_id
                        else location_id
                    end as location_id,                  
                    last_non_transit_location_id,
                    
					encounter_type,
					date(t2.death_date) as death_date,

					enrollment_date,
                    
					case
						when arv_first_regimen_start_date < enrollment_date then 0
                        when enrollment_date between date_format(endDate,"%Y-%m-01")  and endDate then 1
						else 0
					end as enrolled_this_month,

					case
						when transfer_in_date between date_format(endDate,"%Y-%m-01")  and endDate then 1
						else 0
					end as transfer_in_this_month,
					transfer_in_location_id,
					transfer_in_date,


					case
						when transfer_out_date between date_format(endDate,"%Y-%m-01")  and endDate then 1
						else 0
					end as transfer_out_this_month,
					
					transfer_out_location_id,
					transfer_out_date,
					case
						when date_format(endDate, "%Y-%m-01") > t2.death_date then @status := "dead"
#						when transfer_out_date < date_format(endDate,"%Y-%m-01") then @status := "transfer_out"
						when date_format(endDate, "%Y-%m-01") > transfer_out_date then @status := "transfer_out"
						when timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 30 day)),endDate) <= 30 then @status := "active"
						when timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 30 day)),endDate) between 31 and 90 then @status := "defaulter"
						when timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 30 day)),endDate) > 90 then @status := "ltfu"
						else @status := "unknown"
					end as status,
					
					case 
						when arv_first_regimen_location_id is not null then arv_first_regimen_location_id 
						when arv_first_regimen_location_id is null and arv_first_regimen_start_date is not null then location_id else
                    null end as arv_first_regimen_location_id,
                    
					arv_first_regimen,
                    get_arv_names(arv_first_regimen) as arv_first_regimen_names,
					date(arv_first_regimen_start_date) as arv_first_regimen_start_date,

					date_format(arv_first_regimen_start_date, "%Y-01-01") as art_cohort_year,

					date_format(arv_first_regimen_start_date,"%Y-%m-01") as art_cohort_month,
                    
                    timestampdiff(month,arv_first_regimen_start_date,endDate) as art_cohort_total_months,

                    case
						when @status != "transfer_out" and month(arv_first_regimen_start_date) = month(endDate) then @art_cohort_num := timestampdiff(year,arv_first_regimen_start_date,endDate)
                        else @art_cohort_num := null 
					end as art_cohort_num,

					case
						when @status="active" and arv_first_regimen_start_date between date_format(endDate,"%Y-%m-01")  and endDate then @started_art_this_month := 1
						else @started_art_this_month :=0
					end as started_art_this_month,
                    

                    case
						when @status != "transfer_out" AND @art_cohort_num = 1
							then @net_12_month_cohort_this_month := 1
                        else @net_12_month_cohort_this_month := 0
					end as net_12_month_cohort_this_month,

                    case
						when @status = "active" AND @art_cohort_num = 1 AND cur_arv_meds is not null
							then @active_on_art_12_month_cohort_this_month := 1
                        else @active_on_art_12_month_cohort_this_month := 0
					end as active_on_art_12_month_cohort_this_month,

                                        
                    case
						when @status = "active" then 1
                        else 0
					end as active_in_care_this_month,
                    
                    case
						when @status="active" and arv_first_regimen is null then 1
                        else 0
                    end as is_pre_art_this_month,
									
					case
						when date_format(arv_first_regimen_start_date,"%Y-%m-01") != date_format(endDate,"%Y-%m-01") 
								AND cur_arv_meds is not null
							AND @status = "active"
						THEN 1
						else 0
					end as art_revisit_this_month,
										
                    arv_start_date,
                    
					cur_arv_meds,
                    get_arv_names(cur_arv_meds) as cur_arv_meds_names,
                    cur_arv_meds_strict,
					cur_arv_line,
                    cur_arv_line_strict,
                    cur_arv_line_reported,
                    
					# We are using liberal definition of cur_arv_meds such that
                    # if a patient previously on ART and not explicilty stopped, the
                    # patient is considered to be on ART. 
					case
						when cur_arv_meds is not null AND @status = "active"
							then 1
						else 0
					end as on_art_this_month,
                    
                    case
						when cur_arv_meds = arv_first_regimen 
								AND @status="active"
                                AND cur_arv_line_reported=1
							then 1
                        else 0
                    end as on_original_first_line_this_month,
                    
                    case
						when cur_arv_meds != arv_first_regimen 
								AND @status="active"
                                AND cur_arv_line_reported=1
							then 1
                        else 0
                    end as on_alt_first_line_this_month,
                    
					case
						when @status = "active" AND cur_arv_line_reported >= 2 then 1
                        else 0
					end as on_second_line_or_higher_this_month,
                        
					
					
					timestampdiff(day,arv_start_date,endDate) as days_since_starting_arvs,
                                        
					case
						when timestampdiff(month, arv_start_date,endDate) > 6 then 1
						else 0
					end as eligible_for_vl,
                    
                    
					timestampdiff(day,vl_1_date,endDate) as days_since_last_vl,
                                                            										
					if(timestampdiff(year,vl_1_date,endDate) < 1,1,0) as vl_in_past_year,
					
                    case
						when @status = "active" AND timestampdiff(year,vl_1_date,endDate) < 1 then @has_vl_this_month := 1
                        else @has_vl_this_month := 0
					end as has_vl_this_month,


                    case
						when @status = "active" AND timestampdiff(year,vl_1_date,endDate) < 1 and vl_1 < 1000 then @is_suppressed_this_month := 1
                        else @is_suppressed_this_month := 0
					end as is_suppressed_this_month,


					case
						when @active_on_art_12_month_cohort_this_month AND @has_vl_this_month then 1
                        else 0
					end as has_vl_12_month_cohort,

					case
						when @active_on_art_12_month_cohort_this_month AND @is_suppressed_this_month then 1
                        else 0
					end as vl_suppressed_12_month_cohort, 

					vl_1, 
					date(vl_1_date) as vl_1_date,    

					vl_2,
					date(vl_2_date) as vl_2_date,

					case
						when vl_1>=1000 and timestampdiff(month,vl_1_date,endDate) between 3 and 12 then @qualifies_for_follow_up_vl := 1
						when vl_2>=1000 and timestampdiff(month,vl_2_date,endDate) between 3 and 12 then @qualifies_for_follow_up_vl := 1
						else @qualifies_for_follow_up_vl := 0	
					end as qualifies_for_follow_up_vl,

					case
						when @qualifies_for_follow_up_vl AND timestampdiff(month,vl_2_date,vl_1_date) < 12 
							then @got_follow_up_vl := 1
						else @got_follow_up_vl := 0
					end as got_follow_up_vl,
					
					case
						when @got_follow_up_vl and vl_1_date between date_format(endDate,"%Y-%m-01") and endDate then @got_follow_up_vl_this_month := 1
						else @got_follow_up_vl_this_month := 0
					end as got_follow_up_vl_this_month,    
					
					case
						when @got_follow_up_vl then timestampdiff(day,vl_2_date, vl_1_date)
						else null
					end as num_days_to_follow_vl,
						

					case
						when @got_follow_up_vl AND vl_1 < 1000 then @follow_up_vl_suppressed := 1
						else @follow_up_vl_suppressed := 0
					end as follow_up_vl_suppressed,
					
					case
						when @got_follow_up_vl_this_month AND vl_1 < 1000 then @follow_up_vl_suppressed_this_month := 1
						else @follow_up_vl_suppressed_this_month := 0
					end as follow_up_vl_suppressed_this_month,

					
					case
						when @got_follow_up_vl AND vl_1 >= 1000 then @follow_up_vl_unsuppressed := 1
						else @follow_up_vl_unsuppressed := 0
					end as follow_up_vl_unsuppressed,
					
					case
						when @got_follow_up_vl_this_month AND vl_1 >= 1000 then @follow_up_vl_unsuppressed_this_month := 1
						else @follow_up_vl_unsuppressed_this_month := 0
					end as follow_up_vl_unsuppressed_this_month,
											

					case
						when 
								vl_1 > 1000 
                                AND vl_1_date > arv_start_date
								AND timestampdiff(month,vl_1_date,endDate) > 3 
                                AND timestampdiff(year,vl_1_date,endDate) <= 1 
							then @needs_follow_up_vl :=1                            
						else @needs_follow_up_vl := 0
					end as needs_follow_up_vl,


    
					case
						when 
								vl_1 > 1000 
                                AND vl_1_date > arv_start_date 
                                AND timestampdiff(month,vl_1_date,endDate) between 3 and 11 
							THEN 1
                        when timestampdiff(month,arv_start_date,endDate) between 6 and 11 AND vl_1 is null THEN 1                        						
						when 
								timestampdiff(month,arv_start_date,endDate) between 12 and 18 
								AND vl_2 is null then 1
						when 
								timestampdiff(month,arv_start_date,endDate) >= 12 
								AND ifnull(timestampdiff(month,vl_1_date,endDate) >= 12,1)
							then 1
                    end as due_for_vl_this_month,
                    
					case
						when vl_1 > 1000 
								AND vl_1_date > arv_start_date 
								AND timestampdiff(month,vl_1_date,endDate) between 3 and 11 
							THEN "Previous VL Unsuppressed"
                        when timestampdiff(month,arv_start_date,endDate) between 6 and 11 AND vl_1 is null then "On ART for 6 months"	
						when 
								timestampdiff(month,arv_start_date,endDate) between 12 and 18
								AND vl_2 is null 
							THEN "On ART for 12 months"
						when 
								timestampdiff(month,arv_start_date,endDate) >= 12 
								AND ifnull(timestampdiff(month,vl_1_date,endDate) >= 12,1) 
							then "Annual VL"
                                
                    end as reason_for_needing_vl_this_month,
			
					

					case
						when vl_1 > 1000 
								AND vl_1_date > arv_start_date 
								AND timestampdiff(month,vl_1_date,endDate) between 3 and 11 
							then timestampdiff(month,vl_1_date,endDate) - 3

                        when timestampdiff(month,arv_start_date,endDate) between 6 and 11 AND vl_1 is null
							then timestampdiff(month,arv_start_date,endDate) - 6

						when 
								timestampdiff(month,arv_start_date,endDate) between 12 and 18
								AND vl_2 is null
							then timestampdiff(month,arv_start_date,endDate) - 12

						when                         
								timestampdiff(month,arv_start_date,endDate) >= 12 
								AND ifnull(timestampdiff(month,vl_1_date,endDate) >= 12,1) 
							then timestampdiff(month,ifnull(vl_1_date,arv_start_date),endDate) - 12
                    end as number_of_months_has_needed_vl,
                    
					@tb_tx_start_date :=  date(tb_tx_start_date) as tb_tx_start_date,#need to know time period, i.e. screened this month or screened in past X months
                    
                    @tb_tx_end_date :=  date(tb_tx_end_date) as tb_tx_end_date,
                    @tb_tx_stop_date :=  date(tb_tx_stop_date) as tb_tx_stop_date,

					case
						when tb_tx_start_date between date_format(endDate,"%Y-%m-01")  and endDate then @started_tb_tx_this_month :=  1
						else @started_tb_tx_this_month :=  0
					end as started_tb_tx_this_month,
                    
                    @on_tb_tx_this_month := on_tb_tx as on_tb_tx_this_month,
                    
                    case
						when on_tb_tx =1 and @started_art_this_month then 1
                        else 0
					end as on_tb_tx_and_started_art_this_month,
																			
					tb_screen,
                    tb_screening_datetime,
                    tb_screening_result,
                    
					case
						when tb_screening_datetime between date_format(endDate, "%Y-%m-01") and endDate
							AND @status = "active" then 1
						else 0
					end as tb_screened_this_visit_this_month,

					case
						when tb_screening_datetime >= date(encounter_datetime) 
							AND @status = "active" then 1
						when tb_screening_datetime between DATE_SUB(endDate, INTERVAL 6 MONTH) and endDate
							AND @status = "active" AND @started_tb_tx_this_month != 1 then 1
						else 0
					end as tb_screened_active_this_month,
                    
					case
						when tb_screening_result = 6971
							AND tb_screening_datetime between date_format(endDate,"%Y-%m-01")  and endDate then 1
                        else 0
                    end as presumed_tb_positive_this_month,

					date(pcp_prophylaxis_start_date) as pcp_prophylaxis_start_date,

					case
						when pcp_prophylaxis_start_date between date_format(endDate,"%Y-%m-01")  and endDate then 1
						else 0
					end as started_pcp_prophylaxis_this_month,
					
					case
						when @status="active" AND pcp_prophylaxis_start_date is not null then 1
						else 0
					end as on_pcp_prophylaxis_this_month,
					
					
					ipt_start_date,
					ipt_stop_date,
					ipt_completion_date,
					case
						when ipt_start_date between date_format(endDate,"%Y-%m-01")  and endDate then 1
						else 0
					end as started_ipt_this_month,
					
					case
						when ipt_start_date is not null and ipt_stop_date is null then 1
						else 0
					end as on_ipt_this_month,
						
					
					case
						when timestampdiff(month,ipt_completion_date,endDate) <= 12 then 1
						else 0
					end as completed_ipt_past_12_months,

					case
						when is_pregnant and edd > endDate then 1
                        else 0
                    end as pregnant_this_month,
                    
                    case
						when is_pregnant and edd > endDate and @started_art_this_month then 1
                        else 0                        
					end as is_pregnant_and_started_art_this_month,

					0 as delivered_this_month,
					
					case
						when condoms_provided_date between date_format(endDate,"%Y-%m-01")  and endDate then 1
						else 0
					end as condoms_provided_this_month,
					
					case
						when condoms_provided_date >= date(encounter_datetime)
							AND timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 30 day)),endDate) <= 30 then 1                            
						else 0
					end as condoms_provided_since_active,


					case
						when modern_contraceptive_method_start_date between date_format(endDate,"%Y-%m-01")  and endDate then 1
						else 0
					end as started_modern_contraception_this_month,
                    
					case
						when modern_contraceptive_method_start_date <= date(encounter_datetime)
							AND timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 30 day)),endDate) <= 30 
                            then 1                            
						else 0
					end as modern_contraception_since_active,

					case
						when modern_contraceptive_method_start_date <= date(encounter_datetime)
							AND @status="active"
							AND gender="F"
                            AND @age>=15
                            then 1                            
						else 0
					end as on_modern_contraception_this_month,
										

                    contraceptive_method,
                    discordant_status,
				
					is_cross_border_country as is_cross_border_country_this_month,
                    
                    case
						when is_cross_border_country = 0 and t2.location_id in (20,19,55,83,12,23,100,130,78,91,106,65,90) and t4.address1 != 'Busia' then @is_cross_border_county_this_month := 1
						else @is_cross_border_county_this_month := 0
					end as is_cross_border_county_this_month,
                    
                    IF(is_cross_border_country = 1 or @is_cross_border_county_this_month = 1, 1, 0) as is_cross_border_this_month,
                    
                    date(last_cross_boarder_screening_datetime) as last_cross_boarder_screening_datetime,
                    
					case
						when t2.travelled_outside_last_3_months = 1
							AND timestampdiff(month,last_cross_boarder_screening_datetime,endDate) <= 3
                            then 1                            
						else 0
					end as travelled_outside_last_3_months,
                    
                    case
						when (t2.travelled_outside_last_6_months = 1 or t2.travelled_outside_last_3_months = 1)
							AND timestampdiff(month,last_cross_boarder_screening_datetime,endDate) <= 6
                            then 1                            
						else 0
					end as travelled_outside_last_6_months,
                    
                    case
						when ( t2.travelled_outside_last_12_months = 1 or t2.travelled_outside_last_6_months = 1 or t2.travelled_outside_last_3_months = 1)
							AND timestampdiff(month,last_cross_boarder_screening_datetime,endDate) <= 12
                            then 1                            
						else 0
					end as travelled_outside_last_12_months,
                    
                    t2.country_of_residence,
                    
					case
						when @status="active"
                            AND @age<19
                            then 1
						else 0
					end as active_and_eligible_for_ovc,

					case
						when (@status="defaulter" OR @status="ltfu")
                            AND @age<=19
                            then 1
						else 0
					end as inactive_and_eligible_for_ovc,

					case
						when t2.ovc_non_enrollment_reason = 1504
                            then 1
						else 0
					end as ovc_non_enrolment_declined,

					case
						when t2.ovc_non_enrollment_reason = 6834
                            then 1
						else 0
					end as ovc_non_enrolment_out_of_catchment_area,

					case
                        when (t2.ovc_exit_date is not null and t2.ovc_exit_date between date_format(t1.endDate,"%Y-%m-01")  and t1.endDate)  then 1
                        else 0
                    end as newly_exited_from_ovc_this_month,

					
                    t2.ovc_exit_date,
                    ca_cx_screen,
                    ca_cx_screening_datetime,
                    ca_cx_screening_result,
					case
						when ca_cx_screening_datetime between date_format(endDate, "%Y-%m-01") and endDate then 1
						else 0
					end as ca_cx_screened_this_visit_this_month,
					case
						when ca_cx_screening_datetime between date_format(endDate, "%Y-%m-01") and endDate AND @status = "active"  then 1
						else 0
					end as ca_cx_screened_active_this_month,

					case
						when tb_screening_result = 6137
							AND tb_screening_datetime between date_format(endDate,"%Y-%m-01")  and endDate then 1
                        else 0
                    end as confirmed_tb_positive_this_month
					
					from etl.dates t1
					join etl.flat_hiv_summary_v15b t2 
					join amrs.person t3 using (person_id)
					left join amrs.person_address t4 using (person_id)
					join etl.hiv_monthly_report_dataset_build_queue__0 t5 using (person_id)
                    

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

				drop temporary table if exists hiv_monthly_report_dataset_1;
				create temporary table hiv_monthly_report_dataset_1
				(select
					*,
					@prev_id := @cur_id as prev_id,
					@cur_id := person_id as cur_id,

					case
						when @prev_id=@cur_id then @prev_location_id := @cur_location_id
                        else @prev_location_id := null
					end as next_location_id,
                    
					case
						when @prev_id=@cur_id and visit_type in (23, 24, 119, 124, 129,43,80,118,120,123) then @cur_location_id := last_non_transit_location_id	
						else @cur_location_id := location_id 	
					end as cur_location_id,
                    
					case
						when @prev_id=@cur_id then @prev_status := @cur_status
						else @prev_status := null
					end as next_status,	
					@cur_status := status as cur_status
						
					from hiv_monthly_report_dataset_0
					order by person_id, endDate desc
				);

				alter table hiv_monthly_report_dataset_1 drop prev_id, drop cur_id, drop cur_status, drop cur_location_id;

				set @prev_id = -1;
				set @cur_id = -1;
				set @cur_status = null;
				set @prev_status = null;
                set @cur_arv_meds = null;
                set @prev_arv_meds = null;
                set @cur_location_id = null;
                set @prev_location_id = null;
				drop temporary table if exists hiv_monthly_report_dataset_2;
				create temporary table hiv_monthly_report_dataset_2
				(select
					t1.*,
					@prev_id := @cur_id as prev_id,
					@cur_id := t1.person_id as cur_id,
					case when t3.in_ovc_this_month = 1
						then 1
                        else 0
					end as enrolled_in_ovc_this_month,
                    
                    case
                        when t1.ovc_exit_date is null then 0
                        when t1.ovc_exit_date is not null and t3.in_ovc_this_month = 1 then 0
                        when t1.ovc_exit_date is not null then 1
                        else 0
                    end as exited_from_ovc_this_month,
                    
                    case
						when @prev_id=@cur_id then @prev_location_id := @cur_location_id
                        else @prev_location_id := null
					end as prev_location_id,
                    
                   case 
						when @prev_id=@cur_id and  visit_type in (23, 24, 119, 124, 129,43,80,118,120,123) then @cur_location_id := last_non_transit_location_id
						else @cur_location_id := location_id 
					end as cur_location_id,
                    
					case
						when @prev_id=@cur_id then @prev_status := @cur_status
						else @prev_status := null
					end as prev_status,	
					@cur_status := status as cur_status,
					
                    case
						when @prev_id = @cur_id then @prev_month_arvs := @cur_arv_meds
                        else @prev_month_arvs := null
                    end as prev_month_arvs,

                    @cur_arv_meds := cur_arv_meds as cur_arv_meds_2,

                    case
						when @prev_id = @cur_id then @prev_month_arvs_names := @cur_arv_meds_names
                        else @prev_month_arvs_names := null
                    end as prev_month_arvs_names,

                    @cur_arv_meds_names := cur_arv_meds_names as cur_arv_meds_names_2,

                    
					days_since_starting_arvs - days_since_last_vl as days_between_arv_start_and_last_vl,

					case
						when @prev_id != @cur_id then 0
                        when cur_arv_meds != @prev_month_arvs then 1
						when (@prev_month_arvs is not null and cur_arv_meds is null) then 1
						else 0
					end as had_med_change_this_month,
					
					case
						when @prev_id != @cur_id then 0
						when @follow_up_vl_unsuppressed AND cur_arv_meds != @prev_month_arvs then 1
						when @follow_up_vl_unsuppressed AND (@prev_month_arvs is not null and cur_arv_meds is null) then 1
						else 0
					end as had_med_change_this_month_after_2_unsuppressed_vls,


					case
						when @prev_id != @cur_id then 0
						when @follow_up_vl_unsuppressed AND cur_arv_meds = @prev_month_arvs then 1
						else 0
					end as same_meds_this_month_after_2_unsuppressed_vls
                    
				
						
					from hiv_monthly_report_dataset_1 t1
					left join patient_monthly_enrollment t3 on (t1.person_id = t3.person_id and t1.endDate = t3.endDate)
					order by t1.person_id, t1.endDate
				);
                                
                -- select * from hiv_monthly_report_dataset_2 where endDate = '2020-06-30'; 

                select now();
				select count(*) as num_rows_to_be_inserted from hiv_monthly_report_dataset_2;
	
				#add data to table
#				replace into hiv_monthly_report_dataset											  
				replace into hiv_monthly_report_dataset_v1_2											  
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
				cur_location_id as location_id,
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
                discordant_status,
                is_cross_border_country_this_month,
				is_cross_border_county_this_month,
				is_cross_border_this_month,
                last_cross_boarder_screening_datetime,
				travelled_outside_last_3_months,
				travelled_outside_last_6_months,
				travelled_outside_last_12_months,
                tb_tx_end_date,
                tb_tx_stop_date,
                country_of_residence,
                active_and_eligible_for_ovc,
				inactive_and_eligible_for_ovc,
				enrolled_in_ovc_this_month,
				ovc_non_enrolment_declined,
				ovc_non_enrolment_out_of_catchment_area,
				newly_exited_from_ovc_this_month,
				exited_from_ovc_this_month,
                ca_cx_screen,
				ca_cx_screening_datetime,
				ca_cx_screening_result,  
                ca_cx_screened_this_visit_this_month,				
                ca_cx_screened_active_this_month,
				confirmed_tb_positive_this_month
					from hiv_monthly_report_dataset_2 t1
                    join amrs.location t2 on (t2.location_id = t1.cur_location_id)
				);
                


				#delete from @queue_table where person_id in (select person_id from hiv_monthly_report_dataset_build_queue__0);
				SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join hiv_monthly_report_dataset_build_queue__0 t2 using (person_id);'); 
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
                
#                select count(*) into @num_in_hmrd from hiv_monthly_report_dataset_v1_2;
                
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
            -- not sure why we need last date_created, I've replaced this with @start
			insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
			select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");

        END$$
DELIMITER ;
