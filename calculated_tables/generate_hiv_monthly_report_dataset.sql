use etl;
drop procedure if exists generate_hiv_monthly_report_dataset_v1_0;

DELIMITER $$
	CREATE PROCEDURE generate_hiv_monthly_report_dataset_v1_0(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int, IN start_date varchar(50))
		BEGIN

			set @start = now();
			set @table_version = "hiv_monthly_report_dataset_v1.0";
			set @last_date_created = (select max(date_created) from etl.flat_hiv_summary_v15b);

			set @sep = " ## ";
			set @lab_encounter_type = 99999;
			set @death_encounter_type = 31;
            
            #drop table if exists hiv_monthly_report_dataset;
            create table if not exists hiv_monthly_report_dataset (
				date_created timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
                elastic_id bigint,
				endDate date,
				person_id int,
				birthdate date,
				age double,
				gender varchar(1),
				encounter_date date,
                visit_this_month boolean,
				rtc_date date,
                days_since_rtc_date int,
                on_schedule boolean,
				location_id mediumint,
                clinic_county varchar(250),
				clinic varchar(250),
				clinic_latitude double,
				clinic_longitude double,
				encounter_type int,
				death_date date,
				enrollment_date date,
				enrolled_this_month boolean,
				transfer_in_this_month boolean,
				transfer_in_location_id mediumint,
				transfer_in_date date,
				transfer_out_this_month boolean,
				transfer_out_location_id mediumint,
				transfer_out_date date,
				prev_status varchar(250),
				status varchar(250),
				next_status varchar(250),
                arv_first_regimen_location_id int,
				arv_first_regimen varchar(250),
				arv_first_regimen_start_date date,
				started_art_this_month boolean,
                art_revisit_this_month boolean,
                arv_start_date date,
				cur_arv_meds varchar(250),
                cur_arv_meds_strict varchar(250),
				cur_arv_line tinyint,
                cur_arv_line_strict tinyint,
				on_art_this_month boolean,
				eligible_for_vl boolean,
				vl_1 int,
				vl_1_date date,
				vl_in_past_year boolean,
				vl_2 int,
				vl_2_date date,
				tb_screen boolean,
				tb_screening_datetime datetime,
				tb_screening_result int,    
                tb_screened_this_month boolean,
                tb_screened_since_active boolean,
				tb_screened_positive_this_month boolean,
				tb_tx_start_date date,
				started_tb_tx_this_month boolean,
				pcp_prophylaxis_start_date date,
				started_pcp_prophylaxis_this_month boolean,
				on_pcp_prophylaxis_this_month boolean,
				ipt_start_date date,
				ipt_stop_date date,
				ipt_completion_date date,
				started_ipt_this_month boolean,
				on_ipt_this_month boolean,
				completed_ipt_past_12_months boolean,
				pregnant_this_month boolean,
				delivered_this_month boolean,
				condoms_provided_this_month boolean,
				condoms_provided_since_active boolean,
				started_modern_contraception_this_month boolean,
                modern_contraception_since_active boolean,
                contraceptive_method int,
                partner_status int,
                
                primary key elastic_id (elastic_id),
				index person_enc_date (person_id, encounter_date),
                index person_report_date (person_id, endDate),
                index endDate_location_id (endDate, location_id)
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
            
            
            SET @dyn_sql=CONCAT('delete t1 from hiv_monthly_report_dataset t1 join ',@queue_table,' t2 using (person_id);'); 
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

                SET @dyn_sql=CONCAT('insert into hiv_monthly_report_dataset_build_queue__0 (select * from ',@queue_table,' limit ',cycle_size,');'); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;
                
                drop temporary table if exists hiv_monthly_report_dataset_0;
				create temporary table hiv_monthly_report_dataset_0
				(select 
					concat(date_format(endDate,"%Y%m"),person_id) as elastic_id,
					endDate, 
					person_id,
					date(birthdate) as birthdate,
					case
						when timestampdiff(year,birthdate,endDate) > 0 then round(timestampdiff(year,birthdate,endDate),0)
						else round(timestampdiff(month,birthdate,endDate)/12,2)
					end as age,
					t3.gender,
					date(encounter_datetime) as encounter_date, 
                    
                    if(encounter_datetime between date_format(endDate,"%Y-%m-01")  and endDate,1,0) as visit_this_month,

					date(rtc_date) as rtc_date,
                    timestampdiff(day,rtc_date, endDate) as days_since_rtc_date,
                    if(date(encounter_datetime) = date(prev_clinical_rtc_date_hiv),1,0) as on_schedule,
					location_id,
                    t4.state_province as clinic_county,
                    # Add subcounty
					t4.name as clinic,
					t4.latitude as clinic_latitude,
					t4.longitude as clinic_longitude,
					encounter_type,
					date(t2.death_date) as death_date,

					enrollment_date,
					case
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
						when t2.death_date <= endDate then @status := "dead"
						when transfer_out_date < date_format(endDate,"%Y-%m-01") then @status := "transfer_out"
						when timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 30 day)),endDate) <= 90 then @status := "active"
						when timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 30 day)),endDate) > 90 then @status := "ltfu"
						else @status := "unknown"
					end as status,
					
					arv_first_regimen_location_id,
                    
					arv_first_regimen,
					date(arv_first_regimen_start_date) as arv_first_regimen_start_date,

					case
						when arv_first_regimen_start_date between date_format(endDate,"%Y-%m-01")  and endDate then 1
						else 0
					end as started_art_this_month,

					case
						when date_format(arv_first_regimen_start_date,"%Y-%m-01") != date_format(endDate,"%Y-%m-01") 
							AND cur_arv_meds is not null
							AND timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 30 day)),endDate) <= 90 then 1                            
						else 0
					end as art_revisit_this_month,
										
                    arv_start_date,
					cur_arv_meds,
                    cur_arv_meds_strict,
					cur_arv_line,
                    cur_arv_line_strict,

					# We are using liberal definition of cur_arv_meds such that
                    # if a patient previously on ART and not explicilty stopped, the
                    # patient is considered to be on ART. 
					case
						when cur_arv_meds is not null 
								AND timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 30 day)),endDate) <= 90
							then 1
						else 0
					end as on_art_this_month,
					
					
					timestampdiff(day,arv_start_date,endDate) as days_since_starting_art,
                    
					case
						when timestampdiff(month, arv_start_date,endDate) > 6 then 1
						else 0
					end as eligible_for_vl,
                                                            										
					vl_1, 
					date(vl_1_date) as vl_1_date,    

					if(timestampdiff(year,vl_1_date,endDate) < 1,1,0) as vl_in_past_year,


					vl_2,
					date(vl_2_date) as vl_2_date,

					
					tb_screen,
                    tb_screening_datetime,
                    tb_screening_result,
                    
					case
						when tb_screening_datetime between date_format(endDate, "%Y-%m-01") and endDate then 1
						else 0
					end as tb_screened_this_month,

					case
						when tb_screening_datetime >= date(encounter_datetime) 
							AND timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 30 day)),endDate) <= 90 then 1
						else 0
					end as tb_screened_since_active,
                    
					case
						when tb_screening_result = 6971
							AND tb_screening_datetime then 1
                        else 0
                    end as tb_screened_positive_this_month,

					date(tb_tx_start_date) as tb_tx_start_date,#need to know time period, i.e. screened this month or screened in past X months
					case
						when tb_tx_start_date between date_format(endDate,"%Y-%m-01")  and endDate then 1
						else 0
					end as started_tb_tx_this_month,

					date(pcp_prophylaxis_start_date) as pcp_prophylaxis_start_date,

					case
						when pcp_prophylaxis_start_date between date_format(endDate,"%Y-%m-01")  and endDate then 1
						else 0
					end as started_pcp_prophylaxis_this_month,
					
					case
						when pcp_prophylaxis_start_date is not null then 1
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

					0 as pregnant_this_month,
					0 as delivered_this_month,
					
					case
						when condoms_provided_date between date_format(endDate,"%Y-%m-01")  and endDate then 1
						else 0
					end as condoms_provided_this_month,
					
					case
						when condoms_provided_date >= date(encounter_datetime)
							AND timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 30 day)),endDate) <= 90 then 1                            
						else 0
					end as condoms_provided_since_active,


					case
						when modern_contraceptive_method_start_date between date_format(endDate,"%Y-%m-01")  and endDate then 1
						else 0
					end as started_modern_contraception_this_month,
                    
					case
						when modern_contraceptive_method_start_date >= date(encounter_datetime)
							AND timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 30 day)),endDate) <= 90 then 1                            
						else 0
					end as modern_contraception_since_active,
                    contraceptive_method,
                    partner_status

					
					from etl.dates t1
					join etl.flat_hiv_summary_v15b t2 
					join amrs.person t3 using (person_id)
					join amrs.location t4 using (location_id)
					join etl.hiv_monthly_report_dataset_build_queue__0 t5 using (person_id)
                    

					where  
							t2.encounter_datetime <= t1.endDate
							and (t2.next_clinical_datetime_hiv is null or t2.next_clinical_datetime_hiv > t1.endDate )
							and t2.is_clinical_encounter=1 
							and t1.endDate between start_date and date_add(now(),interval 4 month)
					order by person_id, endDate
				);
                
                

				set @prev_id = null;
				set @cur_id = null;
				set @cur_status = null;
				set @prev_status = null;

				drop temporary table if exists hiv_monthly_report_dataset_1;
				create temporary table hiv_monthly_report_dataset_1
				(select
					*,
					@prev_id := @cur_id as prev_id,
					@cur_id := person_id as cur_id,
					case
						when @prev_id=@cur_id then @prev_status := @cur_status
						else @prev_status := null
					end as next_status,	
					@cur_status := status as cur_status
						
					from hiv_monthly_report_dataset_0
					order by person_id, endDate desc
				);

				alter table hiv_monthly_report_dataset_1 drop prev_id, drop cur_id, drop cur_status;

				set @prev_id = null;
				set @cur_id = null;
				set @cur_status = null;
				set @prev_status = null;
				drop temporary table if exists hiv_monthly_report_dataset_2;
				create temporary table hiv_monthly_report_dataset_2
				(select
					*,
					@prev_id := @cur_id as prev_id,
					@cur_id := person_id as cur_id,
					case
						when @prev_id=@cur_id then @prev_status := @cur_status
						else @prev_status := null
					end as prev_status,	
					@cur_status := status as cur_status
						
					from hiv_monthly_report_dataset_1
					order by person_id, endDate
				);
                                
                select now();
				select count(*) as num_rows_to_be_inserted from hiv_monthly_report_dataset_2;
	
				#add data to table
				replace into hiv_monthly_report_dataset											  
				(select
					null, #date_created will be automatically set or updated
					elastic_id,
				endDate,
				person_id,
				birthdate,
				age,
				gender,
				encounter_date,
                visit_this_month,
				rtc_date,
                days_since_rtc_date,
                on_schedule,
				location_id,
                clinic_county,
				clinic,
				clinic_latitude,
				clinic_longitude,
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
                arv_first_regimen_location_id,
				arv_first_regimen,
				arv_first_regimen_start_date,
				started_art_this_month,
                art_revisit_this_month,
                arv_start_date,
				cur_arv_meds,
                cur_arv_meds_strict,
				cur_arv_line,
                cur_arv_line_strict,
				on_art_this_month,
				eligible_for_vl,
				vl_1,
				vl_1_date,
				vl_in_past_year,
				vl_2,
				vl_2_date,
				tb_screen,
				tb_screening_datetime,
				tb_screening_result,  
                tb_screened_this_month,
                tb_screened_since_active,
				tb_screened_positive_this_month,
				tb_tx_start_date,
				started_tb_tx_this_month,
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
				delivered_this_month,
				condoms_provided_this_month,
				condoms_provided_since_active,
				started_modern_contraception_this_month,
                modern_contraception_since_active,
                contraceptive_method,
                partner_status
					from hiv_monthly_report_dataset_2
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
                
                select count(*) into @num_in_hmrd from hiv_monthly_report_dataset;
                
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
			insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
			select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");

        END $$
DELIMITER ;
