DELIMITER $$
CREATE  PROCEDURE `generate_flat_onc_patient_history_v1_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN

					set @primary_table := "flat_onc_patient_history";
					set @query_type = query_type;
                     
                    set @total_rows_written = 0;
                    
                    set @encounter_types = "(69,70,86,145,146,147,160,38,39,40,41,42,45,130,141,148,149,150,151,175,142,143,169,170,177,91,92,89,90,93,94,184)";
                    set @clinical_encounter_types = "(69,70,86,145,146,147,160,38,39,40,41,42,45,130,141,148,149,150,151,175,142,143,169,170,177,91,92,89,90,93,94,184)";
                    set @non_clinical_encounter_types = "(-1)";
                    set @other_encounter_types = "(-1)";
                    
					set @start = now();
					set @table_version = "flat_onc_patient_history_v1.0";

					set session sort_buffer_size=512000000;

					set @sep = " ## ";
                    set @boundary = "!!";
					set @last_date_created = (select max(max_date_created) from etl.flat_obs);

					CREATE TABLE IF NOT EXISTS flat_onc_patient_history (
						date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
						person_id INT,
						uuid VARCHAR(100),
						encounter_id INT,
						encounter_datetime DATETIME,
						encounter_type INT,
						visit_id INT,
						visit_type_id SMALLINT,
						visit_start_datetime DATETIME,
						location_id SMALLINT,
                        program_id SMALLINT,
						is_clinical INT,
						enrollment_date DATETIME,
						prev_rtc_date DATETIME,
						rtc_date DATETIME,
						diagnosis INT,
						diagnosis_method INT,
						result_of_diagnosis INT,
						diagnosis_date DATETIME,
						cancer_type INT,
						breast_cancer_type INT,
						non_cancer_diagnosis INT,
						cancer_stage INT,
						overal_cancer_stage_group INT,
						cur_onc_meds VARCHAR(500),
						cur_onc_meds_dose VARCHAR(500),
						cur_onc_meds_frequency VARCHAR(500),
						cur_onc_meds_start_date DATETIME,
						cur_onc_meds_end_date DATETIME,
						oncology_treatment_plan INT,
						reasons_for_surgery INT,
						chemotherapy_intent INT,
						chemotherapy_plan INT,
						drug_route VARCHAR(500),
						medication_history INT,
						other_meds_added INT,
						sickle_cell_drugs INT,
						PRIMARY KEY encounter_id (encounter_id),
						INDEX person_date (person_id , encounter_datetime),
						INDEX location_id_enc_date (location_id , encounter_datetime),
						INDEX enc_date_location_id (encounter_datetime , location_id),
						INDEX location_id_rtc_date (location_id , rtc_date),
						INDEX encounter_type (encounter_type),
						INDEX date_created (date_created)
					);
                        
							
					
                   if(@query_type="build") then
							select 'BUILDING..........................................';

                            set @write_table = concat("flat_onc_patient_history_temp_",queue_number);
							set @queue_table = concat("flat_onc_patient_history_build_queue_",queue_number);
                            set @primary_queue_table = "flat_onc_patient_history_build_queue";
													
							SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

							SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ', @primary_queue_table, ' limit ', queue_size, ');'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  
							
							SET @dyn_sql=CONCAT('delete t1 from ', @primary_queue_table, ' t1 join ',@queue_table, ' t2 using (person_id);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

					end if;
	
					
					if (@query_type="sync") then
							select 'SYNCING..........................................';
							set @write_table = "flat_onc_patient_history";
							set @queue_table = "flat_onc_patient_history_sync_queue";
							CREATE TABLE IF NOT EXISTS flat_onc_patient_history_sync_queue (
								person_id INT PRIMARY KEY
							);                            
                            
							set @last_update = null;

							SELECT 
								MAX(date_updated)
							INTO @last_update FROM
								etl.flat_log
							WHERE
								table_name = @table_version;
														

							replace into flat_onc_patient_history_sync_queue
							(select distinct patient_id
								from amrs.encounter
								where date_changed > @last_update
							);

							replace into flat_onc_patient_history_sync_queue
							(select distinct person_id
								from etl.flat_obs
								where max_date_created > @last_update
							);

							replace into flat_onc_patient_history_sync_queue
							(select distinct person_id
								from etl.flat_lab_obs
								where max_date_created > @last_update
							);

							replace into flat_onc_patient_history_sync_queue
							(select distinct person_id
								from etl.flat_orders
								where max_date_created > @last_update
							);
                            
                            replace into flat_onc_patient_history_sync_queue
                            (select person_id from 
								amrs.person 
								where date_voided > @last_update);


                            replace into flat_onc_patient_history_sync_queue
                            (select person_id from 
								amrs.person 
								where date_changed > @last_update);
                                

					  end if;
                      

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

					SELECT @person_ids_count AS 'num patients to update';

                    SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
                    set @total_time=0;
                    set @cycle_number = 0;
                    

					while @person_ids_count > 0 do

						set @loop_start_time = now();
                        
						drop temporary table if exists flat_onc_patient_history_build_queue__0;
						SET @dyn_sql=CONCAT('create temporary table flat_onc_patient_history_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                    
						drop temporary table if exists flat_onc_patient_history_0a;
                        
						SET @dyn_sql = CONCAT(
								'create temporary table flat_onc_patient_history_0a
								(select
									t1.person_id,
                                    t4.uuid,
									t1.encounter_id,
									t1.encounter_datetime,
									t1.encounter_type,
                                    t1.obs,
									t1.obs_datetimes,
                                    t1.visit_id,
                                    v.visit_type_id,
                                    v.date_started as visit_start_datetime,
									t1.location_id,
                                    t5.program_id,
									case
										when t1.encounter_type in ',@clinical_encounter_types,' then 1
										else null
									end as is_clinical_encounter,

									case
										when t1.encounter_type in ',@non_clinical_encounter_types,' then 20
										when t1.encounter_type in ',@clinical_encounter_types,' then 10
										when t1.encounter_type in', @other_encounter_types, ' then 5
										else 1
									end as encounter_type_sort_index,
									t2.orders
								    from etl.flat_obs t1
									join flat_onc_patient_history_build_queue__0 t0 on t0.person_id = t1.person_id  
									left join etl.flat_orders t2 using(encounter_id)
                                    left join amrs.visit v on (v.visit_id = t1.visit_id)
                                    join amrs.person t4 on t4.person_id = t1.person_id  
                                    left join etl.clinical_encounter_type_map t5 on (t5.encounter_type = t1.encounter_type)
								where t1.encounter_type in ',@encounter_types,');');
                            
                        PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  

                        
						insert into flat_onc_patient_history_0a
						(select
							t1.person_id,
							p.uuid,
							t1.encounter_id,
							t1.test_datetime,
							t1.encounter_type,
                            t1.obs,
							null, #t1.obs_datetimes,
                            null , #t1.visit_id,
                            t3.visit_type_id,
                            t3.date_started as visit_start_datetime,
							t1.location_id,
                            t5.program_id,
							0 as is_clinical_encounter,
							1 as encounter_type_sort_index,
							null as orders
							from etl.flat_lab_obs t1
                            join amrs.person p on p.person_id = t1.person_id  
                            join amrs.visit t3 on t1.person_id = t3.patient_id  
							join flat_onc_patient_history_build_queue__0 t0 on t0.person_id = t1.person_id  
                            join etl.clinical_encounter_type_map t5 on t5.encounter_type = t1.encounter_type
						);


						drop temporary table if exists flat_onc_patient_history_0;
						create temporary table flat_onc_patient_history_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
						(select * from flat_onc_patient_history_0a
						order by person_id, date(encounter_datetime), encounter_type_sort_index
						);


						set @prev_rtc_date:=NULL;
						set @rtc_date:=NULL;
						set @diagnosis:=NULL;
						set @diagnosis_method:=NULL;
						set @result_of_diagnosis:=NULL;
						set @diagnosis_date:=NULL;
						set @cancer_type:=NULL;
						set @breast_cancer_type:=NULL;
						set @non_cancer_diagnosis:=NULL;
						set @cancer_stage:=NULL;
						set @overal_cancer_stage_group:=NULL;
						set @cur_onc_meds_start_date:=NULL;
						set @cur_onc_meds_end_date:=NULL;
						set @cur_onc_meds:=NULL;
						set @cur_onc_meds_dose:=NULL;
						set @cur_onc_meds_frequency:=NULL;
						set @oncology_treatment_plan:=NULL;
						set @reasons_for_surgery:=NULL;
						set @chemotherapy_intent:=NULL;
						set @chemotherapy_plan:=NULL;
						set @drug_route:=NULL;
						set @medication_history:=NULL;
						set @other_meds_added:=NULL;
						set @sickle_cell_drugs:=NULL;
						set @lab_encounter_type:=99999;
                        set @prev_id = null;
						set @cur_id = null;
						set @enrollment_date = null;
						set @cur_rtc_date = null;
						set @prev_rtc_date = null;
                        
                                                
						drop temporary table if exists flat_onc_patient_history_1;
						create temporary table flat_onc_patient_history_1 #(index encounter_id (encounter_id))
						(select 
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,
                            t1.person_id,
                            t1.uuid,
                            t1.encounter_id,
							t1.encounter_datetime,
							t1.encounter_type,
							t1.visit_id,
                            t1.visit_type_id,
                            t1.visit_start_datetime,
						    t1.location_id,
                            t1.program_id,
                            t1.is_clinical_encounter,
                            case
								when @prev_id != @cur_id and t1.encounter_type in (21,@lab_encounter_type) then @enrollment_date := null
								when @prev_id != @cur_id then @enrollment_date := encounter_datetime
								when t1.encounter_type not in (21,@lab_encounter_type) and @enrollment_date is null then @enrollment_date := encounter_datetime
								else @enrollment_date
							end as enrollment_date,
                            case
						        when @prev_id=@cur_id then @prev_rtc_date := @cur_rtc_date
						        else @prev_rtc_date := null
							end as prev_rtc_date,
                            case
								when obs regexp "!!5096=" then @cur_rtc_date := replace(replace((substring_index(substring(obs,locate("!!5096=",obs)),@sep,1)),"!!5096=",""),"!!","")
								when @prev_id = @cur_id then if(@cur_rtc_date > encounter_datetime,@cur_rtc_date,null)
								else @cur_rtc_date := null
							end as rtc_date,
                            case
								when obs regexp "!!6042=" then @diagnosis := replace(replace((substring_index(substring(obs,locate("!!6042=",obs)),@sep,1)),"!!6042=",""),"!!","")
								else @diagnosis := null
							end as diagnosis,
                            @diagnosis_method := GetValues(obs, 6504) as diagnosis_method,
                            @result_of_diagnosis := cast(replace(replace((substring_index(substring(obs,locate("!!7191=",obs)),@sep,1)),"!!7191=",""),"!!","") as unsigned) as result_of_diagnosis,
                            case
                                when obs regexp "!!9728=" then @diagnosis_date := replace(replace((substring_index(substring(obs,locate("!!9728=",obs)),@sep,1)),"!!9728=",""),"!!","")
                                else @diagnosis_date := null
							end as diagnosis_date,
                            case
								when obs regexp "!!7176=" then @cancer_type := GetValues(obs, 7176)
								else @cancer_type:= null
							end as cancer_type,
                            case
								when obs regexp "!!9841=" then @breast_cancer_type := GetValues(obs, 9841)
								else @breast_cancer_type:= null
							end as breast_cancer_type,
                            case
								when obs regexp "!!9847=" then @non_cancer_diagnosis := GetValues(obs, 9847)
								else @non_cancer_diagnosis:= null
							end as non_cancer_diagnosis,
                            case
								when obs regexp "!!6582=" then @cancer_stage:= GetValues(obs, 6582)
								else @cancer_stage:= null
							end as cancer_stage,
                            case
								when obs regexp "!!9868=" then @overal_cancer_stage_group:= GetValues(obs,9868)
								else @overal_cancer_stage_group:= null
							end as overal_cancer_stage_group,
                            case
								when obs regexp "!!9869=(1260)!!" then @cur_onc_meds := null
								when obs regexp "!!9918=" then @cur_onc_meds := GetValues(obs, 9918)
								#when obs regexp "!!7196=" then @cur_onc_meds := GetValues(obs, 7196)
								# when obs regexp "!!6643=" then @cur_onc_meds := GetValues(obs, 6643)
								when @prev_id=@cur_id then @cur_onc_meds
								else @cur_onc_meds:= null
							end as cur_onc_meds,
							case
								when obs regexp "!!1899=" then @cur_onc_meds_dose := GetValues(obs, 1899)
								else @cur_onc_meds_dose:= null
							end as cur_onc_meds_dose,
							case
								when obs regexp "!!1896=" then @cur_onc_meds_frequency := GetValues(obs, 1896)
								else @cur_onc_meds_frequency:= null
							end as cur_onc_meds_frequency,
                            case
								when obs regexp "!!9869=(1256|1259)" or (obs regexp "!!9869=(1257|1259)!!" and @cur_onc_meds_start_date is null ) then @cur_onc_meds_start_date := date(t1.encounter_datetime)
								when obs regexp "!!9869=(1260)!!" then @cur_onc_meds_start_date := null
								when @prev_id != @cur_id then @cur_onc_meds_start_date := null
								else @cur_onc_meds_start_date
							end as cur_onc_meds_start_date,
                            null as cur_onc_meds_end_date,
                            case
                              when obs regexp "!!8723=" then @oncology_treatment_plan := GetValues(obs, 8723)
                              else null
                            end as oncology_treatment_plan,
                            case
                              when obs regexp "!!8725=" then @reasons_for_surgery := GetValues(obs,8725)
                              else null
                            end as reasons_for_surgery,
                            case
                              when obs regexp "!!2206=" then @chemotherapy_intent := GetValues(obs,2206)
                              else null
                            end as chemotherapy_intent,
							case
                              when obs regexp "!!9869=" then @chemotherapy_plan := GetValues(obs,9869)
                              else null
                            end as chemotherapy_plan,
                            case
                              when obs regexp "!!7463=" then @drug_route := GetValues(obs,7463)
                              else null
                            end as drug_route,
                            case
                              when obs regexp "!!9918=" then @medication_history := GetValues(obs,9918)
                              else null
                            end as medication_history,
                            case
                              when obs regexp "!!10198=" then @other_meds_added := GetValues(obs,10198)
                              else null
                            end as other_meds_added,
                            case
                              when obs regexp "!!9710=" then @sickle_cell_drugs := GetValues(obs,9710)
                              else null
                            end as sickle_cell_drugs
                            
						from flat_onc_patient_history_0 t1
							join amrs.person p using (person_id)
						order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
						);# limit 100;

						
						set @prev_id = null;
						set @cur_id = null;


						alter table flat_onc_patient_history_1 drop prev_id, drop cur_id;
                                        


					SELECT COUNT(*) INTO @new_encounter_rows FROM flat_onc_patient_history_1;
                    
					SELECT @new_encounter_rows;                    
					set @total_rows_written = @total_rows_written + @new_encounter_rows;
					SELECT @total_rows_written;
	

					SET @dyn_sql=CONCAT('replace into ',@write_table,											  
						'(select
								null,
								person_id,
								uuid,
								encounter_id,
								encounter_datetime,
								encounter_type,
                                visit_id,
                                visit_type_id,
                                visit_start_datetime,
								location_id,
                                program_id,
								is_clinical_encounter,
								enrollment_date,
								prev_rtc_date,
								rtc_date,
								diagnosis,
								diagnosis_method,
								result_of_diagnosis,
								diagnosis_date,
								cancer_type,
								breast_cancer_type,
								non_cancer_diagnosis,
								cancer_stage,
								overal_cancer_stage_group,
                                cur_onc_meds,
                                cur_onc_meds_dose,
								cur_onc_meds_frequency,
								cur_onc_meds_start_date,
								cur_onc_meds_end_date,
								oncology_treatment_plan,
								reasons_for_surgery,
								chemotherapy_intent,
								chemotherapy_plan,
								drug_route,
								medication_history,
								other_meds_added,
								sickle_cell_drugs

						from flat_onc_patient_history_1)');

					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;


					SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_onc_patient_history_build_queue__0 t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
					#select @person_ids_count := (select count(*) from flat_onc_patient_history_build_queue_2);                        
					SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  

					set @cycle_length = timestampdiff(second,@loop_start_time,now());                   
                    set @total_time = @total_time + @cycle_length;
                    set @cycle_number = @cycle_number + 1;
                    
                    set @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);
					SELECT 
						@person_ids_count AS 'persons remaining',
						@cycle_length AS 'Cycle time (s)',
						CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
						@remaining_time AS 'Est time remaining (min)';

				 end while;
                 
                 if(@query_type="build") then

						SET @dyn_sql=CONCAT('drop table ',@queue_table,';'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                        
                        SET @total_rows_to_write=0;
                        SET @dyn_sql=CONCAT("Select count(*) into @total_rows_to_write from ",@write_table);
                        PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;
                                                
						set @start_write = now();
						SELECT CONCAT(@start_write,' : Writing ',@total_rows_to_write,' to ',@primary_table);

						SET @dyn_sql=CONCAT('replace into ', @primary_table,
							'(select * from ',@write_table,');');
                        PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;
						
                        set @finish_write = now();
                        set @time_to_write = timestampdiff(second,@start_write,@finish_write);
						SELECT CONCAT(@finish_write,' : Completed writing rows. Time to write to primary table: ',@time_to_write,' seconds ');                        
                        
                        SET @dyn_sql=CONCAT('drop table ',@write_table,';'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                        
                        
				end if;
                
									
				set @ave_cycle_length = ceil(@total_time/@cycle_number);
				SELECT CONCAT('Average Cycle Length: ',@ave_cycle_length,' second(s)');
                set @end = now();
				insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
				SELECT CONCAT(@table_version,' : Time to complete: ',TIMESTAMPDIFF(MINUTE, @start, @end),' minutes');

END$$
DELIMITER ;
