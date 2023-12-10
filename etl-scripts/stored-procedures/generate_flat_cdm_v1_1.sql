DELIMITER $$
CREATE PROCEDURE `generate_flat_cdm_v1_1`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
					set @primary_table := "flat_cdm_v1";
					set @query_type = query_type;
#set @query_type = "build";
 
                    
                    set @total_rows_written = 0;
                    
                    set @encounter_types = "(54,55,75,76,77,78,79,83,96,99,100,104,107,108,109,131,171,172)";
                    set @clinical_encounter_types = "(54,55,75,76,77,78,79,83,96,104,107,108,109,171,172)";
                    set @non_clinical_encounter_types = "(131)";
                    set @other_encounter_types = "(-1)";
                    
					set @start = now();
					set @table_version = "flat_cdm_v1.1";

					set session sort_buffer_size=512000000;

					set @sep = " ## ";
                    set @boundary = "!!";
					set @last_date_created = (select max(max_date_created) from etl.flat_obs);

					create table if not exists flat_cdm_v1 (
						date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
						person_id int,
						uuid varchar(100),
						visit_id int,
						encounter_id int,
						encounter_datetime datetime,
						encounter_type int,
						is_clinical_encounter int,
						location_id int,
						location_uuid varchar(100),
						death_date datetime,
						prev_rtc_date datetime,
						rtc_date datetime,

                        lmp date,

						sbp smallint,
                        dbp smallint,
                        pulse smallint,
                        
						fbs decimal,
                        rbs decimal,
                        hb_a1c decimal,
                        hb_a1c_date datetime,

                        creatinine decimal,
                        creatinine_date datetime,
                        
                        total_cholesterol decimal,
                        hdl decimal,
                        ldl decimal,
                        triglycerides decimal,
						lipid_panel_date datetime,

                        dm_status mediumint,
                        htn_status mediumint,
                        dm_meds varchar(500),
                        htn_meds varchar(500),
                        prescriptions text,
                        
                        problems text,

						comorbidities text,  
						rheumatologic_disorder text,
                        kidney_disease text,
                        ckd_staging text,
                        cardiovascular_disorder text,
						neurological_disease text,
						has_past_mhd_tx text,
						eligible_for_depression_care text, 
						anxiety_condition text,
						convulsive_disorder text,
						mood_disorder text,
						indicated_mhd_tx text,
						prev_hbp_findings text,
						type_of_follow_up text,
						review_of_med_history text,
                        
						prev_encounter_datetime_cdm datetime,
						next_encounter_datetime_cdm datetime,
						prev_encounter_type_cdm mediumint,
						next_encounter_type_cdm mediumint,
						prev_clinical_datetime_cdm datetime,
						next_clinical_datetime_cdm datetime,
                        prev_clinical_location_id_cdm mediumint,
                        next_clinical_location_id_cdm mediumint,
						prev_clinical_rtc_date_cdm datetime,
                        next_clinical_rtc_date_cdm datetime,

                        primary key encounter_id (encounter_id),
                        index person_date (person_id, encounter_datetime),
						index person_uuid (uuid),
						index location_enc_date (location_uuid,encounter_datetime),
						index enc_date_location (encounter_datetime, location_uuid),
						index location_id_rtc_date (location_id,rtc_date),
                        index location_uuid_rtc_date (location_uuid,rtc_date),
                        index loc_id_enc_date_next_clinical (location_id, encounter_datetime, next_clinical_datetime_cdm),
                        index encounter_type (encounter_type),
                        index date_created (date_created)
                        
					);
                    
                    
                    					
					if(@query_type="build") then
							select 'BUILDING..........................................';                   												

                            set @write_table = concat("flat_cdm_temp_",queue_number);
							set @queue_table = concat("flat_cdm_build_queue_",queue_number);                    												
							
							
							SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

							SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_cdm_build_queue limit ', queue_size, ');'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  
							
							SET @dyn_sql=CONCAT('delete t1 from flat_cdm_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

					end if;
	
					
					if (@query_type="sync") then
							select 'SYNCING..........................................';
							set @write_table = "flat_cdm_v1";
							set @queue_table = "flat_cdm_sync_queue";
                            create table if not exists flat_cdm_sync_queue (person_id int primary key);                            
                            


							set @last_update = null;

                            select max(date_updated) into @last_update from etl.flat_log where table_name=@table_version;														

							replace into flat_cdm_sync_queue
							(select distinct patient_id
								from amrs.encounter
								where date_changed > @last_update
							);

							replace into flat_cdm_sync_queue
							(select distinct person_id
								from etl.flat_obs
								where max_date_created > @last_update
							);

							replace into flat_cdm_sync_queue
							(select distinct person_id
								from etl.flat_lab_obs
								where max_date_created > @last_update
							);

							replace into flat_cdm_sync_queue
							(select distinct person_id
								from etl.flat_orders
								where max_date_created > @last_update
							);
                            
                            replace into flat_cdm_sync_queue
                            (select person_id from 
								amrs.person 
								where date_voided > @last_update);


                            replace into flat_cdm_sync_queue
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

					select @person_ids_count as 'num patients to update';

					SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  


					drop temporary table if exists prescriptions;
					create temporary table prescriptions (encounter_id int primary key, prescriptions text)
						(
						select
							encounter_id,
							group_concat(obs separator ' $ ') as prescriptions
						from
						(
							select
								t2.encounter_id,
								obs_group_id,
								group_concat(
									case
										when value_coded is not null then concat(@boundary,o.concept_id,'=',value_coded,@boundary)
										when value_numeric is not null then concat(@boundary,o.concept_id,'=',value_numeric,@boundary)
										when value_datetime is not null then concat(@boundary,o.concept_id,'=',date(value_datetime),@boundary)
										when value_text is not null then concat(@boundary,o.concept_id,'=',value_text,@boundary)
										when value_drug is not null then concat(@boundary,o.concept_id,'=',value_drug,@boundary)
										when value_modifier is not null then concat(@boundary,o.concept_id,'=',value_modifier,@boundary)
									end
									order by o.concept_id,value_coded
									separator ' ## '
								) as obs
																
							from amrs.obs o
							join (select encounter_id, obs_id, concept_id as grouping_concept from amrs.obs where concept_id in (7307,7334)) t2 on o.obs_group_id = t2.obs_id
							group by obs_group_id
						) t
						group by encounter_id
					);
                        
                        
					set @total_time=0;
                    set @cycle_number = 0;
                    

					while @person_ids_count > 0 do

						set @loop_start_time = now();
                        
						#create temp table with a set of person ids
						drop temporary table if exists flat_cdm_build_queue__0;
						
						SET @dyn_sql=CONCAT('create temporary table flat_cdm_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  

						



						drop temporary table if exists flat_cdm_0a;
						SET @dyn_sql = CONCAT(
								'create temporary table flat_cdm_0a
								(select
									t1.person_id,
									t1.visit_id,
									t1.encounter_id,
									t1.encounter_datetime,
									t1.encounter_type,
									t1.location_id,
									t1.obs,
									t1.obs_datetimes,
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
									join flat_cdm_build_queue__0 t0 using (person_id)
									left join etl.flat_orders t2 using(encounter_id)
								where t1.encounter_type in ',@encounter_types,');');
                            
                        PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  

                            
						insert into flat_cdm_0a
						(select
							t1.person_id,
							null,
							t1.encounter_id,
							t1.test_datetime,
							t1.encounter_type,
							null, #t1.location_id,
							t1.obs,
							null, #obs_datetimes
							# in any visit, there many be multiple encounters. for this dataset, we want to include only clinical encounters (e.g. not lab or triage visit)
							0 as is_clinical_encounter,
							1 as encounter_type_sort_index,
							null
							from etl.flat_lab_obs t1
								join flat_cdm_build_queue__0 t0 using (person_id)
						);

						drop temporary table if exists flat_cdm_0;
						create temporary table flat_cdm_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
						(select * from flat_cdm_0a
						order by person_id, date(encounter_datetime), encounter_type_sort_index
						);



						set @prev_id = null;
						set @cur_id = null;
                        set @prev_encounter_date = null;
						set @cur_encounter_date = null;
						set @enrollment_date = null;
						set @cur_location = null;
						set @cur_rtc_date = null;
						set @prev_rtc_date = null;
                        
						set @death_date = null;
                        

						#TO DO
						# screened for cervical ca
						# exposed infant

						drop temporary table if exists flat_cdm_1;
						create temporary table flat_cdm_1 (index encounter_id (encounter_id))
						(select
							obs,
							encounter_type_sort_index,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,
							t1.person_id,
							p.uuid,
							t1.visit_id,
							t1.encounter_id,
							@prev_encounter_date := date(@cur_encounter_date) as prev_encounter_date,
                            @cur_encounter_date := date(encounter_datetime) as cur_encounter_date,
							t1.encounter_datetime,                            
							t1.encounter_type,
							t1.is_clinical_encounter,
                            
							death_date,													
							case
								when location_id then @cur_location := location_id
								when @prev_id = @cur_id then @cur_location
								else null
							end as location_id,

							case
						        when @prev_id=@cur_id then @prev_rtc_date := @cur_rtc_date
						        else @prev_rtc_date := null
							end as prev_rtc_date,

							# 5096 = return visit date
							case
								when obs regexp "!!5096=" then @cur_rtc_date := etl.GetValues(obs,5096)
								when @prev_id = @cur_id then if(@cur_rtc_date > encounter_datetime,@cur_rtc_date,null)
								else @cur_rtc_date := null
							end as cur_rtc_date,
                            
                            @lmp := etl.GetValues(obs,1836) as lmp,
                                                
							case
									when obs regexp "!!5085=" then @sbp := etl.GetValues(obs,5085)                                    
							end as sbp,                            
                            
                            @dbp := etl.GetValues(obs,5086) as dbp,

							@pulse := etl.GetValues(obs,5087) as pulse,

							@fbs := etl.GetValues(obs,6252) as fbs,
							@rbs := etl.GetValues(obs,887) as rbs,

                            case
								when obs regexp "!!6126=" then @hb_a1c := etl.GetValues(obs,6126) 
                                when @prev_id = @cur_id then @hb_a1c
                                else @hb_a1c := null						
							end as hb_a1c,
                            
							case
                                when obs regexp "!!6126=" then @hb_a1c_date := ifnull(etl.GetValues(obs_datetimes,6126),encounter_datetime)
								when @prev_id=@cur_id then @hb_a1c_date
                                else @hb_a1c_date:=null
                            end as hb_a1c_date,
                            
							case
								when obs regexp "!!790=" then @creatinine := etl.GetValues(obs,790) 
                                when @prev_id = @cur_id then @creatinine
                                else @creatinine := null						
							end as creatinine,
                            
							case
                                when obs regexp "!!790=" then @creatinine_date := ifnull(etl.GetValues(obs_datetimes,790),encounter_datetime)
								when @prev_id=@cur_id then @creatinine_date
                                else @creatinine_date:=null
                            end as creatinine_date,
                            

                            case
								when obs regexp "!!1006=" then @total_cholesterol := etl.GetValues(obs,1006) 
                                when @prev_id = @cur_id then @total_cholesterol
                                else @total_cholesterol := null						
							end as total_cholesterol,
                            
                            case
								when obs regexp "!!1007=" then @hdl := etl.GetValues(obs,1007) 
                                when @prev_id = @cur_id then @hdl
                                else @hdl := null						
							end as hdl,

                            case
								when obs regexp "!!1008=" then @ldl := etl.GetValues(obs,1008) 
                                when @prev_id = @cur_id then @ldl
                                else @ldl := null						
							end as ldl,

                            case
								when obs regexp "!!1009=" then @triglycerides := etl.GetValues(obs,1009) 
                                when @prev_id = @cur_id then @triglycerides
                                else @triglycerides := null						
							end as triglycerides,
                            
                            case
                                when obs regexp "!!1006=" then @lipid_panel_date := ifnull(etl.GetValues(obs_datetimes,1006),encounter_datetime)
								when @prev_id=@cur_id then @lipid_panel_date
                                else @lipid_panel_date:=null
                            end as lipid_panel_date,
                            
                            
							@dm_status := etl.GetValues(obs,7287) as dm_status,
                            @htn_status := etl.GetValues(obs,7288) as htn_status,
                            @dm_meds := etl.GetValues(obs,7290) as dm_meds,
							@htn_meds := etl.GetValues(obs,10241) as htn_meds,
                            t2.prescriptions as prescriptions,

                            @problems := concat(etl.GetValues(obs,6042 ), ' ## ', etl.GetValues(obs,11679)) as problems,
							@comorbidities := etl.GetValues(obs,10239 ) as comorbidities,
							@rheumatologic_disorder := etl.GetValues(obs,12293) as rheumatologic_disorder
                            @kidney_disease := etl.GetValues(obs, 6033) as kidney_disease,
                            @ckd_staging := etl.GetValues(obs,10101) as ckd_staging,
                            @cardiovascular_disorder := etl.GetValues(obs, 7971) as cardiovascular_disorder,
                            @neurological_disease := etl.GetValues(obs, 1129) as neurological_disease,
							@has_past_mhd_tx := etl.GetValues(obs, 10280) as has_past_mhd_tx,
							@eligible_for_depression_care := etl.GetValues(obs, 10293) as eligible_for_depression_care,
							@anxiety_condition := etl.GetValues(obs, 11231) as anxiety_condition,
							@convulsive_disorder := etl.GetValues(obs, 11791) as convulsive_disorder,
							@mood_disorder := etl.GetValues(obs, 11279) as mood_disorder,
							@indicated_mhd_tx := etl.GetValues(obs, 7781) as indicated_mhd_tx,
							@prev_hbp_findings := etl.GetValues(obs, 9092) as prev_hbp_findings,
							@type_of_follow_up := etl.GetValues(obs, 2332) as type_of_follow_up,
							@review_of_med_history := etl.GetValues(obs, 6245) as review_of_med_history
							
						from flat_cdm_0 t1
							join amrs.person p using (person_id)
                            left outer join prescriptions t2 using (encounter_id)						
						);
                        

						set @prev_id = null;
						set @cur_id = null;
						set @prev_encounter_datetime = null;
						set @cur_encounter_datetime = null;

						set @prev_clinical_datetime = null;
						set @cur_clinical_datetime = null;

						set @next_encounter_type = null;
						set @cur_encounter_type = null;

                        set @prev_clinical_location_id = null;
						set @cur_clinical_location_id = null;


						alter table flat_cdm_1 drop prev_id, drop cur_id;

						drop table if exists flat_cdm_2;
						create temporary table flat_cdm_2
						(select *,
							@prev_id := @cur_id as prev_id,
							@cur_id := person_id as cur_id,

							case
								when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
								else @prev_encounter_datetime := null
							end as next_encounter_datetime_cdm,

							@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
								else @next_encounter_type := null
							end as next_encounter_type_cdm,

							@cur_encounter_type := encounter_type as cur_encounter_type,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as next_clinical_datetime_cdm,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as next_clinical_location_id_cdm,

							case
								when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
								when @prev_id = @cur_id then @cur_clinical_datetime
								else @cur_clinical_datetime := null
							end as cur_clinic_datetime,

                            case
								when is_clinical_encounter then @cur_clinical_location_id := location_id
								when @prev_id = @cur_id then @cur_clinical_location_id
								else @cur_clinical_location_id := null
							end as cur_clinic_location_id,

						    case
								when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
								else @prev_clinical_rtc_date := null
							end as next_clinical_rtc_date_cdm,

							case
								when is_clinical_encounter then @cur_clinical_rtc_date := cur_rtc_date
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinical_rtc_date

							from flat_cdm_1
							order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
						);

						alter table flat_cdm_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;


						set @prev_id = null;
						set @cur_id = null;
						set @prev_encounter_type = null;
						set @cur_encounter_type = null;
						set @prev_encounter_datetime = null;
						set @cur_encounter_datetime = null;
						set @prev_clinical_datetime = null;
						set @cur_clinical_datetime = null;
                        set @prev_clinical_location_id = null;
						set @cur_clinical_location_id = null;

						drop temporary table if exists flat_cdm_3;
						create temporary table flat_cdm_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
						(select
							*,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,

							case
						        when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
						        else @prev_encounter_type:=null
							end as prev_encounter_type_cdm,	
                            @cur_encounter_type := encounter_type as cur_encounter_type,

							case
						        when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
						        else @prev_encounter_datetime := null
						    end as prev_encounter_datetime_cdm,

                            @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as prev_clinical_datetime_cdm,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as prev_clinical_location_id_cdm,

							case
								when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
								when @prev_id = @cur_id then @cur_clinical_datetime
								else @cur_clinical_datetime := null
							end as cur_clinical_datetime,

                            case
								when is_clinical_encounter then @cur_clinical_location_id := location_id
								when @prev_id = @cur_id then @cur_clinical_location_id
								else @cur_clinical_location_id := null
							end as cur_clinical_location_id,

							case
								when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
								else @prev_clinical_rtc_date := null
							end as prev_clinical_rtc_date_cdm,

							case
								when is_clinical_encounter then @cur_clinical_rtc_date := cur_rtc_date
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinic_rtc_date

							from flat_cdm_2 t1
							order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
                                        


					select count(*) into @new_encounter_rows from flat_cdm_3;
                    
                    select @new_encounter_rows;                    
					set @total_rows_written = @total_rows_written + @new_encounter_rows;
                    select @total_rows_written;
	
                    
					#add data to table
					SET @dyn_sql=CONCAT('replace into ',@write_table,											  
						'(select
                        null,
						person_id,
						t1.uuid,
						visit_id,
						encounter_id,
						encounter_datetime,
						encounter_type,
						is_clinical_encounter,
						location_id,
						t2.uuid as location_uuid,
						death_date,
						prev_rtc_date,
						cur_rtc_date,
                        lmp,
						sbp,
                        dbp,
                        pulse,
                        fbs,
                        rbs,
                        hb_a1c,
                        hb_a1c_date,
                        creatinine,
                        creatinine_date,
                        total_cholesterol,
                        hdl,
                        ldl,
                        triglycerides,
                        lipid_panel_date,
                        dm_status,
                        htn_status,
                        dm_meds,
                        htn_meds,
                        prescriptions,
                        problems,
						comorbidities,
						rheumatologic_disorder,
                        kidney_disease,
                        ckd_staging,
                        cardiovascular_disorder,
                        neurological_disease,
						has_past_mhd_tx,
						eligible_for_depression_care,
						anxiety_condition,
						convulsive_disorder,
						mood_disorder,
						indicated_mhd_tx,
						prev_hbp_findings,
						type_of_follow_up,
						review_of_med_history,
                        
						prev_encounter_datetime_cdm,
						next_encounter_datetime_cdm,
						prev_encounter_type_cdm,
						next_encounter_type_cdm,
						prev_clinical_datetime_cdm,
						next_clinical_datetime_cdm,
                        prev_clinical_location_id_cdm,
                        next_clinical_location_id_cdm,
						prev_clinical_rtc_date_cdm,
                        next_clinical_rtc_date_cdm
                        
                        from flat_cdm_3 t1
                        join amrs.location t2 using (location_id))');

					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    

				    #delete from @queue_table where person_id in (select person_id from flat_cdm_build_queue__0);

					SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_cdm_build_queue__0 t2 using (person_id);'); 
#					select @dyn_sql;
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
					#select @person_ids_count := (select count(*) from flat_cdm_build_queue_2);                        
					SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    #select @person_ids_count as remaining_in_build_queue;

					set @cycle_length = timestampdiff(second,@loop_start_time,now());
					#select concat('Cycle time: ',@cycle_length,' seconds');                    
                    set @total_time = @total_time + @cycle_length;
                    set @cycle_number = @cycle_number + 1;
                    
                    #select ceil(@person_ids_count / cycle_size) as remaining_cycles;
                    set @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);
                    #select concat("Estimated time remaining: ", @remaining_time,' minutes');

					select @person_ids_count as 'persons remaining', @cycle_length as 'Cycle time (s)', ceil(@person_ids_count / cycle_size) as remaining_cycles, @remaining_time as 'Est time remaining (min)';

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
						select concat(@start_write, " : Writing ",@total_rows_to_write, ' to ',@primary_table);

						SET @dyn_sql=CONCAT('replace into ', @primary_table,
							'(select * from ',@write_table,');');
                        PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;
						
                        set @finish_write = now();
                        set @time_to_write = timestampdiff(second,@start_write,@finish_write);
                        select concat(@finish_write, ' : Completed writing rows. Time to write to primary table: ', @time_to_write, ' seconds ');                        
                        
                        SET @dyn_sql=CONCAT('drop table ',@write_table,';'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                        
                        
				end if;
                
									
				set @ave_cycle_length = ceil(@total_time/@cycle_number);
                select CONCAT('Average Cycle Length: ', @ave_cycle_length, ' second(s)');
                
				 set @end = now();
				 insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
				 select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");

		END$$
DELIMITER ;
