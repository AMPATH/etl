use etl;
drop procedure if exists generate_flat_appointment;
DELIMITER $$
CREATE PROCEDURE `generate_flat_appointment`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
					select @start := now();
					select @table_version := "flat_appointment_v1.0";
					set @primary_table := "flat_appointment";
					set @query_type = query_type;
                    set @queue_table = "";
                    set @total_rows_written = 0;


					set session sort_buffer_size=512000000;
					select @sep := " ## ";
					select @last_date_created := (select max(max_date_created) from etl.flat_obs);
        
					-- drop table if exists etl.flat_appointment;
					create table if not exists etl.flat_appointment
							(
                            date_created timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            person_id int,
							encounter_id int primary key,
							encounter_datetime datetime,
							visit_id int,
							visit_type_id smallint,
                            location_id smallint,
							program_id smallint,
                            department_id smallint,
							visit_start_datetime datetime,
							is_clinical boolean,

							prev_encounter_datetime datetime,
							next_encounter_datetime datetime,

							prev_encounter_type smallint,
							encounter_type smallint,
							next_encounter_type smallint,

							prev_rtc_date datetime,
							rtc_date datetime,
							med_pickup_rtc_date datetime,
							next_rtc_date datetime,
							scheduled_date datetime,
							isGeneralAppt smallint,
    						isMedPickupAppt smallint,

							prev_clinical_encounter_datetime datetime,
							next_clinical_encounter_datetime datetime,

							prev_clinical_rtc_date datetime,
							next_clinical_rtc_date datetime,

							prev_clinical_encounter_type smallint,
							next_clinical_encounter_type smallint,

							prev_program_encounter_datetime datetime,
							next_program_encounter_datetime datetime,

							prev_program_rtc_date datetime,
							next_program_rtc_date datetime,

							prev_program_encounter_type datetime,
							next_program_encounter_type smallint,
                            
                            prev_program_clinical_datetime datetime,
							next_program_clinical_datetime datetime,

							prev_program_clinical_rtc_date datetime,
							next_program_clinical_rtc_date datetime,
                            
                            prev_department_encounter_datetime datetime,
							next_department_encounter_datetime datetime,

							prev_department_rtc_date datetime,
							next_department_rtc_date datetime,

							prev_department_encounter_type datetime,
							next_department_encounter_type smallint,
                            
                            prev_department_clinical_datetime datetime,
							next_department_clinical_datetime datetime,

							prev_department_clinical_rtc_date datetime,
							next_department_clinical_rtc_date datetime,
                            
                            
                            index person_date (person_id, encounter_datetime),
							index location_rtc (location_id,rtc_date),
							index location_med_pickup_rtc_date(location_id,scheduled_date),
							index location_enc_date (location_id, encounter_datetime),
							index enc_date_location (encounter_datetime, location_id),
							index loc_id_enc_date_next_clinical (location_id, program_id, encounter_datetime, next_program_encounter_datetime),
							index encounter_type (encounter_type)
							);
                            
					if(query_type="build") then
							select 'BUILDING..........................................';
							set @write_table = concat("flat_appointment_temp_",queue_number);
                            set @primary_queue_table = "flat_appointment_build_queue";
							set @queue_table = concat("flat_appointment_build_queue_",queue_number); 

							SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

							#create  table if not exists @queue_table (person_id int, primary key (person_id));
							SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ', @primary_queue_table, ' limit ', queue_size, ');'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  
							
							#delete t1 from flat_hiv_summary_build_queue t1 join @queue_table t2 using (person_id)
							SET @dyn_sql=CONCAT('delete t1 from ', @primary_queue_table, ' t1 join ',@queue_table, ' t2 using (person_id);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

					end if;
	
					if(query_type="sync") then
							select "SYNCING.......................................";
							set @primary_queue_table = "flat_appointment_sync_queue";
                            set @write_table = concat("flat_appointment");
                            set @queue_table = "flat_appointment_sync_queue";
                            create table if not exists flat_hiv_summary_sync_queue (person_id int primary key);  
                            
							select @last_update := (select max(date_updated) from etl.flat_log where table_name=@table_version);

							# then use the max_date_created from amrs.encounter. This takes about 10 seconds and is better to avoid.
							select @last_update :=
								if(@last_update is null,
									(select max(e.date_created) from amrs.encounter e join etl.flat_appointment using (encounter_id)),
									@last_update);

							#otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
							select @last_update := if(@last_update,@last_update,'1900-01-01');
                            
                            
                            replace into etl.flat_appointment_sync_queue
							(select distinct patient_id #, min(encounter_datetime) as start_date
								from amrs.encounter
								where date_changed > @last_update
							);
                                    
							replace into flat_appointment_sync_queue
							(select distinct person_id #, min(encounter_datetime) as start_date
								from etl.flat_obs
								where max_date_created > @last_update
							#	group by person_id
							# limit 10
							);

					end if;
						
					create table if not exists flat_appointment_queue(person_id int, primary key (person_id));
                    
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
                    
                    SET @dyn_sql=CONCAT('delete t1 from ',@write_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  

					set @total_time=0;
                    set @cycle_number = 0;
                    

					while @person_ids_count > 0 do

							set @loop_start_time = now();
                        
							drop temporary table if exists flat_appointment_queue__0;
							SET @dyn_sql=CONCAT('create temporary table flat_appointment_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  



							drop temporary table if exists etl.mapping_data_0;
							create temporary table etl.mapping_data_0
							(select
									t1.patient_id as person_id,
									t1.encounter_id,
									t1.encounter_datetime, 
									t1.encounter_type,
									t1.visit_id,
									t1.location_id,
									t2.date_started as visit_start_datetime,
									t3.visit_type_id                                            
									,t4.obs
								from flat_appointment_queue__0 t5
									join amrs.encounter t1 on t5.person_id = t1.patient_id
									left outer join amrs.visit t2 using (visit_id)
									left outer join amrs.visit_type t3 using (visit_type_id)
									join etl.flat_obs t4 using (encounter_id)
								where t1.voided=0 
								order by t1.patient_id,t1.encounter_datetime
							);

							drop temporary table if exists etl.mapping_data;
							create temporary table etl.mapping_data
							(select t1.*,
									t5.is_clinical,
									t5.program_id,
                                    t7.department_id
								from etl.mapping_data_0 t1
								left outer join etl.clinical_encounter_type_map t5 using (encounter_type)
                                left outer join etl.program_department_map t7 using (program_id)
							);
                            

									#### Add "next" columns
									set @cur_id = null;
									set @prev_id = null;
									set @prev_encounter_datetime = null;
									set @cur_encounter_datetime = null;
									set @prev_rtc_date = null;
									set @rtc_date = null;
									set @prev_is_clinical = null;
									set @cur_is_clinical = null;
									set @prev_clinical_rtc_date = null;
									set @cur_clinical_rtc_date  = null;
									set @prev_encounter_type = null;
									set @cur_encounter_type = null;
                                    

									drop temporary table if exists etl.foo_0;
									create temporary table etl.foo_0
									(select t1.*,
										@prev_id := @cur_id as prev_id,
									#	@cur_id := person_id as cur_id,    
										@cur_id := person_id as cur_id,
										
										case
											when @prev_id=@cur_id then @prev_rtc_date := @cur_rtc_date
											else @prev_rtc_date := null
										end as next_rtc_date,

										# 5096 = return visit date
										case
											when obs regexp "!!5096=" then @cur_rtc_date := replace(replace((substring_index(substring(obs,locate("!!5096=",obs)),@sep,1)),"!!5096=",""),"!!","")
											when @prev_id = @cur_id then if(@cur_rtc_date > encounter_datetime,@cur_rtc_date,null)
											else @cur_rtc_date := null
										end as cur_rtc_date,

										case
												when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
												else @prev_encounter_datetime := null
											end as next_encounter_datetime,

										@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

										case
											when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
											else @prev_encounter_type := null
										end as next_encounter_type,

											@cur_encounter_type := encounter_type as cur_encounter_type,

									# Clinical Appointments

											case
												when @prev_id = @cur_id then @prev_is_clinical := @cur_is_clinical
												else @prev_is_clinical := null
											end as next_is_clinical,

											
											case
												when is_clinical
													then @cur_is_clinical := is_clinical
												else @cur_is_clinical := null
											end as cur_is_clinical,

											case
												when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
												else @prev_clinical_datetime := null
											end as next_clinical_datetime,


											case
												when is_clinical then @cur_clinical_datetime := encounter_datetime
												when @prev_id = @cur_id then @cur_clinical_datetime
												else @cur_clinical_datetime := null
											end as cur_clinical_datetime,

											case
												when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
												else @prev_clinical_rtc_date := null
											end as next_clinical_rtc_date,

											case
												when is_clinical then @cur_clinical_rtc_date := @cur_rtc_date
												when @prev_id = @cur_id then @cur_clinical_rtc_date
												else @cur_clinical_rtc_date:= null
											end as cur_clinical_rtc_date,
											
											case
												when @prev_id=@cur_id then @prev_clinical_encounter_type := @cur_clinical_encounter_type
												else @prev_clinical_encounter_type := null
											end as next_clinical_encounter_type,

											case
												when is_clinical then @cur_clinical_encounter_type := encounter_type 
												when @prev_id = @cur_id then @cur_clinical_encounter_type
												else @cur_clinical_encounter_type := null
											end as cur_clinical_encounter_type            

										
										from etl.mapping_data t1
										order by person_id, encounter_datetime desc
									 );   

									###***************************************************************************************************************************************
									# Adding "prev" columns

									alter table etl.foo_0 
										drop prev_id 
										,drop cur_id 
										,drop cur_rtc_date
										,drop cur_encounter_type
										,drop cur_encounter_datetime
										,drop cur_is_clinical
										,drop cur_clinical_datetime
										,drop cur_clinical_encounter_type
										,drop cur_clinical_rtc_date    
									;

									set @cur_id = null;
									set @prev_id = null;

									set @prev_encounter_datetime = null;
									set @cur_encounter_datetime = null;

									set @prev_rtc_date = null;
									set @rtc_date = null;

									set @prev_is_clinical = null;
									set @cur_is_clinical = null;

									set @prev_clinical_rtc_date = null;
									set @cur_clinical_rtc_date  = null;

									set @prev_encounter_type = null;
									set @cur_encounter_type = null;


									drop temporary table if exists etl.foo_1;
									create temporary table etl.foo_1
									(select t1.*,
										@prev_id := @cur_id as prev_id,
									#	@cur_id := person_id as cur_id,    
										@cur_id := person_id as cur_id,
										

#										case
#											when @prev_id=@cur_id then @prev_visit_id := @cur_visit_id
#											else @prev_visit_id := null
#										end as prev_visit_id,
									
#										@cur_visit_id := visit_id as cur_visit_id,
                                        
#                                        case
#											when @prev_id != @cur_id then @prev_visit_type_id := null
#											when @cur_visit_id != @prev_visit_id then @prev_visit_type_id := @cur_visit_type_id
#                                          else @prev_visit_type_id
#										end as prev_visit_type_id,
                                            
#										@cur_visit_type_id := visit_type_id as cur_visit_type_id,
	

										case
											when @prev_id=@cur_id then @prev_rtc_date := @cur_rtc_date
											else @prev_rtc_date := null
										end as prev_rtc_date,

										# 5096 = return visit date
										case
											when obs regexp "!!5096=" then @cur_rtc_date := replace(replace((substring_index(substring(obs,locate("!!5096=",obs)),@sep,1)),"!!5096=",""),"!!","")
											when @prev_id = @cur_id then if(@cur_rtc_date > encounter_datetime,@cur_rtc_date,null)
											else @cur_rtc_date := null
										end as cur_rtc_date,

										case
											when obs regexp "!!9605=" then @cur_med_rtc_date := replace(replace((substring_index(substring(obs,locate("!!9605=",obs)),@sep,1)),"!!9605=",""),"!!","")
											when @prev_id = @cur_id then if(@cur_med_rtc_date > encounter_datetime,@cur_med_rtc_date,null)
											else @cur_med_rtc_date := null
										end as cur_med_rtc_date,

										case
												when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
												else @prev_encounter_datetime := null
											end as prev_encounter_datetime,

										@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

										case
											when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
											else @prev_encounter_type := null
										end as prev_encounter_type,

											@cur_encounter_type := encounter_type as cur_encounter_type,

									# Clinical Appointments

											case
												when @prev_id = @cur_id then @prev_is_clinical := @cur_is_clinical
												else @prev_is_clinical := null
											end as prev_is_clinical,

											
											case
												when is_clinical
													then @cur_is_clinical := is_clinical
												else @cur_is_clinical := null
											end as cur_is_clinical,

											case
												when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
												else @prev_clinical_datetime := null
											end as prev_clinical_datetime,


											case
												when is_clinical then @cur_clinical_datetime := encounter_datetime
												when @prev_id = @cur_id then @cur_clinical_datetime
												else @cur_clinical_datetime := null
											end as cur_clinical_datetime,

											case
												when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
												else @prev_clinical_rtc_date := null
											end as prev_clinical_rtc_date,

											case
												when is_clinical then @cur_clinical_rtc_date := @cur_rtc_date
												when @prev_id = @cur_id then @cur_clinical_rtc_date
												else @cur_clinical_rtc_date:= null
											end as cur_clinical_rtc_date,
											
											case
												when @prev_id=@cur_id then @prev_clinical_encounter_type := @cur_clinical_encounter_type
												else @prev_clinical_encounter_type := null
											end as prev_clinical_encounter_type,

											case
												when is_clinical then @cur_clinical_encounter_type := encounter_type 
												when @prev_id = @cur_id then @cur_clinical_encounter_type
												else @cur_clinical_encounter_type := null
											end as cur_clinical_encounter_type            

										
										from etl.foo_0 t1
										order by person_id, encounter_datetime
									 );   


									# ***********************************************************************************************************************************
									# PROGAM BASED APPOINTMENT DATA
									# Adding "next" columns

									alter table etl.foo_1
										drop prev_id 
										,drop cur_id 
										,drop cur_encounter_type
										,drop cur_encounter_datetime
									;

#select * from foo_1;

									set @cur_id = null;
									set @prev_id = null;
									 
									set @prev_program_id = null;
									set @cur_program_id = null;

									set @prev_program_encounter_datetime = null;
									set @cur_program_encounter_datetime = null;

									set @prev_program_rtc_date = null;
									set @cur_program_rtc_date  = null;

									set @prev_program_encounter_type = null;
									set @cur_program_encounter_type  = null;
                                    
									set @prev_program_clinical_datetime = null;
									set @cur_program_clinical_datetime = null;
                                    
									set @prev_program_clinical_rtc_date = null;	
									set @cur_program_clinical_rtc_date = null;


									drop temporary table if exists etl.foo_2;
									create temporary table etl.foo_2
									(select *,
											@prev_id := @cur_id as prev_id,
											#	@cur_id := person_id as cur_id,    
											@cur_id := person_id as cur_id,
										
											case
												when @prev_id = @cur_id then @prev_program_id := @cur_program_id
												else @prev_program_id := null
											end as next_program_id,
											
											@cur_program_id := program_id as cur_program_id,

											case
												when @prev_id != @cur_id then @prev_program_datetime := null
												when @prev_program_id != @cur_program_id then @prev_program_datetime := null            
												else @prev_program_datetime := @cur_program_datetime
											end as next_program_encounter_datetime,

											case
												when program_id is not null then @cur_program_datetime := encounter_datetime 
                                                else @cur_program_datetime := null
											end as cur_program_encounter_datetime,

											case
												when @prev_id != @cur_id then @prev_program_rtc_date := null
												when @prev_program_id != @cur_program_id then @prev_program_rtc_date := null            
												else @prev_program_rtc_date := @cur_program_rtc_date
											end as next_program_rtc_date,

											@cur_program_rtc_date := cur_rtc_date as cur_program_rtc_date,        
													
											case
												when @prev_id!=@cur_id then @prev_program_encounter_type := null
												when @prev_program_id != @cur_program_id then @prev_program_encounter_type := null            
												else @prev_program_encounter_type := @cur_program_encounter_type
											end as next_program_encounter_type,

											@cur_program_encounter_type := encounter_type as cur_program_encounter_type,
                                            
											case
												when @prev_id != @cur_id then @prev_program_clinical_datetime := null
												when @prev_program_id != @cur_program_id then @prev_program_clinical_datetime := null  
												else @prev_program_clinical_datetime := @cur_program_clinical_datetime
											end as next_program_clinical_datetime,

											case
												when  program_id is not null and is_clinical then @cur_program_clinical_datetime := encounter_datetime 
												when @prev_id = @cur_id then @cur_program_clinical_datetime
												else @cur_program_clinical_datetime := null
											end cur_program_clinical_datetime,

										
											case
												when @prev_id != @cur_id then @prev_program_clinical_rtc_date := null
												when @prev_program_id != @cur_program_id then @prev_program_clinical_rtc_date := null            
												else @prev_program_clinical_rtc_date := @cur_program_clinical_rtc_date        
											end as next_program_clinical_rtc_date,

											case
												when  program_id is not null and is_clinical then @cur_program_clinical_rtc_date := cur_rtc_date 
												when @prev_id = @cur_id then @cur_program_clinical_rtc_date
												else @cur_program_clinical_rtc_date := null
											end cur_program_clinical_rtc_date


										from etl.foo_1
										order by person_id, program_id, encounter_datetime desc
									);

									
									#******************#******************
									# Adding Program "prev" columns

									alter table etl.foo_2
										drop prev_id 
										,drop cur_id 
										,drop cur_program_id
										,drop cur_program_encounter_datetime
										,drop cur_program_encounter_type
										,drop cur_program_rtc_date
										,drop cur_program_clinical_datetime
										,drop cur_program_clinical_rtc_date
									;


									set @cur_id = null;
									set @prev_id = null;
									 
									set @prev_program_id = null;
									set @cur_program_id = null;

									set @prev_program_encounter_datetime = null;
									set @cur_program_encounter_datetime = null;

									set @prev_program_rtc_date = null;
									set @cur_program_rtc_date  = null;

									set @prev_program_encounter_type = null;
									set @cur_program_encounter_type  = null;

									set @prev_program_clinical_datetime = null;
									set @cur_program_clinical_datetime = null;

									set @prev_program_clinical_rtc_date = null;	
									set @cur_program_clinical_rtc_date = null;


									drop temporary table if exists etl.foo_3;
									create temporary table etl.foo_3
									(select *,
											@prev_id := @cur_id as prev_id,
											#	@cur_id := person_id as cur_id,    
											@cur_id := person_id as cur_id,
										
											case
												when @prev_id = @cur_id then @prev_program_id := @cur_program_id
												else @prev_program_id := null
											end as prev_program_id,
											
											@cur_program_id := program_id as cur_program_id,

											case
												when @prev_id != @cur_id then @prev_program_datetime := null
												when @prev_program_id != @cur_program_id then @prev_program_datetime := null            
												else @prev_program_datetime := @cur_program_datetime
											end as prev_program_encounter_datetime,

											case
												when program_id is not null then @cur_program_datetime := encounter_datetime 
                                                else @cur_program_datetime := null
											end as cur_program_encounter_datetime,


											case
												when @prev_id != @cur_id then @prev_program_rtc_date := null
												when @prev_program_id != @cur_program_id then @prev_program_rtc_date := null            
												else @prev_program_rtc_date := @cur_program_rtc_date
											end as prev_program_rtc_date,

											case
												when program_id is not null then @cur_program_rtc_date := cur_rtc_date 
												else @cur_program_rtc_date := null
											end as cur_program_rtc_date,
													
											case
												when @prev_id!=@cur_id then @prev_program_encounter_type := null
												when @prev_program_id != @cur_program_id then @prev_program_encounter_type := null            
												else @prev_program_encounter_type := @cur_program_encounter_type
											end as prev_program_encounter_type,

											case
												when program_id is not null then @cur_program_encounter_type := encounter_type 
                                                else @cur_program_encounter_type := null
											end as cur_program_encounter_type,


											case
												when @prev_id != @cur_id then @prev_program_clinical_datetime := null
												when @prev_program_id != @cur_program_id then @prev_program_clinical_datetime := null  
												else @prev_program_clinical_datetime := @cur_program_clinical_datetime
											end as prev_program_clinical_datetime,

											case
												when  program_id is not null and is_clinical then @cur_program_clinical_datetime := encounter_datetime 
												when @prev_id = @cur_id then @cur_program_clinical_datetime
												else @cur_program_clinical_datetime := null
											end cur_program_clinical_datetime,


											case
												when @prev_id != @cur_id then @prev_program_clinical_rtc_date := null
												when @prev_program_id != @cur_program_id then @prev_program_clinical_rtc_date := null            
												else @prev_program_clinical_rtc_date := @cur_program_clinical_rtc_date        
											end as prev_program_clinical_rtc_date,

											case
												when  program_id is not null and is_clinical then @cur_program_clinical_rtc_date := cur_rtc_date 
												when @prev_id = @cur_id then @cur_program_clinical_rtc_date
												else @cur_program_clinical_rtc_date := null
											end cur_program_clinical_rtc_date

											
										from etl.foo_2
										order by person_id, program_id, encounter_datetime
									);




									# ***********************************************************************************************************************************
									# DEPARTMENT BASED APPOINTMENT DATA
									# Adding "next" columns


									alter table etl.foo_3
										drop prev_id 
										,drop cur_id 
										,drop cur_program_encounter_type
										,drop cur_program_encounter_datetime
                                        ,drop cur_program_clinical_datetime
                                        ,drop cur_program_rtc_date
                                        ,drop cur_program_clinical_rtc_date
                                        
									;

									set @cur_id = null;
									set @prev_id = null;
									 
									set @prev_department_id = null;
									set @cur_department_id = null;

									set @prev_department_datetime = null;
									set @cur_department_datetime = null;

									set @prev_department_rtc_date = null;
									set @cur_department_rtc_date  = null;

									set @prev_department_encounter_type = null;
									set @cur_department_encounter_type  = null;
                                    
									set @prev_department_clinical_datetime = null;
									set @cur_department_clinical_datetime = null;
									set @prev_department_clinical_rtc_date = null;	
									set @cur_department_clinical_rtc_date = null;


									drop temporary table if exists etl.foo_4;
									create temporary table etl.foo_4
									(select *,
											@prev_id := @cur_id as prev_id,
											#	@cur_id := person_id as cur_id,    
											@cur_id := person_id as cur_id,
										
											case
												when @prev_id = @cur_id then @prev_department_id := @cur_department_id
												else @prev_department_id := null
											end as next_department_id,
											
											@cur_department_id := department_id as cur_department_id,

											case
												when @prev_id != @cur_id then @prev_department_datetime := null
												when @prev_department_id != @cur_department_id then @prev_department_datetime := null            
												else @prev_department_datetime := @cur_department_datetime
											end as next_department_encounter_datetime,

											case
												when department_id is not null then @cur_department_datetime := encounter_datetime 
                                                else @cur_department_datetime := null
											end as cur_department_encounter_datetime,

											case
												when @prev_id != @cur_id then @prev_department_rtc_date := null
												when @prev_department_id != @cur_department_id then @prev_department_rtc_date := null            
												else @prev_department_rtc_date := @cur_department_rtc_date
											end as next_department_rtc_date,

											@cur_department_rtc_date := cur_rtc_date as cur_department_rtc_date,        
													
											case
												when @prev_id!=@cur_id then @prev_department_encounter_type := null
												when @prev_department_id != @cur_department_id then @prev_department_encounter_type := null            
												else @prev_department_encounter_type := @cur_department_encounter_type
											end as next_department_encounter_type,

											@cur_department_encounter_type := encounter_type as cur_department_encounter_type,
                                            
                                            case
												when @prev_id != @cur_id then @prev_department_clinical_datetime := null
												when @prev_department_id != @cur_department_id then @prev_department_clinical_datetime := null  
												else @prev_department_datetime := @cur_department_clinical_datetime
											end as next_department_clinical_datetime,

											case
												when  department_id is not null and is_clinical then @cur_department_clinical_datetime := encounter_datetime 
												when @prev_id = @cur_id then @cur_department_clinical_datetime
												else @cur_department_clinical_datetime := null
											end cur_department_clinical_datetime,


											case
												when @prev_id != @cur_id then @prev_department_clinical_rtc_date := null
												when @prev_department_id != @cur_department_id then @prev_department_clinical_rtc_date := null            
												else @prev_department_clinical_rtc_date := @cur_department_clinical_rtc_date        
											end as next_department_clinical_rtc_date,

											case
												when  department_id is not null and is_clinical then @cur_department_clinical_rtc_date := cur_rtc_date 
												when @prev_id = @cur_id then @cur_department_clinical_rtc_date
												else @cur_department_clinical_rtc_date := null
											end cur_department_clinical_rtc_date
                                            
                                            
										from etl.foo_3
										order by person_id, department_id, encounter_datetime desc
									);


									
									#******************#******************
									# Adding Department "prev" columns

									alter table etl.foo_4
										drop prev_id 
										,drop cur_id 
										,drop cur_department_id
										,drop cur_department_encounter_datetime
										,drop cur_department_encounter_type
										,drop cur_department_rtc_date
										,drop cur_department_clinical_datetime
										,drop cur_department_clinical_rtc_date
									;


									set @cur_id = -1;
									set @prev_id = -1;
									 
									set @prev_department_id = null;
									set @cur_department_id = null;

									set @prev_department_datetime = null;
									set @cur_department_datetime = null;

									set @prev_department_rtc_date = null;
									set @cur_department_rtc_date  = null;

									set @prev_department_encounter_type = null;
									set @cur_department_encounter_type  = null;

									set @prev_department_clinical_datetime = null;
									set @cur_department_clinical_datetime = null;

									set @prev_department_clinical_rtc_date = null;	
									set @cur_department_clinical_rtc_date = null;

									set @prev_location_id = null;	
									set @cur_location_id = null;

									drop temporary table if exists etl.foo_5;
									create temporary table etl.foo_5
									(select *,
											@prev_id := @cur_id as prev_id,  
											@cur_id := person_id as cur_id,

											case
												when @prev_id = @cur_id then @prev_location_id := @cur_location_id
												else @prev_location_id := null
											end as prev_location_id,

											-- Do not change location if patient is on inbetween visit or transit vist
											case
												when @prev_id = @cur_id and visit_type_id in (23, 24, 119, 124) and @cur_location_id is not null then
												@cur_location_id
												else
												@cur_location_id := location_id 
											end as cur_location_id,
										
											case
												when @prev_id = @cur_id then @prev_department_id := @cur_department_id
												else @prev_department_id := null
											end as prev_department_id,
											
											@cur_department_id := department_id as cur_department_id,

											case
												when @prev_id != @cur_id then @prev_department_datetime := null
												when @prev_department_id != @cur_department_id then @prev_department_datetime := null            
												else @prev_department_datetime := @cur_department_datetime
											end as prev_department_encounter_datetime,

											case
												when department_id is not null then @cur_department_datetime := encounter_datetime 
                                                else @cur_department_datetime := null
											end as cur_department_encounter_datetime,


											case
												when @prev_id != @cur_id then @prev_department_rtc_date := null
												when @prev_department_id != @cur_department_id then @prev_department_rtc_date := null            
												else @prev_department_rtc_date := @cur_department_rtc_date
											end as prev_department_rtc_date,

											case
												when department_id is not null then @cur_department_rtc_date := cur_rtc_date 
												else @cur_department_rtc_date := null
											end as cur_department_rtc_date,
													
											case
												when @prev_id!=@cur_id then @prev_department_encounter_type := null
												when @prev_department_id != @cur_department_id then @prev_department_encounter_type := null            
												else @prev_department_encounter_type := @cur_department_encounter_type
											end as prev_department_encounter_type,

											case
												when department_id is not null then @cur_department_encounter_type := encounter_type 
                                                else @cur_department_encounter_type := null
											end as cur_department_encounter_type,


											case
												when @prev_id != @cur_id then @prev_department_clinical_datetime := null
												when @prev_department_id != @cur_department_id then @prev_department_clinical_datetime := null  
												else @prev_department_clinical_datetime := @cur_department_clinical_datetime
											end as prev_department_clinical_datetime,

											case
												when  department_id is not null and is_clinical then @cur_department_clinical_datetime := encounter_datetime 
												when @prev_id = @cur_id then @cur_department_clinical_datetime
												else @cur_department_clinical_datetime := null
											end cur_department_clinical_datetime,


											case
												when @prev_id != @cur_id then @prev_department_clinical_rtc_date := null
												when @prev_department_id != @cur_department_id then @prev_department_clinical_rtc_date := null            
												else @prev_department_clinical_rtc_date := @cur_department_clinical_rtc_date        
											end as prev_department_clinical_rtc_date,

											case
												when  department_id is not null and is_clinical then @cur_department_clinical_rtc_date := cur_rtc_date 
												when @prev_id = @cur_id then @cur_department_clinical_rtc_date
												else @cur_department_clinical_rtc_date := null
											end cur_department_clinical_rtc_date

											
										from etl.foo_4
										order by person_id, program_id, encounter_datetime
									);

									-- Merge MedPickUp Dates and RTC dates to form schedule
									drop temporary table if exists A;
									create temporary table A(select encounter_id, cur_rtc_date as scheduled_date, 1 as isGeneralAppt, 0 as isMedPickupAppt from foo_5 where cur_med_rtc_date is null or cur_rtc_date = cur_med_rtc_date);

									drop temporary table  if exists B;
									create temporary table B(select encounter_id, cur_rtc_date as scheduled_date, 1 as isGeneralAppt, 0 as isMedPickupAppt from foo_5 where cur_med_rtc_date is not null and cur_rtc_date <> cur_med_rtc_date);

									drop temporary table  if exists  C;
									create temporary table C(select encounter_id, cur_med_rtc_date as scheduled_date, 0 as isGeneralAppt, 1 as isMedPickupAppt from foo_5 where cur_med_rtc_date is not null and cur_rtc_date <> cur_med_rtc_date);

									drop temporary table if exists merged;
									create temporary table merged(index enc_id(encounter_id)) (
									select * from
										(select * from A
										union
										select * from B
										union
										select * from C) tm
									);

									drop temporary table if exists final_stage;
									create temporary table final_stage(SELECT 
										a.*,
										scheduled_date,
										isGeneralAppt,
										isMedPickupAppt
									FROM
										merged f
											JOIN
										foo_5 a USING (encounter_id));

									SET @dyn_sql=CONCAT('replace into ',@write_table,
									'(
										date_created,
										person_id,
										encounter_id,
										encounter_datetime,
										visit_id,
										visit_type_id,
										location_id,
										program_id,
										department_id,
										visit_start_datetime,
										is_clinical,

										prev_encounter_datetime,
										next_encounter_datetime,

										prev_encounter_type,
										encounter_type,
										next_encounter_type,

										prev_rtc_date,
										rtc_date,
										med_pickup_rtc_date,
										next_rtc_date,
										scheduled_date,
										isGeneralAppt,
										isMedPickupAppt,

										prev_clinical_encounter_datetime,
										next_clinical_encounter_datetime,

										prev_clinical_rtc_date,
										next_clinical_rtc_date,

										prev_clinical_encounter_type,
										next_clinical_encounter_type,

										prev_program_encounter_datetime,
										next_program_encounter_datetime,

										prev_program_rtc_date,
										next_program_rtc_date,

										prev_program_encounter_type,
										next_program_encounter_type,
										
										prev_program_clinical_datetime,
										next_program_clinical_datetime,

										prev_program_clinical_rtc_date,
										next_program_clinical_rtc_date,
										
										prev_department_encounter_datetime,
										next_department_encounter_datetime,

										prev_department_rtc_date,
										next_department_rtc_date,

										prev_department_encounter_type,
										next_department_encounter_type,
										
										prev_department_clinical_datetime,
										next_department_clinical_datetime,

										prev_department_clinical_rtc_date,
										next_department_clinical_rtc_date
									 )',		
									'select
										NULL,
										person_id,
										encounter_id,
										encounter_datetime,
										visit_id,
										visit_type_id,
										cur_location_id,
										program_id,
                                        department_id,
										visit_start_datetime,
										is_clinical boolean,

										prev_encounter_datetime,
										next_encounter_datetime,

										prev_encounter_type,
										encounter_type,
										next_encounter_type,

										prev_rtc_date,
										cur_rtc_date,
										cur_med_rtc_date,
										next_rtc_date,
										scheduled_date,
										isGeneralAppt,
										isMedPickupAppt,

										prev_clinical_datetime,
										next_clinical_datetime,

										prev_clinical_rtc_date,
										next_clinical_rtc_date,

										prev_clinical_encounter_type,
										next_clinical_encounter_type,

										prev_program_encounter_datetime,
										next_program_encounter_datetime,

										prev_program_rtc_date,
										next_program_rtc_date,

										prev_program_encounter_type,
										next_program_encounter_type,
										
										prev_program_clinical_datetime,
										next_program_clinical_datetime,

										prev_program_clinical_rtc_date,
										next_program_clinical_rtc_date,
										
										
										prev_department_encounter_datetime,
										next_department_encounter_datetime,

										prev_department_rtc_date,
										next_department_rtc_date,

										prev_department_encounter_type,
										next_department_encounter_type,
										
										prev_department_clinical_datetime,
										next_department_clinical_datetime,

										prev_department_clinical_rtc_date,
										next_department_clinical_rtc_date
                                        
									from etl.final_stage
								;');

								-- select @dyn_sql;

                                PREPARE s1 from @dyn_sql; 
								EXECUTE s1; 
								DEALLOCATE PREPARE s1;  

								SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_appointment_queue__0 t2 using (person_id);'); 
								PREPARE s1 from @dyn_sql; 
								EXECUTE s1; 
								DEALLOCATE PREPARE s1;  
								
								SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';'); 
								PREPARE s1 from @dyn_sql; 
								EXECUTE s1; 
								DEALLOCATE PREPARE s1;  
								
								select @person_ids_count as remaining_in_build_queue;

								set @cycle_length = timestampdiff(second,@loop_start_time,now());
								select concat('Cycle time: ',@cycle_length,' seconds');                    
								set @total_time = @total_time + @cycle_length;
								set @cycle_number = @cycle_number + 1;
								
								select ceil(@person_ids_count / cycle_size) as remaining_cycles;
								set @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);
								select concat("Estimated time remaining: ", @remaining_time,' minutes');

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
                
				select @end := now();
				insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
				select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");

		END$$
DELIMITER ;
