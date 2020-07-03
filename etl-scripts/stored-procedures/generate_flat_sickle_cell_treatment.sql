DELIMITER $$
CREATE PROCEDURE `generate_flat_sickle_cell_treatment`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
					set @primary_table := "flat_sickle_cell_treatment";
					set @query_type = query_type;
                     
                    set @total_rows_written = 0;
                    
                    set @encounter_types = "(93,94)";
                    set @clinical_encounter_types = "(-1)";
                    set @non_clinical_encounter_types = "(-1)";
                    set @other_encounter_types = "(-1)";
                    
					set @start = now();
					set @table_version = "flat_sickle_cell_treatment_v1.0";

					set session sort_buffer_size=512000000;

					set @sep = " ## ";
                    set @boundary = "!!";
					set @last_date_created = (select max(max_date_created) from etl.flat_obs);

					#delete from etl.flat_log where table_name like "%flat_sickle_cell_treatment%";
					#drop table etl.flat_sickle_cell_treatment;


					#drop table if exists flat_sickle_cell_treatment;
					create table if not exists flat_sickle_cell_treatment (
							date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            person_id int,
							encounter_id int,
							encounter_type int,
							encounter_datetime datetime,
							visit_id int,
							location_id int,
							location_uuid varchar (100),
							#location_name char (100),
							gender char (100),
							age int,
                            cur_visit_type INT,
							who_refered_you INT,
							how_long_did_it_take_to_travel INT,
							mode_of_transportation_to_the_clinic INT,
							main_occupation INT,
							average_mount_of_income_per_month INT,
							the_number_of_people_live_with_you INT,
							patient_level_of_education INT,
							next_of_kin_level_of_education INT,
							current_marital_status INT,
							the_number_of_children INT,
							is_the_patient_currently_use_any_form_of_family_planning INT,
							is_the_patient_currently_breastfeed INT,
							does_the_patient_smoke_cigarettes INT,
							sticks_of_cigarette_perday INT,
							years_of_cigarette_use INT,
							duration_since_last_use_of_cigarette INT,
							does_the_patient_use_tobacco INT,
							does_the_patient_drink_alcohol INT,
							ecog_performance INT,
							general_exam INT,
							heent INT,
							chest INT,
							heart INT,
							abdomenexam INT,
							urogenital INT,
							extremities INT,
							testicular_exam INT,
							nodal_survey INT,
							musculoskeletal INT,
							neurologic INT,
							echo INT,
                            ct_head INT,
							ct_neck INT,
							ct_chest INT,
							ct_abdomen INT,
							ct_spine INT,
							mri_head INT,
							mri_neck INT,
							mri_chest INT,
							mri_abdomen INT,
							mri_spine INT,
							ultrasound_renal INT,
							ultrasound_hepatic INT,
							obstetric_ultrasound INT,
							mri_arms INT,
							mri_pelvic INT,
							mri_legs INT,
							abonimal_ultrasound INT,
							breast_ultrasound INT,
							x_ray_shoulder INT,
							x_ray_pelvis INT,
							x_ray_abdomen INT,
							x_ray_skull INT,
							x_ray_leg INT,
							x_ray_hand INT,
							x_ray_foot INT,
							x_ray_chest INT,
							x_ray_arm INT,
							x_ray_spine INT,
							diagnosis_establish INT,
							hemoglobin_f INT,
							hemoglobin_a INT,
							hemoglobin_s INT,
							hemoglobin_a2c INT,
							screening_diagnosis INT,
							diagnosis INT,
                            test_ordered INT,
							other_radiology varchar(500),
							other_laboratory varchar(500),
							vaccination_given INT,
							prescribed_drug INT,
							dosage_in_milligrams INT,
							duration_in_weeks INT,
							other_prescribed_drug varchar(500),
							referrals INT,
							return_to_clinic_date DATETIME,
                            
							prev_encounter_datetime_sickle_cell_treatment datetime,
							next_encounter_datetime_sickle_cell_treatment datetime,
							prev_encounter_type_sickle_cell_treatment mediumint,
							next_encounter_type_sickle_cell_treatment mediumint,
							prev_clinical_datetime_sickle_cell_treatment datetime,
							next_clinical_datetime_sickle_cell_treatment datetime,
							prev_clinical_location_id_sickle_cell_treatment mediumint,
							next_clinical_location_id_sickle_cell_treatment mediumint,
							prev_clinical_rtc_date_sickle_cell_treatment datetime,
							next_clinical_rtc_date_sickle_cell_treatment datetime,

							primary key encounter_id (encounter_id),
							index person_date (person_id, encounter_datetime),
							index location_enc_date (location_uuid,encounter_datetime),
							index enc_date_location (encounter_datetime, location_uuid),
							index loc_id_enc_date_next_clinical (location_id, encounter_datetime, next_clinical_datetime_sickle_cell_treatment),
							index encounter_type (encounter_type),
							index date_created (date_created)
							
						);
                        
							
					
                        if(@query_type="build") then
							select 'BUILDING..........................................';
							
#set @write_table = concat("flat_sickle_cell_treatment_temp_",1);
#set @queue_table = concat("flat_sickle_cell_treatment_build_queue_",1);                    												

                            set @write_table = concat("flat_sickle_cell_treatment_temp_",queue_number);
							set @queue_table = concat("flat_sickle_cell_treatment_build_queue_",queue_number);                    												
							

#drop table if exists flat_sickle_cell_treatment_temp_1;							
							SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

							#create  table if not exists @queue_table (person_id int, primary key (person_id));
							SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_sickle_cell_treatment_build_queue limit ', queue_size, ');'); 
#SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_sickle_cell_treatment_build_queue limit 500);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  
							
							#delete t1 from flat_sickle_cell_treatment_build_queue t1 join @queue_table t2 using (person_id)
							SET @dyn_sql=CONCAT('delete t1 from flat_sickle_cell_treatment_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

					end if;
	
					
					if (@query_type="sync") then
							select 'SYNCING..........................................';
							set @write_table = "flat_sickle_cell_treatment";
							set @queue_table = "flat_sickle_cell_treatment_sync_queue";
                            create table if not exists flat_sickle_cell_treatment_sync_queue (person_id int primary key);                            
                            
							set @last_update = null;

                            select max(date_updated) into @last_update from etl.flat_log where table_name=@table_version;

#select max(date_created) into @last_update from etl.flat_log where table_name like "%sickle_cell_treatment%";

#select @last_update;														
select "Finding patients in amrs.encounters...";

							replace into flat_sickle_cell_treatment_sync_queue
							(select distinct patient_id
								from amrs.encounter
								where date_changed > @last_update
							);
						
                        
select "Finding patients in flat_obs...";

							replace into flat_sickle_cell_treatment_sync_queue
							(select distinct person_id
								from etl.flat_obs
								where max_date_created > @last_update
							);


select "Finding patients in flat_lab_obs...";
							replace into flat_sickle_cell_treatment_sync_queue
							(select distinct person_id
								from etl.flat_lab_obs
								where max_date_created > @last_update
							);

select "Finding patients in flat_orders...";

							replace into flat_sickle_cell_treatment_sync_queue
							(select distinct person_id
								from etl.flat_orders
								where max_date_created > @last_update
							);
                            
                            replace into flat_sickle_cell_treatment_sync_queue
                            (select person_id from 
								amrs.person 
								where date_voided > @last_update);


                            replace into flat_sickle_cell_treatment_sync_queue
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


					#delete t1 from flat_sickle_cell_treatment t1 join @queue_table t2 using (person_id);
					SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
                    set @total_time=0;
                    set @cycle_number = 0;
                    

					while @person_ids_count > 0 do

						set @loop_start_time = now();
                        
						#create temp table with a set of person ids
						drop temporary table if exists flat_sickle_cell_treatment_build_queue__0;
						
                        #create temporary table flat_sickle_cell_treatment_build_queue__0 (select * from flat_sickle_cell_treatment_build_queue_2 limit 5000); #TODO _ change this when data_fetch_size changes

#SET @dyn_sql=CONCAT('create temporary table flat_sickle_cell_treatment_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit 100);'); 
						SET @dyn_sql=CONCAT('create temporary table flat_sickle_cell_treatment_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                    
						drop temporary table if exists flat_sickle_cell_treatment_0a;
						SET @dyn_sql = CONCAT(
								'create temporary table flat_sickle_cell_treatment_0a
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
									join flat_sickle_cell_treatment_build_queue__0 t0 using (person_id)
									left join etl.flat_orders t2 using(encounter_id)
								where t1.encounter_type in ',@encounter_types,');');
                            
                        PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                    
                    

                        
					
						insert into flat_sickle_cell_treatment_0a
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
								join flat_sickle_cell_treatment_build_queue__0 t0 using (person_id)
						);


						drop temporary table if exists flat_sickle_cell_treatment_0;
						create temporary table flat_sickle_cell_treatment_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
						(select * from flat_sickle_cell_treatment_0a
						order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
							set @cur_visit_type = null;
							set @who_refered_you = null;
							set @how_long_did_it_take_to_travel = null;
							set @mode_of_transportation_to_the_clinic = null;
							set @main_occupation = null;
							set @average_mount_of_income_per_month = null;
							set @the_number_of_people_live_with_you = null;
							set @patient_level_of_education = null;
							set @next_of_kin_level_of_education = null;
							set @current_marital_status = null;
							set @the_number_of_children = null;
							set @is_the_patient_currently_use_any_form_of_family_planning = null;
							set @is_the_patient_currently_breastfeed = null;
							set @does_the_patient_smoke_cigarettes = null;
							set @sticks_of_cigarette_perday = null;
							set @years_of_cigarette_use = null;
							set @duration_since_last_use_of_cigarette = null;
							set @does_the_patient_use_tobacco = null;
							set @does_the_patient_drink_alcohol = null;
							set @ecog_performance = null;
							set @general_exam = null;
							set @heent = null;
							set @chest = null;
							set @heart = null;
							set @abdomenexam = null;
							set @urogenital = null;
							set @extremities = null;
							set @testicular_exam = null;
							set @nodal_survey = null;
							set @musculoskeletal = null;
							set @neurologic = null;
							set @echo = null;
							set @ct_neck = null;
							set @ct_chest = null;
							set @ct_abdomen = null;
							set @ct_spine = null;
							set @mri_head = null;
							set @mri_neck = null;
							set @mri_chest = null;
							set @mri_abdomen = null;
							set @mri_spine = null;
							set @ultrasound_renal = null;
							set @ultrasound_hepatic = null;
							set @obstetric_ultrasound = null;
							set @mri_arms = null;
							set @mri_pelvic = null;
							set @mri_legs = null;
							set @abonimal_ultrasound = null;
							set @breast_ultrasound = null;
							set @x_ray_shoulder = null;
							set @x_ray_pelvis = null;
							set @x_ray_abdomen = null;
							set @x_ray_skull = null;
							set @x_ray_leg = null;
							set @x_ray_hand = null;
							set @x_ray_foot = null;
							set @x_ray_chest = null;
							set @x_ray_arm = null;
							set @x_ray_spine = null;
							set @diagnosis_establish = null;
							set @hemoglobin_f = null;
							set @hemoglobin_a = null;
							set @hemoglobin_s = null;
							set @hemoglobin_a2c = null;
							set @screening_diagnosis = null;
							set @diagnosis = null;
                            set @test_ordered = null;
							set @other_radiology = null;
							set @other_laboratory = null;
							set @vaccination_given = null;
							set @prescribed_drug = null;
							set @dosage_in_milligrams = null;
							set @duration_in_weeks = null;
							set @other_prescribed_drug = null;
							set @referrals = null;
							set @return_to_clinic_date = null;

                                                
						drop temporary table if exists flat_sickle_cell_treatment_1;
						create temporary table flat_sickle_cell_treatment_1 #(index encounter_id (encounter_id))
						(select 
							obs,
							encounter_type_sort_index,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,
							t1.person_id,
							t1.encounter_id,
							t1.encounter_type,
							t1.encounter_datetime,
							t1.visit_id,
							#t4.name as location_name,
						    t1.location_id,
                            t1.is_clinical_encounter,
							p.gender,
                            p.death_date,
							case
								when timestampdiff(year,birthdate,curdate()) > 0 then round(timestampdiff(year,birthdate,curdate()),0)
								else round(timestampdiff(month,birthdate,curdate())/12,2)
							end as age,
                            case	
								when obs regexp "!!1839=7850!!" then @cur_visit_type := 1
								when obs regexp "!!1839=2345!!" then @cur_visit_type := 2
								when obs regexp "!!1839=1246!!" then @cur_visit_type := 3
								when obs regexp "!!1839=1837!!" then @cur_visit_type := 4
								when obs regexp "!!1839=1838!!" then @cur_visit_type := 5
								else @cur_visit_type = null
					        end as cur_visit_type,
                            case	
								when obs regexp "!!6749=978!!" then @who_refered_you := 1
								when obs regexp "!!6749=7037!!" then @who_refered_you := 2
								when obs regexp "!!6749=6479!!" then @who_refered_you := 3
					        end as who_refered_you,
                            case	
								when obs regexp "!!5605=1049!!" then @how_long_did_it_take_to_travel := 1
								when obs regexp "!!5605=1050!!" then @how_long_did_it_take_to_travel := 2
								when obs regexp "!!5605=1051!!" then @how_long_did_it_take_to_travel := 3
                                when obs regexp "!!5605=6412!!" then @how_long_did_it_take_to_travel := 4
								when obs regexp "!!5605=6413!!" then @how_long_did_it_take_to_travel := 5
                                when obs regexp "!!5605=6414!!" then @how_long_did_it_take_to_travel := 6
					        end as how_long_did_it_take_to_travel,
                            case	
								when obs regexp "!!6468=6413!!" then @mode_of_transportation_to_the_clinic := 1
								when obs regexp "!!6468=6416!!" then @mode_of_transportation_to_the_clinic := 2
								when obs regexp "!!6468=6580!!" then @mode_of_transportation_to_the_clinic := 3
                                when obs regexp "!!6468=6469!!" then @mode_of_transportation_to_the_clinic := 4
								when obs regexp "!!6468=6470!!" then @mode_of_transportation_to_the_clinic := 5
                                when obs regexp "!!6468=6471!!" then @mode_of_transportation_to_the_clinic := 6
					        end as mode_of_transportation_to_the_clinic,
                            case	
								when obs regexp "!!1972=1967!!" then @main_occupation := 1
								when obs regexp "!!1972=6401!!" then @main_occupation := 2
								when obs regexp "!!1972=6408!!" then @main_occupation := 3
                                when obs regexp "!!1972=6966!!" then @main_occupation := 4
								when obs regexp "!!1972=1969!!" then @main_occupation := 5
                                when obs regexp "!!1972=1968!!" then @main_occupation := 6
                                when obs regexp "!!1972=1966!!" then @main_occupation := 7
								when obs regexp "!!1972=8407!!" then @main_occupation := 8
                                when obs regexp "!!1972=6284!!" then @main_occupation := 9
								when obs regexp "!!1972=8589!!" then @main_occupation := 10
                                when obs regexp "!!1972=5619!!" then @main_occupation := 11
                                when obs regexp "!!1972=5622!!" then @main_occupation := 12
					        end as main_occupation,
                            case	
								when obs regexp "!!7003=7002!!" then @average_mount_of_income_per_month := 1
								when obs regexp "!!7003=6313!!" then @average_mount_of_income_per_month := 2
								when obs regexp "!!7003=6314!!" then @average_mount_of_income_per_month := 3
                                when obs regexp "!!7003=6315!!" then @average_mount_of_income_per_month := 4
								when obs regexp "!!7003=6316!!" then @average_mount_of_income_per_month := 5
                                when obs regexp "!!7003=6317!!" then @average_mount_of_income_per_month := 6
					        end as average_mount_of_income_per_month,
                            case
								when obs regexp "!!6801=" then @the_number_of_people_live_with_you := GetValues(obs,6801) 
								else @the_number_of_people_live_with_you := null
							end as the_number_of_people_live_with_you,
                            case	
								when obs regexp "!!1605=1107!!" then @patient_level_of_education := 1
								when obs regexp "!!1605=6214!!" then @patient_level_of_education := 2
								when obs regexp "!!1605=6215!!" then @patient_level_of_education := 3
                                when obs regexp "!!1605=1604!!" then @patient_level_of_education := 4
					        end as patient_level_of_education,
                            case	
								when obs regexp "!!8749=1107!!" then @next_of_kin_level_of_education := 1
								when obs regexp "!!8749=6214!!" then @next_of_kin_level_of_education := 2
								when obs regexp "!!8749=6215!!" then @next_of_kin_level_of_education := 3
                                when obs regexp "!!8749=1604!!" then @next_of_kin_level_of_education := 4
					        end as next_of_kin_level_of_education,
                            case	
								when obs regexp "!!1054=1175!!" then @current_marital_status := 1
								when obs regexp "!!1054=1057!!" then @current_marital_status := 2
								when obs regexp "!!1054=5555!!" then @current_marital_status := 3
                                when obs regexp "!!1054=6290!!" then @current_marital_status := 4
								when obs regexp "!!1054=1060!!" then @current_marital_status := 5
                                when obs regexp "!!1054=1056!!" then @current_marital_status := 6
                                when obs regexp "!!1054=1058!!" then @current_marital_status := 7
                                when obs regexp "!!1054=1059!!" then @current_marital_status := 8
					        end as current_marital_status,
                            case
								when obs regexp "!!1728=" then @the_number_of_children := GetValues(obs,1728) 
								else @the_number_of_children := null
							end as the_number_of_children,
                            case	
								when obs regexp "!!6683=1065!!" then @is_the_patient_currently_use_any_form_of_family_planning := 1
								when obs regexp "!!6683=1066!!" then @is_the_patient_currently_use_any_form_of_family_planning := 2
								when obs regexp "!!6683=1679!!" then @is_the_patient_currently_use_any_form_of_family_planning := 3
					        end as is_the_patient_currently_use_any_form_of_family_planning,
                            case	
								when obs regexp "!!2056=1065!!" then @is_the_patient_currently_breastfeed := 1
								when obs regexp "!!2056=1066!!" then @is_the_patient_currently_breastfeed := 2
					        end as is_the_patient_currently_breastfeed,
                            case	
								when obs regexp "!!6473=1065!!" then @does_the_patient_smoke_cigarettes := 1
								when obs regexp "!!6473=1066!!" then @does_the_patient_smoke_cigarettes := 2
								when obs regexp "!!6473=1679!!" then @does_the_patient_smoke_cigarettes := 3
					        end as does_the_patient_smoke_cigarettes,
                            case
								when obs regexp "!!2069=" then @sticks_of_cigarette_perday := GetValues(obs,2069) 
								else @sticks_of_cigarette_perday := null
							end as sticks_of_cigarette_perday,
                            case
								when obs regexp "!!2070=" then @years_of_cigarette_use := GetValues(obs,2070) 
								else @years_of_cigarette_use := null
							end as years_of_cigarette_use,
                            case
								when obs regexp "!!2068=" then @duration_since_last_use_of_cigarette := GetValues(obs,2068) 
								else @duration_since_last_use_of_cigarette := null
							end as duration_since_last_use_of_cigarette,
                           case	
								when obs regexp "!!7973=1065!!" then @does_the_patient_use_tobacco := 1
								when obs regexp "!!7973=1066!!" then @does_the_patient_use_tobacco := 2
								when obs regexp "!!7973=1679!!" then @does_the_patient_use_tobacco := 3
					        end as does_the_patient_use_tobacco,
                            case	
								when obs regexp "!!1684=1065!!" then @does_the_patient_drink_alcohol := 1
								when obs regexp "!!1684=1066!!" then @does_the_patient_drink_alcohol := 2
								when obs regexp "!!1684=1679!!" then @does_the_patient_drink_alcohol := 3
					        end as does_the_patient_drink_alcohol,
                            case	
						when obs regexp "!!6584=1115!!" then @ecog_performance := 1
						when obs regexp "!!6584=6585!!" then @ecog_performance := 2
						when obs regexp "!!6584=6586!!" then @ecog_performance := 3
						when obs regexp "!!6584=6587!!" then @ecog_performance := 4
                        when obs regexp "!!6584=6588!!" then @ecog_performance := 5
						else @ecog_performance = null
					end as ecog_performance,
					case	
						when obs regexp "!!1119=1115!!" then @general_exam := 1
						when obs regexp "!!1119=5201!!" then @general_exam := 2
						when obs regexp "!!1119=5245!!" then @general_exam := 3
						when obs regexp "!!1119=215!!" then @general_exam := 4
                        when obs regexp "!!1119=589!!" then @general_exam := 5
						else @general_exam = null
					end as general_exam,
                    case	
						when obs regexp "!!1122=1118!!" then @heent := 1
						when obs regexp "!!1122=1115!!" then @heent := 2
						when obs regexp "!!1122=5192!!" then @heent := 3
						when obs regexp "!!1122=516!!" then @heent := 4
                        when obs regexp "!!1122=513!!" then @heent := 5
                        when obs regexp "!!1122=5170!!" then @heent := 6
                        when obs regexp "!!1122=5334!!" then @heent := 7
                        when obs regexp "!!1122=6672!!" then @heent := 8
						else @heent = null
					end as heent,
					case	
						when obs regexp "!!1123=1118!!" then @chest := 1
						when obs regexp "!!1123=1115!!" then @chest := 2
						when obs regexp "!!1123=5138!!" then @chest := 3
						when obs regexp "!!1123=5115!!" then @chest := 4
                        when obs regexp "!!1123=5116!!" then @chest := 5
                        when obs regexp "!!1123=5181!!" then @chest := 6
                        when obs regexp "!!1123=5127!!" then @chest := 7
						else @chest = null
					end as chest,
					case	
						when obs regexp "!!1124=1118!!" then @heart := 1
						when obs regexp "!!1124=1115!!" then @heart := 2
						when obs regexp "!!1124=1117!!" then @heart := 3
						when obs regexp "!!1124=550!!" then @heart := 4
                        when obs regexp "!!1124=5176!!" then @heart := 5
                        when obs regexp "!!1124=5162!!" then @heart := 6
                        when obs regexp "!!1124=5164!!" then @heart := 7
						else @heart = null
					end as heart,	
					case	
						when obs regexp "!!1125=1118!!" then @abdomenexam := 1
						when obs regexp "!!1125=1115!!" then @abdomenexam := 2
						when obs regexp "!!1125=5105!!" then @abdomenexam := 3
						when obs regexp "!!1125=5008!!" then @abdomenexam := 4
                        when obs regexp "!!1125=5009!!" then @abdomenexam := 5
                        when obs regexp "!!1125=581!!" then @abdomenexam := 6
                        when obs regexp "!!1125=5103!!" then @abdomenexam := 7
						else @abdomenexam = null
					end as abdomenexam,		
		            case	
						when obs regexp "!!1126=1118!!" then @urogenital := 1
						when obs regexp "!!1126=1115!!" then @urogenital := 2
						when obs regexp "!!1126=1116!!" then @urogenital := 3
						when obs regexp "!!1126=2186!!" then @urogenital := 4
                        when obs regexp "!!1126=864!!" then @urogenital := 5
                        when obs regexp "!!1126=6334!!" then @urogenital := 6
                        when obs regexp "!!1126=1447!!" then @urogenital := 7
                        when obs regexp "!!1126=5993!!" then @urogenital := 8
                        when obs regexp "!!1126=8998!!" then @urogenital := 9
						when obs regexp "!!1126=1489!!" then @urogenital := 10
                        when obs regexp "!!1126=8417!!" then @urogenital := 11
                        when obs regexp "!!1126=8261!!" then @urogenital := 12
						else @urogenital = null
					end as urogenital,
					case	
						when obs regexp "!!1127=1118!!" then @extremities := 1
						when obs regexp "!!1127=1115!!" then @extremities := 2
						when obs regexp "!!1127=590!!" then @extremities := 3
						when obs regexp "!!1127=7293!!" then @extremities := 4
                        when obs regexp "!!1127=134!!" then @extremities := 5
						else @extremities = null
					end as extremities,
                    case	
						when obs regexp "!!8420=1118!!" then @testicular_exam := 1
						when obs regexp "!!8420=1115!!" then @testicular_exam := 2
						when obs regexp "!!8420=1116!!" then @testicular_exam := 3
						else @testicular_exam = null
					end as testicular_exam,
                    case	
						when obs regexp "!!1121=1118!!" then @nodal_survey := 1
						when obs regexp "!!1121=1115!!" then @nodal_survey := 2
						when obs regexp "!!1121=1116!!" then @nodal_survey := 3
                        when obs regexp "!!1121=8261!!" then @nodal_survey := 4
						else @nodal_survey = null
					end as nodal_survey,
					case	
							when obs regexp "!!1128=1118!!" then @musculoskeletal := 1
							when obs regexp "!!1128=1115!!" then @musculoskeletal := 2
							when obs regexp "!!1128=1116!!" then @musculoskeletal := 3
							else @musculoskeletal = null
					end as musculoskeletal,
                    case	
						when obs regexp "!!1129=1118!!" then @neurologic := 1
						when obs regexp "!!1129=1115!!" then @neurologic := 2
						when obs regexp "!!1129=599!!" then @neurologic := 3
						when obs regexp "!!1129=497!!" then @neurologic := 4
                        when obs regexp "!!1129=5108!!" then @neurologic := 5
                        when obs regexp "!!1129=6005!!" then @neurologic := 6
						else @neurologic = null
					end as neurologic,
                    case	
						when obs regexp "!!1536=1115!!" then @echo := 1
						when obs regexp "!!1536=1116!!" then @echo := 2
						when obs regexp "!!1536=1532!!" then @echo := 3
						when obs regexp "!!1536=1533!!" then @echo := 4
                        when obs regexp "!!1536=1538!!" then @echo := 5
                        when obs regexp "!!1536=5622!!" then @echo := 6
						else @echo = null
					end as echo,
                    case	
						when obs regexp "!!846=1115!!" then @ct_head := 1
						when obs regexp "!!846=1116!!" then @ct_head := 2
						else @ct_head = null
					end as ct_head,
                    case	
						when obs regexp "!!9839=1115!!" then @ct_neck := 1
						when obs regexp "!!9839=1116!!" then @ct_neck := 2
						else @ct_neck = null
					end as ct_neck,
                    case	
						when obs regexp "!!7113=1115!!" then @ct_chest := 1
						when obs regexp "!!7113=1116!!" then @ct_chest := 2
						else @ct_chest = null
					end as ct_chest,
                    case	
						when obs regexp "!!7114=1115!!" then @ct_abdomen := 1
						when obs regexp "!!7114=1116!!" then @ct_abdomen := 2
						else @ct_abdomen = null
					end as ct_abdomen,
                    case	
						when obs regexp "!!9840=1115!!" then @ct_spine := 1
						when obs regexp "!!9840=1116!!" then @ct_spine := 2
						else @ct_spine = null
					end as ct_spine,
                    case	
						when obs regexp "!!9881=1115!!" then @mri_head := 1
						when obs regexp "!!9881=1116!!" then @mri_head := 2
						else @mri_head = null
					end as mri_head,
                    case	
						when obs regexp "!!9882=1115!!" then @mri_neck := 1
						when obs regexp "!!9882=1116!!" then @mri_neck := 2
						else @mri_neck = null
					end as mri_neck,
                    case	
						when obs regexp "!!9883=1115!!" then @mri_chest := 1
						when obs regexp "!!9883=1116!!" then @mri_chest := 2
						else @mri_chest = null
					end as mri_chest,
                    case	
						when obs regexp "!!9884=1115!!" then @mri_abdomen := 1
						when obs regexp "!!9884=1116!!" then @mri_abdomen := 2
						else @mri_abdomen = null
					end as mri_abdomen,
                    case	
						when obs regexp "!!9885=1115!!" then @mri_spine := 1
						when obs regexp "!!9885=1116!!" then @mri_spine := 2
						else @mri_spine = null
					end as mri_spine,
                    case	
						when obs regexp "!!7115=1115!!" then @ultrasound_renal := 1
						when obs regexp "!!7115=1116!!" then @ultrasound_renal := 2
						else @ultrasound_renal = null
					end as ultrasound_renal,
                    case	
						when obs regexp "!!852=1115!!" then @ultrasound_hepatic := 1
						when obs regexp "!!852=1116!!" then @ultrasound_hepatic := 2
						else @ultrasound_hepatic = null
					end as ultrasound_hepatic,
                    case	
						when obs regexp "!!6221=1115!!" then @obstetric_ultrasound := 1
						when obs regexp "!!6221=1116!!" then @obstetric_ultrasound := 2
						else @obstetric_ultrasound = null
					end as obstetric_ultrasound,
                    case	
						when obs regexp "!!9951=1115!!" then @mri_arms := 1
						when obs regexp "!!9951=1116!!" then @mri_arms := 2
						else @mri_arms = null
					end as mri_arms,
                    case	
						when obs regexp "!!9952=1115!!" then @mri_pelvic := 1
						when obs regexp "!!9952=1116!!" then @mri_pelvic := 2
						else @mri_pelvic = null
					end as mri_pelvic,
                    case	
						when obs regexp "!!9953=1115!!" then @mri_legs := 1
						when obs regexp "!!9953=1116!!" then @mri_legs := 2
						else @mri_legs = null
					end as mri_legs,
                    case	
						when obs regexp "!!845=1115!!" then @abonimal_ultrasound := 1
						when obs regexp "!!845=1116!!" then @abonimal_ultrasound := 2
						else @abonimal_ultrasound = null
					end as abonimal_ultrasound,
                    case	
						when obs regexp "!!9596=1115!!" then @breast_ultrasound := 1
						when obs regexp "!!9596=1116!!" then @breast_ultrasound := 2
						else @breast_ultrasound = null
					end as breast_ultrasound,
                    case	
						when obs regexp "!!394=1115!!" then @x_ray_shoulder := 1
						when obs regexp "!!394=1116!!" then @x_ray_shoulder := 2
						else @x_ray_shoulder = null
					end as x_ray_shoulder,
                    case	
						when obs regexp "!!392=1115!!" then @x_ray_pelvis := 1
						when obs regexp "!!392=1116!!" then @x_ray_pelvis := 2
						else @x_ray_pelvis = null
					end as x_ray_pelvis,
                    case	
						when obs regexp "!!101=1115!!" then @x_ray_abdomen := 1
						when obs regexp "!!101=1116!!" then @x_ray_abdomen := 2
						else @x_ray_abdomen = null
					end as x_ray_abdomen,
                    case	
						when obs regexp "!!386=1115!!" then @x_ray_skull := 1
						when obs regexp "!!386=1116!!" then @x_ray_skull := 2
						else @x_ray_skull = null
					end as x_ray_skull,
                    case	
						when obs regexp "!!380=1115!!" then @x_ray_leg := 1
						when obs regexp "!!380=1116!!" then @x_ray_leg := 2
						else @x_ray_leg = null
					end as x_ray_leg,
                    case	
						when obs regexp "!!382=1115!!" then @x_ray_hand := 1
						when obs regexp "!!382=1116!!" then @x_ray_hand := 2
						else @x_ray_hand = null
					end as x_ray_hand,
                    case	
						when obs regexp "!!384=1115!!" then @x_ray_foot := 1
						when obs regexp "!!384=1116!!" then @x_ray_foot := 2
						else @x_ray_foot = null
					end as x_ray_foot,
                    case	
						when obs regexp "!!12=1115!!" then @x_ray_chest := 1
						when obs regexp "!!12=1116!!" then @x_ray_chest := 2
						else @x_ray_chest = null
					end as x_ray_chest,
                    case	
						when obs regexp "!!377=1115!!" then @x_ray_arm := 1
						when obs regexp "!!377=1116!!" then @x_ray_arm := 2
						else @x_ray_arm = null
					end as x_ray_arm,
                    case	
						when obs regexp "!!390=1115!!" then @x_ray_spine := 1
						when obs regexp "!!390=1116!!" then @x_ray_spine := 2
						else @x_ray_spine = null
					end as x_ray_spine,
                    case	
						when obs regexp "!!6504=9009!!" then @diagnosis_establish := 1
						when obs regexp "!!6504=9008!!" then @diagnosis_establish := 2
						else @diagnosis_establish = null
					end as diagnosis_establish,
                    case
						when obs regexp "!!9010=" then @hemoglobin_f := GetValues(obs,9010) 
						else @hemoglobin_f := null
					end as hemoglobin_f,
                    case
						when obs regexp "!!9011=" then @hemoglobin_a := GetValues(obs,9011) 
						else @hemoglobin_a := null
					end as hemoglobin_a,
                    case
						when obs regexp "!!9699=" then @hemoglobin_s := GetValues(obs,9699) 
						else @hemoglobin_s := null
					end as hemoglobin_s,
                    case
						when obs regexp "!!9012=" then @hemoglobin_a2c := GetValues(obs,9012) 
						else @hemoglobin_a2c := null
					end as hemoglobin_a2c,
                    case	
						when obs regexp "!!9008=703!!" then @screening_diagnosis := 1
						when obs regexp "!!9008=664!!" then @screening_diagnosis := 2
						else @screening_diagnosis = null
					end as screening_diagnosis,
                    case
						when obs regexp "!!6042=" then @diagnosis := GetValues(obs,6042) 
						else @diagnosis := null
					end as diagnosis,
                    		case
								when obs regexp "!!1271=1019!!" then @test_ordered := 1
								when obs regexp "!!1271=790!!" then @test_ordered := 2
                                when obs regexp "!!1271=953!!" then @test_ordered := 3
                                when obs regexp "!!1271=6898!!" then @test_ordered := 4
                                when obs regexp "!!1271=9009!!" then @test_ordered := 5
								when obs regexp "!!1271=1327!!" then @test_ordered := 6
                                when obs regexp "!!1271=10205!!" then @test_ordered := 7
                                when obs regexp "!!1271=8595!!" then @test_ordered := 8
                                when obs regexp "!!1271=8596!!" then @test_ordered := 9
								when obs regexp "!!1271=10125!!" then @test_ordered := 10
                                when obs regexp "!!1271=10123!!" then @test_ordered := 11
                                when obs regexp "!!1271=846!!" then @test_ordered := 12
                                when obs regexp "!!1271=9839!!" then @test_ordered := 13
								when obs regexp "!!1271=7113!!" then @test_ordered := 14
                                when obs regexp "!!1271=7114!!" then @test_ordered := 15
                                when obs regexp "!!1271=9840!!" then @test_ordered := 16
                                when obs regexp "!!1271=1536!!" then @test_ordered := 17
                                when obs regexp "!!1271=9881!!" then @test_ordered := 18
								when obs regexp "!!1271=9882!!" then @test_ordered := 19
                                when obs regexp "!!1271=9883!!" then @test_ordered := 20
                                when obs regexp "!!1271=9951!!" then @test_ordered := 21
                                when obs regexp "!!1271=9884!!" then @test_ordered := 22
								when obs regexp "!!1271=9952!!" then @test_ordered := 23
                                when obs regexp "!!1271=9885!!" then @test_ordered := 24
                                when obs regexp "!!1271=9953!!" then @test_ordered := 25
                                when obs regexp "!!1271=845!!" then @test_ordered := 26
								when obs regexp "!!1271=9596!!" then @test_ordered := 27
                                when obs regexp "!!1271=7115!!" then @test_ordered := 28
                                when obs regexp "!!1271=852!!" then @test_ordered := 29
                                when obs regexp "!!1271=394!!" then @test_ordered := 30
								when obs regexp "!!1271=392!!" then @test_ordered := 31
                                when obs regexp "!!1271=101!!" then @test_ordered := 32
                                when obs regexp "!!1271=386!!" then @test_ordered := 33
                                when obs regexp "!!1271=380!!" then @test_ordered := 34
                                 when obs regexp "!!1271=882!!" then @test_ordered := 35
								when obs regexp "!!1271=384!!" then @test_ordered := 36
                                when obs regexp "!!1271=12!!" then @test_ordered := 37
                                when obs regexp "!!1271=377!!" then @test_ordered := 38
                                when obs regexp "!!1271=390!!" then @test_ordered := 39
								else @test_ordered := null
							end as test_ordered,
                            case
								when obs regexp "!!8190=" then @other_radiology := GetValues(obs,8190) 
								else @other_radiology := null
							end as other_radiology,
                            case
								when obs regexp "!!9538=" then @other_laboratory := GetValues(obs,9538) 
								else @other_laboratory := null
							end as other_laboratory,
                            case	
								when obs regexp "!!984=1107!!" then @vaccination_given := 1
								when obs regexp "!!984=6957!!" then @vaccination_given := 2
                                when obs regexp "!!984=9700!!" then @vaccination_given := 3
								else @vaccination_given = null
					        end as vaccination_given,
                            case	
								when obs regexp "!!8726=7211!!" then @prescribed_drug := 1
								when obs regexp "!!8726=257!!" then @prescribed_drug := 2
                                when obs regexp "!!8726=784!!" then @prescribed_drug := 3
                                when obs regexp "!!8726=97!!" then @prescribed_drug := 4
                                when obs regexp "!!8726=5622!!" then @prescribed_drug := 5
								else @prescribed_drug = null
					        end as prescribed_drug,
                            case
								when obs regexp "!!1899=" then @dosage_in_milligrams := GetValues(obs,1899) 
								else @dosage_in_milligrams := null
							end as dosage_in_milligrams,
                            case
								when obs regexp "!!1893=" then @duration_in_weeks := GetValues(obs,1893) 
								else @duration_in_weeks := null
							end as duration_in_weeks,
                            case
								when obs regexp "!!1915=" then @other_prescribed_drug := GetValues(obs,1915) 
								else @other_prescribed_drug := null
							end as other_prescribed_drug,
                            case
								when obs regexp "!!1272=1107!!" then @referrals := 1
								when obs regexp "!!1272=6569!!" then @referrals := 2
                                when obs regexp "!!1272=6571!!" then @referrals := 3
                                when obs regexp "!!1272=6572!!" then @referrals := 4
                                when obs regexp "!!1272=6573!!" then @referrals := 5
                                when obs regexp "!!1272=1905!!" then @referrals := 6
                                when obs regexp "!!1272=1286!!" then @referrals := 7
                                when obs regexp "!!1272=5622!!" then @referrals := 8
								else @referrals := null
							end as referrals,
                            case
								when obs regexp "!!5096=" then @return_to_clinic_date := GetValues(obs,5096) 
								else @return_to_clinic_date := null
							end as return_to_clinic_date
						
		
						from flat_sickle_cell_treatment_0 t1
							join amrs.person p using (person_id)
						order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
						);# limit 100;

						
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


						alter table flat_sickle_cell_treatment_1 drop prev_id, drop cur_id;

						drop table if exists flat_sickle_cell_treatment_2;
						create temporary table flat_sickle_cell_treatment_2
						(select *,
							@prev_id := @cur_id as prev_id,
							@cur_id := person_id as cur_id,

							case
								when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
								else @prev_encounter_datetime := null
							end as next_encounter_datetime_sickle_cell_treatment,

							@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
								else @next_encounter_type := null
							end as next_encounter_type_sickle_cell_treatment,

							@cur_encounter_type := encounter_type as cur_encounter_type,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as next_clinical_datetime_sickle_cell_treatment,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as next_clinical_location_id_sickle_cell_treatment,

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
							end as next_clinical_rtc_date_sickle_cell_treatment,

							case
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinical_rtc_date

							from flat_sickle_cell_treatment_1
							order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
						);

						alter table flat_sickle_cell_treatment_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;


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

						drop temporary table if exists flat_sickle_cell_treatment_3;
						create temporary table flat_sickle_cell_treatment_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
						(select
							*,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,

							case
						        when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
						        else @prev_encounter_type:=null
							end as prev_encounter_type_sickle_cell_treatment,	
                            @cur_encounter_type := encounter_type as cur_encounter_type,

							case
						        when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
						        else @prev_encounter_datetime := null
						    end as prev_encounter_datetime_sickle_cell_treatment,

                            @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as prev_clinical_datetime_sickle_cell_treatment,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as prev_clinical_location_id_sickle_cell_treatment,

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
							end as prev_clinical_rtc_date_sickle_cell_treatment,

							case
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinic_rtc_date

							from flat_sickle_cell_treatment_2 t1
							order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
                                        


					select count(*) into @new_encounter_rows from flat_sickle_cell_treatment_3;
                    
                    select @new_encounter_rows;                    
					set @total_rows_written = @total_rows_written + @new_encounter_rows;
                    select @total_rows_written;

					SET @dyn_sql=CONCAT('replace into ',@write_table,											  
						'(select
								null,
								person_id,
								encounter_id,
								encounter_type,
								encounter_datetime,
								visit_id,
							   location_id,
							   t2.uuid as location_uuid,
								gender,
								age,
								cur_visit_type,
								who_refered_you,
								how_long_did_it_take_to_travel,
								mode_of_transportation_to_the_clinic,
								main_occupation,
								average_mount_of_income_per_month,
								the_number_of_people_live_with_you,
								patient_level_of_education,
								next_of_kin_level_of_education,
								current_marital_status,
								the_number_of_children,
								is_the_patient_currently_use_any_form_of_family_planning,
								is_the_patient_currently_breastfeed,
								does_the_patient_smoke_cigarettes,
								sticks_of_cigarette_perday,
								years_of_cigarette_use,
								duration_since_last_use_of_cigarette,
								does_the_patient_use_tobacco,
								does_the_patient_drink_alcohol,
								ecog_performance,
								general_exam,
								heent,
								chest,
								heart,
								abdomenexam,
								urogenital,
								extremities,
								testicular_exam,
								nodal_survey,
								musculoskeletal,
								neurologic,
								echo,
                                ct_head,
								ct_neck,
								ct_chest,
								ct_abdomen,
								ct_spine,
								mri_head,
								mri_neck,
								mri_chest,
								mri_abdomen,
								mri_spine,
								ultrasound_renal,
								ultrasound_hepatic,
								obstetric_ultrasound,
								mri_arms,
								mri_pelvic,
								mri_legs,
								abonimal_ultrasound,
								breast_ultrasound,
								x_ray_shoulder,
								x_ray_pelvis,
								x_ray_abdomen,
								x_ray_skull,
								x_ray_leg,
								x_ray_hand,
								x_ray_foot,
								x_ray_chest,
								x_ray_arm,
								x_ray_spine,
								diagnosis_establish,
								hemoglobin_f,
								hemoglobin_a,
								hemoglobin_s,
								hemoglobin_a2c,
								screening_diagnosis,
								diagnosis,
                                test_ordered,
								other_radiology,
								other_laboratory,
								vaccination_given,
								prescribed_drug,
								dosage_in_milligrams,
								duration_in_weeks,
								other_prescribed_drug,
								referrals,
								return_to_clinic_date,
                                
                                prev_encounter_datetime_sickle_cell_treatment,
								next_encounter_datetime_sickle_cell_treatment,
								prev_encounter_type_sickle_cell_treatment,
								next_encounter_type_sickle_cell_treatment,
								prev_clinical_datetime_sickle_cell_treatment,
								next_clinical_datetime_sickle_cell_treatment,
								prev_clinical_location_id_sickle_cell_treatment,
								next_clinical_location_id_sickle_cell_treatment,
								prev_clinical_rtc_date_sickle_cell_treatment,
								next_clinical_rtc_date_sickle_cell_treatment

						from flat_sickle_cell_treatment_3 t1
                        join amrs.location t2 using (location_id))');

					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;


					SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_sickle_cell_treatment_build_queue__0 t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
					#select @person_ids_count := (select count(*) from flat_sickle_cell_treatment_build_queue_2);                        
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

#select 1;
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
