DELIMITER $$
CREATE PROCEDURE `generate_flat_general_oncology_treatment_v1_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
					set @primary_table := "flat_general_oncology_treatment";
					set @query_type = query_type;
                     
                    set @total_rows_written = 0;
                    
                    set @encounter_types = "(38,39)";
                    set @clinical_encounter_types = "(-1)";
                    set @non_clinical_encounter_types = "(-1)";
                    set @other_encounter_types = "(-1)";
                    
					set @start = now();
					set @table_version = "flat_general_oncology_treatment_v1.0";

					set session sort_buffer_size=512000000;

					set @sep = " ## ";
                    set @boundary = "!!";
					set @last_date_created = (select max(max_date_created) from etl.flat_obs);

					#delete from etl.flat_log where table_name like "%flat_general_oncology_treatment%";
					#drop table etl.flat_general_oncology_treatment;


					#drop table if exists flat_general_oncology_treatment;
					create table if not exists flat_general_oncology_treatment (
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
							ecog_performance INT,
							general_exam INT,
							heent INT,
							breast_findings INT,
							breast_finding_location INT,
							breast_findingquadrant INT,
							chest INT,
							heart INT,
							abdomenexam INT,
							urogenital INT,
							extremities INT,
							testicular_exam INT,
							nodal_survey INT,
							musculoskeletal INT,
							skin_exam_findings INT,
							body_part INT,
							laterality INT,
							measure_first_direction INT,
							measure_second_direction INT,
							neurologic INT,
							bnody_part INT,
							patient_on_chemo INT,
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
							mri_arms INT,
							mri_pelvic INT,
							mri_legs INT,
							ultrasound_renal INT,
							ultrasound_hepatic INT,
							obstetric_ultrasound INT,
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
							method_of_diagnosis INT,
							diagnosis_on_biopsy INT,
							biopsy_consistent_clinic_susp INT,
							diagnosis_results varchar(500),
							cancer_type INT,
							other_cancer varchar(500),
							type_of_sarcoma INT,
							type_of_gu INT,
							type_of_gi_cancer INT,
							head_and_neck_cancer INT,
							gynecologic_cancer INT,
							lympoma_cancer INT,
							skin_cancer INT,
							breast_cancer INT,
							other_solid_cancer INT,
							type_of_leukemeia INT,
							non_cancer INT,
							diagnosis_date datetime,
							new_or_recurrence_cancer INT,
							cancer_stage INT,
                            overall_cancer_staging INT,
							test_ordered INT,
							other_radiology varchar(500),
							other_laboratory varchar(500),
							treatment_plan INT,
							other_treatment_plan varchar(500),
							hospitalization INT,
							radiation_location INT,
							surgery_reason INT,
							surgical_procedure varchar(500),
							hormonal_drug INT,
							targeted_therapies INT,
							targeted_therapies_frequency INT,
							targeted_therapy_start_date datetime,
							targeted_therapy_stop_date datetime,
							chemotherapy_plan INT,
							treatment_on_clinical_trial INT,
							chemo_start_date datetime,
							treatment_intent INT,
							chemo_regimen varchar(500),
							chemo_cycle INT,
							total_planned_chemo_cycle INT,
							chemo_drug INT,
							dosage_in_milligrams INT,
							drug_route INT,
							other_drugs INT,
							other_medication varchar(500),
							purpose INT, 
							referral_ordered INT,
							return_to_clinic_date datetime,
                            
							prev_encounter_datetime_general_oncology_treatment datetime,
							next_encounter_datetime_general_oncology_treatment datetime,
							prev_encounter_type_general_oncology_treatment mediumint,
							next_encounter_type_general_oncology_treatment mediumint,
							prev_clinical_datetime_general_oncology_treatment datetime,
							next_clinical_datetime_general_oncology_treatment datetime,
							prev_clinical_location_id_general_oncology_treatment mediumint,
							next_clinical_location_id_general_oncology_treatment mediumint,
							prev_clinical_rtc_date_general_oncology_treatment datetime,
							next_clinical_rtc_date_general_oncology_treatment datetime,

							primary key encounter_id (encounter_id),
							index person_date (person_id, encounter_datetime),
							index location_enc_date (location_uuid,encounter_datetime),
							index enc_date_location (encounter_datetime, location_uuid),
							index loc_id_enc_date_next_clinical (location_id, encounter_datetime, next_clinical_datetime_general_oncology_treatment),
							index encounter_type (encounter_type),
							index date_created (date_created)
							
						);
                        
							
					
                        if(@query_type="build") then
							select 'BUILDING..........................................';
							
                           #set @write_table = concat("flat_general_oncology_treatment_temp_",1);
                           #set @queue_table = concat("flat_general_oncology_treatment_build_queue_",1);                    												

                            set @write_table = concat("flat_general_oncology_treatment_temp_",queue_number);
							set @queue_table = concat("flat_general_oncology_treatment_build_queue_",queue_number);                    												
							

                            #drop table if exists flat_general_oncology_treatment_temp_1;							
							SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

							#create  table if not exists @queue_table (person_id int, primary key (person_id));
							SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_general_oncology_treatment_build_queue limit ', queue_size, ');'); 
                            #SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_general_oncology_treatment_build_queue limit 500);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  
							
							#delete t1 from flat_general_oncology_treatment_build_queue t1 join @queue_table t2 using (person_id)
							SET @dyn_sql=CONCAT('delete t1 from flat_general_oncology_treatment_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

					end if;
	
					
					if (@query_type="sync") then
							select 'SYNCING..........................................';
							set @write_table = "flat_general_oncology_treatment";
							set @queue_table = "flat_general_oncology_treatment_sync_queue";
                            create table if not exists flat_general_oncology_treatment_sync_queue (person_id int primary key);                            
                            
							set @last_update = null;

                            select max(date_updated) into @last_update from etl.flat_log where table_name=@table_version;

							#select max(date_created) into @last_update from etl.flat_log where table_name like "%general_oncology_treatment%";

							#select @last_update;														
                           select "Finding patients in amrs.encounters...";

							replace into flat_general_oncology_treatment_sync_queue
							(select distinct patient_id
								from amrs.encounter
								where date_changed > @last_update
							);
						
                        
							select "Finding patients in flat_obs...";

							replace into flat_general_oncology_treatment_sync_queue
							(select distinct person_id
								from etl.flat_obs
								where max_date_created > @last_update
							);


							select "Finding patients in flat_lab_obs...";
							replace into flat_general_oncology_treatment_sync_queue
							(select distinct person_id
								from etl.flat_lab_obs
								where max_date_created > @last_update
							);

							select "Finding patients in flat_orders...";

							replace into flat_general_oncology_treatment_sync_queue
							(select distinct person_id
								from etl.flat_orders
								where max_date_created > @last_update
							);
                            
                            replace into flat_general_oncology_treatment_sync_queue
                            (select person_id from 
								amrs.person 
								where date_voided > @last_update);


                            replace into flat_general_oncology_treatment_sync_queue
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


					#delete t1 from flat_general_oncology_treatment t1 join @queue_table t2 using (person_id);
					SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
                    set @total_time=0;
                    set @cycle_number = 0;
                    

					while @person_ids_count > 0 do

						set @loop_start_time = now();
                        
						#create temp table with a set of person ids
						drop temporary table if exists flat_general_oncology_treatment_build_queue__0;
						
                        #create temporary table flat_general_oncology_treatment_build_queue__0 (select * from flat_general_oncology_treatment_build_queue_2 limit 5000); #TODO - change this when data_fetch_size changes

						#SET @dyn_sql=CONCAT('create temporary table flat_general_oncology_treatment_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit 100);'); 
						SET @dyn_sql=CONCAT('create temporary table flat_general_oncology_treatment_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                    
						drop temporary table if exists flat_general_oncology_treatment_0a;
						SET @dyn_sql = CONCAT(
								'create temporary table flat_general_oncology_treatment_0a
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
									join flat_general_oncology_treatment_build_queue__0 t0 using (person_id)
									left join etl.flat_orders t2 using(encounter_id)
								where t1.encounter_type in ',@encounter_types,');');
                            
                        PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                    
                    

                        
					
						insert into flat_general_oncology_treatment_0a
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
								join flat_general_oncology_treatment_build_queue__0 t0 using (person_id)
						);


						drop temporary table if exists flat_general_oncology_treatment_0;
						create temporary table flat_general_oncology_treatment_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
						(select * from flat_general_oncology_treatment_0a
						order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
                        
                        set @cur_visit_type = null;
						set @ecog_performance = null;
						set @general_exam = null;
						set @heent = null;
						set @breast_findings = null;
						set @breast_finding_location = null;
						set @breast_findingquadrant = null;
						set @chest = null;
						set @heart = null;
						set @abdomenexam = null;
						set @urogenital = null;
						set @extremities = null;
						set @testicular_exam = null;
						set @nodal_survey = null;
						set @musculoskeletal = null;
						set @skin_exam_findings = null;
						set @body_part = null;
						set @laterality = null;
						set @measure_first_direction = null;
						set @measure_first_direction = null;
						set @neurologic = null;
						set @bnody_part = null;
						set @patient_on_chemo = null;
						set @echo = null;
						set @ct_head = null;
						set @ct_neck = null;
						set @ct_chest = null;
						set @ct_abdomen = null;
						set @ct_spine = null;
						set @mri_head = null;
						set @mri_neck = null;
						set @mri_chest = null;
						set @mri_abdomen = null;
						set @mri_spine = null;
						set @mri_arms = null;
						set @mri_pelvic = null;
						set @mri_legs = null;
						set @ultrasound_renal= null;
						set @ultrasound_hepatic = null;
						set @obstetric_ultrasound = null;
						set @abonimal_ultrasound = null;
						set @breast_ultrasound = null;
                        set @x_ray_shoulder= null;
							set @x_ray_pelvis= null;
							set @x_ray_abdomen= null;
							set @x_ray_skull= null;
							set @x_ray_leg= null;
							set @x_ray_hand= null;
							set @x_ray_foot= null;
							set @x_ray_chest= null;
							set @x_ray_arm= null;
							set @x_ray_spine= null;
							set @method_of_diagnosis= null;
							set @diagnosis_on_biopsy= null;
							set @biopsy_consistent_clinic_susp= null;
							set @diagnosis_results= null;
							set @cancer_type= null;
							set @other_cancer= null;
							set @type_of_sarcoma= null;
							set @type_of_gu= null;
							set @type_of_gi_cancer= null;
							set @head_and_neck_cancer= null;
							set @gynecologic_cancer= null;
							set @lympoma_cancer= null;
							set @skin_cancer= null;
							set @breast_cancer= null;
							set @other_solid_cancer= null;
							set @type_of_leukemeia= null;
							set @non_cancer= null;
							set @diagnosis_date= null;
							set @new_or_recurrence_cancer= null;
							set @cancer_stage= null;

                                                
						drop temporary table if exists flat_general_oncology_treatment_1;
						create temporary table flat_general_oncology_treatment_1 #(index encounter_id (encounter_id))
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
								when obs regexp "!!1834=1068!!" then @cur_visit_type := 1
								when obs regexp "!!1834=7850!!" then @cur_visit_type := 2
                                when obs regexp "!!1834=1246!!" then @cur_visit_type := 3
                                when obs regexp "!!1834=2345!!" then @cur_visit_type := 4
                                when obs regexp "!!1834=10037!!" then @cur_visit_type := 5
								else @cur_visit_type := null
							end as cur_visit_type,
                            case
								when obs regexp "!!6584=1115!!" then @ecog_performance := 1
								when obs regexp "!!6584=6585!!" then @ecog_performance := 2
                                when obs regexp "!!6584=6586!!" then @ecog_performance := 3
                                when obs regexp "!!6584=6587!!" then @ecog_performance := 4
                                when obs regexp "!!6584=6588!!" then @ecog_performance := 5
								else @ecog_performance := null
							end as ecog_performance,
                             case
								when obs regexp "!!1119=1115!!" then @general_exam := 1
								when obs regexp "!!1119=5201!!" then @general_exam := 2
                                when obs regexp "!!1119=5245!!" then @general_exam := 3
                                when obs regexp "!!1119=215!!" then @general_exam := 4
                                when obs regexp "!!1119=589!!" then @general_exam := 5
								else @general_exam := null
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
								else @heent := null
							end as heent,
                            case
								when obs regexp "!!6251=1115!!" then @breast_findings := 1
								when obs regexp "!!6251=9689!!" then @breast_findings := 2
                                when obs regexp "!!6251=9690!!" then @breast_findings := 3
                                when obs regexp "!!6251=9687!!" then @breast_findings := 4
                                when obs regexp "!!6251=9688!!" then @breast_findings := 5
                                when obs regexp "!!6251=5313!!" then @breast_findings := 6
                                when obs regexp "!!6251=6493!!" then @breast_findings := 7
                                when obs regexp "!!6251=6250!!" then @breast_findings := 8
								else @breast_findings := null
							end as breast_findings,
                            case
								when obs regexp "!!9696=5141!!" then @breast_finding_location := 1
								when obs regexp "!!9696=5139!!" then @breast_finding_location := 2
								else @breast_finding_location := null
							end as breast_finding_location,
                            case
								when obs regexp "!!8268=5107!!" then @breast_findingquadrant := 1
								when obs regexp "!!8268=1883!!" then @breast_findingquadrant := 2
                                when obs regexp "!!8268=1882!!" then @breast_findingquadrant := 3
								when obs regexp "!!8268=5104!!" then @breast_findingquadrant := 4
                                when obs regexp "!!8268=9695!!" then @breast_findingquadrant := 5
								else @breast_findingquadrant := null
							end as breast_findingquadrant,
                            case
								when obs regexp "!!1123=1118!!" then @chest := 1
								when obs regexp "!!1123=1115!!" then @chest := 2
                                when obs regexp "!!1123=5138!!" then @chest := 3
								when obs regexp "!!1123=5115!!" then @chest := 4
                                when obs regexp "!!1123=5116!!" then @chest := 5
                                when obs regexp "!!1123=5181!!" then @chest := 6
                                when obs regexp "!!1123=5127!!" then @chest := 7
								else @chest := null
							end as chest,
                             case
								when obs regexp "!!1124=1118!!" then @heart := 1
								when obs regexp "!!1124=1115!!" then @heart := 2
                                when obs regexp "!!1124=1117!!" then @heart := 3
								when obs regexp "!!1124=550!!" then @heart := 4
                                when obs regexp "!!1124=5176!!" then @heart := 5
                                when obs regexp "!!1124=5162!!" then @heart := 6
                                when obs regexp "!!1124=5164!!" then @heart := 7
								else @heart := null
							end as heart,
                             case
								when obs regexp "!!1125=1118!!" then @abdomenexam := 1
								when obs regexp "!!1125=1115!!" then @abdomenexam := 2
                                when obs regexp "!!1125=5105!!" then @abdomenexam := 3
								when obs regexp "!!1125=5008!!" then @abdomenexam := 4
                                when obs regexp "!!1125=5009!!" then @abdomenexam := 5
                                when obs regexp "!!1125=581!!" then @abdomenexam := 6
                                when obs regexp "!!1125=5103!!" then @abdomenexam := 7
								else @abdomenexam := null
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
								else @urogenital := null
							end as urogenital,
                            case
								when obs regexp "!!1127=1118!!" then @extremities := 1
								when obs regexp "!!1127=1115!!" then @extremities := 2
                                when obs regexp "!!1127=590!!" then @extremities := 3
								when obs regexp "!!1127=7293!!" then @extremities := 4
                                when obs regexp "!!1127=134!!" then @extremities := 5
								else @extremities := null
							end as extremities,
                            case
								when obs regexp "!!8420=1118!!" then @testicular_exam := 1
								when obs regexp "!!8420=1115!!" then @testicular_exam := 2
                                when obs regexp "!!8420=1116!!" then @testicular_exam := 3
								else @testicular_exam := null
							end as testicular_exam,
                            case
								when obs regexp "!!8420=1118!!" then @nodal_survey := 1
								when obs regexp "!!8420=1115!!" then @nodal_survey := 2
                                when obs regexp "!!8420=1116!!" then @nodal_survey := 3
                                when obs regexp "!!8420=8261!!" then @nodal_survey := 4
								else @nodal_survey := null
							end as nodal_survey,
                            case
								when obs regexp "!!1128=1118!!" then @musculoskeletal := 1
								when obs regexp "!!1128=1115!!" then @musculoskeletal := 2
                                when obs regexp "!!1128=1116!!" then @musculoskeletal := 3
								else @musculoskeletal := null
							end as musculoskeletal,
                            case
								when obs regexp "!!1120=1107!!" then @skin_exam_findings := 1
								when obs regexp "!!1120=1118!!" then @skin_exam_findings := 2
                                when obs regexp "!!1120=582!!" then @skin_exam_findings := 3
								else @skin_exam_findings := null
							end as skin_exam_findings,
                            case
								when obs regexp "!!8265=6599!!" then @body_part := 1
								when obs regexp "!!8265=6598!!" then @body_part := 2
                                when obs regexp "!!8265=1237!!" then @body_part := 3
                                when obs regexp "!!8265=1236!!" then @body_part := 4
								when obs regexp "!!8265=1349!!" then @body_part := 5
                                when obs regexp "!!8265=6601!!" then @body_part := 6
                                when obs regexp "!!8265=1350!!" then @body_part := 7
								when obs regexp "!!8265=6600!!" then @body_part := 8
                                when obs regexp "!!8265=6597!!" then @body_part := 9
								else @body_part := null
							end as body_part,
                            case
								when obs regexp "!!8264=5139!!" then @laterality := 1
								when obs regexp "!!8264=5141!!" then @laterality := 2
								else @laterality := null
							end as laterality,
                            case
								when obs regexp "!!8270=" then @measure_first_direction := GetValues(obs,8270) 
								else @measure_first_direction := null
							end as measure_first_direction,
                            case
								when obs regexp "!!8271=" then @measure_second_direction := GetValues(obs,8271) 
								else @measure_second_direction := null
							end as measure_second_direction,
                            case
								when obs regexp "!!1129=1118!!" then @neurologic := 1
								when obs regexp "!!1129=1115!!" then @neurologic := 2
                                when obs regexp "!!1129=599!!" then @neurologic := 3
								when obs regexp "!!1129=497!!" then @neurologic := 4
                                when obs regexp "!!1129=5108!!" then @neurologic := 5
                                when obs regexp "!!1129=6005!!" then @neurologic := 6
								else @neurologic := null
							end as neurologic,
                            case
								when obs regexp "!!10071=10072!!" then @bnody_part := 1
								when obs regexp "!!10071=10073!!" then @bnody_part := 2
                                when obs regexp "!!10071=10074!!" then @bnody_part := 3
								else @bnody_part := null
							end as bnody_part,
                            case
								when obs regexp "!!6575=1065!!" then @patient_on_chemo := 1
								when obs regexp "!!6575=1066!!" then @patient_on_chemo := 2
								else @patient_on_chemo := null
							end as patient_on_chemo,
                            case
								when obs regexp "!!1536=1115!!" then @echo := 1
								when obs regexp "!!1536=1116!!" then @echo := 2
                                when obs regexp "!!1536=1532!!" then @echo := 3
								when obs regexp "!!1536=1533!!" then @echo := 4
                                when obs regexp "!!1536=1538!!" then @echo := 5
                                when obs regexp "!!1536=5622!!" then @echo := 6
								else @echo := null
							end as echo,
                            case
								when obs regexp "!!846=1115!!" then @ct_head := 1
								when obs regexp "!!846=1116!!" then @ct_head := 2
								else @ct_head := null
							end as ct_head,
                             case
								when obs regexp "!!9839=1115!!" then @ct_neck := 1
								when obs regexp "!!9839=1116!!" then @ct_neck := 2
								else @ct_neck := null
							end as ct_neck,
                            case
								when obs regexp "!!7113=1115!!" then @ct_chest := 1
								when obs regexp "!!7113=1116!!" then @ct_chest := 2
								else @ct_chest := null
							end as ct_chest,
                            case
								when obs regexp "!!7114=1115!!" then @ct_abdomen := 1
								when obs regexp "!!7114=1116!!" then @ct_abdomen := 2
								else @ct_abdomen := null
							end as ct_abdomen,
                             case
								when obs regexp "!!9840=1115!!" then @ct_spine := 1
								when obs regexp "!!9840=1116!!" then @ct_spine := 2
								else @ct_spine := null
							end as ct_spine,
                            case
								when obs regexp "!!9881=1115!!" then @mri_head := 1
								when obs regexp "!!9881=1116!!" then @mri_head := 2
								else @mri_head := null
							end as mri_head,
                             case
								when obs regexp "!!9882=1115!!" then @mri_neck := 1
								when obs regexp "!!9882=1116!!" then @mri_neck := 2
								else @mri_neck := null
							end as mri_neck,
                             case
								when obs regexp "!!9883=1115!!" then @mri_chest := 1
								when obs regexp "!!9883=1116!!" then @mri_chest := 2
								else @mri_chest := null
							end as mri_chest,
                             case
								when obs regexp "!!9884=1115!!" then @mri_abdomen := 1
								when obs regexp "!!9884=1116!!" then @mri_abdomen := 2
								else @mri_abdomen := null
							end as mri_abdomen,
                            case
								when obs regexp "!!9885=1115!!" then @mri_spine := 1
								when obs regexp "!!9885=1116!!" then @mri_spine := 2
								else @mri_spine := null
							end as mri_spine,
                            case
								when obs regexp "!!9951=1115!!" then @mri_arms := 1
								when obs regexp "!!9951=1116!!" then @mri_arms := 2
								else @mri_arms := null
							end as mri_arms,
                            case
								when obs regexp "!!9952=1115!!" then @mri_pelvic := 1
								when obs regexp "!!9952=1116!!" then @mri_pelvic := 2
								else @mri_pelvic := null
							end as mri_pelvic,
                            case
								when obs regexp "!!9953=1115!!" then @mri_legs := 1
								when obs regexp "!!9953=1116!!" then @mri_legs := 2
								else @mri_legs := null
							end as mri_legs,
                            case
								when obs regexp "!!7115=1115!!" then @ultrasound_renal := 1
								when obs regexp "!!7115=1116!!" then @ultrasound_renal := 2
								else @ultrasound_renal := null
							end as ultrasound_renal,
                            case
								when obs regexp "!!852=1115!!" then @ultrasound_hepatic := 1
								when obs regexp "!!852=1116!!" then @ultrasound_hepatic := 2
								else @ultrasound_hepatic := null
							end as ultrasound_hepatic,
                            case
								when obs regexp "!!6221=1115!!" then @obstetric_ultrasound := 1
								when obs regexp "!!6221=1116!!" then @obstetric_ultrasound := 2
								else @obstetric_ultrasound := null
							end as obstetric_ultrasound,
                            case
								when obs regexp "!!845=1115!!" then @abonimal_ultrasound := 1
								when obs regexp "!!845=1116!!" then @abonimal_ultrasound := 2
								else @abonimal_ultrasound := null
							end as abonimal_ultrasound,
                            case
								when obs regexp "!!9596=1115!!" then @breast_ultrasound := 1
								when obs regexp "!!9596=1116!!" then @breast_ultrasound := 2
								else @breast_ultrasound := null
							end as breast_ultrasound,
                            case
								when obs regexp "!!394=1115!!" then @x_ray_shoulder := 1
								when obs regexp "!!394=1116!!" then @x_ray_shoulder := 2
								else @x_ray_shoulder := null
							end as x_ray_shoulder,
                            case
								when obs regexp "!!392=1115!!" then @x_ray_pelvis := 1
								when obs regexp "!!392=1116!!" then @x_ray_pelvis := 2
								else @x_ray_pelvis := null
							end as x_ray_pelvis,
                            case
								when obs regexp "!!101=1115!!" then @x_ray_abdomen := 1
								when obs regexp "!!101=1116!!" then @x_ray_abdomen := 2
								else @x_ray_abdomen := null
							end as x_ray_abdomen,
                            case
								when obs regexp "!!386=1115!!" then @x_ray_skull := 1
								when obs regexp "!!386=1116!!" then @x_ray_skull := 2
								else @x_ray_skull := null
							end as x_ray_skull,
                            case
								when obs regexp "!!380=1115!!" then @x_ray_leg := 1
								when obs regexp "!!380=1116!!" then @x_ray_leg := 2
								else @x_ray_leg := null
							end as x_ray_leg,
                            case
								when obs regexp "!!382=1115!!" then @x_ray_hand := 1
								when obs regexp "!!382=1116!!" then @x_ray_hand := 2
								else @x_ray_hand := null
							end as x_ray_hand,
                            case
								when obs regexp "!!384=1115!!" then @x_ray_foot := 1
								when obs regexp "!!384=1116!!" then @x_ray_foot := 2
								else @x_ray_foot := null
							end as x_ray_foot,
                            case
								when obs regexp "!!12=1115!!" then @x_ray_chest := 1
								when obs regexp "!!12=1116!!" then @x_ray_chest := 2
								else @x_ray_chest := null
							end as x_ray_chest,
                            case
								when obs regexp "!!377=1115!!" then @x_ray_arm := 1
								when obs regexp "!!377=1116!!" then @x_ray_arm := 2
								else @x_ray_arm := null
							end as x_ray_arm,
                            case
								when obs regexp "!!390=1115!!" then @x_ray_spine := 1
								when obs regexp "!!390=1116!!" then @x_ray_spine := 2
								else @x_ray_spine := null
							end as x_ray_spine,
                            case
								when obs regexp "!!6504=6505!!" then @method_of_diagnosis := 1
								when obs regexp "!!6504=6506!!" then @method_of_diagnosis := 2
                                when obs regexp "!!6504=6507!!" then @method_of_diagnosis := 3
								when obs regexp "!!6504=6508!!" then @method_of_diagnosis := 4
                                when obs regexp "!!6504=6902!!" then @method_of_diagnosis := 5
								when obs regexp "!!6504=7189!!" then @method_of_diagnosis := 6
								else @method_of_diagnosis := null
							end as method_of_diagnosis,
                            case
								when obs regexp "!!6509=7190!!" then @diagnosis_on_biopsy := 1
								when obs regexp "!!6509=6510!!" then @diagnosis_on_biopsy := 2
                                when obs regexp "!!6509=6511!!" then @diagnosis_on_biopsy := 3
								when obs regexp "!!6509=6512!!" then @diagnosis_on_biopsy := 4
                                when obs regexp "!!6509=6513!!" then @diagnosis_on_biopsy := 5
								when obs regexp "!!6509=10075!!" then @diagnosis_on_biopsy := 6
                                when obs regexp "!!6509=10076!!" then @diagnosis_on_biopsy := 7
								else @diagnosis_on_biopsy := null
							end as diagnosis_on_biopsy,
                            case
								when obs regexp "!!6605=1065!!" then @biopsy_consistent_clinic_susp := 1
								when obs regexp "!!6605=1066!!" then @biopsy_consistent_clinic_susp := 2
								else @biopsy_consistent_clinic_susp := null
							end as biopsy_consistent_clinic_susp,
                            case
								when obs regexp "!!7191=" then @diagnosis_results := GetValues(obs,7191) 
								else @diagnosis_results := null
							end as diagnosis_results,
                            case
								when obs regexp "!!7176=6485!!" then @cancer_type := 1
								when obs regexp "!!7176=6514!!" then @cancer_type := 2
                                when obs regexp "!!7176=6520!!" then @cancer_type := 3
								when obs regexp "!!7176=6528!!" then @cancer_type := 4
                                when obs regexp "!!7176=6536!!" then @cancer_type := 5
								when obs regexp "!!7176=6551!!" then @cancer_type := 6
                                when obs regexp "!!7176=6540!!" then @cancer_type := 7
                                when obs regexp "!!7176=6544!!" then @cancer_type := 8
                                when obs regexp "!!7176=216!!" then @cancer_type := 9
                                when obs regexp "!!7176=9846!!" then @cancer_type := 10
                                when obs regexp "!!7176=5622!!" then @cancer_type := 11
								else @cancer_type := null
							end as cancer_type,
                            case
								when obs regexp "!!1915=" then @other_cancer := GetValues(obs,1915) 
								else @other_cancer := null
							end as other_cancer,
                            case
								when obs regexp "!!9843=507!!" then @type_of_sarcoma := 1
								when obs regexp "!!9843=6486!!" then @type_of_sarcoma := 2
                                when obs regexp "!!9843=6487!!" then @type_of_sarcoma := 3
								when obs regexp "!!9843=6488!!" then @type_of_sarcoma := 4
                                when obs regexp "!!9843=6489!!" then @type_of_sarcoma := 5
								when obs regexp "!!9843=6490!!" then @type_of_sarcoma := 6
								else @type_of_sarcoma := null
							end as type_of_sarcoma,
                            case
								when obs regexp "!!6514=6515!!" then @type_of_gu := 1
								when obs regexp "!!6514=6516!!" then @type_of_gu := 2
                                when obs regexp "!!6514=6517!!" then @type_of_gu := 3
								when obs regexp "!!6514=6518!!" then @type_of_gu := 4
                                when obs regexp "!!6514=6519!!" then @type_of_gu := 5
								when obs regexp "!!6514=5622!!" then @type_of_gu := 6
								else @type_of_gu := null
							end as type_of_gu,
                            case
								when obs regexp "!!6520=6521!!" then @type_of_gi_cancer := 1
								when obs regexp "!!6520=6522!!" then @type_of_gi_cancer := 2
                                when obs regexp "!!6520=6523!!" then @type_of_gi_cancer := 3
								when obs regexp "!!6520=6524!!" then @type_of_gi_cancer := 4
                                when obs regexp "!!6520=6525!!" then @type_of_gi_cancer := 5
								when obs regexp "!!6520=6526!!" then @type_of_gi_cancer := 6
                                when obs regexp "!!6520=6527!!" then @type_of_gi_cancer := 7
                                when obs regexp "!!6520=6568!!" then @type_of_gi_cancer := 8
								when obs regexp "!!6520=5622!!" then @type_of_gi_cancer := 9
								else @type_of_gi_cancer := null
							end as type_of_gi_cancer,
                            case
								when obs regexp "!!6528=6529!!" then @head_and_neck_cancer := 1
								when obs regexp "!!6528=6530!!" then @head_and_neck_cancer := 2
                                when obs regexp "!!6528=6531!!" then @head_and_neck_cancer := 3
								when obs regexp "!!6528=6532!!" then @head_and_neck_cancer := 4
                                when obs regexp "!!6528=6533!!" then @head_and_neck_cancer := 5
								when obs regexp "!!6528=6534!!" then @head_and_neck_cancer := 6
                                when obs regexp "!!6528=5622!!" then @head_and_neck_cancer := 7
								else @head_and_neck_cancer := null
							end as head_and_neck_cancer,
                            case
								when obs regexp "!!6536=6537!!" then @gynecologic_cancer := 1
								when obs regexp "!!6536=6538!!" then @gynecologic_cancer := 2
                                when obs regexp "!!6536=6539!!" then @gynecologic_cancer := 3
								when obs regexp "!!6536=5622!!" then @gynecologic_cancer := 4
								else @gynecologic_cancer := null
							end as gynecologic_cancer,
                            case
								when obs regexp "!!6551=6553!!" then @lympoma_cancer := 1
								when obs regexp "!!6551=6552!!" then @lympoma_cancer := 2
                                when obs regexp "!!6551=8423!!" then @lympoma_cancer := 3
								when obs regexp "!!6551=5622!!" then @lympoma_cancer := 4
								else @lympoma_cancer := null
							end as lympoma_cancer,
                            case
								when obs regexp "!!6540=6541!!" then @skin_cancer := 1
								when obs regexp "!!6540=6542!!" then @skin_cancer := 2
                                when obs regexp "!!6540=6543!!" then @skin_cancer := 3
								when obs regexp "!!6540=5622!!" then @skin_cancer := 4
								else @skin_cancer := null
							end as skin_cancer,
                            case
								when obs regexp "!!9841=6545!!" then @breast_cancer := 1
								when obs regexp "!!9841=9842!!" then @breast_cancer := 2
                                when obs regexp "!!9841=5622!!" then @breast_cancer := 3
								else @breast_cancer := null
							end as breast_cancer,
                            case
								when obs regexp "!!9846=8424!!" then @other_solid_cancer := 1
								when obs regexp "!!9846=8425!!" then @other_solid_cancer := 2
                                when obs regexp "!!9846=9845!!" then @other_solid_cancer := 3
                                when obs regexp "!!9846=5622!!" then @other_solid_cancer := 4
								else @other_solid_cancer := null
							end as other_solid_cancer,
                            case
								when obs regexp "!!9844=6547!!" then @type_of_leukemeia := 1
								when obs regexp "!!9844=6548!!" then @type_of_leukemeia := 2
                                when obs regexp "!!9844=6549!!" then @type_of_leukemeia := 3
                                when obs regexp "!!9844=6550!!" then @type_of_leukemeia := 4
                                when obs regexp "!!9844=5622!!" then @type_of_leukemeia := 5
								else @type_of_leukemeia := null
							end as type_of_leukemeia,
                            case
								when obs regexp "!!9847=1226!!" then @non_cancer := 1
								when obs regexp "!!9847=6556!!" then @non_cancer := 2
                                when obs regexp "!!9847=9870!!" then @non_cancer := 3
                                when obs regexp "!!9847=2!!" then @non_cancer := 4
                                when obs regexp "!!9847=6557!!" then @non_cancer := 5
                                when obs regexp "!!9847=5622!!" then @non_cancer := 6
								else @non_cancer := null
							end as non_cancer,
                            case
								when obs regexp "!!9728=" then @diagnosis_date := GetValues(obs,9728) 
								else @diagnosis_date := null
							end as diagnosis_date,
							case
								when obs regexp "!!9848=9849!!" then @new_or_recurrence_cancer := 1
								when obs regexp "!!9848=9850!!" then @new_or_recurrence_cancer := 2
								else @new_or_recurrence_cancer := null
							end as new_or_recurrence_cancer,
                            case
								when obs regexp "!!6582=1067!!" then @cancer_stage := 1
								when obs regexp "!!6582=6566!!" then @cancer_stage := 2
                                when obs regexp "!!6582=10206!!" then @cancer_stage := 3
                                when obs regexp "!!6582=1175!!" then @cancer_stage := 4
								else @cancer_stage := null
							end as cancer_stage,
                            case
								when obs regexp "!!9868=9851!!" then @overall_cancer_staging := 1
								when obs regexp "!!9868=9852!!" then @overall_cancer_staging := 2
                                when obs regexp "!!9868=9853!!" then @overall_cancer_staging := 3
                                when obs regexp "!!9868=9854!!" then @overall_cancer_staging := 4
                                when obs regexp "!!9868=9855!!" then @overall_cancer_staging := 5
								when obs regexp "!!9868=9856!!" then @overall_cancer_staging := 6
                                when obs regexp "!!9868=9857!!" then @overall_cancer_staging := 7
                                when obs regexp "!!9868=9858!!" then @overall_cancer_staging := 8
                                when obs regexp "!!9868=9859!!" then @overall_cancer_staging := 9
								when obs regexp "!!9868=9860!!" then @overall_cancer_staging := 10
                                when obs regexp "!!9868=9861!!" then @overall_cancer_staging := 11
                                when obs regexp "!!9868=9862!!" then @overall_cancer_staging := 12
                                when obs regexp "!!9868=9863!!" then @overall_cancer_staging := 13
								when obs regexp "!!9868=9864!!" then @overall_cancer_staging := 14
                                when obs regexp "!!9868=9865!!" then @overall_cancer_staging := 15
                                when obs regexp "!!9868=9866!!" then @overall_cancer_staging := 16
                                when obs regexp "!!9868=9867!!" then @overall_cancer_staging := 17
								else @overall_cancer_staging := null
							end as overall_cancer_staging,
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
								when obs regexp "!!8723=7465!!" then @treatment_plan := 1
								when obs regexp "!!8723=8427!!" then @treatment_plan := 2
                                when obs regexp "!!8723=6575!!" then @treatment_plan := 3
                                when obs regexp "!!8723=9626!!" then @treatment_plan := 4
                                when obs regexp "!!8723=10038!!" then @treatment_plan := 5
                                when obs regexp "!!8723=10232!!" then @treatment_plan := 6
                                when obs regexp "!!8723=5622!!" then @treatment_plan := 7
								else @treatment_plan := null
							end as treatment_plan,
                            case
								when obs regexp "!!10039=" then @other_treatment_plan := GetValues(obs,10039) 
								else @other_treatment_plan := null
							end as other_treatment_plan,
                            case
								when obs regexp "!!6419=1065!!" then @hospitalization := 1
								when obs regexp "!!6419=1066!!" then @hospitalization := 2
								else @hospitalization := null
							end as hospitalization,
                            case
								when obs regexp "!!9916=1349!!" then @radiation_location := 1
								when obs regexp "!!9916=8193!!" then @radiation_location := 2
                                when obs regexp "!!9916=9226!!" then @radiation_location := 3
                                when obs regexp "!!9916=9227!!" then @radiation_location := 4
                                when obs regexp "!!9916=9228!!" then @radiation_location := 5
                                when obs regexp "!!9916=9229!!" then @radiation_location := 6
                                when obs regexp "!!9916=9230!!" then @radiation_location := 7
                                when obs regexp "!!9916=9231!!" then @radiation_location := 8
                                when obs regexp "!!9916=5622!!" then @radiation_location := 9
								else @radiation_location := null
							end as radiation_location,
                             case
								when obs regexp "!!8725=8428!!" then @surgery_reason := 1
								when obs regexp "!!8725=8724!!" then @surgery_reason := 2
                                when obs regexp "!!8725=8727!!" then @surgery_reason := 3
                                when obs regexp "!!8725=10233!!" then @surgery_reason := 4
								else @surgery_reason := null
							end as surgery_reason,
                            case
								when obs regexp "!!10230=" then @surgical_procedure := GetValues(obs,10230) 
								else @surgical_procedure := null
							end as surgical_procedure,
                            case
								when obs regexp "!!9918=8513!!" then @hormonal_drug := 1
								when obs regexp "!!9918=8514!!" then @hormonal_drug := 2
                                when obs regexp "!!9918=8519!!" then @hormonal_drug := 3
                                when obs regexp "!!9918=8515!!" then @hormonal_drug := 4
                                when obs regexp "!!9918=8518!!" then @hormonal_drug := 5
								when obs regexp "!!9918=7217!!" then @hormonal_drug := 6
                                when obs regexp "!!9918=10140!!" then @hormonal_drug := 7
                                when obs regexp "!!9918=5622!!" then @hormonal_drug := 8
								else @hormonal_drug := null
							end as hormonal_drug,
                            case
								when obs regexp "!!10234=8489!!" then @targeted_therapies := 1
								when obs regexp "!!10234=8498!!" then @targeted_therapies := 2
                                when obs regexp "!!10234=8501!!" then @targeted_therapies := 3
                                when obs regexp "!!10234=5622!!" then @targeted_therapies := 4
								else @targeted_therapies := null
							end as targeted_therapies,
                            case
								when obs regexp "!!1896=1891!!" then @targeted_therapies_frequency := 1
								when obs regexp "!!1896=1099!!" then @targeted_therapies_frequency := 2
                                when obs regexp "!!1896=7682!!" then @targeted_therapies_frequency := 3
                                when obs regexp "!!1896=9933!!" then @targeted_therapies_frequency := 4
                                when obs regexp "!!1896=1098!!" then @targeted_therapies_frequency := 5
                                when obs regexp "!!1896=7772!!" then @targeted_therapies_frequency := 6
								else @targeted_therapies_frequency := null
							end as targeted_therapies_frequency,
                             case
								when obs regexp "!!10235=" then @targeted_therapy_start_date := GetValues(obs,10235) 
								else @targeted_therapy_start_date := null
							end as targeted_therapy_start_date,
                            case
								when obs regexp "!!10236=" then @targeted_therapy_stop_date := GetValues(obs,10236) 
								else @targeted_therapy_stop_date := null
							end as targeted_therapy_stop_date,
                            case
								when obs regexp "!!9869=1256!!" then @chemotherapy_plan := 1
								when obs regexp "!!9869=1259!!" then @chemotherapy_plan := 2
                                when obs regexp "!!9869=1260!!" then @chemotherapy_plan := 3
                                when obs regexp "!!9869=1257!!" then @chemotherapy_plan := 4
                                when obs regexp "!!9869=6576!!" then @chemotherapy_plan := 5
								else @chemotherapy_plan := null
							end as chemotherapy_plan,
                            case
								when obs regexp "!!9917=1065!!" then @treatment_on_clinical_trial := 1
								when obs regexp "!!9917=1066!!" then @treatment_on_clinical_trial := 2
								else @treatment_on_clinical_trial := null
							end as treatment_on_clinical_trial,
                            case
								when obs regexp "!!1190=" then @chemo_start_date := GetValues(obs,1190) 
								else @chemo_start_date := null
							end as chemo_start_date,
                            case
								when obs regexp "!!2206=9218!!" then @treatment_intent := 1
								when obs regexp "!!2206=8428!!" then @treatment_intent := 2
                                when obs regexp "!!2206=8724!!" then @treatment_intent := 3
                                when obs regexp "!!2206=9219!!" then @treatment_intent := 4
								else @treatment_intent := null
							end as treatment_intent,
                            case
								when obs regexp "!!9946=" then @chemo_regimen := GetValues(obs,9946) 
								else @chemo_regimen := null
							end as chemo_regimen,
                            case
								when obs regexp "!!6643=" then @chemo_cycle := GetValues(obs,6643) 
								else @chemo_cycle := null
							end as chemo_cycle,
                            case
								when obs regexp "!!6644=" then @total_planned_chemo_cycle := GetValues(obs,6644) 
								else @total_planned_chemo_cycle := null
							end as total_planned_chemo_cycle,
                            case
								when obs regexp "!!9918=8478!!" then @chemo_drug := 1
								when obs regexp "!!9918=7217!!" then @chemo_drug := 2
                                when obs regexp "!!9918=7203!!" then @chemo_drug := 3
                                when obs regexp "!!9918=7215!!" then @chemo_drug := 4
                                when obs regexp "!!9918=7223!!" then @chemo_drug := 5
								when obs regexp "!!9918=7198!!" then @chemo_drug := 6
                                when obs regexp "!!9918=491!!" then @chemo_drug := 7
                                when obs regexp "!!9918=7199!!" then @chemo_drug := 8
                                when obs regexp "!!9918=7218!!" then @chemo_drug := 9
								when obs regexp "!!9918=7205!!" then @chemo_drug := 10
                                when obs regexp "!!9918=7212!!" then @chemo_drug := 11
                                when obs regexp "!!9918=8513!!" then @chemo_drug := 12
                                when obs regexp "!!9918=7202!!" then @chemo_drug := 13
								when obs regexp "!!9918=7209!!" then @chemo_drug := 14
                                when obs regexp "!!9918=7197!!" then @chemo_drug := 15
                                when obs regexp "!!9918=8506!!" then @chemo_drug := 16
                                when obs regexp "!!9918=7210!!" then @chemo_drug := 17
                                when obs regexp "!!9918=8499!!" then @chemo_drug := 18
								when obs regexp "!!9918=8492!!" then @chemo_drug := 19
                                when obs regexp "!!9918=8522!!" then @chemo_drug := 20
                                when obs regexp "!!9918=8510!!" then @chemo_drug := 21
                                when obs regexp "!!9918=8511!!" then @chemo_drug := 22
								when obs regexp "!!9918=9919!!" then @chemo_drug := 23
                                when obs regexp "!!9918=8518!!" then @chemo_drug := 24
                                
                                when obs regexp "!!9918=8516!!" then @chemo_drug := 25
                                when obs regexp "!!9918=8481!!" then @chemo_drug := 26
								when obs regexp "!!9918=7207!!" then @chemo_drug := 27
                                when obs regexp "!!9918=7204!!" then @chemo_drug := 28
                                when obs regexp "!!9918=7201!!" then @chemo_drug := 29
                                when obs regexp "!!9918=8519!!" then @chemo_drug := 30
								when obs regexp "!!9918=8507!!" then @chemo_drug := 31
                                when obs regexp "!!9918=8479!!" then @chemo_drug := 32
                                when obs regexp "!!9918=7213!!" then @chemo_drug := 33
                                when obs regexp "!!9918=10139!!" then @chemo_drug := 34
                                 when obs regexp "!!9918=10140!!" then @chemo_drug := 35
								when obs regexp "!!9918=5622!!" then @chemo_drug := 36
								else @chemo_drug := null
							end as chemo_drug,
                            case
								when obs regexp "!!1899=" then @dosage_in_milligrams := GetValues(obs,1899) 
								else @dosage_in_milligrams := null
							end as dosage_in_milligrams,
                            case
								when obs regexp "!!7463=7458!!" then @drug_route := 1
								when obs regexp "!!7463=10078!!" then @drug_route := 2
                                when obs regexp "!!7463=10079!!" then @drug_route := 3
                                when obs regexp "!!7463=7597!!" then @drug_route := 4
                                when obs regexp "!!7463=7581!!" then @drug_route := 5
                                when obs regexp "!!7463=7609!!" then @drug_route := 6
                                when obs regexp "!!7463=7616!!" then @drug_route := 7
                                when obs regexp "!!7463=7447!!" then @drug_route := 8
								else @drug_route := null
							end as drug_route,
                            case
								when obs regexp "!!1895=" then @other_drugs := GetValues(obs,1895) 
								else @other_drugs := null
							end as other_drugs,
                            case
								when obs regexp "!!1779=" then @other_medication := GetValues(obs,1779) 
								else @other_medication := null
							end as other_medication,
                            case
								when obs regexp "!!2206=9220!!" then @purpose := 1
								when obs regexp "!!2206=8428!!" then @purpose := 2
								else @purpose := null
							end as purpose,
                             case
								when obs regexp "!!1272=1107!!" then @referral_ordered := 1
								when obs regexp "!!1272=8724!!" then @referral_ordered := 2
                                when obs regexp "!!1272=5484!!" then @referral_ordered := 3
                                when obs regexp "!!1272=7054!!" then @referral_ordered := 4
                                when obs regexp "!!1272=6419!!" then @referral_ordered := 5
                                when obs regexp "!!1272=5490!!" then @referral_ordered := 6
                                when obs regexp "!!1272=5486!!" then @referral_ordered := 7
                                when obs regexp "!!1272=5483!!" then @referral_ordered := 8
                                when obs regexp "!!1272=5622!!" then @referral_ordered := 9
								else @referral_ordered := null
							end as referral_ordered,
                            case
								when obs regexp "!!5096=" then @return_to_clinic_date := GetValues(obs,5096) 
								else @return_to_clinic_date := null
							end as return_to_clinic_date
                            
		
						from flat_general_oncology_treatment_0 t1
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


						alter table flat_general_oncology_treatment_1 drop prev_id, drop cur_id;

						drop table if exists flat_general_oncology_treatment_2;
						create temporary table flat_general_oncology_treatment_2
						(select *,
							@prev_id := @cur_id as prev_id,
							@cur_id := person_id as cur_id,

							case
								when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
								else @prev_encounter_datetime := null
							end as next_encounter_datetime_general_oncology_treatment,

							@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
								else @next_encounter_type := null
							end as next_encounter_type_general_oncology_treatment,

							@cur_encounter_type := encounter_type as cur_encounter_type,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as next_clinical_datetime_general_oncology_treatment,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as next_clinical_location_id_general_oncology_treatment,

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
							end as next_clinical_rtc_date_general_oncology_treatment,

							case
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinical_rtc_date

							from flat_general_oncology_treatment_1
							order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
						);

						alter table flat_general_oncology_treatment_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;


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

						drop temporary table if exists flat_general_oncology_treatment_3;
						create temporary table flat_general_oncology_treatment_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
						(select
							*,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,

							case
						        when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
						        else @prev_encounter_type:=null
							end as prev_encounter_type_general_oncology_treatment,	
                            @cur_encounter_type := encounter_type as cur_encounter_type,

							case
						        when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
						        else @prev_encounter_datetime := null
						    end as prev_encounter_datetime_general_oncology_treatment,

                            @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as prev_clinical_datetime_general_oncology_treatment,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as prev_clinical_location_id_general_oncology_treatment,

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
							end as prev_clinical_rtc_date_general_oncology_treatment,

							case
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinic_rtc_date

							from flat_general_oncology_treatment_2 t1
							order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
                                        


					select count(*) into @new_encounter_rows from flat_general_oncology_treatment_3;
                    
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
								ecog_performance,
								general_exam,
								heent,
								breast_findings,
								breast_finding_location,
								breast_findingquadrant,
								chest,
								heart,
								abdomenexam,
								urogenital,
								extremities,
								testicular_exam,
								nodal_survey,
								musculoskeletal,
								skin_exam_findings,
								body_part,
								laterality,
								measure_first_direction,
								measure_second_direction,
								neurologic,
								bnody_part,
								patient_on_chemo,
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
								mri_arms,
								mri_pelvic,
								mri_legs,
								ultrasound_renal,
								ultrasound_hepatic,
								obstetric_ultrasound,
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
								method_of_diagnosis,
								diagnosis_on_biopsy,
								biopsy_consistent_clinic_susp,
								diagnosis_results,
								cancer_type,
								other_cancer,
								type_of_sarcoma,
								type_of_gu,
								type_of_gi_cancer,
								head_and_neck_cancer,
								gynecologic_cancer,
								lympoma_cancer,
								skin_cancer,
								breast_cancer,
								other_solid_cancer,
								type_of_leukemeia,
								non_cancer,
								diagnosis_date,
								new_or_recurrence_cancer,
								cancer_stage,
                                overall_cancer_staging,
								test_ordered,
								other_radiology,
								other_laboratory,
								treatment_plan,
								other_treatment_plan,
								hospitalization,
								radiation_location,
								surgery_reason,
								surgical_procedure,
								hormonal_drug,
								targeted_therapies,
								targeted_therapies_frequency,
								targeted_therapy_start_date,
								targeted_therapy_stop_date,
								chemotherapy_plan,
								treatment_on_clinical_trial,
								chemo_start_date,
								treatment_intent,
								chemo_regimen,
								chemo_cycle,
								total_planned_chemo_cycle,
								chemo_drug,
								dosage_in_milligrams,
								drug_route,
								other_drugs,
								other_medication,
								purpose,
								referral_ordered,
								return_to_clinic_date,
								
                                
                                prev_encounter_datetime_general_oncology_treatment,
								next_encounter_datetime_general_oncology_treatment,
								prev_encounter_type_general_oncology_treatment,
								next_encounter_type_general_oncology_treatment,
								prev_clinical_datetime_general_oncology_treatment,
								next_clinical_datetime_general_oncology_treatment,
								prev_clinical_location_id_general_oncology_treatment,
								next_clinical_location_id_general_oncology_treatment,
								prev_clinical_rtc_date_general_oncology_treatment,
								next_clinical_rtc_date_general_oncology_treatment

						from flat_general_oncology_treatment_3 t1
                        join amrs.location t2 using (location_id))');

					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;


					SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_general_oncology_treatment_build_queue__0 t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
					#select @person_ids_count := (select count(*) from flat_general_oncology_treatment_build_queue_2);                        
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
				 #insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
				 select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");

		END$$

DELIMITER ;

