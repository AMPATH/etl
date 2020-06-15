DELIMITER $$
CREATE PROCEDURE `generate_flat_hemophilia_v1_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
					set @primary_table := "flat_hemophilia";
					set @query_type = query_type;
                     
                    set @total_rows_written = 0;
                    
                    set @encounter_types = "(91,92)";
                    set @clinical_encounter_types = "(-1)";
                    set @non_clinical_encounter_types = "(-1)";
                    set @other_encounter_types = "(-1)";
                    
					set @start = now();
					set @table_version = "flat_hemophilia_v1.0";

					set session sort_buffer_size=512000000;

					set @sep = " ## ";
                    set @boundary = "!!";
					set @last_date_created = (select max(max_date_created) from etl.flat_obs);

					#delete from etl.flat_log where table_name like "%flat_hemophilia%";
					#drop table etl.flat_hemophilia;


					#drop table if exists flat_hemophilia;
					create table if not exists flat_hemophilia (
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
							chest INT,
							heart INT,
							abdomenexam INT,
							urogenital INT,
							extremities INT,
							testicular_exam INT,
							nodal_survey INT,
							musculoskeletal INT,
							neurologic INT,
							anatomic_location INT,
						    laterality INT,
							physical_activity INT,
							recommendations INT,
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
                            age_at_diagnosis INT,
							age_at_first_bleed INT,
							site_first_bleed INT,
							bleeding_disorder INT,
							severity INT,
							baseline_factor INT,
							diagnosis_method INT,
							confirmed_ampath_hematology INT,
                            test_ordered INT,
							other_radiology varchar(500),
							other_laboratory varchar(500),
                            education_provided INT,
							blood_transfusion INT,
							referral INT,
							reasons_for_refferal INT,
							rtc DATETIME,
                            
							prev_encounter_datetime_hemophilia datetime,
							next_encounter_datetime_hemophilia datetime,
							prev_encounter_type_hemophilia mediumint,
							next_encounter_type_hemophilia mediumint,
							prev_clinical_datetime_hemophilia datetime,
							next_clinical_datetime_hemophilia datetime,
							prev_clinical_location_id_hemophilia mediumint,
							next_clinical_location_id_hemophilia mediumint,
							prev_clinical_rtc_date_hemophilia datetime,
							next_clinical_rtc_date_hemophilia datetime,

							primary key encounter_id (encounter_id),
							index person_date (person_id, encounter_datetime),
							index location_enc_date (location_uuid,encounter_datetime),
							index enc_date_location (encounter_datetime, location_uuid),
							index loc_id_enc_date_next_clinical (location_id, encounter_datetime, next_clinical_datetime_hemophilia),
							index encounter_type (encounter_type),
							index date_created (date_created)
							
						);
                        
							
					
                        if(@query_type="build") then
							select 'BUILDING..........................................';
							
#set @write_table = concat("flat_hemophilia_temp_",1);
#set @queue_table = concat("flat_hemophilia_build_queue_",1);                    												

                            set @write_table = concat("flat_hemophilia_temp_",queue_number);
							set @queue_table = concat("flat_hemophilia_build_queue_",queue_number);                    												
							

#drop table if exists flat_hemophilia_temp_1;							
							SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

							#create  table if not exists @queue_table (person_id int, primary key (person_id));
							SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_hemophilia_build_queue limit ', queue_size, ');'); 
#SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_hemophilia_build_queue limit 500);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  
							
							#delete t1 from flat_hemophilia_build_queue t1 join @queue_table t2 using (person_id)
							SET @dyn_sql=CONCAT('delete t1 from flat_hemophilia_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

					end if;
	
					
					if (@query_type="sync") then
							select 'SYNCING..........................................';
							set @write_table = "flat_hemophilia";
							set @queue_table = "flat_hemophilia_sync_queue";
                            create table if not exists flat_hemophilia_sync_queue (person_id int primary key);                            
                            
							set @last_update = null;

                            select max(date_updated) into @last_update from etl.flat_log where table_name=@table_version;

#select max(date_created) into @last_update from etl.flat_log where table_name like "%hemophilia%";

#select @last_update;														
select "Finding patients in amrs.encounters...";

							replace into flat_hemophilia_sync_queue
							(select distinct patient_id
								from amrs.encounter
								where date_changed > @last_update
							);
						
                        
select "Finding patients in flat_obs...";

							replace into flat_hemophilia_sync_queue
							(select distinct person_id
								from etl.flat_obs
								where max_date_created > @last_update
							);


select "Finding patients in flat_lab_obs...";
							replace into flat_hemophilia_sync_queue
							(select distinct person_id
								from etl.flat_lab_obs
								where max_date_created > @last_update
							);

select "Finding patients in flat_orders...";

							replace into flat_hemophilia_sync_queue
							(select distinct person_id
								from etl.flat_orders
								where max_date_created > @last_update
							);
                            
                            replace into flat_hemophilia_sync_queue
                            (select person_id from 
								amrs.person 
								where date_voided > @last_update);


                            replace into flat_hemophilia_sync_queue
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


					#delete t1 from flat_hemophilia t1 join @queue_table t2 using (person_id);
					SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
                    set @total_time=0;
                    set @cycle_number = 0;
                    

					while @person_ids_count > 0 do

						set @loop_start_time = now();
                        
						#create temp table with a set of person ids
						drop temporary table if exists flat_hemophilia_build_queue__0;
						
                        #create temporary table flat_hemophilia_build_queue__0 (select * from flat_hemophilia_build_queue_2 limit 5000); #TODO - change this when data_fetch_size changes

#SET @dyn_sql=CONCAT('create temporary table flat_hemophilia_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit 100);'); 
						SET @dyn_sql=CONCAT('create temporary table flat_hemophilia_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                    
						drop temporary table if exists flat_hemophilia_0a;
						SET @dyn_sql = CONCAT(
								'create temporary table flat_hemophilia_0a
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
									join flat_hemophilia_build_queue__0 t0 using (person_id)
									left join etl.flat_orders t2 using(encounter_id)
								where t1.encounter_type in ',@encounter_types,');');
                            
                        PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                    
                    

                        
					
						insert into flat_hemophilia_0a
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
								join flat_hemophilia_build_queue__0 t0 using (person_id)
						);


						drop temporary table if exists flat_hemophilia_0;
						create temporary table flat_hemophilia_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
						(select * from flat_hemophilia_0a
						order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
							set @cur_visit_type = null;
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
                            set @anatomic_location = null;
							set @laterality = null;
							set @physical_activity = null;
							set @recommendations = null;
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
                            set @age_at_diagnosis = null;
							set @age_at_first_bleed = null;
							set @site_first_bleed = null;
							set @bleeding_disorder = null;
							set @severity = null;
							set @baseline_factor = null;
							set @diagnosis_method = null;
							set @confirmed_ampath_hematology = null;
                            set @test_ordered = null;
							set @other_radiology = null;
							set @other_laboratory = null;
                            set @education_provided = null;
							set @blood_transfusion  = null;
							set @referral = null;
							set @reasons_for_refferal = null;
							set @rtc = null;

                                                
						drop temporary table if exists flat_hemophilia_1;
						create temporary table flat_hemophilia_1 #(index encounter_id (encounter_id))
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
								when obs regexp "!!1834=7850!!" then @cur_visit_type := 1
								when obs regexp "!!1834=10037!!" then @cur_visit_type := 2
								when obs regexp "!!1834=2345!!" then @cur_visit_type := 3
								when obs regexp "!!1834=1246!!" then @cur_visit_type := 4
								when obs regexp "!!1834=1068!!" then @cur_visit_type := 5
								else @cur_visit_type = null
					        end as cur_visit_type,
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
						when obs regexp "!!9947=8770!!" then @anatomic_location := 1
						when obs regexp "!!9947=8768!!" then @anatomic_location := 2
						when obs regexp "!!9947=8772!!" then @anatomic_location := 3
						when obs regexp "!!9947=8769!!" then @anatomic_location := 4
                        when obs regexp "!!9947=8771!!" then @anatomic_location := 5
                        when obs regexp "!!9947=8773!!" then @anatomic_location := 6
                        when obs regexp "!!9947=8782!!" then @anatomic_location := 7
						when obs regexp "!!9947=1345!!" then @anatomic_location := 8
                        when obs regexp "!!9947=1347!!" then @anatomic_location := 9
                        when obs regexp "!!9947=8783!!" then @anatomic_location := 10
						else @anatomic_location = null
					end as anatomic_location,
                    case	
						when obs regexp "!!8264=5141!!" then @laterality := 1
						when obs regexp "!!8264=5139!!" then @laterality := 2
						when obs regexp "!!8264=2399!!" then @laterality := 3
						else @laterality = null
					end as laterality,
                    case	
						when obs regexp "!!8797=8798!!" then @physical_activity := 1
						when obs regexp "!!8797=8799!!" then @physical_activity := 2
						when obs regexp "!!8797=8800!!" then @physical_activity := 3
                        when obs regexp "!!8797=8801!!" then @physical_activity := 3
						else @physical_activity = null
					end as physical_activity,
                    case	
						when obs regexp "!!1705=1107!!" then @recommendations := 1
						when obs regexp "!!1705=8802!!" then @recommendations := 2
						when obs regexp "!!1705=8812!!" then @recommendations := 3
						when obs regexp "!!1705=8804!!" then @recommendations := 4
                        when obs regexp "!!1705=8805!!" then @recommendations := 5
                        when obs regexp "!!1705=8808!!" then @recommendations := 6
                        when obs regexp "!!1705=8813!!" then @recommendations := 7
						when obs regexp "!!1705=8806!!" then @recommendations := 8
                        when obs regexp "!!1705=8809!!" then @recommendations := 9
                        when obs regexp "!!1705=8814!!" then @recommendations := 10
                        when obs regexp "!!1705=8803!!" then @recommendations := 11
                        when obs regexp "!!1705=8810!!" then @recommendations := 12
						when obs regexp "!!1705=8815!!" then @recommendations := 13
                        when obs regexp "!!1705=8807!!" then @recommendations := 14
                        when obs regexp "!!1705=8811!!" then @recommendations := 15
                        when obs regexp "!!1705=8816!!" then @recommendations := 16
						else @recommendations = null
					end as recommendations,
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
						when obs regexp "!!8778=" then @age_at_diagnosis := GetValues(obs,8778)
						else @age_at_diagnosis := null
					end as age_at_diagnosis,
                    case
						when obs regexp "!!8779=" then @age_at_first_bleed := GetValues(obs,8779)
						else @age_at_first_bleed := null
					end as age_at_first_bleed,
                    case	
						when obs regexp "!!8780=8771!!" then @site_first_bleed := 1
						when obs regexp "!!8780=8768!!" then @site_first_bleed := 2
                        when obs regexp "!!8780=8769!!" then @site_first_bleed := 3
                        when obs regexp "!!8780=8773!!" then @site_first_bleed := 4
                        when obs regexp "!!8780=7941!!" then @site_first_bleed := 5
                        when obs regexp "!!8780=8757!!" then @site_first_bleed := 6
                        when obs regexp "!!8780=5622!!" then @site_first_bleed := 7
						else @site_first_bleed = null
					end as site_first_bleed,
                    case	
						when obs regexp "!!6402=8789!!" then @bleeding_disorder := 1
						when obs regexp "!!6402=8790!!" then @bleeding_disorder := 2
                        when obs regexp "!!6402=8791!!" then @bleeding_disorder := 3
                        when obs regexp "!!6402=5622!!" then @bleeding_disorder := 4
						else @bleeding_disorder = null
					end as bleeding_disorder,
                    case	
						when obs regexp "!!8792=1743!!" then @severity := 1
						when obs regexp "!!8792=1744!!" then @severity := 2
                        when obs regexp "!!8792=1745!!" then @severity := 3
                        when obs regexp "!!8792=1067!!" then @severity := 4
                        when obs regexp "!!8792=8989!!" then @severity := 5
						else @severity = null
					end as severity,
                    case
						when obs regexp "!!8794=" then @baseline_factor := GetValues(obs,8794)
						else @baseline_factor := null
					end as baseline_factor,
                    case	
						when obs regexp "!!6504=8795!!" then @diagnosis_method := 1
						when obs regexp "!!6504=6506!!" then @diagnosis_method := 2
						else @diagnosis_method = null
					end as diagnosis_method,
                    case	
						when obs regexp "!!8796=1065!!" then @confirmed_ampath_hematology := 1
						when obs regexp "!!8796=1066!!" then @confirmed_ampath_hematology := 2
                        when obs regexp "!!8796=1624!!" then @confirmed_ampath_hematology := 2
						else @confirmed_ampath_hematology = null
					end as confirmed_ampath_hematology,
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
								when obs regexp "!!6327=8817!!" then @education_provided := 1
								when obs regexp "!!6327=8820!!" then @education_provided := 2
								when obs regexp "!!6327=8823!!" then @education_provided := 3
								when obs regexp "!!6327=7055!!" then @education_provided := 4
								when obs regexp "!!6327=8821!!" then @education_provided := 5
                                when obs regexp "!!6327=8824!!" then @education_provided := 6
								when obs regexp "!!6327=8821!!" then @education_provided := 7
								when obs regexp "!!6327=2373!!" then @education_provided := 8
								else @education_provided = null
							end as education_provided,
                            case	
								when obs regexp "!!8743=8774!!" then @blood_transfusion := 1
								when obs regexp "!!8743=8775!!" then @blood_transfusion := 2
								else @blood_transfusion = null
							end as blood_transfusion,
                             case	
								when obs regexp "!!1272=1905!!" then @referral := 1
								when obs regexp "!!1272=8825!!" then @referral := 2
								when obs regexp "!!1272=1580!!" then @referral := 3
								when obs regexp "!!1272=1902!!" then @referral := 4
								else @referral = null
							end as referral,
                            case	
								when obs regexp "!!2327=1578!!" then @reasons_for_refferal := 1
								when obs regexp "!!2327=1548!!" then @reasons_for_refferal := 2
								when obs regexp "!!2327=2329!!" then @reasons_for_refferal := 3
								when obs regexp "!!2327=2330!!" then @reasons_for_refferal := 4
								when obs regexp "!!2327=5484!!" then @reasons_for_refferal := 5
                                when obs regexp "!!2327=6502!!" then @reasons_for_refferal := 6
								when obs regexp "!!2327=5622!!" then @reasons_for_refferal := 7
								when obs regexp "!!2327=5096!!" then @reasons_for_refferal := 8
								else @reasons_for_refferal = null
							end as reasons_for_refferal,
                            case
								when obs regexp "!!5096=" then @rtc := GetValues(obs,5096) 
								else @rtc := null
							end as rtc
                    
		
						from flat_hemophilia_0 t1
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


						alter table flat_hemophilia_1 drop prev_id, drop cur_id;

						drop table if exists flat_hemophilia_2;
						create temporary table flat_hemophilia_2
						(select *,
							@prev_id := @cur_id as prev_id,
							@cur_id := person_id as cur_id,

							case
								when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
								else @prev_encounter_datetime := null
							end as next_encounter_datetime_hemophilia,

							@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
								else @next_encounter_type := null
							end as next_encounter_type_hemophilia,

							@cur_encounter_type := encounter_type as cur_encounter_type,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as next_clinical_datetime_hemophilia,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as next_clinical_location_id_hemophilia,

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
							end as next_clinical_rtc_date_hemophilia,

							case
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinical_rtc_date

							from flat_hemophilia_1
							order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
						);

						alter table flat_hemophilia_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;


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

						drop temporary table if exists flat_hemophilia_3;
						create temporary table flat_hemophilia_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
						(select
							*,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,

							case
						        when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
						        else @prev_encounter_type:=null
							end as prev_encounter_type_hemophilia,	
                            @cur_encounter_type := encounter_type as cur_encounter_type,

							case
						        when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
						        else @prev_encounter_datetime := null
						    end as prev_encounter_datetime_hemophilia,

                            @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as prev_clinical_datetime_hemophilia,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as prev_clinical_location_id_hemophilia,

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
							end as prev_clinical_rtc_date_hemophilia,

							case
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinic_rtc_date

							from flat_hemophilia_2 t1
							order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
                                        


					select count(*) into @new_encounter_rows from flat_hemophilia_3;
                    
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
								chest,
								heart,
								abdomenexam,
								urogenital,
								extremities,
								testicular_exam,
								nodal_survey,
								musculoskeletal,
								neurologic,
                                anatomic_location,
								laterality,
								physical_activity,
								recommendations,
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
                                age_at_diagnosis,
								age_at_first_bleed,
								site_first_bleed,
								bleeding_disorder,
								severity,
								baseline_factor,
								diagnosis_method,
								confirmed_ampath_hematology,
                                test_ordered,
								other_radiology,
								other_laboratory,
                                education_provided,
								blood_transfusion,
								referral,
								reasons_for_refferal,
								rtc,
                                
                                prev_encounter_datetime_hemophilia,
								next_encounter_datetime_hemophilia,
								prev_encounter_type_hemophilia,
								next_encounter_type_hemophilia,
								prev_clinical_datetime_hemophilia,
								next_clinical_datetime_hemophilia,
								prev_clinical_location_id_hemophilia,
								next_clinical_location_id_hemophilia,
								prev_clinical_rtc_date_hemophilia,
								next_clinical_rtc_date_hemophilia

						from flat_hemophilia_3 t1
                        join amrs.location t2 using (location_id))');

					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;


					SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_hemophilia_build_queue__0 t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
					#select @person_ids_count := (select count(*) from flat_hemophilia_build_queue_2);                        
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
