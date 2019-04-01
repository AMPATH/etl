CREATE PROCEDURE `generate_flat_breast_cancer_treatment_v1_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
					set @primary_table := "flat_breast_cancer_treatment";
					set @query_type = query_type;
                     
                    set @total_rows_written = 0;
                    
                    set @encounter_types = "(142,143)";
                    set @clinical_encounter_types = "(-1)";
                    set @non_clinical_encounter_types = "(-1)";
                    set @other_encounter_types = "(-1)";
                    
					set @start = now();
					set @table_version = "flat_breast_cancer_treatment_v1.0";

					set session sort_buffer_size=512000000;

					set @sep = " ## ";
                    set @boundary = "!!";
					set @last_date_created = (select max(max_date_created) from etl.flat_obs);

					#delete from etl.flat_log where table_name like "%flat_breast_cancer_treatment%";
					#drop table etl.flat_breast_cancer_treatment;


					#drop table if exists flat_breast_cancer_treatment;
					create table if not exists flat_breast_cancer_treatment (
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
						breast_findings INT,
						breast_finding_location INT,
						breast_finding_quadrant INT,
						heart INT,
						abdomenexam INT,
						urogenital INT,
						extremities INT,
						nodal_survey INT,
						skin_lessions INT,
						lessions_body_part INT,
						lessions_Laterality INT,
						mass_measure_First_direc INT,
						mass_measure_second_direc INT,
						musculoskeletal INT,
						neurologic INT,
						currently_on_chemo INT,
						test_ordered INT,
						other_radiology varchar(500),
						other_laboratory varchar(500),
						method_of_diagnosis INT,
						diagnosis_based_on_biopsy_type INT,
						biopsy_concordance INT,
						breast_cancer_type INT,
						diagnosis_date DATETIME,
						cancer_stage INT,
						overall_cancer_stage INT,
						treatment_plan INT,
						other_treatment_plan INT,
						hospitalization INT,
						radiation_location INT,
						surgery_reason INT,
						surgical_procedure varchar(500),
						hormonal_drug INT,
						targeted_therapies INT,
						targeted_therapies_frequency INT,
						targeted_therapy_start_date DATETIME,
						targeted_therapy_stop_date DATETIME,
						chemotherapy_plan INT,
						chemo_start_date DATETIME,
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
						rtc INT,
                            
							prev_encounter_datetime_breast_cancer_treatment datetime,
							next_encounter_datetime_breast_cancer_treatment datetime,
							prev_encounter_type_breast_cancer_treatment mediumint,
							next_encounter_type_breast_cancer_treatment mediumint,
							prev_clinical_datetime_breast_cancer_treatment datetime,
							next_clinical_datetime_breast_cancer_treatment datetime,
							prev_clinical_location_id_breast_cancer_treatment mediumint,
							next_clinical_location_id_breast_cancer_treatment mediumint,
							prev_clinical_rtc_date_breast_cancer_treatment datetime,
							next_clinical_rtc_date_breast_cancer_treatment datetime,

							primary key encounter_id (encounter_id),
							index person_date (person_id, encounter_datetime),
							index location_enc_date (location_uuid,encounter_datetime),
							index enc_date_location (encounter_datetime, location_uuid),
							index loc_id_enc_date_next_clinical (location_id, encounter_datetime, next_clinical_datetime_breast_cancer_treatment),
							index encounter_type (encounter_type),
							index date_created (date_created)
							
						);
                        
							
					
                        if(@query_type="build") then
							select 'BUILDING..........................................';
							
#set @write_table = concat("flat_breast_cancer_treatment_temp_",1);
#set @queue_table = concat("flat_breast_cancer_treatment_build_queue_",1);                    												

                            set @write_table = concat("flat_breast_cancer_treatment_temp_",queue_number);
							set @queue_table = concat("flat_breast_cancer_treatment_build_queue_",queue_number);                    												
							

#drop table if exists flat_breast_cancer_treatment_temp_1;							
							SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

							#create  table if not exists @queue_table (person_id int, primary key (person_id));
							SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_breast_cancer_treatment_build_queue limit ', queue_size, ');'); 
#SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_breast_cancer_treatment_build_queue limit 500);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  
							
							#delete t1 from flat_breast_cancer_treatment_build_queue t1 join @queue_table t2 using (person_id)
							SET @dyn_sql=CONCAT('delete t1 from flat_breast_cancer_treatment_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

					end if;
	
					
					if (@query_type="sync") then
							select 'SYNCING..........................................';
							set @write_table = "flat_breast_cancer_treatment";
							set @queue_table = "flat_breast_cancer_treatment_sync_queue";
                            create table if not exists flat_breast_cancer_treatment_sync_queue (person_id int primary key);                            
                            
							set @last_update = null;

                            select max(date_updated) into @last_update from etl.flat_log where table_name=@table_version;

#select max(date_created) into @last_update from etl.flat_log where table_name like "%breast_cancer_treatment%";

#select @last_update;														
select "Finding patients in amrs.encounters...";

							replace into flat_breast_cancer_treatment_sync_queue
							(select distinct patient_id
								from amrs.encounter
								where date_changed > @last_update
							);
						
                        
select "Finding patients in flat_obs...";

							replace into flat_breast_cancer_treatment_sync_queue
							(select distinct person_id
								from etl.flat_obs
								where max_date_created > @last_update
							);


select "Finding patients in flat_lab_obs...";
							replace into flat_breast_cancer_treatment_sync_queue
							(select distinct person_id
								from etl.flat_lab_obs
								where max_date_created > @last_update
							);

select "Finding patients in flat_orders...";

							replace into flat_breast_cancer_treatment_sync_queue
							(select distinct person_id
								from etl.flat_orders
								where max_date_created > @last_update
							);
                            
                            replace into flat_breast_cancer_treatment_sync_queue
                            (select person_id from 
								amrs.person 
								where date_voided > @last_update);


                            replace into flat_breast_cancer_treatment_sync_queue
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


					#delete t1 from flat_breast_cancer_treatment t1 join @queue_table t2 using (person_id);
					SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
                    set @total_time=0;
                    set @cycle_number = 0;
                    

					while @person_ids_count > 0 do

						set @loop_start_time = now();
                        
						#create temp table with a set of person ids
						drop temporary table if exists flat_breast_cancer_treatment_build_queue__0;
						
                        #create temporary table flat_breast_cancer_treatment_build_queue__0 (select * from flat_breast_cancer_treatment_build_queue_2 limit 5000); #TODO - change this when data_fetch_size changes

#SET @dyn_sql=CONCAT('create temporary table flat_breast_cancer_treatment_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit 100);'); 
						SET @dyn_sql=CONCAT('create temporary table flat_breast_cancer_treatment_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                    
						drop temporary table if exists flat_breast_cancer_treatment_0a;
						SET @dyn_sql = CONCAT(
								'create temporary table flat_breast_cancer_treatment_0a
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
									join flat_breast_cancer_treatment_build_queue__0 t0 using (person_id)
									left join etl.flat_orders t2 using(encounter_id)
								where t1.encounter_type in ',@encounter_types,');');
                            
                        PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                    
                    

                        
					
						insert into flat_breast_cancer_treatment_0a
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
								join flat_breast_cancer_treatment_build_queue__0 t0 using (person_id)
						);


						drop temporary table if exists flat_breast_cancer_treatment_0;
						create temporary table flat_breast_cancer_treatment_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
						(select * from flat_breast_cancer_treatment_0a
						order by person_id, date(encounter_datetime), encounter_type_sort_index
						);

                                                
						drop temporary table if exists flat_breast_cancer_treatment_1;
						create temporary table flat_breast_cancer_treatment_1 #(index encounter_id (encounter_id))
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
								when obs regexp "!!1839=7037!!" then @cur_visit_type := 1
								when obs regexp "!!1839=7875!!" then @cur_visit_type := 2
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
								when obs regexp "!!1122=1118!!" then @chest := 1
								when obs regexp "!!1123=1115!!" then @chest := 2
                                when obs regexp "!!1123=5138!!" then @chest := 3
								when obs regexp "!!1123=5115!!" then @chest := 4
                                when obs regexp "!!1123=5116!!" then @chest := 5
								when obs regexp "!!1123=5181!!" then @chest := 6
                                when obs regexp "!!1123=5127!!" then @chest := 7
								else @chest := null
							end as chest,
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
								when obs regexp "!!8268=5107!!" then @breast_finding_quadrant := 1
								when obs regexp "!!8268=1883!!" then @breast_finding_quadrant := 2
                                when obs regexp "!!8268=1882!!" then @breast_finding_quadrant := 3
								when obs regexp "!!8268=5104!!" then @breast_finding_quadrant := 4
                                when obs regexp "!!8268=9695!!" then @breast_finding_quadrant := 5
								else @breast_finding_quadrant := null
							end as breast_finding_quadrant,
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
								when obs regexp "!!1121=1118!!" then @nodal_survey := 1
								when obs regexp "!!1121=1115!!" then @nodal_survey := 2
                                when obs regexp "!!1121=1116!!" then @nodal_survey := 3
								when obs regexp "!!1121=8261!!" then @nodal_survey := 4
								else @nodal_survey := null
							end as nodal_survey,
                            case
								when obs regexp "!!1120=1107!!" then @skin_lessions := 1
								when obs regexp "!!1120=1118!!" then @skin_lessions := 2
                                when obs regexp "!!1120=582!!" then @skin_lessions := 3
								else @skin_lessions := null
							end as skin_lessions,
                            case
								when obs regexp "!!8265=6599!!" then @lessions_body_part := 1
								when obs regexp "!!8265=6598!!" then @lessions_body_part := 2
                                when obs regexp "!!8265=1237!!" then @lessions_body_part := 3
								when obs regexp "!!8265=1236!!" then @lessions_body_part := 4
                                when obs regexp "!!8265=1349!!" then @lessions_body_part := 5
                                when obs regexp "!!8265=6601!!" then @lessions_body_part := 6
								when obs regexp "!!8265=1350!!" then @lessions_body_part := 7
                                when obs regexp "!!8265=6600!!" then @lessions_body_part := 8
								when obs regexp "!!8265=6597!!" then @lessions_body_part := 9
								else @lessions_body_part := null
							end as lessions_body_part,
                            case
								when obs regexp "!!8264=5139!!" then @lessions_Laterality := 1
								when obs regexp "!!8264=5141!!" then @lessions_Laterality := 2
								else @lessions_Laterality := null
							end as lessions_Laterality,
                            case
								when obs regexp "!!8270=" then @mass_measure_First_direc := GetValues(obs, 8270)
								else @mass_measure_First_direc := null
							end as mass_measure_First_direc,
                            case
								when obs regexp "!!8271=" then @mass_measure_second_direc := GetValues(obs, 8271)
								else @mass_measure_second_direc := null
							end as mass_measure_second_direc,
                              case
								when obs regexp "!!1128=1118!!" then @musculoskeletal := 1
								when obs regexp "!!1128=1115!!" then @musculoskeletal := 2
                                when obs regexp "!!1128=1116!!" then @musculoskeletal := 3
								else @musculoskeletal := null
							end as musculoskeletal,
                             case
								when obs regexp "!!1129=1118!!" then @neurologic := 1
								when obs regexp "!!1129=1115!!" then @neurologic := 2
                                when obs regexp "!!1129=599!!" then @neurologic := 3
                                when obs regexp "!!1129=497!!" then @neurologic := 4
                                when obs regexp "!!1129=5108!!" then @neurologic := 5
								else @neurologic := null
							end as neurologic,
                             case
								when obs regexp "!!6575=1065!!" then @currently_on_chemo := 1
								when obs regexp "!!6575=1107!!" then @currently_on_chemo := 2
								else @currently_on_chemo := null
							end as currently_on_chemo,
                            case
								when obs regexp "!!1271=1107!!" then @test_ordered := 1
								when obs regexp "!!1271=1019!!" then @test_ordered := 2
                                when obs regexp "!!1271=790!!" then @test_ordered := 3
								when obs regexp "!!1271=953!!" then @test_ordered := 4
                                when obs regexp "!!1271=6898!!" then @test_ordered := 5
								when obs regexp "!!1271=9009!!" then @test_ordered := 6
								else @test_ordered := null
							end as test_ordered,
                             case
								when obs regexp "!!8190=" then @other_radiology := GetValues(obs, 8190)
								else @other_radiology := null
							end as other_radiology,
                            case
								when obs regexp "!!9538=" then @other_laboratory := GetValues(obs, 9538)
								else @other_laboratory := null
							end as other_laboratory,
                            case
								when obs regexp "!!6504=7189!!" then @method_of_diagnosis := 1
								when obs regexp "!!6504=6507!!" then @method_of_diagnosis := 2
                                when obs regexp "!!6504=6505!!" then @method_of_diagnosis := 3
								when obs regexp "!!6504=6506!!" then @method_of_diagnosis := 4
								else @method_of_diagnosis := null
							end as method_of_diagnosis,
                            case
								when obs regexp "!!6509=6510!!" then @diagnosis_based_on_biopsy_type := 1
								when obs regexp "!!6509=6511!!" then @diagnosis_based_on_biopsy_type := 2
                                when obs regexp "!!6509=7190!!" then @diagnosis_based_on_biopsy_type := 3
								when obs regexp "!!6509=6512!!" then @diagnosis_based_on_biopsy_type := 4
								else @diagnosis_based_on_biopsy_type := null
							end as diagnosis_based_on_biopsy_type,
                            case
								when obs regexp "!!6605=1065!!" then @biopsy_concordance := 1
								when obs regexp "!!6605=1066!!" then @biopsy_concordance := 2
								else @biopsy_concordance := null
							end as biopsy_concordance,
                            case
								when obs regexp "!!9841=6545!!" then @breast_cancer_type := 1
								when obs regexp "!!9841=9842!!" then @breast_cancer_type := 2
                                when obs regexp "!!9841=5622!!" then @breast_cancer_type := 3
								else @breast_cancer_type := null
							end as breast_cancer_type,
                            case
								when obs regexp "!!9728=" then @diagnosis_date := GetValues(obs, 9728)
								else @diagnosis_date := null
							end as diagnosis_date,
                            case
								when obs regexp "!!6582=1067!!" then @cancer_stage := 1
								when obs regexp "!!6582=6566!!" then @cancer_stage := 2
                                when obs regexp "!!6582=10206!!" then @cancer_stage := 3
                                when obs regexp "!!6582=1175!!" then @cancer_stage := 4
								else @cancer_stage := null
							end as cancer_stage,
                            case
								when obs regexp "!!9868=9851!!" then @overall_cancer_stage := 1
								when obs regexp "!!9868=9852!!" then @overall_cancer_stage := 2
                                when obs regexp "!!9868=9853!!" then @overall_cancer_stage := 3
                                when obs regexp "!!9868=9854!!" then @overall_cancer_stage := 4
                                when obs regexp "!!9868=9855!!" then @overall_cancer_stage := 5
								when obs regexp "!!9868=9856!!" then @overall_cancer_stage := 6
                                when obs regexp "!!9868=9857!!" then @overall_cancer_stage := 7
                                when obs regexp "!!9868=9858!!" then @overall_cancer_stage := 8
                                when obs regexp "!!9868=9859!!" then @overall_cancer_stage := 9
								when obs regexp "!!9868=9860!!" then @overall_cancer_stage := 10
                                when obs regexp "!!9868=9861!!" then @overall_cancer_stage := 11
                                when obs regexp "!!9868=9862!!" then @overall_cancer_stage := 12
                                when obs regexp "!!9868=9863!!" then @overall_cancer_stage := 13
								when obs regexp "!!9868=9864!!" then @overall_cancer_stage := 14
                                when obs regexp "!!9868=9865!!" then @overall_cancer_stage := 15
                                when obs regexp "!!9868=9866!!" then @overall_cancer_stage := 16
                                when obs regexp "!!9868=9867!!" then @overall_cancer_stage := 17
								else @overall_cancer_stage := null
                                
							end as overall_cancer_stage,
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
								when obs regexp "!!10039=" then @other_treatment_plan := GetValues(obs, 10039)
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
								when obs regexp "!!10230=" then @surgical_procedure := GetValues(obs, 10230)
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
							when obs regexp "!!10235=" then @targeted_therapy_start_date := GetValues(obs, 10235)
								else @targeted_therapy_start_date := null
						  end as targeted_therapy_start_date,
                          case
							when obs regexp "!!10236=" then @targeted_therapy_stop_date := GetValues(obs, 10236)
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
							when obs regexp "!!1190=" then @chemo_start_date := GetValues(obs, 1190)
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
							when obs regexp "!!9946=" then @chemo_regimen := GetValues(obs, 9946)
								else @chemo_regimen := null
						  end as chemo_regimen,
                          case
							when obs regexp "!!6643=" then @chemo_cycle := GetValues(obs, 6643)
								else @chemo_cycle := null
						  end as chemo_cycle,
                          case
							when obs regexp "!!6644=" then @total_planned_chemo_cycle := GetValues(obs, 6644)
								else @total_planned_chemo_cycle := null
						  end as total_planned_chemo_cycle,
                          case
							when obs regexp "!!9918=" then @chemo_drug := GetValues(obs, 9918)
								else @chemo_drug := null
						  end as chemo_drug,
                          case
							when obs regexp "!!1899=" then @dosage_in_milligrams := GetValues(obs, 1899)
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
							when obs regexp "!!1895=" then @other_drugs := GetValues(obs, 1895)
								else @other_drugs := null
						  end as other_drugs,
                          case
							when obs regexp "!!1779=" then @other_medication := GetValues(obs, 1779)
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
                                when obs regexp "!!1272=5486!!" then @referral_ordered := 3
								when obs regexp "!!1272=5884!!" then @referral_ordered := 4
                                when obs regexp "!!1272=6570!!" then @referral_ordered := 5
								when obs regexp "!!1272=5622!!" then @referral_ordered := 6
								else @referral_ordered := null
							end as referral_ordered,
						case
							when obs regexp "!!5096=" then @rtc := GetValues(obs, 5096)
								else @rtc := null
						  end as rtc
		
						from flat_breast_cancer_treatment_0 t1
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


						alter table flat_breast_cancer_treatment_1 drop prev_id, drop cur_id;

						drop table if exists flat_breast_cancer_treatment_2;
						create temporary table flat_breast_cancer_treatment_2
						(select *,
							@prev_id := @cur_id as prev_id,
							@cur_id := person_id as cur_id,

							case
								when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
								else @prev_encounter_datetime := null
							end as next_encounter_datetime_breast_cancer_treatment,

							@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
								else @next_encounter_type := null
							end as next_encounter_type_breast_cancer_treatment,

							@cur_encounter_type := encounter_type as cur_encounter_type,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as next_clinical_datetime_breast_cancer_treatment,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as next_clinical_location_id_breast_cancer_treatment,

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
							end as next_clinical_rtc_date_breast_cancer_treatment,

							case
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinical_rtc_date

							from flat_breast_cancer_treatment_1
							order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
						);

						alter table flat_breast_cancer_treatment_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;


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

						drop temporary table if exists flat_breast_cancer_treatment_3;
						create temporary table flat_breast_cancer_treatment_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
						(select
							*,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,

							case
						        when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
						        else @prev_encounter_type:=null
							end as prev_encounter_type_breast_cancer_treatment,	
                            @cur_encounter_type := encounter_type as cur_encounter_type,

							case
						        when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
						        else @prev_encounter_datetime := null
						    end as prev_encounter_datetime_breast_cancer_treatment,

                            @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as prev_clinical_datetime_breast_cancer_treatment,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as prev_clinical_location_id_breast_cancer_treatment,

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
							end as prev_clinical_rtc_date_breast_cancer_treatment,

							case
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinic_rtc_date

							from flat_breast_cancer_treatment_2 t1
							order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
                                        


					select count(*) into @new_encounter_rows from flat_breast_cancer_treatment_3;
                    
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
											general_exam ,
											heent,
											chest ,
											breast_findings ,
											breast_finding_location ,
											breast_finding_quadrant ,
											heart ,
											abdomenexam ,
											urogenital ,
											extremities,
											nodal_survey,
											skin_lessions,
											lessions_body_part,
											lessions_Laterality,
											mass_measure_First_direc ,
											mass_measure_second_direc ,
											musculoskeletal ,
											neurologic ,
											currently_on_chemo ,
											test_ordered ,
											other_radiology ,
											other_laboratory ,
											method_of_diagnosis ,
											diagnosis_based_on_biopsy_type ,
											biopsy_concordance ,
											breast_cancer_type ,
											diagnosis_date,
											cancer_stage ,
											overall_cancer_stage ,
											treatment_plan ,
											other_treatment_plan ,
											hospitalization ,
											radiation_location ,
											surgery_reason ,
											surgical_procedure ,
											hormonal_drug ,
											targeted_therapies ,
											targeted_therapies_frequency ,
											targeted_therapy_start_date ,
											targeted_therapy_stop_date,
											chemotherapy_plan ,
											chemo_start_date ,
											treatment_intent ,
											chemo_regimen ,
											chemo_cycle ,
											total_planned_chemo_cycle ,
											chemo_drug ,
											dosage_in_milligrams ,
											Drug_route ,
											other_drugs ,
											other_medication ,
											purpose ,
											referral_ordered ,
											rtc,
											prev_encounter_datetime_breast_cancer_treatment,
											next_encounter_datetime_breast_cancer_treatment,
											prev_encounter_type_breast_cancer_treatment,
											next_encounter_type_breast_cancer_treatment,
											prev_clinical_datetime_breast_cancer_treatment,
											next_clinical_datetime_breast_cancer_treatment,
											prev_clinical_location_id_breast_cancer_treatment,
											next_clinical_location_id_breast_cancer_treatment,
											prev_clinical_rtc_date_breast_cancer_treatment,
											next_clinical_rtc_date_breast_cancer_treatment

						from flat_breast_cancer_treatment_3 t1
                        join amrs.location t2 using (location_id))');

					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;


					SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_breast_cancer_treatment_build_queue__0 t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
					#select @person_ids_count := (select count(*) from flat_breast_cancer_treatment_build_queue_2);                        
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
				 #insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
				 select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");

		END