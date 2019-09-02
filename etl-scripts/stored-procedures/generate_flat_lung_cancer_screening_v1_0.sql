CREATE PROCEDURE `generate_flat_lung_cancer_screening_v1_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
					set @primary_table := "flat_lung_cancer_screening";
					set @query_type = query_type;
                     
                    set @total_rows_written = 0;
                    
                    set @encounter_types = "(177,185)";
                    set @clinical_encounter_types = "(-1)";
                    set @non_clinical_encounter_types = "(-1)";
                    set @other_encounter_types = "(-1)";
                    
					set @start = now();
					set @table_version = "flat_lung_cancer_screening_v1.0";

					set session sort_buffer_size=512000000;

					set @sep = " ## ";
                    set @boundary = "!!";
					set @last_date_created = (select max(max_date_created) from etl.flat_obs);

					#delete from etl.flat_log where table_name like "%flat_lung_cancer_screening%";
					#drop table etl.flat_lung_cancer_screening;


					#drop table if exists flat_lung_cancer_screening;
					create table if not exists flat_lung_cancer_screening (
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
							referral_from INT,
							chief_complains INT,
							complain_duration INT,
							number_of_days INT,
							number_of_weeks INT,
							number_of_months INT,
							number_of_years INT,
							ever_smoked_cigarettes INT,
							number_of_sticks INT,
							cigarette_duration INT,
							tobacco_use INT,
							tobacco_duration INT,
							main_occupation INT,
							chemical_exposure INT,
							asbestos_exposure INT,
							other_cancer_in_family INT,
							hiv_status INT,
							previous_tb_treatment INT,
							treatment_duration INT,
							number_of_tb_treatment INT,
							previous_test_ordered INT,
							sputum_aafbs_results INT,
							gene_expert_results INT,
							x_ray_results INT,
							procedure_ordered INT,
							imaging_test_ordered INT,
							radiology_test VARCHAR(1000),
							follow_up_care_plan INT,
							rtc_date DATETIME,
							imaging_results INT,
							ct_findings INT,
                            Imaging_results_description varchar(500),
							biospy_procedure_done INT,
                            biopsy_workup_date DATETIME,
							biospy_results INT,
                            other_condition varchar(500),
							type_of_malignancy INT,
							other_malignancy VARCHAR(1000),
							lung_cancer_type INT,
							non_small_cancer INT,
							repository_disease INT,
							diagnosis_date DATETIME,
							referral_ordered INT,
							return_date DATETIME,
                            
							prev_encounter_datetime_lung_cancer_screening datetime,
							next_encounter_datetime_lung_cancer_screening datetime,
							prev_encounter_type_lung_cancer_screening mediumint,
							next_encounter_type_lung_cancer_screening mediumint,
							prev_clinical_datetime_lung_cancer_screening datetime,
							next_clinical_datetime_lung_cancer_screening datetime,
							prev_clinical_location_id_lung_cancer_screening mediumint,
							next_clinical_location_id_lung_cancer_screening mediumint,
							prev_clinical_rtc_date_lung_cancer_screening datetime,
							next_clinical_rtc_date_lung_cancer_screening datetime,

							primary key encounter_id (encounter_id),
							index person_date (person_id, encounter_datetime),
							index location_enc_date (location_uuid,encounter_datetime),
							index enc_date_location (encounter_datetime, location_uuid),
							index loc_id_enc_date_next_clinical (location_id, encounter_datetime, next_clinical_datetime_lung_cancer_screening),
							index encounter_type (encounter_type),
							index date_created (date_created)
							
						);
                        
							
					
                        if(@query_type="build") then
							select 'BUILDING..........................................';
							
#set @write_table = concat("flat_lung_cancer_screening_temp_",1);
#set @queue_table = concat("flat_lung_cancer_screening_build_queue_",1);                    												

                            set @write_table = concat("flat_lung_cancer_screening_temp_",queue_number);
							set @queue_table = concat("flat_lung_cancer_screening_build_queue_",queue_number);                    												
							

#drop table if exists flat_lung_cancer_screening_temp_1;							
							SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

							#create  table if not exists @queue_table (person_id int, primary key (person_id));
							SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_lung_cancer_screening_build_queue limit ', queue_size, ');'); 
#SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_lung_cancer_screening_build_queue limit 500);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  
							
							#delete t1 from flat_lung_cancer_screening_build_queue t1 join @queue_table t2 using (person_id)
							SET @dyn_sql=CONCAT('delete t1 from flat_lung_cancer_screening_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

					end if;
	
					
					if (@query_type="sync") then
							select 'SYNCING..........................................';
							set @write_table = "flat_lung_cancer_screening";
							set @queue_table = "flat_lung_cancer_screening_sync_queue";
                            create table if not exists flat_lung_cancer_screening_sync_queue (person_id int primary key);                            
                            
							set @last_update = null;

                            select max(date_updated) into @last_update from etl.flat_log where table_name=@table_version;

#select max(date_created) into @last_update from etl.flat_log where table_name like "%lung_cancer_screening%";

#select @last_update;														
select "Finding patients in amrs.encounters...";

							replace into flat_lung_cancer_screening_sync_queue
							(select distinct patient_id
								from amrs.encounter
								where date_changed > @last_update
							);
						
                        
select "Finding patients in flat_obs...";

							replace into flat_lung_cancer_screening_sync_queue
							(select distinct person_id
								from etl.flat_obs
								where max_date_created > @last_update
							);


select "Finding patients in flat_lab_obs...";
							replace into flat_lung_cancer_screening_sync_queue
							(select distinct person_id
								from etl.flat_lab_obs
								where max_date_created > @last_update
							);

select "Finding patients in flat_orders...";

							replace into flat_lung_cancer_screening_sync_queue
							(select distinct person_id
								from etl.flat_orders
								where max_date_created > @last_update
							);
                            
                            replace into flat_lung_cancer_screening_sync_queue
                            (select person_id from 
								amrs.person 
								where date_voided > @last_update);


                            replace into flat_lung_cancer_screening_sync_queue
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


					#delete t1 from flat_lung_cancer_screening t1 join @queue_table t2 using (person_id);
					SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
                    set @total_time=0;
                    set @cycle_number = 0;
                    

					while @person_ids_count > 0 do

						set @loop_start_time = now();
                        
						#create temp table with a set of person ids
						drop temporary table if exists flat_lung_cancer_screening_build_queue__0;
						
                        #create temporary table flat_lung_cancer_screening_build_queue__0 (select * from flat_lung_cancer_screening_build_queue_2 limit 5000); #TODO - change this when data_fetch_size changes

#SET @dyn_sql=CONCAT('create temporary table flat_lung_cancer_screening_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit 100);'); 
						SET @dyn_sql=CONCAT('create temporary table flat_lung_cancer_screening_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                    
						drop temporary table if exists flat_lung_cancer_screening_0a;
						SET @dyn_sql = CONCAT(
								'create temporary table flat_lung_cancer_screening_0a
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
									join flat_lung_cancer_screening_build_queue__0 t0 using (person_id)
									left join etl.flat_orders t2 using(encounter_id)
								where t1.encounter_type in ',@encounter_types,');');
                            
                        PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                    
                    

                        
					
						insert into flat_lung_cancer_screening_0a
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
								join flat_lung_cancer_screening_build_queue__0 t0 using (person_id)
						);


						drop temporary table if exists flat_lung_cancer_screening_0;
						create temporary table flat_lung_cancer_screening_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
						(select * from flat_lung_cancer_screening_0a
						order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
                        
                        
                        set @cur_visit_type = null;
                        set @referral_from = null;
                        set @chief_complains = null;
                        set @complain_duration = null;
						set @number_of_days = null;
						set @number_of_weeks = null;
						set @number_of_months = null;
						set @number_of_years = null;
						set @ever_smoked_cigarettes = null;
						set @number_of_sticks = null;
						set @cigarette_duration = null;
						set @tobacco_use = null;
						set @tobacco_duration = null;
						set @main_occupation = null;
						set @chemical_exposure = null;
						set @asbestos_exposure = null;
						set @other_cancer_in_family = null;
						set @hiv_status = null;
                        set @previous_tb_treatment = null;
						set @treatment_duration = null;
						set @number_of_tb_treatment= null;
						set @previous_test_ordered = null;
						set @sputum_aafbs_results = null;
						set @gene_expert_results = null;
						set @x_ray_results = null;
						set @procedure_ordered= null;
						set @imaging_test_ordered = null;
						set @radiology_test = null;
						set @follow_up_care_plan = null;
						set @rtc_date = null;
						set @imaging_results= null;
						set @ct_findings = null;
                        set @Imaging_results_description = null;
						set @biospy_procedure_done = null;
                        set @biopsy_workup_date = null;
						set @biospy_results = null;
                        set @other_condition = null;
						set @type_of_malignancy = null;
						set @other_malignancy = null;
						set @lung_cancer_type = null;
						set @non_small_cancer = null;
						set @repository_disease = null;
						set @diagnosis_date = null;
						set @referral_ordered = null;
						set @return_date = null;

                                                
						drop temporary table if exists flat_lung_cancer_screening_1;
						create temporary table flat_lung_cancer_screening_1 #(index encounter_id (encounter_id))
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

						when obs regexp "!!6749=6572!!" then @referral_from := 1

						when obs regexp "!!6749=8161!!" then @referral_from := 2

						when obs regexp "!!6749=2242!!" then @referral_from := 3

						when obs regexp "!!6749=978!!" then @referral_from := 4

						when obs regexp "!!6749=5487!!" then @referral_from := 5

						when obs regexp "!!6749=1275!!" then @referral_from := 6

						when obs regexp "!!6749=5622!!" then @referral_from := 7

						else @referral_from = null

					end as referral_from,

					case	
						when obs regexp "!!5219=107!!" then @chief_complains := 1

						when obs regexp "!!5219=5960!!" then @chief_complains := 2

						when obs regexp "!!5219=6786!!" then @chief_complains := 3

						when obs regexp "!!5219=136!!" then @chief_complains := 4

						when obs regexp "!!5219=7225!!" then @chief_complains := 5

						when obs regexp "!!5219=456!!" then @chief_complains := 6

						when obs regexp "!!5219=832!!" then @chief_complains := 7

						when obs regexp "!!5219=5622!!" then @chief_complains := 8

						else @chief_complains = null

					end as chief_complains,

					case	
						when obs regexp "!!8777=1072!!" then @complain_duration := 1
						when obs regexp "!!8777=1073!!" then @complain_duration := 2
						when obs regexp "!!8777=1074!!" then @complain_duration := 3
						when obs regexp "!!8777=8787!!" then @complain_duration := 4
						else @complain_duration = null
					end as complain_duration,
					case 
						when obs regexp "!!1892=" then @number_of_days := GetValues(obs,1892)
                        else @number_of_days = null
					end as number_of_days,
                    case 
						when obs regexp "!!1893=" then @number_of_weeks := GetValues(obs,1893)
                        else @number_of_weeks = null
					end as number_of_weeks,
                    case 
						when obs regexp "!!1894=" then @number_of_months := GetValues(obs,1894)
                        else @number_of_months = null
					end as number_of_months,
                    case 
						when obs regexp "!!7953=" then @number_of_years := GetValues(obs,7953)
                        else @number_of_years = null
					end as number_of_years,
					case	
						when obs regexp "!!6473=1065!!" then @ever_smoked_cigarettes := 1
						when obs regexp "!!6473=1066!!" then @ever_smoked_cigarettes := 2
						when obs regexp "!!6473=1679!!" then @ever_smoked_cigarettes := 3
						else @ever_smoked_cigarettes = null
					end as ever_smoked_cigarettes,
					case 
						when obs regexp "!!2069=" then @number_of_sticks := GetValues(obs,2069)
                        else @number_of_sticks = null
					end as number_of_sticks,
                    case 
						when obs regexp "!!2070=" then @cigarette_duration := GetValues(obs,2070)
                        else @cigarette_duration = null
					end as cigarette_duration,
					
					case	
						when obs regexp "!!7973=1065!!" then @tobacco_use := 1
						when obs regexp "!!7973=1066!!" then @tobacco_use := 2
						when obs regexp "!!7973=1679!!" then @tobacco_use := 3
						else @tobacco_use = null
					end as tobacco_use,
                    case 
						when obs regexp "!!8144=" then @tobacco_duration := GetValues(obs,8144)
                        else @tobacco_duration = null
					end as tobacco_duration,
					
					case	
						when obs regexp "!!1972=1967!!" then @main_occupation := 1
						when obs regexp "!!1972=1966!!" then @main_occupation := 2
						when obs regexp "!!1972=1971!!" then @main_occupation := 3
						when obs regexp "!!1972=8710!!" then @main_occupation := 4
						when obs regexp "!!1972=10369!!" then @main_occupation := 5
						when obs regexp "!!1972=10368!!" then @main_occupation := 6
						when obs regexp "!!1972=5622!!" then @main_occupation := 7
						else @main_occupation = null
					end as main_occupation,
					case	
						when obs regexp "!!10310=1107!!" then @chemical_exposure := 1
						when obs regexp "!!10310=10308!!" then @chemical_exposure := 2
						when obs regexp "!!10310=10370!!" then @chemical_exposure := 3
						when obs regexp "!!10310=10309!!" then @chemical_exposure := 4
						when obs regexp "!!10310=5622!!" then @chemical_exposure := 5
						else @chemical_exposure = null
					end as chemical_exposure,
					case	
						when obs regexp "!!10312=1065!!" then @asbestos_exposure := 1
						when obs regexp "!!10312=1066!!" then @asbestos_exposure := 2
						else @asbestos_exposure = null
					end as asbestos_exposure,
					
					case	
						when obs regexp "!!9635=1107!!" then @other_cancer_in_family := 1
						when obs regexp "!!9635=978!!" then @other_cancer_in_family := 2
						when obs regexp "!!9635=1692!!" then @other_cancer_in_family := 3
						when obs regexp "!!9635=972!!" then @other_cancer_in_family := 4
						when obs regexp "!!9635=1671!!" then @other_cancer_in_family := 5
						when obs regexp "!!9635=1393!!" then @other_cancer_in_family := 6
						when obs regexp "!!9635=1392!!" then @other_cancer_in_family := 7
						when obs regexp "!!9635=1395!!" then @other_cancer_in_family := 8
						when obs regexp "!!9635=1394!!" then @other_cancer_in_family := 9
						when obs regexp "!!9635=1673!!" then @other_cancer_in_family := 10
						else @other_cancer_in_family = null
					end as other_cancer_in_family,
					case	
						when obs regexp "!!6709=1067!!" then @hiv_status := 1
						when obs regexp "!!6709=664!!" then @hiv_status := 2
						when obs regexp "!!6709=703!!" then @hiv_status := 3
						else @hiv_status = null
					end as hiv_status,
                    case	
						when obs regexp "!!10242=1065!!" then @previous_tb_treatment := 1
						when obs regexp "!!10242=1066!!" then @previous_tb_treatment := 2
						else @previous_tb_treatment = null
					end as previous_tb_treatment,
					case 
						when obs regexp "!!1894=" then @treatment_duration := GetValues(obs,1894)
                        else @treatment_duration = null
					end as treatment_duration,
                    case 
						when obs regexp "!!9304=" then @number_of_tb_treatment := GetValues(obs,9304)
                        else @number_of_tb_treatment = null
					end as number_of_tb_treatment,
					case	
						when obs regexp "!!2028=1107!!" then @previous_test_ordered := 1
						when obs regexp "!!2028=8064!!" then @previous_test_ordered := 2
						when obs regexp "!!2028=9543!!" then @previous_test_ordered := 3
						when obs regexp "!!2028=12!!" then @previous_test_ordered := 4
						when obs regexp "!!2028=5622!!" then @previous_test_ordered := 5
						else @previous_test_ordered = null
					end as previous_test_ordered,

					case	
						when obs regexp "!!307=703!!" then @sputum_aafbs_results := 1
						when obs regexp "!!307=664!!" then @sputum_aafbs_results := 2
						when obs regexp "!!307=1118!!" then @sputum_aafbs_results := 3
						else @sputum_aafbs_results = null
					end as sputum_aafbs_results,	

					case	
						when obs regexp "!!8070=703!!" then @gene_expert_results := 1
						when obs regexp "!!8070=664!!" then @gene_expert_results := 2
						when obs regexp "!!8070=1138!!" then @gene_expert_results := 3
						else @gene_expert_results = null
					end as gene_expert_results,

					case	
						when obs regexp "!!12=1115!!" then @x_ray_results := 1
						when obs regexp "!!12=1116!!" then @x_ray_results := 2
						when obs regexp "!!12=1118!!" then @x_ray_results := 3
						else @x_ray_results = null
					end as x_ray_results,

					case	
						when obs regexp "!!10127=1107!!" then @procedure_ordered := 1
						when obs regexp "!!10127=10075!!" then @procedure_ordered := 2
						when obs regexp "!!10127=10076!!" then @procedure_ordered := 3
						when obs regexp "!!10127=10126!!" then @procedure_ordered := 4
						when obs regexp "!!10127=5622!!" then @procedure_ordered := 5
						else @procedure_ordered = null
					end as procedure_ordered,
					
					case	
						when obs regexp "!!1271=12!!" then @imaging_test_ordered := 1
						when obs regexp "!!1271=7113!!" then @imaging_test_ordered := 2
						when obs regexp "!!1271=5622!!" then @imaging_test_ordered := 3
						else @imaging_test_ordered = null
					end as imaging_test_ordered,
					case 
						when obs regexp "!!8190=" then @radiology_test := GetValues(obs,8190)
                        else @radiology_test = null
					end as radiology_test,
					case	
						when obs regexp "!!9930=1107!!" then @follow_up_care_plan := 1
						when obs regexp "!!9930=9725!!" then @follow_up_care_plan := 2
						when obs regexp "!!9930=5622!!" then @follow_up_care_plan := 3
						else @follow_up_care_plan = null
					end as follow_up_care_plan,
				   case 
						when obs regexp "!!5096=" then @rtc_date := GetValues(obs,5096)
                        else @rtc_date = null
					end as rtc_date,
				   
					case	
						when obs regexp "!!12=1115!!" then @imaging_results := 1
						when obs regexp "!!12=1116!!" then @imaging_results := 2
						when obs regexp "!!12=5622!!" then @imaging_results := 3
						else @imaging_results = null
					end as imaging_results,       
				   
					case	
						when obs regexp "!!7113=1115!!" then @ct_findings := 1
						when obs regexp "!!7113=10318!!" then @ct_findings := 2
						when obs regexp "!!7113=2418!!" then @ct_findings := 3
						when obs regexp "!!7113=1136!!" then @ct_findings := 4
						when obs regexp "!!7113=5622!!" then @ct_findings := 5
						else @ct_findings = null
					end as ct_findings, 
                    case 
						when obs regexp "!!10077=" then @Imaging_results_description := GetValues(obs,10077)
                        else @Imaging_results_description = null
					end as Imaging_results_description,
					case	
						when obs regexp "!!10391=1065!!" then @biospy_procedure_done := 1
						when obs regexp "!!10391=1066!!" then @biospy_procedure_done := 2
						else @biospy_procedure_done = null
					end as biospy_procedure_done, 
                    case 
						when obs regexp "!!10060=" then @biopsy_workup_date := GetValues(obs,10060)
                        else @biopsy_workup_date = null
					end as biopsy_workup_date,
					case	
						when obs regexp "!!10231=10052!!" then @biospy_results := 1
						when obs regexp "!!10231=10212!!" then @biospy_results := 2
                        when obs regexp "!!10231=5622!!" then @biospy_results := 3
						else @biospy_results = null
					end as biospy_results,
                    
                    
                    case 
						when obs regexp "!!1915=" then @other_condition := GetValues(obs,1915)
                        else @other_condition = null
					end as other_condition,
                    case 
						when obs regexp "!!9846=" then @type_of_malignancy := GetValues(obs,9846)
                        else @type_of_malignancy = null
					end as type_of_malignancy,
                    case 
						when obs regexp "!!10390=" then @other_malignancy := GetValues(obs,10390)
                        else @other_malignancy = null
					end as other_malignancy,
					case	
						when obs regexp "!!7176=10129!!" then @lung_cancer_type := 1
						when obs regexp "!!7176=10130!!" then @lung_cancer_type := 2
						when obs regexp "!!7176=5622!!" then @lung_cancer_type := 3
						else @lung_cancer_type = null
					end as lung_cancer_type,
					case	
						when obs regexp "!!10132=7421!!" then @non_small_cancer := 1
						when obs regexp "!!10132=7422!!" then @non_small_cancer := 2
						when obs regexp "!!10132=10131!!" then @non_small_cancer := 3
						when obs regexp "!!10132=10209!!" then @non_small_cancer := 4
						else @non_small_cancer = null
					end as non_small_cancer,

					case	
						when obs regexp "!!10319=1295!!" then @repository_disease := 1
						when obs regexp "!!10319=58!!" then @repository_disease := 2
						when obs regexp "!!10319=10211!!" then @repository_disease := 3
						when obs regexp "!!10319=10210!!" then @repository_disease := 4
						when obs regexp "!!10319=5622!!" then @repository_disease := 5
						else @repository_disease = null
					end as repository_disease,
                    case 
						when obs regexp "!!9728=" then @diagnosis_date := GetValues(obs,9728)
                        else @diagnosis_date = null
					end as diagnosis_date,
					case	
						when obs regexp "!!1272=10299!!" then @referral_ordered := 1
						when obs regexp "!!1272=8161!!" then @referral_ordered := 2
						when obs regexp "!!1272=8724!!" then @referral_ordered := 3
						when obs regexp "!!1272=5486!!" then @referral_ordered := 4
						when obs regexp "!!1272=10200!!" then @referral_ordered := 5
						when obs regexp "!!1272=6570!!" then @referral_ordered := 6
						when obs regexp "!!1272=5622!!" then @referral_ordered := 7
						else @referral_ordered = null
					end as referral_ordered,
                    
                    case 
						when obs regexp "!!5096=" then @return_date := GetValues(obs,5096)
                        else @return_date = null
					end as return_date
		
						from flat_lung_cancer_screening_0 t1
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


						alter table flat_lung_cancer_screening_1 drop prev_id, drop cur_id;

						drop table if exists flat_lung_cancer_screening_2;
						create temporary table flat_lung_cancer_screening_2
						(select *,
							@prev_id := @cur_id as prev_id,
							@cur_id := person_id as cur_id,

							case
								when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
								else @prev_encounter_datetime := null
							end as next_encounter_datetime_lung_cancer_screening,

							@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
								else @next_encounter_type := null
							end as next_encounter_type_lung_cancer_screening,

							@cur_encounter_type := encounter_type as cur_encounter_type,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as next_clinical_datetime_lung_cancer_screening,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as next_clinical_location_id_lung_cancer_screening,

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
							end as next_clinical_rtc_date_lung_cancer_screening,

							case
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinical_rtc_date

							from flat_lung_cancer_screening_1
							order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
						);

						alter table flat_lung_cancer_screening_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;


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

						drop temporary table if exists flat_lung_cancer_screening_3;
						create temporary table flat_lung_cancer_screening_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
						(select
							*,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,

							case
						        when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
						        else @prev_encounter_type:=null
							end as prev_encounter_type_lung_cancer_screening,	
                            @cur_encounter_type := encounter_type as cur_encounter_type,

							case
						        when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
						        else @prev_encounter_datetime := null
						    end as prev_encounter_datetime_lung_cancer_screening,

                            @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as prev_clinical_datetime_lung_cancer_screening,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as prev_clinical_location_id_lung_cancer_screening,

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
							end as prev_clinical_rtc_date_lung_cancer_screening,

							case
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinic_rtc_date

							from flat_lung_cancer_screening_2 t1
							order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
                                        


					select count(*) into @new_encounter_rows from flat_lung_cancer_screening_3;
                    
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
								referral_from,
								chief_complains,
								complain_duration,
								number_of_days,
								number_of_weeks,
								number_of_months,
								number_of_years,
											ever_smoked_cigarettes,
											number_of_sticks,
											cigarette_duration,
											tobacco_use,
											tobacco_duration,
											main_occupation,
											chemical_exposure,
											asbestos_exposure,
											other_cancer_in_family,
											hiv_status,
                                            previous_tb_treatment,
											treatment_duration,
											number_of_tb_treatment,
											previous_test_ordered,
											sputum_aafbs_results,
											gene_expert_results,
											x_ray_results,
											procedure_ordered,
											imaging_test_ordered,
											radiology_test,
											follow_up_care_plan,
											rtc_date,
											imaging_results,
											ct_findings,
                                            Imaging_results_description,
											biospy_procedure_done,
                                            biopsy_workup_date,
											biospy_results,
                                            other_condition,
                                            type_of_malignancy,
											other_malignancy,
											lung_cancer_type,
											non_small_cancer,
											repository_disease,
											diagnosis_date,
											referral_ordered,
											return_date,
								
                                
                                prev_encounter_datetime_lung_cancer_screening,
								next_encounter_datetime_lung_cancer_screening,
								prev_encounter_type_lung_cancer_screening,
								next_encounter_type_lung_cancer_screening,
								prev_clinical_datetime_lung_cancer_screening,
								next_clinical_datetime_lung_cancer_screening,
								prev_clinical_location_id_lung_cancer_screening,
								next_clinical_location_id_lung_cancer_screening,
								prev_clinical_rtc_date_lung_cancer_screening,
								next_clinical_rtc_date_lung_cancer_screening

						from flat_lung_cancer_screening_3 t1
                        join amrs.location t2 using (location_id))');

					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;


					SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_lung_cancer_screening_build_queue__0 t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
					#select @person_ids_count := (select count(*) from flat_lung_cancer_screening_build_queue_2);                        
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

		END