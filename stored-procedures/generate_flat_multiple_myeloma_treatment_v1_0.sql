CREATE PROCEDURE `generate_flat_multiple_myeloma_treatment_v1_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
					set @primary_table := "flat_multiple_myeloma_treatment";
					set @query_type = query_type;
#set @query_type = "build";
                     
                    set @total_rows_written = 0;
                    
                    set @encounter_types = "(89,90)";
                    set @clinical_encounter_types = "(89,90)";
                    set @non_clinical_encounter_types = "(-1)";
                    set @other_encounter_types = "(-1)";
                    
					set @start = now();
					set @table_version = "flat_multiple_myeloma_treatment_v1.0";

					set session sort_buffer_size=512000000;

					set @sep = " ## ";
                    set @boundary = "!!";
					set @last_date_created = (select max(max_date_created) from etl.flat_obs);

					CREATE TABLE IF NOT EXISTS flat_multiple_myeloma_treatment (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    person_id INT,
    encounter_id INT,
    encounter_type INT,
    encounter_datetime DATETIME,
    visit_id INT,
    location_id INT,
    gender CHAR(100),
    age INT,
    encounter_purpose INT,
    ecog_performance_index INT,
    diagnosis_method INT,
    diagnosis INT,
    cancer_stage INT,
    overall_cancer_stage INT,
    lab_test_order INT,
    other_lab_test INT,
    mm_supportive_plan INT,
    mm_signs_symptoms INT,
    chemotherapy_plan INT,
    chemo_start_date DATETIME,
    chemo_cycle INT,
    reason_chemo_stop INT,
    chemotherapy_drug INT,
    dosage_in_milligrams INT,
    drug_route INT,
    other_drugs INT,
    other_medication INT,
    purpose INT,
    education_given_today INT,
    referral INT,
    next_app_date DATETIME,
    prev_encounter_datetime_multiple_myeloma DATETIME,
    next_encounter_datetime_multiple_myeloma DATETIME,
    prev_encounter_type_multiple_myeloma MEDIUMINT,
    next_encounter_type_multiple_myeloma MEDIUMINT,
    prev_clinical_datetime_multiple_myeloma DATETIME,
    next_clinical_datetime_multiple_myeloma DATETIME,
    prev_clinical_location_id_multiple_myeloma MEDIUMINT,
    next_clinical_location_id_multiple_myeloma MEDIUMINT,
    prev_clinical_rtc_date_multiple_myeloma DATETIME,
    next_clinical_rtc_date_multiple_myeloma DATETIME,
    PRIMARY KEY encounter_id (encounter_id),
    INDEX person_date (person_id , encounter_datetime),
    INDEX location_id_rtc_date (location_id , next_app_date),
    INDEX loc_id_enc_date_next_clinical (location_id , encounter_datetime , next_clinical_datetime_multiple_myeloma),
    INDEX encounter_type (encounter_type),
    INDEX date_created (date_created)
);
                        
							
					
                        if(@query_type="build") then
							select 'BUILDING..........................................';
							
#set @write_table = concat("flat_breast_cancer_screening_temp_",1);
#set @queue_table = concat("flat_breast_cancer_screening_build_queue_",1);                    												

                            set @write_table = concat("flat_multiple_myeloma_treatment_temp_",queue_number);
							set @queue_table = concat("flat_multiple_myeloma_treatment_build_queue_",queue_number);                    												
							

#drop table if exists flat_breast_cancer_screening_temp_1;							
							SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

							#create  table if not exists @queue_table (person_id int, primary key (person_id));
							SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_multiple_myeloma_treatment_build_queue limit ', queue_size, ');'); 
#SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_breast_cancer_screening_build_queue limit 500);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  
							
							#delete t1 from flat_breast_cancer_screening_build_queue t1 join @queue_table t2 using (person_id)
							SET @dyn_sql=CONCAT('delete t1 from flat_multiple_myeloma_treatment_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

					end if;
	
					
					if (@query_type="sync") then
							select 'SYNCING..........................................';
							set @write_table = "flat_multiple_myeloma_treatment";
							set @queue_table = "flat_multiple_myeloma_treatment_sync_queue";
CREATE TABLE IF NOT EXISTS flat_multiple_myeloma_treatment_sync_queue (
    person_id INT PRIMARY KEY
);                            
                            
							set @last_update = null;

SELECT 
    MAX(date_updated)
INTO @last_update FROM
    etl.flat_log
WHERE
    table_name = @table_version;

SELECT 'Finding patients in amrs.encounters...';

							replace into flat_multiple_myeloma_treatment_sync_queue
							(select distinct patient_id
								from amrs.encounter
								where date_changed > @last_update
							);
						
                        
SELECT 'Finding patients in flat_obs...';

							replace into flat_multiple_myeloma_treatment_sync_queue
							(select distinct person_id
								from etl.flat_obs
								where max_date_created > @last_update
							);


SELECT 'Finding patients in flat_lab_obs...';
							replace into flat_multiple_myeloma_treatment_sync_queue
							(select distinct person_id
								from etl.flat_lab_obs
								where max_date_created > @last_update
							);

SELECT 'Finding patients in flat_orders...';

							replace into flat_multiple_myeloma_treatment_sync_queue
							(select distinct person_id
								from etl.flat_orders
								where max_date_created > @last_update
							);
                            
                            replace into flat_multiple_myeloma_treatment_sync_queue
                            (select person_id from 
								amrs.person 
								where date_voided > @last_update);


                            replace into flat_multiple_myeloma_treatment_sync_queue
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


					#delete t1 from flat_breast_cancer_screening t1 join @queue_table t2 using (person_id);
					SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
                    set @total_time=0;
                    set @cycle_number = 0;
                    

					while @person_ids_count > 0 do

						set @loop_start_time = now();
                        
						#create temp table with a set of person ids
						drop temporary table if exists flat_multiple_myeloma_treatment_build_queue__0;
						
                        #create temporary table flat_breast_cancer_screening_build_queue__0 (select * from flat_breast_cancer_screening_build_queue_2 limit 5000); #TODO - change this when data_fetch_size changes

#SET @dyn_sql=CONCAT('create temporary table flat_breast_cancer_screening_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit 100);'); 
						SET @dyn_sql=CONCAT('create temporary table flat_multiple_myeloma_treatment_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                    
						drop temporary table if exists flat_multiple_myeloma_treatment_0a;
						SET @dyn_sql = CONCAT(
								'create temporary table flat_multiple_myeloma_treatment_0a
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
									join flat_multiple_myeloma_treatment_build_queue__0 t0 using (person_id)
									left join etl.flat_orders t2 using(encounter_id)
								where t1.encounter_type in ',@encounter_types,');');
                            
                        PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                    
                    

                        
					
						insert into flat_multiple_myeloma_treatment_0a
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
								join flat_multiple_myeloma_treatment_build_queue__0 t0 using (person_id)
						);


						drop temporary table if exists flat_multiple_myeloma_treatment_0;
						create temporary table flat_multiple_myeloma_treatment_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
						(select * from flat_multiple_myeloma_treatment_0a
						order by person_id, date(encounter_datetime), encounter_type_sort_index
						);


						set @encounter_purpose = null;

                        set @ecog_performance_index = null;

						set @diagnosis_method =null;

                        set @diagnosis =null;

                        set @cancer_stage =null;

                        set @overall_cancer_stage =null;

                        set @lab_test_order =null;

                        set @other_lab_test =null;

                        set @mm_supportive_plan =null;
                        
                        set @mm_signs_symptoms = null;

                        set @chemotherapy_plan =null;

                        set @chemo_start_date =null;

                        set @chemo_cycle =null;

                        set @reason_chemo_stop =null;

                        set @chemotherapy_drug =null;

                        set @dosage_in_milligrams =null;

                        set @drug_route =null;

                        set @other_drugs =null;

                        set @other_medication =null;

                        set @purpose =null;

                        set @education_given_today =null;

                        set @referral =null;

                        set @next_app_date = null;
                                                
						drop temporary table if exists flat_multiple_myeloma_treatment_1;
						create temporary table flat_multiple_myeloma_treatment_1 #(index encounter_id (encounter_id))
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
                            #p.death_date,
                            
                            
							case
								when timestampdiff(year,birthdate,curdate()) > 0 then round(timestampdiff(year,birthdate,curdate()),0)
								else round(timestampdiff(month,birthdate,curdate())/12,2)
							end as age,
							
                            case
								when obs regexp "!!1834=7850!!" then @encounter_purpose := 1
								when obs regexp "!!1834=10037!!" then @encounter_purpose := 2
                                when obs regexp "!!1834=2345!!" then @encounter_purpose := 3
                                when obs regexp "!!1834=1068!!" then @encounter_purpose := 4
                                when obs regexp "!!1834=1246!!" then @encounter_purpose := 5
								else @encounter_purpose := null
							end as encounter_purpose,
                            
                           
							case
								when obs regexp "!!6584=1115!!" then @ecog_performance_index := 1
								when obs regexp "!!6584=6585!!" then @ecog_performance_index := 2
                                when obs regexp "!!6584=6586!!" then @ecog_performance_index := 3
                                when obs regexp "!!6584=6587!!" then @ecog_performance_index := 4
                                when obs regexp "!!6584=6588!!" then @ecog_performance_index := 5
								else @ecog_performance_index := null
							end as ecog_performance_index,
                            
                            
                             case
								when obs regexp "!!6504=8594!!" then @diagnosis_method := 1
								when obs regexp "!!6504=6902!!" then @diagnosis_method := 2
								when obs regexp "!!6504=10142!!" then @diagnosis_method := 3
								else @diagnosis_method := null
							end as diagnosis_method,
                            
                            
                            case
								when obs regexp "!!6042=" then @diagnosis := GetValues(obs,6042) 
								else @diagnosis := null
							end as diagnosis,
                            
                            
							case
								when obs regexp "!!6582=1067!!" then @cancer_stage := 1
								when obs regexp "!!6582=10206!!" then @cancer_stage := 2
                                when obs regexp "!!6582=6566!!" then @cancer_stage := 3
                                when obs regexp "!!6582=1175!!" then @cancer_stage := 4
								else @cancer_stage := null
							end as cancer_stage,
                            
                            
                            case
								when obs regexp "!!9868=9852!!" then @overall_cancer_stage := 1
								when obs regexp "!!9868=9856!!" then @overall_cancer_stage := 2
                                when obs regexp "!!9868=9860!!" then @overall_cancer_stage := 3
                               	else @overall_cancer_stage := null
							end as overall_cancer_stage,
                            
                            
                            case
								when obs regexp "!!6583=1107!!" then @lab_test_order := 1
								when obs regexp "!!6583=790!!" then @lab_test_order := 2
                                when obs regexp "!!6583=1019!!" then @lab_test_order := 3
                                when obs regexp "!!6583=953!!" then @lab_test_order := 4
                                when obs regexp "!!6583=10205!!" then @lab_test_order := 5
                                when obs regexp "!!6583=8596!!" then @lab_test_order := 6
                                when obs regexp "!!6583=8595!!" then @lab_test_order := 7
                                when obs regexp "!!6583=5622!!" then @lab_test_order := 8
                               	else @lab_test_order := null
							end as lab_test_order,
                            
                            
                            case
								when obs regexp "!!9538=" then @other_lab_test := GetValues(obs,9538) 
								else @other_lab_test := null
							end as other_lab_test,
                            
                            
                            case
								when obs regexp "!!10198=88!!" then @mm_supportive_plan := 1
								when obs regexp "!!10198=257!!" then @mm_supportive_plan := 2
                                when obs regexp "!!10198=8598!!" then @mm_supportive_plan := 3
                                when obs regexp "!!10198=8597!!" then @mm_supportive_plan := 4
                                when obs regexp "!!10198=1195!!" then @mm_supportive_plan := 5
                                when obs regexp "!!10198=8410!!" then @mm_supportive_plan := 6
                                when obs regexp "!!10198=7458!!" then @mm_supportive_plan := 7
                                when obs regexp "!!10198=8479!!" then @mm_supportive_plan := 8
                                when obs regexp "!!10198=10140!!" then @mm_supportive_plan := 9
                                when obs regexp "!!10198=7207!!" then @mm_supportive_plan := 10
                                when obs regexp "!!10198=5622!!" then @mm_supportive_plan := 11
                               	else @mm_supportive_plan := null
							end as mm_supportive_plan,
                            
                            case
								when obs regexp "!!10170=10141!!" then @mm_signs_symptoms := 1
								when obs regexp "!!10170=1885!!" then @mm_signs_symptoms := 2
                                when obs regexp "!!10170=3!!" then @mm_signs_symptoms := 3
                                when obs regexp "!!10170=8592!!" then @mm_signs_symptoms := 4
                                when obs regexp "!!10170=5978!!" then @mm_signs_symptoms := 5
                                when obs regexp "!!10170=5949!!" then @mm_signs_symptoms := 6
                                when obs regexp "!!10170=5622!!" then @mm_signs_symptoms := 7
								else @mm_signs_symptoms := null
							end as mm_signs_symptoms,
                            
                            
                            case
								when obs regexp "!!9869=1256!!" then @chemotherapy_plan := 1
								when obs regexp "!!9869=1259!!" then @chemotherapy_plan := 2
                                when obs regexp "!!9869=1260!!" then @chemotherapy_plan := 3
                                when obs regexp "!!9869=1257!!" then @chemotherapy_plan := 4
                                when obs regexp "!!9869=6576!!" then @chemotherapy_plan := 5
                               	else @chemotherapy_plan := null
							end as chemotherapy_plan,
                            
                            
                            case
								when obs regexp "!!1190=" then @chemo_start_date := GetValues(obs,1190) 
								else @chemo_start_date := null
							end as chemo_start_date,
                            
                            
							case
								when obs regexp "!!6643=[0-9]"  then @chemo_cycle:=cast(GetValues(obs,6643) as unsigned)
								else @chemo_cycle := null
							end as chemo_cycle,
                            
                            
                            case
								when obs regexp "!!9927=1267!!" then @reason_chemo_stop := 1
								when obs regexp "!!9927=7391!!" then @reason_chemo_stop := 2
                                when obs regexp "!!9927=6629!!" then @reason_chemo_stop := 3
                                when obs regexp "!!9927=6627!!" then @reason_chemo_stop := 4
                                when obs regexp "!!9927=1879!!" then @reason_chemo_stop := 5
                                when obs regexp "!!9927=5622!!" then @reason_chemo_stop := 6
                               	else @reason_chemo_stop := null
							end as reason_chemo_stop,
                                                        
                            
                            case
								when obs regexp "!!9918=491!!" then @chemotherapy_drug := 1
								when obs regexp "!!9918=7207!!" then @chemotherapy_drug := 2
                                when obs regexp "!!9918=8486!!" then @chemotherapy_drug := 3
                                when obs regexp "!!9918=7203!!" then @chemotherapy_drug := 4
                                when obs regexp "!!9918=8479!!" then @chemotherapy_drug := 5
                                when obs regexp "!!9918=7213!!" then @chemotherapy_drug := 6
                                when obs regexp "!!9918=8480!!" then @chemotherapy_drug := 7
                                when obs regexp "!!9918=5622!!" then @chemotherapy_drug := 8
                               	else @chemotherapy_drug := null
							end as chemotherapy_drug,
                            
                            
                            case
								when obs regexp "!!1899=[0-9]"  then @dosage_in_milligrams:=cast(GetValues(obs,1899) as unsigned) 
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
								when obs regexp "!!6327=8728!!" then @education_given_today := 1
								when obs regexp "!!6327=8742!!" then @education_given_today := 2
                                when obs regexp "!!6327=1905!!" then @education_given_today := 3
                                when obs regexp "!!6327=10208!!" then @education_given_today := 4
                                when obs regexp "!!6327=8730!!" then @education_given_today := 5
                                when obs regexp "!!6327=8371!!" then @education_given_today := 6
                                when obs regexp "!!6327=5622!!" then @education_given_today := 7
                                else @education_given_today := null
							end as education_given_today,
                            
                            
                            case
								when obs regexp "!!1272=1107!!" then @referral := 1
								when obs regexp "!!1272=8724!!" then @referral := 2
                                when obs regexp "!!1272=6571!!" then @referral := 3
                                when obs regexp "!!1272=1286!!" then @referral := 4
                                when obs regexp "!!1272=6572!!" then @referral := 5
                                when obs regexp "!!1272=6573!!" then @referral := 6
                                when obs regexp "!!1272=1905!!" then @referral := 7
                                when obs regexp "!!1272=5622!!" then @referral := 8
                                else @referral := null
							end as referral,
                            
                            
                            case
								when obs regexp "!!5096=" then @next_app_date := GetValues(obs,5096) 
								else @next_app_date := null
							end as next_app_date
		
						from flat_multiple_myeloma_treatment_0 t1
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


						alter table flat_multiple_myeloma_treatment_1 drop prev_id, drop cur_id;

						drop table if exists flat_multiple_myeloma_treatment_2;
						create temporary table flat_multiple_myeloma_treatment_2
						(select *,
							@prev_id := @cur_id as prev_id,
							@cur_id := person_id as cur_id,

							case
								when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
								else @prev_encounter_datetime := null
							end as next_encounter_datetime_multiple_myeloma,

							@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
								else @next_encounter_type := null
							end as next_encounter_type_multiple_myeloma,

							@cur_encounter_type := encounter_type as cur_encounter_type,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as next_clinical_datetime_multiple_myeloma,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as next_clinical_location_id_multiple_myeloma,

							

						    case
								when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
								else @prev_clinical_rtc_date := null
							end as next_clinical_rtc_date_multiple_myeloma,
                            
                            case
								when is_clinical_encounter then @cur_clinical_rtc_date := next_app_date
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinical_rtc_date


							from flat_multiple_myeloma_treatment_1
							order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
						);

						alter table flat_multiple_myeloma_treatment_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;


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

						drop temporary table if exists flat_multiple_myeloma_treatment_3;
						create temporary table flat_multiple_myeloma_treatment_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
						(select
							*,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,

							case
						        when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
						        else @prev_encounter_type:=null
							end as prev_encounter_type_multiple_myeloma,	
                            @cur_encounter_type := encounter_type as cur_encounter_type,

							case
						        when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
						        else @prev_encounter_datetime := null
						    end as prev_encounter_datetime_multiple_myeloma,

                            @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as prev_clinical_datetime_multiple_myeloma,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as prev_clinical_location_id_multiple_myeloma,

							

							case
								when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
								else @prev_clinical_rtc_date := null
							end as prev_clinical_rtc_date_multiple_myeloma,
                            
                            case
								when is_clinical_encounter then @cur_clinical_rtc_date := next_app_date
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinic_rtc_date

							from flat_multiple_myeloma_treatment_2 t1
							order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
                                        


					SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    flat_multiple_myeloma_treatment_3;
                    
SELECT @new_encounter_rows;                    
					set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

					SET @dyn_sql=CONCAT('replace into ',@write_table,											  
						'(select
								null,
								 person_id,

							encounter_id,

							encounter_type,

							encounter_datetime,

							visit_id,

							location_id,

							gender,

							age,

							encounter_purpose,

							ecog_performance_index,

                            diagnosis_method,

                            diagnosis,

                            cancer_stage,

                            overall_cancer_stage,

                            lab_test_order,

                            other_lab_test,

                            mm_supportive_plan,
                            
                            mm_signs_symptoms,

                            chemotherapy_plan,

                            chemo_start_date,

                            chemo_cycle,

                            reason_chemo_stop,

                            chemotherapy_drug,

                            dosage_in_milligrams,

                            drug_route,

                            other_drugs,

                            other_medication,

                            purpose,

                            education_given_today,

                            referral,

                            next_app_date,

							

							

							prev_encounter_datetime_multiple_myeloma,

							next_encounter_datetime_multiple_myeloma,

							prev_encounter_type_multiple_myeloma,

							next_encounter_type_multiple_myeloma,

							prev_clinical_datetime_multiple_myeloma,

							next_clinical_datetime_multiple_myeloma,

							prev_clinical_location_id_multiple_myeloma,

							next_clinical_location_id_multiple_myeloma,

							prev_clinical_rtc_date_multiple_myeloma,

							next_clinical_rtc_date_multiple_myeloma

						from flat_multiple_myeloma_treatment_3 t1
                        join amrs.location t2 using (location_id))');

					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;


					SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_multiple_myeloma_treatment_build_queue__0 t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
					#select @person_ids_count := (select count(*) from flat_breast_cancer_screening_build_queue_2);                        
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
SELECT 
    @person_ids_count AS 'persons remaining',
    @cycle_length AS 'Cycle time (s)',
    CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
    @remaining_time AS 'Est time remaining (min)';

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
						SELECT 
    CONCAT(@start_write,
            ' : Writing ',
            @total_rows_to_write,
            ' to ',
            @primary_table);

						SET @dyn_sql=CONCAT('replace into ', @primary_table,
							'(select * from ',@write_table,');');
                        PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;
						
                        set @finish_write = now();
                        set @time_to_write = timestampdiff(second,@start_write,@finish_write);
SELECT 
    CONCAT(@finish_write,
            ' : Completed writing rows. Time to write to primary table: ',
            @time_to_write,
            ' seconds ');                        
                        
                        SET @dyn_sql=CONCAT('drop table ',@write_table,';'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                        
                        
				end if;
                
									
				set @ave_cycle_length = ceil(@total_time/@cycle_number);
SELECT 
    CONCAT('Average Cycle Length: ',
            @ave_cycle_length,
            ' second(s)');
                
				 set @end = now();
				 insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
				SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');

		END