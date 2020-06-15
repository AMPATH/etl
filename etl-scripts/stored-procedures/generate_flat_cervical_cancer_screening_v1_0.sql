DELIMITER $$
CREATE  PROCEDURE `generate_flat_cervical_cancer_screening_v1_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
					set @primary_table := "flat_cervical_cancer_screening";
					set @query_type = query_type;
#set @query_type = "build";
                     
                    set @total_rows_written = 0;
                    
                    set @encounter_types = "(69,70,147)";
                    set @clinical_encounter_types = "(69,70,147)";
                    set @non_clinical_encounter_types = "(-1)";
                    set @other_encounter_types = "(-1)";
                    
					set @start = now();
					set @table_version = "flat_cervical_cancer_screening_v1.0";

					set session sort_buffer_size=512000000;

					set @sep = " ## ";
                    set @boundary = "!!";
					set @last_date_created = (select max(max_date_created) from etl.flat_obs);

CREATE TABLE IF NOT EXISTS flat_cervical_cancer_screening (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    person_id INT,
    encounter_id INT,
    encounter_type INT,
    encounter_datetime DATETIME,
    visit_id INT,
    location_id INT,
    location_uuid VARCHAR(100),
    uuid VARCHAR(100),
    age INT,
    encounter_purpose INT,
    cur_visit_type INT,
    actual_scheduled_date DATETIME,
    gravida INT,
    parity INT,
    Mensturation_status INT,
    lmp DATETIME,
    pregnancy_status INT,
    pregnant_edd DATETIME,
    reason_not_pregnant INT,
    hiv_status INT,
    viral_load FLOAT,
    viral_load_date DATETIME,
    prior_via_done INT,
    prior_via_result INT,
    prior_via_date DATETIME,
    cur_via_result INT,
    visual_impression_cervix INT,
    visual_impression_vagina INT,
    visual_impression_vulva INT,
    via_procedure_done INT,
    other_via_procedure_done VARCHAR(1000),
    via_management_plan INT,
    other_via_management_plan VARCHAR(1000),
    via_assessment_notes TEXT,
    via_rtc_date DATETIME,
    prior_dysplasia_done INT,
    previous_via_result INT,
    previous_via_result_date DATETIME,
    prior_papsmear_result INT,
    prior_biopsy_result INT,
    prior_biopsy_result_date DATETIME,
    prior_biopsy_result_other VARCHAR(1000),
    prior_biopsy_result_date_other DATETIME,
    past_dysplasia_treatment INT,
    treatment_specimen_pathology INT,
    satisfactory_colposcopy INT,
    colposcopy_findings INT,
    cervica_lesion_size INT,
    dysplasia_cervix_impression INT,
    dysplasia_vagina_impression INT,
    dysplasia_vulva_impression INT,
    dysplasia_procedure_done INT,
    other_dysplasia_procedure_done VARCHAR(1000),
    dysplasia_mgmt_plan INT,
    other_dysplasia_mgmt_plan VARCHAR(1000),
    dysplasia_assesment_notes TEXT,
    dysplasia_rtc_date DATETIME,
    Pap_smear_results INT,
    leep_location INT,
    Cervix_biopsy_results INT,
    Vagina_biopsy_results INT,
    Vulva_biopsy_result INT,
    endometrium_biopsy INT,
    ECC INT,
    biopsy_results_mngmt_plan INT,
    cancer_staging INT,
    next_app_date DATETIME,
    prev_encounter_datetime_cervical_cancer_screening DATETIME,
    next_encounter_datetime_cervical_cancer_screening DATETIME,
    prev_encounter_type_cervical_cancer_screening MEDIUMINT,
    next_encounter_type_cervical_cancer_screening MEDIUMINT,
    prev_clinical_datetime_cervical_cancer_screening DATETIME,
    next_clinical_datetime_cervical_cancer_screening DATETIME,
    prev_clinical_location_id_cervical_cancer_screening MEDIUMINT,
    next_clinical_location_id_cervical_cancer_screening MEDIUMINT,
    prev_clinical_rtc_date_cervical_cancer_screening DATETIME,
    next_clinical_rtc_date_cervical_cancer_screening DATETIME,
    PRIMARY KEY encounter_id (encounter_id),
    INDEX person_date (person_id , encounter_datetime),
    INDEX location_enc_date (location_uuid , encounter_datetime),
    INDEX enc_date_location (encounter_datetime , location_uuid),
    INDEX location_id_rtc_date (location_id , next_app_date),
    INDEX location_uuid_rtc_date (location_uuid , next_app_date),
    INDEX loc_id_enc_date_next_clinical (location_id , encounter_datetime , next_clinical_datetime_cervical_cancer_screening),
    INDEX encounter_type (encounter_type),
    INDEX date_created (date_created)
);
                        
							
					
                        if(@query_type="build") then
							select 'BUILDING..........................................';
							
#set @write_table = concat("flat_cervical_cancer_screening_temp_",1);
#set @queue_table = concat("flat_cervical_cancer_screening_build_queue_",1);                    												

                            set @write_table = concat("flat_cervical_cancer_screening_temp_",queue_number);
							set @queue_table = concat("flat_cervical_cancer_screening_build_queue_",queue_number);                    												
							

#drop table if exists flat_cervical_cancer_screening_temp_1;							
							SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

							#create  table if not exists @queue_table (person_id int, primary key (person_id));
							SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_cervical_cancer_screening_build_queue limit ', queue_size, ');'); 
#SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_cervical_cancer_screening_build_queue limit 500);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  
							
							#delete t1 from flat_cervical_cancer_screening_build_queue t1 join @queue_table t2 using (person_id)
							SET @dyn_sql=CONCAT('delete t1 from flat_cervical_cancer_screening_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

					end if;
	
					
					if (@query_type="sync") then
							select 'SYNCING..........................................';
							set @write_table = "flat_cervical_cancer_screening";
							set @queue_table = "flat_cervical_cancer_screening_sync_queue";
CREATE TABLE IF NOT EXISTS flat_cervical_cancer_screening_sync_queue (
    person_id INT PRIMARY KEY
);                            
                            
							set @last_update = null;

SELECT 
    MAX(date_updated)
INTO @last_update FROM
    etl.flat_log
WHERE
    table_name = @table_version;

#select max(date_created) into @last_update from etl.flat_log where table_name like "%cervical_cancer_screening%";

#select @last_update;														

							replace into flat_cervical_cancer_screening_sync_queue
							(select distinct patient_id
								from amrs.encounter
								where date_changed > @last_update
							);

							replace into flat_cervical_cancer_screening_sync_queue
							(select distinct person_id
								from etl.flat_obs
								where max_date_created > @last_update
							);

							replace into flat_cervical_cancer_screening_sync_queue
							(select distinct person_id
								from etl.flat_lab_obs
								where max_date_created > @last_update
							);

							replace into flat_cervical_cancer_screening_sync_queue
							(select distinct person_id
								from etl.flat_orders
								where max_date_created > @last_update
							);
                            
                            replace into flat_cervical_cancer_screening_sync_queue
                            (select person_id from 
								amrs.person 
								where date_voided > @last_update);


                            replace into flat_cervical_cancer_screening_sync_queue
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


					#delete t1 from flat_cervical_cancer_screening t1 join @queue_table t2 using (person_id);
					SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
                    set @total_time=0;
                    set @cycle_number = 0;
                    

					while @person_ids_count > 0 do

						set @loop_start_time = now();
                        
						#create temp table with a set of person ids
						drop temporary table if exists flat_cervical_cancer_screening_build_queue__0;
						
                        #create temporary table flat_cervical_cancer_screening_build_queue__0 (select * from flat_cervical_cancer_screening_build_queue_2 limit 5000); #TODO - change this when data_fetch_size changes

#SET @dyn_sql=CONCAT('create temporary table flat_cervical_cancer_screening_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit 100);'); 
						SET @dyn_sql=CONCAT('create temporary table flat_cervical_cancer_screening_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                    
						drop temporary table if exists flat_cervical_cancer_screening_0a;
						SET @dyn_sql = CONCAT(
								'create temporary table flat_cervical_cancer_screening_0a
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
									join flat_cervical_cancer_screening_build_queue__0 t0 using (person_id)
									left join etl.flat_orders t2 using(encounter_id)
								where t1.encounter_type in ',@encounter_types,');');
                            
                        PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                    
                    

                        
					
						insert into flat_cervical_cancer_screening_0a
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
								join flat_cervical_cancer_screening_build_queue__0 t0 using (person_id)
						);


						drop temporary table if exists flat_cervical_cancer_screening_0;
						create temporary table flat_cervical_cancer_screening_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
						(select * from flat_cervical_cancer_screening_0a
						order by person_id, date(encounter_datetime), encounter_type_sort_index
						);


						set @prev_id = null;
                        set @cur_id = null;
                        set @encounter_purpose =null;
                        set @cur_visit_type = null;
                        set @actual_scheduled_date = null;
						set @gravida = null;
                        set @parity = null;
						set @Mensturation_status = null;
                        set @lmp = null;
                        set @pregnancy_status = null;
						set @pregnant_edd = null;
                        set @preason_not_pregnant = null;
                        set @hiv_status = null;
                        set @viral_load = null;
                        set @viral_load_date = null;
						set @prior_via_done = null;
                        set @prior_via_result = null;
						set @prior_via_date = null;
                        set @cur_via_result = null;
                        set @visual_impression_cervix = null;
						set @visual_impression_vagina = null;
                        set @visual_impression_vulva = null;
                        set @via_procedure_done = null;
                        set @other_via_procedure_done = null;
						set @via_management_plan = null;
                        set @other_via_management_plan = null;
                        set @via_assessment_notes = null;
						set @via_rtc_date = null;
                        set @prior_dysplasia_done = null;
                        set @previous_via_result = null;
                        set @previous_via_result_date = null;
                        set @prior_papsmear_result = null;
						set @prior_biopsy_result = null;
                        set @prior_biopsy_result_date = null;
						set @prior_biopsy_result_other = null;
                        set @prior_biopsy_result_date_other = null;
                        set @past_dysplasia_treatment = null;
						set @treatment_specimen_pathology = null;
                        set @satisfactory_colposcopy = null;
                        set @colposcopy_findings = null;
						set @cervica_lesion_size = null;
                        set @dysplasia_cervix_impression = null;
						set @dysplasia_vagina_impression = null;
                        set @dysplasia_vulva_impression = null;
                        set @dysplasia_procedure_done = null;
						set @other_dysplasia_procedure_done = null;
                        set @dysplasia_mgmt_plan = null;
                        set @other_dysplasia_mgmt_plan = null;
						set @dysplasia_assesment_notes = null;
                        set @dysplasia_rtc_date = null;
                        set @Pap_smear_results = null;
                        set @leep_location =null;
						set @Cervix_biopsy_results = null;
						set @Vagina_biopsy_results =null;
						set @Vulva_biopsy_result =null;
                        set @endometrium_biopsy =null;
						set @ECC =null;
						set @biopsy_results_mngmt_plan =null;
                        set @cancer_staging =null;
                                                
						drop temporary table if exists flat_cervical_cancer_screening_1;
						create temporary table flat_cervical_cancer_screening_1 #(index encounter_id (encounter_id))
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
								when obs regexp "!!1834=9651!!" then @encounter_purpose := 1
								when obs regexp "!!1834=1154!!" then @encounter_purpose := 2
                                when obs regexp "!!1834=1246!!" then @encounter_purpose := 3
                                when obs regexp "!!1834=5622!!" then @encounter_purpose := 4
								else @encounter_purpose := null
							end as encounter_purpose,
                             case
								when obs regexp "!!1839=1838!!" then @cur_visit_type := 1
								when obs regexp "!!1839=1837!!" then @cur_visit_type := 2
                                when obs regexp "!!1839=1246!!" then @cur_visit_type := 3
                                when obs regexp "!!1839=7850!!" then @cur_visit_type := 4
                                when obs regexp "!!1839=9569!!" then @cur_visit_type := 5
								else @cur_visit_type := null
							end as cur_visit_type,
                            case
								when obs regexp "!!7029=" then @actual_scheduled_date := replace(replace((substring_index(substring(obs,locate("!!7029=",obs)),@sep,1)),"!!7029=",""),"!!","")
								else @actual_scheduled_date := null
							end as actual_scheduled_date,
                            case
								when obs regexp "!!5624=[0-9]"  then @gravida:=cast(replace(replace((substring_index(substring(obs,locate("!!5624=",obs)),@sep,1)),"!!5624=",""),"!!","") as unsigned)
								else @gravida := null
							end as gravida,
                            case
								when obs regexp "!!1053=[0-9]"  then @parity:=cast(replace(replace((substring_index(substring(obs,locate("!!1053=",obs)),@sep,1)),"!!1053=",""),"!!","") as unsigned)
								else @parity := null
							end as parity,
                            case
								when obs regexp "!!2061=5989!!" then @Mensturation_status := 1
                                when obs regexp "!!2061=6496!!" then @prev_exam_results := 2
								else @Mensturation_status := null
							end as Mensturation_status,
                             case
								when obs regexp "!!1836=" then @lmp := replace(replace((substring_index(substring(obs,locate("!!1836=",obs)),@sep,1)),"!!1836=",""),"!!","")
								else @lmp := null
							end as lmp,
                            case
								when obs regexp "!!8351=1065!!" then @pregnancy_status := 1
								when obs regexp "!!8351=1066!!" then @pregnancy_status := 0
								else @pregnancy_status := null
							end as pregnancy_status, 
                             case
								when obs regexp "!!5596=" then @pregnant_edd := replace(replace((substring_index(substring(obs,locate("!!5596=",obs)),@sep,1)),"!!5596=",""),"!!","")
								else @pregnant_edd := null
							end as pregnant_edd,
                            case
								when obs regexp "!!9733=9729!!" then @reason_not_pregnant := 1
								when obs regexp "!!9733=9730!!" then @reason_not_pregnant := 2
                                when obs regexp "!!9733=9731!!" then @reason_not_pregnant := 3
                                when obs regexp "!!9733=9732!!" then @reason_not_pregnant := 4
								else @reason_not_pregnant := null
							end as reason_not_pregnant,
                             case
								when obs regexp "!!6709=664!!" then @hiv_status := 1
								when obs regexp "!!6709=703!!" then @hiv_status := 2
								when obs regexp "!!6709=1067!!" then @hiv_status := 3
								else @hiv_status := null
							end as hiv_status,
                             case
								when obs regexp "!!856=[0-9]"  then @viral_load:=cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") as unsigned)
								else @viral_load := null
							end as viral_load,
                             case
								when obs regexp "!!000=" then @viral_load_date := replace(replace((substring_index(substring(obs,locate("!!000=",obs)),@sep,1)),"!!000=",""),"!!","")
								else @viral_load_date := null
							end as viral_load_date,
                             case
								when obs regexp "!!9589=1065!!" then @prior_via_done := 1
								when obs regexp "!!9589=1066!!" then @prior_via_done := 0
								else @prior_via_done := null
							end as prior_via_done,
                             case
								when obs regexp "!!7381=664!!" then @prior_via_result := 1
								when obs regexp "!!7381=703!!" then @prior_via_result := 2
								else @prior_via_result := null
							end as prior_via_result,
                           case
								when obs regexp "!!000=" then @prior_via_date := replace(replace((substring_index(substring(obs,locate("!!000=",obs)),@sep,1)),"!!000=",""),"!!","")
								else @prior_via_date := null
							end as prior_via_date,
                             case
								when obs regexp "!!9590=1115!!" then @cur_via_result := 1
								when obs regexp "!!9590=7469!!" then @cur_via_result := 2
                                when obs regexp "!!9590=9593!!" then @cur_via_result := 3
                                when obs regexp "!!9590=7472!!" then @cur_via_result := 4
                                when obs regexp "!!9590=7293!!" then @cur_via_result := 5
                                when obs regexp "!!9590=7470!!" then @cur_via_result := 6
                                when obs regexp "!!9590=6497!!" then @cur_via_result := 7
                                when obs regexp "!!9590=5245!!" then @cur_via_result := 8
                                when obs regexp "!!9590=9591!!" then @cur_via_result := 9
                                when obs regexp "!!9590=9592!!" then @cur_via_result := 10
                                when obs regexp "!!9590=6497!!" then @cur_via_result := 11
								else @cur_via_resul := null
							end as cur_via_result,
                            case
								when obs regexp "!!7484=1115!!" then @visual_impression_cervix := 1
								when obs regexp "!!7484=7507!!" then @visual_impression_cervix := 2
                                when obs regexp "!!7484=7508!!" then @visual_impression_cervix := 3
								else @visual_impression_cervix := null
							end as visual_impression_cervix,
                            case
								when obs regexp "!!7490=1115!!" then @visual_impression_vagina := 1
								when obs regexp "!!7490=1447!!" then @visual_impression_vagina := 2
                                 when obs regexp "!!7490=9181!!" then @visual_impression_vagina := 3
								else @visual_impression_vagina := null
							end as visual_impression_vagina,
                             case
								when obs regexp "!!7490=1115!!" then @visual_impression_vulva := 1
								when obs regexp "!!7490=1447!!" then @visual_impression_vulva := 2
                                when obs regexp "!!7490=9177!!" then @visual_impression_vulva := 3
								else @visual_impression_vulva := null
							end as visual_impression_vulva,
                            case
								when obs regexp "!!7479=1107!!" then @via_procedure_done := 1
								when obs regexp "!!7479=6511!!" then @via_procedure_done := 2
                                when obs regexp "!!7479=7466!!" then @via_procedure_done := 3
                                when obs regexp "!!7479=7147!!" then @via_procedure_done := 4
                                when obs regexp "!!7479=9724!!" then @via_procedure_done := 5
                                 when obs regexp "!!7479=6510!!" then @via_procedure_done := 6
                                when obs regexp "!!7479=5622!!" then @via_procedure_done := 7
								else @via_procedure_done := null
							end as via_procedure_done,
                             case
								when obs regexp "!!1915=" then @other_via_procedure_done := replace(replace((substring_index(substring(obs,locate("!!1915=",obs)),@sep,1)),"!!1915=",""),"!!","")
								else @other_via_procedure_done := null
							end as other_via_procedure_done,
                            case
								when obs regexp "!!7500=9725!!" then @via_management_plan := 1
								when obs regexp "!!7500=9178!!" then @via_management_plan := 2
                                when obs regexp "!!7500=7497!!" then @via_management_plan := 3
                                when obs regexp "!!7500=7383!!" then @via_management_plan := 4
                                when obs regexp "!!7500=7499!!" then @via_management_plan := 5
								when obs regexp "!!7500=5622!!" then @via_management_plan := 6
								else @via_management_plan := null
							end as via_management_plan, 
                            case
								when obs regexp "!!1915=" then @other_via_management_plan := replace(replace((substring_index(substring(obs,locate("!!1915=",obs)),@sep,1)),"!!1915=",""),"!!","")
								else @other_via_management_plan := null
							end as other_via_management_plan,
                            case
								when obs regexp "!!7222=" then @via_assessment_notes := replace(replace((substring_index(substring(obs,locate("!!7222=",obs)),@sep,1)),"!!7222=",""),"!!","")
								else @via_assessment_notes := null
							end as via_assessment_notes,
                           case
								when obs regexp "!!5096=" then @via_rtc_date := replace(replace((substring_index(substring(obs,locate("!!5096=",obs)),@sep,1)),"!!5096=",""),"!!","")
								else @via_rtc_date := null
							end as via_rtc_date, 
                             case
								when obs regexp "!!7379=1065!!" then @prior_dysplasia_done := 1
								when obs regexp "!!7379=1066!!" then @prior_dysplasia_done := 0
								else @prior_dysplasia_done := null
							end as prior_dysplasia_done,
                            case
								when obs regexp "!!7381=664!!" then @previous_via_result := 1
								when obs regexp "!!7381=703!!" then @previous_via_result := 2
								else @previous_via_result := null
							end as previous_via_result,
                            case
								when obs regexp "!!000=" then @previous_via_result_date := replace(replace((substring_index(substring(obs,locate("!!000=",obs)),@sep,1)),"!!000=",""),"!!","")
								else @previous_via_result_date := null
							end as previous_via_result_date,
                            case
								when obs regexp "!!7423=1115!!" then @prior_papsmear_result := 1
								when obs regexp "!!7423=7417!!" then @prior_papsmear_result := 2
                                when obs regexp "!!7423=7418!!" then @prior_papsmear_result := 3
                                when obs regexp "!!7423=7419!!" then @prior_papsmear_result := 4
                                when obs regexp "!!7423=7420!!" then @prior_papsmear_result := 5
								when obs regexp "!!7423=7421!!" then @prior_papsmear_result := 6
                                when obs regexp "!!7423=7422!!" then @prior_papsmear_result := 7
								else @prior_papsmear_result := null
							end as prior_papsmear_result, 
                            case
								when obs regexp "!!7426=1115!!" then @prior_biopsy_result := 1
								when obs regexp "!!7426=7424!!" then @prior_biopsy_result := 2
                                when obs regexp "!!7426=7425!!" then @prior_biopsy_result := 3
                                when obs regexp "!!7426=7216!!" then @prior_biopsy_result := 4
                                when obs regexp "!!7426=7421!!" then @prior_biopsy_result := 5
								else @prior_biopsy_result := null
							end as prior_biopsy_result, 
                            case
								when obs regexp "!!000=" then @prior_biopsy_result_date := replace(replace((substring_index(substring(obs,locate("!!000=",obs)),@sep,1)),"!!000=",""),"!!","")
								else @prior_biopsy_result_date := null
							end as prior_biopsy_result_date,
							case
								when obs regexp "!!7400=" then @prior_biopsy_result_other := replace(replace((substring_index(substring(obs,locate("!!7400=",obs)),@sep,1)),"!!7400=",""),"!!","")
								else @prior_biopsy_result_other := null
							end as prior_biopsy_result_other,
                            case
								when obs regexp "!!000=" then @prior_biopsy_result_date_other := replace(replace((substring_index(substring(obs,locate("!!000=",obs)),@sep,1)),"!!000=",""),"!!","")
								else @prior_biopsy_result_date_other := null
							end as prior_biopsy_result_date_other,
                             case
								when obs regexp "!!7467=7466!!" then @past_dysplasia_treatment := 1
								when obs regexp "!!7467=7147!!" then @past_dysplasia_treatment := 2
                                when obs regexp "!!7467=7465!!" then @past_dysplasia_treatment := 3
                                when obs regexp "!!7467=5622!!" then @past_dysplasia_treatment := 4
								else @past_dysplasia_treatment := null
							end as past_dysplasia_treatment, 
                            case
								when obs regexp "!!7579=1115!!" then @treatment_specimen_pathology := 1
								when obs regexp "!!7579=149!!" then @treatment_specimen_pathology := 2
                                when obs regexp "!!7579=9620!!" then @treatment_specimen_pathology := 3
                                when obs regexp "!!7579=7424!!" then @treatment_specimen_pathology := 4
                                when obs regexp "!!7579=7425!!" then @treatment_specimen_pathology := 5
                                when obs regexp "!!7579=7216!!" then @treatment_specimen_pathology := 6
                                when obs regexp "!!7579=7421!!" then @treatment_specimen_pathology := 7
                                when obs regexp "!!7579=9618!!" then @treatment_specimen_pathology := 8
								else @treatment_specimen_pathology := null
							end as treatment_specimen_pathology, 
                           case
								when obs regexp "!!7428=1065!!" then @satisfactory_colposcopy := 1
								when obs regexp "!!7428=1066!!" then @satisfactory_colposcopy := 0
                                when obs regexp "!!7428=1118!!" then @satisfactory_colposcopy := 2
								else @satisfactory_colposcopy := null
							end as satisfactory_colposcopy, 
                             case
								when obs regexp "!!7383=1115!!" then @colposcopy_findings := 1
								when obs regexp "!!7383=7469!!" then @colposcopy_findings := 2
                                when obs regexp "!!7383=7473!!" then @colposcopy_findings := 3
                                when obs regexp "!!7383=7470!!" then @colposcopy_findings := 4
                                when obs regexp "!!7383=7471!!" then @colposcopy_findings := 5
                                when obs regexp "!!7383=7472!!" then @colposcopy_findings := 6
								else @colposcopy_findings := null
							end as colposcopy_findings, 
                            case
								when obs regexp "!!7477=7474!!" then @cervica_lesion_size := 1
								when obs regexp "!!7477=9619!!" then @cervica_lesion_size := 2
                                when obs regexp "!!7477=7476!!" then @cervica_lesion_size := 3
								else @cervica_lesion_size := null
							end as cervica_lesion_size, 
                           case
								when obs regexp "!!7484=1115!!" then @dysplasia_cervix_impression := 1
								when obs regexp "!!7484=7424!!" then @dysplasia_cervix_impression := 2
                                when obs regexp "!!7484=7425!!" then @dysplasia_cervix_impression := 3
                                when obs regexp "!!7484=7216!!" then @dysplasia_cervix_impression := 4
                                when obs regexp "!!7484=7421!!" then @dysplasia_cervix_impression := 5
								else @dysplasia_cervix_impression := null
							end as dysplasia_cervix_impression, 
                            case
								when obs regexp "!!7490=1115!!" then @dysplasia_vagina_impression := 1
								when obs regexp "!!7490=1447!!" then @dysplasia_vagina_impression := 2
                                when obs regexp "!!7490=9177!!" then @dysplasia_vagina_impression := 3
								else @dysplasia_vagina_impression := null
							end as dysplasia_vagina_impression, 
                           case
								when obs regexp "!!7487=1115!!" then @dysplasia_vulva_impression := 1
								when obs regexp "!!7487=1447!!" then @dysplasia_vulva_impression := 2
                                when obs regexp "!!7487=9177!!" then @dysplasia_vulva_impression := 3
								else @dysplasia_vulva_impression := null
							end as dysplasia_vulva_impression, 
                          case
								when obs regexp "!!7479=1107!!" then @dysplasia_procedure_done := 1
								when obs regexp "!!7479=6511!!"  then @dysplasia_procedure_done := 2
                                when obs regexp "!!7479=7466!!" then @dysplasia_procedure_done := 3
                                when obs regexp "!!7479=7147!!" then @dysplasia_procedure_done := 4
                                when obs regexp "!!7479=9724!!" then @dysplasia_procedure_done := 5
                                when obs regexp "!!7479=6510!!" then @dysplasia_procedure_done := 6
                                when obs regexp "!!7479=5622!!" then @dysplasia_procedure_done := 7
								else @dysplasia_procedure_done := null
							end as dysplasia_procedure_done, 
                             case
								when obs regexp "!!1915=" then @other_dysplasia_procedure_done := replace(replace((substring_index(substring(obs,locate("!!1915=",obs)),@sep,1)),"!!1915=",""),"!!","")
								else @other_dysplasia_procedure_done := null
							end as other_dysplasia_procedure_done,
                             case
								when obs regexp "!!7500=9725!!" then @dysplasia_mgmt_plan := 1
								when obs regexp "!!7500=9178!!" then @dysplasia_mgmt_plan := 2
                                when obs regexp "!!7500=7497!!" then @dysplasia_mgmt_plan := 3
                                when obs regexp "!!7500=7383!!" then @dysplasia_mgmt_plan := 4
                                when obs regexp "!!7500=7499!!" then @dysplasia_mgmt_plan := 5
								when obs regexp "!!7500=5622!!" then @dysplasia_mgmt_plan:= 6
								else @dysplasia_mgmt_plan := null
							end as dysplasia_mgmt_plan, 
                            case
								when obs regexp "!!1915=" then @other_dysplasia_mgmt_plan := replace(replace((substring_index(substring(obs,locate("!!1915=",obs)),@sep,1)),"!!1915=",""),"!!","")
								else @other_dysplasia_mgmt_plan := null
							end as other_dysplasia_mgmt_plan,
                            case
								when obs regexp "!!7222=" then @dysplasia_assesment_notes := replace(replace((substring_index(substring(obs,locate("!!7222=",obs)),@sep,1)),"!!7222=",""),"!!","")
								else @dysplasia_assesment_notes := null
							end as dysplasia_assesment_notes,
                             case
								when obs regexp "!!5096=" then @dysplasia_rtc_date := replace(replace((substring_index(substring(obs,locate("!!5096=",obs)),@sep,1)),"!!5096=",""),"!!","")
								else @dysplasia_rtc_date := null
							end as dysplasia_rtc_date,
                           case
								when obs regexp "!!7423=1115!!" then @Pap_smear_results := 1
								when obs regexp "!!7423=7417!!" then @Pap_smear_results := 2
                                when obs regexp "!!7423=7418!!" then @Pap_smear_results := 3
                                when obs regexp "!!7423=7419!!" then @Pap_smear_results := 4
                                when obs regexp "!!7423=7420!!" then @Pap_smear_results := 5
								when obs regexp "!!7423=7422!!" then @Pap_smear_results := 6
								else @Pap_smear_results := null
							end as Pap_smear_results,
                             case
								when obs regexp "!!8268=8266!!" then @leep_location := 1
								when obs regexp "!!8268=8267!!" then @leep_location := 2
								else @leep_location := null
							end as leep_location,
                            case
								when obs regexp "!!7645=1115!!" then @Cervix_biopsy_results := 1
								when obs regexp "!!7645=7424!!" then @Cervix_biopsy_results := 2
								when obs regexp "!!7645=7425!!" then @Cervix_biopsy_results := 3
                                when obs regexp "!!7645=7216!!" then @Cervix_biopsy_results := 4
                                when obs regexp "!!7645=1447!!" then @Cervix_biopsy_results := 5
								when obs regexp "!!7645=149!!" then  @Cervix_biopsy_results := 6
								when obs regexp "!!7645=8282!!" then @Cervix_biopsy_results := 7
								when obs regexp "!!7645=9620!!" then @Cervix_biopsy_results := 8
                                when obs regexp "!!7645=8276!!" then @Cervix_biopsy_results := 9
                                when obs regexp "!!7645=9617!!" then @Cervix_biopsy_results := 10
                                when obs regexp "!!7645=9621!!" then @Cervix_biopsy_results := 11
								when obs regexp "!!7645=7421!!" then @Cervix_biopsy_results := 12
                                when obs regexp "!!7645=7422!!" then @Cervix_biopsy_results := 13
								when obs regexp "!!7645=9618!!" then @Cervix_biopsy_results := 14
								else @Cervix_biopsy_results := null
							end as Cervix_biopsy_results,
                            case
								when obs regexp "!!7647=1115!!" then @Vagina_biopsy_results := 1
								when obs regexp "!!7647=7492!!" then @Vagina_biopsy_results := 2
                                when obs regexp "!!7647=7491!!" then @Vagina_biopsy_results := 3
                                when obs regexp "!!7647=7435!!" then @Vagina_biopsy_results := 4
                                when obs regexp "!!7647=6537!!" then @Vagina_biopsy_results := 5
								when obs regexp "!!7647=1447!!" then @Vagina_biopsy_results := 6
                                when obs regexp "!!7647=8282!!" then @Vagina_biopsy_results := 7
								when obs regexp "!!7647=9620!!" then @Vagina_biopsy_results := 8
                                when obs regexp "!!7647=8276!!" then @Vagina_biopsy_results := 9
                                when obs regexp "!!7647=9617!!" then @Vagina_biopsy_results := 10
                                when obs regexp "!!7647=9621!!" then @Vagina_biopsy_results := 11
								when obs regexp "!!7647=7421!!" then @Vagina_biopsy_results := 12
                                when obs regexp "!!7647=7422!!" then @Vagina_biopsy_results := 13
								when obs regexp "!!7647=9618!!" then @Vagina_biopsy_results := 14
								else @Vagina_biopsy_results := null
							end as Vagina_biopsy_results,
                            case
								when obs regexp "!!7646=1115!!" then @Vulva_biopsy_result := 1
								when obs regexp "!!7646=7489!!" then @Vulva_biopsy_result := 2
                                when obs regexp "!!7646=7488!!" then @Vulva_biopsy_result := 3
                                when obs regexp "!!7646=7483!!" then @Vulva_biopsy_result := 4
                                when obs regexp "!!7646=9618!!" then @Vulva_biopsy_result := 5
								when obs regexp "!!7646=1447!!" then @Vulva_biopsy_result := 6
								when obs regexp "!!7646=8282!!" then @Vulva_biopsy_result := 7
								when obs regexp "!!7646=9620!!" then @Vulva_biopsy_result := 8
                                when obs regexp "!!7646=8276!!" then @Vulva_biopsy_result := 9
                                when obs regexp "!!7646=9617!!" then @Vulva_biopsy_result := 10
                                when obs regexp "!!7646=9621!!" then @Vulva_biopsy_result := 11
								when obs regexp "!!7646=7421!!" then @Vulva_biopsy_result := 12
                                when obs regexp "!!7646=7422!!" then @Vulva_biopsy_result := 13
								else @Vulva_biopsy_result := null
							end as Vulva_biopsy_result,
                            case
								when obs regexp "!!10207=1115!!" then @endometrium_biopsy := 1
								when obs regexp "!!10207=8276!!" then @endometrium_biopsy := 2
                                when obs regexp "!!10207=9617!!" then @endometrium_biopsy := 3
                                when obs regexp "!!10207=9621!!" then @endometrium_biopsy := 4
                                when obs regexp "!!10207=9618!!" then @endometrium_biopsy := 5
								when obs regexp "!!10207=7421!!" then @endometrium_biopsy := 6
								when obs regexp "!!10207=8282!!" then @endometrium_biopsy := 7
								when obs regexp "!!10207=9620!!" then @endometrium_biopsy := 8
                                when obs regexp "!!10207=7422!!" then @endometrium_biopsy := 9
								else @endometrium_biopsy := null
							end as endometrium_biopsy,
                             case
								when obs regexp "!!10204=1115!!" then @ECC := 1
								when obs regexp "!!10204=7424!!" then @ECC := 2
								when obs regexp "!!10204=7425!!" then @ECC := 3
                                when obs regexp "!!10204=7216!!" then @ECC := 4
                                when obs regexp "!!10204=1447!!" then @ECC := 5
								when obs regexp "!!10204=149!!" then  @ECC := 6
								when obs regexp "!!10204=8282!!" then @ECC := 7
								when obs regexp "!!10204=9620!!" then @ECC := 8
                                when obs regexp "!!10204=8276!!" then @ECC := 9
                                when obs regexp "!!10204=9617!!" then @ECC := 10
                                when obs regexp "!!10204=9621!!" then @ECC := 11
								when obs regexp "!!10204=7421!!" then @ECC := 12
                                when obs regexp "!!10204=7422!!" then @ECC := 13
								when obs regexp "!!10204=9618!!" then @ECC := 14
								else @ECC := null
							end as ECC,
							case
								when obs regexp "!!7500=9725!!" then @biopsy_results_mngmt_plan := 1
								when obs regexp "!!7500=9178!!" then @biopsy_results_mngmt_plan := 2
                                when obs regexp "!!7500=9498!!" then @biopsy_results_mngmt_plan := 2
                                when obs regexp "!!7500=7496!!" then @biopsy_results_mngmt_plan := 3
                                when obs regexp "!!7500=7497!!" then @biopsy_results_mngmt_plan := 4
                                when obs regexp "!!7500=7499!!" then @biopsy_results_mngmt_plan := 5
                                when obs regexp "!!7500=6105!!" then @biopsy_results_mngmt_plan := 6
                                when obs regexp "!!7500=7147!!" then @biopsy_results_mngmt_plan := 7
                                when obs regexp "!!7500=7466!!" then @biopsy_results_mngmt_plan := 8
                                when obs regexp "!!7500=10200!!" then @biopsy_results_mngmt_plan := 9
								else @biopsy_results_mngmt_plan := null
							end as biopsy_results_mngmt_plan,
                            case
								when obs regexp "!!9868=9852!!" then @cancer_staging := 1
								when obs regexp "!!9868=9856!!" then @cancer_staging := 2
                                when obs regexp "!!9868=9860!!" then @cancer_staging := 3
                                when obs regexp "!!9868=9864!!" then @cancer_staging := 4
								else @cancer_staging := null
							end as cancer_staging,
                            null as next_app_date
		
						from flat_cervical_cancer_screening_0 t1
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


						alter table flat_cervical_cancer_screening_1 drop prev_id, drop cur_id;

						drop table if exists flat_cervical_cancer_screening_2;
						create temporary table flat_cervical_cancer_screening_2
						(select *,
							@prev_id := @cur_id as prev_id,
							@cur_id := person_id as cur_id,

							case
								when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
								else @prev_encounter_datetime := null
							end as next_encounter_datetime_cervical_cancer_screening,

							@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
								else @next_encounter_type := null
							end as next_encounter_type_cervical_cancer_screening,

							@cur_encounter_type := encounter_type as cur_encounter_type,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as next_clinical_datetime_cervical_cancer_screening,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as next_clinical_location_id_cervical_cancer_screening,

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
							end as next_clinical_rtc_date_cervical_cancer_screening,

							case
								when is_clinical_encounter then @cur_clinical_rtc_date := next_app_date
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinical_rtc_date

							from flat_cervical_cancer_screening_1
							order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
						);

						alter table flat_cervical_cancer_screening_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;


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

						drop temporary table if exists flat_cervical_cancer_screening_3;
						create temporary table flat_cervical_cancer_screening_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
						(select
							*,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,

							case
						        when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
						        else @prev_encounter_type:=null
							end as prev_encounter_type_cervical_cancer_screening,	
                            @cur_encounter_type := encounter_type as cur_encounter_type,

							case
						        when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
						        else @prev_encounter_datetime := null
						    end as prev_encounter_datetime_cervical_cancer_screening,

                            @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as prev_clinical_datetime_cervical_cancer_screening,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as prev_clinical_location_id_cervical_cancer_screening,

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
							end as prev_clinical_rtc_date_cervical_cancer_screening,

							case
								when is_clinical_encounter then @cur_clinical_rtc_date := next_app_date
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinic_rtc_date

							from flat_cervical_cancer_screening_2 t1
							order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
                                        


					SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    flat_cervical_cancer_screening_3;
                    
SELECT @new_encounter_rows;                    
					set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

					SET @dyn_sql=CONCAT('replace into ',@write_table,											  
						'(select
								null,
								person_id ,
                        encounter_id,
                        encounter_type,
                        encounter_datetime,
						visit_id,
                       location_id,
                       t2.uuid as location_uuid,
						uuid,
                        age,
                        encounter_purpose,
                       cur_visit_type,
							actual_scheduled_date,
							gravida,
							parity,
							Mensturation_status,
							lmp,
							pregnancy_status,
							pregnant_edd,
							reason_not_pregnant,
							hiv_status,
							viral_load,
							viral_load_date,
							prior_via_done,
							prior_via_result,
							prior_via_date,
							cur_via_result,
							visual_impression_cervix,
							visual_impression_vagina,
							visual_impression_vulva,
							via_procedure_done,
							other_via_procedure_done,
							via_management_plan,
							other_via_management_plan,
							via_assessment_notes,
							via_rtc_date,
							prior_dysplasia_done,
							previous_via_result,
							previous_via_result_date,
							prior_papsmear_result, 
							prior_biopsy_result,
							prior_biopsy_result_date,
							prior_biopsy_result_other,
							prior_biopsy_result_date_other,
							past_dysplasia_treatment,
							treatment_specimen_pathology,
							satisfactory_colposcopy,
							colposcopy_findings,
							cervica_lesion_size,
							dysplasia_cervix_impression,
							dysplasia_vagina_impression,
							dysplasia_vulva_impression,
							dysplasia_procedure_done,
							other_dysplasia_procedure_done,
							dysplasia_mgmt_plan,
							other_dysplasia_mgmt_plan,
							dysplasia_assesment_notes,
							dysplasia_rtc_date,
                            Pap_smear_results,
                            leep_location,
                            Cervix_biopsy_results,
                            Vagina_biopsy_results,
                            Vulva_biopsy_result,
                            endometrium_biopsy,
                            ECC,
                            biopsy_results_mngmt_plan,
                            cancer_staging,
                            next_app_date,
                                
                                prev_encounter_datetime_cervical_cancer_screening,
								next_encounter_datetime_cervical_cancer_screening,
								prev_encounter_type_cervical_cancer_screening,
								next_encounter_type_cervical_cancer_screening,
								prev_clinical_datetime_cervical_cancer_screening,
								next_clinical_datetime_cervical_cancer_screening,
								prev_clinical_location_id_cervical_cancer_screening,
								next_clinical_location_id_cervical_cancer_screening,
								prev_clinical_rtc_date_cervical_cancer_screening,
								next_clinical_rtc_date_cervical_cancer_screening

						from flat_cervical_cancer_screening_3 t1
                        join amrs.location t2 using (location_id))');

					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;


					SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_cervical_cancer_screening_build_queue__0 t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
					#select @person_ids_count := (select count(*) from flat_cervical_cancer_screening_build_queue_2);                        
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

		END$$
DELIMITER ;
