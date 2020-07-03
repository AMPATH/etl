DELIMITER $$
CREATE  PROCEDURE `generate_flat_lung_cancer_treatment`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
        SET @primary_table := "flat_lung_cancer_treatment";
        SET @query_type := query_type;
                        
        SET @total_rows_written := 0;

        SET @encounter_types := "(169,170)";
        SET @clinical_encounter_types := "(-1)";
        SET @non_clinical_encounter_types := "(-1)";
        SET @other_encounter_types := "(-1)";
                        
        SET @start := now();
        SET @table_version := "flat_lung_cancer_treatment_v1_0";

        set session sort_buffer_size := 512000000;

        SET @sep := " ## ";
        SET @boundary := "!!";
        SET @last_date_created := (select max(max_date_created) from etl.flat_obs);

        CREATE TABLE IF NOT EXISTS flat_lung_cancer_treatment (
            date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            person_id INT,
            encounter_id INT,
            encounter_type INT,
            encounter_datetime DATETIME,
            visit_id INT,
            location_id INT,
            location_uuid VARCHAR(100),
            gender CHAR(100),
            age INT,
            birthdate DATE,
            death_date DATE,
            cur_visit_type INT,
            referred_from INT,
            facility_name VARCHAR(100),
            referral_date DATETIME,
            referred_by INT,
            marital_status INT,
            main_occupation INT,
            patient_level_education INT,
            smoke_cigarettes INT,
            number_of_sticks INT,
            cigga_smoke_duration INT,
            duration_since_last_use INT,
            tobacco_use INT,
            tobacco_use_duration INT,
            drink_alcohol INT,
            chemical_exposure INT,
            asbestos_exposure INT,
            any_family_mem_with_cancer INT,
            family_member INT,
            chief_complaints INT,
            complain_duration INT,
            number_days INT,
            number_weeks INT,
            number_months INT,
            number_years INT,
            ecog_performance INT,
            general_exam INT,
            heent INT,
            chest INT,
            heart INT,
            abdomen_exam INT,
            urogenital INT,
            extremities INT,
            testicular_exam INT,
            nodal_survey INT,
            musculo_skeletal INT,
            neurologic INT,
            previous_tb_treatment INT,
            ever_diagnosed INT,
            year_of_diagnosis INT,
            date_of_diagnosis DATE,
            hiv_status INT,
            chest_xray_result INT,
            chest_ct_scan_results INT,
            pet_scan_results INT,
            abdominal_ultrasound_result INT,
            other_imaging_results INT,
            test_ordered INT,
            other_radiology VARCHAR(500),
            other_laboratory VARCHAR(500),
            procedure_ordered INT,
            procedure_performed INT,
            other_procedures VARCHAR(500),
            procedure_date DATETIME,
            lung_cancer_type INT,
            small_cell_lung_ca_staging INT,
            non_small_cell_ca_type INT,
            staging INT,
            cancer_staging_date DATETIME,
            metastasis_region VARCHAR(500),
            treatment_plan INT,
            other_treatment_plan INT,
            radiotherapy_sessions INT,
            surgery_date DATETIME,
            chemotherapy_plan INT,
            chemo_start_date DATETIME,
            chemo_stop_date DATETIME,
            treatment_intent INT,
            chemo_regimen VARCHAR(500),
            chemo_cycle INT,
            total_planned_chemo_cycle INT,
            chemo_drug INT,
            dosage_in_milligrams INT,
            drug_route INT,
            other_drugs INT,
            purpose INT,
            referral_ordered INT,
            rtc DATETIME,
            prev_encounter_datetime_lung_cancer_treatment DATETIME,
            next_encounter_datetime_lung_cancer_treatment DATETIME,
            prev_encounter_type_lung_cancer_treatment MEDIUMINT,
            next_encounter_type_lung_cancer_treatment MEDIUMINT,
            prev_clinical_datetime_lung_cancer_treatment DATETIME,
            next_clinical_datetime_lung_cancer_treatment DATETIME,
            prev_clinical_location_id_lung_cancer_treatment MEDIUMINT,
            next_clinical_location_id_lung_cancer_treatment MEDIUMINT,
            prev_clinical_rtc_date_lung_cancer_treatment DATETIME,
            next_clinical_rtc_date_lung_cancer_treatment DATETIME,
            PRIMARY KEY encounter_id (encounter_id),
            INDEX person_date (person_id, encounter_datetime),
            INDEX location_enc_date (location_uuid, encounter_datetime),
            INDEX enc_date_location (encounter_datetime, location_uuid),
            INDEX loc_id_enc_date_next_clinical (location_id, encounter_datetime, next_clinical_datetime_lung_cancer_treatment),
            INDEX encounter_type (encounter_type),
            INDEX date_created (date_created)
        );
        
        if (@query_type="build") then
            select 'BUILDING..........................................';
                                                  
            set @write_table = concat("flat_lung_cancer_treatment_temp_",queue_number);
            set @queue_table = concat("flat_lung_cancer_treatment_build_queue_",queue_number);                    												
                      
            SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
            PREPARE s1 from @dyn_sql; 
            EXECUTE s1; 
            DEALLOCATE PREPARE s1;  

            SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_lung_cancer_treatment_build_queue limit ', queue_size, ');'); 
            PREPARE s1 from @dyn_sql; 
            EXECUTE s1; 
            DEALLOCATE PREPARE s1;  
            
            SET @dyn_sql=CONCAT('delete t1 from flat_lung_cancer_treatment_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
            PREPARE s1 from @dyn_sql; 
            EXECUTE s1; 
            DEALLOCATE PREPARE s1;  
        end if;

        if (@query_type="sync") then
            select 'SYNCING..........................................';
            set @write_table = "flat_lung_cancer_treatment";
            set @queue_table = "flat_lung_cancer_treatment_sync_queue";
            CREATE TABLE IF NOT EXISTS flat_lung_cancer_treatment_sync_queue (
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

                replace into flat_lung_cancer_treatment_sync_queue
                (select distinct patient_id
                  from amrs.encounter
                  where date_changed > @last_update
                );
              
                          
            SELECT 'Finding patients in flat_obs...';

                replace into flat_lung_cancer_treatment_sync_queue
                (select distinct person_id
                  from etl.flat_obs
                  where max_date_created > @last_update
                );


            SELECT 'Finding patients in flat_lab_obs...';
                replace into flat_lung_cancer_treatment_sync_queue
                (select distinct person_id
                  from etl.flat_lab_obs
                  where max_date_created > @last_update
                );

            SELECT 'Finding patients in flat_orders...';
                replace into flat_lung_cancer_treatment_sync_queue
                (select distinct person_id
                  from etl.flat_orders
                  where max_date_created > @last_update
                );
                              
                replace into flat_lung_cancer_treatment_sync_queue
                (select person_id from 
                  amrs.person 
                  where date_voided > @last_update);

                replace into flat_lung_cancer_treatment_sync_queue
                (select person_id from 
                  amrs.person 
                  where date_changed > @last_update);           
        
        end if;        

        -- Remove test patients
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
          drop temporary table if exists flat_lung_cancer_treatment_build_queue__0;

          SET @dyn_sql=CONCAT('create temporary table flat_lung_cancer_treatment_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
          PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
          DEALLOCATE PREPARE s1;  
                  
          drop temporary table if exists flat_lung_cancer_treatment_0a;
          SET @dyn_sql = CONCAT(
              'create temporary table flat_lung_cancer_treatment_0a
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
                join flat_lung_cancer_treatment_build_queue__0 t0 using (person_id)
                left join etl.flat_orders t2 using(encounter_id)
              where t1.encounter_type in ',@encounter_types,');');
                          
          PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
          DEALLOCATE PREPARE s1;  
                
          insert into flat_lung_cancer_treatment_0a
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
              join flat_lung_cancer_treatment_build_queue__0 t0 using (person_id)
          );


          drop temporary table if exists flat_lung_cancer_treatment_0;
          create temporary table flat_lung_cancer_treatment_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
          (select * from flat_lung_cancer_treatment_0a
          order by person_id, date(encounter_datetime), encounter_type_sort_index
          );
                      
          set @birthdate = null; 
          set @death_date = null; 
          set @cur_visit_type = null;
          set @referred_from = null;
          set @facility_name = null;
          set @referral_date = null;
          set @referred_by = null;
          set @marital_status = null;
          set @main_occupation = null;
          set @patient_level_education = null;
          set @smoke_cigarettes = null;
          set @number_of_sticks = null;
          set @cigga_smoke_duration = null;
          set @duration_since_last_use = null;
          set @tobacco_use = null;
          set @tobacco_use_duration = null;
          set @drink_alcohol = null;
          set @chemical_exposure = null;
          set @asbestos_exposure = null;
          set @any_family_mem_with_cancer = null;
          set @family_member = null;
          set @chief_complaints = null;
          set @complain_duration = null;
          set @number_days = null;
          set @number_weeks = null;
          set @number_months = null;
          set @number_years = null;
          set @ecog_performance = null;
          set @general_exam = null;
          set @heent = null;
          set @chest = null;
          set @heart = null;
          set @abdomen_exam = null;
          set @urogenital = null;
          set @extremities = null;
          set @testicular_exam = null;
          set @nodal_survey = null;
          set @musculo_skeletal = null;
          set @neurologic = null;
          set @previous_tb_treatment = null;
          set @ever_diagnosed = null;
          set @year_of_diagnosis = null;
          set @date_of_diagnosis = null;
          set @hiv_status = null;
          set @chest_xray_result= null;
          set @chest_ct_scan_results = null;
          set @pet_scan_results = null;
          set @abdominal_ultrasound_result = null;
          set @other_imaging_results = null;
          set @test_ordered = null;
          set @other_radiology = null;
          set @other_laboratory = null;
          set @procedure_ordered= null;
          set @procedure_performed = null;
          set @other_procedures = null;
          set @procedure_date = null;
          set @lung_cancer_type = null;
          set @small_cell_lung_ca_staging = null;
          set @non_small_cell_ca_type = null;
          set @staging = null;
          set @cancer_staging_date= null;
          set @metastasis_region = null;
          set @treatment_plan = null;
          set @other_treatment_plan = null;
          set @radiotherapy_sessions= null;
          set @surgery_date = null;
          set @chemotherapy_plan = null;
          set @chemo_start_date = null;
          set @chemo_stop_date = null;
          set @treatment_intent = null;
          set @chemo_regimen = null;
          set @chemo_cycle = null;
          set @total_planned_chemo_cycle = null;
          set @chemo_drug = null;
          set @dosage_in_milligrams = null;
          set @drug_route = null;
          set @other_drugs = null;
          set @purpose = null;
          set @referral_ordered = null;
          set @rtc = null;
                  
          drop temporary table if exists flat_lung_cancer_treatment_1;
          create temporary table flat_lung_cancer_treatment_1 #(index encounter_id (encounter_id))
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
            t1.location_id,
            t1.is_clinical_encounter,
            p.gender,
            p.birthdate,
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
                when obs regexp "!!6749=8161!!" then @referred_from := 1
                when obs regexp "!!6749=5487!!" then @referred_from := 2
                when obs regexp "!!6749=1275!!" then @referred_from := 3
                when obs regexp "!!6749=2242!!" then @referred_from := 4
                when obs regexp "!!6749=6572!!" then @referred_from := 5
                when obs regexp "!!6749=978!!" then @referred_from := 6
                when obs regexp "!!6749=5622!!" then @referred_from := 7
                else @referred_from := null
            end as referred_from,
            case 
                when obs regexp "!!6748=" then @facility_name := GetValues(obs,6748)
                else @facility_name = null
            end as facility_name,
            case 
                when obs regexp "!!9158=" then @referral_date := GetValues(obs,9158)
                else @referral_date = null
            end as referral_date,
            case
                when obs regexp "!!10135=1496!!" then @referred_by := 1
                when obs regexp "!!10135=6280!!" then @referred_by := 2
                when obs regexp "!!10135=5507!!" then @referred_by := 3
                when obs regexp "!!10135=5622!!" then @referred_by := 4
                else @referred_by := null
            end as referred_by,
            case
                when obs regexp "!!1054=1175!!" then @marital_status := 1
                when obs regexp "!!1054=1057!!" then @marital_status := 2
                when obs regexp "!!1054=5555!!" then @marital_status := 3
                when obs regexp "!!1054=6290!!" then @marital_status := 4
                when obs regexp "!!1054=1060!!" then @marital_status := 5
                when obs regexp "!!1054=1056!!" then @marital_status := 6
                when obs regexp "!!1054=1058!!" then @marital_status := 7
                when obs regexp "!!1054=1059!!" then @marital_status := 8
                else @marital_status := null
            end as marital_status,
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
                else @main_occupation = null
            end as main_occupation,
            case
                when obs regexp "!!1605=1107!!" then @patient_level_education := 1
                when obs regexp "!!1605=6214!!" then @patient_level_education := 2
                when obs regexp "!!1605=6215!!" then @patient_level_education := 3
                when obs regexp "!!1605=1604!!" then @patient_level_education := 4
                else @patient_level_education := null
            end as patient_level_education,

            case
                when obs regexp "!!6473=1065!!" then @smoke_cigarettes := 1
                when obs regexp "!!6473=1066!!" then @smoke_cigarettes := 2
                when obs regexp "!!6473=1679!!" then @smoke_cigarettes := 3
                else @smoke_cigarettes := null
            end as smoke_cigarettes,
            case 
                when obs regexp "!!2069=" then @number_of_sticks := GetValues(obs,2069)
                else @number_of_sticks = null
            end as number_of_sticks,
            case 
                when obs regexp "!!2070=" then @cigga_smoke_duration := GetValues(obs,2070)
                else @cigga_smoke_duration = null
            end as cigga_smoke_duration,
            case 
                when obs regexp "!!2068=" then @duration_since_last_use := GetValues(obs,2068)
                else @duration_since_last_use = null
            end as duration_since_last_use,
            case	
                when obs regexp "!!7973=1065!!" then @tobacco_use := 1
                when obs regexp "!!7973=1066!!" then @tobacco_use := 2
                when obs regexp "!!7973=1679!!" then @tobacco_use := 3
                else @tobacco_use = null
            end as tobacco_use,
            case 
                when obs regexp "!!8144=" then @tobacco_use_duration := GetValues(obs,8144)
                else @tobacco_use_duration = null
            end as tobacco_use_duration,
            case
                when obs regexp "!!1684=1065!!" then @drink_alcohol := 1
                when obs regexp "!!1684=1066!!" then @drink_alcohol := 2
                when obs regexp "!!1684=1679!!" then @drink_alcohol := 3
                else @drink_alcohol := null
            end as drink_alcohol,  
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
                when obs regexp "!!6802=1065!!" then @any_family_mem_with_cancer := 1
                when obs regexp "!!6802=1067!!" then @any_family_mem_with_cancer := 2
                else @any_family_mem_with_cancer = null
            end as any_family_mem_with_cancer,
            case
                when obs regexp "!!9635=1107!!" then @family_member := 1
                when obs regexp "!!9635=978!!" then @family_member := 2
                when obs regexp "!!9635=1692!!" then @family_member := 3
                when obs regexp "!!9635=972!!" then @family_member := 4
                when obs regexp "!!9635=1671!!" then @family_member := 5
                when obs regexp "!!9635=1393!!" then @family_member := 6
                when obs regexp "!!9635=1392!!" then @family_member := 7
                when obs regexp "!!9635=1395!!" then @family_member := 8
                when obs regexp "!!9635=1394!!" then @family_member := 9
                when obs regexp "!!9635=1673!!" then @family_member := 10
                else @family_member := null
            end as family_member,
            case
                when obs regexp "!!5219=136!!" then @chief_complaints := 1
                when obs regexp "!!5219=107!!" then @chief_complaints := 2
                when obs regexp "!!5219=6786!!" then @chief_complaints := 3
                when obs regexp "!!5219=5954!!" then @chief_complaints := 4
                when obs regexp "!!5219=832!!" then @chief_complaints := 5
                when obs regexp "!!5219=5960!!" then @chief_complaints := 6
                when obs regexp "!!5219=10121!!" then @chief_complaints := 7
                when obs regexp "!!5219=5622!!" then @chief_complaints := 8
                else @chief_complaints := null
            end as chief_complaints,
            case	
                when obs regexp "!!8777=1072!!" then @complain_duration := 1
                when obs regexp "!!8777=1073!!" then @complain_duration := 2
                when obs regexp "!!8777=1074!!" then @complain_duration := 3
                when obs regexp "!!8777=8787!!" then @complain_duration := 4
                else @complain_duration = null
            end as complain_duration,
            case 
                when obs regexp "!!1892=" then @number_days := GetValues(obs,1892)
                else @number_days = null
            end as number_days,
            case 
                when obs regexp "!!1893=" then @number_weeks := GetValues(obs,1893)
                else @number_weeks = null
            end as number_weeks,
            case 
                when obs regexp "!!1894=" then @number_months := GetValues(obs,1894)
                else @number_months = null
            end as number_months,
            case 
                when obs regexp "!!7953=" then @number_years := GetValues(obs,7953)
                else @number_years = null
            end as number_years,
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
                when obs regexp "!!1125=1118!!" then @abdomen_exam := 1
                when obs regexp "!!1125=1115!!" then @abdomen_exam := 2
                when obs regexp "!!1125=5105!!" then @abdomen_exam := 3
                when obs regexp "!!1125=5008!!" then @abdomen_exam := 4
                when obs regexp "!!1125=5009!!" then @abdomen_exam := 5
                when obs regexp "!!1125=581!!" then @abdomen_exam := 6
                when obs regexp "!!1125=5103!!" then @abdomen_exam := 7
                else @abdomen_exam = null
            end as abdomen_exam,		
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
                when obs regexp "!!1128=1118!!" then @musculo_skeletal := 1
                when obs regexp "!!1128=1115!!" then @musculo_skeletal := 2
                when obs regexp "!!1128=1116!!" then @musculo_skeletal := 3
                else @musculo_skeletal = null
            end as musculo_skeletal,
            case	
                when obs regexp "!!1129=1118!!" then @neurologic := 1
                when obs regexp "!!1129=1115!!" then @neurologic := 2
                when obs regexp "!!1129=599!!" then @neurologic := 3
                when obs regexp "!!1129=497!!" then @neurologic := 4
                when obs regexp "!!1129=5108!!" then @neurologic := 5
                else @neurologic = null
            end as neurologic,
            case 
                when obs regexp "!!5704=" then @previous_tb_treatment := GetValues(obs,5704)
                else @previous_tb_treatment = null
            end as previous_tb_treatment,
            case	
                when obs regexp "!!6245=142!!" then @ever_diagnosed := 1
                when obs regexp "!!6245=5!!" then @ever_diagnosed := 2
                when obs regexp "!!6245=6237!!" then @ever_diagnosed := 3
                when obs regexp "!!6245=903!!" then @ever_diagnosed := 4
                when obs regexp "!!6245=6033!!" then @ever_diagnosed := 5
                when obs regexp "!!6245=43!!" then @ever_diagnosed := 6
                when obs regexp "!!6245=58!!" then @ever_diagnosed := 7
                else @ever_diagnosed = null
            end as ever_diagnosed,
            case 
                when obs regexp "!!9281=" then @year_of_diagnosis := GetValues(obs,9281)
                else @year_of_diagnosis = null
            end as year_of_diagnosis,
            case 
                when obs regexp "!!9728=" then @date_of_diagnosis := DATE(replace(replace((substring_index(substring(obs, locate("!!9728=", obs)), @sep, 1)), "!!9728=", ""), "!!", ""))
                else @date_of_diagnosis = null
            end as date_of_diagnosis,
            case	
                when obs regexp "!!6709=1067!!" then @hiv_status := 1
                when obs regexp "!!6709=664!!" then @hiv_status := 2
                when obs regexp "!!6709=703!!" then @hiv_status := 3
                else @hiv_status = null
            end as hiv_status,
            case	
                when obs regexp "!!12=1115!!" then @chest_xray_result := 1
                when obs regexp "!!12=1116!!" then @chest_xray_result := 2
                else @chest_xray_result = null
            end as chest_xray_result,
            case	
                when obs regexp "!!7113=1115!!" then @chest_ct_scan_results := 1
                when obs regexp "!!7113=1117!!" then @chest_ct_scan_results := 2
                else @chest_ct_scan_results = null
            end as chest_ct_scan_results,
            case	
                when obs regexp "!!10123=1115!!" then @pet_scan_results := 1
                when obs regexp "!!10123=1118!!" then @pet_scan_results := 2
                else @pet_scan_results = null
            end as pet_scan_results,
            case	
                when obs regexp "!!845=1115!!" then @abdominal_ultrasound_result := 1
                when obs regexp "!!845=1119!!" then @abdominal_ultrasound_result := 2
                else @abdominal_ultrasound_result = null
            end as abdominal_ultrasound_result,
            case 
                when obs regexp "!!10124=" then @other_imaging_results = GetValues(obs,10124)
                else @other_imaging_results = null
            end as other_imaging_results,

            case	
                when obs regexp "!!1271=1107!!" then @test_ordered := 1
                when obs regexp "!!1271=1019!!" then @test_ordered := 2
                when obs regexp "!!1271=790!!" then @test_ordered := 3
                when obs regexp "!!1271=953!!" then @test_ordered := 4
                when obs regexp "!!1271=6898!!" then @test_ordered := 5
                when obs regexp "!!1271=9009!!" then @test_ordered := 6
                else @test_ordered = null
            end as test_ordered,
            case 
                when obs regexp "!!8190=" then @other_radiology := GetValues(obs,8190)
                else @other_radiology = null
            end as other_radiology,
            case 
                when obs regexp "!!9538=" then @other_laboratory := GetValues(obs,9538)
                else @other_laboratory = null
            end as other_laboratory,
            case	
                when obs regexp "!!10127=1107!!" then @procedure_ordered := 1
                when obs regexp "!!10127=10075!!" then @procedure_ordered := 2
                when obs regexp "!!10127=10076!!" then @procedure_ordered := 3
                when obs regexp "!!10127=10126!!" then @procedure_ordered := 4
                when obs regexp "!!10127=5622!!" then @procedure_ordered := 5
                else @procedure_ordered = null
            end as procedure_ordered,
            case	
                when obs regexp "!!10442=1107!!" then @procedure_performed := 1
                when obs regexp "!!10442=10075!!" then @procedure_performed := 2
                when obs regexp "!!10442=10076!!" then @procedure_performed := 3
                when obs regexp "!!10442=10126!!" then @procedure_performed := 4
                when obs regexp "!!10442=5622!!" then @procedure_performed := 5
                else @procedure_performed = null
            end as procedure_performed,
            case 
                when obs regexp "!!1915=" then @other_procedures := GetValues(obs,1915)
                else @other_procedures = null
            end as other_procedures,
            case 
                when obs regexp "!!10443=" then @procedure_date := GetValues(obs,10443)
                else @procedure_date = null
            end as procedure_date,

            case	
                when obs regexp "!!7176=10129!!" then @lung_cancer_type := 1
                when obs regexp "!!7176=10130!!" then @lung_cancer_type := 2
                when obs regexp "!!7176=5622!!" then @lung_cancer_type := 3
                else @lung_cancer_type = null
            end as lung_cancer_type,
            case	
                when obs regexp "!!10137=6563!!" then @small_cell_lung_ca_staging := 1
                when obs regexp "!!10137=6564!!" then @small_cell_lung_ca_staging := 2
                else @small_cell_lung_ca_staging = null
            end as small_cell_lung_ca_staging,
            case	
                when obs regexp "!!10132=7421!!" then @non_small_cell_ca_type := 1
                when obs regexp "!!10132=7422!!" then @non_small_cell_ca_type := 2
                when obs regexp "!!10132=10131!!" then @non_small_cell_ca_type := 3
                when obs regexp "!!10132=10209!!" then @non_small_cell_ca_type := 3
                else @non_small_cell_ca_type = null
            end as non_small_cell_ca_type,
            case	
                when obs regexp "!!9868=9852!!" then @staging := 1
                when obs regexp "!!9868=9856!!" then @staging := 2
                when obs regexp "!!9868=9860!!" then @staging := 3
                when obs regexp "!!9868=9864!!" then @staging := 4
                when obs regexp "!!9868=6563!!" then @staging := 5
                when obs regexp "!!9868=6564!!" then @staging := 6
                else @staging = null
            end as staging,
            case 
                when obs regexp "!!10441=" then @cancer_staging_date := GetValues(obs,10441)
                else @cancer_staging_date = null
            end as cancer_staging_date,
            case 
                when obs regexp "!!10445=" then @metastasis_region := GetValues(obs,10445)
                else @metastasis_region = null
            end as metastasis_region,
            case	
                when obs regexp "!!8723=6575!!" then @treatment_plan := 1
                when obs regexp "!!8723=8427!!" then @treatment_plan := 2
                when obs regexp "!!8723=7465!!" then @treatment_plan := 3
                when obs regexp "!!8723=5622!!" then @treatment_plan := 4
                else @treatment_plan = null
            end as treatment_plan,
            case 
                when obs regexp "!!10039=" then @other_treatment_plan := GetValues(obs,10039)
                else @other_treatment_plan = null
            end as other_treatment_plan,
            case 
                when obs regexp "!!10133=" then @radiotherapy_sessions := GetValues(obs,10133)
                else @radiotherapy_sessions = null
            end as radiotherapy_sessions,
            case 
                when obs regexp "!!10134=" then @surgery_date := GetValues(obs,10134)
                else @surgery_date = null
            end as surgery_date,
            case	
                when obs regexp "!!9869=1256!!" then @chemotherapy_plan := 1
                when obs regexp "!!9869=1259!!" then @chemotherapy_plan := 2
                when obs regexp "!!9869=1260!!" then @chemotherapy_plan := 3
                when obs regexp "!!9869=1257!!" then @chemotherapy_plan := 4
                else @chemotherapy_plan = null
            end as chemotherapy_plan,
            case 
                when obs regexp "!!1190=" then @chemo_start_date := GetValues(obs,1190)
                else @chemo_start_date = null
            end as chemo_start_date,
            case
                when obs regexp "!!9869=1260!!" then @chemo_start_date := GetValues(obs_datetimes, 9869)
                else @chemo_stop_date = null
            end as chemo_stop_date,
            case	
                when obs regexp "!!2206=9218!!" then @treatment_intent := 1
                when obs regexp "!!2206=8428!!" then @treatment_intent := 2
                when obs regexp "!!2206=8724!!" then @treatment_intent := 3
                when obs regexp "!!2206=9219!!" then @treatment_intent := 4
                else @treatment_intent = null
            end as treatment_intent,
            case 
                when obs regexp "!!9946=" then @chemo_regimen := GetValues(obs,9946)
                else @chemo_regimen = null
            end as chemo_regimen,
            case 
                when obs regexp "!!6643=" then @chemo_cycle := GetValues(obs,6643)
                else @chemo_cycle = null
            end as chemo_cycle,
            case 
                when obs regexp "!!6644=" then @total_planned_chemo_cycle := GetValues(obs,6644)
                else @total_planned_chemo_cycle = null
            end as total_planned_chemo_cycle,
            case	
                when obs regexp "!!9918=7202!!" then @chemo_drug := 1
                when obs regexp "!!9918=8506!!" then @chemo_drug := 2
                when obs regexp "!!9918=7210!!" then @chemo_drug := 3
                when obs regexp "!!9918=8492!!" then @chemo_drug := 4
                when obs regexp "!!9918=8507!!" then @chemo_drug := 5
                else @chemo_drug = null
            end as chemo_drug,
            case 
                when obs regexp "!!1899=" then @dosage_in_milligrams := GetValues(obs,1899)
                else @dosage_in_milligrams = null
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
                else @drug_route = null
            end as drug_route,
            case 
                when obs regexp "!!1895=" then @other_drugs := GetValues(obs,1895)
                else @other_drugs = null
            end as other_drugs,
            case	
                when obs regexp "!!2206=9220!!" then @purpose := 1
                when obs regexp "!!2206=8428!!" then @purpose := 2
                else @purpose = null
            end as purpose,
            case	
                when obs regexp "!!1272=1107!!" then @referral_ordered := 1
                when obs regexp "!!1272=8724!!" then @referral_ordered := 2
                when obs regexp "!!1272=5486!!" then @referral_ordered := 3
                when obs regexp "!!1272=5884!!" then @referral_ordered := 4
                when obs regexp "!!1272=6570!!" then @referral_ordered := 5
                when obs regexp "!!1272=5622!!" then @referral_ordered := 6
                else @referral_ordered = null
            end as referral_ordered,
            case 
                when obs regexp "!!5096=" then @rtc := GetValues(obs,5096)
                else @rtc = null
            end as rtc
  
          from flat_lung_cancer_treatment_0 t1
            join amrs.person p using (person_id)
          order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
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

          alter table flat_lung_cancer_treatment_1 drop prev_id, drop cur_id;

          drop table if exists flat_lung_cancer_treatment_2;
          create temporary table flat_lung_cancer_treatment_2
          (select *,
          @prev_id := @cur_id as prev_id,
          @cur_id := person_id as cur_id,

          case
              when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
              else @prev_encounter_datetime := null
          end as next_encounter_datetime_lung_cancer_treatment,

          @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

          case
              when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
              else @next_encounter_type := null
          end as next_encounter_type_lung_cancer_treatment,

          @cur_encounter_type := encounter_type as cur_encounter_type,

          case
              when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
              else @prev_clinical_datetime := null
          end as next_clinical_datetime_lung_cancer_treatment,

          case
              when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
              else @prev_clinical_location_id := null
          end as next_clinical_location_id_lung_cancer_treatment,

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
          end as next_clinical_rtc_date_lung_cancer_treatment,

          case
              when @prev_id = @cur_id then @cur_clinical_rtc_date
              else @cur_clinical_rtc_date:= null
          end as cur_clinical_rtc_date

          from flat_lung_cancer_treatment_1
          order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
          );

          alter table flat_lung_cancer_treatment_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;


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

          drop temporary table if exists flat_lung_cancer_treatment_3;
          create temporary table flat_lung_cancer_treatment_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
          (select
          *,
          @prev_id := @cur_id as prev_id,
          @cur_id := t1.person_id as cur_id,

          case
              when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
              else @prev_encounter_type:=null
          end as prev_encounter_type_lung_cancer_treatment,	
          @cur_encounter_type := encounter_type as cur_encounter_type,

          case
              when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
              else @prev_encounter_datetime := null
          end as prev_encounter_datetime_lung_cancer_treatment,

          @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

          case
              when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
              else @prev_clinical_datetime := null
          end as prev_clinical_datetime_lung_cancer_treatment,

          case
              when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
              else @prev_clinical_location_id := null
          end as prev_clinical_location_id_lung_cancer_treatment,

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
          end as prev_clinical_rtc_date_lung_cancer_treatment,

          case
              when @prev_id = @cur_id then @cur_clinical_rtc_date
              else @cur_clinical_rtc_date:= null
          end as cur_clinic_rtc_date

          from flat_lung_cancer_treatment_2 t1
          order by person_id, date(encounter_datetime), encounter_type_sort_index
          );
                                      
          SELECT 
              COUNT(*)
          INTO @new_encounter_rows FROM
              flat_lung_cancer_treatment_3;
                    
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
                t2.uuid as location_uuid,
                gender,
                age,
                birthdate,
                death_date,
                cur_visit_type,
                referred_from,
                facility_name,
                referral_date,
                referred_by,
                marital_status,
                main_occupation,
                patient_level_education,
                smoke_cigarettes,
                number_of_sticks,
                cigga_smoke_duration,
                duration_since_last_use,
                tobacco_use,
                tobacco_use_duration,
                drink_alcohol,
                chemical_exposure,
                asbestos_exposure,
                any_family_mem_with_cancer,
                family_member,
                chief_complaints,
                complain_duration,
                number_days,
                number_weeks,
                number_months,
                number_years,
                ecog_performance,
                general_exam,
                heent,
                chest,
                heart,
                abdomen_exam,
                urogenital,
                extremities,
                testicular_exam,
                nodal_survey,
                musculo_skeletal,
                neurologic,
                previous_tb_treatment,
                ever_diagnosed,
                year_of_diagnosis,
                date_of_diagnosis,
                hiv_status,
                chest_xray_result,
                chest_ct_scan_results,
                pet_scan_results,
                abdominal_ultrasound_result,
                other_imaging_results,
                test_ordered,
                other_radiology,
                other_laboratory,
                procedure_ordered,
                procedure_performed,
                other_procedures,
                procedure_date,
                lung_cancer_type,
                small_cell_lung_ca_staging,
                non_small_cell_ca_type,
                staging,
                cancer_staging_date,
                metastasis_region,
                treatment_plan,
                other_treatment_plan,
                radiotherapy_sessions,
                surgery_date,
                chemotherapy_plan,
                chemo_start_date,
                chemo_stop_date,
                treatment_intent,
                chemo_regimen,
                chemo_cycle,
                total_planned_chemo_cycle,
                chemo_drug,
                dosage_in_milligrams,
                drug_route,
                other_drugs,
                purpose,
                referral_ordered,
                rtc,
                prev_encounter_datetime_lung_cancer_treatment,
                next_encounter_datetime_lung_cancer_treatment,
                prev_encounter_type_lung_cancer_treatment,
                next_encounter_type_lung_cancer_treatment,
                prev_clinical_datetime_lung_cancer_treatment,
                next_clinical_datetime_lung_cancer_treatment,
                prev_clinical_location_id_lung_cancer_treatment,
                next_clinical_location_id_lung_cancer_treatment,
                prev_clinical_rtc_date_lung_cancer_treatment,
                next_clinical_rtc_date_lung_cancer_treatment

                from flat_lung_cancer_treatment_3 t1
                join amrs.location t2 using (location_id))');

          PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
          DEALLOCATE PREPARE s1;

          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_lung_cancer_treatment_build_queue__0 t2 using (person_id);'); 
          PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
          DEALLOCATE PREPARE s1;  
                            
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
                
        if (@query_type="build") then
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
