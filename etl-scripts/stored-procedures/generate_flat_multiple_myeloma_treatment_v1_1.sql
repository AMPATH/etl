CREATE PROCEDURE `generate_flat_multiple_myeloma_treatment_v1_1`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
    -- v1.1: Add diagnosis date, serum_m_protein, treatment_plan, other_treatment_plan, remission_plan and remission_start_date columns.
    --       Also modified encounter_datetime column to just encounter_date (removed the timestamp as per the myeloma team's request).
    SET @primary_table := "flat_multiple_myeloma_treatment";
    SET @query_type := query_type;
                
    SET @total_rows_written := 0;
    SET @encounter_types := "(89,90)";
    SET @clinical_encounter_types := "(89,90)";
    SET @non_clinical_encounter_types := "(-1)";
    SET @other_encounter_types := "(-1)";
              
    SET @start := NOW();
    SET @table_version := "flat_multiple_myeloma_treatment_v1.1";

    SET session sort_buffer_size = 512000000;

    SET @sep := " ## ";
    SET @boundary := "!!";
    SET @last_date_created := (SELECT max(max_date_created) FROM etl.flat_obs);

    CREATE TABLE IF NOT EXISTS flat_multiple_myeloma_treatment (
        date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        person_id INT,
        encounter_id INT,
        encounter_type INT,
        encounter_date DATE,
        visit_id INT,
        location_id INT,
        gender CHAR(100),
        age INT,
        encounter_purpose INT,
        ecog_performance_index INT,
        diagnosis_method INT,
        diagnosis INT,
        diagnosis_date DATE,
        cancer_stage INT,
        overall_cancer_stage INT,
        lab_test_order INT,
        other_lab_test INT,
        serum_m_protein INT,
        treatment_plan INT,
        other_treatment_plan VARCHAR(255),
        remission_plan INT,
        remission_start_date DATE,
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
        INDEX person_date (person_id, encounter_date),
        INDEX location_id_rtc_date (location_id, next_app_date),
        INDEX loc_id_enc_date_next_clinical (location_id, encounter_date, next_clinical_datetime_multiple_myeloma),
        INDEX encounter_type (encounter_type),
        INDEX date_created (date_created)
    );
          
    IF (@query_type = "build") THEN
        SELECT 'BUILDING..........................................';               												

        SET @write_table := CONCAT("flat_multiple_myeloma_treatment_temp_",queue_number);
        SET @queue_table := CONCAT("flat_multiple_myeloma_treatment_build_queue_",queue_number);                    												
                    
        SET @dyn_sql := CONCAT('CREATE TABLE IF NOT EXISTS ', @write_table, ' LIKE ', @primary_table);
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  

        SET @dyn_sql := CONCAT('CREATE TABLE IF NOT EXISTS ', @queue_table, ' (SELECT * FROM flat_multiple_myeloma_treatment_build_queue limit ', queue_size, ');'); 
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
        
        SET @dyn_sql := CONCAT('DELETE t1 FROM flat_multiple_myeloma_treatment_build_queue t1 join ', @queue_table, ' t2 using (person_id);'); 
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
    END IF;
	
    IF (@query_type = "sync") THEN
        SELECT 'SYNCING..........................................';

        SET @write_table := "flat_multiple_myeloma_treatment";
        SET @queue_table := "flat_multiple_myeloma_treatment_sync_queue";
      
        CREATE TABLE IF NOT EXISTS flat_multiple_myeloma_treatment_sync_queue (person_id INT PRIMARY KEY);                            
                      
			  SET @last_update := null;

        SELECT MAX(date_updated) INTO @last_update FROM etl.flat_log WHERE table_name = @table_version;

        SELECT 'Finding patients in amrs.encounters...';
            REPLACE INTO flat_multiple_myeloma_treatment_sync_queue
            (SELECT distinct patient_id
              FROM amrs.encounter
              where date_changed > @last_update
            );
						
        SELECT 'Finding patients in flat_obs...';
            REPLACE INTO flat_multiple_myeloma_treatment_sync_queue
            (SELECT distinct person_id
              FROM etl.flat_obs
              where max_date_created > @last_update
            );

        SELECT 'Finding patients in flat_lab_obs...';
            REPLACE INTO flat_multiple_myeloma_treatment_sync_queue
            (SELECT distinct person_id
              FROM etl.flat_lab_obs
              where max_date_created > @last_update
            );

        SELECT 'Finding patients in flat_orders...';
            REPLACE INTO flat_multiple_myeloma_treatment_sync_queue
            (SELECT distinct person_id
              FROM etl.flat_orders
              where max_date_created > @last_update
            );
                            
            REPLACE INTO flat_multiple_myeloma_treatment_sync_queue
            (SELECT person_id 
              FROM amrs.person 
              where date_voided > @last_update
            );

            REPLACE INTO flat_multiple_myeloma_treatment_sync_queue
            (SELECT person_id 
              FROM amrs.person 
              where date_changed > @last_update
            );
    END IF;
                      
	  -- Remove test patients
		SET @dyn_sql := CONCAT('DELETE t1 FROM ', @queue_table,' t1
        JOIN amrs.person_attribute t2 USING (person_id)
        WHERE t2.person_attribute_type_id = 28 AND value = "true" AND voided = 0');
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  

    SET @person_ids_count := 0;
    SET @dyn_sql := CONCAT('SELECT COUNT(*) INTO @person_ids_count FROM ', @queue_table); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;

    SELECT @person_ids_count AS 'num patients to update';

    SET @dyn_sql := CONCAT('DELETE t1 FROM ', @primary_table, ' t1 JOIN ', @queue_table,' t2 USING (person_id);'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
                                    
    SET @total_time := 0;
    SET @cycle_number := 0;
                    
		WHILE @person_ids_count > 0 DO
        SET @loop_start_time := NOW();
                        
        -- Create temporary table with a set of person ids
        DROP TEMPORARY TABLE IF EXISTS flat_multiple_myeloma_treatment_build_queue__0;
						
        SET @dyn_sql := CONCAT('CREATE TEMPORARY TABLE flat_multiple_myeloma_treatment_build_queue__0 (person_id INT PRIMARY KEY) (SELECT * FROM ', @queue_table,' limit ', cycle_size,');'); 
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
                    
        DROP TEMPORARY TABLE IF EXISTS flat_multiple_myeloma_treatment_0a;
        SET @dyn_sql := CONCAT(
            'CREATE TEMPORARY TABLE flat_multiple_myeloma_treatment_0a
            (SELECT
                t1.person_id,
                t1.visit_id,
                t1.encounter_id,
                t1.encounter_datetime,
                t1.encounter_type,
                t1.location_id,
                t1.obs,
                t1.obs_datetimes,
                CASE
                  WHEN t1.encounter_type in ', @clinical_encounter_types, ' THEN 1
                  ELSE null
                END AS is_clinical_encounter,
                CASE
                  WHEN t1.encounter_type in ', @non_clinical_encounter_types, ' THEN 20
                  WHEN t1.encounter_type in ', @clinical_encounter_types, ' THEN 10
                  WHEN t1.encounter_type in', @other_encounter_types, ' THEN 5
                  ELSE 1
                END AS encounter_type_sort_index,
                t2.orders
            FROM etl.flat_obs t1
                JOIN 
                    flat_multiple_myeloma_treatment_build_queue__0 t0 USING (person_id)
                LEFT JOIN
                    etl.flat_orders t2 using (encounter_id)
            WHERE 
                t1.encounter_type in ', @encounter_types ,');'
        );
                            
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  

        INSERT INTO flat_multiple_myeloma_treatment_0a
        (SELECT
            t1.person_id,
            null,
            t1.encounter_id,
            t1.test_datetime,
            t1.encounter_type,
            null,
            t1.obs,
            null,
            -- in any visit, there many be multiple encounters. for this dataset, we want to include only clinical encounters (e.g. not lab or triage visit)
            0 as is_clinical_encounter,
            1 as encounter_type_sort_index,
            null
          FROM
            etl.flat_lab_obs t1
                JOIN
            flat_multiple_myeloma_treatment_build_queue__0 t0 USING (person_id)
        );

        DROP TEMPORARY TABLE IF EXISTS flat_multiple_myeloma_treatment_0;
        CREATE TEMPORARY TABLE flat_multiple_myeloma_treatment_0 (INDEX encounter_id (encounter_id), INDEX person_enc (person_id, encounter_datetime))
        (SELECT
            *
        FROM 
            flat_multiple_myeloma_treatment_0a
        ORDER BY
          person_id, DATE(encounter_datetime), encounter_type_sort_index
        );

        SET @encounter_purpose := null;
        SET @ecog_performance_index := null;
        SET @diagnosis_method := null;
        SET @diagnosis := null;
        SET @diagnosis_date := null;
        SET @cancer_stage := null;
        SET @overall_cancer_stage := null;
        SET @lab_test_order := null;
        SET @other_lab_test := null;
        SET @serum_m_protein := null; 
        SET @treatment_plan := null;
        SET @other_treatment_plan := null;
        SET @remission_plan := null;
        SET @remission_start_date := null;
        SET @mm_supportive_plan := null;
        SET @mm_signs_symptoms := null;
        SET @chemotherapy_plan := null;
        SET @chemo_start_date := null;
        SET @chemo_cycle := null;
        SET @reason_chemo_stop := null;
        SET @chemotherapy_drug := null;
        SET @dosage_in_milligrams := null;
        SET @drug_route := null;
        SET @other_drugs := null;
        SET @other_medication := null;
        SET @purpose := null;
        SET @education_given_today := null;
        SET @referral := null;
        SET @next_app_date := null;
                                              
        DROP TEMPORARY TABLE IF EXISTS flat_multiple_myeloma_treatment_1;

        CREATE TEMPORARY TABLE flat_multiple_myeloma_treatment_1
        (SELECT 
            obs,
            encounter_type_sort_index,
            @prev_id := @cur_id as prev_id,
            @cur_id := t1.person_id as cur_id,
            t1.person_id,
            t1.encounter_id,
            t1.encounter_type,
            t1.encounter_datetime,
            t1.visit_id,
            -- t4.name as location_name,
            t1.location_id,
            t1.is_clinical_encounter,
            p.gender,
            -- p.death_date,           
            CASE
                WHEN TIMESTAMPDIFF(YEAR, p.birthdate, curDATE()) > 0 THEN round(TIMESTAMPDIFF(YEAR, p.birthdate, curDATE()), 0)
                ELSE ROUND(TIMESTAMPDIFF(MONTH, p.birthdate, curDATE()) / 12, 2)
            END AS age,
            CASE
                WHEN obs REGEXP "!!1834=7850!!" THEN @encounter_purpose := 1
                WHEN obs REGEXP "!!1834=10037!!" THEN @encounter_purpose := 2
                WHEN obs REGEXP "!!1834=2345!!" THEN @encounter_purpose := 3
                WHEN obs REGEXP "!!1834=1068!!" THEN @encounter_purpose := 4
                WHEN obs REGEXP "!!1834=1246!!" THEN @encounter_purpose := 5
                ELSE @encounter_purpose := null
            END AS encounter_purpose,
            CASE
                WHEN obs REGEXP "!!6584=1115!!" THEN @ecog_performance_index := 1
                WHEN obs REGEXP "!!6584=6585!!" THEN @ecog_performance_index := 2
                WHEN obs REGEXP "!!6584=6586!!" THEN @ecog_performance_index := 3
                WHEN obs REGEXP "!!6584=6587!!" THEN @ecog_performance_index := 4
                WHEN obs REGEXP "!!6584=6588!!" THEN @ecog_performance_index := 5
                ELSE @ecog_performance_index := null
            END AS ecog_performance_index,
            CASE
                WHEN obs REGEXP "!!6504=8594!!" THEN @diagnosis_method := 1
                WHEN obs REGEXP "!!6504=6902!!" THEN @diagnosis_method := 2
                WHEN obs REGEXP "!!6504=10142!!" THEN @diagnosis_method := 3
                ELSE @diagnosis_method := null
            END AS diagnosis_method,
            CASE
                WHEN obs REGEXP "!!6042=" THEN @diagnosis := GetValues(obs, 6042) 
                ELSE @diagnosis := null
            END AS diagnosis,
            CASE
                WHEN obs REGEXP "!!9728=" THEN @diagnosis_date := GetValues(obs, 9728)
            END AS diagnosis_date,
            CASE
                WHEN obs REGEXP "!!6582=1067!!" THEN @cancer_stage := 1
                WHEN obs REGEXP "!!6582=10206!!" THEN @cancer_stage := 2
                WHEN obs REGEXP "!!6582=6566!!" THEN @cancer_stage := 3
                WHEN obs REGEXP "!!6582=1175!!" THEN @cancer_stage := 4
                ELSE @cancer_stage := null
            END AS cancer_stage,
            CASE
                WHEN obs REGEXP "!!9868=9852!!" THEN @overall_cancer_stage := 1
                WHEN obs REGEXP "!!9868=9856!!" THEN @overall_cancer_stage := 2
                WHEN obs REGEXP "!!9868=9860!!" THEN @overall_cancer_stage := 3
                ELSE @overall_cancer_stage := null
            END AS overall_cancer_stage,
            CASE
                WHEN obs REGEXP "!!6583=1107!!" THEN @lab_test_order := 1
                WHEN obs REGEXP "!!6583=790!!" THEN @lab_test_order := 2
                WHEN obs REGEXP "!!6583=1019!!" THEN @lab_test_order := 3
                WHEN obs REGEXP "!!6583=953!!" THEN @lab_test_order := 4
                WHEN obs REGEXP "!!6583=10205!!" THEN @lab_test_order := 5
                WHEN obs REGEXP "!!6583=8596!!" THEN @lab_test_order := 6
                WHEN obs REGEXP "!!6583=8595!!" THEN @lab_test_order := 7
                WHEN obs REGEXP "!!6583=5622!!" THEN @lab_test_order := 8
                ELSE @lab_test_order := null
            END AS lab_test_order,
            CASE
                WHEN obs REGEXP "!!9538=" THEN @other_lab_test := GetValues(obs, 9538) 
                ELSE @other_lab_test := null
            END AS other_lab_test,
            CASE
                WHEN obs REGEXP "!!8731=" THEN @serum_m_protein := GetValues(obs, 8731)
            END AS serum_m_protein,
            CASE
                WHEN obs REGEXP "!!8723=10586" THEN @treatment_plan := 1
                WHEN obs REGEXP "!!8723=6576" THEN @treatment_plan := 2
                WHEN obs REGEXP "!!8723=1107" THEN @treatment_plan := 3
                WHEN obs REGEXP "!!8723=5622" THEN @treatment_plan := 4
            END AS treatment_plan,
            CASE 
                WHEN obs REGEXP "!!10039=" THEN @other_treatment_plan := GetValues(obs, 10039)
            END AS other_treatment_plan,
            CASE
                WHEN obs REGEXP "!!10584=1260" THEN @remission_plan := 1
                WHEN obs REGEXP "!!10584=10581" THEN @remission_plan := 2
                WHEN obs REGEXP "!!10584=10582" THEN @remission_plan := 3
                WHEN obs REGEXP "!!10584=10583" THEN @remission_plan := 4
            END AS remission_plan,
            CASE
                WHEN obs REGEXP "!!10585" THEN @remission_start_date := GetValues(obs, 10585)
            END AS remission_start_date,   
            CASE
                WHEN obs REGEXP "!!10198=88!!" THEN @mm_supportive_plan := 1
                WHEN obs REGEXP "!!10198=257!!" THEN @mm_supportive_plan := 2
                WHEN obs REGEXP "!!10198=8598!!" THEN @mm_supportive_plan := 3
                WHEN obs REGEXP "!!10198=8597!!" THEN @mm_supportive_plan := 4
                WHEN obs REGEXP "!!10198=1195!!" THEN @mm_supportive_plan := 5
                WHEN obs REGEXP "!!10198=8410!!" THEN @mm_supportive_plan := 6
                WHEN obs REGEXP "!!10198=7458!!" THEN @mm_supportive_plan := 7
                WHEN obs REGEXP "!!10198=8479!!" THEN @mm_supportive_plan := 8
                WHEN obs REGEXP "!!10198=10140!!" THEN @mm_supportive_plan := 9
                WHEN obs REGEXP "!!10198=7207!!" THEN @mm_supportive_plan := 10
                WHEN obs REGEXP "!!10198=5622!!" THEN @mm_supportive_plan := 11
                ELSE @mm_supportive_plan := null
            END AS mm_supportive_plan,
            CASE
                WHEN obs REGEXP "!!10170=10141!!" THEN @mm_signs_symptoms := 1
                WHEN obs REGEXP "!!10170=1885!!" THEN @mm_signs_symptoms := 2
                WHEN obs REGEXP "!!10170=3!!" THEN @mm_signs_symptoms := 3
                WHEN obs REGEXP "!!10170=8592!!" THEN @mm_signs_symptoms := 4
                WHEN obs REGEXP "!!10170=5978!!" THEN @mm_signs_symptoms := 5
                WHEN obs REGEXP "!!10170=5949!!" THEN @mm_signs_symptoms := 6
                WHEN obs REGEXP "!!10170=5622!!" THEN @mm_signs_symptoms := 7
                ELSE @mm_signs_symptoms := null
            END AS mm_signs_symptoms,
            CASE
                WHEN obs REGEXP "!!9869=1256!!" THEN @chemotherapy_plan := 1
                WHEN obs REGEXP "!!9869=1259!!" THEN @chemotherapy_plan := 2
                WHEN obs REGEXP "!!9869=1260!!" THEN @chemotherapy_plan := 3
                WHEN obs REGEXP "!!9869=1257!!" THEN @chemotherapy_plan := 4
                WHEN obs REGEXP "!!9869=6576!!" THEN @chemotherapy_plan := 5
                ELSE @chemotherapy_plan := null
            END AS chemotherapy_plan,
            CASE
                WHEN obs REGEXP "!!1190=" THEN @chemo_start_date := GetValues(obs,1190) 
                ELSE @chemo_start_date := null
            END AS chemo_start_date,
            CASE
                WHEN obs REGEXP "!!6643=[0-9]" THEN @chemo_cycle := CAST(GetValues(obs,6643) AS unsigned)
                ELSE @chemo_cycle := null
            END AS chemo_cycle,
            CASE
                WHEN obs REGEXP "!!9927=1267!!" THEN @reason_chemo_stop := 1
                WHEN obs REGEXP "!!9927=7391!!" THEN @reason_chemo_stop := 2
                WHEN obs REGEXP "!!9927=6629!!" THEN @reason_chemo_stop := 3
                WHEN obs REGEXP "!!9927=6627!!" THEN @reason_chemo_stop := 4
                WHEN obs REGEXP "!!9927=1879!!" THEN @reason_chemo_stop := 5
                WHEN obs REGEXP "!!9927=5622!!" THEN @reason_chemo_stop := 6
                ELSE @reason_chemo_stop := null
            END AS reason_chemo_stop,
            CASE
                WHEN obs REGEXP "!!9918=491!!" THEN @chemotherapy_drug := 1
                WHEN obs REGEXP "!!9918=7207!!" THEN @chemotherapy_drug := 2
                WHEN obs REGEXP "!!9918=8486!!" THEN @chemotherapy_drug := 3
                WHEN obs REGEXP "!!9918=7203!!" THEN @chemotherapy_drug := 4
                WHEN obs REGEXP "!!9918=8479!!" THEN @chemotherapy_drug := 5
                WHEN obs REGEXP "!!9918=7213!!" THEN @chemotherapy_drug := 6
                WHEN obs REGEXP "!!9918=8480!!" THEN @chemotherapy_drug := 7
                WHEN obs REGEXP "!!9918=5622!!" THEN @chemotherapy_drug := 8
                ELSE @chemotherapy_drug := null
            END AS chemotherapy_drug,
            CASE
                WHEN obs REGEXP "!!1899=[0-9]" THEN @dosage_in_milligrams := CAST(GetValues(obs, 1899) AS unsigned) 
                ELSE @dosage_in_milligrams := null
            END AS dosage_in_milligrams,
            CASE
                WHEN obs REGEXP "!!7463=7458!!" THEN @drug_route := 1
                WHEN obs REGEXP "!!7463=10078!!" THEN @drug_route := 2
                WHEN obs REGEXP "!!7463=10079!!" THEN @drug_route := 3
                WHEN obs REGEXP "!!7463=7597!!" THEN @drug_route := 4
                WHEN obs REGEXP "!!7463=7581!!" THEN @drug_route := 5
                WHEN obs REGEXP "!!7463=7609!!" THEN @drug_route := 6
                WHEN obs REGEXP "!!7463=7616!!" THEN @drug_route := 7
                WHEN obs REGEXP "!!7463=7447!!" THEN @drug_route := 8
                ELSE @drug_route := null
            END AS drug_route,
            CASE
                WHEN obs REGEXP "!!1895=" THEN @other_drugs := GetValues(obs, 1895) 
                ELSE @other_drugs := null
            END AS other_drugs,
            CASE
                WHEN obs REGEXP "!!1779=" THEN @other_medication := GetValues(obs, 1779) 
                ELSE @other_medication := null
            END AS other_medication,
            CASE
                WHEN obs REGEXP "!!2206=9220!!" THEN @purpose := 1
                WHEN obs REGEXP "!!2206=8428!!" THEN @purpose := 2
                ELSE @purpose := null
            END AS purpose,
            CASE
                WHEN obs REGEXP "!!6327=8728!!" THEN @education_given_today := 1
                WHEN obs REGEXP "!!6327=8742!!" THEN @education_given_today := 2
                WHEN obs REGEXP "!!6327=1905!!" THEN @education_given_today := 3
                WHEN obs REGEXP "!!6327=10208!!" THEN @education_given_today := 4
                WHEN obs REGEXP "!!6327=8730!!" THEN @education_given_today := 5
                WHEN obs REGEXP "!!6327=8371!!" THEN @education_given_today := 6
                WHEN obs REGEXP "!!6327=5622!!" THEN @education_given_today := 7
                ELSE @education_given_today := null
            END AS education_given_today,
            CASE
                WHEN obs REGEXP "!!1272=1107!!" THEN @referral := 1
                WHEN obs REGEXP "!!1272=8724!!" THEN @referral := 2
                WHEN obs REGEXP "!!1272=6571!!" THEN @referral := 3
                WHEN obs REGEXP "!!1272=1286!!" THEN @referral := 4
                WHEN obs REGEXP "!!1272=6572!!" THEN @referral := 5
                WHEN obs REGEXP "!!1272=6573!!" THEN @referral := 6
                WHEN obs REGEXP "!!1272=1905!!" THEN @referral := 7
                WHEN obs REGEXP "!!1272=5622!!" THEN @referral := 8
                ELSE @referral := null
            END AS referral,
            CASE
                WHEN obs REGEXP "!!5096=" THEN @next_app_date := GetValues(obs, 5096) 
                ELSE @next_app_date := null
            END AS next_app_date
        FROM 
            flat_multiple_myeloma_treatment_0 t1
        JOIN 
            amrs.person p using (person_id)
        ORDER BY
            person_id, DATE(encounter_datetime) DESC, encounter_type_sort_index DESC
        );

          SET @prev_id := null;
          SET @cur_id := null;
          SET @prev_encounter_datetime := null;
          SET @cur_encounter_datetime := null;

          SET @prev_clinical_datetime := null;
          SET @cur_clinical_datetime := null;

          SET @next_encounter_type := null;
          SET @cur_encounter_type := null;

          SET @prev_clinical_location_id := null;
          SET @cur_clinical_location_id := null;

          ALTER TABLE flat_multiple_myeloma_treatment_1 DROP prev_id, DROP cur_id;

          DROP TABLE IF EXISTS flat_multiple_myeloma_treatment_2;
          CREATE TEMPORARY TABLE flat_multiple_myeloma_treatment_2
          (SELECT 
              *,
              @prev_id := @cur_id as prev_id,
              @cur_id := person_id as cur_id,
              CASE
                  WHEN @prev_id = @cur_id THEN @prev_encounter_datetime := @cur_encounter_datetime
                  ELSE @prev_encounter_datetime := null
              END AS next_encounter_datetime_multiple_myeloma,
              @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,
              CASE
                  when @prev_id = @cur_id THEN @next_encounter_type := @cur_encounter_type
                  ELSE @next_encounter_type := null
              END AS next_encounter_type_multiple_myeloma,
              @cur_encounter_type := encounter_type as cur_encounter_type,
              CASE
                  when @prev_id = @cur_id THEN @prev_clinical_datetime := @cur_clinical_datetime
                  ELSE @prev_clinical_datetime := null
              END AS next_clinical_datetime_multiple_myeloma,
              CASE
                  when @prev_id = @cur_id THEN @prev_clinical_location_id := @cur_clinical_location_id
                  ELSE @prev_clinical_location_id := null
              END AS next_clinical_location_id_multiple_myeloma,
              CASE
                  when @prev_id = @cur_id THEN @prev_clinical_rtc_date := @cur_clinical_rtc_date
                  ELSE @prev_clinical_rtc_date := null
              END AS next_clinical_rtc_date_multiple_myeloma,
              CASE
                  when is_clinical_encounter THEN @cur_clinical_rtc_date := next_app_date
                  when @prev_id = @cur_id THEN @cur_clinical_rtc_date
                  ELSE @cur_clinical_rtc_date:= null
              END AS cur_clinical_rtc_date
          FROM
              flat_multiple_myeloma_treatment_1
          ORDER BY
              person_id, DATE(encounter_datetime) DESC, encounter_type_sort_index DESC
          );

          ALTER TABLE flat_multiple_myeloma_treatment_2 DROP prev_id, DROP cur_id, DROP cur_encounter_type, DROP cur_encounter_datetime, DROP cur_clinical_rtc_date;

          SET @prev_id := null;
          SET @cur_id := null;
          SET @prev_encounter_type := null;
          SET @cur_encounter_type := null;
          SET @prev_encounter_datetime := null;
          SET @cur_encounter_datetime := null;
          SET @prev_clinical_datetime := null;
          SET @cur_clinical_datetime := null;
          SET @prev_clinical_location_id := null;
          SET @cur_clinical_location_id := null;

          DROP TEMPORARY TABLE IF EXISTS flat_multiple_myeloma_treatment_3;
          CREATE TEMPORARY TABLE flat_multiple_myeloma_treatment_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime DESC))
          (SELECT
              *,
              @prev_id := @cur_id as prev_id,
              @cur_id := t1.person_id as cur_id,
              CASE
                  when @prev_id = @cur_id THEN @prev_encounter_type := @cur_encounter_type
                  ELSE @prev_encounter_type := null
              END AS prev_encounter_type_multiple_myeloma,	
              @cur_encounter_type := encounter_type as cur_encounter_type,
              CASE
                  when @prev_id = @cur_id THEN @prev_encounter_datetime := @cur_encounter_datetime
                  ELSE @prev_encounter_datetime := null
              END AS prev_encounter_datetime_multiple_myeloma,
              @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,
              CASE
                  when @prev_id = @cur_id THEN @prev_clinical_datetime := @cur_clinical_datetime
                  ELSE @prev_clinical_datetime := null
              END AS prev_clinical_datetime_multiple_myeloma,
              CASE
                  when @prev_id = @cur_id THEN @prev_clinical_location_id := @cur_clinical_location_id
                  ELSE @prev_clinical_location_id := null
              END AS prev_clinical_location_id_multiple_myeloma,
              CASE
                  when @prev_id = @cur_id THEN @prev_clinical_rtc_date := @cur_clinical_rtc_date
                  ELSE @prev_clinical_rtc_date := null
              END AS prev_clinical_rtc_date_multiple_myeloma,
              CASE
                  when is_clinical_encounter THEN @cur_clinical_rtc_date := next_app_date
                  when @prev_id = @cur_id THEN @cur_clinical_rtc_date
                  ELSE @cur_clinical_rtc_date:= null
              END AS cur_clinic_rtc_date
          FROM
              flat_multiple_myeloma_treatment_2 t1
          ORDER BY 
              person_id, DATE(encounter_datetime), encounter_type_sort_index
          );

        SELECT 
            COUNT(*)
        INTO 
            @new_encounter_rows
        FROM
            flat_multiple_myeloma_treatment_3;
                  
        SELECT @new_encounter_rows;                    
        SET @total_rows_written := @total_rows_written + @new_encounter_rows;
        SELECT @total_rows_written;

        SET @dyn_sql := CONCAT('REPLACE INTO ', @write_table,											  
          '(SELECT
                null,
                person_id,
                encounter_id,
                encounter_type,
                DATE(t1.encounter_datetime) AS encounter_date,
                visit_id,
                location_id,
                gender,
                age,
                encounter_purpose,
                ecog_performance_index,
                diagnosis_method,
                diagnosis,
                diagnosis_date,
                cancer_stage,
                overall_cancer_stage,
                lab_test_order,
                other_lab_test,
                serum_m_protein,
                treatment_plan,
                other_treatment_plan,
                remission_plan,
                remission_start_date,
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
            FROM 
                flat_multiple_myeloma_treatment_3 t1
            JOIN   
                amrs.location t2 USING (location_id))'
        );

        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;

        SET @dyn_sql := CONCAT('DELETE t1 FROM ', @queue_table, ' t1 JOIN flat_multiple_myeloma_treatment_build_queue__0 t2 USING (person_id);'); 
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
                  
                  
        -- SELECT @person_ids_count := (SELECT count(*) FROM flat_breast_cancer_screening_build_queue_2);                        
        SET @dyn_sql := CONCAT('SELECT COUNT(*) INTO @person_ids_count FROM ', @queue_table, ';'); 
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
                  
        -- SELECT @person_ids_count as remaining_in_build_queue;

        SET @cycle_length := TIMESTAMPDIFF(SECOND, @loop_start_time, NOW());
        -- SELECT CONCAT('Cycle time: ',@cycle_length,' seconds');                    
        SET @total_time := @total_time + @cycle_length;
        SET @cycle_number := @cycle_number + 1;
        -- SELECT ceil(@person_ids_count / cycle_size) as remaining_cycles;
        SET @remaining_time := ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);

        SELECT 
            @person_ids_count AS 'persons remaining',
            @cycle_length AS 'Cycle time (s)',
            CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
            @remaining_time AS 'Est time remaining (min)';

	  END WHILE;
                 
    IF (@query_type = "build") THEN
        SET @dyn_sql := CONCAT('DROP TABLE ', @queue_table, ';'); 
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
                        
        SET @total_rows_to_write := 0;
        SET @dyn_sql := CONCAT("SELECT COUNT(*) INTO @total_rows_to_write FROM ", @write_table);
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;
                                                
        SET @start_write := NOW();
        SELECT CONCAT(@start_write, ' : Writing ', @total_rows_to_write, ' to ', @primary_table);

        SET @dyn_sql := CONCAT('REPLACE INTO ', @primary_table, '(SELECT * FROM ', @write_table, ');');
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;
						
        SET @finish_write := NOW();
        SET @time_to_write := TIMESTAMPDIFF(SECOND, @start_write, @finish_write);
  
        SELECT CONCAT(@finish_write, ' : Completed writing rows. Time to write to primary table: ', @time_to_write, ' seconds ');
                        
        SET @dyn_sql := CONCAT('drop table ', @write_table, ';'); 
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;                      
    END IF;
							
		SET @ave_cycle_length := ceil(@total_time / @cycle_number);
    SELECT CONCAT('Average Cycle Length: ', @ave_cycle_length, ' second(s)');
				 
    SET @end := NOW();
    INSERT INTO etl.flat_log VALUES (@start, @last_date_created, @table_version, TIMESTAMPDIFF(SECOND, @start, @end));
    SELECT CONCAT(@table_version, ': Time to complete: ', TIMESTAMPDIFF(MINUTE, @start, @end), ' minutes');
END