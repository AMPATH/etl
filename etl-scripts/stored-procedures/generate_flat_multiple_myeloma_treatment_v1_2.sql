DELIMITER $$
CREATE PROCEDURE `generate_flat_multiple_myeloma_treatment_v1_2`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
    -- v1.1: Add diagnosis date, serum_m_protein, treatment_plan, other_treatment_plan, remission_plan and remission_start_date columns.
    --       Also modified encounter_datetime column to just encounter_date (removed the timestamp as per the myeloma team's request).
    --
    -- v1.2: Add identifiers, patient name and phone number columns.
    -- v1.3: Add death date and assessment notes columns.

    SET @primary_table := "flat_multiple_myeloma_treatment";
    SET @query_type := query_type;
                
    SET @total_rows_written := 0;
    SET @encounter_types := "(89,90,141)";
    SET @clinical_encounter_types := "(89,90,141)";
    SET @non_clinical_encounter_types := "(-1)";
    SET @other_encounter_types := "(-1)";
              
    SET @start := NOW();
    SET @table_version := "flat_multiple_myeloma_treatment_v1.2";

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
        identifiers VARCHAR(255),
        person_name VARCHAR(255),
        death_date DATE,
        phone_number VARCHAR(50),
        county VARCHAR(100),
        encounter_purpose INT,
        ecog_performance_index INT,
        bp_systolic DECIMAL(5, 1),
        bp_diastolic DECIMAL(5, 1),
        heart_rate DECIMAL(5, 1),
        respiratory_rate DECIMAL(5, 1),
        temperature DECIMAL(5, 1),
        weight_kg DECIMAL(5, 1),
        height_cm DECIMAL(5, 1),
        blood_oxygen_saturation DECIMAL(5, 1),
        body_mass_index DECIMAL(5, 1),
        body_surface_area DECIMAL(5, 1),
        diagnostic_method INT,
        diagnosis INT,
        diagnosis_date DATE,
        cancer_stage INT,
        overall_cancer_stage INT,
        ct_scan_head INT,
        ct_scan_neck INT,
        ct_scan_chest INT,
        ct_scan_spine INT,
        ct_scan_abdominal INT,
        ultrasound_renal INT,
        ultrasound_hepatic INT,
        obstetric_ultrasound INT,
        ultrasound_abdomen INT,
        breast_ultrasound INT,
        xray_shoulder INT,
        xray_pelvis INT,
        xray_abdomen INT,
        xray_skull INT,
        xray_leg INT,
        xray_hand INT,
        xray_foot INT,
        xray_chest INT,
        xray_arm INT,
        xray_spine INT,
        echo_test INT,
        mri_head INT,
        mri_neck INT,
        mri_arms INT,
        mri_chest INT,
        mri_spine INT,
        mri_abdominal INT,
        mri_pelvic INT,
        mri_legs INT,
        imaging_results_description VARCHAR(1000),
        clinical_media_caption_text VARCHAR(1000),
        other_imaging_results VARCHAR(1000),
        lab_test_ordered_for_next_visit INT,
        other_lab_test INT,
        red_blood_cells_count DECIMAL(5, 1),
        hemoglobin DECIMAL(5, 1),
        mean_corpuscular_volume DECIMAL(5, 1),
        mean_corpuscular_hemoglobin DECIMAL(5, 1),
        mean_cell_hemoglobin_concentration DeCIMAL(5, 1),
        red_cell_distribution_width DECIMAL(5, 1),
        platelets_count DECIMAL(5, 1),
        serum_white_blood_cells_count DECIMAL(5, 1),
        absolute_neutrophil_count DECIMAL(5, 1),
        serum_uric_acid_test DECIMAL(5, 1),
        serum_creatinine DECIMAL(5, 1),
        serum_sodium DECIMAL(5, 1),
        serum_potassium DECIMAL(5, 1),
        serum_chloride DECIMAL(5, 1),
        serum_albumin DECIMAL(5, 1),
        serum_alpha_one_globulin DECIMAL(5, 1),
        serum_alpha_two_globulin DECIMAL(5, 1),
        serum_beta_globulin DECIMAL(5, 1),
        serum_gamma_globulin DECIMAL(5, 1),
        serum_m_protein DECIMAL(5, 1),
        urine_alpha_one_globulin DECIMAL(5, 1),
        urine_alpha_two_globulin DECIMAL(5, 1),
        urine_beta_globulin DECIMAL(5, 1),
        urine_gamma_globulin DECIMAL(5, 1),
        urinary_albumin DECIMAL(5, 1),
        urine_m_protein DECIMAL(5, 1),
        kappa_light_chains DECIMAL(5, 1),
        kappa_lambda_ratio DECIMAL(5, 1),
        pus_cells_in_urine INT,
        protein_in_urine INT,
        leukocytes_in_urine INT,
        ketones_in_urine INT,
        glucose_in_urine INT,
        nitrites_in_urine INT,
        reticulocytes_percentage INT,
        serum_total_bilirubin DECIMAL(5, 1),
        serum_direct_bilirubin DECIMAL(5, 1),
        gamma_glutamyl_transferase DECIMAL(5, 1),
        serum_glutamic_oxaloacetic_transaminase DECIMAL(5, 1),
        serum_glutamic_pyruvic_transaminase DECIMAL(5, 1),
        serum_total_protein DECIMAL(5, 1),
        serum_alkaline_phospahatase DECIMAL(5, 1),
        serum_lactate_dehydrogenase DECIMAL(5, 1),
        lab_results_notes VARCHAR(255),
        oncology_treatment_plan INT,
        other_treatment_plan VARCHAR(255),
        remission_plan INT,
        remission_start_date DATE,
        mm_supportive_care_plan INT,
        mm_signs_symptoms INT,
        chemotherapy_plan INT,
        chemo_start_date DATETIME,
        chemo_cycle INT,
        reason_chemo_stop INT,
        chemotherapy_regimen VARCHAR(255),
        dosage_in_milligrams INT,
        drug_route INT,
        other_drugs INT,
        other_medication INT,
        reason_for_medication_use INT,
        assessment_notes VARCHAR(1000),
        education_given_today INT,
        referral_ordered INT,
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
        INDEX person_date (person_id , encounter_date),
        INDEX location_id_rtc_date (location_id , next_app_date),
        INDEX loc_id_enc_date_next_clinical (location_id , encounter_date , next_clinical_datetime_multiple_myeloma),
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
      
    CREATE TABLE IF NOT EXISTS flat_multiple_myeloma_treatment_sync_queue (
        person_id INT PRIMARY KEY
    );                            
                      
    SET @last_update := null;

    SELECT 
        MAX(date_updated)
    INTO @last_update FROM
        etl.flat_log
    WHERE
        table_name = @table_version;

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
    
    DROP TEMPORARY TABLE IF EXISTS multiple_myeloma_patient_identifiers;
              
	  SET @dyn_sql := CONCAT("CREATE TEMPORARY TABLE IF NOT EXISTS multiple_myeloma_patient_identifiers (SELECT p.person_id,
      GROUP_CONCAT(DISTINCT id.identifier
        SEPARATOR ', ') AS identifiers FROM ",
      @queue_table, " `p`
        LEFT JOIN
      amrs.patient_identifier `id` ON (p.person_id = id.patient_id
        AND (id.voided IS NULL || id.voided = 0))
       GROUP BY p.person_id);"); 
    
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
        SET @bp_systolic := null;
        SET @bp_diastolic := null;
        SET @heart_rate := null;
        SET @respiratory_rate := null;
        SET @temperature := null;
        SET @weight_kg := null;
        SET @height_cm := null;
        SET @blood_oxygen_saturation := null;
        SET @body_mass_index := null;
        SET @body_surface_area := null;
        SET @diagnostic_method := null;
        SET @diagnosis := null;
        SET @diagnosis_date := null;
        SET @death_date := null;
        SET @cancer_stage := null;
        SET @overall_cancer_stage := null;
        SET @ct_scan_head := null;
        SET @ct_scan_neck := null;
        SET @ct_scan_chest := null;
        SET @ct_scan_spine := null;
        SET @ct_scan_abdominal := null;
        SET @ultrasound_renal := null;
        SET @ultrasound_hepatic := null;
        SET @obstetric_ultrasound := null;
        SET @ultrasound_abdomen := null;
        SET @breast_ultrasound := null;
        SET @xray_shoulder := null;
        SET @xray_pelvis := null;
        SET @xray_abdomen := null;
        SET @xray_skull := null;
        SET @xray_leg := null;
        SET @xray_hand := null;
        SET @xray_foot := null;
        SET @xray_chest := null;
        SET @xray_arm := null;
        SET @xray_spine := null;
        SET @echo_test := null;
        SET @mri_head := null;
        SET @mri_neck := null;
        SET @mri_arms := null;
        SET @mri_chest := null;
        SET @mri_spine := null;
        SET @mri_abdominal := null;
        SET @mri_pelvic := null;
        SET @mri_legs := null;
        SET @imaging_results_description := null;
        SET @clinical_media_caption_text := null;
        SET @other_imaging_results := null;
        SET @lab_test_ordered_for_next_visit := null;
        SET @other_lab_test := null;
        SET @red_blood_cells_count := null;
        SET @hemoglobin := null;
        SET @mean_corpuscular_volume := null;
        SET @mean_corpuscular_hemoglobin := null;
        SET @mean_cell_hemoglobin_concentration := null;
        SET @red_cell_distribution_width := null;
        SET @platelets_count := null;
        SET @serum_white_blood_cells_count := null;
        SET @absolute_neutrophil_count := null;
        SET @serum_uric_acid_test := null;
        SET @serum_creatinine := null;
        SET @serum_sodium := null;
        SET @serum_potassium := null;
        SET @serum_chloride := null;
        SET @serum_albumin := null;
        SET @serum_alpha_one_globulin := null;
        SET @serum_alpha_two_globulin := null;
        SET @serum_beta_globulin := null;
        SET @serum_gamma_globulin := null;
        SET @serum_m_protein := null;
        SET @urine_alpha_one_globulin := null;
        SET @urine_alpha_two_globulin := null;
        SET @urine_beta_globulin := null;
        SET @urine_gamma_globulin := null;
        SET @urinary_albumin := null;
        SET @urine_m_protein := null;
        SET @kappa_light_chains := null;
        SET @kappa_lambda_ratio := null;
        SET @pus_cells_in_urine := null;
        SET @protein_in_urine := null;
        SET @leukocytes_in_urine := null;
        SET @ketones_in_urine := null;
        SET @glucose_in_urine := null;
        SET @nitrites_in_urine := null;
        SET @reticulocytes_percentage := null;
        SET @serum_total_bilirubin := null;
        SET @serum_direct_bilirubin := null;
        SET @gamma_glutamyl_transferase := null;
        SET @serum_glutamic_oxaloacetic_transaminase := null;
        SET @serum_glutamic_pyruvic_transaminase := null;
        SET @serum_total_protein := null;
        SET @serum_alkaline_phospahatase := null;
        SET @serum_lactate_dehydrogenase := null;
        SET @lab_results_notes := null;
        SET @oncology_treatment_plan := null;
        SET @other_treatment_plan := null;
        SET @remission_plan := null;
        SET @remission_start_date := null;
        SET @mm_supportive_care_plan := null;
        SET @mm_signs_symptoms := null;
        SET @chemotherapy_plan := null;
        SET @chemo_start_date := null;
        SET @chemo_cycle := null;
        SET @reason_chemo_stop := null;
        SET @chemotherapy_regimen := null;
        SET @dosage_in_milligrams := null;
        SET @drug_route := null;
        SET @other_drugs := null;
        SET @other_medication := null;
        SET @reason_for_medication_use := null;
        SET @assessment_notes := null;
        SET @education_given_today := null;
        SET @referral_ordered := null;
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
            CASE
                WHEN TIMESTAMPDIFF(YEAR, p.birthdate, curDATE()) > 0 THEN round(TIMESTAMPDIFF(YEAR, p.birthdate, curDATE()), 0)
                ELSE ROUND(TIMESTAMPDIFF(MONTH, p.birthdate, curDATE()) / 12, 2)
            END AS age,
            mmpi.identifiers,
            CONCAT(COALESCE(person_name.given_name, ''),
			          ' ',
            COALESCE(person_name.middle_name, ''),
                ' ',
            COALESCE(person_name.family_name, '')) AS person_name,
            p.death_date,
					  contacts.value AS phone_number,
            person_address.address1 as county,
            CASE
                WHEN obs REGEXP "!!1834=7850!!" THEN @encounter_purpose := 1 -- Initial visit
                WHEN obs REGEXP "!!1834=10037!!" THEN @encounter_purpose := 2 -- Second opinion
                WHEN obs REGEXP "!!1834=2345!!" THEN @encounter_purpose := 3 -- Follow-up
                WHEN obs REGEXP "!!1834=1068!!" THEN @encounter_purpose := 4 -- Symptomatic
                WHEN obs REGEXP "!!1834=1246!!" THEN @encounter_purpose := 5 -- Scheduled visit
                WHEN obs REGEXP "!!1834=5622!!" THEN @encounter_purpose := 6 -- Other (non-coded)
                ELSE @encounter_purpose := null
            END AS encounter_purpose,
            CASE
                WHEN obs REGEXP "!!6584=1115!!" THEN @ecog_performance_index := 1 -- Normal
                WHEN obs REGEXP "!!6584=6585!!" THEN @ecog_performance_index := 2 -- Symptomatic but ambulatory
                WHEN obs REGEXP "!!6584=6586!!" THEN @ecog_performance_index := 3 -- Debilitated, bedridden less than 50% of the day
                WHEN obs REGEXP "!!6584=6587!!" THEN @ecog_performance_index := 4 -- Debilitated, bedridden greater than 50% of the day
                WHEN obs REGEXP "!!6584=6588!!" THEN @ecog_performance_index := 5 -- Bedridden 100%
                ELSE @ecog_performance_index := null
            END AS ecog_performance_index,
            CASE
               WHEN obs REGEXP "!!5085=" THEN @bp_systolic := GetValues(obs, 5085)
               ELSE @bp_systolic := null
            END AS bp_systolic,
            CASE
               WHEN obs REGEXP "!!5086=" THEN @bp_diastolic := GetValues(obs, 5086)
               ELSE @bp_diastolic := null
            END AS bp_diastolic,
            CASE
               WHEN obs REGEXP "!!5087=" THEN @heart_rate := GetValues(obs, 5087)
               ELSE @heart_rate := null
            END AS heart_rate,
            CASE
               WHEN obs REGEXP "!!5242=" THEN @respiratory_rate := GetValues(obs, 5242)
               ELSE @respiratory_rate := null
            END AS respiratory_rate,
            CASE
               WHEN obs REGEXP "!!5088=" THEN @temperature := GetValues(obs, 5088)
               ELSE @temperature := null
            END AS temperature,
            CASE
               WHEN obs REGEXP "!!5089=" THEN @weight_kg := GetValues(obs, 5089)
               ELSE @weight_kg := null
            END AS weight_kg,
            CASE
               WHEN obs REGEXP "!!5090=" THEN @height_cm := GetValues(obs, 5090)
               ELSE @height_cm := null
            END AS height_cm,
            CASE
               WHEN obs REGEXP "!!5092=" THEN @blood_oxygen_saturation := GetValues(obs, 5092)
               ELSE @blood_oxygen_saturation := null
            END AS blood_oxygen_saturation,
            CASE
               WHEN obs REGEXP "!!1342=" THEN @body_mass_index := GetValues(obs, 1342)
               ELSE @body_mass_index := null
            END AS body_mass_index,
            CASE
               WHEN obs REGEXP "!!980=" THEN @body_surface_area := GetValues(obs, 980)
               ELSE @body_surface_area := null
            END AS body_surface_area,
            CASE
                WHEN obs REGEXP "!!6504=8594!!" THEN @diagnostic_method := 1 -- Positive myeloma screening test
                WHEN obs REGEXP "!!6504=6902!!" THEN @diagnostic_method := 2 -- Bone marrow aspiration
                WHEN obs REGEXP "!!6504=10142!!" THEN @diagnostic_method := 3 -- Hyperproteinemia
                ELSE @diagnostic_method := null
            END AS diagnostic_method,
            CASE
                WHEN obs REGEXP "!!6042=" THEN @diagnosis := GetValues(obs, 6042) 
                ELSE @diagnosis := null
            END AS diagnosis,
            CASE
                WHEN obs REGEXP "!!9728=" THEN @diagnosis_date := GetValues(obs, 9728)
            END AS diagnosis_date,
            CASE
                WHEN obs REGEXP "!!6582=1067!!" THEN @cancer_stage := 1 -- Unknown
                WHEN obs REGEXP "!!6582=10206!!" THEN @cancer_stage := 2 -- Durie-Salmon system
                WHEN obs REGEXP "!!6582=6566!!" THEN @cancer_stage := 3 -- Complete staging
                WHEN obs REGEXP "!!6582=1175!!" THEN @cancer_stage := 4 -- Not applicable
                ELSE @cancer_stage := null
            END AS cancer_stage,
            CASE
                WHEN obs REGEXP "!!9868=9852!!" THEN @overall_cancer_stage := 1 -- Stage I
                WHEN obs REGEXP "!!9868=9856!!" THEN @overall_cancer_stage := 2 -- Stage II
                WHEN obs REGEXP "!!9868=9860!!" THEN @overall_cancer_stage := 3 -- Stage III
                WHEN obs REGEXP "!!9868=9851!!" THEN @overall_cancer_stage := 4 -- Stage 0
                WHEN obs REGEXP "!!9868=9854!!" THEN @overall_cancer_stage := 5 -- Stage IB
                WHEN obs REGEXP "!!9868=9855!!" THEN @overall_cancer_stage := 6 -- Stage IC
                WHEN obs REGEXP "!!9868=9864!!" THEN @overall_cancer_stage := 7 -- Stage IV
                WHEN obs REGEXP "!!9868=9865!!" THEN @overall_cancer_stage := 8 -- Stage IVA
                ELSE @overall_cancer_stage := null
            END AS overall_cancer_stage,

            CASE 
                WHEN obs REGEXP "!!846=1115!!" THEN @ct_scan_head := 1 -- Normal
                WHEN obs REGEXP "!!846=1116!!" THEN @ct_scan_head := 2 -- Abnormal
                ELSE @ct_scan_head := null
            END AS ct_scan_head,
            CASE 
                WHEN obs REGEXP "!!9839=1115!!" THEN @ct_scan_neck := 1 -- Normal
                WHEN obs REGEXP "!!9839=1116!!" THEN @ct_scan_neck := 2 -- Abnormal
                ELSE @ct_scan_neck := null
            END AS ct_scan_neck,
            CASE 
                WHEN obs REGEXP "!!7113=1115!!" THEN @ct_scan_chest := 1 -- Normal
                WHEN obs REGEXP "!!7113=1116!!" THEN @ct_scan_chest := 2 -- Abnormal
                ELSE @ct_scan_chest := null
            END AS ct_scan_chest,
            CASE
                WHEN obs REGEXP "!!9840=1115!!" THEN @ct_scan_spine := 1 -- Normal
                WHEN obs REGEXP "!!9840=1116!!" THEN @ct_scan_spine := 2 -- Abnormal
                ELSE @ct_scan_spine := null
            END AS ct_scan_spine,
            CASE 
                WHEN obs REGEXP "!!7114=1115!!" THEN @ct_scan_abdominal := 1 -- Normal
                WHEN obs REGEXP "!!7114=1116!!" THEN @ct_scan_abdominal := 2 -- Abnormal
                ELSE @ct_scan_abdominal := null
            END AS ct_scan_abdominal,
            CASE
                WHEN obs REGEXP "!!7115=1115!!" THEN @ultrasound_renal := 1 -- Normal 
                WHEN obs REGEXP "!!7115=1116!!" THEN @ultrasound_renal := 2 -- Abnormal
                ELSE @ultrasound_renal := null
            END AS ultrasound_renal,
            CASE
                WHEN obs REGEXP "!!852=1115!!" THEN @ultrasound_hepatic := 1 -- Normal
                WHEN obs REGEXP "!!852=1116!!" THEN @ultrasound_hepatic := 2 -- Abnormal
                ELSE @ultrasound_hepatic := null
            END AS ultrasound_hepatic,
            CASE
                WHEN obs REGEXP "!!6221=1115!!" THEN @obstetric_ultrasound_ := 1 -- Normal
                WHEN obs REGEXP "!!6221=1116!!" THEN @obstetric_ultrasound := 2 -- Abnormal
                ELSE @obstetric_ultrasound := null
            END AS obstetric_ultrasound,
            CASE
                WHEN obs REGEXP "!!845=1115!!" THEN @ultrasound_abdomen := 1 -- Normal
                WHEN obs REGEXP "!!845=1116!!" THEN @ultrasound_abdomen := 2 -- Abnormal
                WHEN obs REGEXP "!!845=5103!!" THEN @ultrasound_abdomen := 3 -- Abdominal mass
                ELSE @ultrasound_abdomen := null
            END AS ultrasound_abdomen,
            CASE
                WHEN obs REGEXP "!!9596=1115!!" THEN @breast_ultrasound := 1 -- Normal
                WHEN obs REGEXP "!!9596=1116!!" THEN @breast_ultrasound := 2 -- Abnormal
                ELSE @breast_ultrasound := null
            END AS breast_ultrasound,
            CASE
                WHEN obs REGEXP "!!394=1115!!" THEN @xray_shoulder := 1 -- Normal 
                WHEN obs REGEXP "!!394=1116!!" THEN @xray_shoulder := 2 -- Abnormal
                ELSE @xray_shoulder := null
            END AS xray_shoulder,
            CASE
                WHEN obs REGEXP "!!392=1115!!" THEN @xray_pelvis := 1 -- Normal
                WHEN obs REGEXP "!!392=1116!!" THEN @xray_pelvis := 2 -- Abnormal
                ELSE @xray_pelvis := null
            END AS xray_pelvis,
            CASE
                WHEN obs REGEXP "!!101=1115!!" THEN @xray_abdomen := 1 -- Normal
                WHEN obs REGEXP "!!101=1116!!" THEN @xray_abdomen := 2 -- Abnormal
                ELSE @xray_abdomen := null
            END AS xray_abdomen,
            CASE
                WHEN obs REGEXP "!!386=1115!!" THEN @xray_skull := 1 -- Normal
                WHEN obs REGEXP "!!386=1116!!" THEN @xray_skull := 2 -- Abnormal
                ELSE @xray_skull := null
            END AS xray_skull,
            CASE
                WHEN obs REGEXP "!!380=1115!!" THEN @xray_leg := 1 -- Normal
                WHEN obs REGEXP "!!380=1116!!" THEN @xray_leg := 2 -- Abnormal
                ELSE @xray_leg := null
            END AS xray_leg,
            CASE
                WHEN obs REGEXP "!!382=1115!!" THEN @xray_hand := 1 -- Normal
                WHEN obs REGEXP "!!382=1116!!" THEN @xray_hand := 2 -- Abnormal
                ELSE @xray_hand := null
            END AS xray_hand,
            CASE
                WHEN obs REGEXP "!!384=1115!!" THEN @xray_foot := 1 -- Normal
                WHEN obs REGEXP "!!384=1116!!" THEN @xray_foot := 2 -- Abnormal
                ELSE @xray_foot := null
            END AS xray_foot,
            CASE
                WHEN obs REGEXP "!!12=1115!!" THEN @xray_chest := 1 -- Normal
                WHEN obs REGEXP "!!12=1116!!" THEN @xray_chest := 2 -- Abnormal
                ELSE @xray_chest := null
            END AS xray_chest,
            CASE
                WHEN obs REGEXP "!!377=1115!!" THEN @xray_arm := 1 -- Normal
                WHEN obs REGEXP "!!377=1116!!" THEN @xray_arm := 2 -- Abnormal
                ELSE @xray_arm := null
            END AS xray_arm,
            CASE
                WHEN obs REGEXP "!!390=1115!!" THEN @xray_spine := 1 -- Normal
                WHEN obs REGEXP "!!390=1116!!" THEN @xray_spine:= 2 -- Abnormal
                ELSE @xray_spine := null
            END AS xray_spine,
            CASE 
                WHEN obs REGEXP "!!1536=1115!!" THEN @echo_test := 1 -- Normal
                WHEN obs REGEXP "!!1536=1116!!" THEN @echo_test := 2 -- Abnormal
                WHEN obs REGEXP "!!1536=1538!!" THEN @echo_test := 3 -- Dilated cardiomyopathy
                WHEN obs REGEXP "!!1536=1532!!" THEN @echo_test := 4 -- Left ventricular hypertrophy
                WHEN obs REGEXP "!!1536=1533!!" THEN @echo_test := 5 -- Right ventricular hypertophy
                WHEN obs REGEXP "!!1536=5622!!" THEN @echo_test := 6 -- Other
                ELSE @echo_test := null
            END AS echo_test, 
            CASE
                WHEN obs REGEXP "!!9881=1115!!" THEN @mri_head := 1 -- Normal
                WHEN obs REGEXP "!!9881=1116!!" THEN @mri_head := 2 -- Abnormal
                ELSE @mri_head := null
            END AS mri_head,
            CASE
                WHEN obs REGEXP "!!9882=1115!!" THEN @mri_neck := 1 -- Normal
                WHEN obs REGEXP "!!9882=1116!!" THEN @mri_neck := 2 -- Abnormal
                ELSE @mri_neck := null
            END AS mri_neck,
            CASE
                WHEN obs REGEXP "!!9951=1115!!" THEN @mri_arms := 1 -- Normal
                WHEN obs REGEXP "!!9951=1116!!" THEN @mri_arms := 2 -- Abnormal
                ELSE @mri_arms := null
            END AS mri_arms,
            CASE
                WHEN obs REGEXP "!!9883=1115!!" THEN @mri_chest := 1 -- Normal
                WHEN obs REGEXP "!!9883=1116!!" THEN @mri_chest := 2 -- Abnormal
                ELSE @mri_chest := null
            END AS mri_chest,
            CASE
                WHEN obs REGEXP "!!9885=1115!!" THEN @mri_spine := 1 -- Normal
                WHEN obs REGEXP "!!9885=1116!!" THEN @mri_spine := 2 -- Abnormal
                ELSE @mri_spine := null
            END AS mri_spine,
            CASE
                WHEN obs REGEXP "!!9884=1115!!" THEN @mri_abdominal := 1 -- Normal
                WHEN obs REGEXP "!!9884=1116!!" THEN @mri_abdominal := 2 -- Abnormal
                ELSE @mri_abdominal := null
            END AS mri_abdominal,
            CASE
                WHEN obs REGEXP "!!9952=1115!!" THEN @mri_pelvic := 1 -- Normal
                WHEN obs REGEXP "!!9952=1116!!" THEN @mri_pelvic := 2 -- Abnormal
                ELSE @mri_pelvic := null
            END AS mri_pelvic,
            CASE
                WHEN obs REGEXP "!!9953=1115!!" THEN @mri_legs := 1 -- Normal
                WHEN obs REGEXP "!!9953=1116!!" THEN @mri_legs := 2 -- Abnormal
                ELSE @mri_legs := null
            END AS mri_legs,
            CASE
                WHEN obs REGEXP "!!10077=" THEN @imaging_results_description := GetValues(obs, 10077)
                ELSE @imaging_results_description := null
            END AS imaging_results_description,
            CASE
                WHEN obs REGEXP "!!9057=" THEN @clinical_media_caption_text := GetValues(obs, 9057)
                ELSE @clinical_media_caption_text := null
            END AS clinical_media_caption_text,
            CASE
                WHEN obs REGEXP "!!10124=" THEN @other_imaging_results := GetValues(obs, 10124)
                ELSE @other_imaging_results := null
            END AS other_imaging_results,          
            CASE
                WHEN obs REGEXP "!!6583=1107!!" THEN @lab_test_ordered_for_next_visit := 1 -- None
                WHEN obs REGEXP "!!6583=790!!" THEN @lab_test_ordered_for_next_visit := 2 -- Serum creatinine
                WHEN obs REGEXP "!!6583=1019!!" THEN @lab_test_ordered_for_next_visit := 3 -- Complete blood count
                WHEN obs REGEXP "!!6583=953!!" THEN @lab_test_ordered_for_next_visit := 4 -- Liver function tests
                WHEN obs REGEXP "!!6583=10205!!" THEN @lab_test_ordered_for_next_visit := 5 -- Serum free light chain test
                WHEN obs REGEXP "!!6583=8596!!" THEN @lab_test_ordered_for_next_visit := 6 -- Urine protein electrophoresis, 24hrs
                WHEN obs REGEXP "!!6583=8595!!" THEN @lab_test_ordered_for_next_visit := 7 -- Serum protein electrophoresis
                WHEN obs REGEXP "!!6583=5622!!" THEN @lab_test_ordered_for_next_visit := 8 -- Other (non-coded)
                WHEN obs REGEXP "!!6583=1327!!" THEN @lab_test_ordered_for_next_visit := 9 -- Reticulocytes %, microscopic exam
                WHEN obs REGEXP "!!6583=9009!!" THEN @lab_test_ordered_for_next_visit := 10 -- Hemoglobin with electrophoresis
                ELSE @lab_test_ordered_for_next_visit := null
            END AS lab_test_ordered_for_next_visit,
            CASE
                WHEN obs REGEXP "!!9538=" THEN @other_lab_test := GetValues(obs, 9538) 
                ELSE @other_lab_test := null
            END AS other_lab_test,

            CASE
                WHEN obs REGEXP "!!679=" THEN @red_blood_cells_count := GetValues(obs, 679)
                ELSE @red_blood_cells_count := null
            END AS red_blood_cells_count,

            CASE
                WHEN obs REGEXP "!!21=" THEN @hemoglobin := GetValues(obs, 21)
                ELSE @hemoglobin := null
            END AS hemoglobin,
            CASE
                WHEN obs REGEXP "!!851=" THEN @mean_corpuscular_volume := GetValues(obs, 851)
                ELSE @mean_corpuscular_volume := null
            END AS mean_corpuscular_volume,
            CASE
                WHEN obs REGEXP "!!1018=" THEN @mean_corpuscular_hemoglobin := GetValues(obs, 1018)
                ELSE @mean_corpuscular_hemoglobin := null
            END AS mean_corpuscular_hemoglobin,
            CASE
                WHEN obs REGEXP "!!1017=" THEN @mean_cell_hemoglobin_concentration := GetValues(obs, 1017)
                ELSE @mean_cell_hemoglobin_concentration := null
            END AS mean_cell_hemoglobin_concentration,
            CASE
                WHEN obs REGEXP "!!1016=" THEN @red_cell_distribution_width := GetValues(obs, 1016)
                ELSE @red_cell_distribution_width := null
            END AS red_cell_distribution_width,
            CASE
                WHEN obs REGEXP "!!729=" THEN @platelets_count := GetValues(obs, 729)
                ELSE @platelets_count := null
            END AS platelets_count,
            CASE
                WHEN obs REGEXP "!!678=" THEN @serum_white_blood_cells_count := GetValues(obs, 678)
                ELSE @serum_white_blood_cells_count := null
            END AS serum_white_blood_cells_count,
            CASE
                WHEN obs REGEXP "!!1330=" THEN @absolute_neutrophil_count := GetValues(obs, 1330)
                ELSE @absolute_neutrophil_count := null
            END AS absolute_neutrophil_count,
            CASE
                WHEN obs REGEXP "!!6134=" THEN @serum_uric_acid_test := GetValues(obs, 6134)
                ELSE @serum_uric_acid_test := null
            END AS serum_uric_acid_test,
            CASE
                WHEN obs REGEXP "!!790=" THEN @serum_creatinine := GetValues(obs, 790)
                ELSE @serum_creatinine := null
            END AS serum_creatinine,
            CASE
                WHEN obs REGEXP "!!1132=" THEN @serum_sodium := GetValues(obs, 1132)
                ELSE @serum_sodium := null
            END AS serum_sodium,
            CASE
                WHEN obs REGEXP "!!1133=" THEN @serum_potassium := GetValues(obs, 1133)
                ELSE @serum_potassium := null
            END AS serum_potassium,
            CASE
                WHEN obs REGEXP "!!1134=" THEN @serum_chloride := GetValues(obs, 1134)
                ELSE @serum_chloride := null
            END AS serum_chloride,
            CASE
                WHEN obs REGEXP "!!848=" THEN @serum_albumin := GetValues(obs, 848)
                ELSE @serum_albumin := null
            END AS serum_albumin,
            CASE
                WHEN obs REGEXP "!!8732=" THEN @serum_alpha_one_globulin := GetValues(obs, 8732)
                ELSE @serum_alpha_one_globulin := null
            END AS serum_alpha_one_globulin,
            CASE
                WHEN obs REGEXP "!!8733=" THEN @serum_alpha_two_globulin := GetValues(obs, 8733)
                ELSE @serum_alpha_two_globulin := null
            END AS serum_alpha_two_globulin,
            CASE
                WHEN obs REGEXP "!!8734=" THEN @serum_beta_globulin := GetValues(obs, 8734)
                ELSE @serum_beta_globulin := null
            END AS serum_beta_globulin,
            CASE
                WHEN obs REGEXP "!!8735=" THEN @serum_gamma_globulin := GetValues(obs, 8735)
                ELSE @serum_gamma_globulin := null
            END AS serum_gamma_globulin,
            CASE
                WHEN obs REGEXP "!!8731=" THEN @serum_m_protein := GetValues(obs, 8731)
                ELSE @serum_m_protein := null
            END AS serum_m_protein,
            CASE
                WHEN obs REGEXP "!!8737=" THEN @urine_alpha_one_globulin := GetValues(obs, 8737)
                ELSE @urine_alpha_one_globulin := null
            END AS urine_alpha_one_globulin,
            CASE
                WHEN obs REGEXP "!!8738=" THEN @urine_alpha_two_globulin := GetValues(obs, 8738)
                ELSE @urine_alpha_two_globulin := null
            END AS urine_alpha_two_globulin,
            CASE
                WHEN obs REGEXP "!!8739=" THEN @urine_beta_globulin := GetValues(obs, 8739)
                ELSE @urine_beta_globulin := null
            END AS urine_beta_globulin,
            CASE
                WHEN obs REGEXP "!!8740=" THEN @urine_gamma_globulin := GetValues(obs, 8740)
                ELSE @urine_gamma_globulin := null
            END AS urine_gamma_globulin,
            CASE
                WHEN obs REGEXP "!!849=" THEN @urinary_albumin := GetValues(obs, 849)
                ELSE @urinary_albumin := null
            END AS urinary_albumin,
            CASE
                WHEN obs REGEXP "!!8736=" THEN @urine_m_protein := GetValues(obs, 8736)
                ELSE @urine_m_protein := null
            END AS urine_m_protein,
            CASE
                WHEN obs REGEXP "!!10195=" THEN @kappa_light_chains := GetValues(obs, 10195)
                ELSE @kappa_light_chains := null
            END AS kappa_light_chains,
            CASE
                WHEN obs REGEXP "!!10197=" THEN @kappa_lambda_ratio := GetValues(obs, 10197)
                ELSE @kappa_lambda_ratio := null
            END AS kappa_lambda_ratio,
            CASE 
                WHEN obs REGEXP "!!1984=664!!" THEN @pus_cells_in_urine := 1 -- Negative
                WHEN obs REGEXP "!!1984=703!!" THEN @pus_cells_in_urine := 2 -- Positive
                WHEN obs REGEXP "!!1984=2074!!" THEN @pus_cells_in_urine := 3 -- Strong positive
                WHEN obs REGEXP "!!1984=2075!!" THEN @pus_cells_in_urine := 4 -- Stronger positive
                ELSE @pus_cells_in_urine := null
            END AS pus_cells_in_urine,
            CASE 
                WHEN obs REGEXP "!!2339=664!!" THEN @protein_in_urine := 1 -- Negative
                WHEN obs REGEXP "!!2339=703!!" THEN @protein_in_urine := 2 -- Positive
                WHEN obs REGEXP "!!2339=2074!!" THEN @protein_in_urine := 3 -- Strong positive
                WHEN obs REGEXP "!!2339=2075!!" THEN @protein_in_urine := 4 -- Stronger positive
                ELSE @protein_in_urine := null
            END AS protein_in_urine,
            CASE 
                WHEN obs REGEXP "!!6337=664!!" THEN @leukocytes_in_urine := 1 -- Negative
                WHEN obs REGEXP "!!6337=703!!" THEN @leukocytes_in_urine := 2  -- Positive
                WHEN obs REGEXP "!!6337=2074!!" THEN @leukocytes_in_urine := 3 -- Strong positive
                WHEN obs REGEXP "!!6337=2075!!" THEN @leukocytes_in_urine := 4 -- Stronger positive
                WHEN obs REGEXP "!!6337=2301!!" THEN @leukocytes_in_urine := 5 -- 1+
                WHEN obs REGEXP "!!6337=10900!!" THEN @leukocytes_in_urine := 6 -- Trace
                ELSE @leukocytes_in_urine := null
            END AS leukocytes_in_urine,
            CASE 
                WHEN obs REGEXP "!!7276=664!!" THEN @ketones_in_urine := 1 -- Negative
                WHEN obs REGEXP "!!7276=703!!" THEN @ketones_in_urine := 2 -- Positive
                WHEN obs REGEXP "!!7276=2074!!" THEN @ketones_in_urine := 3 -- Strong positive
                WHEN obs REGEXP "!!7276=2075!!" THEN @ketones_in_urine := 4 -- Stronger positive
                WHEN obs REGEXP "!!7276=1138!!" THEN @ketones_in_urine := 5 -- Indeterminate
                WHEN obs REGEXP "!!7276=10900!!" THEN @ketones_in_urine := 6 -- Trace
                WHEN obs REGEXP "!!7276=2301!!" THEN @ketones_in_urine := 7 -- 1+
                ELSE @ketones_in_urine := null
            END AS ketones_in_urine,
            CASE 
                WHEN obs REGEXP "!!2340=664!!" THEN @glucose_in_urine := 1 -- Negative
                WHEN obs REGEXP "!!2340=703!!" THEN @glucose_in_urine := 2 -- Positive
                WHEN obs REGEXP "!!2340=2074!!" THEN @glucose_in_urine := 3 -- Strong positive
                WHEN obs REGEXP "!!2340=2075!!" THEN @glucose_in_urine := 4 -- Stronger positive
                ELSE @glucose_in_urine := null
            END AS glucose_in_urine,
            CASE 
                WHEN obs REGEXP "!!9307=664!!" THEN @nitrites_in_urine := 1 -- Negative
                WHEN obs REGEXP "!!9307=703!!" THEN @nitrites_in_urine := 2 -- Positive
                WHEN obs REGEXP "!!9307=2074!!" THEN @nitrites_in_urine := 3 -- Strong positive
                WHEN obs REGEXP "!!9307=2075!!" THEN @nitrites_in_urine := 4 -- Stronger positive
                ELSE @nitrites_in_urine := null
            END AS nitrites_in_urine,
            CASE 
                WHEN obs REGEXP "!!1327=" THEN @reticulocytes_percentage := GetValues(obs, 1327)
                ELSE @reticulocytes_percentage := null
            END AS reticulocytes_percentage,
            CASE 
                WHEN obs REGEXP "!!655=" THEN @serum_total_bilirubin := GetValues(obs, 655)
                ELSE @serum_total_bilirubin := null
            END AS serum_total_bilirubin,
            CASE 
                WHEN obs REGEXP "!!1297=" THEN @serum_direct_bilirubin := GetValues(obs, 1297)
                ELSE @serum_direct_bilirubin := null
            END AS serum_direct_bilirubin,
            CASE 
                WHEN obs REGEXP "!!6123=" THEN @gamma_glutamyl_transferase := GetValues(obs, 6123)
                ELSE @gamma_glutamyl_transferase := null
            END AS gamma_glutamyl_transferase,
            CASE 
                WHEN obs REGEXP "!!653=" THEN @serum_glutamic_oxaloacetic_transaminase := GetValues(obs, 653)
                ELSE @serum_glutamic_oxaloacetic_transaminase := null
            END AS serum_glutamic_oxaloacetic_transaminase,
            CASE 
                WHEN obs REGEXP "!!654=" THEN @serum_glutamic_pyruvic_transaminase := GetValues(obs, 654)
                ELSE @serum_glutamic_pyruvic_transaminase := null
            END AS serum_glutamic_pyruvic_transaminase,
            CASE 
                WHEN obs REGEXP "!!717=" THEN @serum_total_protein := GetValues(obs, 717)
                ELSE @serum_total_protein := null
            END AS serum_total_protein,
            CASE 
                WHEN obs REGEXP "!!785=" THEN @serum_alkaline_phospahatase := GetValues(obs, 785)
                ELSE @serum_alkaline_phospahatase := null
            END AS serum_alkaline_phospahatase,
            CASE 
                WHEN obs REGEXP "!!1014=" THEN @serum_lactate_dehydrogenase := GetValues(obs, 1014)
                ELSE @serum_lactate_dehydrogenase := null
            END AS serum_lactate_dehydrogenase,
            CASE
                WHEN obs REGEXP "!!9538=" THEN @lab_results_notes := GetValues(obs, 9538)
                ELSE @lab_results_notes := null
            END AS lab_results_notes,
            CASE
                WHEN obs REGEXP "!!8723=10586!!" THEN @oncology_treatment_plan := 1 -- In remission
                WHEN obs REGEXP "!!8723=6575!!" THEN @oncology_treatment_plan := 2 -- Chemotherapy
                WHEN obs REGEXP "!!8723=1107!!" THEN @oncology_treatment_plan := 3 -- None
                WHEN obs REGEXP "!!8723=5622!!" THEN @oncology_treatment_plan := 4 -- Other (non-coded)
            END AS oncology_treatment_plan,
            CASE 
                WHEN obs REGEXP "!!10039=" THEN @other_treatment_plan := GetValues(obs, 10039)
            END AS other_treatment_plan,
            CASE
                WHEN obs REGEXP "!!10584=1260!!" THEN @remission_plan := 1 -- Stop all medications
                WHEN obs REGEXP "!!10584=10581!!" THEN @remission_plan := 2 -- Continue drug holiday
                WHEN obs REGEXP "!!10584=10582!!" THEN @remission_plan := 3 -- Start maintenance therapy
                WHEN obs REGEXP "!!10584=10583!!" THEN @remission_plan := 4 -- Continue maintenance therapy
            END AS remission_plan,
            CASE
                WHEN obs REGEXP "!!10585" THEN @remission_start_date := GetValues(obs, 10585)
            END AS remission_start_date,   
            CASE
                WHEN obs REGEXP "!!10198=88!!" THEN @mm_supportive_care_plan := 1 -- Aspirin
                WHEN obs REGEXP "!!10198=257!!" THEN @mm_supportive_care_plan := 2 -- Folic acid
                WHEN obs REGEXP "!!10198=8598!!" THEN @mm_supportive_care_plan := 3 -- Vitamin B12 supplement, injection
                WHEN obs REGEXP "!!10198=8597!!" THEN @mm_supportive_care_plan := 4 -- Vitamin D supplements
                WHEN obs REGEXP "!!10198=1195!!" THEN @mm_supportive_care_plan := 5 -- Antibiotics
                WHEN obs REGEXP "!!10198=8410!!" THEN @mm_supportive_care_plan := 6 -- Anticoagulant medication
                WHEN obs REGEXP "!!10198=7458!!" THEN @mm_supportive_care_plan := 7 -- IV fluid injection, drug route
                WHEN obs REGEXP "!!10198=8479!!" THEN @mm_supportive_care_plan := 8 -- Thalidomide
                WHEN obs REGEXP "!!10198=10140!!" THEN @mm_supportive_care_plan := 9 -- Zoledronic acid
                WHEN obs REGEXP "!!10198=7207!!" THEN @mm_supportive_care_plan := 10 -- Decadron
                WHEN obs REGEXP "!!10198=5622!!" THEN @mm_supportive_care_plan := 11 -- Other (non-coded)
                ELSE @mm_supportive_care_plan := null
            END AS mm_supportive_care_plan,
            CASE
                WHEN obs REGEXP "!!10170=10141!!" THEN @mm_signs_symptoms := 1 -- Hypercalcemia
                WHEN obs REGEXP "!!10170=1885!!" THEN @mm_signs_symptoms := 2 -- Renal failure
                WHEN obs REGEXP "!!10170=3!!" THEN @mm_signs_symptoms := 3 -- Anemia
                WHEN obs REGEXP "!!10170=8592!!" THEN @mm_signs_symptoms := 4 -- Lytic bone lesions
                WHEN obs REGEXP "!!10170=5978!!" THEN @mm_signs_symptoms := 5 -- Nausea
                WHEN obs REGEXP "!!10170=5949!!" THEN @mm_signs_symptoms := 6 -- Fatigue
                WHEN obs REGEXP "!!10170=5622!!" THEN @mm_signs_symptoms := 7 -- Other (non-coded) 
                ELSE @mm_signs_symptoms := null
            END AS mm_signs_symptoms,
            CASE
                WHEN obs REGEXP "!!9869=1256!!" THEN @chemotherapy_plan := 1 -- Start drugs
                WHEN obs REGEXP "!!9869=1259!!" THEN @chemotherapy_plan := 2 -- Change regimen
                WHEN obs REGEXP "!!9869=1260!!" THEN @chemotherapy_plan := 3 -- Stop all medications 
                WHEN obs REGEXP "!!9869=1257!!" THEN @chemotherapy_plan := 4 -- Continue regimen
                WHEN obs REGEXP "!!9869=6576!!" THEN @chemotherapy_plan := 5 -- Chemotherapy regimen modifications
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
                WHEN obs REGEXP "!!9927=1267!!" THEN @reason_chemo_stop := 1 -- Completed
                WHEN obs REGEXP "!!9927=7391!!" THEN @reason_chemo_stop := 2 -- Resistant
                WHEN obs REGEXP "!!9927=6629!!" THEN @reason_chemo_stop := 3 -- Progressive disease
                WHEN obs REGEXP "!!9927=6627!!" THEN @reason_chemo_stop := 4 -- Partial response
                WHEN obs REGEXP "!!9927=1879!!" THEN @reason_chemo_stop := 5 -- Toxicity, cause
                WHEN obs REGEXP "!!9927=5622!!" THEN @reason_chemo_stop := 6 -- Other (non-coded)
                ELSE @reason_chemo_stop := null
            END AS reason_chemo_stop,
            CASE
				        WHEN obs REGEXP "!!9918=" THEN @chemotherapy_regimen := normalize_chemo_drugs(obs, '9918')
                ELSE @chemotherapy_regimen := null
            END AS chemotherapy_regimen,
            CASE
                WHEN obs REGEXP "!!1899=[0-9]" THEN @dosage_in_milligrams := CAST(GetValues(obs, 1899) AS unsigned) 
                ELSE @dosage_in_milligrams := null
            END AS dosage_in_milligrams,
            CASE
                WHEN obs REGEXP "!!7463=7458!!" THEN @drug_route := 1 -- IV fluid injection
                WHEN obs REGEXP "!!7463=10078!!" THEN @drug_route := 2 -- Intrathecal
                WHEN obs REGEXP "!!7463=10079!!" THEN @drug_route := 3 -- Intra ommaya
                WHEN obs REGEXP "!!7463=7597!!" THEN @drug_route := 4 -- Subcutaneous injection
                WHEN obs REGEXP "!!7463=7581!!" THEN @drug_route := 5 -- Intramuscular injection
                WHEN obs REGEXP "!!7463=7609!!" THEN @drug_route := 6 -- Intrarterial injection
                WHEN obs REGEXP "!!7463=7616!!" THEN @drug_route := 7 -- Intradermal injection
                WHEN obs REGEXP "!!7463=7447!!" THEN @drug_route := 8 -- Oral administration
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
                WHEN obs REGEXP "!!2206=9220!!" THEN @reason_for_medication_use := 1 -- Adjuvant intent
                WHEN obs REGEXP "!!2206=8428!!" THEN @reason_for_medication_use := 2 -- Curative care
                ELSE @reason_for_medication_use := null
            END AS reason_for_medication_use,
            CASE 
                WHEN obs REGEXP "!!7222=" THEN @assessment_notes := GetValues(obs, 7222)
            END AS assessment_notes,
            CASE
                WHEN obs REGEXP "!!6327=8728!!" THEN @education_given_today := 1 -- None
                WHEN obs REGEXP "!!6327=8742!!" THEN @education_given_today := 2 -- Thalidomide and lenalidomide education
                WHEN obs REGEXP "!!6327=1905!!" THEN @education_given_today := 3 -- Physiotherapy services
                WHEN obs REGEXP "!!6327=10208!!" THEN @education_given_today := 4 -- Emergency return precaution
                WHEN obs REGEXP "!!6327=8730!!" THEN @education_given_today := 5 -- Pregnancy prevention
                WHEN obs REGEXP "!!6327=8371!!" THEN @education_given_today := 6 -- Treatment supporter preparation
                WHEN obs REGEXP "!!6327=5622!!" THEN @education_given_today := 7 -- Other (non-coded)
                ELSE @education_given_today := null
            END AS education_given_today,
            CASE
                WHEN obs REGEXP "!!1272=1107!!" THEN @referral_ordered := 1 -- None
                WHEN obs REGEXP "!!1272=8724!!" THEN @referral_ordered := 2 -- Palliative care
                WHEN obs REGEXP "!!1272=6571!!" THEN @referral_ordered := 3 -- Referral to surgery
                WHEN obs REGEXP "!!1272=1286!!" THEN @referral_ordered := 4 -- AMPATH
                WHEN obs REGEXP "!!1272=6572!!" THEN @referral_ordered := 5 -- Radiology
                WHEN obs REGEXP "!!1272=6573!!" THEN @referral_ordered := 6 -- Referral to pathology
                WHEN obs REGEXP "!!1272=1905!!" THEN @referral_ordered := 7 -- Physiotheraphy services
                WHEN obs REGEXP "!!1272=5622!!" THEN @referral_ordered := 8 -- Other (non-coded)
                ELSE @referral_ordered := null
            END AS referral_ordered,
            CASE
                WHEN obs REGEXP "!!5096=" THEN @next_app_date := GetValues(obs, 5096) 
                ELSE @next_app_date := null
            END AS next_app_date
        FROM 
            flat_multiple_myeloma_treatment_0 t1
        JOIN 
            amrs.person p using (person_id)
        LEFT JOIN 
			      amrs.person_name `person_name` ON (t1.person_id = person_name.person_id
            AND (person_name.voided IS NULL || person_name.voided = 0))
        LEFT JOIN 
			      multiple_myeloma_patient_identifiers `mmpi` ON t1.person_id = mmpi.person_id
        LEFT JOIN
            amrs.person_attribute `contacts` ON t1.person_id = contacts.person_id 
            AND (contacts.voided IS NULL || contacts.voided = 0) AND contacts.person_attribute_type_id = 10
        LEFT JOIN
            amrs.person_address `person_address` ON (t1.person_id = person_address.person_id)
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
INTO @new_encounter_rows FROM
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
                identifiers,
                person_name,
                death_date,
                phone_number,
                county,
                encounter_purpose,
                ecog_performance_index,
                bp_systolic,
                bp_diastolic,
                heart_rate,
                respiratory_rate,
                temperature,
                weight_kg,
                height_cm,
                blood_oxygen_saturation,
                body_mass_index,
                body_surface_area,
                diagnostic_method,
                diagnosis,
                diagnosis_date,
                cancer_stage,
                overall_cancer_stage,
                ct_scan_head,
                ct_scan_neck,
                ct_scan_chest,
                ct_scan_spine,
                ct_scan_abdominal,
                ultrasound_renal,
                ultrasound_hepatic,
                obstetric_ultrasound,
                ultrasound_abdomen,
                breast_ultrasound,
                xray_shoulder,
                xray_pelvis,
                xray_abdomen,
                xray_skull,
                xray_leg,
                xray_hand,
                xray_foot,
                xray_chest,
                xray_arm,
                xray_spine,
                echo_test,
                mri_head,
                mri_neck,
                mri_arms,
                mri_chest,
                mri_spine,
                mri_abdominal,
                mri_pelvic,
                mri_legs,
                imaging_results_description,
                clinical_media_caption_text,
                other_imaging_results,
                lab_test_ordered_for_next_visit,
                other_lab_test,
                red_blood_cells_count,
                hemoglobin,
                mean_corpuscular_volume,
                mean_corpuscular_hemoglobin,
                mean_cell_hemoglobin_concentration,
                red_cell_distribution_width,
                platelets_count,
                serum_white_blood_cells_count,
                absolute_neutrophil_count,
                serum_uric_acid_test,
                serum_creatinine,
                serum_sodium,
                serum_potassium,
                serum_chloride,
                serum_albumin,
                serum_alpha_one_globulin,
                serum_alpha_two_globulin,
                serum_beta_globulin,
                serum_gamma_globulin,
                serum_m_protein,
                urine_alpha_one_globulin,
                urine_alpha_two_globulin,
                urine_beta_globulin,
                urine_gamma_globulin,
                urinary_albumin,
                urine_m_protein,
                kappa_light_chains,
                kappa_lambda_ratio,
                pus_cells_in_urine,
                protein_in_urine,
                leukocytes_in_urine,
                ketones_in_urine,
                glucose_in_urine,
                nitrites_in_urine,
                reticulocytes_percentage,
                serum_total_bilirubin,
                serum_direct_bilirubin,
                gamma_glutamyl_transferase,
                serum_glutamic_oxaloacetic_transaminase,
                serum_glutamic_pyruvic_transaminase,
                serum_total_protein,
                serum_alkaline_phospahatase,
                serum_lactate_dehydrogenase,
                lab_results_notes,
                oncology_treatment_plan,
                other_treatment_plan,
                remission_plan,
                remission_start_date,
                mm_supportive_care_plan,
                mm_signs_symptoms,
                chemotherapy_plan,
                chemo_start_date,
                chemo_cycle,
                reason_chemo_stop,
                chemotherapy_regimen,
                dosage_in_milligrams,
                drug_route,
                other_drugs,
                other_medication,
                reason_for_medication_use,
                assessment_notes,
                education_given_today,
                referral_ordered,
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
SELECT 
    CONCAT(@start_write,
            ' : Writing ',
            @total_rows_to_write,
            ' to ',
            @primary_table);

        SET @dyn_sql := CONCAT('REPLACE INTO ', @primary_table, '(SELECT * FROM ', @write_table, ');');
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;
						
        SET @finish_write := NOW();
        SET @time_to_write := TIMESTAMPDIFF(SECOND, @start_write, @finish_write);
  
SELECT 
    CONCAT(@finish_write,
            ' : Completed writing rows. Time to write to primary table: ',
            @time_to_write,
            ' seconds ');
                        
        SET @dyn_sql := CONCAT('drop table ', @write_table, ';'); 
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;                      
    END IF;
							
		SET @ave_cycle_length := ceil(@total_time / @cycle_number);
SELECT 
    CONCAT('Average Cycle Length: ',
            @ave_cycle_length,
            ' second(s)');
				 
    SET @end := NOW();
    INSERT INTO etl.flat_log VALUES (@start, @last_date_created, @table_version, TIMESTAMPDIFF(SECOND, @start, @end));
SELECT 
    CONCAT(@table_version,
            ': Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');
END$$
DELIMITER ;
