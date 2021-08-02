CREATE PROCEDURE `generate_flat_cervical_cancer_screening_v1_2`(IN query_type VARCHAR(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
  SET @primary_table := "flat_cervical_cancer_screening";
  SET @query_type := query_type;
  SET @total_rows_written := 0;
  
  SET @encounter_types := "(69,70,147)";
  SET @clinical_encounter_types := "(69,70,147)";
  SET @non_clinical_encounter_types := "(-1)";
  SET @other_encounter_types := "(-1)";
                  
  SET @start := now();
  SET @table_version := "flat_cervical_cancer_screening_v1.2";

  SET session sort_buffer_size := 512000000;

  SET @sep := " ## ";
  SET @boundary := "!!";
  SET @last_date_created := (SELECT MAX(max_date_created) FROM etl.flat_obs);

  CREATE TABLE IF NOT EXISTS flat_cervical_cancer_screening (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    person_id INT,
    encounter_id INT,
    encounter_type INT,
    encounter_datetime DATETIME,
    visit_id INT,
    location_id INT,
    location_uuid VARCHAR(100),
    age INT,
    -- CERVICAL CANCER SCREENING FORM (enc type 69)
    cur_visit_type INT,
    reasons_for_current_visit TINYINT,
    actual_scheduled_visit_date DATETIME,
    gravida TINYINT,
    parity TINYINT,
    menstruation_status TINYINT,
    last_menstrual_period_date DATETIME,
    pregnancy_status TINYINT,
    estimated_delivery_date DATETIME,
    last_delivery_date DATETIME,
    reason_not_pregnant TINYINT,
    hiv_status TINYINT,
    enrolled_in_hiv_care TINYINT,
    primary_care_facility INT,
    primary_care_facility_non_ampath VARCHAR(70),
    prior_via_done TINYINT,
    prior_via_result TINYINT,
    prior_via_date DATETIME,
    screening_method VARCHAR(150),
    via_or_via_vili_test_result TINYINT,
    observations_from_positive_via_or_via_vili_test VARCHAR(200),
    visual_impression_cervix TINYINT,
    visual_impression_vagina TINYINT,
    visual_impression_vulva TINYINT,
    hpv_test_result TINYINT,
    hpv_type TINYINT,
    pap_smear_test_result TINYINT,
    procedures_done_this_visit TINYINT,
    treatment_method TINYINT,
    other_treatment_method_non_coded VARCHAR(100),
    leep_status TINYINT,
    cryotherapy_status TINYINT,
    thermocoagulation_status TINYINT,
    screening_assessment_notes VARCHAR(500),
    screening_follow_up_plan TINYINT,
    screening_rtc_date DATETIME,
    -- DYSPLASIA FORM (enc type 70)
    dysp_visit_type TINYINT,
    dysp_history TINYINT,
    dysp_past_via_result TINYINT,
    dysp_past_via_result_date DATETIME,
    dysp_past_pap_smear_result VARCHAR(200),
    dysp_past_biopsy_result TINYINT,
    dysp_past_biopsy_result_date DATETIME,
    dysp_past_biopsy_result_non_coded VARCHAR(200),
    dysp_past_treatment VARCHAR(100),
    dysp_past_treatment_specimen_pathology VARCHAR(100),
    dysp_satisfactory_colposcopy TINYINT,
    dysp_colposcopy_findings VARCHAR(250),
    dysp_cervical_lesion_size TINYINT,
    dysp_visual_impression_cervix TINYINT,
    dysp_visual_impression_vagina TINYINT,
    dysp_visual_impression_vulva TINYINT,
    dysp_procedure_done VARCHAR(250),
    dysp_management_plan VARCHAR(150),
    dysp_assessment_notes VARCHAR(500),
    dysp_rtc_date DATETIME,
    -- GYN PATHOLOGY RESULTS FORM (enc type 147)
    gynp_pap_smear_results TINYINT,
    gynp_pap_smear_results_date DATETIME,
    gynp_biopsy_sample_collection_date DATETIME,
    gynp_diagnosis_date DATETIME,
    gynp_date_patient_notified_of_results DATETIME,
    gynp_procedure_done TINYINT,
    gynp_biopsy_cervix_result TINYINT,
    gynp_leep_location TINYINT,
    gynp_vagina_result TINYINT,
    gynp_vulva_result TINYINT,
    gynp_endometrium_result TINYINT,
    gynp_ecc_result TINYINT,
    gynp_lab_test_non_coded VARCHAR(150),
    gynp_date_patient_informed_and_referred DATETIME,
    gynp_pathology_management_plan VARCHAR(300),
    gynp_assessment_notes VARCHAR(500),
    gynp_rtc_date DATETIME,
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
    INDEX location_id_rtc_date (location_id , screening_rtc_date),
    INDEX location_uuid_rtc_date (location_uuid , screening_rtc_date),
    INDEX loc_id_enc_date_next_clinical (location_id , encounter_datetime , next_clinical_datetime_cervical_cancer_screening),
    INDEX encounter_type (encounter_type),
    INDEX date_created (date_created)
  );
                        
  IF (@query_type = "build") THEN
  	SELECT 'BUILDING..........................................';              												
      SET @write_table := concat("flat_cervical_cancer_screening_temp_",queue_number);
      SET @queue_table := concat("flat_cervical_cancer_screening_build_queue_", queue_number);                    												
            
      SET @dyn_sql := CONCAT('create table if not exists ', @write_table,' like ', @primary_table);
      PREPARE s1 FROM @dyn_sql; 
      EXECUTE s1; 
      DEALLOCATE PREPARE s1;  

      SET @dyn_sql := CONCAT('Create table if not exists ', @queue_table, ' (SELECT * FROM flat_cervical_cancer_screening_build_queue limit ', queue_size, ');'); 
      PREPARE s1 FROM @dyn_sql; 
      EXECUTE s1; 
      DEALLOCATE PREPARE s1;  
      
      SET @dyn_sql := CONCAT('delete t1 FROM flat_cervical_cancer_screening_build_queue t1 JOIN ', @queue_table, ' t2 using (person_id);'); 
      PREPARE s1 FROM @dyn_sql; 
      EXECUTE s1; 
      DEALLOCATE PREPARE s1;  
  END IF;
	
  IF (@query_type = "sync") THEN
      SELECT 'SYNCING..........................................';
      SET @write_table := "flat_cervical_cancer_screening";
      SET @queue_table := "flat_cervical_cancer_screening_sync_queue";

    CREATE TABLE IF NOT EXISTS flat_cervical_cancer_screening_sync_queue (
      person_id INT PRIMARY KEY
    );                            
                            
    SET @last_update := null;

    SELECT 
        MAX(date_updated)
    INTO @last_update FROM
        etl.flat_log
    WHERE
      table_name = @table_version;										

    REPLACE INTO flat_cervical_cancer_screening_sync_queue
    (SELECT DISTINCT patient_id
      FROM amrs.encounter
      WHERE date_changed > @last_update
    );

    REPLACE INTO flat_cervical_cancer_screening_sync_queue
    (SELECT DISTINCT person_id
      FROM etl.flat_obs
      WHERE max_date_created > @last_update
    );

    REPLACE INTO flat_cervical_cancer_screening_sync_queue
    (SELECT DISTINCT person_id
      FROM etl.flat_lab_obs
      WHERE max_date_created > @last_update
    );

    REPLACE INTO flat_cervical_cancer_screening_sync_queue
    (SELECT DISTINCT person_id
      FROM etl.flat_orders
      WHERE max_date_created > @last_update
    );
                  
    REPLACE INTO flat_cervical_cancer_screening_sync_queue
    (SELECT person_id FROM 
      amrs.person 
      WHERE date_voided > @last_update);


    REPLACE INTO flat_cervical_cancer_screening_sync_queue
    (SELECT person_id FROM 
      amrs.person 
      WHERE date_changed > @last_update);
  END IF;
                      
  -- Remove test patients
  SET @dyn_sql := CONCAT('delete t1 FROM ', @queue_table,' t1
      JOIN amrs.person_attribute t2 using (person_id)
      WHERE t2.person_attribute_type_id=28 AND value="true" AND voided=0');
  PREPARE s1 FROM @dyn_sql; 
  EXECUTE s1; 
  DEALLOCATE PREPARE s1;  

  SET @person_ids_count := 0;
  SET @dyn_sql := CONCAT('SELECT count(*) into @person_ids_count FROM ', @queue_table); 
  PREPARE s1 FROM @dyn_sql; 
  EXECUTE s1; 
  DEALLOCATE PREPARE s1;

  SELECT @person_ids_count AS 'num patients to update';

  SET @dyn_sql := CONCAT('delete t1 FROM ',@primary_table, ' t1 JOIN ',@queue_table,' t2 using (person_id);'); 
  PREPARE s1 FROM @dyn_sql; 
  EXECUTE s1; 
  DEALLOCATE PREPARE s1;  
                                  
  SET @total_time := 0;
  SET @cycle_number := 0;
                  
  WHILE @person_ids_count > 0 DO
    SET @loop_start_time = now();
                    
    DROP TEMPORARY TABLE IF EXISTS flat_cervical_cancer_screening_build_queue__0;
        
    SET @dyn_sql=CONCAT('CREATE TEMPORARY TABLE flat_cervical_cancer_screening_build_queue__0 (person_id int primary key) (SELECT * FROM ',@queue_table,' limit ',cycle_size,');'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
                
    DROP TEMPORARY TABLE IF EXISTS flat_cervical_cancer_screening_0a;
    SET @dyn_sql = CONCAT(
      'CREATE TEMPORARY TABLE flat_cervical_cancer_screening_0a
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
            WHEN t1.encounter_type in ', @clinical_encounter_types,' THEN 1
            ELSE null
        END AS is_clinical_encounter,
        CASE
            WHEN t1.encounter_type in ', @non_clinical_encounter_types,' THEN 20
            WHEN t1.encounter_type in ', @clinical_encounter_types,' THEN 10
            WHEN t1.encounter_type in', @other_encounter_types, ' THEN 5
            ELSE 1
        END AS encounter_type_sort_index,
        t2.orders
      FROM etl.flat_obs t1
        JOIN flat_cervical_cancer_screening_build_queue__0 t0 using (person_id)
        left JOIN etl.flat_orders t2 using(encounter_id)
      WHERE t1.encounter_type in ',@encounter_types,');'
    );
                          
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
      
    INSERT INTO flat_cervical_cancer_screening_0a
    (SELECT
        t1.person_id,
        null,
        t1.encounter_id,
        t1.test_datetime,
        t1.encounter_type,
        null, 
        -- t1.location_id,
        t1.obs,
        null, #obs_datetimes
        -- in any visit, there many be multiple encounters. for this dataset, we want to include only clinical encounters (e.g. not lab or triage visit)
        0 as is_clinical_encounter,
        1 as encounter_type_sort_index,
        null
        FROM etl.flat_lab_obs t1
        JOIN flat_cervical_cancer_screening_build_queue__0 t0 using (person_id)
    );
    
    SELECT CONCAT('Creating positive VIA or VIA/VILI test observations table...');
    DROP TEMPORARY TABLE IF EXISTS flat_cervical_cancer_screening_positive_test_observations;
    CREATE TEMPORARY TABLE flat_cervical_cancer_screening_positive_test_observations
    (SELECT 
      o.person_id as 'obs_person_id', 
      o.encounter_id, 
      o.value_coded, 
      GROUP_CONCAT(cn.name) as 'positive_via_or_vili_test_observations'
    FROM
      flat_cervical_cancer_screening_0a t1
    JOIN amrs.obs o on (t1.person_id = o.person_id AND t1.encounter_id = o.encounter_id)
    JOIN amrs.concept_name cn on (cn.concept_id = o.value_coded AND cn.voided = 0)
    WHERE
      o.concept_id = 9590
      AND cn.locale_preferred = 1
      AND o.value_coded NOT IN (1115)
    GROUP BY o.person_id,o.encounter_id
    );
    
    SELECT CONCAT('Done creating positive VIgA or VIA/VILI test observations table...');

    DROP TEMPORARY TABLE IF EXISTS flat_cervical_cancer_screening_0;
    CREATE TEMPORARY TABLE flat_cervical_cancer_screening_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
    (SELECT * FROM flat_cervical_cancer_screening_0a
        order by person_id, date(encounter_datetime), encounter_type_sort_index
    );

    SET @prev_id := null;
    SET @cur_id := null;
    -- CERVICAL CANCER SCREENING FORM (enc type 69)
    SET @cur_visit_type := null;
    SET @reasons_for_current_visit := null;
    SET @actual_scheduled_visit_date := null;
    SET @gravida := null;
    SET @parity := null;
    SET @menstruation_status := null;
    SET @last_menstrual_period_date := null;
    SET @pregnancy_status := null;
    SET @estimated_delivery_date := null;
    SET @last_delivery_date := null;
    SET @preason_not_pregnant := null;
    SET @hiv_status := null;
    SET @enrolled_in_hiv_care := null;
    SET @primary_care_facility := null;
    SET @primary_care_facility_non_ampath := null;
    SET @prior_via_done := null;
    SET @prior_via_result := null;
    SET @prior_via_date := null;
    SET @screening_method := null;
    SET @via_or_via_vili_test_result := null;
    SET @observations_from_positive_via_or_via_vili_test := null;
    SET @visual_impression_cervix := null;
    SET @visual_impression_vagina := null;
    SET @visual_impression_vulva := null;
    SET @hpv_test_result := null;
    SET @hpv_type := null;
    SET @pap_smear_test_result := null;
    SET @procedures_done_this_visit := null;
    SET @treatment_method := null;
    SET @other_treatment_method_non_coded := null;
    SET @leep_status := null;
    SET @cryotherapy_status := null;
    SET @thermocoagulation_status := null;
    SET @screening_assessment_notes := null;
    SET @screening_follow_up_plan := null;
    SET @screening_rtc_date := null;
    -- DYSPLASIA FORM (enc type 70)
    SET @dysp_visit_type := null;
    SET @dysp_history := null;
    SET @dysp_past_via_result := null;
    SET @dysp_past_pap_smear_result := null;
    SET @dysp_past_biopsy_result := null;
    SET @dysp_past_biopsy_result_date := null;
    SET @dysp_past_biopsy_result_non_coded := null;
    SET @dysp_past_treatment := null;
    SET @dysp_past_treatment_specimen_pathology := null;
    SET @dysp_satisfactory_colposcopy := null;
    SET @dysp_colposcopy_findings := null;
    SET @dysp_cervical_lesion_size := null;
    SET @dysp_visual_impression_cervix := null;
    SET @dysp_visual_impression_vagina := null;
    SET @dysp_vulva_impression_vulva := null;
    SET @dysp_procedure_done := null;
    SET @dysp_management_plan := null;
    SET @dysp_assessment_notes := null;
    SET @dysp_rtc_date := null;
    -- GYN PATHOLOGY RESULTS FORM (enc type 147)
    SET @gynp_pap_smear_results := null;
    SET @gynp_pap_smear_results_date := null;
    SET @gynp_biopsy_sample_collection_date := null;
    SET @gynp_diagnosis_date := null;
    SET @gynp_date_patient_notified_of_results := null;
    SET @gynp_procedure_done := null;
    SET @gynp_biopsy_cervix_result := null;
    SET @gynp_leep_location := null;
    SET @gynp_vagina_result := null;
    SET @gynp_vulva_result := null;
    SET @gynp_endometrium_result := null;
    SET @gynp_ecc_result := null;
    SET @gynp_lab_test_non_coded := null;
    SET @gynp_date_patient_informed_and_referred := null;
    SET @gynp_pathology_management_plan := null;
    SET @gynp_assessment_notes := null;
    SET @gynp_rtc_date := null;
                                          
    DROP TEMPORARY TABLE IF EXISTS flat_cervical_cancer_screening_1;
    CREATE TEMPORARY TABLE flat_cervical_cancer_screening_1 #(index encounter_id (encounter_id))
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
        p.death_date,

        CASE
          WHEN timestampdiff(year,birthdate,curdate()) > 0 THEN round(timestampdiff(year,birthdate,curdate()),0)
          ELSE round(timestampdiff(month,birthdate,curdate())/12,2)
        END AS age,
        CASE
          -- NEW
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!1839=1246!!" THEN @cur_visit_type := 1 -- Scheduled visit
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!1839=1837!!" THEN @cur_visit_type := 2 -- Unscheduled visit early
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!1839=1838!!" THEN @cur_visit_type := 3 -- Unscheduled visit late
          -- OLD
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!1839=7850!!" THEN @cur_visit_type := 4 -- Initial visit
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!1839=9569!!" THEN @cur_visit_type := 5 -- Peer follow-up visit
          ELSE @cur_visit_type := null
        END AS cur_visit_type,
        CASE
          -- NEW
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!1834=7850!!" THEN @reasons_for_current_visit := 1 -- Initial screening
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!1834=432!!" THEN @reasons_for_current_visit := 2 -- Routine screening
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!1834=11755!!" THEN @reasons_for_current_visit := 3 -- Post-Treatment screening
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!1834=1185!!" THEN @reasons_for_current_visit := 4 -- Treatment visit
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!1834=11758!!" THEN @reasons_for_current_visit := 5 -- Complications
          -- OLD
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!1834=9651!!" THEN @reasons_for_current_visit := 6 -- Annual screening
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!1834=1154!!" THEN @reasons_for_current_visit := 7 -- New complaints
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!1834=1246!!" THEN @reasons_for_current_visit := 8 -- Scheduled visit
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!1834=5622!!" THEN @reasons_for_current_visit := 9 -- Other (non-coded)
          ELSE @reasons_for_current_visit := null
        END AS reasons_for_current_visit,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7029=" THEN @actual_scheduled_visit_date := GetValues(obs, 7029)
          ELSE @actual_scheduled_visit_date := null
        END AS actual_scheduled_visit_date,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!5624=" THEN @gravida := GetValues(obs, 5624)
          ELSE @gravida := null
        END AS gravida,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!1053=" THEN @parity := GetValues(obs, 1053)
          ELSE @parity := null
        END AS parity,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!2061=5989!!" THEN @menstruation_status := 1 -- Menstruating 
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!2061=6496!!" THEN @menstruation_status := 2 -- Post-menopausal
          ELSE @menstruation_status := null
        END AS menstruation_status,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!1836=" THEN @last_menstrual_period_date := GetValues(obs, 1836)
          ELSE @last_menstrual_period_date := null
        END AS last_menstrual_period_date,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!8351=1065!!" THEN @pregnancy_status := 1 -- Yes
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!8351=1066!!" THEN @pregnancy_status := 0 -- No
          ELSE @pregnancy_status := null
        END AS pregnancy_status, 
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!5596=" THEN @estimated_delivery_date := GetValues(obs, 5596)
          ELSE @estimated_delivery_date := null
        END AS estimated_delivery_date,
        CASE 
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!5599=" THEN @last_delivery_date := GetValues(obs, 5599)
          ELSE @last_delivery_date := null
        END AS last_delivery_date,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!9733=9729!!" THEN @reason_not_pregnant := 1 -- Pregnancy not suspected
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!9733=9730!!" THEN @reason_not_pregnant := 2 -- Pregnancy test is negative
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!9733=9731!!" THEN @reason_not_pregnant := 3 -- Hormonal contraception
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!9733=9732!!" THEN @reason_not_pregnant := 4 -- Postpartum less than six weeks
          ELSE @reason_not_pregnant := null
        END AS reason_not_pregnant,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!6709=664!!" THEN @hiv_status := 1 -- Negative
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!6709=703!!" THEN @hiv_status := 2 -- Positive
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!6709=1067!!" THEN @hiv_status := 3 -- Unknown
          ELSE @hiv_status := null
        END AS hiv_status,
        CASE 
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!6152=1065!!" THEN @enrolled_in_hiv_care := 1 -- Yes
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!6152=1066!!" THEN @enrolled_in_hiv_care := 2 -- No
          ELSE @enrolled_in_hiv_care := null
        END AS enrolled_in_hiv_care,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!6152=1065!!" THEN @primary_care_facility := pa.value
          ELSE @primary_care_facility := null
        END AS primary_care_facility,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!6152=1065!!" AND obs REGEXP "!!10000=" THEN @primary_care_facility_non_ampath := GetValues(obs, 10000)
          ELSE @primary_care_facility_non_ampath := null
        END AS primary_care_facility_non_ampath,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!9589=1065!!" THEN @prior_via_done := 1 -- Yes
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!9589=1066!!" THEN @prior_via_done := 0 -- No
          ELSE @prior_via_done := null
        END AS prior_via_done,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7381=664!!" THEN @prior_via_result := 1 -- Positive
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7381=703!!" THEN @prior_via_result := 2 -- Negative
          ELSE @prior_via_result := null
        END AS prior_via_result,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7381=" THEN @prior_via_date := GetValues(obs_datetimes, 7381) -- Could be buggy!
          ELSE @prior_via_date := null
        END AS prior_via_date,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!10402=" THEN @screening_method := GetValues(obs, 10402) 
          ELSE @screening_method := null
        END AS screening_method,
        CASE 
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!9434=664!!" OR GetValues(obs, 9590) LIKE '%1115%' THEN @via_or_via_vili_test_result := 1 -- Negative
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!9434=703!!" OR GetValues(obs, 9590) IS NOT NULL AND GetValues(obs, 9590) NOT LIKE '%1115%' THEN @via_or_via_vili_test_result := 2 -- Positive
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!9434=6971!!" THEN @via_or_via_vili_test_result := 3 -- Suspicious of cancer
          ELSE @via_or_via_vili_test_result := null
        END AS via_or_via_vili_test_result,
        v.positive_via_or_vili_test_observations as `observations_from_positive_via_or_via_vili_test`,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7484=1115!!" THEN @visual_impression_cervix := 1 -- Normal
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7484=7507!!" THEN @visual_impression_cervix := 2 -- Positive VIA with Aceto white area
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7484=7508!!" THEN @visual_impression_cervix := 3 -- Positive VIA with suspicious lesion
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7484=7424!!" THEN @visual_impression_cervix := 4 -- CIN grade 1
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7484=7425!!" THEN @visual_impression_cervix := 5 -- CIN grade 2
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7484=7216!!" THEN @visual_impression_cervix := 6 -- CIN grade 3
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7484=6537!!" THEN @visual_impression_cervix := 7 -- Cervical cancer
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7484=7421!!" THEN @visual_impression_cervix := 8 -- Squamous cell carcinoma, NOS
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7484=5622!!" THEN @visual_impression_cervix := 9 -- Other (non-coded)
          ELSE @visual_impression_cervix := null
        END AS visual_impression_cervix,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7490=1115!!" THEN @visual_impression_vagina := 1 -- Normal
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7490=1447!!" THEN @visual_impression_vagina := 2 -- Warts, genital
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7490=9181!!" THEN @visual_impression_vagina := 3 -- Suspicious of cancer, vaginal lesion
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7490=1116!!" THEN @visual_impression_vagina := 4 -- Abnormal
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7490=9177!!" THEN @visual_impression_vagina := 5 -- Suspicious of cancer, vulva lesion
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7490=7492!!" THEN @visual_impression_vagina := 6 -- VIN grade 1
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7490=7491!!" THEN @visual_impression_vagina := 7 -- VIN grade 2
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7490=7435!!" THEN @visual_impression_vagina := 8 -- VIN grade 3
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7490=5622!!" THEN @visual_impression_vagina := 9 -- Other (non-coded)
          ELSE @visual_impression_vagina := null
        END AS visual_impression_vagina,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7490=1115!!" THEN @visual_impression_vulva := 1 -- Normal
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7490=1447!!" THEN @visual_impression_vulva := 2 -- Warts, genital
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7490=9177!!" THEN @visual_impression_vulva := 3 -- Suspicious of cancer, vulval lesion
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7490=1116!!" THEN @visual_impression_vulva := 4 -- Abnormal
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7490=7489!!" THEN @visual_impression_vulva := 5 -- Condyloma or VIN grade 1, 
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7490=7488!!" THEN @visual_impression_vulva := 6 -- VIN grade 2
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7490=7483!!" THEN @visual_impression_vulva := 7 -- VIN grade 3
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7490=5622!!" THEN @visual_impression_vulva := 8 -- Other (non-coded)
          ELSE @visual_impression_vulva := null
        END AS visual_impression_vulva,
          CASE 
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!2322=664!!" THEN @hpv_test_result := 1 -- Negative
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!2322=703!!" THEN @hpv_test_result := 2 -- Positive
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!2322=1138!!" THEN @hpv_test_result := 3 -- Indeterminate -> Unknown (different label)
          ELSE @hpv_test_result := null
        END AS hpv_test_result,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!11776=11773!!" THEN @hpv_type := 1 -- HPV TYPE 16
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!11776=11774!!" THEN @hpv_type := 2 -- HPV TYPE 18
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!11776=11775!!" THEN @hpv_type := 3 -- HPV TYPE 45
          ELSE @hpv_type := null
        END AS hpv_type,
        CASE 
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7423=1115!!" THEN @pap_smear_test_result := 1 -- Normal
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7423=7417!!" THEN @pap_smear_test_result := 2 -- ASCUS / ASC-H
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7423=7419!!" THEN @pap_smear_test_result := 3 -- LSIL
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7423=7420!!" THEN @pap_smear_test_result := 4 -- HSIL/CIS
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7423=7418!!" THEN @pap_smear_test_result := 5 -- AGUS
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7423=10055!!" THEN @pap_smear_test_result := 6 -- Invasive cancer
          ELSE @pap_smear_test_result := null
        END AS pap_smear_test_result,
        CASE
        -- OLD
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7479=1107!!" THEN @procedures_done_this_visit := 1 -- None
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7479=6511!!" THEN @procedures_done_this_visit := 2 -- Excisional/surgical biopsy
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7479=7466!!" THEN @procedures_done_this_visit := 3 -- Cryotherapy
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7479=7147!!" THEN @procedures_done_this_visit := 4 -- LEEP
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7479=885!!" THEN @procedures_done_this_visit := 5 -- Pap smear
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7479=9724!!" THEN @procedures_done_this_visit := 6 -- Cervical polypectomy
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7479=6510!!" THEN @procedures_done_this_visit := 7 -- Core needle biopsy
        -- NEW
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7479=7478!!" THEN @procedures_done_this_visit := 8 -- Endocervical curettage
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7479=11826!!" THEN @procedures_done_this_visit := 9 -- Cervical biopsy
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7479=11769!!" THEN @procedures_done_this_visit := 10 -- Endometrial biopsy
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7479=10202!!" THEN @procedures_done_this_visit := 11 -- Punch biopsy
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7479=11828!!" THEN @procedures_done_this_visit := 12 -- Vaginal biopsy
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7479=11829!!" THEN @procedures_done_this_visit := 13 -- Vault biopsy
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7479=11827!!" THEN @procedures_done_this_visit := 14 -- Vulval biopsy
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7479=5622!!" THEN @procedures_done_this_visit := 15 -- Other (non-coded)
          ELSE @procedures_done_this_visit := null
        END AS procedures_done_this_visit,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!10380=1107!!" THEN @treatment_method := 1 -- None  
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!10380=7466!!" THEN @treatment_method := 2 -- Cryotherapy
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!10380=7147!!" THEN @treatment_method := 3 -- LEEP 
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!10380=11757!!" THEN @treatment_method := 4 -- Thermocoagulation
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!10380=1667!!" THEN @treatment_method := 5 -- Other treatment methods
          ELSE @treatment_method := null
        END AS treatment_method,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!10039=" THEN @other_treatment_method_non_coded := GetValues(obs, 10039)
          ELSE @other_treatment_method_non_coded := null
        END AS other_treatment_method_non_coded,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!11761=10756!!" THEN @cryotherapy_status := 1 -- Done
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!11761=11760!!" THEN @cryotherapy_status := 2 -- Single Visit Approach
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!11761=11759!!" THEN @cryotherapy_status := 3 -- Postponed
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!11761=10115!!" THEN @cryotherapy_status := 4 -- Referred
          ELSE @cryotherapy_status := null
        END AS cryotherapy_status,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!11762=11760!!" THEN @leep_status := 1 -- Single Visit Approach
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!11762=11759!!" THEN @leep_status := 2 -- Postponed
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!11762=10115!!" THEN @leep_status := 3 -- Referred
          ELSE @leep_status := null
        END AS leep_status,
        CASE 
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!11763=10756!!" THEN @thermocoagulation_status := 1 -- Done  
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!11763=11760!!" THEN @thermocoagulation_status := 2 -- Single Visit Approach
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!11763=11759!!" THEN @thermocoagulation_status := 3 -- Postponed
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!11763=10115!!" THEN @thermocoagulation_status := 4 -- Referred
          ELSE @thermocoagulation_status := null
        END AS thermocoagulation_status,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7222=" THEN @screening_assessment_notes := GetValues(obs, 7222)
          ELSE @screening_assessment_notes := null
        END AS screening_assessment_notes,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7500=9725!!" THEN @screening_follow_up_plan := 1 -- Return for results
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7500=9178!!" THEN @screening_follow_up_plan := 2 -- VIA follow-up in three to six months
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7500=7496!!" THEN @screening_follow_up_plan := 3 -- Routine yearly VIA
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7500=7497!!" THEN @screening_follow_up_plan := 4 -- Routine 3 year VIA
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7500=7383!!" THEN @screening_follow_up_plan := 5 -- Colposcopy planned
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7500=7499!!" THEN @screening_follow_up_plan := 6 -- Gynecologic oncology services
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!7500=5622!!" THEN @screening_follow_up_plan := 7 -- Other (non-coded)
          ELSE @screening_follow_up_plan := null
        END AS screening_follow_up_plan,
        CASE
          WHEN t1.encounter_type = 69 AND obs REGEXP "!!5096=" THEN @screening_rtc_date := GetValues(obs, 5096)
          ELSE @screening_rtc_date := null
        END AS screening_rtc_date,
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!1839=1911!!" THEN @dysp_visit_type := 1 -- New visit
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!1839=1246!!" THEN @dysp_visit_type := 1 -- Scheduled visit
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!1839=1837!!" THEN @dysp_visit_type := 2 -- Unscheduled visit early
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!1839=1838!!" THEN @dysp_visit_type := 3 -- Unscheduled visit late
          ELSE @dysp_visit_type := null
        END AS dysp_visit_type,
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7379=1065!!" THEN @dysp_history := 1 -- Yes
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7379=1066!!" THEN @dysp_history := 0 -- No
          ELSE @dysp_history := null
        END AS dysp_history,
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7381=664!!" THEN @dysp_past_via_result := 1 -- Negative
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7381=703!!" THEN @dysp_past_via_result := 2 -- Positive
          ELSE @dysp_past_via_result := null
        END AS dysp_past_via_result,
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7381=" THEN @dysp_past_via_result_date := GetValues(obs_datetimes, 7381)
          ELSE @dysp_past_via_result_date := null
        END AS dysp_past_via_result_date,
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7423=" THEN @dysp_past_pap_smear_result := GetValues(obs, 7423)
          ELSE @dysp_past_pap_smear_result := null
        END AS dysp_past_pap_smear_result,
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7426=1115!!" THEN @dysp_past_biopsy_result := 1 -- Normal
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7426=7424!!" THEN @dysp_past_biopsy_result := 2 -- CIN 1
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7426=7425!!" THEN @dysp_past_biopsy_result := 3 -- CIN 2
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7426=7216!!" THEN @dysp_past_biopsy_result := 4 -- CIN 3
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7426=7421!!" THEN @dysp_past_biopsy_result := 5 -- Carcinoma
          ELSE @dysp_past_biopsy_result := null
        END AS dysp_past_biopsy_result, 
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7426=" THEN @dysp_past_biopsy_result_date := GetValues(obs_datetimes, 7426)
          ELSE @dysp_past_biopsy_result_date := null
        END AS dysp_past_biopsy_result_date,
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7400=" THEN @dysp_past_biopsy_result_non_coded := GetValues(obs, 7400)
          ELSE @dysp_past_biopsy_result_non_coded := null
        END AS dysp_past_biopsy_result_non_coded,
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7467=" THEN @dysp_past_treatment := GetValues(obs, 7467)
          ELSE @dysp_past_treatment := null
        END AS dysp_past_treatment, 
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7579=" THEN @dysp_past_treatment_specimen_pathology := GetValues(obs, 7579)
          ELSE @dysp_past_treatment_specimen_pathology := null
        END AS dysp_past_treatment_specimen_pathology, 
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7428=1065!!" THEN @dysp_satisfactory_colposcopy := 1 -- Yes
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7428=1066!!" THEN @dysp_satisfactory_colposcopy := 2 -- No
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7428=1118!!" THEN @dysp_satisfactory_colposcopy := 3 -- Not done
          ELSE @dysp_satisfactory_colposcopy := null
        END AS dysp_satisfactory_colposcopy,
        CASE 
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7383=" THEN @dysp_colposcopy_findings := GetValues(obs, 7383)
          ELSE @dysp_colposcopy_findings := null
        END AS dysp_colposcopy_findings,
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7477=7474!!" THEN @dysp_cervical_lesion_size := 1 -- > 50% of cervix
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7477=7474!!" THEN @dysp_cervical_lesion_size := 2 -- < 50% of cervix
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7477=7476!!" THEN @dysp_cervical_lesion_size := 3 -- Cervical lesion extends into endocervical canal
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7477=9619!!" THEN @dysp_cervical_lesion_size := 4 -- Transformation zone > 50% of cervix
          ELSE @dysp_cervical_lesion_size := null
        END AS dysp_cervical_lesion_size, 
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7484=1115!!" THEN @dysp_visual_impression_cervix := 1 -- Normal
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7484=7424!!" THEN @dysp_visual_impression_cervix := 2 -- CIN 1
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7484=7425!!" THEN @dysp_visual_impression_cervix := 3 -- CIN 2
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7484=7216!!" THEN @dysp_visual_impression_cervix := 4 -- CIN 3
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7484=7421!!" THEN @dysp_visual_impression_cervix := 5 -- Carcinoma
          ELSE @dysp_visual_impression_cervix := null
        END AS dysp_visual_impression_cervix,
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7490=1115!!" THEN @dysp_visual_impression_vagina := 1 -- Normal
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7490=1447!!" THEN @dysp_visual_impression_vagina := 2 -- Warts, genital
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7490=9181!!" THEN @dysp_visual_impression_vagina := 3 -- Suspicious of cancer, vaginal lesion
          ELSE @dysp_visual_impression_vagina := null
        END AS dysp_visual_impression_vagina,
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7490=1115!!" THEN @dysp_visual_impression_vulva := 1 -- Normal
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7490=1447!!" THEN @dysp_visual_impression_vulva := 2 -- Warts, genital
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7490=9177!!" THEN @dysp_visual_impression_vulva := 3 -- Suspicious of cancer, vulval lesion
          ELSE @dysp_visual_impression_vulva := null
        END AS dysp_visual_impression_vulva,
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7479=" THEN @dysp_procedure_done := GetValues(obs, 7479) -- Consider changing to multi-SELECT
          ELSE @dysp_procedure_done := null
        END AS dysp_procedure_done,
        -- FREETEXT FIELD (Problematic!)
        --   CASE
        --        WHEN t1.encounter_type = 70 AND obs REGEXP "!!1915=" THEN @other_dysplasia_procedure_done_non_coded := GetValues(obs, 1915)
        --        ELSE @other_dysplasia_procedure_done_non_coded := null
        --   END AS other_dysplasia_procedure_done_non_coded,
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7500=" THEN @dysp_management_plan := GetValues(obs, 7500)
          ELSE @dysp_management_plan := null
        END AS dysp_management_plan,
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!7222=" THEN @dysp_assessment_notes := GetValues(obs, 7222)
          ELSE @dysp_assessment_notes := null
        END AS dysp_assessment_notes,
        CASE
          WHEN t1.encounter_type = 70 AND obs REGEXP "!!5096=" THEN @dysp_rtc_date := GetValues(obs, 5096)
          ELSE @dysp_rtc_date := null
        END AS dysp_rtc_date,

        -- GYN PATHOLOGY RESULTS FORM (enc type 147)
        CASE 
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7423=1115!!" THEN @gynp_pap_smear_results := 1 -- Normal
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7423=7417!!" THEN @gynp_pap_smear_results := 2 -- ASCUS
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7423=7418!!" THEN @gynp_pap_smear_results := 3 -- AGUS
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7423=7419!!" THEN @gynp_pap_smear_results := 4 -- LSIL
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7423=7420!!" THEN @gynp_pap_smear_results := 5 -- HSIL
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7423=7422!!" THEN @gynp_pap_smear_results := 6 -- Carcinoma
          ELSE @gynp_pap_smear_results := null
        END AS gynp_pap_smear_results,
        CASE 
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7423=" THEN @gynp_pap_smear_results_date := GetValues(obs_datetimes, 7423)
          ELSE @gynp_pap_smear_results_date := null
        END AS gynp_pap_smear_results_date,
        CASE 
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10060=" THEN @gynp_biopsy_sample_collection_date := GetValues(obs, 10060)
          ELSE @gynp_biopsy_sample_collection_date := null
        END AS gynp_biopsy_sample_collection_date,
        CASE 
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!9728=" THEN @gynp_diagnosis_date := GetValues(obs, 9728) 
          ELSE @gynp_diagnosis_date := null
        END AS gynp_diagnosis_date,
        CASE 
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10061=" THEN @gynp_date_patient_notified_of_results := GetValues(obs, 10061)
          ELSE @gynp_date_patient_notified_of_results := null
        END AS gynp_date_patient_notified_of_results,
        CASE 
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10127=10202!!" THEN @gynp_procedure_done := 1 -- Punch biopsy
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10127=7147!!" THEN @gynp_procedure_done := 2 -- LEEP
          ELSE @gynp_procedure_done := null
        END AS gynp_procedure_done,
        CASE 
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7645=1115!!" THEN @gynp_biopsy_cervix_result := 1 -- Normal
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7645=7424!!" THEN @gynp_biopsy_cervix_result := 2 -- CIN 1
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7645=7425!!" THEN @gynp_biopsy_cervix_result := 3 -- CIN 2
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7645=7216!!" THEN @gynp_biopsy_cervix_result := 4 -- CIN 3
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7645=1447!!" THEN @gynp_biopsy_cervix_result := 5 -- Genital warts
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7645=149!!" THEN  @gynp_biopsy_cervix_result := 6 -- Cervicitis
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7645=8282!!" THEN @gynp_biopsy_cervix_result := 7 -- Cervical squamous metaplasia
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7645=9620!!" THEN @gynp_biopsy_cervix_result := 8 -- Condylomata
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7645=8276!!" THEN @gynp_biopsy_cervix_result := 9 -- Cervical squamous cell carcinoma
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7645=9617!!" THEN @gynp_biopsy_cervix_result := 10 -- Microinvasive carcinoma
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7645=9621!!" THEN @gynp_biopsy_cervix_result := 11 -- Adenocarcinoma in situ
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7645=7421!!" THEN @gynp_biopsy_cervix_result := 12 -- Squamous cell carcinoma, NOS
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7645=7422!!" THEN @gynp_biopsy_cervix_result := 13 -- Adenocarcinoma
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7645=9618!!" THEN @gynp_biopsy_cervix_result := 14 -- Invasive Adenocarcinoma
          ELSE @gynp_biopsy_cervix_result := null
        END AS gynp_biopsy_cervix_result,
        CASE
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!8268=8266!!" THEN @leep_location := 1 -- Superficial 
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!8268=8267!!" THEN @leep_location := 2 -- Deep
          ELSE @gynp_leep_location := null
        END AS gynp_leep_location,
        CASE 
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7647=1115!!" THEN @gynp_vagina_result := 1 -- Normal
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7647=7492!!" THEN @gynp_vagina_result := 2 -- VAIN 1
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7647=7491!!" THEN @gynp_vagina_result := 3 -- VAIN 2
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7647=7435!!" THEN @gynp_vagina_result := 4 -- VAIN 3
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7647=6537!!" THEN @gynp_vagina_result := 5 -- Cervical cancer
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7647=1447!!" THEN @gynp_vagina_result := 6 -- Genital warts
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7647=8282!!" THEN @gynp_vagina_result := 7 -- Cervical squamous metaplasia
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7647=9620!!" THEN @gynp_vagina_result := 8 -- Condylomata
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7647=8276!!" THEN @gynp_vagina_result := 9 -- Cervical squamous cell carcinoma
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7647=9617!!" THEN @gynp_vagina_result := 10 -- Microinvasive carcinoma
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7647=9621!!" THEN @gynp_vagina_result := 11 -- Adenocarcinoma in situ
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7647=7421!!" THEN @gynp_vagina_result := 12 -- Squamous cell carcinoma, NOS
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7647=7422!!" THEN @gynp_vagina_result := 13 -- Adenocarcinoma
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7647=9618!!" THEN @gynp_vagina_result := 14 -- Invasive adenocarcinoma
          ELSE @gynp_vagina_result := null
        END AS gynp_vagina_result,
        CASE
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7646=1115!!" THEN @gynp_vulva_result := 1 -- Normal
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7646=7489!!" THEN @gynp_vulva_result := 2 -- Condyloma or vulvar intraepithelial neoplasia grade 1
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7646=7488!!" THEN @gynp_vulva_result := 3 -- Vulvar intraepithelial neoplasia grade 2
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7646=7483!!" THEN @gynp_vulva_result := 4 -- Vulvar intraepithelial neoplasia grade 3
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7646=9618!!" THEN @gynp_vulva_result := 5 -- Invasive adenocarcinoma
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7646=1447!!" THEN @gynp_vulva_result := 6 -- Genital warts
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7646=8282!!" THEN @gynp_vulva_result := 7 -- Cervical squamous metaplasia
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7646=9620!!" THEN @gynp_vulva_result := 8 -- Condylomata
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7646=8276!!" THEN @gynp_vulva_result := 9 -- Cervical squamous cell carcinoma
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7646=9617!!" THEN @gynp_vulva_result := 10 -- Microinvasive carcinoma
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7646=9621!!" THEN @gynp_vulva_result := 11 -- Adenocarcinoma in situ
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7646=7421!!" THEN @gynp_vulva_result := 12 -- Squamous cell carcinoma, otherwise not specified
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7646=7422!!" THEN @gynp_vulva_result := 13 -- Adenocarcinoma
          ELSE @gynp_vulva_result := null
        END AS gynp_vulva_result,
        CASE 
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10207=1115!!" THEN @gynp_endometrium_result := 1 -- Normal
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10207=9620!!" THEN @gynp_endometrium_result := 2 -- Condylomata
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10207=8282!!" THEN @gynp_endometrium_result := 3 -- Cervical squamous metaplasia
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10207=8726!!" THEN @gynp_endometrium_result := 4 -- Invasive squamous cell carcinoma
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10207=9617!!" THEN @gynp_endometrium_result := 5 -- Microinvasive carcinoma
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10207=9621!!" THEN @gynp_endometrium_result := 6 -- Adenocarcinoma in situ
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10207=7421!!" THEN @gynp_endometrium_result := 7 -- Squamous cell carcinoma
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10207=7422!!" THEN @gynp_endometrium_result := 8 -- Adenocarcinoma
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10207=9618!!" THEN @gynp_endometrium_result := 9 -- Invasive adenocarcinoma
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10207=5622!!" THEN @gynp_endometrium_result := 10 -- Other (non-coded)
          ELSE @gynp_endometrium_result := null
        END AS gynp_endometrium_result,
        CASE
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10204=1115!!" THEN @gynp_ecc_result := 1 -- Normal
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10204=7424!!" THEN @gynp_ecc_result := 2 -- CIN 1
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10204=7425!!" THEN @gynp_ecc_result := 3 -- CIN 2
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10204=7216!!" THEN @gynp_ecc_result := 4 -- CIN 3
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10204=149!!" THEN  @gynp_ecc_result := 5 -- Cervicitis
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10204=8282!!" THEN @gynp_ecc_result := 6 -- Cervical squamous metaplasia
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10204=9620!!" THEN @gynp_ecc_result := 7 -- Condylomata
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10204=8276!!" THEN @gynp_ecc_result := 8 -- Cervical squamous cell carcinoma
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10204=9617!!" THEN @gynp_ecc_result := 9 -- Microinvasive carcinoma
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10204=9621!!" THEN @gynp_ecc_result := 10 -- Adenocarcinoma in situ
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10204=7421!!" THEN @gynp_ecc_result := 11 -- Squamous cell carcinoma, otherwise not specified
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10204=7422!!" THEN @gynp_ecc_result := 12 -- Adenocarcinoma
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!10204=9618!!" THEN @gynp_ecc_result := 13 -- Invasive adenocarcinoma
          ELSE @gynp_ecc_result := null
        END AS gynp_ecc_result,
        CASE
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!9538=" THEN @gynp_lab_test_non_coded := GetValues(obs, 9538)
          ELSE @gynp_lab_test_non_coded := null
        END AS gynp_lab_test_non_coded,
        CASE
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!9706=" THEN @gynp_date_patient_informed_and_referred := GetValues(obs, 9706) 
          ELSE @gynp_date_patient_informed_and_referred := null
        END AS gynp_date_patient_informed_and_referred,
        CASE
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7500=9725!!" THEN @gynp_pathology_management_plan := 1 -- Return for result
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7500=9178!!" THEN @gynp_pathology_management_plan := 2 -- VIA follow-up in three months
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7500=7498!!" THEN @gynp_pathology_management_plan := 3 -- Complete VIA or Pap smear in six months
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7500=7496!!" THEN @gynp_pathology_management_plan := 4 -- Complete VIA in one year
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7500=7497!!" THEN @gynp_pathology_management_plan := 5 -- Complete VIA in three years
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7500=7499!!" THEN @gynp_pathology_management_plan := 6 -- Gynecologic oncology services
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7500=6105!!" THEN @gynp_pathology_management_plan := 7 -- Referred to clinician today
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7500=7147!!" THEN @gynp_pathology_management_plan := 8 -- LEEP
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7500=7466!!" THEN @gynp_pathology_management_plan := 9 -- Cryotherapy
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7500=10200!!" THEN @gynp_pathology_management_plan := 10 -- Repeat procedure
          ELSE @gynp_pathology_management_plan := null
        END AS gynp_pathology_management_plan,
        CASE 
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!7222=" THEN @gynp_assessment_notes := GetValues(obs, 7222)
          ELSE @gynp_assessment_notes := null
        END AS gynp_assessment_notes,
        CASE
          WHEN t1.encounter_type = 147 AND obs REGEXP "!!5096=" THEN @gynp_rtc_date := GetValues(obs, 5096)
          ELSE @gynp_rtc_date := null
        END AS gynp_rtc_date  

      FROM flat_cervical_cancer_screening_0 t1
      left JOIN flat_cervical_cancer_screening_positive_test_observations `v` on (v.obs_person_id = t1.person_id AND t1.encounter_id = v.encounter_id)
      LEFT JOIN amrs.person AS p using (person_id)
      LEFT JOIN amrs.person_attribute AS pa ON (t1.person_id = pa.person_id) AND pa.person_attribute_type_id = 7 AND pa.voided = 0
      ORDER BY person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
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

    ALTER TABLE flat_cervical_cancer_screening_1 drop prev_id, drop cur_id;

    DROP TABLE IF EXISTS flat_cervical_cancer_screening_2;
    CREATE TEMPORARY TABLE flat_cervical_cancer_screening_2
    (SELECT *,
      @prev_id := @cur_id as prev_id,
      @cur_id := person_id as cur_id,

      CASE
        WHEN @prev_id = @cur_id THEN @prev_encounter_datetime := @cur_encounter_datetime
        ELSE @prev_encounter_datetime := null
      END AS next_encounter_datetime_cervical_cancer_screening,

      @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

      CASE
        WHEN @prev_id = @cur_id THEN @next_encounter_type := @cur_encounter_type
        ELSE @next_encounter_type := null
      END AS next_encounter_type_cervical_cancer_screening,

      @cur_encounter_type := encounter_type as cur_encounter_type,

      CASE
        WHEN @prev_id = @cur_id THEN @prev_clinical_datetime := @cur_clinical_datetime
        ELSE @prev_clinical_datetime := null
      END AS next_clinical_datetime_cervical_cancer_screening,

      CASE
        WHEN @prev_id = @cur_id THEN @prev_clinical_location_id := @cur_clinical_location_id
        ELSE @prev_clinical_location_id := null
      END AS next_clinical_location_id_cervical_cancer_screening,

      CASE
        WHEN is_clinical_encounter THEN @cur_clinical_datetime := encounter_datetime
        WHEN @prev_id = @cur_id THEN @cur_clinical_datetime
        ELSE @cur_clinical_datetime := null
      END AS cur_clinic_datetime,

      CASE
        WHEN is_clinical_encounter THEN @cur_clinical_location_id := location_id
        WHEN @prev_id = @cur_id THEN @cur_clinical_location_id
        ELSE @cur_clinical_location_id := null
      END AS cur_clinic_location_id,

      CASE
        WHEN @prev_id = @cur_id THEN @prev_clinical_rtc_date := @cur_clinical_rtc_date
        ELSE @prev_clinical_rtc_date := null
      END AS next_clinical_rtc_date_cervical_cancer_screening,

      CASE
        WHEN is_clinical_encounter THEN @cur_clinical_rtc_date := screening_rtc_date
        WHEN @prev_id = @cur_id THEN @cur_clinical_rtc_date
        ELSE @cur_clinical_rtc_date:= null
      END AS cur_clinical_rtc_date

      FROM flat_cervical_cancer_screening_1
      order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
    );

    ALTER TABLE flat_cervical_cancer_screening_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;

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

    DROP TEMPORARY TABLE IF EXISTS flat_cervical_cancer_screening_3;
    CREATE TEMPORARY TABLE flat_cervical_cancer_screening_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
    (SELECT
      *,
      @prev_id := @cur_id as prev_id,
      @cur_id := t1.person_id as cur_id,
      CASE
        WHEN @prev_id=@cur_id THEN @prev_encounter_type := @cur_encounter_type
        ELSE @prev_encounter_type:=null
      END AS prev_encounter_type_cervical_cancer_screening,	
      @cur_encounter_type := encounter_type as cur_encounter_type,
      CASE
        WHEN @prev_id=@cur_id THEN @prev_encounter_datetime := @cur_encounter_datetime
        ELSE @prev_encounter_datetime := null
      END AS prev_encounter_datetime_cervical_cancer_screening,
      @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,
      CASE
        WHEN @prev_id = @cur_id THEN @prev_clinical_datetime := @cur_clinical_datetime
        ELSE @prev_clinical_datetime := null
      END AS prev_clinical_datetime_cervical_cancer_screening,
      CASE
        WHEN @prev_id = @cur_id THEN @prev_clinical_location_id := @cur_clinical_location_id
        ELSE @prev_clinical_location_id := null
      END AS prev_clinical_location_id_cervical_cancer_screening,
      CASE
        WHEN is_clinical_encounter THEN @cur_clinical_datetime := encounter_datetime
        WHEN @prev_id = @cur_id THEN @cur_clinical_datetime
        ELSE @cur_clinical_datetime := null
      END AS cur_clinical_datetime,
      CASE
        WHEN is_clinical_encounter THEN @cur_clinical_location_id := location_id
        WHEN @prev_id = @cur_id THEN @cur_clinical_location_id
        ELSE @cur_clinical_location_id := null
      END AS cur_clinical_location_id,
      CASE
        WHEN @prev_id = @cur_id THEN @prev_clinical_rtc_date := @cur_clinical_rtc_date
        ELSE @prev_clinical_rtc_date := null
      END AS prev_clinical_rtc_date_cervical_cancer_screening,
      CASE
        WHEN is_clinical_encounter THEN @cur_clinical_rtc_date := screening_rtc_date
        WHEN @prev_id = @cur_id THEN @cur_clinical_rtc_date
        ELSE @cur_clinical_rtc_date:= null
      END AS cur_clinic_rtc_date
      FROM flat_cervical_cancer_screening_2 t1
      order by person_id, date(encounter_datetime), encounter_type_sort_index
    );
                                    
    SELECT 
      COUNT(*)
      INTO @new_encounter_rows FROM
      flat_cervical_cancer_screening_3;
              
    SELECT @new_encounter_rows;                    
    SET @total_rows_written = @total_rows_written + @new_encounter_rows;
    SELECT @total_rows_written;

    SET @dyn_sql=CONCAT('REPLACE INTO ',@write_table,											  
      '(SELECT
        null,
        person_id,
        encounter_id,
        encounter_type,
        encounter_datetime,
        visit_id,
        location_id,
        t2.uuid as location_uuid,
        age,
        cur_visit_type,
        reasons_for_current_visit,
        actual_scheduled_visit_date,
        gravida,
        parity,
        menstruation_status,
        last_menstrual_period_date,
        pregnancy_status,
        estimated_delivery_date,
        last_delivery_date,
        reason_not_pregnant,
        hiv_status,
        enrolled_in_hiv_care,
        primary_care_facility,
        primary_care_facility_non_ampath,
        prior_via_done,
        prior_via_result,
        prior_via_date,
        screening_method,
        via_or_via_vili_test_result,
        observations_from_positive_via_or_via_vili_test,
        visual_impression_cervix,
        visual_impression_vagina,
        visual_impression_vulva,
        hpv_test_result,
        hpv_type,
        pap_smear_test_result,
        procedures_done_this_visit,
        treatment_method,
        other_treatment_method_non_coded,
        leep_status,
        cryotherapy_status,
        thermocoagulation_status,
        screening_assessment_notes,
        screening_follow_up_plan,
        screening_rtc_date,        
        dysp_visit_type,
        dysp_history,
        dysp_past_via_result,
        dysp_past_via_result_date,
        dysp_past_pap_smear_result,
        dysp_past_biopsy_result,
        dysp_past_biopsy_result_date,
        dysp_past_biopsy_result_non_coded,
        dysp_past_treatment,
        dysp_past_treatment_specimen_pathology,
        dysp_satisfactory_colposcopy,
        dysp_colposcopy_findings,
        dysp_cervical_lesion_size,
        dysp_visual_impression_cervix,
        dysp_visual_impression_vagina,
        dysp_visual_impression_vulva,
        dysp_procedure_done,
        dysp_management_plan,
        dysp_assessment_notes,
        dysp_rtc_date,
        gynp_pap_smear_results,
        gynp_pap_smear_results_date,
        gynp_biopsy_sample_collection_date,
        gynp_diagnosis_date,
        gynp_date_patient_notified_of_results,
        gynp_procedure_done,
        gynp_biopsy_cervix_result,
        gynp_leep_location,
        gynp_vagina_result,
        gynp_vulva_result,
        gynp_endometrium_result,
        gynp_ecc_result,
        gynp_lab_test_non_coded,
        gynp_date_patient_informed_and_referred,
        gynp_pathology_management_plan,
        gynp_assessment_notes,
        gynp_rtc_date,
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

        FROM flat_cervical_cancer_screening_3 t1
        JOIN amrs.location t2 using (location_id))');       

    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;


    SET @dyn_sql=CONCAT('delete t1 FROM ',@queue_table,' t1 JOIN flat_cervical_cancer_screening_build_queue__0 t2 using (person_id);'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
                        
    SET @dyn_sql=CONCAT('SELECT count(*) into @person_ids_count FROM ',@queue_table,';'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;

    SET @cycle_length := timestampdiff(second,@loop_start_time,now());
    SET @total_time := @total_time + @cycle_length;
    SET @cycle_number := @cycle_number + 1;
    
    SET @remaining_time := ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);

    SELECT 
      @person_ids_count AS 'persons remaining',
      @cycle_length AS 'Cycle time (s)',
      CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
      @remaining_time AS 'Est time remaining (min)';

  END WHILE;
                 
  IF (@query_type = "build") THEN
    SET @dyn_sql := CONCAT('drop table ', @queue_table, ';'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
                    
    SET @total_rows_to_write := 0;
    SET @dyn_sql := CONCAT("SELECT count(*) into @total_rows_to_write FROM ", @write_table);
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;
                                            
    SET @start_write := now();
    SELECT 
      CONCAT(@start_write,
        ' : Writing ',
        @total_rows_to_write,
        ' to ',
        @primary_table);

    SET @dyn_sql := CONCAT('REPLACE INTO ', @primary_table, '(SELECT * FROM ',@write_table,');');
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;
    
    SET @finish_write := now();
    SET @time_to_write := timestampdiff(second,@start_write,@finish_write);

    SELECT 
      CONCAT(@finish_write,
        ' : Completed writing rows. Time to write to primary table: ',
        @time_to_write,
        ' seconds ');                        
                    
        SET @dyn_sql := CONCAT('drop table ',@write_table,';'); 
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
  END IF;
                
	SET @ave_cycle_length := ceil(@total_time/@cycle_number);

  SELECT 
    CONCAT('Average Cycle Length: ',
      @ave_cycle_length,
      ' second(s)');
        
  SET @end := now();
  INSERT INTO etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
  SELECT
    CONCAT(@table_version,
      ' : Time to complete: ',
      TIMESTAMPDIFF(MINUTE, @start, @end),
      ' minutes');
END