CREATE PROCEDURE `generate_flat_onc_patient_history_v1_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
  SET @primary_table := "flat_onc_patient_history";
  SET @query_type := query_type;
              
  SET @total_rows_written := 0;

  SET @encounter_types := "(69,70,86,145,146,147,160,38,39,40,41,42,45,130,141,148,149,150,151,175,142,143,169,170,177,91,92,89,90,93,94,184)";
  SET @clinical_encounter_types := "(69,70,86,145,146,147,160,38,39,40,41,42,45,130,141,148,149,150,151,175,142,143,169,170,177,91,92,89,90,93,94,184)";
  SET @non_clinical_encounter_types := "(-1)";
  SET @other_encounter_types := "(-1)";
            
  SET @start := now();
  SET @table_version := "flat_onc_patient_history_v1.0";

  SET session sort_buffer_size := 512000000;

  SET @sep := " ## ";
  SET @boundary := "!!";
  SET @last_date_created := (SELECT MAX(max_date_created) FROM etl.flat_obs);

  CREATE TABLE IF NOT EXISTS flat_onc_patient_history (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    person_id INT,
    uuid VARCHAR(100),
    encounter_id INT,
    encounter_datetime DATETIME,
    encounter_type INT,
    encounter_type_name VARCHAR(100),
    visit_id INT,
    visit_type_id SMALLINT,
    visit_start_datetime DATETIME,
    location_id SMALLINT,
    program_id SMALLINT,
    is_clinical INT,
    enrollment_date DATETIME,
    prev_rtc_date DATETIME,
    rtc_date DATETIME,
    diagnosis INT,
    diagnosis_method INT,
    result_of_diagnosis INT,
    diagnosis_date DATETIME,
    breast_exam_findings_this_visit VARCHAR(5),
    via_or_via_vili_test_result INT,
    observations_from_positive_via_or_via_vili_test VARCHAR(200),
    prior_via_result INT,
    prior_via_date DATETIME,
    hiv_status INT,
    cancer_type INT,
    cancer_subtype INT,
    breast_cancer_type INT,
    non_cancer_diagnosis INT,
    cancer_stage INT,
    overall_cancer_stage_group INT,
    cur_onc_meds VARCHAR(500),
    cur_onc_meds_dose VARCHAR(500),
    cur_onc_meds_frequency VARCHAR(500),
    cur_onc_meds_start_date DATETIME,
    cur_onc_meds_end_date DATETIME,
    oncology_treatment_plan INT,
    chemotherapy INT,
    current_chemo_cycle INT,
    total_chemo_cycles_planned INT,
    therapeutic_notes VARCHAR(1000),
    cancer_diagnosis_status INT,
    reasons_for_surgery INT,
    chemotherapy_intent INT,
    chemotherapy_plan INT,
    chemotherapy_regimen VARCHAR(50),
    drug_route VARCHAR(500),
    medication_history INT,
    other_meds_added INT,
    sickle_cell_drugs INT,
    PRIMARY KEY encounter_id (encounter_id),
    INDEX person_date (person_id, encounter_datetime),
    INDEX location_id_enc_date (location_id, encounter_datetime),
    INDEX enc_date_location_id (encounter_datetime, location_id),
    INDEX location_id_rtc_date (location_id, rtc_date),
    INDEX encounter_type (encounter_type),
    INDEX date_created (date_created)
  );        

  IF (@query_type = "build") THEN
      SELECT 'BUILDING..........................................';

      SET @write_table := CONCAT("flat_onc_patient_history_temp_", queue_number);
      SET @queue_table := CONCAT("flat_onc_patient_history_build_queue_", queue_number);
      SET @primary_queue_table := "flat_onc_patient_history_build_queue";
                  
      SET @dyn_sql=CONCAT('CREATE TABLE IF NOT EXISTS ',@write_table,' like ',@primary_table);
      PREPARE s1 FROM @dyn_sql; 
      EXECUTE s1; 
      DEALLOCATE PREPARE s1;  

      SET @dyn_sql=CONCAT('CREATE TABLE IF NOT EXISTS ',@queue_table,' (SELECT * FROM ', @primary_queue_table, ' limit ', queue_size, ');'); 
      PREPARE s1 FROM @dyn_sql; 
      EXECUTE s1; 
      DEALLOCATE PREPARE s1;  
      
      SET @dyn_sql=CONCAT('DELETE t1 FROM ', @primary_queue_table, ' t1 JOIN ',@queue_table, ' t2 using (person_id);'); 
      PREPARE s1 FROM @dyn_sql; 
      EXECUTE s1; 
      DEALLOCATE PREPARE s1;
  END IF;

  IF (@query_type = "sync") THEN
      SELECT 'SYNCING..........................................';
      SET @write_table := "flat_onc_patient_history";
      SET @queue_table := "flat_onc_patient_history_sync_queue";
      
      CREATE TABLE IF NOT EXISTS flat_onc_patient_history_sync_queue (
          person_id INT PRIMARY KEY
      );                            
                    
      SET @last_update := NULL;

      SELECT 
        MAX(date_updated)
      INTO @last_update FROM
        etl.flat_log
      WHERE
        table_name = @table_version;					

      REPLACE INTO flat_onc_patient_history_sync_queue
      (SELECT DISTINCT patient_id
        from amrs.encounter
        where date_changed > @last_update
      );

      REPLACE INTO flat_onc_patient_history_sync_queue
      (SELECT DISTINCT person_id
        from etl.flat_obs
        where max_date_created > @last_update
      );

      REPLACE INTO flat_onc_patient_history_sync_queue
      (SELECT DISTINCT person_id
        from etl.flat_lab_obs
        where max_date_created > @last_update
      );

      REPLACE INTO flat_onc_patient_history_sync_queue
      (SELECT DISTINCT person_id
        from etl.flat_orders
        where max_date_created > @last_update
      );
                    
      REPLACE INTO flat_onc_patient_history_sync_queue
      (SELECT person_id FROM 
        amrs.person 
        where date_voided > @last_update);

      REPLACE INTO flat_onc_patient_history_sync_queue
      (SELECT person_id FROM 
        amrs.person 
        where date_changed > @last_update);
  END IF;

  -- Remove test patients
  SET @dyn_sql := CONCAT('DELETE t1 FROM ',@queue_table,' t1
    JOIN amrs.person_attribute t2 using (person_id)
    where t2.person_attribute_type_id=28 and value="true" and voided=0');
  PREPARE s1 FROM @dyn_sql; 
  EXECUTE s1; 
  DEALLOCATE PREPARE s1;  

  SET @person_ids_count := 0;
  SET @dyn_sql := CONCAT('SELECT count(*) into @person_ids_count FROM ',@queue_table); 
  PREPARE s1 FROM @dyn_sql; 
  EXECUTE s1; 
  DEALLOCATE PREPARE s1;

  SELECT @person_ids_count AS 'num patients to update';

  SET @dyn_sql := CONCAT('DELETE t1 FROM ',@primary_table, ' t1 JOIN ',@queue_table,' t2 using (person_id);'); 
  PREPARE s1 FROM @dyn_sql; 
  EXECUTE s1; 
  DEALLOCATE PREPARE s1;  
            
  SET @total_time := 0;
  SET @cycle_number := 0;
            
  WHILE @person_ids_count > 0 DO
    SET @loop_start_time := now();
                
    DROP TEMPORARY TABLE IF EXISTS flat_onc_patient_history_build_queue__0;
    SET @dyn_sql := CONCAT('CREATE TEMPORARY TABLE flat_onc_patient_history_build_queue__0 (person_id int primary key) (SELECT * FROM ',@queue_table,' limit ',cycle_size,');'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
            
    DROP TEMPORARY TABLE IF EXISTS flat_onc_patient_history_0a;
                
    SET @dyn_sql := CONCAT(
        'CREATE TEMPORARY TABLE flat_onc_patient_history_0a
        (select
          t1.person_id,
          t4.uuid,
          t1.encounter_id,
          t1.encounter_datetime,
          t1.encounter_type,
          t1.obs,
          t1.obs_datetimes,
          t1.visit_id,
          v.visit_type_id,
          v.date_started AS visit_start_datetime,
          t1.location_id,
          t5.program_id,
          et.name,
          CASE
            when t1.encounter_type in ',@clinical_encounter_types,' then 1
            else NULL
          END AS is_clinical_encounter,
          CASE
            when t1.encounter_type in ',@non_clinical_encounter_types,' then 20
            when t1.encounter_type in ',@clinical_encounter_types,' then 10
            when t1.encounter_type in', @other_encounter_types, ' then 5
            else 1
          END AS encounter_type_sort_index,
          t2.orders
            FROM etl.flat_obs t1
          JOIN flat_onc_patient_history_build_queue__0 t0 on t0.person_id = t1.person_id  
          LEFT JOIN etl.flat_orders t2 using(encounter_id)
          LEFT JOIN amrs.visit v on (v.visit_id = t1.visit_id)
          JOIN amrs.person t4 on t4.person_id = t1.person_id  
          LEFT JOIN etl.clinical_encounter_type_map t5 on (t5.encounter_type = t1.encounter_type)
          LEFT JOIN amrs.encounter_type et on (et.encounter_type_id = t1.encounter_type)
        where t1.encounter_type in ',@encounter_types,');');
                    
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  

    INSERT INTO flat_onc_patient_history_0a
    (SELECT
      t1.person_id,
      p.uuid,
      t1.encounter_id,
      t1.test_datetime,
      t1.encounter_type,
      t1.obs,
      NULL,
      NULL,
      t3.visit_type_id,
      t3.date_started AS visit_start_datetime,
      t1.location_id,
      t5.program_id,
      et.name,
      0 AS is_clinical_encounter,
      1 AS encounter_type_sort_index,
      NULL AS orders
      from etl.flat_lab_obs t1
      JOIN 
        amrs.person p on p.person_id = t1.person_id  
      JOIN 
        amrs.visit t3 on t1.person_id = t3.patient_id  
      JOIN 
        flat_onc_patient_history_build_queue__0 t0 on t0.person_id = t1.person_id  
      JOIN
        etl.clinical_encounter_type_map t5 on t5.encounter_type = t1.encounter_type
      JOIN
        amrs.encounter_type et on et.encounter_type_id = t1.encounter_type
    );

    SELECT CONCAT('Creating positive VIA or VIA/VILI test observations table...');
    DROP TEMPORARY TABLE IF EXISTS flat_cervical_cancer_screening_positive_test_observations;
    CREATE TEMPORARY TABLE flat_cervical_cancer_screening_positive_test_observations
    (SELECT 
      o.person_id as 'obs_person_id', 
      o.encounter_id, 
      o.value_coded, 
      GROUP_CONCAT(COALESCE(CONCAT(UPPER(SUBSTRING(TRIM(cn.name), 1, 1)), LOWER(SUBSTRING(TRIM(cn.name), 2))), '') SEPARATOR ', ') AS 'positive_via_or_vili_test_observations'
    FROM
      flat_onc_patient_history_0a t1
    JOIN amrs.obs o ON (t1.person_id = o.person_id AND t1.encounter_id = o.encounter_id)
    JOIN amrs.concept_name cn ON (cn.concept_id = o.value_coded AND cn.voided = 0)
    WHERE o.concept_id = 9590 
    AND cn.locale_preferred = 1
    AND o.value_coded NOT IN (1115)
    GROUP BY o.person_id,o.encounter_id
    );

    SELECT CONCAT('Done creating positive VIA or VIA/VILI test observations table...');

    DROP TEMPORARY TABLE IF EXISTS flat_onc_patient_history_0;
    CREATE TEMPORARY TABLE flat_onc_patient_history_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
    (SELECT * FROM flat_onc_patient_history_0a
        ORDER BY person_id, date(encounter_datetime), encounter_type_sort_index
    );

    SET @prev_rtc_date := NULL;
    SET @rtc_date := NULL;
    SET @diagnosis := NULL;
    SET @diagnosis_method := NULL;
    SET @result_of_diagnosis := NULL;
    SET @diagnosis_date := NULL;
    SET @breast_exam_findings_this_visit := NULL;
    SET @via_or_via_vili_test_result := NULL;
    SET @observations_from_positive_via_or_via_vili_test := NULL;
    SET @prior_via_result := NULL;
    SET @prior_via_date := NULL;
    SET @hiv_status := NULL; 
    SET @cancer_type := NULL;
    SET @cancer_subtype := NULL;
    SET @breast_cancer_type := NULL;
    SET @non_cancer_diagnosis := NULL;
    SET @cancer_stage := NULL;
    SET @overall_cancer_stage_group := NULL;
    SET @cur_onc_meds_start_date := NULL;
    SET @cur_onc_meds_end_date := NULL;
    SET @cur_onc_meds := NULL;
    SET @cur_onc_meds_dose := NULL;
    SET @cur_onc_meds_frequency := NULL;
    SET @oncology_treatment_plan := NULL;
    SET @chemotherapy := NULL;
    SET @current_chemo_cycle := NULL;
    SET @total_chemo_cycles_planned := NULL;
    SET @therapeutic_notes := NULL; 
    SET @cancer_diagnosis_status := NULL;
    SET @reasons_for_surgery := NULL;
    SET @chemotherapy_intent := NULL;
    SET @chemotherapy_plan := NULL;
    SET @chemotherapy_regimen := NULL;
    SET @drug_route := NULL;
    SET @medication_history := NULL;
    SET @other_meds_added := NULL;
    SET @sickle_cell_drugs := NULL;
    SET @lab_encounter_type := 99999;
    SET @prev_id := NULL;
    SET @cur_id := NULL;
    SET @enrollment_date := NULL;
    SET @cur_rtc_date := NULL;
    SET @prev_rtc_date := NULL; 
                                        
    DROP TEMPORARY TABLE IF EXISTS flat_onc_patient_history_1;
    CREATE TEMPORARY TABLE flat_onc_patient_history_1 #(index encounter_id (encounter_id))
    (SELECT 
      @prev_id := @cur_id AS prev_id,
      @cur_id := t1.person_id AS cur_id,
      t1.person_id,
      t1.uuid,
      t1.encounter_id,
      t1.encounter_datetime,
      t1.encounter_type,
      t1.name as `encounter_type_name`,
      t1.visit_id,
      t1.visit_type_id,
      t1.visit_start_datetime,
      t1.location_id,
      t1.program_id,
      t1.is_clinical_encounter,
      CASE
        WHEN @prev_id != @cur_id AND t1.encounter_type IN (21, @lab_encounter_type) THEN @enrollment_date := NULL
        WHEN @prev_id != @cur_id THEN @enrollment_date := encounter_datetime
        WHEN t1.encounter_type NOT IN (21, @lab_encounter_type) AND @enrollment_date IS NULL THEN @enrollment_date := encounter_datetime
        ELSE @enrollment_date
      END AS enrollment_date,
      CASE
        WHEN @prev_id = @cur_id THEN @prev_rtc_date := @cur_rtc_date
        ELSE @prev_rtc_date := NULL
      END AS prev_rtc_date,
      CASE
        WHEN obs regexp "!!5096=" THEN @cur_rtc_date := REPLACE(REPLACE((substring_index(substring(obs,locate("!!5096=", obs)), @sep, 1)),"!!5096=",""),"!!","")
        WHEN @prev_id = @cur_id THEN IF (@cur_rtc_date > encounter_datetime, @cur_rtc_date, NULL)
        ELSE @cur_rtc_date := NULL
      END AS rtc_date,
      CASE
        WHEN obs regexp "!!7179=" THEN @diagnosis := REPLACE(REPLACE((substring_index(substring(obs,locate("!!7179=",obs)),@sep,1)),"!!7179=",""),"!!","")
        ELSE @diagnosis := NULL
      END AS diagnosis,
      @diagnosis_method := GetValues(obs, 6504) AS diagnosis_method,
      @result_of_diagnosis := CAST(REPLACE(REPLACE((substring_index(substring(obs,locate("!!7191=",obs)),@sep,1)),"!!7191=",""),"!!","") AS unsigned) AS result_of_diagnosis,
      CASE
        WHEN obs regexp "!!9728=" THEN @diagnosis_date := REPLACE(REPLACE((substring_index(substring(obs,locate("!!9728=",obs)),@sep,1)),"!!9728=",""),"!!","")
        ELSE @diagnosis_date := NULL
      END AS diagnosis_date,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!6251=" THEN @breast_exam_findings_this_visit := SUBSTRING(GetValues(obs, 6251), -4)
        ELSE @breast_exam_findings_this_visit := NULL
      END AS breast_exam_findings_this_visit,
      CASE 
        WHEN t1.encounter_type = 69 AND obs REGEXP "!!9434=664!!" OR GetValues(obs, 9590) LIKE '%1115%' THEN @via_or_via_vili_test_result := 1 -- Negative
        WHEN t1.encounter_type = 69 AND obs REGEXP "!!9434=703!!" OR GetValues(obs, 9590) IS NOT NULL AND GetValues(obs, 9590) NOT LIKE '%1115%' THEN @via_or_via_vili_test_result := 2 -- Positive
        WHEN t1.encounter_type = 69 AND obs REGEXP "!!9434=6971!!" THEN @via_or_via_vili_test_result := 3 -- Suspicious of cancer
        ELSE @via_or_via_vili_test_result := null
      END AS via_or_via_vili_test_result,
      vvto.positive_via_or_vili_test_observations as `observations_from_positive_via_or_via_vili_test`,
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
        WHEN obs regexp "!!6709=" THEN @hiv_status :=  GetValues(obs, 6709)
        ELSE @hiv_status := NULL
      END AS hiv_status,
      CASE
        WHEN obs regexp "!!6042=6555!!" THEN @cancer_type := 6555
        WHEN obs regexp "!!7176=" THEN @cancer_type := GetValues(obs, 7176)
        ELSE @cancer_type := NULL
      END AS cancer_type,
      CASE
        WHEN obs REGEXP "!!9841=" THEN @cancer_subtype := GetValues(obs, 9841) -- Breast cancer type
        WHEN obs REGEXP "!!6536=" THEN @cancer_subtype := GetValues(obs, 6536) -- Gynecologic cancer type
        WHEN obs REGEXP "!!9843=" THEN @cancer_subtype := GetValues(obs, 9843) -- Sarcoma cancer type
        WHEN obs REGEXP "!!9844=" THEN @cancer_subtype := GetValues(obs, 9844) -- Leukemia cancer type
        WHEN obs REGEXP "!!6551=" THEN @cancer_subtype := GetValues(obs, 6551) -- Lymphoma cancer type
        WHEN obs REGEXP "!!9846=" THEN @cancer_subtype := GetValues(obs, 9846) -- Other solid cancer type
        WHEN obs REGEXP "!!6540=" THEN @cancer_subtype := GetValues(obs, 6540) -- Skin cancer type
        WHEN obs REGEXP "!!6528=" THEN @cancer_subtype := GetValues(obs, 6528) -- Head and neck cancer type
        WHEN obs REGEXP "!!6514=" THEN @cancer_subtype := GetValues(obs, 6514) -- Genitourinary cancer type
        WHEN obs REGEXP "!!6520=" THEN @cancer_subtype := GetValues(obs, 6520) -- Gastrointestinal cancer
        WHEN obs REGEXP "!!10132=" THEN @cancer_subtype := GetValues(obs, 10132) -- Non-small cell lung cancer type
        ELSE @cancer_subtype := null
      END AS cancer_subtype,
      CASE
        WHEN obs regexp "!!9841=" THEN @breast_cancer_type := GetValues(obs, 9841)
        ELSE @breast_cancer_type := NULL
      END AS breast_cancer_type,
      CASE
        WHEN obs regexp "!!9847=" THEN @non_cancer_diagnosis := GetValues(obs, 9847)
        ELSE @non_cancer_diagnosis := NULL
      END AS non_cancer_diagnosis,
      CASE
        WHEN obs regexp "!!6582=" THEN @cancer_stage := GetValues(obs, 6582)
        ELSE @cancer_stage := NULL
      END AS cancer_stage,
      CASE
        WHEN obs regexp "!!9868=" THEN @overall_cancer_stage_group := GetValues(obs,9868)
        ELSE @overall_cancer_stage_group:= NULL
      END AS overall_cancer_stage_group,
      CASE
        WHEN obs regexp "!!9869=(1260)!!" THEN @cur_onc_meds := NULL
        WHEN obs regexp "!!9918=" THEN @cur_onc_meds := GetValues(obs, 9918)
        -- WHEN obs regexp "!!7196=" THEN @cur_onc_meds := GetValues(obs, 7196)
        -- WHEN obs regexp "!!6643=" THEN @cur_onc_meds := GetValues(obs, 6643)
        WHEN @prev_id=@cur_id THEN @cur_onc_meds
        ELSE @cur_onc_meds:= NULL
      END AS cur_onc_meds,
      CASE
        WHEN obs regexp "!!1899=" THEN @cur_onc_meds_dose := GetValues(obs, 1899)
        ELSE @cur_onc_meds_dose := NULL
      END AS cur_onc_meds_dose,
      CASE
        WHEN obs regexp "!!1896=" THEN @cur_onc_meds_frequency := GetValues(obs, 1896)
        ELSE @cur_onc_meds_frequency := NULL
      END AS cur_onc_meds_frequency,
      CASE
        WHEN obs regexp "!!9869=(1256|1259)" or (obs regexp "!!9869=(1257|1259)!!" and @cur_onc_meds_start_date is NULL ) THEN @cur_onc_meds_start_date := DATE(t1.encounter_datetime)
        WHEN obs regexp "!!9869=(1260)!!" THEN @cur_onc_meds_start_date := NULL
        WHEN @prev_id != @cur_id THEN @cur_onc_meds_start_date := NULL
        ELSE @cur_onc_meds_start_date
      END AS cur_onc_meds_start_date,
      NULL AS cur_onc_meds_end_date,
      CASE
        WHEN obs regexp "!!8723=" THEN @oncology_treatment_plan := GetValues(obs, 8723)
        ELSE @oncology_treatment_plan := NULL
      END AS oncology_treatment_plan,
      CASE
        WHEN obs regexp "!!6575=" THEN @chemotherapy := GetValues(obs, 6575)
        ELSE @chemotherapy := NULL
      END AS chemotherapy,
      CASE
        WHEN obs regexp "!!6643=" THEN @current_chemo_cycle := GetValues(obs, 6643)
        ELSE @current_chemo_cycle := NULL
      END AS current_chemo_cycle,
      CASE
        WHEN obs regexp "!!6644=" THEN @total_chemo_cycles_planned := GetValues(obs, 6644)
        ELSE @total_chemo_cycles_planned := NULL
      END AS total_chemo_cycles_planned,
      CASE
        WHEN obs regexp "!!7222=" THEN @therapeutic_notes := GetValues(obs, 7222)
        ELSE @therapeutic_notes := NULL
      END AS therapeutic_notes,
      CASE
        WHEN obs regexp "!!9848=" THEN @cancer_diagnosis_status := GetValues(obs, 9848)
        ELSE @cancer_diagnosis_status := NULL
      END AS cancer_diagnosis_status,
      CASE
        WHEN obs regexp "!!8725=" THEN @reasons_for_surgery := GetValues(obs, 8725)
        ELSE NULL
      END AS reasons_for_surgery,
      CASE
        WHEN obs regexp "!!2206=" THEN @chemotherapy_intent := GetValues(obs, 2206)
        ELSE @chemotherapy_intent := NULL
      END AS chemotherapy_intent,
      CASE
        WHEN obs regexp "!!9869=" THEN @chemotherapy_plan := GetValues(obs, 9869)
        ELSE @chemotherapy_plan := NULL
      END AS chemotherapy_plan,
      CASE
        WHEN obs regexp "!!9946=" THEN @chemotherapy_regimen := GetValues(obs, 9946)
        ELSE @chemotherapy_regimen := NULL
      END AS chemotherapy_regimen,
      CASE
        WHEN obs regexp "!!7463=" THEN @drug_route := GetValues(obs, 7463)
        ELSE @drug_route := NULL
      END AS drug_route,
      CASE
        WHEN obs regexp "!!9918=" THEN @medication_history := GetValues(obs, 9918)
        ELSE @medication_history := NULL
      END AS medication_history,
      CASE
        WHEN obs regexp "!!10198=" THEN @other_meds_added := GetValues(obs, 10198)
        ELSE @other_meds_added := NULL
      END AS other_meds_added,
      CASE
        WHEN obs regexp "!!9710=" THEN @sickle_cell_drugs := GetValues(obs, 9710)
        ELSE @sickle_cell_drugs := NULL
      END AS sickle_cell_drugs
    FROM flat_onc_patient_history_0 t1
      JOIN
        amrs.person p using (person_id)
      LEFT JOIN
        flat_cervical_cancer_screening_positive_test_observations `vvto` ON (vvto.obs_person_id = t1.person_id AND t1.encounter_id = vvto.encounter_id)
      ORDER BY person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
    );
    
    SET @prev_id := NULL;
    SET @cur_id := NULL;

    ALTER TABLE flat_onc_patient_history_1 drop prev_id, drop cur_id;

    SELECT COUNT(*) INTO @new_encounter_rows FROM flat_onc_patient_history_1;
            
    SELECT @new_encounter_rows;                    
    SET @total_rows_written := @total_rows_written + @new_encounter_rows;
    SELECT @total_rows_written;

    SET @dyn_sql=CONCAT('REPLACE INTO ', @write_table,											  
    '(SELECT
        NULL,
        person_id,
        uuid,
        encounter_id,
        encounter_datetime,
        encounter_type,
        encounter_type_name,
        visit_id,
        visit_type_id,
        visit_start_datetime,
        location_id,
        program_id,
        is_clinical_encounter,
        enrollment_date,
        prev_rtc_date,
        rtc_date,
        diagnosis,
        diagnosis_method,
        result_of_diagnosis,
        diagnosis_date,
        breast_exam_findings_this_visit,
        via_or_via_vili_test_result,
        observations_from_positive_via_or_via_vili_test,
        prior_via_result,
        prior_via_date,
        hiv_status,
        cancer_type,
        cancer_subtype,
        breast_cancer_type,
        non_cancer_diagnosis,
        cancer_stage,
        overall_cancer_stage_group,
        cur_onc_meds,
        cur_onc_meds_dose,
        cur_onc_meds_frequency,
        cur_onc_meds_start_date,
        cur_onc_meds_end_date,
        oncology_treatment_plan,
        chemotherapy,
        current_chemo_cycle,
        total_chemo_cycles_planned,
        therapeutic_notes,
        cancer_diagnosis_status,
        reasons_for_surgery,
        chemotherapy_intent,
        chemotherapy_plan,
        chemotherapy_regimen,
        drug_route,
        medication_history,
        other_meds_added,
        sickle_cell_drugs
    FROM
        flat_onc_patient_history_1)');

    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;


    SET @dyn_sql := CONCAT('DELETE t1 FROM ',@queue_table,' t1 JOIN flat_onc_patient_history_build_queue__0 t2 using (person_id);'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
                            
    SET @dyn_sql := CONCAT('SELECT count(*) into @person_ids_count FROM ',@queue_table,';'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  

    SET @cycle_length := TIMESTAMPDIFF(second,@loop_start_time,now());                   
    SET @total_time := @total_time + @cycle_length;
    SET @cycle_number := @cycle_number + 1;
    
    SET @remaining_time := CEIL((@total_time / @cycle_number) * CEIL(@person_ids_count / cycle_size) / 60);
    SELECT 
      @person_ids_count AS 'persons remaining',
      @cycle_length AS 'Cycle time (s)',
      CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
      @remaining_time AS 'Est time remaining (min)';
  END WHILE;
          
  IF (@query_type="build") THEN
    SET @dyn_sql := CONCAT('DROP TABLE ',@queue_table,';'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
                
    SET @total_rows_to_write=0;
    SET @dyn_sql := CONCAT("Select count(*) into @total_rows_to_write FROM ",@write_table);
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;
                                        
    SET @start_write := now();
    SELECT CONCAT(@start_write,' : Writing ', @total_rows_to_write,' to ', @primary_table);

    SET @dyn_sql := CONCAT('REPLACE INTO ', @primary_table,
      '(SELECT * FROM ', @write_table,');');
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;
    
    SET @finish_write := now();
    SET @time_to_write := TIMESTAMPDIFF(second, @start_write, @finish_write);
    SELECT CONCAT(@finish_write,' : Completed writing rows. Time to write to primary table: ', @time_to_write, ' seconds ');                        
                
    SET @dyn_sql := CONCAT('DROP TABLE ', @write_table, ';'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;                
  END IF;

  SET @ave_cycle_length := CEIL(@total_time/@cycle_number);
  SELECT CONCAT('Average Cycle Length: ', @ave_cycle_length, ' second(s)');
  SET @end := now();
  INSERT INTO etl.flat_log values (@start, @last_date_created, @table_version, TIMESTAMPDIFF(second, @start, @end));
  SELECT CONCAT(@table_version,' : Time to complete: ', TIMESTAMPDIFF(MINUTE, @start, @end),' minutes');
END