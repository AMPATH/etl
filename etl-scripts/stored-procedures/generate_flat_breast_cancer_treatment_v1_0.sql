CREATE PROCEDURE `generate_flat_breast_cancer_treatment_v1_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
  SET @primary_table := "flat_breast_cancer_treatment";
  SET @query_type := query_type;
              
  SET @total_rows_written := 0;

  SET @encounter_types := "(142, 143)";
  SET @clinical_encounter_types := "(142, 143)";
  SET @non_clinical_encounter_types := "(-1)";
  SET @other_encounter_types := "(-1)";
            
  SET @start := now();
  SET @table_version := "flat_breast_cancer_treatment_v1.0";

  SET session sort_buffer_size := 512000000;

  SET @sep := " ## ";
  SET @boundary := "!!";
  SET @last_date_created := (SELECT MAX(max_date_created) FROM etl.flat_obs);

  CREATE TABLE IF NOT EXISTS flat_breast_cancer_treatment (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    person_id INT,
    encounter_id INT,
    encounter_type INT,
    encounter_datetime DATETIME,
    visit_id INT,
    location_id INT,
    location_uuid VARCHAR(100),
	  gender CHAR(4),
    age INT,
    death_date DATETIME,
    initial_purpose_of_visit TINYINT,
    initial_chief_complaint TINYINT,
    initial_pain TINYINT,
    initial_pain_score TINYINT,
    initial_review_of_systems_heent VARCHAR(150),
    initial_review_of_systems_general VARCHAR(150),
    initial_review_of_systems_cardiopulmonary VARCHAR(150),
    initial_review_of_systems_gastrointestinal VARCHAR(150),
    initial_review_of_systems_genitourinary VARCHAR(150),
    initial_last_menstrual_period_date DATETIME,
    initial_review_of_systems_musculoskeletal VARCHAR(150),
    initial_ecog_index TINYINT,
    initial_general_exam_findings VARCHAR(150),
    initial_heent_exam_findings VARCHAR(150),
    initial_chest_exam_findings VARCHAR(150),
    initial_breast_exam_findings VARCHAR(100),
    initial_breast_finding_location VARCHAR(50),
    initial_breast_finding_quadrant VARCHAR(100),
    initial_heart_exam_findings VARCHAR(150),
    initial_abdomen_exam_findings VARCHAR(150),
    initial_urogenital_exam_findings VARCHAR(150),
    initial_extremities_exam_findings VARCHAR(150),
    initial_testicular_exam_findings VARCHAR(150),
    initial_nodal_survey_exam_findings VARCHAR(150),
    initial_musculoskeletal_exam_findings VARCHAR(150),
    initial_neurologic_exam_findings VARCHAR(150),
    initial_physical_examination_notes VARCHAR(200),
    initial_patient_currently_on_chemo TINYINT,
    initial_ct_head TINYINT,
    initial_ct_neck TINYINT,
    initial_ct_chest TINYINT,
    initial_ct_spine TINYINT,
    initial_ct_abdomen TINYINT,
    initial_ultrasound_renal TINYINT,
    initial_ultrasound_hepatic TINYINT,
    initial_ultrasound_obstetric TINYINT,
    initial_ultrasound_abdominal TINYINT,
    initial_ultrasound_breast TINYINT,
    initial_xray_shoulder TINYINT,
    initial_xray_pelvis TINYINT,
    initial_xray_abdomen TINYINT,
    initial_xray_skull TINYINT,
    initial_xray_leg TINYINT,
    initial_xray_hand TINYINT,
    initial_xray_foot TINYINT,
    initial_xray_chest TINYINT,
    initial_xray_arm TINYINT,
    initial_xray_spine TINYINT,
    initial_echocardiography TINYINT,
    initial_mri_head TINYINT,
    initial_mri_neck TINYINT,
    initial_mri_arms TINYINT,
    initial_mri_chest TINYINT,
    initial_mri_spine TINYINT,
    initial_mri_abdomen TINYINT,
    initial_mri_pelvis TINYINT,
    initial_mri_legs TINYINT,
    initial_imaging_results_description VARCHAR(150),
    initial_rbc_test_result INT,
    initial_hgb_test_result INT,
    initial_mcv_test_result INT,
    initial_mch_test_result INT,
    initial_mchc_test_result INT,
    initial_rdw_test_result INT,
    initial_platelets_test_result INT,
    initial_wbc_test_result INT,
    initial_anc_test_result INT,
    initial_pcv_test_result INT,
    initial_urea_test_result INT,
    initial_creatinine_test_result INT,
    initial_sodium_test_result INT,
    initial_potassium_test_result INT,
    initial_chloride_test_result INT,
    initial_serum_total_bilirubin_test_result INT,
    initial_serum_direct_bilirubin_test_result INT,
    initial_gamma_glutamyl_transferase_test_result INT,
    initial_serum_glutamic_oxaloacetic_transaminase_test_result INT,
    initial_serum_total_protein INT,
    initial_serum_albumin INT,
    initial_serum_alkaline_phosphate INT,
    initial_serum_lactate_dehydrogenase INT,
    initial_hemoglobin_f_test_result INT,
    initial_hemoglobin_a_test_result INT,
    initial_hemoglobin_s_test_result INT,
    initial_hemoglobin_a2c_test_result INT,
    initial_urinalysis_pus_cells TINYINT,
    initial_urinalysis_protein TINYINT,
    initial_urinalysis_leucocytes TINYINT,
    initial_urinalysis_ketones TINYINT,
    initial_urinalysis_glucose TINYINT,
    initial_urinalysis_nitrites TINYINT,
    initial_reticulocytes TINYINT,
    initial_total_psa INT,
    initial_carcinoembryonic_antigen_test INT,
    initial_carbohydrate_antigen_19_9 INT,
    initial_lab_results_notes VARCHAR(200),
    initial_other_radiology_orders VARCHAR(100),
    initial_diagnosis_established_using VARCHAR(100),
    initial_diagnosis_based_on_biopsy_type VARCHAR(100),
    initial_biopsy_concordant_with_clinical_suspicions VARCHAR(100),
    initial_breast_cancer_type TINYINT,
    initial_diagnosis_date DATETIME,
    initial_cancer_stage TINYINT,
    initial_overall_cancer_stage_group TINYINT,
    initial_cancer_staging_date DATETIME,
    initial_metastasis_region VARCHAR(100),
    initial_treatment_plan TINYINT,
    initial_reason_for_surgery TINYINT,
    initial_surgical_procedure_done VARCHAR(100),
    initial_radiation_location TINYINT,
    initial_treatment_hospitalization_required TINYINT,
    initial_chemotherapy_plan TINYINT,
    initial_total_planned_chemotherapy_cycles TINYINT,
    initial_current_chemotherapy_cycle TINYINT,
    initial_reasons_for_changing_modifying_or_stopping_chemo TINYINT,
    initial_chemotherapy_intent TINYINT,
    initial_chemotherapy_start_date DATETIME,
    initial_chemotherapy_regimen VARCHAR(100),
    initial_chemotherapy_drug_started VARCHAR(300),
    initial_chemotherapy_drug_dosage_in_mg VARCHAR(50),
    initial_chemotherapy_drug_route VARCHAR(100),
    initial_chemotherapy_medication_frequency VARCHAR(100),
    initial_non_chemotherapy_drug VARCHAR(200),
    initial_other_non_coded_chemotherapy_drug VARCHAR(200),
    initial_non_chemotherapy_drug_purpose VARCHAR(100),
    initial_hospitalization_due_to_toxicity TINYINT,
    initial_presence_of_neurotoxicity TINYINT,
    initial_disease_status_at_end_of_treatment TINYINT,
    initial_assessment_notes VARCHAR(200),
    initial_general_plan_referrals VARCHAR(200),
    rtc_date DATETIME,
    return_purpose_of_visit TINYINT,
    return_chief_complaint TINYINT,
    return_pain TINYINT,
    return_pain_score TINYINT,
    return_review_of_systems_heent VARCHAR(150),
    return_review_of_systems_general VARCHAR(150),
    return_review_of_systems_cardiopulmonary VARCHAR(150),
    return_review_of_systems_gastrointestinal VARCHAR(150),
    return_review_of_systems_genitourinary VARCHAR(150),
    return_last_menstrual_period_date DATETIME,
    return_review_of_systems_musculoskeletal VARCHAR(150),
    return_ecog_index TINYINT,
    return_general_exam_findings VARCHAR(150),
    return_heent_exam_findings VARCHAR(150),
    return_chest_exam_findings VARCHAR(150),
    return_breast_exam_findings VARCHAR(100),
    return_breast_finding_location VARCHAR(50),
    return_breast_finding_quadrant VARCHAR(100),
    return_heart_exam_findings VARCHAR(150),
    return_abdomen_exam_findings VARCHAR(150),
    return_urogenital_exam_findings VARCHAR(150),
    return_extremities_exam_findings VARCHAR(150),
    return_testicular_exam_findings VARCHAR(150),
    return_nodal_survey_exam_findings VARCHAR(150),
    return_musculoskeletal_exam_findings VARCHAR(150),
    return_neurologic_exam_findings VARCHAR(150),
    return_physical_examination_notes VARCHAR(200),
    return_patient_currently_on_chemo TINYINT,
    return_ct_head TINYINT,
    return_ct_neck TINYINT,
    return_ct_chest TINYINT,
    return_ct_spine TINYINT,
    return_ct_abdomen TINYINT,
    return_ultrasound_renal TINYINT,
    return_ultrasound_hepatic TINYINT,
    return_ultrasound_obstetric TINYINT,
    return_ultrasound_abdominal TINYINT,
    return_ultrasound_breast TINYINT,
    return_xray_shoulder TINYINT,
    return_xray_pelvis TINYINT,
    return_xray_abdomen TINYINT,
    return_xray_skull TINYINT,
    return_xray_leg TINYINT,
    return_xray_hand TINYINT,
    return_xray_foot TINYINT,
    return_xray_chest TINYINT,
    return_xray_arm TINYINT,
    return_xray_spine TINYINT,
    return_echocardiography TINYINT,
    return_mri_head TINYINT,
    return_mri_neck TINYINT,
    return_mri_arms TINYINT,
    return_mri_chest TINYINT,
    return_mri_spine TINYINT,
    return_mri_abdomen TINYINT,
    return_mri_pelvis TINYINT,
    return_mri_legs TINYINT,
    return_imaging_results_description VARCHAR(150),
    return_rbc_test_result INT,
    return_hgb_test_result INT,
    return_mcv_test_result INT,
    return_mch_test_result INT,
    return_mchc_test_result INT,
    return_rdw_test_result INT,
    return_platelets_test_result INT,
    return_wbc_test_result INT,
    return_anc_test_result INT,
    return_pcv_test_result INT,
    return_urea_test_result INT,
    return_creatinine_test_result INT,
    return_sodium_test_result INT,
    return_potassium_test_result INT,
    return_chloride_test_result INT,
    return_serum_total_bilirubin_test_result INT,
    return_serum_direct_bilirubin_test_result INT,
    return_gamma_glutamyl_transferase_test_result INT,
    return_serum_glutamic_oxaloacetic_transaminase_test_result INT,
    return_serum_total_protein INT,
    return_serum_albumin INT,
    return_serum_alkaline_phosphate INT,
    return_serum_lactate_dehydrogenase INT,
    return_hemoglobin_f_test_result INT,
    return_hemoglobin_a_test_result INT,
    return_hemoglobin_s_test_result INT,
    return_hemoglobin_a2c_test_result INT,
    return_urinalysis_pus_cells TINYINT,
    return_urinalysis_protein TINYINT,
    return_urinalysis_leucocytes TINYINT,
    return_urinalysis_ketones TINYINT,
    return_urinalysis_glucose TINYINT,
    return_urinalysis_nitrites TINYINT,
    return_reticulocytes TINYINT,
    return_total_psa INT,
    return_carcinoembryonic_antigen_test INT,
    return_carbohydrate_antigen_19_9 INT,
    return_lab_results_notes VARCHAR(200),
    return_other_radiology_orders VARCHAR(100),
    return_diagnosis_established_using VARCHAR(100),
    return_diagnosis_based_on_biopsy_type VARCHAR(100),
    return_biopsy_concordant_with_clinical_suspicions VARCHAR(100),
    return_breast_cancer_type TINYINT,
    return_diagnosis_date DATETIME,
    return_cancer_stage TINYINT,
    return_overall_cancer_stage_group TINYINT,
    return_cancer_staging_date DATETIME,
    return_metastasis_region VARCHAR(100),
    return_treatment_plan TINYINT,
    return_reason_for_surgery TINYINT,
    return_surgical_procedure_done VARCHAR(100),
    return_radiation_location TINYINT,
    return_treatment_hospitalization_required TINYINT,
    return_chemotherapy_plan TINYINT,
    return_total_planned_chemotherapy_cycles TINYINT,
    return_current_chemotherapy_cycle TINYINT,
    return_reasons_for_changing_modifying_or_stopping_chemo TINYINT,
    return_chemotherapy_intent TINYINT,
    return_chemotherapy_start_date DATETIME,
    return_chemotherapy_regimen VARCHAR(100),
    return_chemotherapy_drug_started VARCHAR(300),
    return_chemotherapy_drug_dosage_in_mg VARCHAR(50),
    return_chemotherapy_drug_route VARCHAR(100),
    return_chemotherapy_medication_frequency VARCHAR(100),
    return_non_chemotherapy_drug VARCHAR(200),
    return_other_non_coded_chemotherapy_drug VARCHAR(200),
    return_non_chemotherapy_drug_purpose VARCHAR(100),
    return_hospitalization_due_to_toxicity TINYINT,
    return_presence_of_neurotoxicity TINYINT,
    return_disease_status_at_end_of_treatment TINYINT,
    return_assessment_notes VARCHAR(200),
    return_general_plan_referrals VARCHAR(200),
    return_rtc_date DATETIME,
    prev_encounter_datetime_breast_cancer_treatment DATETIME,
    next_encounter_datetime_breast_cancer_treatment DATETIME,
    prev_encounter_type_breast_cancer_treatment MEDIUMINT,
    next_encounter_type_breast_cancer_treatment MEDIUMINT,
    prev_clinical_datetime_breast_cancer_treatment DATETIME,
    next_clinical_datetime_breast_cancer_treatment DATETIME,
    prev_clinical_location_id_breast_cancer_treatment MEDIUMINT,
    next_clinical_location_id_breast_cancer_treatment MEDIUMINT,
    prev_clinical_rtc_date_breast_cancer_treatment DATETIME,
    next_clinical_rtc_date_breast_cancer_treatment DATETIME,
    PRIMARY KEY encounter_id (encounter_id),
    INDEX person_date (person_id, encounter_datetime),
    INDEX location_enc_date (location_uuid, encounter_datetime),
    INDEX enc_date_location (encounter_datetime, location_uuid),
    INDEX location_id_rtc_date (location_id, rtc_date),
    INDEX location_uuid_rtc_date (location_uuid, rtc_date),
    INDEX loc_id_enc_date_next_clinical (location_id, encounter_datetime , next_clinical_datetime_breast_cancer_treatment),
    INDEX encounter_type (encounter_type),
    INDEX date_created (date_created)
  );        

  IF (@query_type = "build") THEN
      SELECT 'BUILDING..........................................';

      SET @write_table := CONCAT("flat_breast_cancer_treatment_temp_", queue_number);
      SET @queue_table := CONCAT("flat_breast_cancer_treatment_build_queue_", queue_number);
      SET @primary_queue_table := "flat_breast_cancer_treatment_build_queue";
                  
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
      SET @write_table := "flat_breast_cancer_treatment";
      SET @queue_table := "flat_breast_cancer_treatment_sync_queue";
      
      CREATE TABLE IF NOT EXISTS flat_breast_cancer_treatment_sync_queue (
        person_id INT PRIMARY KEY
      );                            
                    
      SET @last_update := NULL;

      SELECT 
        MAX(date_updated)
      INTO @last_update FROM
        etl.flat_log
      WHERE
        table_name = @table_version;					

      REPLACE INTO flat_breast_cancer_treatment_sync_queue
      (SELECT DISTINCT patient_id
        from amrs.encounter
        where date_changed > @last_update
      );

      REPLACE INTO flat_breast_cancer_treatment_sync_queue
      (SELECT DISTINCT person_id
        from etl.flat_obs
        where max_date_created > @last_update
      );

      REPLACE INTO flat_breast_cancer_treatment_sync_queue
      (SELECT DISTINCT person_id
        from etl.flat_lab_obs
        where max_date_created > @last_update
      );

      REPLACE INTO flat_breast_cancer_treatment_sync_queue
      (SELECT DISTINCT person_id
        from etl.flat_orders
        where max_date_created > @last_update
      );
                    
      REPLACE INTO flat_breast_cancer_treatment_sync_queue
      (SELECT person_id FROM 
        amrs.person 
        where date_voided > @last_update);

      REPLACE INTO flat_breast_cancer_treatment_sync_queue
      (SELECT person_id FROM 
        amrs.person 
        where date_changed > @last_update);
  END IF;

  -- Remove test patients
  SET @dyn_sql := CONCAT('DELETE t1 FROM ',@queue_table,' t1
    JOIN amrs.person_attribute t2 using (person_id)
    where t2.person_attribute_type_id = 28 and value="true" and voided=0');
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
                
    DROP TEMPORARY TABLE IF EXISTS flat_breast_cancer_treatment_build_queue__0;
    SET @dyn_sql := CONCAT('CREATE TEMPORARY TABLE flat_breast_cancer_treatment_build_queue__0 (person_id int primary key) (SELECT * FROM ',@queue_table,' limit ',cycle_size,');'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
            
    DROP TEMPORARY TABLE IF EXISTS flat_breast_cancer_treatment_0a;
                
    SET @dyn_sql := CONCAT(
        'CREATE TEMPORARY TABLE flat_breast_cancer_treatment_0a
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
            WHEN t1.encounter_type in ', @clinical_encounter_types,' then 1
            ELSE null
          end as is_clinical_encounter,
          CASE
            WHEN t1.encounter_type in ', @non_clinical_encounter_types,' then 20
            WHEN t1.encounter_type in ', @clinical_encounter_types,' then 10
            WHEN t1.encounter_type in', @other_encounter_types, ' then 5
            ELSE 1
          END AS encounter_type_sort_index,
          t2.orders
        FROM etl.flat_obs t1
          JOIN flat_breast_cancer_treatment_build_queue__0 t0 USING (person_id)
          LEFT JOIN etl.flat_orders t2 using(encounter_id)
        WHERE t1.encounter_type IN ', @encounter_types,');');
                    
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  

    INSERT INTO flat_breast_cancer_treatment_0a
    (SELECT
        t1.person_id,
        NULL,
        t1.encounter_id,
        t1.test_datetime,
        t1.encounter_type,
        NULL, 
        -- t1.location_id,
        t1.obs,
        NULL,
        -- obs_datetimes
        -- in any visit, there many be multiple encounters. for this dataset, we want to include only clinical encounters (e.g. not lab or triage visit)
        0 as is_clinical_encounter,
        1 as encounter_type_sort_index,
        NULL AS orders
      FROM etl.flat_lab_obs t1
      JOIN 
        flat_breast_cancer_treatment_build_queue__0 t0 using (person_id)
    );

    DROP TEMPORARY TABLE IF EXISTS flat_breast_cancer_treatment_0;
    CREATE TEMPORARY TABLE flat_breast_cancer_treatment_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
    (SELECT * FROM flat_breast_cancer_treatment_0a
        ORDER BY person_id, date(encounter_datetime), encounter_type_sort_index
    );

    SET @initial_purpose_of_visit := NULL;
    SET @initial_chief_complaint := NULL;
    SET @initial_pain := NULL;
    SET @initial_pain_score := NULL;
    SET @initial_review_of_systems_heent := NULL;
    SET @initial_review_of_systems_general := NULL;
    SET @initial_review_of_systems_cardiopulmonary := NULL;
    SET @initial_review_of_systems_gastrointestinal := NULL;
    SET @initial_review_of_systems_genitourinary := NULL;
    SET @initial_last_menstrual_period_date := NULL;
    SET @initial_review_of_systems_musculoskeletal := NULL;
    SET @initial_ecog_index := NULL;
    SET @initial_general_exam_findings := NULL;
    SET @initial_heent_exam_findings := NULL;
    SET @initial_chest_exam_findings := NULL;
    SET @initial_breast_exam_findings := NULL;
    SET @initial_breast_finding_location := NULL;
    SET @initial_breast_finding_quadrant := NULL;
    SET @initial_heart_exam_findings := NULL;
    SET @initial_abdomen_exam_findings := NULL;
    SET @initial_urogenital_exam_findings := NULL;
    SET @initial_extremities_exam_findings := NULL;
    SET @initial_testicular_exam_findings := NULL;
    SET @initial_nodal_survey_exam_findings := NULL;
    SET @initial_musculoskeletal_exam_findings := NULL;
    SET @initial_neurologic_exam_findings := NULL;
    SET @initial_physical_examination_notes := NULL;
    SET @initial_patient_currently_on_chemo := NULL;
    SET @initial_ct_head := NULL;
    SET @initial_ct_neck := NULL;
    SET @initial_ct_chest := NULL;
    SET @initial_ct_spine := NULL;
    SET @initial_ct_abdomen := NULL;
    SET @initial_ultrasound_renal := NULL;
    SET @initial_ultrasound_hepatic := NULL;
    SET @initial_ultrasound_obstetric := NULL;
    SET @initial_ultrasound_abdominal := NULL;
    SET @initial_ultrasound_breast := NULL;
    SET @initial_xray_shoulder := NULL;
    SET @initial_xray_pelvis := NULL;
    SET @initial_xray_abdomen := NULL;
    SET @initial_xray_skull := NULL;
    SET @initial_xray_leg := NULL;
    SET @initial_xray_hand := NULL;
    SET @initial_xray_foot := NULL;
    SET @initial_xray_chest := NULL;
    SET @initial_xray_arm := NULL;
    SET @initial_xray_spine := NULL;
    SET @initial_echocardiography := NULL;
    SET @initial_mri_head := NULL;
    SET @initial_mri_neck := NULL;
    SET @initial_mri_arms := NULL;
    SET @initial_mri_chest := NULL;
    SET @initial_mri_spine := NULL;
    SET @initial_mri_abdomen := NULL;
    SET @initial_mri_pelvis := NULL;
    SET @initial_mri_legs := NULL;
    SET @initial_imaging_results_description := NULL;
    SET @initial_rbc_test_result := NULL;
    SET @initial_hgb_test_result := NULL;
    SET @initial_mcv_test_result := NULL;
    SET @initial_mch_test_result := NULL;
    SET @initial_mchc_test_result := NULL;
    SET @initial_rdw_test_result := NULL;
    SET @initial_platelets_test_result := NULL;
    SET @initial_wbc_test_result := NULL;
    SET @initial_anc_test_result := NULL;
    SET @initial_pcv_test_result := NULL;
    SET @initial_urea_test_result := NULL;
    SET @initial_creatinine_test_result := NULL;
    SET @initial_sodium_test_result := NULL;
    SET @initial_potassium_test_result := NULL;
    SET @initial_chloride_test_result := NULL;
    SET @initial_serum_total_bilirubin_test_result := NULL;
    SET @initial_serum_direct_bilirubin_test_result := NULL;
    SET @initial_gamma_glutamyl_transferase_test_result := NULL;
    SET @initial_serum_glutamic_oxaloacetic_transaminase_test_result := NULL;
    SET @initial_serum_total_protein := NULL;
    SET @initial_serum_albumin := NULL;
    SET @initial_serum_alkaline_phosphate := NULL;
    SET @initial_serum_lactate_dehydrogenase := NULL;
    SET @initial_hemoglobin_f_test_result := NULL;
    SET @initial_hemoglobin_a_test_result := NULL;
    SET @initial_hemoglobin_s_test_result := NULL;
    SET @initial_hemoglobin_a2c_test_result := NULL;
    SET @initial_urinalysis_pus_cells := NULL;
    SET @initial_urinalysis_protein := NULL;
    SET @initial_urinalysis_leucocytes := NULL;
    SET @initial_urinalysis_ketones := NULL;
    SET @initial_urinalysis_glucose := NULL;
    SET @initial_urinalysis_nitrites := NULL;
    SET @initial_reticulocytes := NULL;
    SET @initial_total_psa := NULL;
    SET @initial_carcinoembryonic_antigen_test := NULL;
    SET @initial_carbohydrate_antigen_19_9 := NULL;
    SET @initial_lab_results_notes := NULL;
    SET @initial_other_radiology_orders := NULL;
    SET @initial_diagnosis_established_using := NULL;
    SET @initial_diagnosis_based_on_biopsy_type := NULL;
    SET @initial_biopsy_concordant_with_clinical_suspicions := NULL;
    SET @initial_breast_cancer_type := NULL;
    SET @initial_diagnosis_date := NULL;
    SET @initial_cancer_stage := NULL;
    SET @initial_overall_cancer_stage_group := NULL;
    SET @initial_cancer_staging_date := NULL;
    SET @initial_metastasis_region := NULL;
    SET @initial_treatment_plan := NULL;
    SET @initial_reason_for_surgery := NULL;
    SET @initial_surgical_procedure_done := NULL;
    SET @initial_radiation_location := NULL;
    SET @initial_treatment_hospitalization_required := NULL;
    SET @initial_chemotherapy_plan := NULL;
    SET @initial_total_planned_chemotherapy_cycles := NULL;
    SET @initial_current_chemotherapy_cycle := NULL;
    SET @initial_reasons_for_changing_modifying_or_stopping_chemo := NULL;
    SET @initial_chemotherapy_intent := NULL;
    SET @initial_chemotherapy_start_date := NULL;
    SET @initial_chemotherapy_regimen := NULL;
    SET @initial_chemotherapy_drug_started := NULL;
    SET @initial_chemotherapy_drug_dosage_in_mg := NULL;
    SET @initial_chemotherapy_drug_route := NULL;
    SET @initial_chemotherapy_medication_frequency := NULL;
    SET @initial_non_chemotherapy_drug := NULL;
    SET @initial_other_non_coded_chemotherapy_drug := NULL;
    SET @initial_non_chemotherapy_drug_purpose := NULL;
    SET @initial_hospitalization_due_to_toxicity := NULL;
    SET @initial_presence_of_neurotoxicity := NULL;
    SET @initial_disease_status_at_end_of_treatment := NULL;
    SET @initial_assessment_notes := NULL;
    SET @initial_general_plan_referrals := NULL;
    SET @rtc_date := NULL;
    SET @return_purpose_of_visit := NULL;
    SET @return_chief_complaint := NULL;
    SET @return_pain := NULL;
    SET @return_pain_score := NULL;
    SET @return_review_of_systems_heent := NULL;
    SET @return_review_of_systems_general := NULL;
    SET @return_review_of_systems_cardiopulmonary := NULL;
    SET @return_review_of_systems_gastrointestinal := NULL;
    SET @return_review_of_systems_genitourinary := NULL;
    SET @return_last_menstrual_period_date := NULL;
    SET @return_review_of_systems_musculoskeletal := NULL;
    SET @return_ecog_index := NULL;
    SET @return_general_exam_findings := NULL;
    SET @return_heent_exam_findings := NULL;
    SET @return_chest_exam_findings := NULL;
    SET @return_breast_exam_findings := NULL;
    SET @return_breast_finding_location := NULL;
    SET @return_breast_finding_quadrant := NULL;
    SET @return_heart_exam_findings := NULL;
    SET @return_abdomen_exam_findings := NULL;
    SET @return_urogenital_exam_findings := NULL;
    SET @return_extremities_exam_findings := NULL;
    SET @return_testicular_exam_findings := NULL;
    SET @return_nodal_survey_exam_findings := NULL;
    SET @return_musculoskeletal_exam_findings := NULL;
    SET @return_neurologic_exam_findings := NULL;
    SET @return_physical_examination_notes := NULL;
    SET @return_patient_currently_on_chemo := NULL;
    SET @return_ct_head := NULL;
    SET @return_ct_neck := NULL;
    SET @return_ct_chest := NULL;
    SET @return_ct_spine := NULL;
    SET @return_ct_abdomen := NULL;
    SET @return_ultrasound_renal := NULL;
    SET @return_ultrasound_hepatic := NULL;
    SET @return_ultrasound_obstetric := NULL;
    SET @return_ultrasound_abdominal := NULL;
    SET @return_ultrasound_breast := NULL;
    SET @return_xray_shoulder := NULL;
    SET @return_xray_pelvis := NULL;
    SET @return_xray_abdomen := NULL;
    SET @return_xray_skull := NULL;
    SET @return_xray_leg := NULL;
    SET @return_xray_hand := NULL;
    SET @return_xray_foot := NULL;
    SET @return_xray_chest := NULL;
    SET @return_xray_arm := NULL;
    SET @return_xray_spine := NULL;
    SET @return_echocardiography := NULL;
    SET @return_mri_head := NULL;
    SET @return_mri_neck := NULL;
    SET @return_mri_arms := NULL;
    SET @return_mri_chest := NULL;
    SET @return_mri_spine := NULL;
    SET @return_mri_abdomen := NULL;
    SET @return_mri_pelvis := NULL;
    SET @return_mri_legs := NULL;
    SET @return_imaging_results_description := NULL;
    SET @return_rbc_test_result := NULL;
    SET @return_hgb_test_result := NULL;
    SET @return_mcv_test_result := NULL;
    SET @return_mch_test_result := NULL;
    SET @return_mchc_test_result := NULL;
    SET @return_rdw_test_result := NULL;
    SET @return_platelets_test_result := NULL;
    SET @return_wbc_test_result := NULL;
    SET @return_anc_test_result := NULL;
    SET @return_pcv_test_result := NULL;
    SET @return_urea_test_result := NULL;
    SET @return_creatinine_test_result := NULL;
    SET @return_sodium_test_result := NULL;
    SET @return_potassium_test_result := NULL;
    SET @return_chloride_test_result := NULL;
    SET @return_serum_total_bilirubin_test_result := NULL;
    SET @return_serum_direct_bilirubin_test_result := NULL;
    SET @return_gamma_glutamyl_transferase_test_result := NULL;
    SET @return_serum_glutamic_oxaloacetic_transaminase_test_result := NULL;
    SET @return_serum_total_protein := NULL;
    SET @return_serum_albumin := NULL;
    SET @return_serum_alkaline_phosphate := NULL;
    SET @return_serum_lactate_dehydrogenase := NULL;
    SET @return_hemoglobin_f_test_result := NULL;
    SET @return_hemoglobin_a_test_result := NULL;
    SET @return_hemoglobin_s_test_result := NULL;
    SET @return_hemoglobin_a2c_test_result := NULL;
    SET @return_urinalysis_pus_cells := NULL;
    SET @return_urinalysis_protein := NULL;
    SET @return_urinalysis_leucocytes := NULL;
    SET @return_urinalysis_ketones := NULL;
    SET @return_urinalysis_glucose := NULL;
    SET @return_urinalysis_nitrites := NULL;
    SET @return_reticulocytes := NULL;
    SET @return_total_psa := NULL;
    SET @return_carcinoembryonic_antigen_test := NULL;
    SET @return_carbohydrate_antigen_19_9 := NULL;
    SET @return_lab_results_notes := NULL;
    SET @return_other_radiology_orders := NULL;
    SET @return_diagnosis_established_using := NULL;
    SET @return_diagnosis_based_on_biopsy_type := NULL;
    SET @return_biopsy_concordant_with_clinical_suspicions := NULL;
    SET @return_breast_cancer_type := NULL;
    SET @return_diagnosis_date := NULL;
    SET @return_cancer_stage := NULL;
    SET @return_overall_cancer_stage_group := NULL;
    SET @return_cancer_staging_date := NULL;
    SET @return_metastasis_region := NULL;
    SET @return_treatment_plan := NULL;
    SET @return_reason_for_surgery := NULL;
    SET @return_surgical_procedure_done := NULL;
    SET @return_radiation_location := NULL;
    SET @return_treatment_hospitalization_required := NULL;
    SET @return_chemotherapy_plan := NULL;
    SET @return_total_planned_chemotherapy_cycles := NULL;
    SET @return_current_chemotherapy_cycle := NULL;
    SET @return_reasons_for_changing_modifying_or_stopping_chemo := NULL;
    SET @return_chemotherapy_intent := NULL;
    SET @return_chemotherapy_start_date := NULL;
    SET @return_chemotherapy_regimen := NULL;
    SET @return_chemotherapy_drug_started := NULL;
    SET @return_chemotherapy_drug_dosage_in_mg := NULL;
    SET @return_chemotherapy_drug_route := NULL;
    SET @return_chemotherapy_medication_frequency := NULL;
    SET @return_non_chemotherapy_drug := NULL;
    SET @return_other_non_coded_chemotherapy_drug := NULL;
    SET @return_non_chemotherapy_drug_purpose := NULL;
    SET @return_hospitalization_due_to_toxicity := NULL;
    SET @return_presence_of_neurotoxicity := NULL;
    SET @return_disease_status_at_end_of_treatment := NULL;
    SET @return_assessment_notes := NULL;
    SET @return_general_plan_referrals := NULL;
    SET @return_rtc_date := NULL;
                                        
    DROP TEMPORARY TABLE IF EXISTS flat_breast_cancer_treatment_1;
    CREATE TEMPORARY TABLE flat_breast_cancer_treatment_1 #(index encounter_id (encounter_id))
    (SELECT
      encounter_type_sort_index,
      @prev_id := @cur_id AS prev_id,
      @cur_id := t1.person_id AS cur_id,
      t1.person_id,
      t1.encounter_id,
      t1.encounter_type,
      t1.encounter_datetime,
      t1.visit_id,
      t1.location_id,
      t1.is_clinical_encounter,
      t2.uuid AS location_uuid,
      p.gender,
      CASE
        WHEN TIMESTAMPDIFF(year, birthdate, curdate()) > 0 THEN ROUND(TIMESTAMPDIFF(year, birthdate, curdate()), 0)
        ELSE ROUND(TIMESTAMPDIFF(month, birthdate, curdate()) / 12, 2)
      END AS age,
			p.death_date,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1834=7850!!" THEN @initial_purpose_of_visit := 1 -- Initial visit
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1834=10037!!" THEN @initial_purpose_of_visit :=  2 -- Second opinion
        ELSE @initial_purpose_of_visit := NULL
      END AS initial_purpose_of_visit,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!5219=5006!!" THEN @initial_chief_complaint := 1 -- Asymptomatic
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!5219=1068!!" THEN @initial_chief_complaint := 2 -- Symptomatic
        ELSE @initial_chief_complaint := NULL
      END AS initial_chief_complaint,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6613=1065!!" THEN @initial_pain := 1 -- Yes
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6613=1066!!" THEN @initial_pain := 2 -- No
        ELSE @initial_pain := NULL
      END AS initial_pain,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!7080=" THEN @initial_pain_score := GetValues(obs, 7080)
        ELSE @initial_pain_score := NULL
      END AS initial_pain_score,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1070=" THEN @initial_review_of_systems_heent := GetValues(obs, 1070)
        ELSE @initial_review_of_systems_heent := NULL
      END AS initial_review_of_systems_heent,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1069=" THEN @initial_review_of_systems_general := GetValues(obs, 1069)
        ELSE @initial_review_of_systems_general := NULL
      END AS initial_review_of_systems_general,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1071=" THEN @initial_review_of_systems_cardiopulmonary := GetValues(obs, 1071)
        ELSE @initial_review_of_systems_cardiopulmonary := NULL
      END AS initial_review_of_systems_cardiopulmonary,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1078=" THEN @initial_review_of_systems_gastrointestinal := GetValues(obs, 1078)
        ELSE @initial_review_of_systems_gastrointestinal := NULL
      END AS initial_review_of_systems_gastrointestinal,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1080=" THEN @initial_review_of_systems_genitourinary := GetValues(obs, 1080)
        ELSE @initial_review_of_systems_genitourinary := NULL
      END AS initial_review_of_systems_genitourinary,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1836=" THEN @initial_last_menstrual_period_date := GetValues(obs, 1836)
        ELSE @initial_last_menstrual_period_date := NULL
      END AS initial_last_menstrual_period_date,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1081=" THEN @initial_review_of_systems_musculoskeletal := GetValues(obs, 1081)
        ELSE @initial_review_of_systems_musculoskeletal := NULL
      END AS initial_review_of_systems_musculoskeletal,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6584=1115!!" THEN @initial_ecog_index := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6584=6585!!" THEN @initial_ecog_index := 2 -- Symptomatic but ambulatory
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6584=6586!!" THEN @initial_ecog_index := 3 -- Debilitated, bedridden less than 80% of the day
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6584=6587!!" THEN @initial_ecog_index := 4 -- Debilitated, greater than 50% of the day
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6584=6588!!" THEN @initial_ecog_index := 5 -- Bedridden 100%
        ELSE @initial_ecog_index := NULL
      END AS initial_ecog_index,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1119=" THEN @initial_general_exam_findings := GetValues(obs, 1119)
        ELSE @initial_general_exam_findings := NULL
      END AS initial_general_exam_findings,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1122=" THEN @initial_heent_exam_findings := GetValues(obs, 1122)
        ELSE @initial_heent_exam_findings := NULL
      END AS initial_heent_exam_findings,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1123=" THEN @initial_chest_exam_findings := GetValues(obs, 1123)
        ELSE @initial_chest_exam_findings := NULL
      END AS initial_chest_exam_findings,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6251=" THEN @initial_breast_exam_findings := GetValues(obs, 6251)
        ELSE @initial_breast_exam_findings := NULL
      END AS initial_breast_exam_findings,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9696=" THEN @initial_breast_finding_location := GetValues(obs, 9696)
        ELSE @initial_breast_finding_location := NULL
      END AS initial_breast_finding_location,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1239=" THEN @initial_breast_finding_quadrant := GetValues(obs, 1239)
        ELSE @initial_breast_finding_quadrant := NULL
      END AS initial_breast_finding_quadrant,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1124=" THEN @initial_heart_exam_findings := GetValues(obs, 1124)
        ELSE @initial_heart_exam_findings := NULL
      END AS initial_heart_exam_findings,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1125=" THEN @initial_abdomen_exam_findings := GetValues(obs, 1125)
        ELSE @initial_abdomen_exam_findings := NULL
      END AS initial_abdomen_exam_findings,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1126=" THEN @initial_urogenital_exam_findings := GetValues(obs, 1126)
        ELSE @initial_urogenital_exam_findings := NULL
      END AS initial_urogenital_exam_findings,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1127=" THEN @initial_extremities_exam_findings := GetValues(obs, 1127)
        ELSE @initial_extremities_exam_findings := NULL
      END AS initial_extremities_exam_findings,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!8420=" THEN @initial_testicular_exam_findings := GetValues(obs, 8420)
        ELSE @initial_testicular_exam_findings := NULL
      END AS initial_testicular_exam_findings,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1121=" THEN @initial_nodal_survey_exam_findings := GetValues(obs, 1121)
        ELSE @initial_nodal_survey_exam_findings := NULL
      END AS initial_nodal_survey_exam_findings,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1128=" THEN @initial_musculoskeletal_exam_findings := GetValues(obs, 1128)
        ELSE @initial_musculoskeletal_exam_findings := NULL
      END AS initial_musculoskeletal_exam_findings,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1129=" THEN @initial_neurologic_exam_findings := GetValues(obs, 1129)
        ELSE @initial_neurologic_exam_findings := NULL
      END AS initial_neurologic_exam_findings,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!2018=" THEN @initial_physical_examination_notes := GetValues(obs, 2018)
        ELSE @initial_physical_examination_notes := NULL
      END AS initial_physical_examination_notes,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6575=1065" THEN @initial_patient_currently_on_chemo := 1 -- Yes
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6575=1107" THEN @initial_patient_currently_on_chemo := 2 -- No
        ELSE @initial_patient_currently_on_chemo := NULL
      END AS initial_patient_currently_on_chemo,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!846=1115!!" THEN @initial_ct_head := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!846=1116!!" THEN @initial_ct_head := 2 -- Abnormal
        ELSE @initial_ct_head := NULL
      END AS initial_ct_head,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9839=1115!!" THEN @initial_ct_neck := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9839=1116!!" THEN @initial_ct_neck := 2 -- Abnormal
        ELSE @initial_ct_neck := NULL
      END AS initial_ct_neck,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9839=1115!!" THEN @initial_ct_chest := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9839=1116!!" THEN @initial_ct_chest := 2 -- Abnormal
        ELSE @initial_ct_chest := NULL
      END AS initial_ct_chest,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9840=1115!!" THEN @initial_ct_spine := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9840=1116!!" THEN @initial_ct_spine := 2 -- Abnormal
        ELSE @initial_ct_spine := NULL
      END AS initial_ct_spine,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!7114=1115!!" THEN @initial_ct_abdomen := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!7114=1116!!" THEN @initial_ct_abdomen := 2 -- Abnormal
        ELSE @initial_ct_abdomen := NULL
      END AS initial_ct_abdomen,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!7115=1115!!" THEN @initial_ultrasound_renal := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!7115=1116!!" THEN @initial_ultrasound_renal := 2 -- Abnormal
        ELSE @initial_ultrasound_renal := NULL
      END AS initial_ultrasound_renal,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!852=1115!!" THEN @initial_ultrasound_hepatic := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!852=1116!!" THEN @initial_ultrasound_hepatic := 2 -- Abnormal
        ELSE @initial_ultrasound_hepatic := NULL
      END AS initial_ultrasound_hepatic,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6221=1115!!" THEN @initial_ultrasound_obstetric := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6221=1116!!" THEN @initial_ultrasound_obstetric := 2 -- Annormal
        ELSE @initial_ultrasound_obstetric := NULL
      END AS initial_ultrasound_obstetric,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!845=1115!!" THEN @initial_ultrasound_abdominal := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!845=1116!!" THEN @initial_ultrasound_abdominal := 2 -- Abnormal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!845=5103!!" THEN @initial_ultrasound_abdominal := 3 -- Abdominal mass
        ELSE @initial_ultrasound_abdominal := NULL
      END AS initial_ultrasound_abdominal,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9596=1115!!" THEN @initial_ultrasound_breast := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9596=1116!!" THEN @initial_ultrasound_breast := 2 -- Abnormal
        ELSE @initial_ultrasound_breast := NULL
      END AS initial_ultrasound_breast,
      CASE
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!394=1115!!" THEN @initial_xray_shoulder := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!394=1116!!" THEN @initial_xray_shoulder := 2 -- Abnormal
        ELSE @initial_xray_shoulder := NULL
      END AS initial_xray_shoulder,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!392=1115!!" THEN @initial_xray_pelvis := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!392=1116!!" THEN @initial_xray_pelvis := 2 -- Abnormal
        ELSE @initial_xray_pelvis := NULL
      END AS initial_xray_pelvis,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!101=1115!!" THEN @initial_xray_abdomen := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!101=1116!!" THEN @initial_xray_abdomen := 2 -- Abnormal
        ELSE @initial_xray_abdomen := NULL
      END AS initial_xray_abdomen,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!386=1115!!" THEN @initial_xray_skull := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!386=1116!!" THEN @initial_xray_skull := 2 -- Abnormal
        ELSE @initial_xray_skull := NULL
      END AS initial_xray_skull,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!380=1115!!" THEN @initial_xray_leg := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!380=1116!!" THEN @initial_xray_leg := 2 -- Abnormal
        ELSE @initial_xray_leg := NULL
      END AS initial_xray_leg,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!382=1115!!" THEN @initial_xray_hand := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!382=1116!!" THEN @initial_xray_hand := 2 -- Abnormal
        ELSE @initial_xray_hand := NULL
      END AS initial_xray_hand,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!384=1115!!" THEN @initial_xray_foot := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!384=1116!!" THEN @initial_xray_foot := 2 -- Abnormal
        ELSE @initial_xray_foot := NULL
      END AS initial_xray_foot,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!12=1115!!" THEN @initial_xray_chest := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!12=1116!!" THEN @initial_xray_chest := 2 -- Abnormal
        ELSE @initial_xray_chest := NULL
      END AS initial_xray_chest,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!377=1115!!" THEN @initial_xray_arm := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!377=1116!!" THEN @initial_xray_arm := 2 -- Abnormal
        ELSE @initial_xray_arm := NULL
      END AS initial_xray_arm,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!390=1115!!" THEN @initial_xray_spine := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!390=1116!!" THEN @initial_xray_spine := 2 -- Abnormal
        ELSE @initial_xray_spine := NULL
      END AS initial_xray_spine,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1536=1115!!" THEN @initial_echocardiography := 1 -- Normal 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1536=1116!!" THEN @initial_echocardiography := 2 -- Abnormal 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1536=1538!!" THEN @initial_echocardiography := 3 -- Dilated cardiomyopathy 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1536=1532!!" THEN @initial_echocardiography := 4 -- Left ventricular hypertrophy 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1536=1533!!" THEN @initial_echocardiography := 5 -- Right ventricular hypertrophy 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1536=5622!!" THEN @initial_echocardiography := 6 -- Other (non-coded)
        ELSE @initial_echocardiography := NULL
      END AS initial_echocardiography,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9881=1115!!" THEN @initial_mri_head := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9881=1116!!" THEN @initial_mri_head := 2 -- Abnormal
        ELSE @initial_mri_head := NULL
      END AS initial_mri_head,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9882=1115!!" THEN @initial_mri_neck := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9882=1116!!" THEN @initial_mri_neck := 2 -- Abnormal 
        ELSE @initial_mri_neck := NULL
      END AS initial_mri_neck,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9951=1115!!" THEN @initial_mri_arms := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9951=1116!!" THEN @initial_mri_arms := 2 -- Abnormal 
        ELSE @initial_mri_arms := NULL
      END AS initial_mri_arms,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9883=1115!!" THEN @initial_mri_chest := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9883=1116!!" THEN @initial_mri_chest := 2 -- Abnormal 
        ELSE @initial_mri_chest := NULL
      END AS initial_mri_chest,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9885=1115!!" THEN @initial_mri_spine := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9885=1116!!" THEN @initial_mri_spine := 2 -- Abnormal 
        ELSE @initial_mri_spine := NULL
      END AS initial_mri_spine,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9884=1115!!" THEN @initial_mri_abdomen := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9884=1116!!" THEN @initial_mri_abdomen := 2 -- Abnormal 
        ELSE @initial_mri_abdomen := NULL
      END AS initial_mri_abdomen,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9952=1115!!" THEN @initial_mri_pelvis := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9952=1116!!" THEN @initial_mri_pelvis := 2 -- Abnormal 
        ELSE @initial_mri_pelvis := NULL
      END AS initial_mri_pelvis,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9953=1115!!" THEN @initial_mri_legs := 1 -- Normal
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9953=1116!!" THEN @initial_mri_legs := 2 -- Abnormal 
        ELSE @initial_mri_legs := NULL
      END AS initial_mri_legs,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!10077=" THEN @initial_imaging_results_description := GetValues(obs, 10077)
        ELSE @initial_imaging_results_description := NULL
      END AS initial_imaging_results_description,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!679=" THEN @initial_rbc_test_result := GetValues(obs, 679) 
        ELSE @initial_rbc_test_result := NULL
      END AS initial_rbc_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!21=" THEN @initial_hgb_test_result := GetValues(obs, 21) 
        ELSE @initial_hgb_test_result := NULL
      END AS initial_hgb_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!851=" THEN @initial_mcv_test_result := GetValues(obs, 851) 
        ELSE @initial_mcv_test_result := NULL
      END AS initial_mcv_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1018=" THEN @initial_mch_test_result := GetValues(obs, 1018) 
        ELSE @initial_mch_test_result := NULL
      END AS initial_mch_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1017=" THEN @initial_mchc_test_result := GetValues(obs, 1017) 
        ELSE @initial_mchc_test_result := NULL
      END AS initial_mchc_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1016=" THEN @initial_rdw_test_result := GetValues(obs, 1016) 
        ELSE @initial_rdw_test_result := NULL
      END AS initial_rdw_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!729=" THEN @initial_platelets_test_result := GetValues(obs, 729) 
        ELSE @initial_platelets_test_result := NULL
      END AS initial_platelets_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!678=" THEN @initial_wbc_test_result := GetValues(obs, 678) 
        ELSE @initial_wbc_test_result := NULL
      END AS initial_wbc_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1313=" THEN @initial_anc_test_result := GetValues(obs, 1313) 
        ELSE @initial_anc_test_result := NULL
      END AS initial_anc_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1015=" THEN @initial_pcv_test_result := GetValues(obs, 1015) 
        ELSE @initial_pcv_test_result := NULL
      END AS initial_pcv_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6898=" THEN @initial_urea_test_result := GetValues(obs, 6898) 
        ELSE @initial_urea_test_result := NULL
      END AS initial_urea_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!790=" THEN @initial_creatinine_test_result := GetValues(obs, 790) 
        ELSE @initial_creatinine_test_result := NULL
      END AS initial_creatinine_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1132=" THEN @initial_sodium_test_result := GetValues(obs, 1132) 
        ELSE @initial_sodium_test_result := NULL
      END AS initial_sodium_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1133=" THEN @initial_potassium_test_result := GetValues(obs, 1133) 
        ELSE @initial_potassium_test_result := NULL
      END AS initial_potassium_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1134=" THEN @initial_chloride_test_result := GetValues(obs, 1134) 
        ELSE @initial_chloride_test_result := NULL
      END AS initial_chloride_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!655=" THEN @initial_serum_total_bilirubin_test_result := GetValues(obs, 655) 
        ELSE @initial_serum_total_bilirubin_test_result := NULL
      END AS initial_serum_total_bilirubin_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1297=" THEN @initial_serum_direct_bilirubin_test_result := GetValues(obs, 1297) 
        ELSE @initial_serum_direct_bilirubin_test_result := NULL
      END AS initial_serum_direct_bilirubin_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6123=" THEN @initial_gamma_glutamyl_transferase_test_result := GetValues(obs, 6123) 
        ELSE @initial_gamma_glutamyl_transferase_test_result := NULL
      END AS initial_gamma_glutamyl_transferase_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!653=" THEN @initial_serum_glutamic_oxaloacetic_transaminase_test_result := GetValues(obs, 653) 
        ELSE @initial_serum_glutamic_oxaloacetic_transaminase_test_result := NULL
      END AS initial_serum_glutamic_oxaloacetic_transaminase_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!717=" THEN @initial_serum_total_protein := GetValues(obs, 717) 
        ELSE @initial_serum_total_protein := NULL
      END AS initial_serum_total_protein,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!848=" THEN @initial_serum_albumin := GetValues(obs, 848) 
        ELSE @initial_serum_albumin := NULL
      END AS initial_serum_albumin,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!785=" THEN @initial_serum_alkaline_phosphate := GetValues(obs, 785) 
        ELSE @initial_serum_alkaline_phosphate := NULL
      END AS initial_serum_alkaline_phosphate,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1014=" THEN @initial_serum_lactate_dehydrogenase := GetValues(obs, 1014) 
        ELSE @initial_serum_lactate_dehydrogenase := NULL
      END AS initial_serum_lactate_dehydrogenase,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9010=" THEN @initial_hemoglobin_f_test_result := GetValues(obs, 9010) 
        ELSE @initial_hemoglobin_f_test_result := NULL
      END AS initial_hemoglobin_f_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9011=" THEN @initial_hemoglobin_a_test_result := GetValues(obs, 9011) 
        ELSE @initial_hemoglobin_a_test_result := NULL
      END AS initial_hemoglobin_a_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9699=" THEN @initial_hemoglobin_s_test_result := GetValues(obs, 9699) 
        ELSE @initial_hemoglobin_s_test_result := NULL
      END AS initial_hemoglobin_s_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9012=" THEN @initial_hemoglobin_a2c_test_result := GetValues(obs, 9012) 
        ELSE @initial_hemoglobin_a2c_test_result := NULL
      END AS initial_hemoglobin_a2c_test_result,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1984=" THEN @initial_urinalysis_pus_cells := GetValues(obs, 1984) 
        ELSE @initial_urinalysis_pus_cells := NULL
      END AS initial_urinalysis_pus_cells,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!2339=" THEN @initial_urinalysis_protein := GetValues(obs, 2339) 
        ELSE @initial_urinalysis_protein := NULL
      END AS initial_urinalysis_protein,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6337=" THEN @initial_urinalysis_leucocytes := GetValues(obs, 6337) 
        ELSE @initial_urinalysis_leucocytes := NULL
      END AS initial_urinalysis_leucocytes,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!7276=" THEN @initial_urinalysis_ketones := GetValues(obs, 7276) 
        ELSE @initial_urinalysis_ketones := NULL
      END AS initial_urinalysis_ketones,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!2340=" THEN @initial_urinalysis_glucose := GetValues(obs, 2340) 
        ELSE @initial_urinalysis_glucose := NULL
      END AS initial_urinalysis_glucose,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9307=" THEN @initial_urinalysis_nitrites := GetValues(obs, 9307) 
        ELSE @initial_urinalysis_nitrites := NULL
      END AS initial_urinalysis_nitrites,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1327=" THEN @initial_reticulocytes := GetValues(obs, 1327) 
        ELSE @initial_reticulocytes := NULL
      END AS initial_reticulocytes,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!10249=" THEN @initial_total_psa := GetValues(obs, 10249) 
        ELSE @initial_total_psa := NULL
      END AS initial_total_psa,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!10250=" THEN @initial_carcinoembryonic_antigen_test := GetValues(obs, 10250) 
        ELSE @initial_carcinoembryonic_antigen_test := NULL
      END AS initial_carcinoembryonic_antigen_test,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!10251=" THEN @initial_carbohydrate_antigen_19_9 := GetValues(obs, 10251) 
        ELSE @initial_carbohydrate_antigen_19_9 := NULL
      END AS initial_carbohydrate_antigen_19_9,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9538=" THEN @initial_lab_results_notes := GetValues(obs, 9538)
        ELSE @initial_lab_results_notes := NULL
      END AS initial_lab_results_notes,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!8190=" THEN @initial_other_radiology_orders := GetValues(obs, 8190)
        ELSE @initial_other_radiology_orders := NULL
      END AS initial_other_radiology_orders,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6504=" THEN @initial_diagnosis_established_using := GetValues(obs, 6504)
        ELSE @initial_diagnosis_established_using := NULL
      END AS initial_diagnosis_established_using,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6509=" THEN @initial_diagnosis_based_on_biopsy_type := GetValues(obs, 6509)
        ELSE @initial_diagnosis_based_on_biopsy_type := NULL
      END AS initial_diagnosis_based_on_biopsy_type,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6605=" THEN @initial_biopsy_concordant_with_clinical_suspicions := GetValues(obs, 6605)
        ELSE @initial_biopsy_concordant_with_clinical_suspicions := NULL
      END AS initial_biopsy_concordant_with_clinical_suspicions,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9841=6545!!" THEN @initial_breast_cancer_type := 1 -- Inflammatory breast cancer
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9841=9842!!" THEN @initial_breast_cancer_type := 2 -- Ductal cell carcinoma  
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9841=5622!!" THEN @initial_breast_cancer_type := 3 -- Other (non-coded)
        ELSE @initial_breast_cancer_type := NULL
      END AS initial_breast_cancer_type,
      CASE
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9728=" THEN @initial_diagnosis_date := GetValues(obs, 9728)
        ELSE @initial_diagnosis_date := NULL
      END AS initial_diagnosis_date,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6582=6566!!" THEN @initial_cancer_stage := 1 -- Complete staging (TNM)
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6582=10206!!" THEN @initial_cancer_stage := 2 -- Durie salmon system
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6582=1175!!" THEN @initial_cancer_stage := 3 -- Not applicable
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6582=1067!!" THEN @initial_cancer_stage := 4 -- Unknown
        ELSE @initial_cancer_stage := NULL
      END AS initial_cancer_stage,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9868=9851!!" THEN @initial_overall_cancer_stage_group := 1 -- Stage 0
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9868=9852!!" THEN @initial_overall_cancer_stage_group := 2 -- Stage I
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9868=9853!!" THEN @initial_overall_cancer_stage_group := 3 -- Stage IA
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9868=9854!!" THEN @initial_overall_cancer_stage_group := 4 -- Stage IB
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9868=9855!!" THEN @initial_overall_cancer_stage_group := 5 -- Stage IC
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9868=9856!!" THEN @initial_overall_cancer_stage_group := 6 -- Stage II
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9868=9857!!" THEN @initial_overall_cancer_stage_group := 7 -- Stage IIA
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9868=9858!!" THEN @initial_overall_cancer_stage_group := 8 -- Stage IIB
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9868=9859!!" THEN @initial_overall_cancer_stage_group := 9-- Stage IIC
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9868=9860!!" THEN @initial_overall_cancer_stage_group := 10 -- Stage III
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9868=9861!!" THEN @initial_overall_cancer_stage_group := 11 -- Stage IIIA
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9868=9862!!" THEN @initial_overall_cancer_stage_group := 12 -- Stage IIIB
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9868=9863!!" THEN @initial_overall_cancer_stage_group := 13 -- Stage IIIC
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9868=9864!!" THEN @initial_overall_cancer_stage_group := 14 -- Stage IV
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9868=9865!!" THEN @initial_overall_cancer_stage_group := 15 -- Stage IVA
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9868=9867!!" THEN @initial_overall_cancer_stage_group := 16 -- Stage IVC
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9868=9866!!" THEN @initial_overall_cancer_stage_group := 17 -- Stage IVB
        ELSE @initial_overall_cancer_stage_group := NULL
      END AS initial_overall_cancer_stage_group,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!10441=" THEN @initial_cancer_staging_date := GetValues(obs, 10441)
        ELSE @initial_cancer_staging_date := NULL
      END AS initial_cancer_staging_date,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!10445=" THEN @initial_metastasis_region := GetValues(obs, 10445)
        ELSE @initial_metastasis_region := NULL
      END AS initial_metastasis_region,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!8723=10038!!" THEN @initial_treatment_plan := 1 -- Biological therapy
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!8723=6575!!" THEN @initial_treatment_plan := 2 -- Chemotherapy
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!8723=9626!!" THEN @initial_treatment_plan := 3 -- Hormone replacement therapy
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!8723=8427!!" THEN @initial_treatment_plan := 4 -- Radiation therapy
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!8723=7465!!" THEN @initial_treatment_plan := 5 -- Surgery
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!8723=10232!!" THEN @initial_treatment_plan := 6 -- Targeted therapy
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!8723=5622!!" THEN @initial_treatment_plan := 7 -- Other (non-coded)
        ELSE @initial_treatment_plan := NULL
      END AS initial_treatment_plan,
      CASE
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!8725=10233!!" THEN @initial_reason_for_surgery := 1 -- Breast reconstruction
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!8725=8428!!" THEN @initial_reason_for_surgery := 2 -- Curative care
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!8725=8727!!" THEN @initial_reason_for_surgery := 3 -- Diagnostic
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!8725=8724!!" THEN @initial_reason_for_surgery := 4 -- Palliative care
      END AS initial_reason_for_surgery,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!10230=" THEN @initial_surgical_procedure_done := GetValues(obs, 10230)
        ELSE @initial_surgical_procedure_done := NULL
      END AS initial_surgical_procedure_done,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9916=1349!!" THEN @initial_radiation_location := 1 -- Chest
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9916=8193!!" THEN @initial_radiation_location := 2 -- Hands
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9916=9226!!" THEN @initial_radiation_location := 3 -- Stomach
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9916=9227!!" THEN @initial_radiation_location := 4 -- Spine
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9916=9228!!" THEN @initial_radiation_location := 5 -- Bone
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9916=9229!!" THEN @initial_radiation_location := 6 -- Oesophagus
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9916=9230!!" THEN @initial_radiation_location := 7 -- Anus
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9916=9231!!" THEN @initial_radiation_location := 8 -- Rectum
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9916=5622!!" THEN @initial_radiation_location := 9 -- Other (non-coded)
        ELSE @initial_radiation_location := NULL
      END AS initial_radiation_location,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6419=1065!!" THEN @initial_treatment_hospitalization_required := 1 -- Yes
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6419=1066!!" THEN @initial_treatment_hospitalization_required := 2 -- No
        ELSE @initial_treatment_hospitalization_required := NULL
      END AS initial_treatment_hospitalization_required,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9869=1256!!" THEN @initial_chemotherapy_plan := 1 -- Start drugs
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9869=1259!!" THEN @initial_chemotherapy_plan := 2 -- Change regimen
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9869=1260!!" THEN @initial_chemotherapy_plan := 3 -- Stop all medications
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9869=1257!!" THEN @initial_chemotherapy_plan := 4 -- Continue regimen
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9869=6576!!" THEN @initial_chemotherapy_plan := 5 -- Chemotherapy regimen modifications
        ELSE @initial_chemotherapy_plan := NULL
      END AS initial_chemotherapy_plan,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6644=" THEN @initial_total_planned_chemotherapy_cycles := GetValues(obs, 6644)
        ELSE @initial_total_planned_chemotherapy_cycles := NULL
      END AS initial_total_planned_chemotherapy_cycles,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!6643=" THEN @initial_current_chemotherapy_cycle := GetValues(obs, 6643)
        ELSE @initial_current_chemotherapy_cycle := NULL
      END AS initial_current_chemotherapy_cycle,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9927=1267!!" THEN @initial_reasons_for_changing_modifying_or_stopping_chemo := 1 -- Completed
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9927=7391!!" THEN @initial_reasons_for_changing_modifying_or_stopping_chemo := 2 -- No response
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9927=6629!!" THEN @initial_reasons_for_changing_modifying_or_stopping_chemo := 3 -- Progressive disease
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9927=6627!!" THEN @initial_reasons_for_changing_modifying_or_stopping_chemo := 4 -- Partial response
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9927=1879!!" THEN @initial_reasons_for_changing_modifying_or_stopping_chemo := 5 -- Toxicity
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9927=5622!!" THEN @initial_reasons_for_changing_modifying_or_stopping_chemo := 6 -- Other (non-coded)
        ELSE @initial_reasons_for_changing_modifying_or_stopping_chemo := NULL
      END AS initial_reasons_for_changing_modifying_or_stopping_chemo,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!2206=9218!!" THEN @initial_chemotherapy_intent := 1 -- Adjuvant intent
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!2206=8428!!" THEN @initial_chemotherapy_intent := 2 -- Curative intent
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!2206=9219!!" THEN @initial_chemotherapy_intent := 3 -- Neo adjuvant intent
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!2206=8724!!" THEN @initial_chemotherapy_intent := 4 -- Palliative care
        ELSE @initial_chemotherapy_intent := NULL
      END AS initial_chemotherapy_intent,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1190=" THEN @initial_chemotherapy_start_date := GetValues(obs, 1190)
        ELSE @initial_chemotherapy_start_date := NULL
      END AS initial_chemotherapy_start_date,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9946=" THEN @initial_chemotherapy_regimen := GetValues(obs, 9946)
        ELSE @initial_chemotherapy_regimen := NULL
      END AS initial_chemotherapy_regimen,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!9918=" THEN @initial_chemotherapy_drug_started := GetValues(obs, 9918)
        ELSE @initial_chemotherapy_drug_started := NULL
      END AS initial_chemotherapy_drug_started,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1899=" THEN @initial_chemotherapy_drug_dosage_in_mg := GetValues(obs, 1899)
        ELSE @initial_chemotherapy_drug_dosage_in_mg := NULL
      END AS initial_chemotherapy_drug_dosage_in_mg,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!7463=" THEN @initial_chemotherapy_drug_route := GetValues(obs, 7463)
        ELSE @initial_chemotherapy_drug_route := NULL
      END AS initial_chemotherapy_drug_route,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1896=" THEN @initial_chemotherapy_medication_frequency := GetValues(obs, 1896)
        ELSE @initial_chemotherapy_medication_frequency := NULL
      END AS initial_chemotherapy_medication_frequency,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1895=" THEN @initial_non_chemotherapy_drug := GetValues(obs, 1895)
        ELSE @initial_non_chemotherapy_drug := NULL
      END AS initial_non_chemotherapy_drug,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1779=" THEN @initial_other_non_coded_chemotherapy_drug := GetValues(obs, 1779)
        ELSE @initial_other_non_coded_chemotherapy_drug := NULL
      END AS initial_other_non_coded_chemotherapy_drug,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!2206=" THEN @initial_non_chemotherapy_drug_purpose := GetValues(obs, 2206)
        ELSE @initial_non_chemotherapy_drug_purpose := NULL
      END AS initial_non_chemotherapy_drug_purpose,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!10247=1065!!" THEN @initial_hospitalization_due_to_toxicity := 1 -- Yes
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!10247=1066!!" THEN @initial_hospitalization_due_to_toxicity := 2 -- No
        ELSE @initial_hospitalization_due_to_toxicity := NULL
      END AS initial_hospitalization_due_to_toxicity,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!10243=1065!!" THEN @initial_presence_of_neurotoxicity := 1 -- Yes
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!10243=1066!!" THEN @initial_presence_of_neurotoxicity := 2 -- No
        ELSE @initial_presence_of_neurotoxicity := NULL
      END AS initial_presence_of_neurotoxicity,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!10244=9850!!" THEN @initial_disease_status_at_end_of_treatment := 1 -- Cancer recurrence
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!10244=10240!!" THEN @initial_disease_status_at_end_of_treatment := 2 -- No evidence of disease
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!10244=10245!!" THEN @initial_disease_status_at_end_of_treatment := 3 -- Persistently elevated tumor marker
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!10244=10246!!" THEN @initial_disease_status_at_end_of_treatment := 4 -- Recurrence based on imaging
        ELSE @initial_disease_status_at_end_of_treatment := NULL
      END AS initial_disease_status_at_end_of_treatment,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!7222=" THEN @initial_assessment_notes := GetValues(obs, 7222)
        ELSE @initial_assessment_notes := NULL
      END AS initial_assessment_notes,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!1272=" THEN @initial_general_plan_referrals := GetValues(obs, 1272)
        ELSE @initial_general_plan_referrals := NULL
      END AS initial_general_plan_referrals,
      CASE 
        WHEN t1.encounter_type = 142 AND obs REGEXP "!!5096=" THEN @rtc_date := GetValues(obs, 5096)
        ELSE @rtc_date := NULL
      END AS rtc_date,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1834=2345!!" THEN @return_purpose_of_visit := 1 -- Follow-up
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1834=1068!!" THEN @return_purpose_of_visit := 2 -- Symptomatic
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1834=1246!!" THEN @return_purpose_of_visit := 3 -- Scheduled visit
        ELSE @return_purpose_of_visit := NULL
      END AS return_purpose_of_visit,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!5219=5006!!" THEN @return_chief_complaint := 1 -- Asymptomatic
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!5219=1068!!" THEN @return_chief_complaint := 2 -- Symptomatic
        ELSE @return_chief_complaint := NULL
      END AS return_chief_complaint,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6613=1065!!" THEN @return_pain := 1 -- Yes
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6613=1066!!" THEN @return_pain := 2 -- No
        ELSE @return_pain := NULL
      END AS return_pain,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!7080=" THEN @return_pain_score := GetValues(obs, 7080)
        ELSE @return_pain_score := NULL
      END AS return_pain_score,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1070=" THEN @return_review_of_systems_heent := GetValues(obs, 1070)
        ELSE @return_review_of_systems_heent := NULL
      END AS return_review_of_systems_heent,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1069=" THEN @return_review_of_systems_general := GetValues(obs, 1069)
        ELSE @return_review_of_systems_general := NULL
      END AS return_review_of_systems_general,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1071=" THEN @return_review_of_systems_cardiopulmonary := GetValues(obs, 1071)
        ELSE @return_review_of_systems_cardiopulmonary := NULL
      END AS return_review_of_systems_cardiopulmonary,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1078=" THEN @return_review_of_systems_gastrointestinal := GetValues(obs, 1078)
        ELSE @return_review_of_systems_gastrointestinal := NULL
      END AS return_review_of_systems_gastrointestinal,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1080=" THEN @return_review_of_systems_genitourinary := GetValues(obs, 1080)
        ELSE @return_review_of_systems_genitourinary := NULL
      END AS return_review_of_systems_genitourinary,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1836=" THEN @return_last_menstrual_period_date := GetValues(obs, 1836)
        ELSE @return_last_menstrual_period_date := NULL
      END AS return_last_menstrual_period_date,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1081=" THEN @return_review_of_systems_musculoskeletal := GetValues(obs, 1081)
        ELSE @return_review_of_systems_musculoskeletal := NULL
      END AS return_review_of_systems_musculoskeletal,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6584=1115!!" THEN @return_ecog_index := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6584=6585!!" THEN @return_ecog_index := 2 -- Symptomatic but ambulatory
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6584=6586!!" THEN @return_ecog_index := 3 -- Debilitated, bedridden less than 80% of the day
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6584=6587!!" THEN @return_ecog_index := 4 -- Debilitated, greater than 50% of the day
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6584=6588!!" THEN @return_ecog_index := 5 -- Bedridden 100%
        ELSE @return_ecog_index := NULL
      END AS return_ecog_index,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1119=" THEN @return_general_exam_findings := GetValues(obs, 1119)
        ELSE @return_general_exam_findings := NULL
      END AS return_general_exam_findings,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1122=" THEN @return_heent_exam_findings := GetValues(obs, 1122)
        ELSE @return_heent_exam_findings := NULL
      END AS return_heent_exam_findings,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1123=" THEN @return_chest_exam_findings := GetValues(obs, 1123)
        ELSE @return_chest_exam_findings := NULL
      END AS return_chest_exam_findings,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6251=" THEN @return_breast_exam_findings := GetValues(obs, 6251)
        ELSE @return_breast_exam_findings := NULL
      END AS return_breast_exam_findings,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9696=" THEN @return_breast_finding_location := GetValues(obs, 9696)
        ELSE @return_breast_finding_location := NULL
      END AS return_breast_finding_location,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1239=" THEN @return_breast_finding_quadrant := GetValues(obs, 1239)
        ELSE @return_breast_finding_quadrant := NULL
      END AS return_breast_finding_quadrant,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1124=" THEN @return_heart_exam_findings := GetValues(obs, 1124)
        ELSE @return_heart_exam_findings := NULL
      END AS return_heart_exam_findings,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1125=" THEN @return_abdomen_exam_findings := GetValues(obs, 1125)
        ELSE @return_abdomen_exam_findings := NULL
      END AS return_abdomen_exam_findings,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1126=" THEN @return_urogenital_exam_findings := GetValues(obs, 1126)
        ELSE @return_urogenital_exam_findings := NULL
      END AS return_urogenital_exam_findings,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1127=" THEN @return_extremities_exam_findings := GetValues(obs, 1127)
        ELSE @return_extremities_exam_findings := NULL
      END AS return_extremities_exam_findings,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!8420=" THEN @return_testicular_exam_findings := GetValues(obs, 8420)
        ELSE @return_testicular_exam_findings := NULL
      END AS return_testicular_exam_findings,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1121=" THEN @return_nodal_survey_exam_findings := GetValues(obs, 1121)
        ELSE @return_nodal_survey_exam_findings := NULL
      END AS return_nodal_survey_exam_findings,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1128=" THEN @return_musculoskeletal_exam_findings := GetValues(obs, 1128)
        ELSE @return_musculoskeletal_exam_findings := NULL
      END AS return_musculoskeletal_exam_findings,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1129=" THEN @return_neurologic_exam_findings := GetValues(obs, 1129)
        ELSE @return_neurologic_exam_findings := NULL
      END AS return_neurologic_exam_findings,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!2018=" THEN @return_physical_examination_notes := GetValues(obs, 2018)
        ELSE @return_physical_examination_notes := NULL
      END AS return_physical_examination_notes,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6575=1065" THEN @return_patient_currently_on_chemo := 1 -- Yes
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6575=1107" THEN @return_patient_currently_on_chemo := 2 -- No
        ELSE @return_patient_currently_on_chemo := NULL
      END AS return_patient_currently_on_chemo,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!846=1115!!" THEN @return_ct_head := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!846=1116!!" THEN @return_ct_head := 2 -- Abnormal
        ELSE @return_ct_head := NULL
      END AS return_ct_head,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9839=1115!!" THEN @return_ct_neck := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9839=1116!!" THEN @return_ct_neck := 2 -- Abnormal
        ELSE @return_ct_neck := NULL
      END AS return_ct_neck,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9839=1115!!" THEN @return_ct_chest := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9839=1116!!" THEN @return_ct_chest := 2 -- Abnormal
        ELSE @return_ct_chest := NULL
      END AS return_ct_chest,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9840=1115!!" THEN @return_ct_spine := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9840=1116!!" THEN @return_ct_spine := 2 -- Abnormal
        ELSE @return_ct_spine := NULL
      END AS return_ct_spine,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!7114=1115!!" THEN @return_ct_abdomen := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!7114=1116!!" THEN @return_ct_abdomen := 2 -- Abnormal
        ELSE @return_ct_abdomen := NULL
      END AS return_ct_abdomen,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!7115=1115!!" THEN @return_ultrasound_renal := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!7115=1116!!" THEN @return_ultrasound_renal := 2 -- Abnormal
        ELSE @return_ultrasound_renal := NULL
      END AS return_ultrasound_renal,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!852=1115!!" THEN @return_ultrasound_hepatic := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!852=1116!!" THEN @return_ultrasound_hepatic := 2 -- Abnormal
        ELSE @return_ultrasound_hepatic := NULL
      END AS return_ultrasound_hepatic,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6221=1115!!" THEN @return_ultrasound_obstetric := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6221=1116!!" THEN @return_ultrasound_obstetric := 2 -- Abnormal
        ELSE @return_ultrasound_obstetric := NULL
      END AS return_ultrasound_obstetric,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!845=1115!!" THEN @return_ultrasound_abdominal := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!845=1116!!" THEN @return_ultrasound_abdominal := 2 -- Abnormal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!845=5103!!" THEN @return_ultrasound_abdominal := 3 -- Abdominal mass
        ELSE @return_ultrasound_abdominal := NULL
      END AS return_ultrasound_abdominal,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9596=1115!!" THEN @return_ultrasound_breast := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9596=1116!!" THEN @return_ultrasound_breast := 2 -- Abnormal
        ELSE @return_ultrasound_breast := NULL
      END AS return_ultrasound_breast,
      CASE
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!394=1115!!" THEN @return_xray_shoulder := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!394=1116!!" THEN @return_xray_shoulder := 2 -- Abnormal
        ELSE @return_xray_shoulder := NULL
      END AS return_xray_shoulder,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!392=1115!!" THEN @return_xray_pelvis := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!392=1116!!" THEN @return_xray_pelvis := 2 -- Abnormal
        ELSE @return_xray_pelvis := NULL
      END AS return_xray_pelvis,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!101=1115!!" THEN @return_xray_abdomen := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!101=1116!!" THEN @return_xray_abdomen := 2 -- Abnormal
        ELSE @return_xray_abdomen := NULL
      END AS return_xray_abdomen,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!386=1115!!" THEN @return_xray_skull := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!386=1116!!" THEN @return_xray_skull := 2 -- Abnormal
        ELSE @return_xray_skull := NULL
      END AS return_xray_skull,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!380=1115!!" THEN @return_xray_leg := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!380=1116!!" THEN @return_xray_leg := 2 -- Abnormal
        ELSE @return_xray_leg := NULL
      END AS return_xray_leg,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!382=1115!!" THEN @return_xray_hand := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!382=1116!!" THEN @return_xray_hand := 2 -- Abnormal
        ELSE @return_xray_hand := NULL
      END AS return_xray_hand,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!384=1115!!" THEN @return_xray_foot := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!384=1116!!" THEN @return_xray_foot := 2 -- Abnormal
        ELSE @return_xray_foot := NULL
      END AS return_xray_foot,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!12=1115!!" THEN @return_xray_chest := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!12=1116!!" THEN @return_xray_chest := 2 -- Abnormal
        ELSE @return_xray_chest := NULL
      END AS return_xray_chest,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!377=1115!!" THEN @return_xray_arm := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!377=1116!!" THEN @return_xray_arm := 2 -- Abnormal
        ELSE @return_xray_arm := NULL
      END AS return_xray_arm,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!390=1115!!" THEN @return_xray_spine := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!390=1116!!" THEN @return_xray_spine := 2 -- Abnormal
        ELSE @return_xray_spine := NULL
      END AS return_xray_spine,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1536=1115!!" THEN @return_echocardiography := 1 -- Normal 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1536=1116!!" THEN @return_echocardiography := 2 -- Abnormal 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1536=1538!!" THEN @return_echocardiography := 3 -- Dilated cardiomyopathy 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1536=1532!!" THEN @return_echocardiography := 4 -- Left ventricular hypertrophy 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1536=1533!!" THEN @return_echocardiography := 5 -- Right ventricular hypertrophy 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1536=5622!!" THEN @return_echocardiography := 6 -- Other (non-coded)
        ELSE @return_echocardiography := NULL
      END AS return_echocardiography,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9881=1115!!" THEN @return_mri_head := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9881=1116!!" THEN @return_mri_head := 2 -- Abnormal
        ELSE @return_mri_head := NULL
      END AS return_mri_head,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9882=1115!!" THEN @return_mri_neck := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9882=1116!!" THEN @return_mri_neck := 2 -- Abnormal 
        ELSE @return_mri_neck := NULL
      END AS return_mri_neck,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9951=1115!!" THEN @return_mri_arms := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9951=1116!!" THEN @return_mri_arms := 2 -- Abnormal 
        ELSE @return_mri_arms := NULL
      END AS return_mri_arms,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9883=1115!!" THEN @return_mri_chest := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9883=1116!!" THEN @return_mri_chest := 2 -- Abnormal 
        ELSE @return_mri_chest := NULL
      END AS return_mri_chest,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9885=1115!!" THEN @return_mri_spine := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9885=1116!!" THEN @return_mri_spine := 2 -- Abnormal 
        ELSE @return_mri_spine := NULL
      END AS return_mri_spine,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9884=1115!!" THEN @return_mri_abdomen := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9884=1116!!" THEN @return_mri_abdomen := 2 -- Abnormal 
        ELSE @return_mri_abdomen := NULL
      END AS return_mri_abdomen,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9952=1115!!" THEN @return_mri_pelvis := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9952=1116!!" THEN @return_mri_pelvis := 2 -- Abnormal 
        ELSE @return_mri_pelvis := NULL
      END AS return_mri_pelvis,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9953=1115!!" THEN @return_mri_legs := 1 -- Normal
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9953=1116!!" THEN @return_mri_legs := 2 -- Abnormal 
        ELSE @return_mri_legs := NULL
      END AS return_mri_legs,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!10077=" THEN @return_imaging_results_description := GetValues(obs, 10077)
        ELSE @return_imaging_results_description := NULL
      END AS return_imaging_results_description,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!679=" THEN @return_rbc_test_result := GetValues(obs, 679) 
        ELSE @return_rbc_test_result := NULL
      END AS return_rbc_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!21=" THEN @return_hgb_test_result := GetValues(obs, 21) 
        ELSE @return_hgb_test_result := NULL
      END AS return_hgb_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!851=" THEN @return_mcv_test_result := GetValues(obs, 851) 
        ELSE @return_mcv_test_result := NULL
      END AS return_mcv_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1018=" THEN @return_mch_test_result := GetValues(obs, 1018) 
        ELSE @return_mch_test_result := NULL
      END AS return_mch_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1017=" THEN @return_mchc_test_result := GetValues(obs, 1017) 
        ELSE @return_mchc_test_result := NULL
      END AS return_mchc_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1016=" THEN @return_rdw_test_result := GetValues(obs, 1016) 
        ELSE @return_rdw_test_result := NULL
      END AS return_rdw_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!729=" THEN @return_platelets_test_result := GetValues(obs, 729) 
        ELSE @return_platelets_test_result := NULL
      END AS return_platelets_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!678=" THEN @return_wbc_test_result := GetValues(obs, 678) 
        ELSE @return_wbc_test_result := NULL
      END AS return_wbc_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1313=" THEN @return_anc_test_result := GetValues(obs, 1313) 
        ELSE @return_anc_test_result := NULL
      END AS return_anc_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1015=" THEN @return_pcv_test_result := GetValues(obs, 1015) 
        ELSE @return_pcv_test_result := NULL
      END AS return_pcv_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6898=" THEN @return_urea_test_result := GetValues(obs, 6898) 
        ELSE @return_urea_test_result := NULL
      END AS return_urea_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!790=" THEN @return_creatinine_test_result := GetValues(obs, 790) 
        ELSE @return_creatinine_test_result := NULL
      END AS return_creatinine_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1132=" THEN @return_sodium_test_result := GetValues(obs, 1132) 
        ELSE @return_sodium_test_result := NULL
      END AS return_sodium_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1133=" THEN @return_potassium_test_result := GetValues(obs, 1133) 
        ELSE @return_potassium_test_result := NULL
      END AS return_potassium_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1134=" THEN @return_chloride_test_result := GetValues(obs, 1134) 
        ELSE @return_chloride_test_result := NULL
      END AS return_chloride_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!655=" THEN @return_serum_total_bilirubin_test_result := GetValues(obs, 655) 
        ELSE @return_serum_total_bilirubin_test_result := NULL
      END AS return_serum_total_bilirubin_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1297=" THEN @return_serum_direct_bilirubin_test_result := GetValues(obs, 1297) 
        ELSE @return_serum_direct_bilirubin_test_result := NULL
      END AS return_serum_direct_bilirubin_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6123=" THEN @return_gamma_glutamyl_transferase_test_result := GetValues(obs, 6123) 
        ELSE @return_gamma_glutamyl_transferase_test_result := NULL
      END AS return_gamma_glutamyl_transferase_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!653=" THEN @return_serum_glutamic_oxaloacetic_transaminase_test_result := GetValues(obs, 653) 
        ELSE @return_serum_glutamic_oxaloacetic_transaminase_test_result := NULL
      END AS return_serum_glutamic_oxaloacetic_transaminase_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!717=" THEN @return_serum_total_protein := GetValues(obs, 717) 
        ELSE @return_serum_total_protein := NULL
      END AS return_serum_total_protein,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!848=" THEN @return_serum_albumin := GetValues(obs, 848) 
        ELSE @return_serum_albumin := NULL
      END AS return_serum_albumin,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!785=" THEN @return_serum_alkaline_phosphate := GetValues(obs, 785) 
        ELSE @return_serum_alkaline_phosphate := NULL
      END AS return_serum_alkaline_phosphate,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1014=" THEN @return_serum_lactate_dehydrogenase := GetValues(obs, 1014) 
        ELSE @return_serum_lactate_dehydrogenase := NULL
      END AS return_serum_lactate_dehydrogenase,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9010=" THEN @return_hemoglobin_f_test_result := GetValues(obs, 9010) 
        ELSE @return_hemoglobin_f_test_result := NULL
      END AS return_hemoglobin_f_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9011=" THEN @return_hemoglobin_a_test_result := GetValues(obs, 9011) 
        ELSE @return_hemoglobin_a_test_result := NULL
      END AS return_hemoglobin_a_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9699=" THEN @return_hemoglobin_s_test_result := GetValues(obs, 9699) 
        ELSE @return_hemoglobin_s_test_result := NULL
      END AS return_hemoglobin_s_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9012=" THEN @return_hemoglobin_a2c_test_result := GetValues(obs, 9012) 
        ELSE @return_hemoglobin_a2c_test_result := NULL
      END AS return_hemoglobin_a2c_test_result,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1984=" THEN @return_urinalysis_pus_cells := GetValues(obs, 1984) 
        ELSE @return_urinalysis_pus_cells := NULL
      END AS return_urinalysis_pus_cells,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!2339=" THEN @return_urinalysis_protein := GetValues(obs, 2339) 
        ELSE @return_urinalysis_protein := NULL
      END AS return_urinalysis_protein,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6337=" THEN @return_urinalysis_leucocytes := GetValues(obs, 6337) 
        ELSE @return_urinalysis_leucocytes := NULL
      END AS return_urinalysis_leucocytes,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!7276=" THEN @return_urinalysis_ketones := GetValues(obs, 7276) 
        ELSE @return_urinalysis_ketones := NULL
      END AS return_urinalysis_ketones,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!2340=" THEN @return_urinalysis_glucose := GetValues(obs, 2340) 
        ELSE @return_urinalysis_glucose := NULL
      END AS return_urinalysis_glucose,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9307=" THEN @return_urinalysis_nitrites := GetValues(obs, 9307) 
        ELSE @return_urinalysis_nitrites := NULL
      END AS return_urinalysis_nitrites,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1327=" THEN @return_reticulocytes := GetValues(obs, 1327) 
        ELSE @return_reticulocytes := NULL
      END AS return_reticulocytes,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!10249=" THEN @return_total_psa := GetValues(obs, 10249) 
        ELSE @return_total_psa := NULL
      END AS return_total_psa,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!10250=" THEN @return_carcinoembryonic_antigen_test := GetValues(obs, 10250) 
        ELSE @return_carcinoembryonic_antigen_test := NULL
      END AS return_carcinoembryonic_antigen_test,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!10251=" THEN @return_carbohydrate_antigen_19_9 := GetValues(obs, 10251) 
        ELSE @return_carbohydrate_antigen_19_9 := NULL
      END AS return_carbohydrate_antigen_19_9,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9538=" THEN @return_lab_results_notes := GetValues(obs, 9538)
        ELSE @return_lab_results_notes := NULL
      END AS return_lab_results_notes,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!8190=" THEN @return_other_radiology_orders := GetValues(obs, 8190)
        ELSE @return_other_radiology_orders := NULL
      END AS return_other_radiology_orders,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6504=" THEN @return_diagnosis_established_using := GetValues(obs, 6504)
        ELSE @return_diagnosis_established_using := NULL
      END AS return_diagnosis_established_using,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6509=" THEN @return_diagnosis_based_on_biopsy_type := GetValues(obs, 6509)
        ELSE @return_diagnosis_based_on_biopsy_type := NULL
      END AS return_diagnosis_based_on_biopsy_type,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6605=" THEN @return_biopsy_concordant_with_clinical_suspicions := GetValues(obs, 6605)
        ELSE @return_biopsy_concordant_with_clinical_suspicions := NULL
      END AS return_biopsy_concordant_with_clinical_suspicions,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9841=6545!!" THEN @return_breast_cancer_type := 1 -- Inflammatory breast cancer
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9841=9842!!" THEN @return_breast_cancer_type := 2 -- Ductal cell carcinoma  
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9841=5622!!" THEN @return_breast_cancer_type := 3 -- Other (non-coded)
        ELSE @return_breast_cancer_type := NULL
      END AS return_breast_cancer_type,
      CASE
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9728=" THEN @return_diagnosis_date := GetValues(obs, 9728)
        ELSE @return_diagnosis_date := NULL
      END AS return_diagnosis_date,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6582=6566!!" THEN @return_cancer_stage := 1 -- Complete staging (TNM)
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6582=10206!!" THEN @return_cancer_stage := 2 -- Durie salmon system
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6582=1175!!" THEN @return_cancer_stage := 3 -- Not applicable
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6582=1067!!" THEN @return_cancer_stage := 4 -- Unknown
        ELSE @return_cancer_stage := NULL
      END AS return_cancer_stage,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9868=9851!!" THEN @return_overall_cancer_stage_group := 1 -- Stage 0
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9868=9852!!" THEN @return_overall_cancer_stage_group := 2 -- Stage I
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9868=9853!!" THEN @return_overall_cancer_stage_group := 3 -- Stage IA
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9868=9854!!" THEN @return_overall_cancer_stage_group := 4 -- Stage IB
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9868=9855!!" THEN @return_overall_cancer_stage_group := 5 -- Stage IC
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9868=9856!!" THEN @return_overall_cancer_stage_group := 6 -- Stage II
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9868=9857!!" THEN @return_overall_cancer_stage_group := 7 -- Stage IIA
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9868=9858!!" THEN @return_overall_cancer_stage_group := 8 -- Stage IIB
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9868=9859!!" THEN @return_overall_cancer_stage_group := 9-- Stage IIC
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9868=9860!!" THEN @return_overall_cancer_stage_group := 10 -- Stage III
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9868=9861!!" THEN @return_overall_cancer_stage_group := 11 -- Stage IIIA
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9868=9862!!" THEN @return_overall_cancer_stage_group := 12 -- Stage IIIB
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9868=9863!!" THEN @return_overall_cancer_stage_group := 13 -- Stage IIIC
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9868=9864!!" THEN @return_overall_cancer_stage_group := 14 -- Stage IV
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9868=9865!!" THEN @return_overall_cancer_stage_group := 15 -- Stage IVA
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9868=9867!!" THEN @return_overall_cancer_stage_group := 16 -- Stage IVC
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9868=9866!!" THEN @return_overall_cancer_stage_group := 17 -- Stage IVB
        ELSE @return_overall_cancer_stage_group := NULL
      END AS return_overall_cancer_stage_group,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!10441=" THEN @return_cancer_staging_date := GetValues(obs, 10441)
        ELSE @return_cancer_staging_date := NULL
      END AS return_cancer_staging_date,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!10445=" THEN @return_metastasis_region := GetValues(obs, 10445)
        ELSE @return_metastasis_region := NULL
      END AS return_metastasis_region,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!8723=10038!!" THEN @return_treatment_plan := 1 -- Biological therapy
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!8723=6575!!" THEN @return_treatment_plan := 2 -- Chemotherapy
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!8723=9626!!" THEN @return_treatment_plan := 3 -- Hormone replacement therapy
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!8723=8427!!" THEN @return_treatment_plan := 4 -- Radiation therapy
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!8723=7465!!" THEN @return_treatment_plan := 5 -- Surgery
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!8723=10232!!" THEN @return_treatment_plan := 6 -- Targeted therapy
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!8723=5622!!" THEN @return_treatment_plan := 7 -- Other (non-coded)
        ELSE @return_treatment_plan := NULL
      END AS return_treatment_plan,
      CASE
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!8725=10233!!" THEN @return_reason_for_surgery := 1 -- Breast reconstruction
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!8725=8428!!" THEN @return_reason_for_surgery := 2 -- Curative care
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!8725=8727!!" THEN @return_reason_for_surgery := 3 -- Diagnostic
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!8725=8724!!" THEN @return_reason_for_surgery := 4 -- Palliative care
      END AS return_reason_for_surgery,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!10230=" THEN @return_surgical_procedure_done := GetValues(obs, 10230)
        ELSE @return_surgical_procedure_done := NULL
      END AS return_surgical_procedure_done,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9916=1349!!" THEN @return_radiation_location := 1 -- Chest
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9916=8193!!" THEN @return_radiation_location := 2 -- Hands
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9916=9226!!" THEN @return_radiation_location := 3 -- Stomach
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9916=9227!!" THEN @return_radiation_location := 4 -- Spine
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9916=9228!!" THEN @return_radiation_location := 5 -- Bone
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9916=9229!!" THEN @return_radiation_location := 6 -- Oesophagus
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9916=9230!!" THEN @return_radiation_location := 7 -- Anus
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9916=9231!!" THEN @return_radiation_location := 8 -- Rectum
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9916=5622!!" THEN @return_radiation_location := 9 -- Other (non-coded)
        ELSE @return_radiation_location := NULL
      END AS return_radiation_location,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6419=1065!!" THEN @return_treatment_hospitalization_required := 1 -- Yes
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6419=1066!!" THEN @return_treatment_hospitalization_required := 2 -- No
        ELSE @return_treatment_hospitalization_required := NULL
      END AS return_treatment_hospitalization_required,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9869=1256!!" THEN @return_chemotherapy_plan := 1 -- Start drugs
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9869=1259!!" THEN @return_chemotherapy_plan := 2 -- Change regimen
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9869=1260!!" THEN @return_chemotherapy_plan := 3 -- Stop all medications
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9869=1257!!" THEN @return_chemotherapy_plan := 4 -- Continue regimen
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9869=6576!!" THEN @return_chemotherapy_plan := 5 -- Chemotherapy regimen modifications
        ELSE @return_chemotherapy_plan := NULL
      END AS return_chemotherapy_plan,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6644=" THEN @return_total_planned_chemotherapy_cycles := GetValues(obs, 6644)
        ELSE @return_total_planned_chemotherapy_cycles := NULL
      END AS return_total_planned_chemotherapy_cycles,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!6643=" THEN @return_current_chemotherapy_cycle := GetValues(obs, 6643)
        ELSE @return_current_chemotherapy_cycle := NULL
      END AS return_current_chemotherapy_cycle,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9927=1267!!" THEN @return_reasons_for_changing_modifying_or_stopping_chemo := 1 -- Completed
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9927=7391!!" THEN @return_reasons_for_changing_modifying_or_stopping_chemo := 2 -- No response
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9927=6629!!" THEN @return_reasons_for_changing_modifying_or_stopping_chemo := 3 -- Progressive disease
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9927=6627!!" THEN @return_reasons_for_changing_modifying_or_stopping_chemo := 4 -- Partial response
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9927=1879!!" THEN @return_reasons_for_changing_modifying_or_stopping_chemo := 5 -- Toxicity
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9927=5622!!" THEN @return_reasons_for_changing_modifying_or_stopping_chemo := 6 -- Other (non-coded)
        ELSE @return_reasons_for_changing_modifying_or_stopping_chemo := NULL
      END AS return_reasons_for_changing_modifying_or_stopping_chemo,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!2206=9218!!" THEN @return_chemotherapy_intent := 1 -- Adjuvant intent
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!2206=8428!!" THEN @return_chemotherapy_intent := 2 -- Curative intent
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!2206=9219!!" THEN @return_chemotherapy_intent := 3 -- Neo adjuvant intent
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!2206=8724!!" THEN @return_chemotherapy_intent := 4 -- Palliative care
        ELSE @return_chemotherapy_intent := NULL
      END AS return_chemotherapy_intent,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1190=" THEN @return_chemotherapy_start_date := GetValues(obs, 1190)
        ELSE @return_chemotherapy_start_date := NULL
      END AS return_chemotherapy_start_date,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9946=" THEN @return_chemotherapy_regimen := GetValues(obs, 9946)
        ELSE @return_chemotherapy_regimen := NULL
      END AS return_chemotherapy_regimen,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!9918=" THEN @return_chemotherapy_drug_started := GetValues(obs, 9918)
        ELSE @return_chemotherapy_drug_started := NULL
      END AS return_chemotherapy_drug_started,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1899=" THEN @return_chemotherapy_drug_dosage_in_mg := GetValues(obs, 1899)
        ELSE @return_chemotherapy_drug_dosage_in_mg := NULL
      END AS return_chemotherapy_drug_dosage_in_mg,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!7463=" THEN @return_chemotherapy_drug_route := GetValues(obs, 7463)
        ELSE @return_chemotherapy_drug_route := NULL
      END AS return_chemotherapy_drug_route,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1896=" THEN @return_chemotherapy_medication_frequency := GetValues(obs, 1896)
        ELSE @return_chemotherapy_medication_frequency := NULL
      END AS return_chemotherapy_medication_frequency,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1895=" THEN @return_non_chemotherapy_drug := GetValues(obs, 1895)
        ELSE @return_non_chemotherapy_drug := NULL
      END AS return_non_chemotherapy_drug,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1779=" THEN @return_other_non_coded_chemotherapy_drug := GetValues(obs, 1779)
        ELSE @return_other_non_coded_chemotherapy_drug := NULL
      END AS return_other_non_coded_chemotherapy_drug,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!2206=" THEN @return_non_chemotherapy_drug_purpose := GetValues(obs, 2206)
        ELSE @return_non_chemotherapy_drug_purpose := NULL
      END AS return_non_chemotherapy_drug_purpose,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!10247=1065!!" THEN @return_hospitalization_due_to_toxicity := 1 -- Yes
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!10247=1066!!" THEN @return_hospitalization_due_to_toxicity := 2 -- No
        ELSE @return_hospitalization_due_to_toxicity := NULL
      END AS return_hospitalization_due_to_toxicity,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!10243=1065!!" THEN @return_presence_of_neurotoxicity := 1 -- Yes
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!10243=1066!!" THEN @return_presence_of_neurotoxicity := 2 -- No
        ELSE @return_presence_of_neurotoxicity := NULL
      END AS return_presence_of_neurotoxicity,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!10244=9850!!" THEN @return_disease_status_at_end_of_treatment := 1 -- Cancer recurrence
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!10244=10240!!" THEN @return_disease_status_at_end_of_treatment := 2 -- No evidence of disease
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!10244=10245!!" THEN @return_disease_status_at_end_of_treatment := 3 -- Persistently elevated tumor marker
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!10244=10246!!" THEN @return_disease_status_at_end_of_treatment := 4 -- Recurrence based on imaging
        ELSE @return_disease_status_at_end_of_treatment := NULL
      END AS return_disease_status_at_end_of_treatment,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!7222=" THEN @return_assessment_notes := GetValues(obs, 7222)
        ELSE @return_assessment_notes := NULL
      END AS return_assessment_notes,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!1272=" THEN @return_general_plan_referrals := GetValues(obs, 1272)
        ELSE @return_general_plan_referrals := NULL
      END AS return_general_plan_referrals,
      CASE 
        WHEN t1.encounter_type = 143 AND obs REGEXP "!!5096=" THEN @return_rtc_date := GetValues(obs, 5096)
        ELSE @return_rtc_date := NULL
      END AS return_rtc_date

    FROM flat_breast_cancer_treatment_0 t1
      JOIN
        amrs.person p using (person_id)
      JOIN
        amrs.location `t2` using (location_id)
      ORDER BY person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
    );
    
    SET @prev_id := NULL;
    SET @cur_id := NULL;
    SET @prev_encounter_type = NULL;
    SET @cur_encounter_type = NULL;
    SET @prev_encounter_datetime = NULL;
    SET @cur_encounter_datetime = NULL;
    SET @prev_clinical_datetime = NULL;
    SET @cur_clinical_datetime = NULL;
    SET @prev_clinical_location_id = NULL;
    SET @cur_clinical_location_id = NULL;

    ALTER TABLE flat_breast_cancer_treatment_1 drop prev_id, drop cur_id;

    DROP TABLE IF EXISTS flat_breast_cancer_treatment_2;
    CREATE TEMPORARY TABLE flat_breast_cancer_treatment_2

    (SELECT 
      *,
      @prev_id := @cur_id as prev_id,
      @cur_id := person_id as cur_id,

      CASE
        WHEN @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
        ELSE @prev_encounter_datetime := null
      END AS next_encounter_datetime_breast_cancer_treatment,

      @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

      CASE
        WHEN @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
        ELSE @next_encounter_type := null
      END AS next_encounter_type_breast_cancer_treatment,

      @cur_encounter_type := encounter_type as cur_encounter_type,

      CASE
        WHEN @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
        ELSE @prev_clinical_datetime := null
      END AS next_clinical_datetime_breast_cancer_treatment,

      CASE
        WHEN @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
        ELSE @prev_clinical_location_id := null
      END AS next_clinical_location_id_breast_cancer_treatment,

      CASE
        WHEN is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
        WHEN @prev_id = @cur_id then @cur_clinical_datetime
        ELSE @cur_clinical_datetime := null
      END AS cur_clinic_datetime,

      CASE
        WHEN is_clinical_encounter then @cur_clinical_location_id := location_id
        WHEN @prev_id = @cur_id then @cur_clinical_location_id
        ELSE @cur_clinical_location_id := null
      END AS cur_clinic_location_id,

      CASE
        WHEN @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
        ELSE @prev_clinical_rtc_date := null
      END AS next_clinical_rtc_date_breast_cancer_treatment,

      CASE
        WHEN is_clinical_encounter then @cur_clinical_rtc_date := rtc_date
        WHEN @prev_id = @cur_id then @cur_clinical_rtc_date
        ELSE @cur_clinical_rtc_date:= null
      END AS cur_clinical_rtc_date

      FROM flat_breast_cancer_treatment_1
      ORDER BY person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
    );

	  ALTER TABLE flat_breast_cancer_treatment_2 DROP prev_id, DROP cur_id, DROP cur_encounter_type, DROP cur_encounter_datetime, DROP cur_clinical_rtc_date;

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

    DROP TEMPORARY TABLE IF EXISTS flat_breast_cancer_treatment_3;
    CREATE TEMPORARY TABLE flat_breast_cancer_treatment_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
    (SELECT
    *,
    @prev_id := @cur_id as prev_id,
    @cur_id := t1.person_id as cur_id,
    CASE
      WHEN @prev_id=@cur_id THEN @prev_encounter_type := @cur_encounter_type
      ELSE @prev_encounter_type:= NULL
    END AS prev_encounter_type_breast_cancer_treatment,	
    @cur_encounter_type := encounter_type as cur_encounter_type,
    CASE
      WHEN @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
      ELSE @prev_encounter_datetime := NULL
    END AS prev_encounter_datetime_breast_cancer_treatment,
    @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,
    CASE
      WHEN @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
      ELSE @prev_clinical_datetime := NULL
    END AS prev_clinical_datetime_breast_cancer_treatment,
    CASE
      WHEN @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
      ELSE @prev_clinical_location_id := NULL
    END AS prev_clinical_location_id_breast_cancer_treatment,
    CASE
      WHEN is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
      WHEN @prev_id = @cur_id then @cur_clinical_datetime
      ELSE @cur_clinical_datetime := NULL
    END AS cur_clinical_datetime,
    CASE
      WHEN is_clinical_encounter then @cur_clinical_location_id := location_id
      WHEN @prev_id = @cur_id then @cur_clinical_location_id
      ELSE @cur_clinical_location_id := NULL
    END AS cur_clinical_location_id,
    CASE
      WHEN @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
      ELSE @prev_clinical_rtc_date := NULL
    END AS prev_clinical_rtc_date_breast_cancer_treatment,
    CASE
      WHEN @prev_id = @cur_id then @cur_clinical_rtc_date
      ELSE @cur_clinical_rtc_date:= NULL
    END AS cur_clinic_rtc_date
    FROM flat_breast_cancer_treatment_2 t1
    ORDER BY person_id, date(encounter_datetime), encounter_type_sort_index
    );

    SELECT COUNT(*) INTO @new_encounter_rows FROM flat_breast_cancer_treatment_3;
            
    SELECT @new_encounter_rows;                    
    SET @total_rows_written := @total_rows_written + @new_encounter_rows;
    SELECT @total_rows_written;

    SET @dyn_sql=CONCAT('REPLACE INTO ', @write_table,											  
    '(SELECT
        NULL,
        person_id,
        encounter_id,
        encounter_type,
        encounter_datetime,
        visit_id,
        location_id,
        location_uuid,
        gender,
        age,
				death_date,
        initial_purpose_of_visit,
        initial_chief_complaint,
        initial_pain,
        initial_pain_score,
        initial_review_of_systems_heent,
        initial_review_of_systems_general,
        initial_review_of_systems_cardiopulmonary,
        initial_review_of_systems_gastrointestinal,
        initial_review_of_systems_genitourinary,
        initial_last_menstrual_period_date,
        initial_review_of_systems_musculoskeletal,
        initial_ecog_index,
        initial_general_exam_findings,
        initial_heent_exam_findings,
        initial_chest_exam_findings,
        initial_breast_exam_findings,
        initial_breast_finding_location,
        initial_breast_finding_quadrant,
        initial_heart_exam_findings,
        initial_abdomen_exam_findings,
        initial_urogenital_exam_findings,
        initial_extremities_exam_findings,
        initial_testicular_exam_findings,
        initial_nodal_survey_exam_findings,
        initial_musculoskeletal_exam_findings,
        initial_neurologic_exam_findings,
        initial_physical_examination_notes,
        initial_patient_currently_on_chemo,
        initial_ct_head,
        initial_ct_neck,
        initial_ct_chest,
        initial_ct_spine,
        initial_ct_abdomen,
        initial_ultrasound_renal,
        initial_ultrasound_hepatic,
        initial_ultrasound_obstetric,
        initial_ultrasound_abdominal,
        initial_ultrasound_breast,
        initial_xray_shoulder,
        initial_xray_pelvis,
        initial_xray_abdomen,
        initial_xray_skull,
        initial_xray_leg,
        initial_xray_hand,
        initial_xray_foot,
        initial_xray_chest,
        initial_xray_arm,
        initial_xray_spine,
        initial_echocardiography,
        initial_mri_head,
        initial_mri_neck,
        initial_mri_arms,
        initial_mri_chest,
        initial_mri_spine,
        initial_mri_abdomen,
        initial_mri_pelvis,
        initial_mri_legs,
        initial_imaging_results_description,
        initial_rbc_test_result,
        initial_hgb_test_result,
        initial_mcv_test_result,
        initial_mch_test_result,
        initial_mchc_test_result,
        initial_rdw_test_result,
        initial_platelets_test_result,
        initial_wbc_test_result,
        initial_anc_test_result,
        initial_pcv_test_result,
        initial_urea_test_result,
        initial_creatinine_test_result,
        initial_sodium_test_result,
        initial_potassium_test_result,
        initial_chloride_test_result,
        initial_serum_total_bilirubin_test_result,
        initial_serum_direct_bilirubin_test_result,
        initial_gamma_glutamyl_transferase_test_result,
        initial_serum_glutamic_oxaloacetic_transaminase_test_result,
        initial_serum_total_protein,
        initial_serum_albumin,
        initial_serum_alkaline_phosphate,
        initial_serum_lactate_dehydrogenase,
        initial_hemoglobin_f_test_result,
        initial_hemoglobin_a_test_result,
        initial_hemoglobin_s_test_result,
        initial_hemoglobin_a2c_test_result,
        initial_urinalysis_pus_cells,
        initial_urinalysis_protein,
        initial_urinalysis_leucocytes,
        initial_urinalysis_ketones,
        initial_urinalysis_glucose,
        initial_urinalysis_nitrites,
        initial_reticulocytes,
        initial_total_psa,
        initial_carcinoembryonic_antigen_test,
        initial_carbohydrate_antigen_19_9,
        initial_lab_results_notes,
        initial_other_radiology_orders,
        initial_diagnosis_established_using,
        initial_diagnosis_based_on_biopsy_type,
        initial_biopsy_concordant_with_clinical_suspicions,
        initial_breast_cancer_type,
        initial_diagnosis_date,
        initial_cancer_stage,
        initial_overall_cancer_stage_group,
        initial_cancer_staging_date,
        initial_metastasis_region,
        initial_treatment_plan,
        initial_reason_for_surgery,
        initial_surgical_procedure_done,
        initial_radiation_location,
        initial_treatment_hospitalization_required,
        initial_chemotherapy_plan,
        initial_total_planned_chemotherapy_cycles,
        initial_current_chemotherapy_cycle,
        initial_reasons_for_changing_modifying_or_stopping_chemo,
        initial_chemotherapy_intent,
        initial_chemotherapy_start_date,
        initial_chemotherapy_regimen,
        initial_chemotherapy_drug_started,
        initial_chemotherapy_drug_dosage_in_mg,
        initial_chemotherapy_drug_route,
        initial_chemotherapy_medication_frequency,
        initial_non_chemotherapy_drug,
        initial_other_non_coded_chemotherapy_drug,
        initial_non_chemotherapy_drug_purpose,
        initial_hospitalization_due_to_toxicity,
        initial_presence_of_neurotoxicity,
        initial_disease_status_at_end_of_treatment,
        initial_assessment_notes,
        initial_general_plan_referrals,
        rtc_date,
        return_purpose_of_visit,
        return_chief_complaint,
        return_pain,
        return_pain_score,
        return_review_of_systems_heent,
        return_review_of_systems_general,
        return_review_of_systems_cardiopulmonary,
        return_review_of_systems_gastrointestinal,
        return_review_of_systems_genitourinary,
        return_last_menstrual_period_date,
        return_review_of_systems_musculoskeletal,
        return_ecog_index,
        return_general_exam_findings,
        return_heent_exam_findings,
        return_chest_exam_findings,
        return_breast_exam_findings,
        return_breast_finding_location,
        return_breast_finding_quadrant,
        return_heart_exam_findings,
        return_abdomen_exam_findings,
        return_urogenital_exam_findings,
        return_extremities_exam_findings,
        return_testicular_exam_findings,
        return_nodal_survey_exam_findings,
        return_musculoskeletal_exam_findings,
        return_neurologic_exam_findings,
        return_physical_examination_notes,
        return_patient_currently_on_chemo,
        return_ct_head,
        return_ct_neck,
        return_ct_chest,
        return_ct_spine,
        return_ct_abdomen,
        return_ultrasound_renal,
        return_ultrasound_hepatic,
        return_ultrasound_obstetric,
        return_ultrasound_abdominal,
        return_ultrasound_breast,
        return_xray_shoulder,
        return_xray_pelvis,
        return_xray_abdomen,
        return_xray_skull,
        return_xray_leg,
        return_xray_hand,
        return_xray_foot,
        return_xray_chest,
        return_xray_arm,
        return_xray_spine,
        return_echocardiography,
        return_mri_head,
        return_mri_neck,
        return_mri_arms,
        return_mri_chest,
        return_mri_spine,
        return_mri_abdomen,
        return_mri_pelvis,
        return_mri_legs,
        return_imaging_results_description,
        return_rbc_test_result,
        return_hgb_test_result,
        return_mcv_test_result,
        return_mch_test_result,
        return_mchc_test_result,
        return_rdw_test_result,
        return_platelets_test_result,
        return_wbc_test_result,
        return_anc_test_result,
        return_pcv_test_result,
        return_urea_test_result,
        return_creatinine_test_result,
        return_sodium_test_result,
        return_potassium_test_result,
        return_chloride_test_result,
        return_serum_total_bilirubin_test_result,
        return_serum_direct_bilirubin_test_result,
        return_gamma_glutamyl_transferase_test_result,
        return_serum_glutamic_oxaloacetic_transaminase_test_result,
        return_serum_total_protein,
        return_serum_albumin,
        return_serum_alkaline_phosphate,
        return_serum_lactate_dehydrogenase,
        return_hemoglobin_f_test_result,
        return_hemoglobin_a_test_result,
        return_hemoglobin_s_test_result,
        return_hemoglobin_a2c_test_result,
        return_urinalysis_pus_cells,
        return_urinalysis_protein,
        return_urinalysis_leucocytes,
        return_urinalysis_ketones,
        return_urinalysis_glucose,
        return_urinalysis_nitrites,
        return_reticulocytes,
        return_total_psa,
        return_carcinoembryonic_antigen_test,
        return_carbohydrate_antigen_19_9,
        return_lab_results_notes,
        return_other_radiology_orders,
        return_diagnosis_established_using,
        return_diagnosis_based_on_biopsy_type,
        return_biopsy_concordant_with_clinical_suspicions,
        return_breast_cancer_type,
        return_diagnosis_date,
        return_cancer_stage,
        return_overall_cancer_stage_group,
        return_cancer_staging_date,
        return_metastasis_region,
        return_treatment_plan,
        return_reason_for_surgery,
        return_surgical_procedure_done,
        return_radiation_location,
        return_treatment_hospitalization_required,
        return_chemotherapy_plan,
        return_total_planned_chemotherapy_cycles,
        return_current_chemotherapy_cycle,
        return_reasons_for_changing_modifying_or_stopping_chemo,
        return_chemotherapy_intent,
        return_chemotherapy_start_date,
        return_chemotherapy_regimen,
        return_chemotherapy_drug_started,
        return_chemotherapy_drug_dosage_in_mg,
        return_chemotherapy_drug_route,
        return_chemotherapy_medication_frequency,
        return_non_chemotherapy_drug,
        return_other_non_coded_chemotherapy_drug,
        return_non_chemotherapy_drug_purpose,
        return_hospitalization_due_to_toxicity,
        return_presence_of_neurotoxicity,
        return_disease_status_at_end_of_treatment,
        return_assessment_notes,
        return_general_plan_referrals,
        return_rtc_date,
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
    FROM
        flat_breast_cancer_treatment_3)');

    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;


    SET @dyn_sql := CONCAT('DELETE t1 FROM ',@queue_table,' t1 JOIN flat_breast_cancer_treatment_build_queue__0 t2 using (person_id);'); 
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
          
  IF (@query_type = "build") THEN
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