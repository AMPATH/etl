CREATE PROCEDURE `generate_flat_breast_cancer_screening_v1_2`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
  SET @primary_table := "flat_breast_cancer_screening";
  SET @query_type := query_type;
  SET @total_rows_written := 0;

  SET @encounter_types := "(86,145,146,160)";
  SET @clinical_encounter_types := "(86,145,146,160)";
  SET @non_clinical_encounter_types := "(-1)";
  SET @other_encounter_types := "(-1)";
                  
  SET @start := now();
  SET @table_version := "flat_breast_cancer_screening_v1.2";

  SET session sort_buffer_size := 512000000;

  SET @sep := " ## ";
  SET @boundary := "!!";
  SET @last_date_created := (SELECT max(max_date_created) FROM etl.flat_obs);

  CREATE TABLE IF NOT EXISTS flat_breast_cancer_screening (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    person_id INT,
    encounter_id INT,
    encounter_type INT,
    encounter_datetime DATETIME,
    visit_id INT,
    location_id INT,
    gender CHAR(4),
    age INT,
    death_date DATETIME,
    -- BREAST CANCER SCREENING FORM (encounter type 86)
    current_visit_type TINYINT,
    complaints_presenting_with VARCHAR(100),
    had_menses_before_age_twelve TINYINT,
    menses_permanently_stopped TINYINT,
    age_when_menses_stopped TINYINT,
    has_used_hrt TINYINT,
    age_at_first_hrt_use TINYINT,
    age_at_last_hrt_use TINYINT,
    years_of_hrt_use TINYINT,
    type_of_hrt_used TINYINT,
    has_ever_given_birth TINYINT,
    age_at_first_birth TINYINT,
    gravida TINYINT,
    parity TINYINT,
    smokes_cigarettes TINYINT,
    sticks_smoked_per_day TINYINT,
    uses_tobacco TINYINT,
    years_of_tobacco_use TINYINT,
    times_tobacco_used_per_day TINYINT,
    uses_alcohol TINYINT,
    type_of_alcohol_used VARCHAR(50),
    years_of_alcohol_use TINYINT,
    hiv_status TINYINT,
    has_ever_done_a_clinical_breast_exam TINYINT,
    past_clinical_breast_exam_results VARCHAR(50),
    -- date_of_past_clinical_breast_exam DATETIME,
    has_ever_done_a_mammogram TINYINT,
    past_mammogram_results TINYINT,
    has_ever_done_a_breast_ultrasound TINYINT,
    -- date_of_past_breast_ultrasound DATETIME,
    has_ever_done_a_breast_biopsy TINYINT,
    number_of_breast_biopsies_done TINYINT,
    type_of_breast_biopsy_done_in_the_past VARCHAR(100),
    -- date_of_past_breast_biopsy DATETIME,
    ever_had_chest_radiation_treatment TINYINT,
    has_ever_had_breast_surgery_not_for_breast_cancer TINYINT,
    type_of_breast_surgery_done VARCHAR(50),
    personal_history_of_breast_cancer TINYINT,
    age_at_breast_cancer_diagnosis TINYINT,
    family_history_of_breast_cancer TINYINT,
    relatives_with_breast_ca_diagnosis_at_age_fifty_or_younger VARCHAR(100),
    relatives_with_breast_ca_diagnosis_after_age_fifty VARCHAR(100),
    male_relatives_diagnosed_with_breast_cancer VARCHAR(100),
    family_members_diagnosed_with_ovarian_cancer VARCHAR(100),
    relatives_with_history_of_breast_cancer VARCHAR(100),
    breast_exam_findings_this_visit VARCHAR(200),
    breast_findings_location VARCHAR(100),
    breast_findings_quadrant VARCHAR(200),
    breast_mass_shape VARCHAR(50),
    breast_mass_margins VARCHAR(50),
    breast_mass_size VARCHAR(50),
    breast_mass_texture VARCHAR(50),
    breast_mass_mobility VARCHAR(50),
    -- other_breast_findings_non_coded VARCHAR(100),
    breast_symmetry TINYINT,
    lymph_node_exam_findings TINYINT,
    axillary_lymph_nodes_location TINYINT,
    supra_clavicular_location TINYINT,
    infra_clavicular_location TINYINT,
    screening_patient_education VARCHAR(100),
    -- other_non_coded_screening_patient_education VARCHAR(100),
    histological_investigations_done VARCHAR(100),
    radiological_investigations_done VARCHAR(50),
    screening_referral TINYINT,
    -- facility_referred_to_for_management INT,
    screening_rtc_date DATETIME,
    -- 
    -- BREAST INVESTIGATIONS FINDINGS FORM (encounter type 145)
    bif_mammogram_results TINYINT,
    bif_mammogram_tumor_size VARCHAR(100),
    bif_mammogram_workup_date DATETIME,
    bif_mammogram_results_description VARCHAR(200),
    bif_breast_ultrasound_results TINYINT,
    bif_ultrasound_tumor_size VARCHAR(100),
    bif_ultrasound_workup_date DATETIME,
    bif_ultrasound_results_description VARCHAR(100),
    bif_concordant_cbe_and_imaging_results TINYINT,
    bif_fna_breast_results TINYINT,
    bif_fna_workup_date DATETIME,
    bif_fna_degree_of_malignancy TINYINT,
    bif_fna_results_description VARCHAR(200),
    bif_fna_diagnosis_date DATETIME,
    bif_breast_biopsy_results TINYINT,  
    bif_breast_biopsy_malignancy_type TINYINT,
    bif_tumor_grade_or_differentiation TINYINT,
    bif_breast_biopsy_sample_collection_date DATETIME,
    bif_breast_biopsy_results_description VARCHAR(100),
    bif_histology_reporting_date DATETIME,
    bif_ihc_er TINYINT,
    bif_ihc_pr TINYINT,
    bif_ihc_her2 TINYINT,
    bif_ihc_ki67 INT,
    bif_mode_of_breast_screening VARCHAR(100),
    bif_date_patient_informed_and_referred_for_treatment DATETIME,
    bif_referrals_ordered VARCHAR(100),
    bif_assessment_notes VARCHAR(150),
    bif_rtc_date DATETIME,
    -- 
    -- CLINICAL BREAST EXAM (encounter type 146)
    cbe_purpose_of_visit TINYINT,
    cbe_breast_exam_findings VARCHAR(100),
    cbe_breast_findings_location VARCHAR(100),
    cbe_breast_findings_quadrant VARCHAR(200),
    cbe_breast_symmetry TINYINT,
    cbe_breast_mass_shape TINYINT,
    cbe_breast_mass_margins TINYINT,
    cbe_breast_mass_size TINYINT,
    cbe_breast_mass_texture TINYINT,
    cbe_breast_mass_mobility TINYINT,
    cbe_lymph_nodes_findings TINYINT,
    cbe_assessment_notes VARCHAR(150),
    cbe_referrals_ordered VARCHAR(100),
    cbe_referral_date DATETIME,
    cbe_other_referrals_ordered_non_coded VARCHAR(100),
    cbe_patient_education VARCHAR(100),
    cbe_rtc_date DATETIME,
    -- 
    -- BREAST CANCER HISTORY & RISK ASSESSMENT FORM (encounter type 160)
    ra_purpose_of_visit TINYINT,
    ra_had_menses_before_age_twelve TINYINT,
    ra_menses_permanently_stopped TINYINT,
    ra_age_when_menses_stopped TINYINT,
    ra_has_used_hrt TINYINT,
    ra_age_at_first_hrt_use TINYINT,
    ra_age_at_last_hrt_use TINYINT,
    ra_years_of_hrt_use TINYINT,
    ra_type_of_hrt_used TINYINT,
    ra_has_ever_given_birth TINYINT,
    ra_age_at_first_birth TINYINT,
    ra_gravida TINYINT,
    ra_parity TINYINT,
    ra_smokes_cigarettes TINYINT,
    ra_sticks_smoked_per_day TINYINT,
    ra_uses_tobacco TINYINT,
    ra_years_of_tobacco_use TINYINT,
    ra_times_tobacco_used_per_day TINYINT,
    ra_uses_alcohol TINYINT,
    ra_type_of_alcohol_used VARCHAR(50),
    ra_years_of_alcohol_use TINYINT,
    ra_presence_of_chief_complaint TINYINT,
    ra_breast_lump_location VARCHAR(50),
    ra_nipple_discharge_location VARCHAR(50),
    ra_nipple_or_skin_retraction_location VARCHAR(50),
    ra_breast_swelling_location VARCHAR(50),
    ra_breast_rash_location VARCHAR(50),
    ra_breast_pain_location VARCHAR(50),
    ra_other_changes_location VARCHAR(50),
    ra_has_ever_done_a_clinical_breast_exam TINYINT,
    ra_past_clinical_breast_exam_results VARCHAR(50),
    ra_has_ever_done_a_mammogram TINYINT,
    ra_past_mammogram_results TINYINT,
    ra_has_ever_done_a_breast_ultrasound TINYINT,
    ra_has_ever_done_a_breast_biopsy TINYINT,
    ra_number_of_breast_biopsies_done TINYINT,
    ra_type_of_breast_biopsy_done_in_the_past VARCHAR(100),
    ra_ever_had_chest_radiation_treatment TINYINT,
    ra_has_ever_had_breast_surgery_not_for_breast_cancer TINYINT,
    ra_type_of_breast_surgery_done VARCHAR(50),
    ra_any_family_member_diagnosed_with_breast_cancer TINYINT,
    ra_relatives_with_breast_ca_diagnosis_at_age_fifty_or_younger VARCHAR(100),
    ra_relatives_with_breast_ca_diagnosis_after_age_fifty VARCHAR(100),
    ra_male_relatives_diagnosed_with_breast_cancer VARCHAR(100),
    ra_family_members_diagnosed_with_ovarian_cancer VARCHAR(100),
    ra_relatives_with_history_of_breast_cancer VARCHAR(100),
    prev_encounter_datetime_breast_cancer_screening datetime,
    next_encounter_datetime_breast_cancer_screening datetime,
    prev_encounter_type_breast_cancer_screening mediumint,
    next_encounter_type_breast_cancer_screening mediumint,
    prev_clinical_datetime_breast_cancer_screening datetime,
    next_clinical_datetime_breast_cancer_screening datetime,
    prev_clinical_location_id_breast_cancer_screening mediumint,
    next_clinical_location_id_breast_cancer_screening mediumint,
    prev_clinical_rtc_date_breast_cancer_screening datetime,
    next_clinical_rtc_date_breast_cancer_screening datetime,
    PRIMARY KEY encounter_id (encounter_id),
    INDEX person_date (person_id, encounter_datetime),
    INDEX location_enc_date (location_id,encounter_datetime),
    INDEX enc_date_location (encounter_datetime, location_id),
    INDEX location_id_rtc_date (location_id,screening_rtc_date),
    INDEX loc_id_enc_date_next_clinical (location_id, encounter_datetime, next_clinical_datetime_breast_cancer_screening),
    INDEX encounter_type (encounter_type),
    INDEX date_created (date_created)
  );

  IF (@query_type = "build") THEN
  	SELECT 'BUILDING..........................................';               												
    SET @write_table = concat("flat_breast_cancer_screening_temp_",queue_number);
    SET @queue_table = concat("flat_breast_cancer_screening_build_queue_",queue_number);                    												
          
    SET @dyn_sql := CONCAT('create table if not exists ', @write_table,' like ', @primary_table);
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  

    SET @dyn_sql := CONCAT('Create table if not exists ', @queue_table, ' (SELECT * FROM flat_breast_cancer_screening_build_queue limit ', queue_size, ');'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
          
    SET @dyn_sql := CONCAT('DELETE t1 FROM flat_breast_cancer_screening_build_queue t1 JOIN ',@queue_table, ' t2 USING (person_id);'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
  END IF;
	
  IF (@query_type = "sync") THEN
    SELECT 'SYNCING..........................................';
    SET @write_table = "flat_breast_cancer_screening";
    SET @queue_table = "flat_breast_cancer_screening_sync_queue";

    CREATE TABLE IF NOT EXISTS flat_breast_cancer_screening_sync_queue (
        person_id INT PRIMARY KEY
    );                            
                            
    SET @last_update := null;

    SELECT 
      MAX(date_updated)
    INTO @last_update FROM
      etl.flat_log
    WHERE
      table_name = @table_version;										

    REPLACE INTO flat_breast_cancer_screening_sync_queue
    (SELECT DISTINCT patient_id
      FROM amrs.encounter
      WHERE date_changed > @last_update
    );

    REPLACE INTO flat_breast_cancer_screening_sync_queue
    (SELECT DISTINCT person_id
      FROM etl.flat_obs
      WHERE max_date_created > @last_update
    );

    REPLACE INTO flat_breast_cancer_screening_sync_queue
    (SELECT DISTINCT person_id
      FROM etl.flat_lab_obs
      WHERE max_date_created > @last_update
    );

    REPLACE INTO flat_breast_cancer_screening_sync_queue
    (SELECT DISTINCT person_id
      FROM etl.flat_orders
      WHERE max_date_created > @last_update
    );
                  
    REPLACE INTO flat_breast_cancer_screening_sync_queue
    (SELECT person_id FROM 
      amrs.person 
      WHERE date_voided > @last_update);


    REPLACE INTO flat_breast_cancer_screening_sync_queue
    (SELECT person_id FROM 
      amrs.person 
      WHERE date_changed > @last_update);
  END IF;
                      
    -- Remove test patients
  SET @dyn_sql := CONCAT('DELETE t1 FROM ', @queue_table,' t1
      JOIN amrs.person_attribute t2 USING (person_id)
      WHERE t2.person_attribute_type_id=28 and value="true" and voided=0');
  PREPARE s1 FROM @dyn_sql; 
  EXECUTE s1; 
  DEALLOCATE PREPARE s1;  

  SET @person_ids_count := 0;
  SET @dyn_sql := CONCAT('SELECT count(*) into @person_ids_count FROM ', @queue_table); 
  PREPARE s1 FROM @dyn_sql; 
  EXECUTE s1; 
  DEALLOCATE PREPARE s1;

  SELECT @person_ids_count AS 'num patients to update';

  SET @dyn_sql := CONCAT('DELETE t1 FROM ',@primary_table, ' t1 JOIN ',@queue_table,' t2 USING (person_id);'); 
  PREPARE s1 FROM @dyn_sql; 
  EXECUTE s1; 
  DEALLOCATE PREPARE s1;  
                                  
  SET @total_time := 0;
  SET @cycle_number := 0;
                    
  WHILE @person_ids_count > 0 DO
    SET @loop_start_time := now();
    DROP TEMPORARY TABLE IF EXISTS flat_breast_cancer_screening_build_queue__0;
	
    SET @dyn_sql := CONCAT('CREATE TEMPORARY TABLE flat_breast_cancer_screening_build_queue__0 (person_id int primary key) (SELECT * FROM ',@queue_table,' limit ',cycle_size,');'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
                    
    DROP TEMPORARY TABLE IF EXISTS flat_breast_cancer_screening_0a;
    SET @dyn_sql = CONCAT(
      'CREATE TEMPORARY TABLE flat_breast_cancer_screening_0a
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
          WHEN t1.encounter_type in ',@clinical_encounter_types,' then 1
          ELSE null
        END AS is_clinical_encounter,
        CASE
          WHEN t1.encounter_type in ',@non_clinical_encounter_types,' then 20
          WHEN t1.encounter_type in ',@clinical_encounter_types,' then 10
          WHEN t1.encounter_type in', @other_encounter_types, ' then 5
          ELSE 1
        END AS encounter_type_sort_index,
        t2.orders
      FROM etl.flat_obs t1
        JOIN flat_breast_cancer_screening_build_queue__0 t0 USING (person_id)
        left JOIN etl.flat_orders t2 USING(encounter_id)
      WHERE t1.encounter_type in ',@encounter_types,');'
    );
                            
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;   
					
		INSERT INTO flat_breast_cancer_screening_0a
    (SELECT
      t1.person_id,
      null,
      t1.encounter_id,
      t1.test_datetime,
      t1.encounter_type,
      null, 
      -- t1.location_id,
      t1.obs,
      null,
      -- obs_datetimes
      -- in any visit, there many be multiple encounters. for this dataset, we want to include only clinical encounters (e.g. not lab or triage visit)
      0 as is_clinical_encounter,
      1 as encounter_type_sort_index,
      null
    FROM etl.flat_lab_obs t1
    JOIN flat_breast_cancer_screening_build_queue__0 t0 USING (person_id));

    DROP TEMPORARY TABLE IF EXISTS flat_breast_cancer_screening_0;
    CREATE TEMPORARY TABLE flat_breast_cancer_screening_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
    (SELECT * FROM flat_breast_cancer_screening_0a ORDER BY person_id, date(encounter_datetime), encounter_type_sort_index);

    -- BREAST CANCER SCREENING FORM (enc type 86)
    SET @current_visit_type := null;
    SET @complaints_presenting_with := null;
    SET @had_menses_before_age_twelve := null;
    SET @menses_permanently_stopped := null;
    SET @age_when_menses_stopped := null;
    SET @has_used_hrt := null;
    SET @age_at_first_hrt_use := null;
    SET @age_at_last_hrt_use := null;
    SET @years_of_hrt_use := null;
    SET @type_of_hrt_used := null;
    SET @has_ever_given_birth := null;
    SET @age_at_first_birth := null;
    SET @gravida := null;
    SET @parity := null;
    SET @smokes_cigarettes := null;
    SET @sticks_smoked_per_day := null;
    SET @uses_tobacco := null;
    SET @years_of_tobacco_use := null;
    SET @times_tobacco_used_per_day := null;
    SET @uses_alcohol := null;
    SET @type_of_alcohol_used := null;
    SET @years_of_alcohol_use := null;
    SET @hiv_status := null;
    SET @has_ever_done_a_clinical_breast_exam := null;
    SET @past_clinical_breast_exam_results := null;
    -- SET @date_of_past_clinical_breast_exam := null;
    SET @has_ever_done_a_mammogram := null;
    SET @past_mammogram_results := null;
    SET @has_ever_done_a_breast_ultrasound := null;
    -- SET @date_of_past_breast_ultrasound := null;
    SET @has_ever_done_a_breast_biopsy := null;
    SET @number_of_breast_biopsies_done := null;
    SET @type_of_breast_biopsy_done_in_the_past := null;
    -- SET @date_of_past_breast_biopsy := null;
    SET @ever_had_chest_radiation_treatment := null;
    SET @has_ever_had_breast_surgery_not_for_breast_cancer := null;
    SET @type_of_breast_surgery_done := null;
    SET @personal_history_of_breast_cancer := null;
    SET @age_at_breast_cancer_diagnosis := null;
    SET @family_history_of_breast_cancer := null;
    SET @relatives_with_breast_ca_diagnosis_at_age_fifty_or_younger := null;
    SET @relatives_with_breast_ca_diagnosis_after_age_fifty := null;
    SET @male_relatives_diagnosed_with_breast_cancer := null;
    SET @family_members_diagnosed_with_ovarian_cancer := null;
    SET @relatives_with_history_of_breast_cancer := null;
    SET @breast_exam_findings_this_visit := null;
    SET @breast_findings_location := null;
    SET @breast_findings_quadrant := null;
    SET @breast_mass_shape := null;
    SET @breast_mass_margins := null;
    SET @breast_mass_size := null;
    SET @breast_mass_texture := null;
    SET @breast_mass_mobility := null;
    -- SET @other_breast_findings_non_coded := null;
    SET @breast_symmetry := null;
    SET @lymph_node_exam_findings := null;
    SET @axillary_lymph_nodes_location := null;
    SET @supra_clavicular_location := null;
    SET @infra_clavicular_incation := null;
    SET @screening_patient_education := null;
    -- SET @other_non_coded_screening_patient_education := null;
    SET @histological_investigations_done := null;
    SET @radiological_investigations_done := null;
    SET @screening_referral := null;
    -- SET @facility_referred_to_for_management := null;
    SET @screening_rtc_date := null;
    --
    -- BREAST INVESTIGATIONS FINDINGS FORM (encounter type 145)
    SET @bif_mammogram_results := null;
    SET @bif_mammogram_tumor_size := null;
    SET @bif_mammogram_workup_date := null;
    SET @bif_mammogram_results_description := null;
    SET @bif_breast_ultrasound_results := null;
    SET @bif_ultrasound_tumor_size := null;
    SET @bif_ultrasound_workup_date := null;
    SET @bif_ultrasound_results_description := null;
    SET @bif_concordant_cbe_and_imaging_results := null;
    SET @bif_fna_breast_results := null;
    SET @bif_fna_workup_date := null;
    SET @bif_fna_degree_of_malignancy := null;
    SET @bif_fna_results_description := null;
    SET @bif_fna_diagnosis_date := null;
    SET @bif_breast_biopsy_results := null;  
    SET @bif_breast_biopsy_malignancy_type := null;
    SET @bif_tumor_grade_or_differentiation := null;
    SET @bif_breast_biopsy_sample_collection_date := null;
    SET @bif_breast_biopsy_results_description := null;
    SET @bif_histology_reporting_date := null;
    SET @bif_ihc_er := null;
    SET @bif_ihc_pr := null;
    SET @bif_ihc_her2 := null;
    SET @bif_ihc_ki67 := null;
    SET @bif_mode_of_breast_screening := null;
    SET @bif_date_patient_informed_and_referred_for_treatment := null;
    SET @bif_referrals_ordered := null;
    SET @bif_assessment_notes := null;
    SET @bif_rtc_date := null;
    -- 
    -- CLINICAL BREAST EXAM (encounter type 146)
    SET @cbe_purpose_of_visit := null;
    SET @cbe_breast_exam_findings := null;
    SET @cbe_breast_findings_location := null;
    SET @cbe_breast_findings_quadrant := null;
    SET @cbe_breast_mass_shape := null;
    SET @cbe_breast_symmetry := null;
    SET @cbe_breast_mass_shape := null;
    SET @cbe_breast_mass_margins := null;
    SET @cbe_breast_mass_size := null;
    SET @cbe_breast_mass_texture := null;
    SET @cbe_breast_mass_mobility := null;
    SET @cbe_lymph_nodes_findings := null;
    SET @cbe_assessment_notes := null;
    SET @cbe_referrals_ordered := null;
    SET @cbe_referral_date := null;
    SET @cbe_other_referrals_ordered_non_coded := null;
    SET @cbe_patient_education := null;
    SET @cbe_rtc_date := null;
    -- 
    -- BREAST CANCER HISTORY & RISK ASSESSMENT FORM (encounter type 160)
    SET @ra_purpose_of_visit := null;
    SET @ra_had_menses_before_age_twelve := null;
    SET @ra_menses_permanently_stopped := null;
    SET @ra_age_when_menses_stopped := null;
    SET @ra_has_used_hrt := null;
    SET @ra_age_at_first_hrt_use := null;
    SET @ra_age_at_last_hrt_use := null;
    SET @ra_years_of_hrt_use := null;
    SET @ra_type_of_hrt_used := null;
    SET @ra_has_ever_given_birth := null;
    SET @ra_age_at_first_birth := null;
    SET @ra_gravida := null;
    SET @ra_parity := null;
    SET @ra_smokes_cigarettes := null;
    SET @ra_sticks_smoked_per_day := null;
    SET @ra_uses_tobacco := null;
    SET @ra_years_of_tobacco_use := null;
    SET @ra_times_tobacco_used_per_day := null;
    SET @ra_uses_alcohol := null;
    SET @ra_type_of_alcohol_used := null;
    SET @ra_years_of_alcohol_use := null;
    SET @ra_presence_of_chief_complaint := null;
    SET @ra_breast_lump_location := null;
    SET @ra_nipple_discharge_location := null;
    SET @ra_nipple_or_skin_retraction_location := null;
    SET @ra_breast_swelling_location := null;
    SET @ra_breast_rash_location := null;
    SET @ra_breast_pain_location := null;
    SET @ra_other_changes_location := null;
    SET @ra_has_ever_done_a_clinical_breast_exam := null;
    SET @ra_past_clinical_breast_exam_results := null;
    SET @ra_has_ever_done_a_mammogram := null;
    SET @ra_past_mammogram_results := null;
    SET @ra_has_ever_done_a_breast_ultrasound := null;
    SET @ra_has_ever_done_a_breast_biopsy := null;
    SET @ra_number_of_breast_biopsies_done := null;
    SET @ra_type_of_breast_biopsy_done_in_the_past := null;
    SET @ra_ever_had_chest_radiation_treatment := null;
    SET @ra_has_ever_had_breast_surgery_not_for_breast_cancer := null;
    SET @ra_type_of_breast_surgery_done := null;
    SET @ra_any_family_member_diagnosed_with_breast_cancer := null;
    SET @ra_relatives_with_breast_ca_diagnosis_at_age_fifty_or_younger := null;
    SET @ra_relatives_with_breast_ca_diagnosis_after_age_fifty := null;
    SET @ra_male_relatives_diagnosed_with_breast_cancer := null;
    SET @ra_family_members_diagnosed_with_ovarian_cancer := null;
    SET @ra_relatives_with_history_of_breast_cancer := null;
    
    DROP TEMPORARY TABLE IF EXISTS flat_breast_cancer_screening_1;
    CREATE TEMPORARY TABLE flat_breast_cancer_screening_1
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
      t1.location_id,
      t1.is_clinical_encounter,
      p.gender,
      CASE
        WHEN TIMESTAMPDIFF(year,birthdate,curdate()) > 0 then ROUND(TIMESTAMPDIFF(year,birthdate,curdate()),0)
        ELSE ROUND(TIMESTAMPDIFF(month,birthdate,curdate())/12, 2)
      END AS age,
      p.death_date,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!1834=9651!!" THEN @current_visit_type := 1 -- Annual screening
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!1834=1154!!" THEN @current_visit_type := 2 -- New complaints
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!1834=1246!!" THEN @current_visit_type := 3 -- Recall/scheduled visit
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!1834=2345!!" THEN @current_visit_type := 4 -- Follow-up
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!1834=5622!!" THEN @current_visit_type := 5 -- Other (non-coded)
        ELSE @current_visit_type := null
      END AS current_visit_type,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!1834=1154!!" AND obs REGEXP "!!5219=" THEN @complaints_presenting_with := GetValues(obs, 5219)
        ELSE @complaints_presenting_with := null
      END AS complaints_presenting_with,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9560=1065!!" THEN @had_menses_before_age_twelve := 1 -- Yes
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9560=1066!!" THEN @had_menses_before_age_twelve := 2 -- No
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9560=9568!!" THEN @had_menses_before_age_twelve := 3 -- Not sure
        ELSE @had_menses_before_age_twelve := null
      END AS had_menses_before_age_twelve,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9561=1065!!" THEN @menses_permanently_stopped := 1 -- Yes
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9561=1066!!" THEN @menses_permanently_stopped := 2 -- No
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9561=9568!!" THEN @menses_permanently_stopped := 3 -- Not sure
        ELSE @menses_permanently_stopped := null
      END AS menses_permanently_stopped,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9562=" THEN @age_when_menses_stopped := GetValues(obs, 9562) 
        ELSE @age_when_menses_stopped := null
      END AS age_when_menses_stopped,
            CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9626=1065!!" THEN @has_used_hrt := 1 -- Yes
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9626=1066!!" THEN @has_used_hrt := 2 -- No
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9626=9568!!" THEN @has_used_hrt := 3 -- Not sure
        ELSE @has_used_hrt := null
      END AS has_used_hrt,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9627=" THEN @age_at_first_hrt_use := GetValues(obs, 9627) 
        ELSE @age_at_first_hrt_use := null
      END AS age_at_first_hrt_use,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9628=" THEN @age_at_last_hrt_use := GetValues(obs, 9628) 
        ELSE @age_at_last_hrt_use := null
      END AS age_at_last_hrt_use,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9629=" THEN @years_of_hrt_use := GetValues(obs, 9629) 
        ELSE @years_of_hrt_use := null
      END AS years_of_hrt_use,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9630=9573!!" THEN @type_of_hrt_used := 1 -- Estrogen
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9630=6217!!" THEN @type_of_hrt_used := 2 -- Progesterone
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9630=6218!!" THEN @type_of_hrt_used := 3 -- Combined oral contraceptive pill
        -- WHEN t1.encounter_type = 86 AND obs REGEXP "!!9630=" THEN @type_of_hrt_used := 4 -- Levonorgestrel needs to be linked to set
        ELSE @type_of_hrt_used := null
      END AS type_of_hrt_used, 
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9563=1065!!" THEN @has_ever_given_birth := 1 -- Yes
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9563=1066!!" THEN @has_ever_given_birth := 2 -- No
        ELSE @has_ever_given_birth := null
      END AS has_ever_given_birth,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9564=" THEN @age_at_first_birth := GetValues(obs, 9564) 
        ELSE @age_at_first_birth := null
      END AS age_at_first_birth,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!5624=" THEN @gravida := GetValues(obs, 5624) 
        ELSE @gravida := null
      END AS gravida,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!1053=" THEN @parity := GetValues(obs, 1053) 
        ELSE @parity := null
      END AS parity,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!2065=1065!!" THEN @smokes_cigarettes := 1 -- Yes
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!2065=1066!!" THEN @smokes_cigarettes := 2 -- No
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!2065=1679!!" THEN @smokes_cigarettes := 3 -- Stopped
        ELSE @smokes_cigarettes := null
      END AS smokes_cigarettes,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!2069=" THEN @sticks_smoked_per_day := GetValues(obs, 2069) 
        ELSE @sticks_smoked_per_day := null
      END AS sticks_smoked_per_day,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!7973=1065!!" THEN @uses_tobacco := 1 -- Yes 
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!7973=1066!!" THEN @uses_tobacco := 2 -- No
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!7973=1679!!" THEN @uses_tobacco := 3 -- Not sure
        ELSE @uses_tobacco := null
      END AS uses_tobacco,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!8144=" THEN @years_of_tobacco_use := GetValues(obs, 8144) 
        ELSE @years_of_tobacco_use := null
      END AS years_of_tobacco_use,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!8143=" THEN @times_tobacco_used_per_day := GetValues(obs, 8143)
        ELSE @times_tobacco_used_per_day := null
      END AS times_tobacco_used_per_day,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!1684=1065!!" THEN @uses_alcohol := 1 -- Yes 
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!1684=1066!!" THEN @uses_alcohol := 2 -- No
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!1684=1679!!" THEN @uses_alcohol := 3 -- Stopped
        ELSE @uses_alcohol := null
      END AS uses_alcohol,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!1685=" THEN @type_of_alcohol_used := GetValues(obs, 1685) 
        ELSE @type_of_alcohol_used := null
      END AS type_of_alcohol_used,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!8170=" THEN @years_of_alcohol_use := GetValues(obs, 8170) 
        ELSE @years_of_alcohol_use := null
      END AS years_of_alcohol_use,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!6709=664!!" THEN @hiv_status := 1 -- Negative
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!6709=703!!" THEN @hiv_status := 2 -- Positive
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!6709=1067!!" THEN @hiv_status := 3 -- Unknown
        ELSE @hiv_status := null
      END AS hiv_status,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9559=1065!!" THEN @has_ever_done_a_clinical_breast_exam := 1 -- Yes
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9559=1066!!" THEN @has_ever_done_a_clinical_breast_exam := 2 -- No
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9559=9568!!" THEN @has_ever_done_a_clinical_breast_exam := 3 -- Not sure
        ELSE @has_ever_done_a_clinical_breast_exam := null
      END AS has_ever_done_a_clinical_breast_exam,
      CASE
        -- workaround necessary because this question and `breast_findings_this_visit` incorrectly share the same concept
        WHEN t1.encounter_type = 86 AND obs regexp "!!9559=1065!!" AND obs REGEXP "!!6251=" THEN @past_clinical_breast_exam_results := SUBSTRING(GetValues(obs, 6251), 1, 4)
        ELSE @past_clinical_breast_exam_results := null
      END AS past_clinical_breast_exam_results,
      -- CASE
      --   WHEN t1.encounter_type = 86 AND obs REGEXP "!!6251=" THEN @date_of_past_clinical_breast_exam := GetValues(obs_datetimes, 6251) 
      --   ELSE @date_of_past_clinical_breast_exam := null
      -- END AS date_of_past_clinical_breast_exam,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9594=1065!!" THEN @has_ever_done_a_mammogram := 1 -- Yes
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9594=1066!!" THEN @has_ever_done_a_mammogram := 2 -- No
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9594=9568!!" THEN @has_ever_done_a_mammogram := 3 -- Not sure
        ELSE @has_ever_done_a_mammogram := null
      END AS has_ever_done_a_mammogram,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9595=1115!!" THEN @past_mammogram_results := 1 -- Normal
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9595=1116!!" THEN @past_mammogram_results := 2 -- Abnormal
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9595=1067!!" THEN @past_mammogram_results := 3 -- Unknown
        ELSE @past_mammogram_results := null
      END AS past_mammogram_results,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9597=1065!!" THEN @has_ever_done_a_breast_ultrasound := 1 -- Yes
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9597=1066!!" THEN @has_ever_done_a_breast_ultrasound := 2 -- No
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9597=9568!!" THEN @has_ever_done_a_breast_ultrasound := 3 -- Not sure
        ELSE @has_ever_done_a_breast_ultrasound := null
      END AS has_ever_done_a_breast_ultrasound,
      -- CASE
      --   WHEN t1.encounter_type = 86 AND obs REGEXP "!!=" THEN @date_of_past_breast_ultrasound := GetValues(obs, ) 
      --   ELSE @date_of_past_breast_ultrasound := null
      -- END AS date_of_past_breast_ultrasound,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9598=1065!!" THEN @has_ever_done_a_breast_biopsy := 1 -- Yes
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9598=1066!!" THEN @has_ever_done_a_breast_biopsy := 2 -- No
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9598=9568!!" THEN @has_ever_done_a_breast_biopsy := 3 -- Not sure
        ELSE @has_ever_done_a_breast_biopsy := null
      END AS has_ever_done_a_breast_biopsy,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9599=" THEN @number_of_breast_biopsies_done := GetValues(obs, 9599) 
        ELSE @number_of_breast_biopsies_done := null
      END AS number_of_breast_biopsies_done,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!6509=" THEN @type_of_breast_biopsy_done_in_the_past := GetValues(obs, 6509) 
        ELSE @type_of_breast_biopsy_done_in_the_past := null
      END AS type_of_breast_biopsy_done_in_the_past,
      -- CASE
      --   WHEN t1.encounter_type = 86 AND obs REGEXP "!!=!!" THEN @date_of_past_breast_biopsy := GetValues(obs, ) 
      --   ELSE @date_of_past_breast_biopsy := null
      -- END AS date_of_past_breast_biopsy,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9566=1065!!" THEN @ever_had_chest_radiation_treatment := 1 -- Yes
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9566=1066!!" THEN @ever_had_chest_radiation_treatment := 2 -- No
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9566=1679!!" THEN @ever_had_chest_radiation_treatment := 3 -- Not sure
        ELSE @ever_had_chest_radiation_treatment := null
      END AS ever_had_chest_radiation_treatment,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9644=1065!!" THEN @has_ever_had_breast_surgery_not_for_breast_cancer := 1 -- Yes
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9644=1066!!" THEN @has_ever_had_breast_surgery_not_for_breast_cancer := 2 -- No
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9644=1679!!" THEN @has_ever_had_breast_surgery_not_for_breast_cancer := 3 -- Not sure
        ELSE @has_ever_had_breast_surgery_not_for_breast_cancer := null
      END AS has_ever_had_breast_surgery_not_for_breast_cancer,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9566=1065!!" AND obs REGEXP "!!9649=9648!!" THEN @type_of_breast_surgery_done := 1 -- Breast implants
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9566=1065!!" AND obs REGEXP "!!9649=9647!!" THEN @type_of_breast_surgery_done := 2 -- Breast reduction surgery
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9566=1065!!" AND obs REGEXP "!!9649=5622!!" THEN @type_of_breast_surgery_done := 3 -- Other (non-coded)
        ELSE @type_of_breast_surgery_done := null
      END AS type_of_breast_surgery_done,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!11741=1065!!" THEN @personal_history_of_breast_cancer := 1 -- Yes 
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!11741=1066!!" THEN @personal_history_of_breast_cancer := 2 -- No
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!11741=1067!!" THEN @personal_history_of_breast_cancer := 3 -- Unknown
        ELSE @personal_history_of_breast_cancer := null
      END AS personal_history_of_breast_cancer,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9652=" THEN @age_at_breast_cancer_diagnosis := GetValues(obs, 9652) 
        ELSE @age_at_breast_cancer_diagnosis := null
      END AS age_at_breast_cancer_diagnosis,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!10172=1065!!" THEN @family_history_of_breast_cancer := 1 -- Yes
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!10172=1066!!" THEN @family_history_of_breast_cancer := 2 -- No
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!10172=1067!!" THEN @family_history_of_breast_cancer := 3 -- Unknown
        ELSE @family_history_of_breast_cancer := null
      END AS family_history_of_breast_cancer,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9631=" THEN @relatives_with_breast_ca_diagnosis_at_age_fifty_or_younger := GetValues(obs, 9631) 
        ELSE @relatives_with_breast_ca_diagnosis_at_age_fifty_or_younger := null
      END AS relatives_with_breast_ca_diagnosis_at_age_fifty_or_younger,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9632=" THEN @relatives_with_breast_ca_diagnosis_after_age_fifty := GetValues(obs, 9632) 
        ELSE @relatives_with_breast_ca_diagnosis_after_age_fifty := null
      END AS relatives_with_breast_ca_diagnosis_after_age_fifty,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9633=" THEN @male_relatives_diagnosed_with_breast_cancer := GetValues(obs, 9633) 
        ELSE @male_relatives_diagnosed_with_breast_cancer := null
      END AS male_relatives_diagnosed_with_breast_cancer,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9634=" THEN @family_members_diagnosed_with_ovarian_cancer := GetValues(obs, 9634) 
        ELSE @family_members_diagnosed_with_ovarian_cancer := null
      END AS family_members_diagnosed_with_ovarian_cancer,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9635=" THEN @relatives_with_history_of_breast_cancer := GetValues(obs, 9635) 
        ELSE @relatives_with_history_of_breast_cancer := null
      END AS relatives_with_history_of_breast_cancer,
      CASE
        -- workaround necessary because this question and `past_clinical_breast_exam_results` incorrectly share the same concept
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!6251=" THEN @breast_exam_findings_this_visit := SUBSTRING(GetValues(obs, 6251), -4)
        ELSE @breast_exam_findings_this_visit := null
      END AS breast_exam_findings_this_visit,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9696=" THEN @breast_findings_location := GetValues(obs, 9696) 
        ELSE @breast_findings_location := null
      END AS breast_findings_location,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!8268=" THEN @breast_findings_quadrant := GetValues(obs, 8268) 
        ELSE @breast_findings_quadrant := null
      END AS breast_findings_quadrant,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9657=" THEN @breast_mass_shape := GetValues(obs, 9657) 
        ELSE @breast_mass_shape := null
      END AS breast_mass_shape,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9663=" THEN @breast_mass_margins := GetValues(obs, 9663) 
        ELSE @breast_mass_margins := null
      END AS breast_mass_margins,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9669=" THEN @breast_mass_size := GetValues(obs, 9669) 
        ELSE @breast_mass_size := null
      END AS breast_mass_size,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9674=" THEN @breast_mass_texture := GetValues(obs, 9647) 
        ELSE @breast_mass_texture := null
      END AS breast_mass_texture,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9677=" THEN @breast_mass_mobility := GetValues(obs, 9677) 
        ELSE @breast_mass_mobility := null
      END AS breast_mass_mobility,
      -- CASE
      --   WHEN t1.encounter_type = 86 AND obs REGEXP "!!=!!" THEN @other_breast_findings_non_coded := GetValues(obs, ) 
      --   ELSE @other_breast_findings_non_coded := null
      -- END AS other_breast_findings_non_coded,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9681=1065!!" THEN @breast_symmetry := 1 -- Yes
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9681=1066!!" THEN @breast_symmetry := 2 -- No
        ELSE @breast_symmetry := null
      END AS breast_symmetry,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!1121=1115!!" THEN @lymph_node_exam_findings := 1 -- Normal
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!1121=161!!" THEN @lymph_node_exam_findings := 2 -- Lymphadenopathy
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!1121=9675!!" THEN @lymph_node_exam_findings := 3 -- Fixed
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!1121=9676!!" THEN @lymph_node_exam_findings := 4 -- Mobile
        ELSE @lymph_node_exam_findings := null
      END AS lymph_node_exam_findings,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9678=5141!!" THEN @axillary_lymph_nodes_location := 1 -- Right 
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9678=5139!!" THEN @axillary_lymph_nodes_location := 2 -- Left
        ELSE @axillary_lymph_nodes_location := null
      END AS axillary_lymph_nodes_location,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9679=5141!!" THEN @supra_clavicular_location := 1 -- Right
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9679=5139!!" THEN @supra_clavicular_location := 2 -- Left
        ELSE @supra_clavicular_location := null
      END AS supra_clavicular_location,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9680=5141!!" THEN @infra_clavicular_location := 1 -- Right 
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!9680=5139!!" THEN @infra_clavicular_location := 2 -- Left
        ELSE @infra_clavicular_location := null
      END AS infra_clavicular_location,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!6327=9651!!" THEN @screening_patient_education := 1 -- Importance of annual screening
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!6327=2345!!" THEN @screening_patient_education := 2 -- Referral follow-up
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!6327=9692!!" THEN @screening_patient_education := 3 -- Breast self-exam
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!6327=5622!!" THEN @screening_patient_education := 4 -- Other (non-coded))
        ELSE @screening_patient_education := null
      END AS screening_patient_education,
      -- CASE
        -- WHEN t1.encounter_type = 86 AND obs REGEXP "!!=!!" THEN @other_non_coded_screening_patient_education := GetValues(obs, ) 
        -- ELSE @other_non_coded_screening_patient_education := null
      -- END AS other_non_coded_screening_patient_education,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!7479=6510!!" THEN @histological_investigations_done := 1 -- Core needle biopsy
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!7479=6511!!" THEN @histological_investigations_done := 2 -- Excisional biopsy
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!7479=9997!!" THEN @histological_investigations_done := 3 -- Incisional biopsy
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!7479=7190!!" THEN @histological_investigations_done := 4 -- Cytology
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!7479=1107!!" THEN @histological_investigations_done := 5 -- None
        ELSE @histological_investigations_done := null
      END AS histological_investigations_done,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!11742=9595!!" THEN @radiological_investigations_done := 1 -- Mammogram
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!11742=9596!!" THEN @radiological_investigations_done := 2 -- Breast ultrasound
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!11742=1107!!" THEN @radiological_investigations_done := 3 -- None
        ELSE @radiological_investigations_done := null
      END AS radiological_investigations_done,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!1272=1107!!" THEN @screening_referral := 1 -- None 
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!1272=1496!!" THEN @screening_referral := 2 -- Referred to clinician 
        ELSE @screening_referral := null
      END AS screening_referral,
      -- CASE
      --   WHEN t1.encounter_type = 86 AND obs REGEXP "!!=!!" THEN @ := GetValues(obs, ) 
      --   ELSE @facility_referred_to_for_management := null
      -- END AS facility_referred_to_for_management,
      CASE
        WHEN t1.encounter_type = 86 AND obs REGEXP "!!5096=" THEN @screening_rtc_date := GetValues(obs, 5096) 
        ELSE @screening_rtc_date := null
      END AS screening_rtc_date,
      -- 
      -- BREAST INVESTIGATIONS FINDINGS FORM (encounter type 145)
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!9595=1118!!" THEN @bif_mammogram_results := 1 -- not done
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!9595=1115!!" THEN @bif_mammogram_results := 2 -- normal
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!9595=1116!!" THEN @bif_mammogram_results := 3 -- abnormal
        ELSE @bif_mammogram_results := null
      END AS bif_mammogram_results,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10053=" THEN @bif_mammogram_tumor_size := GetValues(obs, 10053)
        ELSE @bif_mammogram_tumor_size := null
      END AS bif_mammogram_tumor_size,
      CASE 
        -- workaround necessary because this question and `bif_fna_diagnosis_date` incorrectly share the same concept
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!9708=" THEN @bif_mammogram_workup_date := SUBSTRING(GetValues(obs, 9708), 1, 19)
        ELSE @bif_mammogram_workup_date := null
      END AS bif_mammogram_workup_date,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10045=" THEN @bif_mammogram_results_description := GetValues(obs, 10045)
        ELSE @bif_mammogram_results_description := null
      END AS bif_mammogram_results_description,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!9596=1118!!" THEN @bif_breast_ultrasound_results := 1 -- not done
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!9596=1115!!" THEN @bif_breast_ultrasound_results := 2 -- normal
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!9596=1116!!" THEN @bif_breast_ultrasound_results := 3 -- abnormal
        ELSE @bif_breast_ultrasound_results := null
      END AS bif_breast_ultrasound_results,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10053=" THEN @bif_ultrasound_tumor_size := GetValues(obs, 10053)
        ELSE @bif_ultrasound_tumor_size := null
      END AS bif_ultrasound_tumor_size,
      CASE 
        -- workaround necessary because this question and `bif_fna_diagnosis_date` incorrectly share the same concept
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!9708=" THEN @bif_ultrasound_workup_date := SUBSTRING(GetValues(obs, 9708), -19)
        ELSE @bif_ultrasound_workup_date := null
      END AS bif_ultrasound_workup_date,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10046=" THEN @bif_ultrasound_results_description := GetValues(obs, 10046)
        ELSE @bif_ultrasound_results_description := null
      END AS bif_ultrasound_results_description,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!9702=1175!!" THEN @bif_concordant_cbe_and_imaging_results := 1 -- not applicable
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!9702=9703!!" THEN @bif_concordant_cbe_and_imaging_results := 2 -- concordant
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!9702=9704!!" THEN @bif_concordant_cbe_and_imaging_results := 3 -- discordant
        ELSE @bif_concordant_cbe_and_imaging_results := null
      END AS bif_concordant_cbe_and_imaging_results,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10051=1118!!" THEN @bif_fna_breast_results := 1 -- not done
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10051=9691!!" THEN @bif_fna_breast_results := 2 -- benign
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10051=10052!!" THEN @bif_fna_breast_results := 3 -- malignant
        ELSE @bif_fna_breast_results := null
      END AS bif_fna_breast_results,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10057=" THEN @bif_fna_workup_date := GetValues(obs, 10057)
        ELSE @bif_fna_workup_date := null
      END AS bif_fna_workup_date,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10054=10055!!" THEN @bif_fna_degree_of_malignancy := 1 -- invasive cancer
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10054=10056!!" THEN @bif_fna_degree_of_malignancy := 2 -- in situ cancer
        ELSE @bif_fna_degree_of_malignancy := null
      END AS bif_fna_degree_of_malignancy,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10058=" THEN @bif_fna_results_description := GetValues(obs, 10058)
        ELSE @bif_fna_results_description := null
      END AS bif_fna_results_description,
      CASE 
        -- workaround necessary because this question and `bif_histology_reporting_date` incorrectly share the same concept
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!9728=" THEN @bif_fna_diagnosis_date := SUBSTRING(GetValues(obs, 9728), 1, 19)
        ELSE @bif_fna_diagnosis_date := null
      END AS bif_fna_diagnosis_date,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!8184=1118!!" THEN @bif_breast_biopsy_results := 1 -- not done
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!8184=9691!!" THEN @bif_breast_biopsy_results := 2 -- benign
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!8184=10052!!" THEN @bif_breast_biopsy_results := 3 -- malignant
        ELSE @bif_breast_biopsy_results := null
      END AS bif_breast_biopsy_results,  
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10543=9842!!" THEN @bif_breast_biopsy_malignancy_type := 1 -- ductal carcinoma in situ
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10543=6545!!" THEN @bif_breast_biopsy_malignancy_type := 2 -- inflammatory breast cancer
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10543=10563!!" THEN @bif_breast_biopsy_malignancy_type := 3 -- invasive ductal carcinoma (ductal breast cancer)
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10543=10562!!" THEN @bif_breast_biopsy_malignancy_type := 4 -- lobular carcinoma in situ
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10543=11743!!" THEN @bif_breast_biopsy_malignancy_type := 5 -- mammary paget disease
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10543=11744!!" THEN @bif_breast_biopsy_malignancy_type := 6 -- medullary carcinoma
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10543=11745!!" THEN @bif_breast_biopsy_malignancy_type := 7 -- metaplastic carcinoma
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10543=11746!!" THEN @bif_breast_biopsy_malignancy_type := 8 -- mucinous (colloid) carcinoma
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10543=11747!!" THEN @bif_breast_biopsy_malignancy_type := 9 -- papillary carcinoma
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10543=10565!!" THEN @bif_breast_biopsy_malignancy_type := 10 -- phyllodes tumour
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10543=11748!!" THEN @bif_breast_biopsy_malignancy_type := 11 -- tubular carcinoma
        ELSE @bif_breast_biopsy_malignancy_type := null
      END AS bif_breast_biopsy_malignancy_type,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!11749=11750!!" THEN @bif_tumor_grade_or_differentiation := 1 -- grade 1 (well differentiated)
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!11749=11751!!" THEN @bif_tumor_grade_or_differentiation := 2 -- grade 2 (moderately differentiated)
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!11749=11752!!" THEN @bif_tumor_grade_or_differentiation := 3 -- grade 3 (poorly differentiated)
        ELSE @bif_tumor_grade_or_differentiation := null
      END AS bif_tumor_grade_or_differentiation,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10060=" THEN @bif_breast_biopsy_sample_collection_date := GetValues(obs, 10060)
        ELSE @bif_breast_biopsy_sample_collection_date := null
      END AS bif_breast_biopsy_sample_collection_date,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!7400=" THEN @bif_breast_biopsy_results_description := GetValues(obs, 7400)
        ELSE @bif_breast_biopsy_results_description := null
      END AS bif_breast_biopsy_results_description,
      CASE 
        -- workaround necessary because this question and `bif_fna_diagnosis_date` incorrectly share the same concept
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!9728=" THEN @bif_histology_reporting_date := SUBSTRING(GetValues(obs, 6251), -19)
        ELSE @bif_histology_reporting_date := null
      END AS bif_histology_reporting_date,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10571=703!!" THEN @bif_ihc_er := 1 -- positive
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10571=664!!" THEN @bif_ihc_er := 2 -- negative
        ELSE @bif_ihc_er := null
      END AS bif_ihc_er,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10572=703!!" THEN @bif_ihc_pr := 1 -- positive
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10572=664!!" THEN @bif_ihc_pr := 2 -- negative
        ELSE @bif_ihc_pr := null
      END AS bif_ihc_pr,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10573=703!!" THEN @bif_ihc_her2 := 1 -- positive
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10573=664!!" THEN @bif_ihc_her2 := 2 -- negative
        ELSE @bif_ihc_her2 := null
      END AS bif_ihc_her2,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!11753=" THEN @bif_ihc_ki67 := GetValues(obs, 11753)
        ELSE @bif_ihc_ki67 := null
      END AS bif_ihc_ki67,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10068=10067!!" THEN @bif_mode_of_breast_screening := 1 -- deteected by CBE
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10068=9595!!" THEN @bif_mode_of_breast_screening := 2 -- detected by mammography
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!10068=9596!!" THEN @bif_mode_of_breast_screening := 3 -- detected by ultrasound
        ELSE @bif_mode_of_breast_screening := null
      END AS bif_mode_of_breast_screening,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!9706=" THEN @bif_date_patient_informed_and_referred_for_treatment := GetValues(obs, 9706)
        ELSE @bif_date_patient_informed_and_referred_for_treatment := null
      END AS bif_date_patient_informed_and_referred_for_treatment,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!1272=" THEN @bif_referrals_ordered := GetValues(obs, 1272)
        ELSE @bif_referrals_ordered := null
      END AS bif_referrals_ordered,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!7222=" THEN @bif_assessment_notes := GetValues(obs, 7222)
        ELSE @bif_assessment_notes := null
      END AS bif_assessment_notes,
      CASE 
        WHEN t1.encounter_type = 145 AND obs REGEXP "!!5096=" THEN @bif_rtc_date := GetValues(obs, 5096)
        ELSE @bif_rtc_date := null
      END AS bif_rtc_date,
      -- 
      -- CLINICAL BREAST EXAM (encounter type 146)
      CASE 
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!1834=9651!!" THEN @cbe_purpose_of_visit := 1 -- annual screening
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!1834=1154!!" THEN @cbe_purpose_of_visit := 2 -- new problem
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!1834=1246!!" THEN @cbe_purpose_of_visit := 3 -- recall/scheduled visit
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!1834=2345!!" THEN @cbe_purpose_of_visit := 4 -- follow up
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!1834=5622!!" THEN @cbe_purpose_of_visit := 5 -- other (non-coded)
        ELSE @cbe_purpose_of_visit := null
      END AS cbe_purpose_of_visit,
      CASE 
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!6251=" THEN @cbe_breast_exam_findings := GetValues(obs, 6251)
        ELSE @cbe_breast_exam_findings := null
      END AS cbe_breast_exam_findings,
      CASE 
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9696=" THEN @cbe_breast_findings_location := GetValues(obs, 9696)
        ELSE @cbe_breast_findings_location := null
      END AS cbe_breast_findings_location,
      CASE 
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!8268=" THEN @cbe_breast_findings_quadrant := GetValues(obs, 8268)
        ELSE @cbe_breast_findings_quadrant := null
      END AS cbe_breast_findings_quadrant,
      CASE 
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9681=1065!!" THEN @cbe_breast_symmetry := 1 -- Yes
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9681=1066!!" THEN @cbe_breast_symmetry := 2 -- No
        ELSE @cbe_breast_symmetry := null
      END AS cbe_breast_symmetry,
      CASE 
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9657=9658!!" THEN @cbe_breast_mass_shape := 1 -- round 
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9657=9659!!" THEN @cbe_breast_mass_shape := 2 -- oval
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9657=9660!!" THEN @cbe_breast_mass_shape := 3 -- irregular
        ELSE @cbe_breast_mass_shape := null
      END AS cbe_breast_mass_shape,
      CASE 
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9663=9661!!" THEN @cbe_breast_mass_margins := 1 -- well-defined
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9663=9662!!" THEN @cbe_breast_mass_margins := 2 -- ill-defined
        ELSE @cbe_breast_mass_margins := null
      END AS cbe_breast_mass_margins,
      CASE 
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9669=9664!!" THEN @cbe_breast_mass_size := 1 -- < 5mm
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9669=9665!!" THEN @cbe_breast_mass_size := 2 -- 5-9mm
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9669=9666!!" THEN @cbe_breast_mass_size := 3 -- 1-2cm
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9669=9667!!" THEN @cbe_breast_mass_size := 4 -- 3-4cm
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9669=9668!!" THEN @cbe_breast_mass_size := 5 -- > 4cm
        ELSE @cbe_breast_mass_size := null
      END AS cbe_breast_mass_size,
      CASE 
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9674=9670!!" THEN @cbe_breast_mass_texture := 1 -- soft
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9674=9671!!" THEN @cbe_breast_mass_texture := 2 -- firm
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9674=9672!!" THEN @cbe_breast_mass_texture := 3 -- rubbery
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9674=9673!!" THEN @cbe_breast_mass_texture := 4 -- hard
        ELSE @cbe_breast_mass_texture := null
      END AS cbe_breast_mass_texture,
      CASE 
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9677=9675!!" THEN @cbe_breast_mass_mobility := 1 -- fixed
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9677=9676!!" THEN @cbe_breast_mass_mobility := 2 -- mobile
        ELSE @cbe_breast_mass_mobility := null
      END AS cbe_breast_mass_mobility,
      CASE 
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!1121=1115!!" THEN @cbe_lymph_nodes_findings := 1 -- within normal limit
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!1121=161!!" THEN @cbe_lymph_nodes_findings := 2 -- enlarged
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!1121=9675!!" THEN @cbe_lymph_nodes_findings := 3 -- fixed
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!1121=9676!!" THEN @cbe_lymph_nodes_findings := 4 -- mobile
        ELSE @cbe_lymph_nodes_findings := null
      END AS cbe_lymph_nodes_findings,
      CASE 
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!7222=" THEN @cbe_assessment_notes := GetValues(obs, 7222)
        ELSE @cbe_assessment_notes := null
      END AS cbe_assessment_notes,
      CASE 
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!1272=" THEN @cbe_referrals_ordered := GetValues(obs, 1272)
        ELSE @cbe_referrals_ordered := null
      END AS cbe_referrals_ordered,
      CASE 
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!9158=" THEN @cbe_referral_date := GetValues(obs, 9158)
        ELSE @cbe_referral_date := null
      END AS cbe_referral_date,
      CASE 
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!1915=" THEN @cbe_other_referrals_ordered_non_coded := GetValues(obs, 1915)
        ELSE @cbe_other_referrals_ordered_non_coded := null
      END AS cbe_other_referrals_ordered_non_coded,
      CASE 
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!6327=" THEN @cbe_patient_education := GetValues(obs, 6327)
        ELSE @cbe_patient_education := null
      END AS cbe_patient_education,
      CASE 
        WHEN t1.encounter_type = 146 AND obs REGEXP "!!5096=" THEN @cbe_rtc_date := GetValues(obs, 5096)
        ELSE @cbe_rtc_date := null
      END AS cbe_rtc_date,
      -- 
      -- BREAST CANCER HISTORY & RISK ASSESSMENT FORM (encounter type 160)
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!1834=9651!!" THEN @ra_purpose_of_visit := 1 -- Annual screening
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!1834=1154!!" THEN @ra_purpose_of_visit := 2 -- New complaints
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!1834=1246!!" THEN @ra_purpose_of_visit := 3 -- Recall/scheduled visit
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!1834=2345!!" THEN @ra_purpose_of_visit := 4 -- Follow-up
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!1834=5622!!" THEN @ra_purpose_of_visit := 5 -- Other (non-coded)
        ELSE @ra_purpose_of_visit := null
      END AS ra_purpose_of_visit,
      CASE
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9560=1065!!" THEN @ra_had_menses_before_age_twelve := 1 -- Yes
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9560=1066!!" THEN @ra_had_menses_before_age_twelve := 2 -- No
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9560=9568!!" THEN @ra_had_menses_before_age_twelve := 3 -- Not sure
        ELSE @ra_had_menses_before_age_twelve := null
      END AS ra_had_menses_before_age_twelve,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9561=1065!!" THEN @ra_menses_permanently_stopped := 1 -- Yes
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9561=1066!!" THEN @ra_menses_permanently_stopped := 2 -- No
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9561=9568!!" THEN @ra_menses_permanently_stopped := 3 -- Not sure
        ELSE @ra_menses_permanently_stopped := null
      END AS ra_menses_permanently_stopped,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9562=" THEN @ra_age_when_menses_stopped := GetValues(obs, 9562)
        ELSE @ra_age_when_menses_stopped := null
      END AS ra_age_when_menses_stopped,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9626=1065!!" THEN @ra_has_used_hrt := 1 -- Yes
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9626=1066!!" THEN @ra_has_used_hrt := 2 -- No
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9626=9568!!" THEN @ra_has_used_hrt := 3 -- Not sure
        ELSE @ra_has_used_hrt := null
      END AS ra_has_used_hrt,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9627=" THEN @ra_age_at_first_hrt_use := GetValues(obs, 9627)
        ELSE @ra_age_at_first_hrt_use := null
      END AS ra_age_at_first_hrt_use,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9628=" THEN @ra_age_at_last_hrt_use := GetValues(obs, 9628) 
        ELSE @ra_age_at_last_hrt_use := null
      END AS ra_age_at_last_hrt_use,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9629=" THEN @ra_years_of_hrt_use := GetValues(obs, 9629)
        ELSE @ra_years_of_hrt_use := null
      END AS ra_years_of_hrt_use,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9630=9573!!" THEN @ra_type_of_hrt_used := 1 -- Estrogen
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9630=6217!!" THEN @ra_type_of_hrt_used := 2 -- Progesterone
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9630=6218!!" THEN @ra_type_of_hrt_used := 3 -- Combined oral contraceptive pill
        ELSE @ra_type_of_hrt_used := null
      END AS ra_type_of_hrt_used,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9563=1065!!" THEN @ra_has_ever_given_birth := 1 -- Yes
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9563=1066!!" THEN @ra_has_ever_given_birth := 2 -- No
        ELSE @ra_has_ever_given_birth := null
      END AS ra_has_ever_given_birth,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9564=" THEN @ra_age_at_first_birth := GetValues(obs, 9564)
        ELSE @ra_age_at_first_birth := null
      END AS ra_age_at_first_birth,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!5624=" THEN @ra_gravida := GetValues(obs, 5624)
        ELSE @ra_gravida := null
      END AS ra_gravida,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!1053=" THEN @ra_parity := GetValues(obs, 1053)
        ELSE @ra_parity := null
      END AS ra_parity,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!2065=1065!!" THEN @ra_smokes_cigarettes := 1 -- Yes
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!2065=1066!!" THEN @ra_smokes_cigarettes := 2 -- No
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!2065=1679!!" THEN @ra_smokes_cigarettes := 3 -- Stopped
        ELSE @ra_smokes_cigarettes := null
      END AS ra_smokes_cigarettes,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!2069=" THEN @ra_sticks_smoked_per_day := GetValues(obs, 2069)
        ELSE @ra_sticks_smoked_per_day := null
      END AS ra_sticks_smoked_per_day,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!7973=1065!!" THEN @ra_uses_tobacco := 1 -- Yes 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!7973=1066!!" THEN @ra_uses_tobacco := 2 -- No
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!7973=1679!!" THEN @ra_uses_tobacco := 3 -- Not sure
        ELSE @ra_uses_tobacco := null
      END AS ra_uses_tobacco,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!8144=" THEN @ra_years_of_tobacco_use := GetValues(obs, 8144)
        ELSE @ra_years_of_tobacco_use := null
      END AS ra_years_of_tobacco_use,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!8143=" THEN @ra_times_tobacco_used_per_day := GetValues(obs, 8143)
        ELSE @ra_times_tobacco_used_per_day := null
      END AS ra_times_tobacco_used_per_day,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!1684=1065!!" THEN @ra_uses_alcohol := 1 -- Yes 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!1684=1066!!" THEN @ra_uses_alcohol := 2 -- No
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!1684=1679!!" THEN @ra_uses_alcohol := 3 -- Stopped
        ELSE @ra_uses_alcohol := null
      END AS ra_uses_alcohol,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!1685=" THEN @ra_type_of_alcohol_used := GetValues(obs, 1685) 
        ELSE @ra_type_of_alcohol_used := null
      END AS ra_type_of_alcohol_used,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!8170=" THEN @ra_years_of_alcohol_use := GetValues(obs, 8170) 
        ELSE @ra_years_of_alcohol_use := null
      END AS ra_years_of_alcohol_use,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9553=5006!!" THEN @ra_presence_of_chief_complaint := 1 -- asymptomatic
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9553=1069!!" THEN @ra_presence_of_chief_complaint := 2 -- symptomatic
        ELSE @ra_presence_of_chief_complaint := null
      END AS ra_presence_of_chief_complaint,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!6697=" THEN @ra_breast_lump_location := GetValues(obs, 6697)
        ELSE @ra_breast_lump_location := null
      END AS ra_breast_lump_location,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9557=" THEN @ra_nipple_discharge_location := GetValues(obs, 9557)
        ELSE @ra_nipple_discharge_location := null
      END AS ra_nipple_discharge_location,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9558=" THEN @ra_nipple_or_skin_retraction_location := GetValues(obs, 9558)
        ELSE @ra_nipple_or_skin_retraction_location := null
      END AS ra_nipple_or_skin_retraction_location,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9574=" THEN @ra_breast_swelling_location := GetValues(obs, 9574)
        ELSE @ra_breast_swelling_location := null
      END AS ra_breast_swelling_location,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9577=" THEN @ra_breast_rash_location := GetValues(obs, 9577)
        ELSE @ra_breast_rash_location := null
      END AS ra_breast_rash_location,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9575=" THEN @ra_breast_pain_location := GetValues(obs, 9575)
        ELSE @ra_breast_pain_location := null
      END AS ra_breast_pain_location,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9576=" THEN @ra_other_changes_location := GetValues(obs, 9576)
        ELSE @ra_other_changes_location := null
      END AS ra_other_changes_location,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9559=1065!!" THEN @ra_has_ever_done_a_clinical_breast_exam := 1 -- Yes
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9559=1066!!" THEN @ra_has_ever_done_a_clinical_breast_exam := 2 -- No
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9559=9568!!" THEN @ra_has_ever_done_a_clinical_breast_exam := 3 -- Not sure
        ELSE @ra_has_ever_done_a_clinical_breast_exam := null
      END AS ra_has_ever_done_a_clinical_breast_exam,
      CASE 
      WHEN t1.encounter_type = 160 AND obs regexp "!!9559=1065!!" AND obs REGEXP "!!6251=" THEN @ra_past_clinical_breast_exam_results := SUBSTRING(GetValues(obs, 6251), 1, 4)
        ELSE @ra_past_clinical_breast_exam_results := null
      END AS ra_past_clinical_breast_exam_results,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9594=1065!!" THEN @ra_has_ever_done_a_mammogram := 1 -- Yes
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9594=1066!!" THEN @ra_has_ever_done_a_mammogram := 2 -- No
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9594=9568!!" THEN @ra_has_ever_done_a_mammogram := 3 -- Not sure
        ELSE @ra_has_ever_done_a_mammogram := null
      END AS ra_has_ever_done_a_mammogram,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9595=1115!!" THEN @ra_past_mammogram_results := 1 -- Normal
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9595=1116!!" THEN @ra_past_mammogram_results := 2 -- Abnormal
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9595=1067!!" THEN @ra_past_mammogram_results := 3 -- Unknown
        ELSE @ra_past_mammogram_results := null
      END AS ra_past_mammogram_results,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9597=1065!!" THEN @ra_has_ever_done_a_breast_ultrasound := 1 -- Yes
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9597=1066!!" THEN @ra_has_ever_done_a_breast_ultrasound := 2 -- No
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9597=9568!!" THEN @ra_has_ever_done_a_breast_ultrasound := 3 -- Not sure
        ELSE @ra_has_ever_done_a_breast_ultrasound := null
      END AS ra_has_ever_done_a_breast_ultrasound,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9598=1065!!" THEN @ra_has_ever_done_a_breast_biopsy := 1 -- Yes
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9598=1066!!" THEN @ra_has_ever_done_a_breast_biopsy := 2 -- No
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9598=9568!!" THEN @ra_has_ever_done_a_breast_biopsy := 3 -- Not sure
        ELSE @ra_has_ever_done_a_breast_biopsy := null
      END AS ra_has_ever_done_a_breast_biopsy,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9599=" THEN @ra_number_of_breast_biopsies_done := GetValues(obs, 9599)
        ELSE @ra_number_of_breast_biopsies_done := null
      END AS ra_number_of_breast_biopsies_done,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!6509=" THEN @ra_type_of_breast_biopsy_done_in_the_past := GetValues(obs, 6509) 
        ELSE @ra_type_of_breast_biopsy_done_in_the_past := null
      END AS ra_type_of_breast_biopsy_done_in_the_past,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9566=1065!!" THEN @ra_ever_had_chest_radiation_treatment := 1 -- Yes
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9566=1066!!" THEN @ra_ever_had_chest_radiation_treatment := 2 -- No
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9566=1679!!" THEN @ra_ever_had_chest_radiation_treatment := 3 -- Not sure
        ELSE @ra_ever_had_chest_radiation_treatment := null
      END AS ra_ever_had_chest_radiation_treatment,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9644=1065!!" THEN @ra_has_ever_had_breast_surgery_not_for_breast_cancer := 1 -- Yes
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9644=1066!!" THEN @ra_has_ever_had_breast_surgery_not_for_breast_cancer := 2 -- No
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9644=1679!!" THEN @ra_has_ever_had_breast_surgery_not_for_breast_cancer := 3 -- Not sure
        ELSE @ra_has_ever_had_breast_surgery_not_for_breast_cancer := null
      END AS ra_has_ever_had_breast_surgery_not_for_breast_cancer,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9566=1065!!" AND obs REGEXP "!!9649=9648!!" THEN @ra_type_of_breast_surgery_done := 1 -- Breast implants
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9566=1065!!" AND obs REGEXP "!!9649=9647!!" THEN @ra_type_of_breast_surgery_done := 2 -- Breast reduction surgery
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9566=1065!!" AND obs REGEXP "!!9649=5622!!" THEN @ra_type_of_breast_surgery_done := 3 -- Other (non-coded)
        ELSE @ra_type_of_breast_surgery_done := null
      END AS ra_type_of_breast_surgery_done,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!10172=1065!!" THEN @ra_any_family_member_diagnosed_with_breast_cancer := 1 -- Yes
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!10172=1066!!" THEN @ra_any_family_member_diagnosed_with_breast_cancer := 2 -- No
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!10172=1067!!" THEN @ra_any_family_member_diagnosed_with_breast_cancer := 3 -- Unknown
        ELSE @ra_any_family_member_diagnosed_with_breast_cancer := null
      END AS ra_any_family_member_diagnosed_with_breast_cancer,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9631=" THEN @ra_relatives_with_breast_ca_diagnosis_at_age_fifty_or_younger := GetValues(obs, 9631) 
        ELSE @ra_relatives_with_breast_ca_diagnosis_at_age_fifty_or_younger := null
      END AS ra_relatives_with_breast_ca_diagnosis_at_age_fifty_or_younger,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9632=" THEN @ra_relatives_with_breast_ca_diagnosis_after_age_fifty := GetValues(obs, 9632) 
        ELSE @ra_relatives_with_breast_ca_diagnosis_after_age_fifty := null
      END AS ra_relatives_with_breast_ca_diagnosis_after_age_fifty,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9633=" THEN @ra_male_relatives_diagnosed_with_breast_cancer := GetValues(obs, 9633) 
        ELSE @ra_male_relatives_diagnosed_with_breast_cancer := null
      END AS ra_male_relatives_diagnosed_with_breast_cancer,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9634=" THEN @ra_family_members_diagnosed_with_ovarian_cancer := GetValues(obs, 9634) 
        ELSE @ra_family_members_diagnosed_with_ovarian_cancer := null
      END AS ra_family_members_diagnosed_with_ovarian_cancer,
      CASE 
        WHEN t1.encounter_type = 160 AND obs REGEXP "!!9635=" THEN @ra_relatives_with_history_of_breast_cancer := GetValues(obs, 9635)
        ELSE @ra_relatives_with_history_of_breast_cancer := null
      END AS ra_relatives_with_history_of_breast_cancer

      FROM flat_breast_cancer_screening_0 t1
      JOIN amrs.person p USING (person_id)
      ORDER BY person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
    );

    SET @prev_id = null;
    SET @cur_id = null;
    SET @prev_encounter_datetime = null;
    SET @cur_encounter_datetime = null;

    SET @prev_clinical_datetime = null;
    SET @cur_clinical_datetime = null;

    SET @next_encounter_type = null;
    SET @cur_encounter_type = null;

    SET @prev_clinical_location_id = null;
    SET @cur_clinical_location_id = null;

    ALTER TABLE flat_breast_cancer_screening_1 DROP prev_id, DROP cur_id;

    DROP TABLE IF EXISTS flat_breast_cancer_screening_2;
    CREATE TEMPORARY TABLE flat_breast_cancer_screening_2

    (SELECT 
      *,
      @prev_id := @cur_id as prev_id,
      @cur_id := person_id as cur_id,

      CASE
        WHEN @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
        ELSE @prev_encounter_datetime := null
      END AS next_encounter_datetime_breast_cancer_screening,

      @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

      CASE
        WHEN @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
        ELSE @next_encounter_type := null
      END AS next_encounter_type_breast_cancer_screening,

      @cur_encounter_type := encounter_type as cur_encounter_type,

      CASE
        WHEN @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
        ELSE @prev_clinical_datetime := null
      END AS next_clinical_datetime_breast_cancer_screening,

      CASE
        WHEN @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
        ELSE @prev_clinical_location_id := null
      END AS next_clinical_location_id_breast_cancer_screening,

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
      END AS next_clinical_rtc_date_breast_cancer_screening,

      CASE
        WHEN is_clinical_encounter then @cur_clinical_rtc_date := screening_rtc_date
        WHEN @prev_id = @cur_id then @cur_clinical_rtc_date
        ELSE @cur_clinical_rtc_date:= null
      END AS cur_clinical_rtc_date

      FROM flat_breast_cancer_screening_1
      ORDER BY person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
    );

	  ALTER TABLE flat_breast_cancer_screening_2 DROP prev_id, DROP cur_id, DROP cur_encounter_type, DROP cur_encounter_datetime, DROP cur_clinical_rtc_date;

    SET @prev_id = null;
    SET @cur_id = null;
    SET @prev_encounter_type = null;
    SET @cur_encounter_type = null;
    SET @prev_encounter_datetime = null;
    SET @cur_encounter_datetime = null;
    SET @prev_clinical_datetime = null;
    SET @cur_clinical_datetime = null;
    SET @prev_clinical_location_id = null;
    SET @cur_clinical_location_id = null;

    DROP TEMPORARY TABLE IF EXISTS flat_breast_cancer_screening_3;
    CREATE TEMPORARY TABLE flat_breast_cancer_screening_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
    (SELECT
      *,
      @prev_id := @cur_id as prev_id,
      @cur_id := t1.person_id as cur_id,

      CASE
        WHEN @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
        ELSE @prev_encounter_type:=null
      END AS prev_encounter_type_breast_cancer_screening,	
      
      @cur_encounter_type := encounter_type as cur_encounter_type,

      CASE
        WHEN @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
        ELSE @prev_encounter_datetime := null
      END AS prev_encounter_datetime_breast_cancer_screening,

      @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

      CASE
        WHEN @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
        ELSE @prev_clinical_datetime := null
      END AS prev_clinical_datetime_breast_cancer_screening,

      CASE
        WHEN @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
        ELSE @prev_clinical_location_id := null
      END AS prev_clinical_location_id_breast_cancer_screening,

      CASE
        WHEN is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
        WHEN @prev_id = @cur_id then @cur_clinical_datetime
        ELSE @cur_clinical_datetime := null
      END AS cur_clinical_datetime,

      CASE
        WHEN is_clinical_encounter then @cur_clinical_location_id := location_id
        WHEN @prev_id = @cur_id then @cur_clinical_location_id
        ELSE @cur_clinical_location_id := null
      END AS cur_clinical_location_id,

      CASE
        WHEN @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
        ELSE @prev_clinical_rtc_date := null
      END AS prev_clinical_rtc_date_breast_cancer_screening,

      CASE
        WHEN is_clinical_encounter then @cur_clinical_rtc_date := screening_rtc_date
        WHEN @prev_id = @cur_id then @cur_clinical_rtc_date
        ELSE @cur_clinical_rtc_date:= null
      END AS cur_clinic_rtc_date

      FROM flat_breast_cancer_screening_2 t1
      ORDER BY person_id, date(encounter_datetime), encounter_type_sort_index
    );

    SELECT count(*) into @new_encounter_rows FROM flat_breast_cancer_screening_3;
            
    SELECT @new_encounter_rows;                    
    SET @total_rows_written = @total_rows_written + @new_encounter_rows;
    SELECT @total_rows_written;

    SET @dyn_sql := CONCAT(
      'REPLACE INTO ', @write_table,											  
      '(SELECT
          null,
          person_id,
          encounter_id,
          encounter_type,
          encounter_datetime,
          visit_id,
          location_id,
          gender,
          age,
          death_date,
          current_visit_type,
          complaints_presenting_with,
          had_menses_before_age_twelve,
          menses_permanently_stopped,
          age_when_menses_stopped,
          has_used_hrt,
          age_at_first_hrt_use,
          age_at_last_hrt_use,
          years_of_hrt_use,
          type_of_hrt_used,
          has_ever_given_birth,
          age_at_first_birth,
          gravida,
          parity,
          smokes_cigarettes,
          sticks_smoked_per_day,
          uses_tobacco,
          years_of_tobacco_use,
          times_tobacco_used_per_day,
          uses_alcohol,
          type_of_alcohol_used,
          years_of_alcohol_use,
          hiv_status,
          has_ever_done_a_clinical_breast_exam,
          past_clinical_breast_exam_results,
          has_ever_done_a_mammogram,
          past_mammogram_results,
          has_ever_done_a_breast_ultrasound,
          has_ever_done_a_breast_biopsy,
          number_of_breast_biopsies_done,
          type_of_breast_biopsy_done_in_the_past,
          ever_had_chest_radiation_treatment,
          has_ever_had_breast_surgery_not_for_breast_cancer,
          type_of_breast_surgery_done,
          personal_history_of_breast_cancer,
          age_at_breast_cancer_diagnosis,
          family_history_of_breast_cancer,
          relatives_with_breast_ca_diagnosis_at_age_fifty_or_younger,
          relatives_with_breast_ca_diagnosis_after_age_fifty,
          male_relatives_diagnosed_with_breast_cancer,
          family_members_diagnosed_with_ovarian_cancer,
          relatives_with_history_of_breast_cancer,
          breast_exam_findings_this_visit,
          breast_findings_location,
          breast_findings_quadrant,
          breast_mass_shape,
          breast_mass_margins,
          breast_mass_size,
          breast_mass_texture,
          breast_mass_mobility,
          breast_symmetry,
          lymph_node_exam_findings,
          axillary_lymph_nodes_location,
          supra_clavicular_location,
          infra_clavicular_location,          
          screening_patient_education,
          histological_investigations_done,
          radiological_investigations_done,
          screening_referral,
          screening_rtc_date,
          bif_mammogram_results,
          bif_mammogram_tumor_size,
          bif_mammogram_workup_date,
          bif_mammogram_results_description,
          bif_breast_ultrasound_results,
          bif_ultrasound_tumor_size,
          bif_ultrasound_workup_date,
          bif_ultrasound_results_description,
          bif_concordant_cbe_and_imaging_results,
          bif_fna_breast_results,
          bif_fna_workup_date,
          bif_fna_degree_of_malignancy,
          bif_fna_results_description,
          bif_fna_diagnosis_date,
          bif_breast_biopsy_results,  
          bif_breast_biopsy_malignancy_type,
          bif_tumor_grade_or_differentiation,
          bif_breast_biopsy_sample_collection_date,
          bif_breast_biopsy_results_description,
          bif_histology_reporting_date,
          bif_ihc_er,
          bif_ihc_pr,
          bif_ihc_her2,
          bif_ihc_ki67,
          bif_mode_of_breast_screening,
          bif_date_patient_informed_and_referred_for_treatment,
          bif_referrals_ordered,
          bif_assessment_notes,
          bif_rtc_date,
          cbe_purpose_of_visit,
          cbe_breast_exam_findings,
          cbe_breast_findings_location,
          cbe_breast_findings_quadrant,
          cbe_breast_symmetry,
          cbe_breast_mass_shape,
          cbe_breast_mass_margins,
          cbe_breast_mass_size,
          cbe_breast_mass_texture,
          cbe_breast_mass_mobility,
          cbe_lymph_nodes_findings,
          cbe_assessment_notes,
          cbe_referrals_ordered,
          cbe_referral_date,
          cbe_other_referrals_ordered_non_coded,
          cbe_patient_education,
          cbe_rtc_date,
          ra_purpose_of_visit,
          ra_had_menses_before_age_twelve,
          ra_menses_permanently_stopped,
          ra_age_when_menses_stopped,
          ra_has_used_hrt,
          ra_age_at_first_hrt_use,
          ra_age_at_last_hrt_use,
          ra_years_of_hrt_use,
          ra_type_of_hrt_used,
          ra_has_ever_given_birth,
          ra_age_at_first_birth,
          ra_gravida,
          ra_parity,
          ra_smokes_cigarettes,
          ra_sticks_smoked_per_day,
          ra_uses_tobacco,
          ra_years_of_tobacco_use,
          ra_times_tobacco_used_per_day,
          ra_uses_alcohol,
          ra_type_of_alcohol_used,
          ra_years_of_alcohol_use,
          ra_presence_of_chief_complaint,
          ra_breast_lump_location,
          ra_nipple_discharge_location,
          ra_nipple_or_skin_retraction_location,
          ra_breast_swelling_location,
          ra_breast_rash_location,
          ra_breast_pain_location,
          ra_other_changes_location,
          ra_has_ever_done_a_clinical_breast_exam,
          ra_past_clinical_breast_exam_results,
          ra_has_ever_done_a_mammogram,
          ra_past_mammogram_results,
          ra_has_ever_done_a_breast_ultrasound,
          ra_has_ever_done_a_breast_biopsy,
          ra_number_of_breast_biopsies_done,
          ra_type_of_breast_biopsy_done_in_the_past,
          ra_ever_had_chest_radiation_treatment,
          ra_has_ever_had_breast_surgery_not_for_breast_cancer,
          ra_type_of_breast_surgery_done,
          ra_any_family_member_diagnosed_with_breast_cancer,
          ra_relatives_with_breast_ca_diagnosis_at_age_fifty_or_younger,
          ra_relatives_with_breast_ca_diagnosis_after_age_fifty,
          ra_male_relatives_diagnosed_with_breast_cancer,
          ra_family_members_diagnosed_with_ovarian_cancer,
          ra_relatives_with_history_of_breast_cancer,
          prev_encounter_datetime_breast_cancer_screening,
          next_encounter_datetime_breast_cancer_screening,
          prev_encounter_type_breast_cancer_screening,
          next_encounter_type_breast_cancer_screening,
          prev_clinical_datetime_breast_cancer_screening,
          next_clinical_datetime_breast_cancer_screening,
          prev_clinical_location_id_breast_cancer_screening,
          next_clinical_location_id_breast_cancer_screening,
          prev_clinical_rtc_date_breast_cancer_screening,
          next_clinical_rtc_date_breast_cancer_screening
        FROM flat_breast_cancer_screening_3 t1
        JOIN amrs.location t2 USING (location_id))'
    );      

    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;

    SET @dyn_sql=CONCAT('DELETE t1 FROM ',@queue_table,' t1 JOIN flat_breast_cancer_screening_build_queue__0 t2 USING (person_id);'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
                        
    SET @dyn_sql=CONCAT('SELECT count(*) into @person_ids_count FROM ',@queue_table,';'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;

    SET @cycle_length := TIMESTAMPDIFF(second,@loop_start_time,now());
    SET @total_time := @total_time + @cycle_length;
    SET @cycle_number := @cycle_number + 1;
    
    SET @remaining_time := ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);

    SELECT 
      @person_ids_count AS 'persons remaining',
      @cycle_length AS 'Cycle time (s)',
      CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
      @remaining_time AS 'Estimated minutes remaining ';

  END WHILE;
                 
  IF (@query_type = "build") THEN
    SET @dyn_sql := CONCAT('DROP table ', @queue_table, ';'); 
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
      CONCAT(
        @start_write,
        ' : Writing ',
        @total_rows_to_write,
        ' to ',
        @primary_table
      );

      SET @dyn_sql := CONCAT('REPLACE INTO ', @primary_table, '(SELECT * FROM ', @write_table,');');
      PREPARE s1 FROM @dyn_sql; 
      EXECUTE s1; 
      DEALLOCATE PREPARE s1;
      
      SET @finish_write := now();
      SET @time_to_write := TIMESTAMPDIFF(second,@start_write,@finish_write);

      SELECT 
        CONCAT(@finish_write,
            ' : Completed writing rows. Time to write to primary table: ',
            @time_to_write,
            ' seconds ');                        
                        
      SET @dyn_sql := CONCAT('DROP table ',@write_table,';'); 
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
  INSERT INTO etl.flat_log values (@start,@last_date_created,@table_version,TIMESTAMPDIFF(second,@start,@end));
	
  SELECT 
    CONCAT(@table_version,
      ' : Time to complete: ',
      TIMESTAMPDIFF(MINUTE, @start, @end),
      ' minutes'
    );

END
