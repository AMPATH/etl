DELIMITER $$
CREATE PROCEDURE `generate_flat_anticoagulation_treatment`(IN query_type VARCHAR(50), IN queue_number INT, IN queue_size INT, IN cycle_size INT)
BEGIN
    SET @primary_table := "flat_anticoagulation_treatment";
    SET @query_type := query_type;
                     
    SET @total_rows_written := 0;
                    
    SET @encounter_types := "(189, 190, 191)"; -- Initial / Triage / Clinical encounters
    
    SET @start := NOW();
    SET @table_version := "flat_anticoagulation_treatment_v1.0";

    SET session sort_buffer_size := 512000000;

    SET @sep := " ## ";
    SET @boundary := "!!";
    SET @last_date_created := (SELECT MAX(max_date_created) FROM etl.flat_obs);

    CREATE TABLE IF NOT EXISTS flat_anticoagulation_treatment (
        date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        person_id int,
        encounter_id int,
        encounter_type int,
	    	encounter_datetime datetime,
        location_id int,
        location_name varchar(100),
        age int,
        gender char(10),
        death_date datetime,
        cur_visit_type int,
        temperature decimal(3, 1),
        weight decimal(3, 1),
        height decimal(3, 1),
        systolic_bp int,
        diastolic_bp int,
        pulse int,
        respiratory_rate int,
        rcc int,
        lcc int,
        inr int,
        chief_complaint int,
        hospitalized_since_last_visit int,
        patient_reported_signs_of_bleeding int,
        blood_in_stool int,
        blood_in_urine int,
        bruising int,
        gum_bleeds int,
        epistaxis int,
        secondary_hemorrhage int,
        severity int,
        time_bleeding_symptoms_last int,
        frequency_of_occurrence_bleeding_symptoms int,
        blood_transfusion_required int,
        units_of_blood_transfused int,
        patient_reported_symptoms_of_blood_clot_or_embolism int,
        presumed_clot_location int,
        blood_clot_onset int,
        time_blood_clot_symptoms_last int,
        frequency_of_occurrence_blood_clot_symptoms int,
        cough_duration int,
        chest_pain_duration int,
        chest_pain_location int,
        chest_pain_quality int,
        change_in_activity_level int,
        change_in_health_status int,
        patient_scheduled_for_surgery int,
        patient_reported_problem_added varchar(1000),
        patient_type int,
        pregnancy_status int,
        insurance int,
        health_assistance int,
        referral_from int,
        anticoag_duration_necessary int,
        conditions_requiring_anticoag int,
        afib_indications_for_anticoag int,
        dvt_or_pe_indications_for_anticoag int,
        hvd_indications_for_anticoag int,
        other_indications_for_anticoag int,
        travel_to_clinic_time int,
        primary_transportation_mode int,
        year_diagnosed int,
        first_anticoag_diagnosis_location int,
        has_other_medical_condition int,
        review_of_medical_history int,
        other_medical_condition int,
        hiv_status_known int,
        last_hiv_test_result int,
        last_hiv_test_date datetime,
        hiv_positive_and_enrolled_in_ampath int,
        has_medication_allergies int,
        medication_allergic_to varchar(100),
        allergic_reaction int,
        smokes_cigarretes int,
        sticks_smoked_per_day int,
        years_of_cig_use int,
        period_since_stopping_smoking int,
        years_since_last_cig_use int,
        uses_tobacco int,
        years_of_tobacco_use int,
        uses_alcohol int,
        type_of_alcohol_used int,
        used_herbal_medicine int,
        herbal_medication_used varchar(100),
        adherence_prior_to_visit int,
        medication_added int,
        medication_frequency int,
        patient_reported_medication_side_effects int,
        change_in_meds_since_last_visit int,
        medication_changed_since_last_visit int,
        other_changed_meds varchar(100),
        past_anticoag_drug_used int,
        drug_action_plan int,
        medication_plan_no_of_milligrams int,
        medication_plan_day_of_the_week int,
        anticoag_adherence_last_seven_days int,
        anticoag_nonadherence int,
        nonadherence_missed_medication_reason int,
        additional_nonadherence_comments varchar(1000),
        other_drug_dispensed int,
        number_of_tablets_other_drug int,
        was_protocol_followed int,
        reason_protocol_not_followed varchar(1000),
        injectable_anticoag_stop_date datetime,
        test_ordered int,
        radiology_test_ordered varchar(1000),
        other_lab_orders varchar(1000),
        referrals_ordered int,
        referral_urgency_level int,
        other_referrals varchar(1000),
        assessment_notes varchar(1000),
        PRIMARY KEY encounter_id (encounter_id),
        INDEX date_created (date_created)
    );

    -- Build step
    IF (@query_type = "build") THEN
        SELECT 'BUILDING..........................................';
							
        SET @write_table := CONCAT('flat_anticoagulation_treatment_temp_', queue_number);
        SET @queue_table := CONCAT('flat_anticoagulation_treatment_build_queue_', queue_number);

        SET @dyn_sql := CONCAT('CREATE TABLE IF NOT EXISTS ', @write_table, ' LIKE ', @primary_table);
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  

        SET @dyn_sql := CONCAT('CREATE TABLE IF NOT EXISTS ', @queue_table, ' (SELECT * FROM flat_anticoagulation_treatment_build_queue LIMIT ', queue_size, ');'); 
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;

        SET @dyn_sql=CONCAT('DELETE t1 from flat_anticoagulation_treatment_build_queue t1 join ', @queue_table, ' t2 using (person_id);'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1; 
    END IF;

    -- Sync step
    IF (@query_type = 'sync') THEN
        SELECT 'SYNCING.....................................';
        
        SET @write_table := 'flat_anticoagulation_treatment';
        SET @queue_table := 'flat_anticoagulation_treatment_sync_queue';

        CREATE TABLE IF NOT EXISTS flat_anticoagulation_treatment_sync_queue (person_id INT PRIMARY KEY);
        
        SET @last_update := null;

        SELECT MAX(date_updated) INTO @last_update FROM etl.flat_log WHERE table_name = @table_version;

        SELECT 'Finding patients in amrs.encounters...';
            REPLACE INTO flat_anticoagulation_treatment_sync_queue
            (SELECT DISTINCT
                patient_id
              FROM
                amrs.encounter
              WHERE 
                date_changed > @last_update
            );

        SELECT 'Finding patients in flat_obs...';
            REPLACE INTO flat_anticoagulation_treatment_sync_queue
            (SELECT DISTINCT
                person_id
              FROM
                etl.flat_obs
              WHERE
                max_date_created > @last_update
            );

        SELECT 'Finding patients in flat_lab_obs...';
            REPLACE INTO flat_anticoagulation_treatment_sync_queue
            (SELECT DISTINCT
                person_id
              FROM
                etl.flat_lab_obs
              WHERE
                max_date_created > @last_update
            );

        SELECT 'Finding patients in flat_orders...';
            REPLACE INTO flat_anticoagulation_treatment_sync_queue
            (SELECT DISTINCT
                person_id
              FROM
                etl.flat_orders
              WHERE
                max_date_created > @last_update
            );

            REPLACE INTO flat_anticoagulation_treatment_sync_queue
            (SELECT 
                person_id
              FROM 
		            amrs.person 
	            WHERE 
                date_voided > @last_update
            );

            REPLACE INTO flat_anticoagulation_treatment_sync_queue
            (SELECT 
                person_id
              FROM 
		            amrs.person 
	            WHERE 
                date_changed > @last_update
            );
    END IF;

    -- Remove test patients
    SET @dyn_sql := CONCAT('DELETE t1 FROM ', @queue_table, ' t1
        JOIN amrs.person_attribute t2 USING (person_id)
        WHERE t2.person_attribute_type_id = 28 AND value = "true" AND voided = 0');
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;
                    
    SET @person_ids_count = 0;
    SET @dyn_sql=CONCAT('SELECT COUNT(*) INTO @person_ids_count FROM ', @queue_table); 
    PREPARE s1 from @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;

    SELECT @person_ids_count AS 'num of patients to update';

    SET @dyn_sql := CONCAT('DELETE t1 FROM ',@primary_table, ' t1 JOIN ', @queue_table,' t2 USING (person_id);'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
 
    SET @person_ids_count = 0;
    SET @dyn_sql := CONCAT('SELECT COUNT(*) INTO @person_ids_count FROM ', @queue_table); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;

    SELECT @person_ids_count AS 'num patients to update';

    SET @dyn_sql := CONCAT('DELETE t1 FROM ', @primary_table, ' t1 join ', @queue_table,' t2 USING (person_id);'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
                    
    SET @total_time = 0;
    SET @cycle_number = 0;
    
    SET @total_time = 0;
    SET @cycle_number = 0;
    
    WHILE @person_ids_count > 0 DO
        SET @loop_start_time := NOW();

        -- Create temporary table with a set of person ids             
        DROP TEMPORARY TABLE IF EXISTS flat_anticoagulation_treatment_build_queue__0;

        SET @dyn_sql := CONCAT('CREATE TEMPORARY TABLE flat_anticoagulation_treatment_build_queue__0 (person_id INT PRIMARY KEY) (SELECT * FROM ', @queue_table, ' LIMIT ', cycle_size, ');');
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
                    
        DROP TEMPORARY TABLE IF EXISTS flat_anticoagulation_treatment_0a;
        SET @dyn_sql := CONCAT(
            'CREATE TEMPORARY TABLE flat_anticoagulation_treatment_0a
            (SELECT 
                t1.person_id,
                t1.encounter_id,
                t1.encounter_datetime,
				        t1.encounter_type,
                t1.location_id,
                t1.obs,
                t1.obs_datetimes,
                t2.orders
            FROM
                etl.flat_obs t1
                    JOIN
                flat_anticoagulation_treatment_build_queue__0 t0 USING (person_id)
                    LEFT JOIN
                etl.flat_orders t2 USING (encounter_id)
            WHERE
                t1.encounter_type IN ', @encounter_types, ');');
                            
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  

        INSERT INTO flat_anticoagulation_treatment_0a
        (SELECT
            t1.person_id,
            t1.encounter_id,
            t1.test_datetime,
            t1.encounter_type,
	          null,
            t1.obs,
            null,
            null
        FROM 
          etl.flat_lab_obs t1
              JOIN
          flat_anticoagulation_treatment_build_queue__0 t0 USING (person_id)
        );

        DROP TEMPORARY TABLE IF EXISTS flat_anticoagulation_treatment_0;
        CREATE TEMPORARY TABLE flat_anticoagulation_treatment_0 (INDEX encounter_id (encounter_id), INDEX person_enc (person_id, encounter_datetime))
        (SELECT 
            * 
        FROM
          flat_anticoagulation_treatment_0a
        ORDER BY
          person_id, DATE(encounter_datetime)
        );
                        
		    SET @cur_visit_type := null;
        SET @temperature := null;
        SET @weight := null;
        SET @height := null;
        SET @systolic_bp := null;
        SET @diastolic_bp := null;
        SET @pulse := null;
        SET @respiratory_rate := null;
        SET @rcc := null;
        SET @lcc := null;
        SET @inr := null;
        SET @chief_complaint := null;
        SET @hospitalized_since_last_visit := null;
        SET @patient_reported_signs_of_bleeding := null;
        SET @blood_in_stool := null;
        SET @blood_in_urine := null;
        SET @bruising := null;
        SET @gum_bleeds := null;
        SET @epistaxis := null;
        SET @secondary_hemorrhage := null;
        SET @severity := null;
        SET @time_bleeding_symptoms_last := null;
        SET @frequency_of_occurrence_bleeding_symptoms := null;
        SET @blood_transfusion_required := null;
        SET @units_of_blood_transfused := null;
        SET @patient_reported_symptoms_of_blood_clot_or_embolism := null;
        SET @presumed_clot_location := null;
        SET @blood_clot_onset := null;
        SET @time_blood_clot_symptoms_last := null;
        SET @frequency_of_occurrence_blood_clot_symptoms := null;
        SET @cough_duration := null;
        SET @chest_pain_duration := null;
        SET @chest_pain_location := null;
        SET @chest_pain_quality := null;
        SET @change_in_activity_level := null;
        SET @change_in_health_status := null;
        SET @patient_scheduled_for_surgery := null;
        SET @patient_reported_problem_added := null;
        SET @patient_type := null;
        SET @pregnancy_status := null;
        SET @insurance := null; 
        SET @health_assistance := null; 
        SET @referral_from := null; 
        SET @anticoag_duration_necessary := null; 
        SET @conditions_requiring_anticoag := null; 
        SET @afib_indications_for_anticoag := null; 
        SET @dvt_or_pe_indications_for_anticoag := null; 
        SET @hvd_indications_for_anticoag := null; 
        SET @other_indications_for_anticoag := null; 
        SET @travel_to_clinic_time := null;
        SET @primary_transportation_mode := null;
        SET @year_diagnosed := null; 
        SET @first_anticoag_diagnosis_location := null; 
        SET @has_other_medical_condition := null; 
        SET @review_of_medical_history := null; 
        SET @other_medical_condition := null; 
        SET @hiv_status_known := null; 
        SET @last_hiv_test_result := null; 
        SET @last_hiv_test_date := null; 
        SET @hiv_positive_and_enrolled_in_ampath := null; 
        SET @has_medication_allergies := null;
        SET @medication_allergic_to := null; 
        SET @allergic_reaction := null; 
        SET @smokes_cigarretes := null; 
        SET @sticks_smoked_per_day := null;
        SET @years_of_cig_use := null;
        SET @period_since_stopping_smoking := null;
        SET @years_since_last_cig_use := null;
        SET @uses_tobacco := null;
        SET @years_of_tobacco_use := null;
        SET @uses_alcohol := null;
        SET @type_of_alcohol_used := null;
        SET @used_herbal_medicine := null;
        SET @herbal_medication_used := null;
        SET @adherence_prior_to_visit := null;
        SET @medication_added := null;
        SET @medication_frequency := null;
        SET @patient_reported_medication_side_effects := null;
        SET @change_in_meds_since_last_visit := null;
        SET @medication_changed_since_last_visit := null;
        SET @other_changed_meds := null;
        SET @past_anticoag_drug_used := null;
        SET @drug_action_plan := null;
        SET @medication_plan_no_of_milligrams := null;
        SET @medication_plan_day_of_the_week := null;
        SET @anticoag_adherence_last_seven_days := null;
        SET @anticoag_nonadherence := null;
        SET @nonadherence_missed_medication_reason := null;
        SET @additional_nonadherence_comments := null;
        SET @other_drug_dispensed := null;
        SET @number_of_tablets_other_drug := null;
        SET @was_protocol_followed := null;
        SET @reason_protocol_not_followed := null;
        SET @injectable_anticoag_stop_date := null;
        SET @test_ordered := null;
        SET @radiology_test_ordered := null;
        SET @other_lab_orders := null;
        SET @referrals_ordered := null;
        SET @referral_urgency_level := null;
        SET @other_referrals := null;
        SET @assessment_notes := null;

        DROP TEMPORARY TABLE IF EXISTS flat_anticoagulation_treatment_1;

        CREATE TEMPORARY TABLE flat_anticoagulation_treatment_1
        (SELECT 
              obs,
              @prev_id := @cur_id as prev_id,
              @cur_id := t1.person_id as cur_id,
              t1.person_id,
              t1.encounter_id,
              t1.encounter_type,
              t1.encounter_datetime,
              t1.location_id,
              l.name AS location_name,
              CASE
                WHEN TIMESTAMPDIFF(YEAR, p.birthdate, curdate()) > 0 THEN round(TIMESTAMPDIFF(YEAR, p.birthdate, curdate()), 0)
                ELSE ROUND(TIMESTAMPDIFF(MONTH, p.birthdate, curdate()) / 12, 2)
              END AS age,
              p.gender,
              p.death_date,
              CASE
                  WHEN obs REGEXP '!!1839=7850!!' THEN @cur_visit_type := 1 -- Initial viist
                  WHEN obs REGEXP '!!1839=7037!!' THEN @cur_visit_type := 2 -- Referred from clinic
                  ELSE @cur_visit_type := null
              END AS cur_visit_type,
              CASE
                  WHEN obs REGEXP '!!5088=' then @temperature := GetValues(obs, 5088)
                  ELSE @temperature := null
              END AS temperature,
              CASE
                  WHEN obs REGEXP '!!5089=' then @weight := GetValues(obs, 5089)
                  ELSE @weight := null
              END AS weight,
			  CASE
                  WHEN obs REGEXP '!!5090=' then @height := GetValues(obs, 5090)
                  ELSE @height := null
              END AS height,
              CASE
                  WHEN obs REGEXP '!!5085=' then @systolic_bp := GetValues(obs, 5085)
                  ELSE @systolic_bp := null
              END AS systolic_bp,
              CASE
                  WHEN obs REGEXP '!!5086=' then @diastolic_bp := GetValues(obs, 5086)
                  ELSE @diastolic_bp := null
              END AS diastolic_bp,
              CASE
                  WHEN obs REGEXP '!!5097=' then @pulse := GetValues(obs, 5097)
                  ELSE @pulse := null
              END AS pulse,
              CASE
                  WHEN obs REGEXP '!!5242=' then @respiratory_rate := GetValues(obs, 5242)
                  ELSE @respiratory_rate := null
              END AS respiratory_rate,
              CASE
                  WHEN obs REGEXP '!!8899=' then @rcc := GetValues(obs, 8899)
                  ELSE @rcc := null
              END AS rcc,
              CASE
                  WHEN obs REGEXP '!!8898=' then @lcc := GetValues(obs, 8898)
                  ELSE @lcc := null
              END AS lcc,
              CASE
                  WHEN obs REGEXP '!!8167=' then @inr := GetValues(obs, 8167)
                  ELSE @inr := null
              END AS inr,
              CASE 
                  WHEN obs REGEXP '!!8916=1107!!' then @chief_complaint := 1 -- No complaints 
                  WHEN obs REGEXP '!!8916=1068!!' then @chief_complaint := 2 -- Having complaints
                  ELSE @chief_complaint := null
              END AS chief_complaint,
              CASE 
                  WHEN obs REGEXP '!!976=1066!!' then @hospitalized_since_last_visit := 0 -- No
                  WHEN obs REGEXP '!!976=1065!!' then @hospitalized_since_last_visit := 1 -- Yes
                  ELSE @hospitalized_since_last_visit := null
              END AS hospitalized_since_last_visit,
              CASE 
                  WHEN obs REGEXP '!!8917=1066!!' then @patient_reported_signs_of_bleeding := 0 -- No
                  WHEN obs REGEXP '!!8917=1065!!' then @patient_reported_signs_of_bleeding := 1 -- Yes
                  WHEN obs REGEXP '!!8917=173!!' then @patient_reported_signs_of_bleeding := 2 -- Epistaxis
                  WHEN obs REGEXP '!!8917=840!!' then @patient_reported_signs_of_bleeding := 3 -- Hematura
                  WHEN obs REGEXP '!!8917=6787!!' then @patient_reported_signs_of_bleeding := 4 -- Blood in stool
                  WHEN obs REGEXP '!!8917=8919!!' then @patient_reported_signs_of_bleeding := 5 -- Gum bleed
                  WHEN obs REGEXP '!!8917=8920!!' then @patient_reported_signs_of_bleeding := 6 -- Bruise
                  WHEN obs REGEXP '!!8917=8926!!' then @patient_reported_signs_of_bleeding := 7 -- Secondary hemorrhage
                  ELSE @patient_reported_signs_of_bleeding := null
              END AS patient_reported_signs_of_bleeding,
              CASE 
                  WHEN obs REGEXP '!!6787=1066!!' then @blood_in_stool := 0 -- No
                  WHEN obs REGEXP '!!6787=1065!!' then @blood_in_stool := 1 -- Yes
                  WHEN obs REGEXP '!!6787=1067!!' then @blood_in_stool := 2 -- Unknown
                  ELSE @blood_in_stool := null
              END AS blood_in_stool,
              CASE 
                  WHEN obs REGEXP '!!840=1066!!' then @blood_in_urine := 0 -- No
                  WHEN obs REGEXP '!!840=1065!!' then @blood_in_urine := 1 -- Yes
                  WHEN obs REGEXP '!!840=1067!!' then @blood_in_urine := 2 -- Unknown
                  ELSE @blood_in_urine := null
              END AS blood_in_urine,
              CASE 
                  WHEN obs REGEXP '!!8920=1066!!' then @bruising := 0
                  WHEN obs REGEXP '!!8920=1065!!' then @bruising := 1
                  WHEN obs REGEXP '!!8920=1067!!' then @bruising := 2
                  ELSE @bruising := null
              END AS bruising,
              CASE 
                  WHEN obs REGEXP '!!8919=1066!!' then @gum_bleeds := 0
                  WHEN obs REGEXP '!!8919=1065!!' then @gum_bleeds := 1
                  WHEN obs REGEXP '!!8919=1067!!' then @gum_bleeds := 2
                  ELSE @gum_bleeds:= null
              END AS gum_bleeds,
              CASE 
                  WHEN obs REGEXP '!!173=1066!!' then @epistaxis := 0
                  WHEN obs REGEXP '!!173=1065!!' then @epistaxis := 1
                  WHEN obs REGEXP '!!173=1067!!' then @epistaxis := 2
                  ELSE @epistaxis := null
              END AS epistaxis,
              CASE
                  WHEN obs REGEXP '!!8926=1066!!' then @secondary_hemorrhage := 0
                  WHEN obs REGEXP '!!8926=1065!!' then @secondary_hemorrhage := 1
                  WHEN obs REGEXP '!!8926=1067!!' then @secondary_hemorrhage := 2
                  ELSE @secondary_hemorrhage := null
              END AS secondary_hemorrhage,
              CASE 
                  WHEN obs REGEXP '!!8792=1743!!' then @severity := 1 -- Mild
                  WHEN obs REGEXP '!!8792=1744!!' then @severity := 2 -- Moderate
                  WHEN obs REGEXP '!!8792=1745!!' then @severity := 3 -- Severe
                  ELSE @severity := null
              END AS severity,
              CASE 
                  WHEN obs REGEXP '!!10490=10491!!' then @time_bleeding_symptoms_last := 1 -- Minutes
                  WHEN obs REGEXP '!!10490=10492!!' then @time_bleeding_symptoms_last := 2 -- Hours
                  WHEN obs REGEXP '!!10490=1072!!' then @time_bleeding_symptoms_last := 3 -- Days
                  ELSE @time_bleeding_symptoms_last := null
              END AS time_bleeding_symptoms_last,
              CASE 
                  WHEN obs REGEXP '!!8929=8927!!' then @frequency_of_occurrence_bleeding_symptoms := 1 -- Once
                  WHEN obs REGEXP '!!8929=1891!!' then @frequency_of_occurrence_bleeding_symptoms := 2 -- Once a day
                  WHEN obs REGEXP '!!8929=1099!!' then @frequency_of_occurrence_bleeding_symptoms := 3 -- Weekly
                  WHEN obs REGEXP '!!8929=8928!!' then @frequency_of_occurrence_bleeding_symptoms := 4 -- Several times per week
                  WHEN obs REGEXP '!!8929=1098!!' then @frequency_of_occurrence_bleeding_symptoms := 5 -- Monthly
                  ELSE @frequency_of_occurrence_bleeding_symptoms := null
              END AS frequency_of_occurrence_bleeding_symptoms,
              CASE 
                  WHEN obs REGEXP '!!8930=1066!!' then @blood_transfusion_required := 0 -- No
                  WHEN obs REGEXP '!!8930=1065!!' then @blood_transfusion_required := 1 -- Yes
                  ELSE @blood_transfusion_required := null
              END AS blood_transfusion_required,
              CASE 
                  WHEN obs REGEXP "!!8412=" then @units_of_blood_transfused := GetValues(obs, 8412)
                  ELSE @units_of_blood_transfused := null
              END AS units_of_blood_transfused,
              CASE 
                  WHEN obs REGEXP '!!8931=1066!!' then @patient_reported_symptoms_of_blood_clot_or_embolism := 0 -- No
                  WHEN obs REGEXP '!!8931=1065!!' then @patient_reported_symptoms_of_blood_clot_or_embolism := 1 -- Yes
                  WHEN obs REGEXP '!!8931=107!!' then @patient_reported_symptoms_of_blood_clot_or_embolism := 2 -- Cough
                  WHEN obs REGEXP '!!8931=136!!' then @patient_reported_symptoms_of_blood_clot_or_embolism := 3 -- Chest pain
                  ELSE @patient_reported_symptoms_of_blood_clot_or_embolism := null
              END AS patient_reported_symptoms_of_blood_clot_or_embolism,
              CASE 
                  WHEN obs REGEXP '!!1239=8940!!' then @presumed_clot_location := 1 -- Brain
                  WHEN obs REGEXP '!!1239=8942!!' then @presumed_clot_location := 1 -- Pulmonary / cardiac
                  WHEN obs REGEXP '!!1239=1237!!' then @presumed_clot_location := 1 -- Upper extremities
                  WHEN obs REGEXP '!!1239=1236!!' then @presumed_clot_location := 1 -- Lower extremities
                  ELSE @presumed_clot_location := null
              END AS presumed_clot_location,
              CASE
                  WHEN obs REGEXP '!!8944=2329!!' then @blood_clot_onset := 1 -- Acute
                  WHEN obs REGEXP '!!8944=2330!!' then @blood_clot_onset := 2 -- Chronic
                  ELSE @blood_clot_onset := null
              END as blood_clot_onset,
              CASE 
                  WHEN obs REGEXP '!!10490=10491!!' then @time_blood_clot_symptoms_last := 1 -- Minutes
                  WHEN obs REGEXP '!!10490=10492!!' then @time_blood_clot_symptoms_last := 2 -- Hours
                  WHEN obs REGEXP '!!10490=1072!!' then @time_blood_clot_symptoms_last := 3 -- Days
                  ELSE @time_blood_clot_symptoms_last := null
              END AS time_blood_clot_symptoms_last,
              CASE 
                  WHEN obs REGEXP '!!8927=1891!!' then @frequency_of_occurrence_blood_clot_symptoms := 1 -- Minutes
                  WHEN obs REGEXP '!!8927=1099!!' then @frequency_of_occurrence_blood_clot_symptoms := 2 -- Hours
                  WHEN obs REGEXP '!!8927=8929!!' then @frequency_of_occurrence_blood_clot_symptoms := 3 -- Days
                  ELSE @frequency_of_occurrence_blood_clot_symptoms := null
              END AS frequency_of_occurrence_blood_clot_symptoms,
              CASE 
                  WHEN obs REGEXP '!!5959=1072!!' then @cough_duration := 1 -- Days
                  WHEN obs REGEXP '!!5959=1073!!' then @cough_duration := 2 -- Weeks
                  WHEN obs REGEXP '!!5959=1074!!' then @cough_duration := 3 -- Months
                  ELSE @cough_duration := null
              END AS cough_duration, 
              CASE 
                  WHEN obs REGEXP '!!5971=1072!!' then @chest_pain_duration := 1 -- Days
                  WHEN obs REGEXP '!!5971=1073!!' then @chest_pain_duration := 2 -- Weeks
                  WHEN obs REGEXP '!!5971=1074!!' then @chest_pain_duration := 3 -- Months
                  ELSE @chest_pain_duration := null
              END AS chest_pain_duration, 
              CASE 
                  WHEN obs REGEXP '!!5976=540!!' then @chest_pain_location := 1 -- Anterior
                  WHEN obs REGEXP '!!5976=5139!!' then @chest_pain_location := 2 -- Left
                  WHEN obs REGEXP '!!5976=541!!' then @chest_pain_location := 3 -- Posterior
                  WHEN obs REGEXP '!!5976=5973!!' then @chest_pain_location := 4 -- Substernal
                  WHEN obs REGEXP '!!5976=5141!!' then @chest_pain_location := 5 -- Right
                  ELSE @chest_pain_location := null
              END AS chest_pain_location, 
              CASE
                  WHEN obs REGEXP '!!909=379!!' then @chest_pain_quality := 1 -- Burning sensation
                  WHEN obs REGEXP '!!909=5972!!' then @chest_pain_quality := 2 -- Pleuritic
                  WHEN obs REGEXP '!!909=705!!' then @chest_pain_quality := 3 -- Pressure sensation
                  WHEN obs REGEXP '!!909=762!!' then @chest_pain_quality := 4 -- Sharp sensation
                  ELSE @chest_pain_quality := null
              END AS chest_pain_quality, 
              CASE 
                  WHEN obs REGEXP '!!8823=1066!!' then @change_in_activity_level := 0 -- No
                  WHEN obs REGEXP '!!8823=6619!!' then @change_in_activity_level := 0 -- Increased activity level
                  WHEN obs REGEXP '!!8823=2413!!' then @change_in_activity_level := 0 -- Decreased activity level
                  ELSE @change_in_activity_level := null
              END AS change_in_activity_level, 
              CASE 
                  WHEN obs REGEXP '!!8947=1066!!' then @change_in_health_status := 0 -- No
                  WHEN obs REGEXP '!!8947=1065!!' then @change_in_health_status := 1 -- Yes
                  WHEN obs REGEXP '!!8947=6593!!' then @change_in_health_status := 2 -- Change of diagnosis
                  WHEN obs REGEXP '!!8947=7899!!' then @change_in_health_status := 3 -- Recovering
                  WHEN obs REGEXP '!!8947=5622!!' then @change_in_health_status := 4 -- Other
                  ELSE @change_in_health_status := null
              END AS change_in_health_status, 
              CASE
                  WHEN obs REGEXP '!!8948=1066!!' then @patient_scheduled_for_surgery := 0 -- No
                  WHEN obs REGEXP '!!8948=1065!!' then @patient_scheduled_for_surgery := 1 -- Yes
                  ELSE @patient_scheduled_for_surgery := null
              END AS patient_scheduled_for_surgery,
              CASE
                  WHEN obs REGEXP '!!2072=' then @patient_reported_problem_added := GetValues(obs, 2072)
                  ELSE @patient_reported_problem_added := null
              END AS patient_reported_problem_added,
              CASE 
                  WHEN obs REGEXP '!!1724=1965!!' THEN @patient_type := 1 -- Outpatient
                  WHEN obs REGEXP '!!1724=5485!!' THEN @patient_type := 2 -- Inpatient
                  ELSE @patient_type := null
              END AS patient_type,
              CASE 
                  WHEN obs REGEXP '!!8351=1065!!' THEN @pregnancy_status := 1 -- Yes
                  WHEN obs REGEXP '!!8351=1066!!' THEN @pregnancy_status := 2 -- No
                  WHEN obs REGEXP '!!8351=1624!!' THEN @pregnancy_status := 3 -- Don't know
                  WHEN obs REGEXP '!!8351=6791!!' THEN @pregnancy_status := 4 -- Possibly
                  ELSE @pregnancy_status := null
              END AS pregnancy_status,
              CASE 
                  WHEN obs REGEXP '!!6266=1107!!' THEN @insurance := 1 -- None
                  WHEN obs REGEXP '!!6266=8890!!' THEN @insurance := 2 -- Employer based insurance
                  WHEN obs REGEXP '!!6266=6815!!' THEN @insurance := 3 -- NHIF
                  WHEN obs REGEXP '!!6266=8891!!' THEN @insurance := 4 -- Private
                  WHEN obs REGEXP '!!6266=5622!!' THEN @insurance := 5 -- Other
                  ELSE @insurance := null
              END AS insurance,
              CASE 
                  WHEN obs REGEXP '!!8294=1066!!' THEN @health_assistance := 0 -- no
                  WHEN obs REGEXP '!!8294=1065!!' THEN @health_assistance := 1-- yes
                  ELSE @health_assistance := null
              END AS health_assistance,
              CASE 
                  WHEN obs REGEXP '!!6749=5485!!' THEN @referral_from := 1 -- Inpatient dept
                  WHEN obs REGEXP '!!6749=1965!!' THEN @referral_from := 2-- Outpatient dept
                  ELSE @referral_from := null
              END AS referral_from,
              CASE 
                  WHEN obs REGEXP '!!8849=8847!!' THEN @anticoag_duration_necessary := 1 -- 1 month
                  WHEN obs REGEXP '!!8849=2305!!' THEN @anticoag_duration_necessary := 2 -- 2 months
                  WHEN obs REGEXP '!!8849=2306!!' THEN @anticoag_duration_necessary := 3 -- 3 months
                  WHEN obs REGEXP '!!8849=8551!!' THEN @anticoag_duration_necessary := 4 -- 6 months
                  WHEN obs REGEXP '!!8849=8848!!' THEN @anticoag_duration_necessary := 5 -- 12 months
                  WHEN obs REGEXP '!!8849=8897!!' THEN @anticoag_duration_necessary := 6 -- Lifetime
                  WHEN obs REGEXP '!!8849=5622!!' THEN @anticoag_duration_necessary := 7 -- Other 
                  ELSE @anticoag_duration_necessary := null
              END AS anticoag_duration_necessary,
              CASE 
                  WHEN obs REGEXP '!!8851=1531!!' THEN @conditions_requiring_anticoag := 1 -- afib
                  WHEN obs REGEXP '!!8851=6647!!' THEN @conditions_requiring_anticoag := 2 -- DVT / PE / blood clots
                  WHEN obs REGEXP '!!8851=8040!!' THEN @conditions_requiring_anticoag := 3 -- HVD
                  WHEN obs REGEXP '!!8851=5622!!' THEN @conditions_requiring_anticoag := 4 -- Other
                  ELSE @conditions_requiring_anticoag := null
              END AS conditions_requiring_anticoag,
              CASE 
                  WHEN obs REGEXP '!!8852=221!!' THEN @afib_indications_for_anticoag := 1 -- rheumatic heart disease
                  WHEN obs REGEXP '!!8852=8076!!' THEN @afib_indications_for_anticoag := 2 -- transient cerebral ischemia
                  WHEN obs REGEXP '!!8852=175!!' THEN @afib_indications_for_anticoag := 3  -- DM
                  WHEN obs REGEXP '!!8852=903!!' THEN @afib_indications_for_anticoag := 4 -- hypertension
                  WHEN obs REGEXP '!!8852=1456!!' THEN @afib_indications_for_anticoag := 5 -- congestive cardiac failure 
                  WHEN obs REGEXP '!!8852=8856!!' THEN @afib_indications_for_anticoag := 6 -- age > 75
                  WHEN obs REGEXP '!!8852=8857!!' THEN @afib_indications_for_anticoag := 7 -- thyrotoxicosis
                  WHEN obs REGEXP '!!8852=8859!!' THEN @afib_indications_for_anticoag := 8 -- personal history of pulmonary embolism
                  WHEN obs REGEXP '!!8852=8858!!' THEN @afib_indications_for_anticoag := 9 -- left ventricular dysfunction
                  WHEN obs REGEXP '!!8852=8860!!' THEN @afib_indications_for_anticoag := 10 -- age 65-75   
                  WHEN obs REGEXP '!!8852=8861!!' THEN @afib_indications_for_anticoag := 11 -- CHD
                  WHEN obs REGEXP '!!8852=8862!!' THEN @afib_indications_for_anticoag := 12 -- cardioversion
                  WHEN obs REGEXP '!!8852=8866!!' THEN @afib_indications_for_anticoag := 13 -- artifical heart valve
                  ELSE @afib_indications_for_anticoag := null
              END AS afib_indications_for_anticoag,
              CASE
                 WHEN obs REGEXP '!!8854=6218!!' THEN @dvt_or_pe_indications_for_anticoag := 1 -- combined hormone oral contraceptive pill 
                 WHEN obs REGEXP '!!8854=1180!!' THEN @dvt_or_pe_indications_for_anticoag := 2 -- postpartum 
                 WHEN obs REGEXP '!!8854=44!!' THEN @dvt_or_pe_indications_for_anticoag := 3 -- pregnancy 
                 WHEN obs REGEXP '!!8854=6647!!' THEN @dvt_or_pe_indications_for_anticoag := 4 -- DVT / PE / blood clots
                 WHEN obs REGEXP '!!8854=1514!!' THEN @dvt_or_pe_indications_for_anticoag := 5 -- malignancy
                 WHEN obs REGEXP '!!8854=884!!' THEN @dvt_or_pe_indications_for_anticoag := 6 -- hiv
                 WHEN obs REGEXP '!!8854=8868!!' THEN @dvt_or_pe_indications_for_anticoag := 7 -- first idiopathic dvt
                 WHEN obs REGEXP '!!8854=8869!!' THEN @dvt_or_pe_indications_for_anticoag := 8 -- factor V leiden mutation
                 WHEN obs REGEXP '!!8854=8870!!' THEN @dvt_or_pe_indications_for_anticoag := 9 -- reversible or time limited risk factors
                 ELSE @dvt_or_pe_indications_for_anticoag := null
              END AS dvt_or_pe_indications_for_anticoag,
              CASE
                  WHEN obs REGEXP '!!8853=8924!!' THEN @hvd_indications_for_anticoag := 1 -- RHD with afib
                  WHEN obs REGEXP '!!8853=8925!!' THEN @hvd_indications_for_anticoag := 2 -- RHD with history of PE
                  WHEN obs REGEXP '!!8853=8863!!' THEN @hvd_indications_for_anticoag := 3 -- RHD with left atrial enlargement
                  WHEN obs REGEXP '!!8853=8864!!' THEN @hvd_indications_for_anticoag := 4 -- RHD with PH and right sided heart failure
                  WHEN obs REGEXP '!!8853=8865!!' THEN @hvd_indications_for_anticoag := 5 -- RHD with left atrial thrombus
                  WHEN obs REGEXP '!!8853=8866!!' THEN @hvd_indications_for_anticoag := 6 -- artificial heart valve
                  WHEN obs REGEXP '!!8853=8867!!' THEN @hvd_indications_for_anticoag := 7 -- RHD with atrial and mitral valve disease
                  ELSE @hvd_indications_for_anticoag := null
              END AS hvd_indications_for_anticoag,
              CASE
                  WHEN obs REGEXP '!!8855=8871!!' THEN @other_indications_for_anticoag := 1 -- Cardioembolytic stroke
                  WHEN obs REGEXP '!!8855=1541!!' THEN @other_indications_for_anticoag := 2 -- PH
                  WHEN obs REGEXP '!!8855=7996!!' THEN @other_indications_for_anticoag := 3 -- Peripheral vascular disease
                  WHEN obs REGEXP '!!8855=8590!!' THEN @other_indications_for_anticoag := 4 -- PE
                  WHEN obs REGEXP '!!8855=8872!!' THEN @other_indications_for_anticoag := 5 -- Sagittal sinus thrombosis
                  WHEN obs REGEXP '!!8855=1538!!' THEN @other_indications_for_anticoag := 6 -- Dilated cardiomyopathy
                  WHEN obs REGEXP '!!8855=1540!!' THEN @other_indications_for_anticoag := 7 -- Mural thrombi
                  WHEN obs REGEXP '!!8855=5622!!' THEN @other_indications_for_anticoag := 8 -- Other non-coded
                  ELSE @other_indications_for_anticoag := null
              END AS other_indications_for_anticoag,
              CASE 
                  WHEN obs REGEXP '!!5605=1049!!' THEN @travel_to_clinic_time := 1 -- < 30 min
                  WHEN obs REGEXP '!!5605=1050!!' THEN @travel_to_clinic_time := 2 -- 30-60 min
                  WHEN obs REGEXP '!!5605=1051!!' THEN @travel_to_clinic_time := 3 -- 1-2 hrs
                  WHEN obs REGEXP '!!5605=1052!!' THEN @travel_to_clinic_time := 4 -- > 2 hrs
                  WHEN obs REGEXP '!!5605=6412!!' THEN @travel_to_clinic_time := 5 -- 2-4 hrs
                  WHEN obs REGEXP '!!5605=6413!!' THEN @travel_to_clinic_time := 6 -- 4-8 hrs
                  WHEN obs REGEXP '!!5605=6414!!' THEN @travel_to_clinic_time := 7 -- > 8 hrs
                  WHEN obs REGEXP '!!5605=8245!!' THEN @travel_to_clinic_time := 8 -- > 1 hr
                  WHEN obs REGEXP '!!5605=8246!!' THEN @travel_to_clinic_time := 9 -- < 1 hr
                  WHEN obs REGEXP '!!5605=8884!!' THEN @travel_to_clinic_time := 10 -- 2-3 hrs 
                  WHEN obs REGEXP '!!5605=8885!!' THEN @travel_to_clinic_time := 11 -- 3-4 hrs
                  WHEN obs REGEXP '!!5605=8886!!' THEN @travel_to_clinic_time := 12 -- > 4 hrs
                  WHEN obs REGEXP '!!5605=1958!!' THEN @travel_to_clinic_time := 13 -- Refusal
                  ELSE @travel_to_clinic_time := null
              END AS travel_to_clinic_time,
              CASE
                  WHEN obs REGEXP '!!6468=6395!!' THEN @primary_transportation_mode := 1 -- Bicycle
                  WHEN obs REGEXP '!!6468=7552!!' THEN @primary_transportation_mode := 2 -- Boat 
                  WHEN obs REGEXP '!!6468=6580!!' THEN @primary_transportation_mode := 3 -- Boda boda
                  WHEN obs REGEXP '!!6468=6416!!' THEN @primary_transportation_mode := 4 -- Matatu
                  WHEN obs REGEXP '!!6468=6396!!' THEN @primary_transportation_mode := 5 -- Motorcycle
                  WHEN obs REGEXP '!!6468=6471!!' THEN @primary_transportation_mode := 6 -- Private car
                  WHEN obs REGEXP '!!6468=6470!!' THEN @primary_transportation_mode := 7 -- Taxi
                  WHEN obs REGEXP '!!6468=6415!!' THEN @primary_transportation_mode := 8 -- Walking
                  ELSE @primary_transportation_mode := null
              END AS primary_transportation_mode,

              CASE
                  WHEN obs REGEXP "!!8887=" THEN @year_diagnosed := GetValues(obs, 8887)
                  ELSE @year_diagnosed := null
              END AS year_diagnosed,
              CASE
                  WHEN obs REGEXP '!!8888=8923!!' THEN @first_anticoag_diagnosis_location := 1 -- Community screen
                  WHEN obs REGEXP '!!8888=6181!!' THEN @first_anticoag_diagnosis_location := 2 -- Hospital / health center
                  WHEN obs REGEXP '!!8888=1965!!' THEN @first_anticoag_diagnosis_location := 3 -- Outpatient clinic
                  ELSE @first_anticoag_diagnosis_location := null
              END AS first_anticoag_diagnosis_location,
              CASE
                  WHEN obs REGEXP '!!2073=1066!!' THEN @has_other_medical_condition := 0 -- no
                  WHEN obs REGEXP '!!2073=1065!!' THEN @has_other_medical_condition := 1 -- yes
                  WHEN obs REGEXP '!!2073=1624!!' THEN @has_other_medical_condition := 2 -- don't know
                  ELSE @has_other_medical_condition := null
              END AS has_other_medical_condition,
              CASE
                  WHEN obs REGEXP '!!6245=3!!' THEN @review_of_medical_history := 1 -- Anemia
                  WHEN obs REGEXP '!!6245=903!!' THEN @review_of_medical_history := 2 -- Hypertension
                  WHEN obs REGEXP '!!6245=5622!!' THEN @review_of_medical_history := 3 -- Other
                  ELSE @review_of_medical_history := null
              END AS review_of_medical_history,
              CASE
                  WHEN obs REGEXP "!!1915=" THEN @other_medical_condition := GetValues(obs, 1915)
                  ELSE @other_medical_condition := null
              END AS other_medical_condition,
              CASE
                  WHEN obs REGEXP '!!6878=1066!!' THEN @hiv_status_known := 0 -- No
                  WHEN obs REGEXP '!!6878=1065!!' THEN @hiv_status_known := 1 -- Yes
                  ELSE @hiv_status_known := null
              END AS hiv_status_known,
              CASE
                  WHEN obs REGEXP '!!1362=1138!!' THEN @last_hiv_test_result := 1 -- Indeterminate
                  WHEN obs REGEXP '!!1362=664!!' THEN @last_hiv_test_result := 2 -- Negative
                  WHEN obs REGEXP '!!1362=703!!' THEN @last_hiv_test_result := 3 -- Positive
                  ELSE @last_hiv_test_result := null
              END AS last_hiv_test_result,
              CASE
                  WHEN obs REGEXP "!!2388=" THEN @last_hiv_test_date := GetValues(obs, 2388)
                  ELSE @last_hiv_test_date := null
              END AS last_hiv_test_date,
              CASE
                  WHEN obs REGEXP '!!6152=1066!!' THEN @hiv_positive_and_enrolled_in_ampath := 0 -- No
                  WHEN obs REGEXP '!!6152=1065!!' THEN @hiv_positive_and_enrolled_in_ampath := 1 -- Yes
                  ELSE @hiv_positive_and_enrolled_in_ampath := null
              END AS hiv_positive_and_enrolled_in_ampath,
              CASE
                  WHEN obs REGEXP '!!7111=1066!!' THEN @has_medication_allergies := 0 -- No
                  WHEN obs REGEXP '!!7111=1065!!' THEN @has_medication_allergies := 1 -- Yes
                  ELSE @has_medication_allergies := null
              END AS has_medication_allergies,
              CASE
                  WHEN obs REGEXP "!!2089=" THEN @medication_allergic_to := GetValues(obs, 2089)
                  ELSE @medication_allergic_to := null
              END AS medication_allergic_to,
              CASE
                  WHEN obs REGEXP '!!2085=7954!!' THEN @allergic_reaction := 1 -- Angioedema
                  WHEN obs REGEXP '!!2085=107!!' THEN @allergic_reaction := 2 -- Cough
                  WHEN obs REGEXP '!!2085=879!!' THEN @allergic_reaction := 3 -- Itching
                  WHEN obs REGEXP '!!2085=512!!' THEN @allergic_reaction := 4 -- Rash
                  WHEN obs REGEXP '!!2085=6001!!' THEN @allergic_reaction := 5 -- Swelling
                  WHEN obs REGEXP '!!2085=5980!!' THEN @allergic_reaction := 6 -- Vomiting
                  WHEN obs REGEXP '!!2085=5209!!' THEN @allergic_reaction := 7 -- Wheeze
                  WHEN obs REGEXP '!!2085=5622!!' THEN @allergic_reaction := 8 -- Other
                  ELSE @allergic_reaction := null
              END AS allergic_reaction,
              CASE
                  WHEN obs REGEXP '!!2065=1066!!' THEN @smokes_cigarretes := 0 -- No
                  WHEN obs REGEXP '!!2065=1065!!' THEN @smokes_cigarretes := 1 -- Yes
                  WHEN obs REGEXP '!!2065=1679!!' THEN @smokes_cigarretes := 2 -- Stopped
                  ELSE @smokes_cigarretes := null
              END AS smokes_cigarretes,
              CASE
                  WHEN obs REGEXP "!!2069=" THEN @sticks_smoked_per_day := GetValues(obs, 2069)
                  ELSE @sticks_smoked_per_day := null
              END AS sticks_smoked_per_day,
              CASE
                  WHEN obs REGEXP "!!2070=" THEN @years_of_cig_use := GetValues(obs, 2070)
                  ELSE @years_of_cig_use := null
              END AS years_of_cig_use,
              CASE
                  WHEN obs REGEXP "!!6581=" THEN @period_since_stopping_smoking := GetValues(obs, 6581)
                  ELSE @period_since_stopping_smoking := null
              END AS period_since_stopping_smoking,
              CASE
                  WHEN obs REGEXP "!!2068=" THEN @years_since_last_cig_use := GetValues(obs, 2068)
                  ELSE @years_since_last_cig_use := null
              END AS years_since_last_cig_use,
              CASE
                  WHEN obs REGEXP '!!7973=1066!!' THEN @uses_tobacco := 0 -- No
                  WHEN obs REGEXP '!!7973=1065!!' THEN @uses_tobacco := 1 -- Yes
                  WHEN obs REGEXP '!!7973=1679!!' THEN @uses_tobacco := 2 -- Stopped
                  ELSE @uses_tobacco := null
              END AS uses_tobacco,
              CASE
                  WHEN obs REGEXP "!!8144=" THEN @years_of_tobacco_use := GetValues(obs, 8144)
                  ELSE @years_of_tobacco_use := null
              END AS years_of_tobacco_use,
              CASE
                  WHEN obs REGEXP '!!1684=1066!!' THEN @uses_alcohol := 0 -- No
                  WHEN obs REGEXP '!!1684=1065!!' THEN @uses_alcohol := 1 -- Yes
                  WHEN obs REGEXP '!!1684=1679!!' THEN @uses_alcohol := 2 -- Stopped
                  ELSE @uses_alcohol := null
              END AS uses_alcohol,
              CASE
                  WHEN obs REGEXP '!!1685=1680!!' THEN @type_of_alcohol_used := 1 -- Beer
                  WHEN obs REGEXP '!!1685=1683!!' THEN @type_of_alcohol_used := 2 -- Busaa 
                  WHEN obs REGEXP '!!1685=1682!!' THEN @type_of_alcohol_used := 3 -- Chang'aa
                  WHEN obs REGEXP '!!1685=1681!!' THEN @type_of_alcohol_used := 4 -- Liquor
                  WHEN obs REGEXP '!!1685=2059!!' THEN @type_of_alcohol_used := 5 -- Wine
                  ELSE @type_of_alcohol_used := null
              END AS type_of_alcohol_used,
              CASE
                  WHEN obs REGEXP '!!8828=1066!!' THEN @used_herbal_medicine := 0 -- No
                  WHEN obs REGEXP '!!8828=1065!!' THEN @used_herbal_medicine := 1 -- Yes
                  ELSE @used_herbal_medicine := null
              END AS used_herbal_medicine,
              CASE
                  WHEN obs REGEXP "!!8889=" THEN @herbal_medication_used := GetValues(obs, 8889)
                  ELSE @herbal_medication_used := null
              END AS herbal_medication_used,
              CASE
                  WHEN obs REGEXP '!!8892=1107!!' THEN @adherence_prior_to_visit := 1 -- None
                  WHEN obs REGEXP '!!8892=1160!!' THEN @adherence_prior_to_visit := 2 -- Few
                  WHEN obs REGEXP '!!8892=1161!!' THEN @adherence_prior_to_visit := 3 -- Half
                  WHEN obs REGEXP '!!8892=1162!!' THEN @adherence_prior_to_visit := 4 -- Most
                  WHEN obs REGEXP '!!8892=1163!!' THEN @adherence_prior_to_visit := 5 -- All
                  ELSE @adherence_prior_to_visit := null
              END AS adherence_prior_to_visit,
              CASE
                  WHEN obs REGEXP "!!1895=" THEN @medication_added := GetValues(obs, 1895)
                  ELSE @medication_added := null
              END AS medication_added,
              CASE
                  WHEN obs REGEXP '!!1896=1891!!' THEN @medication_frequency := 1 -- OD
                  WHEN obs REGEXP '!!1896=1888!!' THEN @medication_frequency := 2 -- BD
                  WHEN obs REGEXP '!!1896=1889!!' THEN @medication_frequency := 3 -- TDS
                  WHEN obs REGEXP '!!1896=7772!!' THEN @medication_frequency := 4 -- PRN
                  WHEN obs REGEXP '!!1896=1890!!' THEN @medication_frequency := 5 -- QID
                  ELSE @medication_frequency := null
              END AS medication_frequency,
              CASE
                  WHEN obs REGEXP '!!8896=107!!' THEN @patient_reported_medication_side_effects := 1 -- Cough
                  WHEN obs REGEXP '!!8896=8895!!' THEN @patient_reported_medication_side_effects := 2 -- Bradycardia
                  WHEN obs REGEXP '!!8896=5987!!' THEN @patient_reported_medication_side_effects := 3 -- Nausea
                  WHEN obs REGEXP '!!8896=5980!!' THEN @patient_reported_medication_side_effects := 4 -- Vomiting
                  WHEN obs REGEXP '!!8896=16!!' THEN @patient_reported_medication_side_effects := 5 -- Diarrhea
                  WHEN obs REGEXP '!!8896=1486!!' THEN @patient_reported_medication_side_effects := 6 -- Hypotension
                  WHEN obs REGEXP '!!8896=8607!!' THEN @patient_reported_medication_side_effects := 7 -- Hepatotoxicity
                  WHEN obs REGEXP '!!8896=3!!' THEN @patient_reported_medication_side_effects := 8 -- Anaemia
                  WHEN obs REGEXP '!!8896=512!!' THEN @patient_reported_medication_side_effects := 9 -- Rash 
                  WHEN obs REGEXP '!!8896=5622!!' THEN @patient_reported_medication_side_effects := 10 -- Other
                  ELSE @patient_reported_medication_side_effects := null
              END AS patient_reported_medication_side_effects,
              CASE
                  WHEN obs REGEXP '!!8952=1066!!' THEN @change_in_meds_since_last_visit := 0 -- No
                  WHEN obs REGEXP '!!8952=1065!!' THEN @change_in_meds_since_last_visit := 1 -- Yes
                  ELSE @change_in_meds_since_last_visit := null
              END AS change_in_meds_since_last_visit,
              CASE
                  WHEN obs REGEXP "!!1895=" THEN @medication_changed_since_last_visit := GetValues(obs, 1895)
                  ELSE @medication_changed_since_last_visit := null
              END AS medication_changed_since_last_visit,
              CASE
                  WHEN obs REGEXP "!!1915=" THEN @other_changed_meds := GetValues(obs, 1915)
                  ELSE @other_changed_meds := null
              END AS other_changed_meds,
              CASE
                  WHEN obs REGEXP "!!1895=!!" THEN @past_anticoag_drug_used := GetValues(obs, 1895)
                  ELSE @past_anticoag_drug_used := null
              END AS past_anticoag_drug_used,
              CASE
                  WHEN obs REGEXP "!!7885=1260" THEN @drug_action_plan := 1 -- Stop Warfarin
                  WHEN obs REGEXP "!!7885=8957" THEN @drug_action_plan := 2 -- Hold one dose
                  WHEN obs REGEXP "!!7885=8958" THEN @drug_action_plan := 3 -- Hold two doses
                  WHEN obs REGEXP "!!7885=8959" THEN @drug_action_plan := 4 -- Hold three doses
                  WHEN obs REGEXP "!!7885=8960" THEN @drug_action_plan := 5 -- Decrease weekly dose by 5-10% 
                  WHEN obs REGEXP "!!7885=8961" THEN @drug_action_plan := 6 -- Decrease weekly dose by 10-20% 
                  WHEN obs REGEXP "!!7885=8962" THEN @drug_action_plan := 7 -- Decrease weekly dose by 15-20% 
                  WHEN obs REGEXP "!!7885=8956" THEN @drug_action_plan := 8 -- Add oral Vitamin K 1-2.5mg SC/PO
                  WHEN obs REGEXP "!!7885=8955" THEN @drug_action_plan := 9 -- Add oral Vitamin K 3-5mg SC/PO one dose
                  WHEN obs REGEXP "!!7885=8974" THEN @drug_action_plan := 10 -- Extra one time dose
                  WHEN obs REGEXP "!!7885=8972" THEN @drug_action_plan := 11 -- Increase weekly dose by 5-10%
                  WHEN obs REGEXP "!!7885=8973" THEN @drug_action_plan := 12 -- Increase weekly dose by 10-15%
                  WHEN obs REGEXP "!!7885=5622" THEN @drug_action_plan := 13 -- Increase weekly dose by 10-20%
                  ELSE @drug_action_plan := null
              END AS drug_action_plan,
              CASE
                  WHEN obs REGEXP "!!1899=" THEN @medication_plan_no_of_milligrams := GetValues(obs, 1899)
                  ELSE @medication_plan_no_of_milligrams := null
              END AS medication_plan_no_of_milligrams,
              CASE
                  WHEN obs REGEXP '!!8963=8963!!' THEN @medication_plan_day_of_the_week := 1 -- Monday
                  WHEN obs REGEXP '!!8963=8964!!' THEN @medication_plan_day_of_the_week := 2 -- Tuesday
                  WHEN obs REGEXP '!!8963=8965!!' THEN @medication_plan_day_of_the_week := 3 -- Wednesday
                  WHEN obs REGEXP '!!8963=8966!!' THEN @medication_plan_day_of_the_week := 4 -- Thursday
                  WHEN obs REGEXP '!!8963=8968!!' THEN @medication_plan_day_of_the_week := 5 -- Friday
                  WHEN obs REGEXP '!!8963=8969!!' THEN @medication_plan_day_of_the_week := 6 -- Saturday
                  WHEN obs REGEXP '!!8963=8970!!' THEN @medication_plan_day_of_the_week := 7 -- Sunday
                  ELSE @medication_plan_day_of_the_week := null
              END AS medication_plan_day_of_the_week,
              CASE
                  WHEN obs REGEXP '!!8976=1163!!' THEN @anticoag_adherence_last_seven_days := 1 -- All
                  WHEN obs REGEXP '!!8976=1162!!' THEN @anticoag_adherence_last_seven_days := 2 -- Most
                  WHEN obs REGEXP '!!8976=1160!!' THEN @anticoag_adherence_last_seven_days := 3 -- Few
                  WHEN obs REGEXP '!!8976=1161!!' THEN @anticoag_adherence_last_seven_days := 4 -- Half
                  WHEN obs REGEXP '!!8976=1107!!' THEN @anticoag_adherence_last_seven_days := 5 -- None
                  ELSE @anticoag_adherence_last_seven_days := null
              END AS anticoag_adherence_last_seven_days,
              CASE
                  WHEN obs REGEXP '!!9006=1066!!' THEN @anticoag_nonadherence := 0 -- No
                  WHEN obs REGEXP '!!9006=1065!!' THEN @anticoag_nonadherence := 1 -- Yes
                  ELSE @anticoag_nonadherence := null
              END AS anticoag_nonadherence,
              CASE
                  WHEN obs REGEXP '!!1668=1648!!' THEN @nonadherence_missed_medication_reason := 1 -- Forgot
                  WHEN obs REGEXP '!!1668=6295!!' THEN @nonadherence_missed_medication_reason := 2 -- Financial barrier
                  WHEN obs REGEXP '!!1668=8978!!' THEN @nonadherence_missed_medication_reason := 3 -- Wrong dose
                  WHEN obs REGEXP '!!1668=1664!!' THEN @nonadherence_missed_medication_reason := 4 -- ADR
                  WHEN obs REGEXP '!!1668=5622!!' THEN @nonadherence_missed_medication_reason := 5 -- Other
                  ELSE @nonadherence_missed_medication_reason := null
              END AS nonadherence_missed_medication_reason,
              CASE
                  WHEN obs REGEXP "!!1915=" THEN @additional_nonadherence_comments := GetValues(obs, 1915)
                  ELSE @additional_nonadherence_comments := null
              END AS additional_nonadherence_comments,
              CASE
                  WHEN obs REGEXP "!!1895=" THEN @other_drug_dispensed := GetValues(obs, 1895)
                  ELSE @other_drug_dispensed := null
              END AS other_drug_dispensed,
              CASE 
                  WHEN obs REGEXP "!!1898=" THEN @number_of_tablets_other_drug := GetValues(obs, 1898)
                  ELSE @number_of_tablets_other_drug := null 
              END AS number_of_tablets_other_drug,
              CASE 
                  WHEN obs REGEXP '!!8982=1066!!' THEN @was_protocol_followed := 0 -- No
                  WHEN obs REGEXP '!!8982=1065!!' THEN @was_protocol_followed := 1 -- Yes
                  ELSE @was_protocol_followed := null 
              END AS was_protocol_followed,
              CASE 
                  WHEN obs REGEXP "!!1915=" THEN @reason_protocol_not_followed := GetValues(obs, 1915)
                  ELSE @reason_protocol_not_followed := null 
              END AS reason_protocol_not_followed,
              CASE 
                  WHEN obs REGEXP "!!9007=" THEN @injectable_anticoag_stop_date := GetValues(obs, 9007)
                  ELSE @injectable_anticoag_stop_date := null 
              END AS injectable_anticoag_stop_date,
              CASE 
                  WHEN obs REGEXP '!!1271=8991!!' THEN @test_ordered := 1 -- Cardiac enzymes
                  WHEN obs REGEXP '!!1271=21!!' THEN @test_ordered := 2 -- HGB
                  WHEN obs REGEXP '!!1271=1010!!' THEN @test_ordered := 3 -- Full lipid panel
                  WHEN obs REGEXP '!!1271=7872!!' THEN @test_ordered := 4 -- Thyroid fn tests
                  WHEN obs REGEXP '!!1271=1019!!' THEN @test_ordered := 5 -- CBC
                  WHEN obs REGEXP '!!1271=790!!' THEN @test_ordered := 6 -- Creatinine
                  WHEN obs REGEXP '!!1271=953!!' THEN @test_ordered := 7 -- Liver fn test
                  WHEN obs REGEXP '!!1271=6898!!' THEN @test_ordered := 8 -- Renal fn blood test
                  WHEN obs REGEXP '!!1271=9009!!' THEN @test_ordered := 9 -- HbE
                  WHEN obs REGEXP '!!1271=1327!!' THEN @test_ordered := 10 -- Reticulocyte count
                  WHEN obs REGEXP '!!1271=10205!!' THEN @test_ordered := 11 -- Serum free light chain 
                  WHEN obs REGEXP '!!1271=8595!!' THEN @test_ordered := 12 -- SPEP
                  WHEN obs REGEXP '!!1271=8596!!' THEN @test_ordered := 13 -- UPEP
                  WHEN obs REGEXP '!!1271=10125!!' THEN @test_ordered := 14 -- Immunohistochemistry
                  WHEN obs REGEXP '!!1271=10123!!' THEN @test_ordered := 15 -- PET Scan
                  WHEN obs REGEXP '!!1271=846!!' THEN @test_ordered := 16 -- CT Head
                  WHEN obs REGEXP '!!1271=9839!!' THEN @test_ordered := 17 -- CT Neck
                  WHEN obs REGEXP '!!1271=7113!!' THEN @test_ordered := 18 -- CT Chest
                  WHEN obs REGEXP '!!1271=7114!!' THEN @test_ordered := 19 -- CT Abdomen
                  WHEN obs REGEXP '!!1271=9840!!' THEN @test_ordered := 20 -- CT Spine 
                  WHEN obs REGEXP '!!1271=1536!!' THEN @test_ordered := 21 -- Echo
                  WHEN obs REGEXP '!!1271=9881!!' THEN @test_ordered := 22 -- MRI Head
                  WHEN obs REGEXP '!!1271=9883!!' THEN @test_ordered := 23 -- MRT Neck
                  WHEN obs REGEXP '!!1271=9951!!' THEN @test_ordered := 24 -- MRT Chest
                  WHEN obs REGEXP '!!1271=9884!!' THEN @test_ordered := 25 -- MRT Arms
                  WHEN obs REGEXP '!!1271=9952!!' THEN @test_ordered := 26 -- MRT Abdomen
                  WHEN obs REGEXP '!!1271=9885!!' THEN @test_ordered := 27 -- MRT Pelvic
                  WHEN obs REGEXP '!!1271=9953!!' THEN @test_ordered := 28 -- MRT Spine
                  WHEN obs REGEXP '!!1271=845!!' THEN @test_ordered := 29 -- MRT Legs
                  WHEN obs REGEXP '!!1271=9596!!' THEN @test_ordered := 30 -- Abdominal ultrasound
                  WHEN obs REGEXP '!!1271=6221!!' THEN @test_ordered := 31 -- Breast ultrasound
                  WHEN obs REGEXP '!!1271=7115!!' THEN @test_ordered := 32 -- Obstretic ultrasound
                  WHEN obs REGEXP '!!1271=852!!' THEN @test_ordered := 33 -- Ultrasound renal
                  WHEN obs REGEXP '!!1271=394!!' THEN @test_ordered := 34 -- Ultrasound hepatic
                  WHEN obs REGEXP '!!1271=392!!' THEN @test_ordered := 35 -- X-ray shoulder
                  WHEN obs REGEXP '!!1271=101!!' THEN @test_ordered := 36 -- X-ray pelvis
                  WHEN obs REGEXP '!!1271=101!!' THEN @test_ordered := 37 -- X-ray abdomen
                  WHEN obs REGEXP '!!1271=386!!' THEN @test_ordered := 38 -- X-ray leg
                  WHEN obs REGEXP '!!1271=380!!' THEN @test_ordered := 39 -- X-ray hand
                  WHEN obs REGEXP '!!1271=382!!' THEN @test_ordered := 40 -- X-ray foot
                  WHEN obs REGEXP '!!1271=12!!' THEN @test_ordered := 41 -- X-ray chest
                  WHEN obs REGEXP '!!1271=377!!' THEN @test_ordered := 42 -- X-ray arm
                  WHEN obs REGEXP '!!1271=390!!' THEN @test_ordered := 43 -- X-ray spine
                  ELSE @test_ordered := null
              END AS test_ordered,
              CASE
                  WHEN obs REGEXP "!!8190=" THEN @radiology_test_ordered := GetValues(obs, 8190)
                  ELSE @radiology_test_ordered := null
              END AS radiology_test_ordered,
              CASE 
                  WHEN obs REGEXP "!!9538=" THEN @other_lab_orders := GetValues(obs, 9538)
                  ELSE @other_lab_orders := null
              END AS other_lab_orders,
              CASE 
                  WHEN obs REGEXP '!!1272=1107!!' THEN @referrals_ordered := 1 -- None
                  WHEN obs REGEXP '!!1272=5485!!' THEN @referrals_ordered := 2 -- Admit to hospital
                  WHEN obs REGEXP '!!1272=5507!!' THEN @referrals_ordered := 3 -- Consultant
                  WHEN obs REGEXP '!!1272=5622!!' THEN @referrals_ordered := 4 -- Other
                  ELSE @referrals_ordered := null
              END AS referrals_ordered,
              CASE 
                  WHEN obs REGEXP '!!8990=8989!!' THEN @referral_urgency_level := 1 -- Immediate 
                  WHEN obs REGEXP '!!8990=8987!!' THEN @referral_urgency_level := 2 -- Within one week
                  WHEN obs REGEXP '!!8990=8847!!' THEN @referral_urgency_level := 3 -- Within one month
                  WHEN obs REGEXP '!!8990=7316!!' THEN @referral_urgency_level := 4 -- Not urgent
                  ELSE @referral_urgency_level := null
              END AS referral_urgency_level,
              CASE
                  WHEN obs REGEXP "!!1915=" THEN @other_referrals := GetValues(obs, 1915)
                  ELSE @other_referrals := null
              END AS other_referrals,
              CASE 
                  WHEN obs REGEXP "!!7222=" THEN @assessment_notes := GetValues(obs, 7222)
                  ELSE @assessment_notes := null
              END AS assessment_notes,
              CASE
                  WHEN obs REGEXP "!!5096=" THEN @rtc_date := GetValues(obs, 5096)
                  ELSE @rtc_date := null
              END AS rtc_date
          FROM 
              flat_anticoagulation_treatment_0 t1
		      LEFT JOIN
		          amrs.location `l` ON l.location_id = t1.location_id
			    JOIN amrs.person p using (person_id)
   	      ORDER BY person_id, date(encounter_datetime) DESC
  	    );

          SET @prev_id = null;
          SET @cur_id = null;
						
          ALTER TABLE flat_anticoagulation_treatment_1 DROP prev_id, DROP cur_id;

	        SELECT 
              COUNT(*)
          INTO 
              @new_encounter_rows
          FROM
              flat_anticoagulation_treatment_1;
                              
          SELECT @new_encounter_rows;
          SET @total_rows_written := @total_rows_written + @new_encounter_rows;
          SELECT @total_rows_written;

          SET @dyn_sql := CONCAT('REPLACE INTO ', @write_table, 										  
              '(SELECT
                    null,
                    person_id,
                    encounter_id,
                    encounter_type,
                    encounter_datetime,
                    location_id,
					          location_name,
                    age,
                    gender,
                    death_date,
                    cur_visit_type,
                    temperature,
                    weight,
                    height,
                    systolic_bp,
                    diastolic_bp,
                    pulse,
                    respiratory_rate,
                    rcc,
                    lcc,
                    inr,
                    chief_complaint,
                    hospitalized_since_last_visit,
                    patient_reported_signs_of_bleeding,
                    blood_in_stool,
                    blood_in_urine,
                    bruising,
                    gum_bleeds,
                    epistaxis,
                    secondary_hemorrhage,
                    severity,
                    time_bleeding_symptoms_last,
                    frequency_of_occurrence_bleeding_symptoms,
                    blood_transfusion_required,
                    units_of_blood_transfused,
                    patient_reported_symptoms_of_blood_clot_or_embolism,
                    presumed_clot_location,
                    blood_clot_onset,
                    time_blood_clot_symptoms_last,
                    frequency_of_occurrence_blood_clot_symptoms,
                    cough_duration,
                    chest_pain_duration,
                    chest_pain_location,
                    chest_pain_quality,
                    change_in_activity_level,
                    change_in_health_status,
                    patient_scheduled_for_surgery,
                    patient_reported_problem_added,
                    patient_type,
                    pregnancy_status,
                    insurance,
                    health_assistance,
                    referral_from,
                    anticoag_duration_necessary,
                    conditions_requiring_anticoag,
                    afib_indications_for_anticoag,
                    dvt_or_pe_indications_for_anticoag,
                    hvd_indications_for_anticoag,
                    other_indications_for_anticoag,
                    travel_to_clinic_time,
                    primary_transportation_mode,
                    year_diagnosed,
                    first_anticoag_diagnosis_location,
                    has_other_medical_condition,
                    review_of_medical_history,
                    other_medical_condition,
                    hiv_status_known,
                    last_hiv_test_result,
                    last_hiv_test_date,
                    hiv_positive_and_enrolled_in_ampath,
                    has_medication_allergies,
                    medication_allergic_to,
                    allergic_reaction,
                    smokes_cigarretes,
                    sticks_smoked_per_day,
                    years_of_cig_use,
                    period_since_stopping_smoking,
                    years_since_last_cig_use,
                    uses_tobacco,
                    years_of_tobacco_use,
                    uses_alcohol,
                    type_of_alcohol_used,
                    used_herbal_medicine,
                    herbal_medication_used,
                    adherence_prior_to_visit,
                    medication_added,
                    medication_frequency,
                    patient_reported_medication_side_effects,
                    change_in_meds_since_last_visit,
                    medication_changed_since_last_visit,
                    other_changed_meds,
                    past_anticoag_drug_used,
                    drug_action_plan,
                    medication_plan_no_of_milligrams,
                    medication_plan_day_of_the_week,
                    anticoag_adherence_last_seven_days,
                    anticoag_nonadherence,
                    nonadherence_missed_medication_reason,
                    additional_nonadherence_comments,
                    other_drug_dispensed,
                    number_of_tablets_other_drug,
                    was_protocol_followed,
                    reason_protocol_not_followed,
                    injectable_anticoag_stop_date,
                    test_ordered,
                    radiology_test_ordered,
                    other_lab_orders,
                    referrals_ordered,
                    referral_urgency_level,
                    other_referrals,
                    assessment_notes
                FROM 
                    flat_anticoagulation_treatment_1 t1
                        JOIN 
                    amrs.location t2 USING (location_id))'
          );

          PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
          DEALLOCATE PREPARE s1;


          SET @dyn_sql=CONCAT('DELETE t1 from ',@queue_table,' t1 JOIN flat_anticoagulation_treatment_build_queue__0 t2 USING (person_id);'); 
          PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
          DEALLOCATE PREPARE s1;  
                                
          SET @dyn_sql=CONCAT('SELECT COUNT(*) INTO @person_ids_count FROM ',@queue_table,';'); 
          PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
          DEALLOCATE PREPARE s1;  
                    
          SET @cycle_length = TIMESTAMPDIFF(second,@loop_start_time,NOW());
          SET @total_time = @total_time + @cycle_length;
          SET @cycle_number = @cycle_number + 1;
          
          SET @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);

          SELECT 
            @person_ids_count AS 'persons remaining',
            @cycle_length AS 'Cycle time (s)',
            CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
            @remaining_time AS 'Est time remaining (min)';
    END WHILE;

    IF (@query_type = 'build') THEN
        SET @dyn_sql := CONCAT('drop table ', @queue_table, ';'); 
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
                        
        SET @total_rows_to_write := 0;
        SET @dyn_sql := CONCAT('SELECT COUNT(*) INTO @total_rows_to_write FROM ', @write_table);
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
        
        SET @dyn_sql := CONCAT('DROP TABLE ', @write_table, ';'); 
        PREPARE s1 FROM @dyn_sql;
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;      
    END IF;
						
    SET @ave_cycle_length := CEIL(@total_time / @cycle_number);
    SELECT CONCAT('Average Cycle Length: ', @ave_cycle_length, ' second(s)');
            
    SET @end := NOW();
    INSERT INTO etl.flat_log VALUES (@start, @last_date_created, @table_version, TIMESTAMPDIFF(SECOND, @start, @end));
    SELECT CONCAT(@table_version, ': Time to complete: ', TIMESTAMPDIFF(MINUTE, @start, @end), ' minutes');
END$$
DELIMITER ;
