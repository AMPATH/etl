DELIMITER $$
CREATE PROCEDURE `generate_flat_breast_cancer_screening_v1_2`(IN query_type VARCHAR(50), IN queue_number INT, IN queue_size INT, IN cycle_size INT)
BEGIN
    SET @primary_table := "flat_breast_cancer_screening";
    SET @query_type := query_type;
                     
    SET @total_rows_written := 0;
                    
    SET @encounter_types = "(86, 145, 146, 160)";
    SET @clinical_encounter_types = "(86, 145, 146, 160)";
    SET @non_clinical_encounter_types = "(-1)";
    SET @other_encounter_types = "(-1)";
    
    SET @start := NOW();
    SET @table_version := "flat_breast_cancer_screening_v1.2";

    SET session sort_buffer_size := 512000000;

    SET @sep := " ## ";
    SET @boundary := "!!";
    SET @last_date_created := (SELECT MAX(max_date_created) FROM etl.flat_obs);

    CREATE TABLE IF NOT EXISTS flat_breast_cancer_screening (
        date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        person_id int,
        encounter_id int,
        encounter_type int,
        encounter_datetime datetime,
        visit_id int,
        location_id int,
        location_uuid varchar (100),
        gender char (100),
        age int,
        encounter_purpose int,
        other_encounter_purpose varchar(1000),
        menstruation_before_12 char,
        menses_stopped_permanently char,
        menses_stop_age int,
        hrt_use char,
        hrt_start_age int,
        hrt_end_age int,
        hrt_use_years int,
        hrt_type_used int,
        given_birth int,
        age_first_birth int,
        gravida int,
        parity int,
        cigarette_smoking int,
        cigarette_smoked_day int,
        tobacco_use int,
        tobacco_use_duration_yrs int,
        alcohol_drinking int,
        alcohol_type int,
        alcohol_use_period_yrs int,
        breast_complaints_3months int,
        breast_mass_location tinyint,
        nipple_discharge_location tinyint,
        nipple_retraction_location tinyint,
        breast_erythrema_location tinyint,
        breast_rash_location tinyint,
        breast_pain_location tinyint,
        other_changes_location tinyint,
        history_of_mammogram tinyint,
        mammogram_results tinyint,
        breast_ultrasound_history tinyint,
        breast_ultrasound_result tinyint,
        history_of_breast_biopsy tinyint,
        breast_biopsy_results tinyint,
        number_of_biopsies tinyint,
        biopsy_type tinyint,
        prev_exam_results int,
        fam_brca_history_bf50 int,
        fam_brca_history_aft50 int,
        fam_male_brca_history int,
        fam_ovarianca_history int,
        fam_relatedca_history int,
        fam_otherca_specify int,
        cur_physical_findings int,
        lymph_nodes_findings int,
        cur_screening_findings int,
        #cur_screening_findings_date int,
        patient_education int,
        patient_education_other varchar(1000),
        referrals_ordered int,
        referred_date datetime,
        procedure_done int,
        next_app_date datetime,	
        cbe_imaging_concordance int,
        mammogram_workup_date datetime,
        date_patient_notified_of_mammogram_results datetime,
        ultrasound_results int,
        ultrasound_workup_date datetime,
        date_patient_notified_of_ultrasound_results datetime,
        fna_results int,
        fna_tumor_size int,
        fna_degree_of_malignancy int,
        fna_workup_date datetime,
        date_patient_notified_of_fna_results datetime,
        biopsy_tumor_size int,
        biopsy_degree_of_malignancy int,
        biopsy_workup_date datetime,
        date_patient_notified_of_biopsy_results datetime,
        biopsy_description varchar(1000),
        diagnosis_date datetime,
        date_patient_informed_and_referred_for_management datetime,
        screening_mode int,
        diagnosis int,
        cancer_staging int,
        hiv_status int,     
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
        primary key encounter_id (encounter_id),
        index person_date (person_id, encounter_datetime),
        index location_enc_date (location_uuid,encounter_datetime),
        index enc_date_location (encounter_datetime, location_uuid),
        index location_id_rtc_date (location_id,next_app_date),
        index location_uuid_rtc_date (location_uuid,next_app_date),
        index loc_id_enc_date_next_clinical (location_id, encounter_datetime, next_clinical_datetime_breast_cancer_screening),
        index encounter_type (encounter_type),
        index date_created (date_created)
    );

    -- Build step
    IF (@query_type = "build") THEN
        SELECT 'BUILDING..........................................';
							
        SET @write_table := CONCAT('flat_breast_cancer_screening_temp_', queue_number);
        SET @queue_table := CONCAT('flat_breast_cancer_screening_build_queue_', queue_number);

        SET @dyn_sql := CONCAT('CREATE TABLE IF NOT EXISTS ', @write_table, ' LIKE ', @primary_table);
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  

        SET @dyn_sql := CONCAT('CREATE TABLE IF NOT EXISTS ', @queue_table, ' (SELECT * FROM flat_breast_cancer_screening_build_queue LIMIT ', queue_size, ');'); 
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;

        SET @dyn_sql := CONCAT('DELETE t1 FROM flat_breast_cancer_screening_build_queue t1 join ', @queue_table, ' t2 using (person_id);'); 
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1; 
    END IF;

    -- Sync step
    IF (@query_type = 'sync') THEN
        SELECT 'SYNCING.....................................';
        
        SET @write_table := 'flat_breast_cancer_screening';
        SET @queue_table := 'flat_breast_cancer_screening_sync_queue';

        CREATE TABLE IF NOT EXISTS flat_breast_cancer_screening_sync_queue (person_id INT PRIMARY KEY);
        
        SET @last_update := null;

        SELECT MAX(date_updated) INTO @last_update FROM etl.flat_log WHERE table_name = @table_version;

        SELECT 'Finding patients in amrs.encounters...';
            REPLACE INTO flat_breast_cancer_screening_sync_queue
            (SELECT DISTINCT
                patient_id
              FROM
                amrs.encounter
              WHERE 
                date_changed > @last_update
            );

        SELECT 'Finding patients in flat_obs...';
            REPLACE INTO flat_breast_cancer_screening_sync_queue
            (SELECT DISTINCT
                person_id
              FROM
                etl.flat_obs
              WHERE
                max_date_created > @last_update
            );

        SELECT 'Finding patients in flat_lab_obs...';
            REPLACE INTO flat_breast_cancer_screening_sync_queue
            (SELECT DISTINCT
                person_id
              FROM
                etl.flat_lab_obs
              WHERE
                max_date_created > @last_update
            );

        SELECT 'Finding patients in flat_orders...';
            REPLACE INTO flat_breast_cancer_screening_sync_queue
            (SELECT DISTINCT
                person_id
              FROM
                etl.flat_orders
              WHERE
                max_date_created > @last_update
            );

            REPLACE INTO flat_breast_cancer_screening_sync_queue
            (SELECT 
                person_id
              FROM 
		            amrs.person 
	            WHERE 
                date_voided > @last_update
            );

            REPLACE INTO flat_breast_cancer_screening_sync_queue
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
        DROP TEMPORARY TABLE IF EXISTS flat_breast_cancer_screening_build_queue__0;

        SET @dyn_sql := CONCAT('CREATE TEMPORARY TABLE flat_breast_cancer_screening_build_queue__0 (person_id INT PRIMARY KEY) (SELECT * FROM ', @queue_table, ' LIMIT ', cycle_size, ');');
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
                    
        DROP TEMPORARY TABLE IF EXISTS flat_breast_cancer_screening_0a;
        SET @dyn_sql := CONCAT(
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
            FROM
                etl.flat_obs t1
                    JOIN
                flat_breast_cancer_screening_build_queue__0 t0 USING (person_id)
                    LEFT JOIN
                etl.flat_orders t2 USING (encounter_id)
            WHERE
                t1.encounter_type IN ', @encounter_types, ');');
                            
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  

        INSERT INTO flat_breast_cancer_screening_0a
        (SELECT
            t1.person_id,
            null,
            t1.encounter_id,
            t1.test_datetime,
            t1.encounter_type,
            null, -- t1.location_id,
            t1.obs,
            null, -- obs_datetimes
            0 as is_clinical_encounter,
            1 as encounter_type_sort_index,
            null
        FROM 
          etl.flat_lab_obs t1
              JOIN
          flat_breast_cancer_screening_build_queue__0 t0 USING (person_id)
        );

        DROP TEMPORARY TABLE IF EXISTS flat_breast_cancer_screening_0;
        CREATE TEMPORARY TABLE flat_breast_cancer_screening_0 (INDEX encounter_id (encounter_id), INDEX person_enc (person_id, encounter_datetime))
        (SELECT 
            * 
        FROM
          flat_breast_cancer_screening_0a
        ORDER BY
          person_id, DATE(encounter_datetime)
        );
                        
        SET @encounter_purpose := null;
        SET @other_encounter_purpose := null;
        SET @menstruation_before_12 := null;
        SET @menses_stopped_permanently := null;
        SET @menses_stop_age := null;
        SET @hrt_use := null;
        SET @hrt_start_age := null;
        SET @hrt_end_age := null;
        SET @hrt_use_years := null;
        SET @hrt_type_used =null;
        SET @given_birth := null;
        SET @age_first_birth := null;
        SET @gravida := null;
        SET @parity := null;
        SET @cigarette_smoking := null;
        SET @cigarette_smoked_day := null;
        SET @tobacco_use := null;
        SET @tobacco_use_duration_yrs := null;
        SET @alcohol_drinking := null;
        SET @alcohol_type := null;
        SET @alcohol_use_period_yrs := null;
        SET @breast_complaints_3months := null;
        SET @breast_mass_location := null;
        SET @nipple_discharge_location := null;
        SET @nipple_retraction_location := null;
        SET @breast_erythrema_location := null;
        SET @breast_rash_location := null;
        SET @breast_pain_location := null;
        SET @other_changes_location := null;
        SET @history_of_mammogram := null;
        SET @mammogram_results := null;
        SET @breast_ultrasound_history := null;
        SET @breast_ultrasound_result := null;
        SET @history_of_breast_biopsy := null;
        SET @breast_biopsy_results := null;
        SET @number_of_biopsies := null;
        SET @biopsy_type := null;
        SET @prev_exam_results := null;
        SET @fam_brca_history_bf50 := null;
        SET @fam_brca_history_aft50 := null;
        SET @fam_male_brca_history := null;
        SET @fam_ovarianca_history := null;
        SET @fam_relatedca_history := null;
        SET @fam_otherca_specify := null;
        SET @cur_physical_findings := null;
        SET @lymph_nodes_findings := null;
        SET @cur_screening_findings := null;
        -- SET @cur_screening_findings_date := null;
        SET @patient_education := null;
        SET @patient_education_other := null;
        SET @referrals_ordered := null; 
        SET @procedure_done := null;
        SET @referred_date := null;
        SET @next_app_date := null;
        SET @cbe_imaging_concordance := null;
        SET @mammogram_workup_date := null; 
        SET @date_patient_notified_of_mammogram_results := null;
        SET @ultrasound_results := null;
        SET @ultrasound_workup_date := null;
        SET @date_patient_notified_of_ultrasound_results := null;
        SET @fna_results := null;
        SET @fna_tumor_size := null;
        SET @fna_degree_of_malignancy := null;
        SET @fna_workup_date := null;
        SET @date_patient_notified_of_fna_results := null;
        SET @biopsy_tumor_size := null;
        SET @biopsy_degree_of_malignancy := null;
        SET @biopsy_workup_date := null;
        SET @date_patient_notified_of_biopsy_results := null;
        SET @biopsy_description := null;
        SET @diagnosis_date := null;
        SET @date_patient_informed_and_referred_for_management := null;
        SET @screening_mode := null;
        SET @diagnosis := null;
        SET @cancer_staging := null;
        SET @hiv_status := null;

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
				-- t4.name as location_name,
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
					when obs regexp "!!1915=" then @other_encounter_purpose := GetValues(obs,1915) 
					else @other_encounter_purpose := null
				end as other_encounter_purpose,
				case
				  when obs regexp "!!9560=1065!!" then @menstruation_before_12 := 1
				  when obs regexp "!!9560=1066!!" then @menstruation_before_12 := 0
				  else @menstruation_before_12 := null
				end as menstruation_before_12,                           
				case
					when obs regexp "!!9561=1065!!" then @menses_stopped_permanently := 1
					when obs regexp "!!9561=1066!!" then @menses_stopped_permanently := 0
					when obs regexp "!!9561=9568!!" then @menses_stopped_permanently := 2
					else @menses_stopped_permanently := null
				end as menses_stopped_permanently,
				case
					when obs regexp "!!9562=[0-9]" and t1.encounter_type = @lab_encounter_type then @menses_stop_age:=cast(GetValues(obs,9562) as unsigned)
					when @prev_id = @cur_id then @menses_stop_age
					else @menses_stop_age := null
				end as menses_stop_age,
				case
					when obs regexp "!!9626=1065!!" then @hrt_use := 1
					when obs regexp "!!9626=1066!!" then @hrt_use := 0
					when obs regexp "!!9626=9568!!" then @hrt_use := 2
					else @hrt_use := null
				end as hrt_use,
				case
					when obs regexp "!!9627=[0-9]" then @hrt_start_age := cast(GetValues(obs,9627) as unsigned)
					else @hrt_start_age := null
				end as hrt_start_age,
				case
					when obs regexp "!!9723=[0-9]" then @hrt_end_age := cast(GetValues(obs,9723) as unsigned) 
					else @hrt_end_age := null
				end as hrt_end_age,
				case
					when obs regexp "!!97629=[0-9]" then @hrt_use_years := cast(GetValues(obs,9629) as unsigned) 
					else @hrt_use_years := null
				end as hrt_use_years,
				case
					when obs regexp "!!9630=9573!!" then @hrt_type_used := 1
					when obs regexp "!!9630=6217!!" then @hrt_type_used := 2
					when obs regexp "!!9630=6218!!" then @hrt_type_used := 3
					else @hrt_type_used := null
				end as hrt_type_used,
                case
					when obs regexp "!!9563=1065!!" then @given_birth := 1
					when obs regexp "!!9563=1066!!" then @given_birth := 0
					else @given_birth := null
				end as given_birth,
				case
					when obs regexp "!!5574=[0-9]" then @age_first_birth := cast(GetValues(obs,5574) as unsigned) 
					else @age_first_birth := null
				end as age_first_birth,
				case
					when obs regexp "!!5624=[0-9]" then @gravida := cast(GetValues(obs,5624) as unsigned)
					else @gravida := null
				end as gravida,
				case
					when obs regexp "!!1053=[0-9]" then @parity := cast(GetValues(obs,1053) as unsigned) 
					else @parity := null
				end as parity,
				case
					when obs regexp "!!9333=" then @cigarette_smoking := GetValues(obs,9333)
					else @cigarette_smoking := null
				end as cigarette_smoking,
				case
					when obs regexp "!!2069=[0-9]" then @cigarette_smoked_day := cast(GetValues(obs,2069) as unsigned) 
					else @cigarette_smoked_day := null
				end as cigarette_smoked_day,
				case
					when obs regexp "!!7973=1065!!" then @tobacco_use := 1
					when obs regexp "!!7973=1066!!" then @tobacco_use := 0
					when obs regexp "!!7973=1679!!" then @tobacco_use := 2
					else @tobacco_use := null
				end as tobacco_use,
                case
					when obs regexp "!!8144=[0-9]" then @tobacco_use_duration_yrs := cast(GetValues(obs, 8144) as unsigned) 
					else @tobacco_use_duration_yrs := null
				end as tobacco_use_duration_yrs,
				case
					when obs regexp "!!1684=1065!!" then @alcohol_drinking := 1
					when obs regexp "!!1684=1066!!" then @alcohol_drinking := 0
					when obs regexp "!!1684=1679!!" then @alcohol_drinking := 2
					else @alcohol_drinking := null
				end as alcohol_drinking,
				case
					when obs regexp "!!1685=1682!!" then @alcohol_type := 1
					when obs regexp "!!1685=1681!!" then @alcohol_type := 2
					when obs regexp "!!1685=1680!!" then @alcohol_type := 3
					when obs regexp "!!1685=1683!!" then @alcohol_type := 4
					when obs regexp "!!1685=2059!!" then @alcohol_type := 5
					when obs regexp "!!1685=5622!!" then @alcohol_type := 6
					else @alcohol_type := null
				end as alcohol_type,
				case
					when obs regexp "!!8170=[0-9]"  then @alcohol_use_period_yrs := cast(GetValues(obs,8170) as unsigned) 
					else @alcohol_use_period_yrs := null
				end as alcohol_use_period_yrs,
                case
					when obs regexp "!!9553=5006!!" then @breast_complaints_3months := 1
					when obs regexp "!!9553=1068!!" then @breast_complaints_3months := 2
					else @breast_complaints_3months := null
				end as breast_complaints_3months,
				case
					when obs regexp "!!6697=2399!!" then @breast_mass_location := 1
					when obs regexp "!!6697=5139!!" then @breast_mass_location := 2
					when obs regexp "!!6697=5141!!" then @breast_mass_location := 3
					when obs regexp "!!6697=6589!!" then @breast_mass_location := 4
					when obs regexp "!!6697=6590!!" then @breast_mass_location := 5
					when obs regexp "!!6697=6591!!" then @breast_mass_location := 6
					when obs regexp "!!6697=9625!!" then @breast_mass_location := 7
					else @breast_mass_location := null
				end as breast_mass_location,
				case
					when obs regexp "!!9557=5139!!" then @nipple_discharge_location := 1
					when obs regexp "!!9557=5141!!" then @nipple_discharge_location := 2
					when obs regexp "!!9557=9625!!" then @nipple_discharge_location := 3
					else @nipple_discharge_location := null
				end as nipple_discharge_location,
				case
					when obs regexp "!!9558=5139!!" then @nipple_retraction_location := 1
					when obs regexp "!!9558=5141!!" then @nipple_retraction_location := 2
					when obs regexp "!!9558=9625!!" then @nipple_retraction_location := 3
					else @nipple_retraction_location := null
				end as nipple_retraction_location,
				case
					when obs regexp "!!9574=5139!!" then @breast_erythrema_location := 1
					when obs regexp "!!9574=5141!!" then @breast_erythrema_location := 2
					when obs regexp "!!9574=9625!!" then @breast_erythrema_location := 3
					else @breast_erythrema_location := null
				end as breast_erythrema_location,
				case
					when obs regexp "!!9577=5139!!" then @breast_rash_location := 1
					when obs regexp "!!9577=5141!!" then @breast_rash_location := 2
					when obs regexp "!!9577=9625!!" then @breast_rash_location := 3
					else @breast_rash_location := null
				end as breast_rash_location,
				case
					when obs regexp "!!9577=5139!!" then @breast_pain_location := 1
					when obs regexp "!!9577=5141!!" then @breast_pain_location := 2
					when obs regexp "!!9577=9625!!" then @breast_pain_location := 3
					else @breast_pain_location := null
				end as breast_pain_location,
				case
					when obs regexp "!!9576=5139!!" then @other_changes_location := 1
					when obs regexp "!!9576=5141!!" then @other_changes_location := 2
					when obs regexp "!!9576=9625!!" then @other_changes_location := 3
					else @other_changes_location := null
				end as other_changes_location,
				case
					when obs regexp "!!9594=1066!!" then @history_of_mammogram := 0
					when obs regexp "!!9594=1065!!" then @history_of_mammogram := 1
					else @history_of_mammogram := null
				end as history_of_mammogram,
                case
					when obs regexp "!!9595=1115!!" then @mammogram_results := 1
					when obs regexp "!!9595=1116!!" then @mammogram_results := 2
					when obs regexp "!!9595=1118!!" then @mammogram_results := 3
					when obs regexp "!!9595=1138!!" then @mammogram_results := 4
					when obs regexp "!!9595=1267!!" then @mammogram_results := 5
					when obs regexp "!!9595=1067!!" then @mammogram_results := 6
				  else @mammogram_results := null
				end as mammogram_results,
				case
					when obs regexp "!!9597=1066!!" then @breast_ultrasound_history := 0		
					when obs regexp "!!9597=1065!!" then @breast_ultrasound_history := 1
					else @breast_ultrasound_history := null
				end as breast_ultrasound_history,
				case
					when obs regexp "!!9596=1115!!" then @breast_ultrasound_result := 1
					when obs regexp "!!9596=1116!!" then @breast_ultrasound_result := 2
					when obs regexp "!!9596=1067!!" then @breast_ultrasound_result := 3
					when obs regexp "!!9596=1118!!" then @breast_ultrasound_result := 4
					else @breast_ultrasound_result := null
				end as breast_ultrasound_result,
				case
					when obs regexp "!!9598=1066!!" then @history_of_breast_biopsy := 0
					when obs regexp "!!9598=1065!!" then @history_of_breast_biopsy := 1
					else @history_of_breast_biopsy := null
				end as history_of_breast_biopsy,
				case
					when obs regexp "!!8184=1115!!" then @breast_biopsy_results := 1
					when obs regexp "!!8184=1116!!" then @breast_biopsy_results := 2
					when obs regexp "!!8184=1118!!" then @breast_biopsy_results := 3
					when obs regexp "!!8184=1067!!" then @breast_biopsy_results := 4
					when obs regexp "!!8184=9691!!" then @breast_biopsy_results := 5
					when obs regexp "!!8184=10052!!" then @breast_biopsy_results := 6
					else @breast_biopsy_results := null
				end as breast_biopsy_results,
                case
					when obs regexp "!!9599=" then @number_of_biopsies := GetValues(obs, 9599)
					else @number_of_biopsies := null
				end as number_of_biopsies,
               case
					when obs regexp "!!6509=6510!!" then @biopsy_type := 1
					when obs regexp "!!6509=6511!!" then @biopsy_type := 2
					when obs regexp "!!6509=6512!!" then @biopsy_type := 3
					when obs regexp "!!6509=6513!!" then @biopsy_type := 4
					when obs regexp "!!6509=7190!!" then @biopsy_type := 5
					when obs regexp "!!6509=8184!!" then @biopsy_type := 6
					when obs regexp "!!6509=10075!!" then @biopsy_type := 7
					when obs regexp "!!6509=10076!!" then @biopsy_type := 8   
					else @biopsy_type := null             
				end as biopsy_type,
                case
					when obs regexp "!!9694=1115!!" then @prev_exam_results := 1
					when obs regexp "!!9694=1116!!" then @prev_exam_results := 0
					when obs regexp "!!9694=1067!!" then @prev_exam_results := 2
					else @prev_exam_results := null
				end as prev_exam_results,
				case
					when obs regexp "!!9631=1672!!" then @fam_brca_history_bf50 := 1
					when obs regexp "!!9631=1107!!" then @fam_brca_history_bf50 := 0
					when obs regexp "!!9631=978!!" then @fam_brca_history_bf50 := 2
					when obs regexp "!!9631=972!!" then @fam_brca_history_bf50 := 3
					when obs regexp "!!9631=1671!!" then @fam_brca_history_bf50 := 4
					when obs regexp "!!9631=1393!!" then @fam_brca_history_bf50 := 5
					when obs regexp "!!9631=1392!!" then @fam_brca_history_bf50 := 6
					when obs regexp "!!9631=1395!!" then @fam_brca_history_bf50 := 7
					when obs regexp "!!9631=1394!!" then @fam_brca_history_bf50 := 8
					else @fam_brca_history_bf50 := null
				end as fam_brca_history_bf50,
				case
					when obs regexp "!!9632=978!!" then @fam_brca_history_aft50 := 1
					when obs regexp "!!9632=1672!!" then @fam_brca_history_aft50 := 2
					when obs regexp "!!9632=972!!" then @fam_brca_history_aft50 := 3
					when obs regexp "!!9632=1671!!" then @fam_brca_history_aft50 := 4
					when obs regexp "!!9632=1393!!" then @fam_brca_history_aft50 := 5
					when obs regexp "!!9632=1392!!" then @fam_brca_history_aft50 := 6
					when obs regexp "!!9632=1395!!" then @fam_brca_history_aft50 := 7
					when obs regexp "!!9632=1394!!" then @fam_brca_history_aft50 := 8
					else @fam_brca_history_aft50 := null
				end as fam_brca_history_aft50,
				case
					when obs regexp "!!9633=978!!" then @fam_male_brca_history := 1
					when obs regexp "!!9633=1672!!" then @fam_male_brca_history := 2
					when obs regexp "!!9633=972!!" then @fam_male_brca_history := 3
					when obs regexp "!!9633=1671!!" then @fam_male_brca_history := 4
					when obs regexp "!!9633=1393!!" then @fam_male_brca_history := 5
					when obs regexp "!!9633=1392!!" then @fam_male_brca_history := 6
					when obs regexp "!!9633=1395!!" then @fam_male_brca_history := 7
					when obs regexp "!!9633=1394!!" then @fam_male_brca_history := 8
					else @fam_male_brca_history := null
				end as fam_male_brca_history,
				case
					when obs regexp "!!9634=978!!" then @fam_ovarianca_history := 1
					when obs regexp "!!9634=1672!!" then @fam_ovarianca_history := 2
					when obs regexp "!!9634=972!!" then @fam_ovarianca_history := 3
					when obs regexp "!!9634=1671!!" then @fam_ovarianca_history := 4
					when obs regexp "!!9634=1393!!" then @fam_ovarianca_history := 5
					when obs regexp "!!9634=1392!!" then @fam_ovarianca_history := 6
					when obs regexp "!!9634=1395!!" then @fam_ovarianca_history := 7
					when obs regexp "!!9634=1394!!" then @fam_ovarianca_history := 8
					else @fam_ovarianca_history := null
				end as fam_ovarianca_history,
				case
					when obs regexp "!!9635=978!!" then @fam_relatedca_history:= 1
					when obs regexp "!!9635=1672!!" then @fam_relatedca_history := 2
					when obs regexp "!!9635=972!!" then @fam_relatedca_history := 3
					when obs regexp "!!9635=1671!!" then @fam_relatedca_history := 4
					when obs regexp "!!9635=1393!!" then @fam_relatedca_history := 5
					when obs regexp "!!9635=1392!!" then @fam_relatedca_history := 6
					when obs regexp "!!9635=1395!!" then @fam_relatedca_history := 7
					when obs regexp "!!9635=1394!!" then @fam_relatedca_history := 8
					else @fam_relatedca_history := null
				end as fam_relatedca_history,
				case
					when obs regexp "!!7176=6529!!" then @fam_otherca_specify := 1
					when obs regexp "!!7176=9636!!" then @fam_otherca_specify := 2
					when obs regexp "!!7176=9637!!" then @fam_otherca_specify := 3
					when obs regexp "!!7176=6485!!" then @fam_otherca_specify := 4
					when obs regexp "!!7176=9638!!" then @fam_otherca_specify := 5
					when obs regexp "!!7176=9639!!" then @fam_otherca_specify := 6
					when obs regexp "!!7176=6522!!" then @fam_otherca_specify := 7
					when obs regexp "!!7176=216!!" then @fam_otherca_specify := 8
					else @fam_otherca_specify := null
				end as fam_otherca_specify,
				case
					when obs regexp "!!6251=1115!!" then @cur_physical_findings:= 0
					when obs regexp "!!6251=115!!" then @cur_physical_findings := 1
					when obs regexp "!!6251=6250!!" then @cur_physical_findings := 2
					when obs regexp "!!6251=6249!!" then @cur_physical_findings := 3
					when obs regexp "!!6251=5622!!" then @cur_physical_findings := 4
					when obs regexp "!!6251=582!!" then @cur_physical_findings := 5
					when obs regexp "!!6251=6493!!" then @cur_physical_findings := 6
					when obs regexp "!!6251=6499!!" then @cur_physical_findings := 7
					when obs regexp "!!6251=1118!!" then @cur_physical_findings := 8
					when obs regexp "!!6251=1481!!" then @cur_physical_findings := 9
					when obs regexp "!!6251=6729!!" then @cur_physical_findings := 10
					when obs regexp "!!6251=1116!!" then @cur_physical_findings := 11
					when obs regexp "!!6251=8188!!" then @cur_physical_findings := 12
					when obs regexp "!!6251=8189!!" then @cur_physical_findings := 13
					when obs regexp "!!6251=1067!!" then @cur_physical_findings := 14
					when obs regexp "!!6251=9689!!" then @cur_physical_findings := 15
					when obs regexp "!!6251=9690!!" then @cur_physical_findings := 16
					when obs regexp "!!6251=9687!!" then @cur_physical_findings := 17
					when obs regexp "!!6251=9688!!" then @cur_physical_findings := 18
					when obs regexp "!!6251=5313!!" then @cur_physical_findings := 19
					when obs regexp "!!6251=9691!!" then @cur_physical_findings := 20
					else @cur_physical_findings := null
				end as cur_physical_findings,
                case
					when obs regexp "!!1121=1115!!" then @lymph_nodes_findings := 1
					when obs regexp "!!1121=161!!" then @lymph_nodes_findings := 2
					when obs regexp "!!1121=9675!!" then @lymph_nodes_findings:= 3
					when obs regexp "!!1121=9676!!" then @lymph_nodes_findings:= 4
					else @lymph_nodes_findings := null
				end as lymph_nodes_findings,
				case
					when obs regexp "!!9748=1115!!" then @cur_screening_findings := 1
					when obs regexp "!!9748=9691!!" then @cur_screening_findings := 3
					when obs regexp "!!9748=1116!!" then @cur_screening_findings := 2
					else @cur_screening_findings := null
				end as cur_screening_findings,
				case
					when obs regexp "!!6327=9651!!" then @patient_education := 1
					when obs regexp "!!6327=2345!!" then @patient_education := 2
					when obs regexp "!!6327=9692!!" then @patient_education:= 3
					when obs regexp "!!6327=5622!!" then @patient_education:= 4
					else @patient_education := null
				end as patient_education,
				case
					when obs regexp "!!1915=" then @patient_education_other := GetValues(obs,1915) 
					else @patient_education_other := null
				end as patient_education_other,                            
				case
					when obs regexp "!!1272=1107!!" then @referrals_ordered := 0
					when obs regexp "!!1272=1496!!" then @referrals_ordered:= 1
					else @referrals_ordered := null
				end as referrals_ordered,
				case
					when obs regexp "!!1272=9596!!" then @procedure_done := 1
					when obs regexp "!!1272=9595!!" then @procedure_done:= 2
					when obs regexp "!!1272=6510!!" then @procedure_done := 3
					when obs regexp "!!1272=7190!!" then @procedure_done:= 4
					when obs regexp "!!1272=6511!!" then @procedure_done := 5
					when obs regexp "!!1272=9997!!" then @procedure_done:= 6
					else @procedure_done := null
				end as procedure_done,
				case
					when obs regexp "!!9158=" then @referred_date := GetValues(obs,9158) 
					else @referred_date := null
				end as referred_date,
				case
					when obs regexp "!!5096=" then @next_app_date := GetValues(obs,5096) 
					else @next_app_date := null
				end as next_app_date,
				case
					when obs regexp "!!9702=9703!!" then @cbe_imaging_concordance := 1
					when obs regexp "!!9702=9704!!" then @cbe_imaging_concordance := 2
					when obs regexp "!!9702=1175!!" then @cbe_imaging_concordance := 3
					else @cbe_imaging_concordance := null
				end as cbe_imaging_concordance,
				case
					when obs regexp "!!9708=" then @mammogram_workup_date := GetValues(obs,9708)
					else @mammogram_workup_date := null
				end as mammogram_workup_date,                    
				case
					when obs regexp "!!9705=" then @date_patient_notified_of_mammogram_results := GetValues(obs,9705) 
					else @date_patient_notified_of_mammogram_results := null
				end as date_patient_notified_of_mammogram_results,
				case
					when obs regexp "!!9596=1115!!" then @ultrasound_results := 1
					when obs regexp "!!9596=1116!!" then @ultrasound_results := 2
					when obs regexp "!!9596=1067!!" then @ultrasound_results := 3
					when obs regexp "!!9596=1118!!" then @ultrasound_results := 4
					else @ultrasound_results := null
				end as ultrasound_results,
				case
					when obs regexp "!!9708=" then @ultrasound_workup_date := GetValues(obs,9708) 
					else @ultrasound_workup_date := null
				end as ultrasound_workup_date,
				case
					when obs regexp "!!10047=" then @date_patient_notified_of_ultrasound_results := GetValues(obs,10047) 
					else @date_patient_notified_of_ultrasound_results := null
				end as date_patient_notified_of_ultrasound_results,
				case
					when obs regexp "!!10051=9691!!" then @fna_results := 1
					when obs regexp "!!10051=10052!!" then @fna_results := 2
					when obs regexp "!!9596=1118!!" then @fna_results := 3
					else @fna_results := null
				end as fna_results,                               
				case
					when obs regexp "!!10053=[0-9]" then @fna_tumor_size:= cast(GetValues(obs,10053) as unsigned) 
					else @fna_tumor_size := null
				end as fna_tumor_size,
				case
					when obs regexp "!!10054=10055!!" then @fna_degree_of_malignancy := 1
					when obs regexp "!!10054=10056!!" then @fna_degree_of_malignancy := 2
					else @fna_degree_of_malignancy := null
				end as fna_degree_of_malignancy,
				case
					when obs regexp "!!10057=" then @fna_workup_date := GetValues(obs,10057) 
					else @fna_workup_date := null
				end as fna_workup_date,                                    
				case
					when obs regexp "!!10059=" then @date_patient_notified_of_fna_results := GetValues(obs,10059)
					else @date_patient_notified_of_fna_results := null
				end as date_patient_notified_of_fna_results,
				case
					when obs regexp "!!10053=[0-9]" then @biopsy_tumor_size:= cast(GetValues(obs,10053) as unsigned) 
					else @biopsy_tumor_size := null
				end as biopsy_tumor_size,                                        
				case
					when obs regexp "!!10054=10055!!" then @biopsy_degree_of_malignancy := 1
					when obs regexp "!!10054=10056!!" then @biopsy_degree_of_malignancy := 2
					else @biopsy_degree_of_malignancy := null
				end as biopsy_degree_of_malignancy,                            
				case
					when obs regexp "!!10060=" then @biopsy_workup_date := GetValues(obs,10060) 
					else @biopsy_workup_date := null
				end as biopsy_workup_date,
				case
					when obs regexp "!!10061=" then @date_patient_notified_of_biopsy_resultse := GetValues(obs,10061) 
					else @date_patient_notified_of_biopsy_results := null
				end as date_patient_notified_of_biopsy_results,
				case
					when obs regexp "!!7400=" then @biopsy_description := GetValues(obs,7400)
					else @biopsy_description := null
				end as biopsy_description,
				case
					when obs regexp "!!9728=" then @diagnosis_date := GetValues(obs, 9728)
					else @diagnosis_date := null
				end as diagnosis_date,
				case
					when obs regexp "!!9706=" then @date_patient_informed_and_referred_for_management := GetValues(obs, 9706) 
					else @date_patient_informed_and_referred_for_management := null
				end as date_patient_informed_and_referred_for_management,
				case
					when obs regexp "!!10068=9595!!" then @screening_mode := 1
					when obs regexp "!!10068=10067!!" then @screening_mode := 2
					when obs regexp "!!10068=9596!!" then @screening_mode := 3
				  else @screening_mode := null
				end as screening_mode,
				case
					when obs regexp "!!6042=" then @diagnosis := GetValues(obs,6042) 
					else @diagnosis := null
				end as diagnosis,                          
				case
					when obs regexp "!!9868=9852!!" then @cancer_staging := 1
					when obs regexp "!!9868=9856!!" then @cancer_staging := 2
					when obs regexp "!!9868=9860!!" then @cancer_staging := 3
					when obs regexp "!!9868=9864!!" then @cancer_staging := 4
					else @cancer_staging := null
				end as cancer_staging,
				case
					when obs regexp "!!6709=664!!" then @hiv_status := 1
					when obs regexp "!!6709=703!!" then @hiv_status := 2
					when obs regexp "!!6709=1067!!" then @hiv_status := 3
					else @hiv_status := null
				end as hiv_status
          FROM 
              flat_breast_cancer_screening_0 t1
			    JOIN amrs.person p using (person_id)
   	      ORDER BY person_id, date(encounter_datetime) DESC, encounter_type_sort_index DESC
  	    );

          SET @prev_id = null;
          SET @cur_id = null;
          set @prev_encounter_datetime = null;
					set @cur_encounter_datetime = null;
					set @prev_clinical_datetime = null;
					set @cur_clinical_datetime = null;

					set @next_encounter_type = null;
					set @cur_encounter_type = null;

          set @prev_clinical_location_id = null;
          set @cur_clinical_location_id = null;
						
          ALTER TABLE flat_breast_cancer_screening_1 DROP prev_id, DROP cur_id;

          drop table if exists flat_breast_cancer_screening_2;
          create temporary table flat_breast_cancer_screening_2
          (select *,
            @prev_id := @cur_id as prev_id,
            @cur_id := person_id as cur_id,

            case
              when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
              else @prev_encounter_datetime := null
            end as next_encounter_datetime_breast_cancer_screening,

            @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

            case
              when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
              else @next_encounter_type := null
            end as next_encounter_type_breast_cancer_screening,

            @cur_encounter_type := encounter_type as cur_encounter_type,

            case
              when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
              else @prev_clinical_datetime := null
            end as next_clinical_datetime_breast_cancer_screening,

                          case
              when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
              else @prev_clinical_location_id := null
            end as next_clinical_location_id_breast_cancer_screening,

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
            end as next_clinical_rtc_date_breast_cancer_screening,

            case
              when is_clinical_encounter then @cur_clinical_rtc_date := next_app_date
              when @prev_id = @cur_id then @cur_clinical_rtc_date
              else @cur_clinical_rtc_date:= null
            end as cur_clinical_rtc_date

            from flat_breast_cancer_screening_1
            order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
          );

          alter table flat_breast_cancer_screening_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;

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

          drop temporary table if exists flat_breast_cancer_screening_3;
          create temporary table flat_breast_cancer_screening_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
          (select
            *,
            @prev_id := @cur_id as prev_id,
            @cur_id := t1.person_id as cur_id,

            case
                  when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
                  else @prev_encounter_type:=null
            end as prev_encounter_type_breast_cancer_screening,	
			@cur_encounter_type := encounter_type as cur_encounter_type,

            case
                  when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
                  else @prev_encounter_datetime := null
			end as prev_encounter_datetime_breast_cancer_screening,

		  @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

            case
              when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
              else @prev_clinical_datetime := null
            end as prev_clinical_datetime_breast_cancer_screening,

			case
              when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
              else @prev_clinical_location_id := null
            end as prev_clinical_location_id_breast_cancer_screening,

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
            end as prev_clinical_rtc_date_breast_cancer_screening,

            case
              when is_clinical_encounter then @cur_clinical_rtc_date := next_app_date
              when @prev_id = @cur_id then @cur_clinical_rtc_date
              else @cur_clinical_rtc_date:= null
            end as cur_clinic_rtc_date

            from flat_breast_cancer_screening_2 t1
            order by person_id, date(encounter_datetime), encounter_type_sort_index
          );

	        SELECT 
              COUNT(*)
          INTO 
              @new_encounter_rows
          FROM
              flat_breast_cancer_screening_1;
                              
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
                    visit_id,
                    location_id,
                    t2.uuid as location_uuid,
                    gender,
                    age,
                    encounter_purpose,	
                    other_encounter_purpose,
                    menstruation_before_12,
                    menses_stopped_permanently,
                    menses_stop_age,
                    hrt_use,
                    hrt_start_age,
                    hrt_end_age,
                    hrt_use_years,
                    hrt_type_used,
                    given_birth,
                    age_first_birth,
                    gravida,
                    parity,
                    cigarette_smoking,
                    cigarette_smoked_day,
                    tobacco_use,
                    tobacco_use_duration_yrs,
                    alcohol_drinking,
                    alcohol_type,
                    alcohol_use_period_yrs,
                    breast_complaints_3months,
                    breast_mass_location,
                    nipple_discharge_location,
                    nipple_retraction_location,
                    breast_erythrema_location,
                    breast_rash_location,
                    breast_pain_location,
                    other_changes_location,
                    history_of_mammogram,
                    mammogram_results,
                    breast_ultrasound_history,
                    breast_ultrasound_result,
                    history_of_breast_biopsy,
                    breast_biopsy_results,
                    number_of_biopsies,
                    biopsy_type,
                    prev_exam_results,
                    fam_brca_history_bf50,
                    fam_brca_history_aft50,
                    fam_male_brca_history,
                    fam_ovarianca_history,
                    fam_relatedca_history,
                    fam_otherca_specify,
                    cur_physical_findings,
                    lymph_nodes_findings,
                    cur_screening_findings,
                    #cur_screening_findings_date,
                    patient_education,
                    patient_education_other,
                    referrals_ordered,
                    referred_date,
                    procedure_done,
                    next_app_date,
                    cbe_imaging_concordance,
                    mammogram_workup_date,
                    date_patient_notified_of_mammogram_results,
                    ultrasound_results,
                    ultrasound_workup_date,
                    date_patient_notified_of_ultrasound_results,
                    fna_results,
                    fna_tumor_size,
                    fna_degree_of_malignancy,
                    fna_workup_date,
                    date_patient_notified_of_fna_results,
                    biopsy_tumor_size,
                    biopsy_degree_of_malignancy,
                    biopsy_workup_date,
                    date_patient_notified_of_biopsy_results,
                    biopsy_description,
                    diagnosis_date,
                    date_patient_informed_and_referred_for_management,
                    screening_mode,
                    diagnosis,
                    cancer_staging,
                    hiv_status,
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
                FROM 
                    flat_breast_cancer_screening_3 t1
                        JOIN 
                    amrs.location t2 USING (location_id))'
          );

          PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
          DEALLOCATE PREPARE s1;


          SET @dyn_sql=CONCAT('DELETE t1 from ',@queue_table,' t1 JOIN flat_breast_cancer_screening_build_queue__0 t2 USING (person_id);'); 
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