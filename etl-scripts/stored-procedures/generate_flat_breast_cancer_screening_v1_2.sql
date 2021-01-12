DELIMITER $$
CREATE PROCEDURE `generate_flat_breast_cancer_screening_v1_2`(IN query_type VARCHAR(50), IN queue_number INT, IN queue_size INT, IN cycle_size INT)
BEGIN
    -- 
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
        location_uuid varchar(100),
        gender char(100),
        age tinyint,
        encounter_purpose tinyint,
        menstruation_before_12 tinyint,
        menses_stopped_permanently tinyint,
        menses_stop_age tinyint,
        hrt_use tinyint,
        hrt_use_years tinyint,
        hrt_type_used tinyint,
        ever_given_birth tinyint,
        age_at_birth_of_first_child tinyint,
        gravida tinyint,
        parity tinyint,
        cigarette_smoking tinyint,
        cigarette_sticks_smoked_per_day tinyint,
        tobacco_use tinyint,
        tobacco_use_duration_in_years tinyint,
        alcohol_consumption tinyint,
        alcohol_type_used tinyint,
        alcohol_use_duration_in_years tinyint,
        hiv_status tinyint,
        presence_of_chief_complaint tinyint,
        history_of_clinical_breast_examination tinyint,
        past_clinical_breast_exam_findings varchar(100),
        history_of_mammogram tinyint,
        past_mammogram_results tinyint,
        history_of_breast_ultrasound tinyint,
        past_breast_ultrasound_results tinyint,
        history_of_breast_biopsy tinyint,
        number_of_biopsies_done tinyint,
        breast_biopsy_type varchar(200),
        past_breast_biopsy_results tinyint,
        history_of_radiation_treatment_to_the_chest tinyint,
        past_breast_surgery_for_non_brca_reasons tinyint,
        which_other_breast_surgery varchar(100),
        any_family_member_diagnosed_with_ca_breast int,
        breast_findings_this_visit varchar(150),
        breast_finding_location varchar(150),
        breast_finding_quadrant varchar(150),
        breast_symmetry tinyint,
        lymph_node_findings tinyint,
        axillary_lymph_nodes_location tinyint,
        clavicular_supra_location tinyint,
        clavicular_infra_location tinyint,
        patient_education varchar(200),
        referrals_ordered tinyint,
        referral_date datetime,
        next_appointment_date datetime,
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
        index location_id_rtc_date (location_id,next_appointment_date),
        index location_uuid_rtc_date (location_uuid,next_appointment_date),
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
        SET @menstruation_before_12 := null;
        SET @menses_stopped_permanently := null;
        SET @menses_stop_age := null;
        SET @hrt_use := null;
        SET @hrt_use_years := null;
        SET @hrt_type_used := null;
        SET @ever_given_birth := null;
        SET @age_at_birth_of_first_child := null;
        SET @gravida := null;
        SET @parity := null;
        SET @cigarette_smoking := null;
        SET @cigarette_sticks_smoked_per_day := null;
        SET @tobacco_use := null;
        SET @tobacco_use_duration_in_years := null;
        SET @alcohol_consumption := null;
        SET @alcohol_type_used := null;
        SET @alcohol_use_duration_in_years := null;
        SET @hiv_status := null;
        SET @presence_of_chief_complaint := null;
        SET @history_of_clinical_breast_examination := null;
        SET @past_clinical_breast_exam_findings := null;
        SET @history_of_mammogram := null;
        SET @past_mammogram_results := null;
        SET @history_of_breast_ultrasound := null;
        SET @past_breast_ultrasound_results := null;
        SET @history_of_breast_biopsy := null;
        SET @number_of_biopsies_done := null;
        SET @breast_biopsy_type := null;
        SET @past_breast_biopsy_results := null;
        SET @history_of_radiation_treatment_to_the_chest := null;
        SET @past_breast_surgery_for_non_brca_reasons := null;
        SET @which_other_breast_surgery := null;
        SET @any_family_member_diagnosed_with_ca_breast := null;
        SET @breast_findings_this_visit := null;
        SET @breast_finding_location := null;
        SET @breast_finding_quadrant := null;
        SET @breast_symmetry := null;
        SET @lymph_node_findings := null;
        SET @axillary_lymph_nodes_location := null;
        SET @clavicular_supra_location := null;
        SET @clavicular_infra_location := null;
        SET @patient_education := null;
        SET @referrals_ordered := null;
        SET @referral_date := null;
        SET @next_appointment_date := null;

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
					when t1.encounter_type = 86 and obs regexp "!!1834=9651!!" then @encounter_purpose := 1
					when t1.encounter_type = 86 and obs regexp "!!1834=1154!!" then @encounter_purpose := 2
					when t1.encounter_type = 86 and obs regexp "!!1834=1246!!" then @encounter_purpose := 3
					when t1.encounter_type = 86 and obs regexp "!!1834=5622!!" then @encounter_purpose := 4
					else @encounter_purpose := null
				end as encounter_purpose,
				case
				  when t1.encounter_type = 86 and obs regexp "!!9560=1066!!" then @menstruation_before_12 := 0
				  when t1.encounter_type = 86 and obs regexp "!!9560=1065!!" then @menstruation_before_12 := 1
				  else @menstruation_before_12 := null
				end as menstruation_before_12,                           
				case
					when t1.encounter_type = 86 and obs regexp "!!9561=1066!!" then @menses_stopped_permanently := 0
					when t1.encounter_type = 86 and obs regexp "!!9561=1065!!" then @menses_stopped_permanently := 1
					when t1.encounter_type = 86 and obs regexp "!!9561=9568!!" then @menses_stopped_permanently := 2
					else @menses_stopped_permanently := null
				end as menses_stopped_permanently,
				case
					when t1.encounter_type = 86 and obs regexp "!!9562=" and t1.encounter_type = @lab_encounter_type then @menses_stop_age := GetValues(obs, 9562)
					when @prev_id = @cur_id then @menses_stop_age
					else @menses_stop_age := null
				end as menses_stop_age,
				case
					when t1.encounter_type = 86 and obs regexp "!!9626=1066!!" then @hrt_use := 0
					when t1.encounter_type = 86 and obs regexp "!!9626=1065!!" then @hrt_use := 1
					when t1.encounter_type = 86 and obs regexp "!!9626=9568!!" then @hrt_use := 2
					else @hrt_use := null
				end as hrt_use,
				case
					when t1.encounter_type = 86 and obs regexp "!!9629=" then @hrt_use_years := GetValues(obs, 9629) 
					else @hrt_use_years := null
				end as hrt_use_years,
				case
					when t1.encounter_type = 86 and obs regexp "!!9630=9573!!" then @hrt_type_used := 1
					when t1.encounter_type = 86 and obs regexp "!!9630=6217!!" then @hrt_type_used := 2
					when t1.encounter_type = 86 and obs regexp "!!9630=6218!!" then @hrt_type_used := 3
					else @hrt_type_used := null
				end as hrt_type_used,
        case
					when t1.encounter_type = 86 and obs regexp "!!9563=1066!!" then @ever_given_birth := 0
					when t1.encounter_type = 86 and obs regexp "!!9563=1065!!" then @ever_given_birth := 1
					else @ever_given_birth := null
				end as ever_given_birth,
				case
					when t1.encounter_type = 86 and obs regexp "!!9564=" then @age_at_birth_of_first_child := GetValues(obs, 9564)
					else @age_at_birth_of_first_child := null
				end as age_at_birth_of_first_child,
				case
					when t1.encounter_type = 86 and obs regexp "!!5624=" then @gravida := GetValues(obs, 5624)
					else @gravida := null
				end as gravida,
				case
					when t1.encounter_type = 86 and obs regexp "!!1053=" then @parity := GetValues(obs, 1053)
					else @parity := null
				end as parity,
        case
					when t1.encounter_type = 86 and obs regexp "!!2065=1066!!" then @cigarette_smoking := 0 -- No
					when t1.encounter_type = 86 and obs regexp "!!2065=1065!!" then @cigarette_smoking := 1 -- Yes
					when t1.encounter_type = 86 and obs regexp "!!2065=1679!!" then @cigarette_smoking := 2 -- Stopped
					else @cigarette_smoking := null
				end as cigarette_smoking,
				case
					when t1.encounter_type = 86 and obs regexp "!!2069=" then @cigarette_sticks_smoked_per_day := GetValues(obs, 2069) 
					else @cigarette_sticks_smoked_per_day := null
				end as cigarette_sticks_smoked_per_day,
				case
					when t1.encounter_type = 86 and obs regexp "!!7973=1066!!" then @tobacco_use := 0 -- No
					when t1.encounter_type = 86 and obs regexp "!!7973=1065!!" then @tobacco_use := 1 -- Yes
					when t1.encounter_type = 86 and obs regexp "!!7973=1679!!" then @tobacco_use := 2 -- Stopped
					else @tobacco_use := null
				end as tobacco_use,
        case
					when t1.encounter_type = 86 and obs regexp "!!8144=" then @tobacco_use_duration_in_years := GetValues(obs, 8144) 
					else @tobacco_use_duration_in_years := null
				end as tobacco_use_duration_in_years,
				case
					when t1.encounter_type = 86 and obs regexp "!!1684=1066!!" then @alcohol_consumption := 0 -- No
					when t1.encounter_type = 86 and obs regexp "!!1684=1065!!" then @alcohol_consumption := 1 -- Yes
					when t1.encounter_type = 86 and obs regexp "!!1684=1679!!" then @alcohol_consumption := 2 -- Stopped
					else @alcohol_consumption := null
				end as alcohol_consumption,
        case
					when t1.encounter_type = 86 and obs regexp "!!1685=1682!!" then @alcohol_type := 1 -- Chang'aa
					when t1.encounter_type = 86 and obs regexp "!!1685=1681!!" then @alcohol_type := 2 -- Liquor
					when t1.encounter_type = 86 and obs regexp "!!1685=1680!!" then @alcohol_type := 3 -- Beer
					when t1.encounter_type = 86 and obs regexp "!!1685=1683!!" then @alcohol_type := 4 -- Busaa
					when t1.encounter_type = 86 and obs regexp "!!1685=2059!!" then @alcohol_type := 5 -- Wine
					when t1.encounter_type = 86 and obs regexp "!!1685=5622!!" then @alcohol_type := 6 -- Other (non-coded)
        end as alcohol_type_used,
        case
          when t1.encounter_type = 86 and obs regexp "!!8170=" then @alcohol_use_duration_in_years := GetValues(obs, 8170)
        end as alcohol_use_duration_in_years,
        case
					when t1.encounter_type = 86 and obs regexp "!!6709=664!!" then @hiv_status := 1 -- Negative
					when t1.encounter_type = 86 and obs regexp "!!6709=703!!" then @hiv_status := 2 -- Positive
					when t1.encounter_type = 86 and obs regexp "!!6709=1067!!" then @hiv_status := 3 -- Unknown
					else @hiv_status := null
				end as hiv_status,
        case
          when t1.encounter_type = 86 and obs regexp "!!9553=5006!!" then @presence_of_chief_complaint := 1 -- Asymptomatic
					when t1.encounter_type = 86 and obs regexp "!!9553=1068!!" then @presence_of_chief_complaint := 2 -- Symptomatic
					else @presence_of_chief_complaint := null
        end as presence_of_chief_complaint,
        case
          when t1.encounter_type = 86 and obs regexp "!!9559=1066!!" then @history_of_clinical_breast_examination := 0 -- No
          when t1.encounter_type = 86 and obs regexp "!!9559=1065!!" then @history_of_clinical_breast_examination := 1 -- Yes
          when t1.encounter_type = 86 and obs regexp "!!9559=9568!!" then @history_of_clinical_breast_examination := 2 -- Not sure
          else @history_of_clinical_breast_examination := null
        end as history_of_clinical_breast_examination,
        case
          -- workaround necessary because this question and `breast_findings_this_visit` incorrectly share the same concept
          when t1.encounter_type = 86 and obs regexp "!!9559=1065!!" and obs regexp "!!6251=" then @past_clinical_breast_exam_findings := SUBSTRING(GetValues(obs, 6251), 1, 4)
          else @past_clinical_breast_exam_findings := null
        end as past_clinical_breast_exam_findings,
				case
					when t1.encounter_type = 86 and obs regexp "!!9594=1066!!" then @history_of_mammogram := 0 -- No
					when t1.encounter_type = 86 and obs regexp "!!9594=1065!!" then @history_of_mammogram := 1 -- Yes
					else @history_of_mammogram := null
				end as history_of_mammogram,
        case
					when t1.encounter_type = 86 and obs regexp "!!9595=1115!!" then @past_mammogram_results := 1 -- Normal
					when t1.encounter_type = 86 and obs regexp "!!9595=1116!!" then @past_mammogram_results := 2 -- Abnormal
					when t1.encounter_type = 86 and obs regexp "!!9595=1118!!" then @past_mammogram_results := 3 -- Not done
					when t1.encounter_type = 86 and obs regexp "!!9595=1138!!" then @past_mammogram_results := 4 -- Indeterminate 
					when t1.encounter_type = 86 and obs regexp "!!9595=1267!!" then @past_mammogram_results := 5 -- Completed
					when t1.encounter_type = 86 and obs regexp "!!9595=1067!!" then @past_mammogram_results := 6 -- Unknown
				  else @past_mammogram_results := null
				end as past_mammogram_results,
        case
					when t1.encounter_type = 86 and obs regexp "!!9597=1066!!" then @history_of_breast_ultrasound := 0 -- No
					when t1.encounter_type = 86 and obs regexp "!!9597=1065!!" then @history_of_breast_ultrasound := 1 -- Yes
					else @history_of_breast_ultrasound := null
				end as history_of_breast_ultrasound,
        case
					when t1.encounter_type = 86 and obs regexp "!!9596=1115!!" then @past_breast_ultrasound_results := 1 -- Normal
					when t1.encounter_type = 86 and obs regexp "!!9596=1116!!" then @past_breast_ultrasound_results := 2 -- Abnormal
					when t1.encounter_type = 86 and obs regexp "!!9596=1067!!" then @past_breast_ultrasound_results := 3 -- Unknown
					when t1.encounter_type = 86 and obs regexp "!!9596=1118!!" then @past_breast_ultrasound_results := 4 -- Not done
					else @past_breast_ultrasound_results := null
				end as past_breast_ultrasound_results,
        case
					when t1.encounter_type = 86 and obs regexp "!!9598=1066!!" then @history_of_breast_biopsy := 0 -- No
					when t1.encounter_type = 86 and obs regexp "!!9598=1065!!" then @history_of_breast_biopsy := 1 -- Yes
					else @history_of_breast_biopsy := null
				end as history_of_breast_biopsy,
        case
					when t1.encounter_type = 86 and obs regexp "!!9599=" then @number_of_biopsies_done := GetValues(obs, 9599)
					else @number_of_biopsies_done := null
				end as number_of_biopsies_done,
        case
          when t1.encounter_type = 86 and obs regexp "!!6509=" then @breast_biopsy_type := GetValues(obs, 6509)
					else @breast_biopsy_type := null             
				end as breast_biopsy_type,
        case
					when t1.encounter_type = 86 and obs regexp "!!8184=1115!!" then @past_breast_biopsy_results := 1 -- Normal
					when t1.encounter_type = 86 and obs regexp "!!8184=1116!!" then @past_breast_biopsy_results := 2 -- Abnormal
					when t1.encounter_type = 86 and obs regexp "!!8184=1118!!" then @past_breast_biopsy_results := 3 -- Not done
					when t1.encounter_type = 86 and obs regexp "!!8184=1067!!" then @past_breast_biopsy_results := 4 -- Unknown
					when t1.encounter_type = 86 and obs regexp "!!8184=9691!!" then @past_breast_biopsy_results := 5 -- Benign
					when t1.encounter_type = 86 and obs regexp "!!8184=10052!!" then @past_breast_biopsy_results := 6 -- Malignant
					else @past_breast_biopsy_results := null
				end as past_breast_biopsy_results,
        case
          when t1.encounter_type = 86 and obs regexp "!!9566=1066!!" then @history_of_radiation_treatment_to_the_chest := 0 -- No
          when t1.encounter_type = 86 and obs regexp "!!9566=1065!!" then @history_of_radiation_treatment_to_the_chest := 1 -- Yes
          when t1.encounter_type = 86 and obs regexp "!!9566=1679!!" then @history_of_radiation_treatment_to_the_chest := 2 -- Not sure
          else @history_of_radiation_treatment_to_the_chest := null
        end as history_of_radiation_treatment_to_the_chest,
        -- reason_for_radiation_treatment -- freetext field
        case
          when t1.encounter_type = 86 and obs regexp "!!9644=1066!!" then @past_breast_surgery_for_non_brca_reasons := 0 -- No
          when t1.encounter_type = 86 and obs regexp "!!9644=1065!!" then @past_breast_surgery_for_non_brca_reasons := 1 -- Yes
          when t1.encounter_type = 86 and obs regexp "!!9644=1679!!" then @past_breast_surgery_for_non_brca_reasons := 2 -- Not sure
          else @past_breast_surgery_for_non_brca_reasons := null
        end as past_breast_surgery_for_non_brca_reasons,
        case 
          when t1.encounter_type = 86 and obs regexp "!!9566=1065!!" and obs regexp "!!9649=" then @which_other_breast_surgery := GetValues(obs, 9649) 
        end as which_other_breast_surgery,
        case
          when t1.encounter_type = 86 and obs regexp "!!10172=1066!!" then @any_family_member_diagnosed_with_ca_breast := 0 -- No
          when t1.encounter_type = 86 and obs regexp "!!10172=1065!!" then @any_family_member_diagnosed_with_ca_breast := 1 -- Yes
          when t1.encounter_type = 86 and obs regexp "!!10172=1067!!" then @any_family_member_diagnosed_with_ca_breast := 2 -- Unknown
          else @any_family_member_diagnosed_with_ca_breast := null
        end as any_family_member_diagnosed_with_ca_breast,          
				case
					when t1.encounter_type = 86 and obs regexp "!!6251=" then @breast_findings_this_visit := SUBSTRING(GetValues(obs, 6251), -4)
					else @breast_findings_this_visit := null
				end as breast_findings_this_visit,
        case
          when t1.encounter_type = 86 and obs regexp "!!9696=" then @breast_finding_location := GetValues(obs, 9696)
          else @breast_finding_location := null
        end as breast_finding_location,
        case
          when t1.encounter_type = 86 and obs regexp "!!8268=" then @breast_finding_quadrant := GetValues(obs, 8268)
          else @breast_finding_quadrant := null
        end as breast_finding_quadrant,
        case
          when t1.encounter_type = 86 and obs regexp "!!9681=1066!!" then @breast_symmetry := 0 -- No
          when t1.encounter_type = 86 and obs regexp "!!9681=1065!!" then @breast_symmetry := 1 -- Yes
          else @breast_symmetry := null
        end as breast_symmetry,
        case
          when t1.encounter_type = 86 and obs regexp "!!1121=1115!!" then @lymph_node_findings := 1 -- Within normal limit
          when t1.encounter_type = 86 and obs regexp "!!1121=161!!" then @lymph_node_findings := 2 -- Enlarged / Lymphadenopathy
          when t1.encounter_type = 86 and obs regexp "!!1121=9675!!" then @lymph_node_findings := 3 -- Fixed
          when t1.encounter_type = 86 and obs regexp "!!1121=9676!!" then @lymph_node_findings := 4 -- Mobile
          else @lymph_node_findings := null
        end as lymph_node_findings,
        case
          when t1.encounter_type = 86 and obs regexp "!!9678=5141!!" then @axillary_lymph_nodes_location := 1 -- Right
          when t1.encounter_type = 86 and obs regexp "!!9678=5139!!" then @axillary_lymph_nodes_location := 2 -- Left
          else @axillary_lymph_nodes_location := null
        end as axillary_lymph_nodes_location,
        case
          when t1.encounter_type = 86 and obs regexp "!!9679=5141!!" then @clavicular_supra_location := 1 -- Right
          when t1.encounter_type = 86 and obs regexp "!!9679=5139!!" then @clavicular_supra_location := 2 -- Left
          else @clavicular_supra_location := null
        end as clavicular_supra_location,
        case
          when t1.encounter_type = 86 and obs regexp "!!9680=5141!!" then @clavicular_infra_location := 1 -- Right
          when t1.encounter_type = 86 and obs regexp "!!9680=5139!!" then @clavicular_infra_location := 2 -- Left
          else @clavicular_infra_location := null
        end as clavicular_infra_location,
        case
					when t1.encounter_type = 86 and obs regexp "!!6327=" then @patient_education := GetValues(obs, 6327)
					else @patient_education := null
				end as patient_education,
        case
					when t1.encounter_type = 86 and obs regexp "!!1272=1107!!" then @referrals_ordered := 0
					when t1.encounter_type = 86 and obs regexp "!!1272=1496!!" then @referrals_ordered := 1
					else @referrals_ordered := null
				end as referrals_ordered,
        case
          when t1.encounter_type = 86 and obs regexp "!!1272=1496!!" and obs regexp "!!9158=" then @referral_date := GetValues(obs, 9158)
          else @referral_date := null
        end as referral_date,
				case
					when t1.encounter_type = 86 and obs regexp "!!5096=" then @next_appointment_date := GetValues(obs, 5096) 
					else @next_appointment_date := null
				end as next_appointment_date
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
              when is_clinical_encounter then @cur_clinical_rtc_date := next_appointment_date
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
              when is_clinical_encounter then @cur_clinical_rtc_date := next_appointment_date
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
                    menstruation_before_12,
                    menses_stopped_permanently,
                    menses_stop_age,
                    hrt_use,
                    hrt_use_years,
                    hrt_type_used,
                    ever_given_birth,
                    age_at_birth_of_first_child,
                    gravida,
                    parity,
                    cigarette_smoking,
                    cigarette_sticks_smoked_per_day,
                    tobacco_use,
                    tobacco_use_duration_in_years,
                    alcohol_consumption,
                    alcohol_type_used,
                    alcohol_use_duration_in_years,
                    hiv_status,
                    presence_of_chief_complaint,
                    history_of_clinical_breast_examination,
                    past_clinical_breast_exam_findings,
                    history_of_mammogram,
                    past_mammogram_results,
                    history_of_breast_ultrasound,
                    past_breast_ultrasound_results,
                    history_of_breast_biopsy,
                    number_of_biopsies_done,
                    breast_biopsy_type,
                    past_breast_biopsy_results,
                    history_of_radiation_treatment_to_the_chest,
                    past_breast_surgery_for_non_brca_reasons,
                    which_other_breast_surgery,
                    any_family_member_diagnosed_with_ca_breast,
                    breast_findings_this_visit,
                    breast_finding_location,
                    breast_finding_quadrant,
                    breast_symmetry,
                    lymph_node_findings,
                    axillary_lymph_nodes_location,
                    clavicular_supra_location,
                    clavicular_infra_location,
                    patient_education,
                    referrals_ordered,
                    referral_date,
                    next_appointment_date,
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
