CREATE PROCEDURE `generate_flat_cervical_cancer_screening_v1_1`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
    -- Rename `Mensturation_status` to `mensturation_status`, `Pap_smear_results` to `pap_smear_results`, `Cervix_biopsy_results` to `cervix_biopsy_results`, 
    --    `Vagina_biopsy_results` to `vagina_biopsy_results`, `Vulva_biopsy_results` to `vulva_biopsy_results` 
    -- Add new columns:
    --    biopsy_workup_date
    --    date_patient_notified
    --    diagnosis_date
    SET @primary_table := "flat_cervical_cancer_screening";
    SET @query_type := query_type;

    SET @total_rows_written := 0;
    
    SET @encounter_types := "(69, 70, 147)";
    SET @clinical_encounter_types := "(69, 70, 147)";
    SET @non_clinical_encounter_types := "(-1)";
    SET @other_encounter_types := "(-1)";
              
    SET @start := now();
    SET @table_version := "flat_cervical_cancer_screening_v1.1";

    SET session sort_buffer_size := 512000000;

    SET @sep := " ## ";
    SET @boundary := "!!";
    SET @last_date_created = (SELECT max(max_date_created) FROM etl.flat_obs);

    CREATE TABLE IF NOT EXISTS flat_cervical_cancer_screening (
        date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        person_id INT,
        encounter_id INT,
        encounter_type INT,
        encounter_datetime DATETIME,
        visit_id INT,
        location_id INT,
        location_uuid VARCHAR(100),
        uuid VARCHAR(100),
        age INT,
        encounter_purpose INT,
        cur_visit_type INT,
        actual_scheduled_date DATETIME,
        gravida INT,
        parity INT,
        menstruation_status INT,
        lmp DATETIME,
        pregnancy_status INT,
        pregnant_edd DATETIME,
        reason_not_pregnant INT,
        hiv_status INT,
        viral_load FLOAT,
        viral_load_date DATETIME,
        prior_via_done INT,
        prior_via_result INT,
        prior_via_date DATETIME,
        cur_via_result INT,
        visual_impression_cervix INT,
        visual_impression_vagina INT,
        visual_impression_vulva INT,
        via_procedure_done INT,
        other_via_procedure_done VARCHAR(1000),
        via_management_plan INT,
        other_via_management_plan VARCHAR(1000),
        via_assessment_notes TEXT,
        via_rtc_date DATETIME,
        prior_dysplasia_done INT,
        previous_via_result INT,
        previous_via_result_date DATETIME,
        prior_papsmear_result INT,
        prior_biopsy_result INT,
        prior_biopsy_result_date DATETIME,
        prior_biopsy_result_other VARCHAR(1000),
        prior_biopsy_result_date_other DATETIME,
        date_patient_informed_referred DATETIME,
        past_dysplasia_treatment INT,
        treatment_specimen_pathology INT,
        satisfactory_colposcopy INT,
        colposcopy_findings INT,
        cervica_lesion_size INT,
        dysplasia_cervix_impression INT,
        dysplasia_vagina_impression INT,
        dysplasia_vulva_impression INT,
        dysplasia_procedure_done INT,
        other_dysplasia_procedure_done VARCHAR(1000),
        dysplasia_mgmt_plan INT,
        other_dysplasia_mgmt_plan VARCHAR(1000),
        dysplasia_assesment_notes TEXT,
        dysplasia_rtc_date DATETIME,
        pap_smear_results INT,
        biopsy_workup_date DATETIME,
        date_patient_notified DATETIME,
        diagnosis_date DATETIME,
        leep_location INT,
        cervix_biopsy_results INT,
        vagina_biopsy_results INT,
        vulva_biopsy_result INT,
        endometrium_biopsy INT,
        ECC INT,
        biopsy_results_mngmt_plan INT,
        cancer_staging INT,
        next_app_date DATETIME,
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
        INDEX location_id_rtc_date (location_id , next_app_date),
        INDEX location_uuid_rtc_date (location_uuid , next_app_date),
        INDEX loc_id_enc_date_next_clinical (location_id , encounter_datetime , next_clinical_datetime_cervical_cancer_screening),
        INDEX encounter_type (encounter_type),
        INDEX date_created (date_created)
    );
                        
		IF (@query_type = "build") THEN					
		    SELECT 'BUILDING..........................................';
                  												
        SET @write_table := CONCAT("flat_cervical_cancer_screening_temp_", queue_number);
        SET @queue_table := CONCAT("flat_cervical_cancer_screening_build_queue_", queue_number);                    												
												
        SET @dyn_sql := CONCAT('CREATE TABLE IF NOT EXISTS ', @write_table,' like ', @primary_table);
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
	
					
		IF (@query_type = 'sync') THEN
        SELECT 'SYNCING..........................................';
        
        SET @write_table := "flat_cervical_cancer_screening";
        SET @queue_table := "flat_cervical_cancer_screening_sync_queue";

        CREATE TABLE IF NOT EXISTS flat_cervical_cancer_screening_sync_queue (person_id INT PRIMARY KEY);
                            
				SET @last_update := null;

        SELECT MAX(date_updated) INTO @last_update FROM etl.flat_log WHERE table_name = @table_version;

        SELECT 'Finding patients in amrs.encounters...';
            REPLACE INTO flat_cervical_cancer_screening_sync_queue
            (SELECT DISTINCT 
                patient_id
              FROM 
                amrs.encounter
              where 
                date_changed > @last_update
            );

        SELECT 'Finding patients in flat_obs...';
            REPLACE INTO flat_cervical_cancer_screening_sync_queue
            (SELECT DISTINCT
                person_id
              FROM 
                etl.flat_obs
              where 
                max_date_created > @last_update
            );

        SELECT 'Finding patients in flat_lab_obs...';
            REPLACE INTO flat_cervical_cancer_screening_sync_queue
            (SELECT DISTINCT
                person_id
              FROM 
                etl.flat_lab_obs
              where 
                max_date_created > @last_update
            );

        SELECT 'Finding patients in flat_orders...';
            REPLACE INTO flat_cervical_cancer_screening_sync_queue
            (SELECT DISTINCT
                person_id
              FROM 
                etl.flat_orders
              where
                max_date_created > @last_update
            );
                            
        REPLACE INTO flat_cervical_cancer_screening_sync_queue
            (SELECT
                person_id
              FROM 
                amrs.person 
						  where 
                date_voided > @last_update
            );


        REPLACE INTO flat_cervical_cancer_screening_sync_queue
            (SELECT
                person_id
              FROM 
							  amrs.person 
							where 
                date_changed > @last_update
            );
    END IF;

		-- Remove test patients
		SET @dyn_sql := CONCAT('delete t1 FROM ',@queue_table,' t1
        JOIN amrs.person_attribute t2 using (person_id)
        where t2.person_attribute_type_id=28 and value="true" and voided=0');
		PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  

    SET @person_ids_count := 0;
    SET @dyn_sql=CONCAT('SELECT count(*) into @person_ids_count FROM ',@queue_table); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;

    SELECT @person_ids_count AS 'num patients to update';

    SET @dyn_sql=CONCAT('delete t1 FROM ',@primary_table, ' t1 JOIN ',@queue_table,' t2 using (person_id);'); 
    PREPARE s1 FROM @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
                                        
    SET @total_time := 0;
    SET @cycle_number := 0;
                    

		WHILE @person_ids_count > 0 DO
        SET @loop_start_time = now();
                        
				-- Create temporary table with a set of person ids
				DROP TEMPORARY TABLE IF EXISTS flat_cervical_cancer_screening_build_queue__0;
        
        SET @dyn_sql=CONCAT('CREATE TEMPORARY TABLE flat_cervical_cancer_screening_build_queue__0 (person_id int primary key) (SELECT * FROM ',@queue_table,' limit ',cycle_size,');'); 
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
                    
        DROP TEMPORARY TABLE IF EXISTS flat_cervical_cancer_screening_0a;
        SET @dyn_sql := CONCAT(
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
            FROM 
              etl.flat_obs t1
            JOIN 
              flat_cervical_cancer_screening_build_queue__0 t0 using (person_id)
            LEFT JOIN
              etl.flat_orders t2 using(encounter_id)
            where t1.encounter_type in ',@encounter_types,');');
                            
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
          t1.obs,
          null,
          0 as is_clinical_encounter,
          1 as encounter_type_sort_index,
          null
          FROM etl.flat_lab_obs t1
            JOIN flat_cervical_cancer_screening_build_queue__0 t0 using (person_id)
        );


        DROP TEMPORARY TABLE IF EXISTS flat_cervical_cancer_screening_0;
        CREATE TEMPORARY TABLE flat_cervical_cancer_screening_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
        (SELECT
            * 
        FROM 
          flat_cervical_cancer_screening_0a
        ORDER BY
          person_id, date(encounter_datetime), encounter_type_sort_index
        );

        SET @prev_id := null;
        SET @cur_id := null;
        SET @encounter_purpose := null;
        SET @cur_visit_type := null;
        SET @actual_scheduled_date := null;
        SET @gravida := null;
        SET @parity := null;
        SET @menstruation_status := null;
        SET @lmp := null;
        SET @pregnancy_status := null;
        SET @pregnant_edd := null;
        SET @preason_not_pregnant := null;
        SET @hiv_status := null;
        SET @viral_load := null;
        SET @viral_load_date := null;
        SET @prior_via_done := null;
        SET @prior_via_result := null;
        SET @prior_via_date := null;
        SET @cur_via_result := null;
        SET @visual_impression_cervix := null;
        SET @visual_impression_vagina := null;
        SET @visual_impression_vulva := null;
        SET @via_procedure_done := null;
        SET @other_via_procedure_done := null;
        SET @via_management_plan := null;
        SET @other_via_management_plan := null;
        SET @via_assessment_notes := null;
        SET @via_rtc_date := null;
        SET @prior_dysplasia_done := null;
        SET @previous_via_result := null;
        SET @previous_via_result_date := null;
        SET @prior_papsmear_result := null;
				SET @prior_biopsy_result := null;
        SET @prior_biopsy_result_date := null;
        SET @prior_biopsy_result_other := null;
        SET @prior_biopsy_result_date_other := null;
        SET @date_patient_informed_referred := null;
        SET @past_dysplasia_treatment := null;
        SET @treatment_specimen_pathology := null;
        SET @satisfactory_colposcopy := null;
        SET @colposcopy_findings := null;
        SET @cervica_lesion_size := null;
        SET @dysplasia_cervix_impression := null;
        SET @dysplasia_vagina_impression := null;
        SET @dysplasia_vulva_impression := null;
        SET @dysplasia_procedure_done := null;
        SET @other_dysplasia_procedure_done := null;
        SET @dysplasia_mgmt_plan := null;
        SET @other_dysplasia_mgmt_plan := null;
        SET @dysplasia_assesment_notes := null;
        SET @dysplasia_rtc_date := null;
        SET @pap_smear_results := null;
        SET @biopsy_workup_date := null;
        SET @date_patient_notified := null;
        SET @diagnosis_date := null;
        SET @leep_location := null;
        SET @cervix_biopsy_results := null;
        SET @vagina_biopsy_results := null;
        SET @vulva_biopsy_result := null;
        SET @endometrium_biopsy := null;
        SET @ECC := null;
        SET @biopsy_results_mngmt_plan := null;
        SET @cancer_staging := null;
                                                
				DROP TEMPORARY TABLE IF EXISTS flat_cervical_cancer_screening_1;
				
        CREATE TEMPORARY TABLE flat_cervical_cancer_screening_1
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
            p.death_date,

						CASE
								WHEN timestampdiff(year, birthdate, curdate()) > 0 then round(timestampdiff(year, birthdate, curdate()),0)
								ELSE round(timestampdiff(month, birthdate, curdate())/12,2)
            END AS age,
							
            CASE
								WHEN obs regexp "!!1834=9651!!" then @encounter_purpose := 1
								WHEN obs regexp "!!1834=1154!!" then @encounter_purpose := 2
                WHEN obs regexp "!!1834=1246!!" then @encounter_purpose := 3
                WHEN obs regexp "!!1834=5622!!" then @encounter_purpose := 4
							  ELSE @encounter_purpose := null
					  END AS encounter_purpose,
            CASE
								WHEN obs regexp "!!1839=1838!!" then @cur_visit_type := 1
								WHEN obs regexp "!!1839=1837!!" then @cur_visit_type := 2
                WHEN obs regexp "!!1839=1246!!" then @cur_visit_type := 3
                WHEN obs regexp "!!1839=7850!!" then @cur_visit_type := 4
                WHEN obs regexp "!!1839=9569!!" then @cur_visit_type := 5
								ELSE @cur_visit_type := null
            END AS cur_visit_type,
            CASE
								WHEN obs regexp "!!7029=" then @actual_scheduled_date := replace(replace((substring_index(substring(obs,locate("!!7029=",obs)),@sep,1)),"!!7029=",""),"!!","")
								ELSE @actual_scheduled_date := null
							END AS actual_scheduled_date,
            CASE
								WHEN obs regexp "!!5624=[0-9]"  then @gravida := cast(replace(replace((substring_index(substring(obs,locate("!!5624=",obs)),@sep,1)),"!!5624=",""),"!!","") as unsigned)
								ELSE @gravida := null
							END AS gravida,
            CASE
								WHEN obs regexp "!!1053=[0-9]"  then @parity := cast(replace(replace((substring_index(substring(obs,locate("!!1053=",obs)),@sep,1)),"!!1053=",""),"!!","") as unsigned)
								ELSE @parity := null
							END AS parity,
            CASE
								WHEN obs regexp "!!2061=5989!!" then @menstruation_status := 1
                WHEN obs regexp "!!2061=6496!!" then @prev_exam_results := 2
								ELSE @menstruation_status := null
            END AS menstruation_status,
            CASE
								WHEN obs regexp "!!1836=" then @lmp := replace(replace((substring_index(substring(obs,locate("!!1836=",obs)),@sep,1)),"!!1836=",""),"!!","")
								ELSE @lmp := null
						END AS lmp,
            CASE
								WHEN obs regexp "!!8351=1065!!" then @pregnancy_status := 1
								WHEN obs regexp "!!8351=1066!!" then @pregnancy_status := 0
								ELSE @pregnancy_status := null
						END AS pregnancy_status, 
            CASE
								WHEN obs regexp "!!5596=" then @pregnant_edd := replace(replace((substring_index(substring(obs,locate("!!5596=",obs)),@sep,1)),"!!5596=",""),"!!","")
								ELSE @pregnant_edd := null
            END AS pregnant_edd,
            CASE
								WHEN obs regexp "!!9733=9729!!" then @reason_not_pregnant := 1
								WHEN obs regexp "!!9733=9730!!" then @reason_not_pregnant := 2
                WHEN obs regexp "!!9733=9731!!" then @reason_not_pregnant := 3
                WHEN obs regexp "!!9733=9732!!" then @reason_not_pregnant := 4
								ELSE @reason_not_pregnant := null
            END AS reason_not_pregnant,
            CASE
								WHEN obs regexp "!!6709=664!!" then @hiv_status := 1
								WHEN obs regexp "!!6709=703!!" then @hiv_status := 2
								WHEN obs regexp "!!6709=1067!!" then @hiv_status := 3
								ELSE @hiv_status := null
						END AS hiv_status,
            CASE
								WHEN obs regexp "!!856=[0-9]"  then @viral_load:=cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") as unsigned)
								ELSE @viral_load := null
            END AS viral_load,
            CASE
								WHEN obs regexp "!!000=" then @viral_load_date := replace(replace((substring_index(substring(obs,locate("!!000=",obs)),@sep,1)),"!!000=",""),"!!","")
								ELSE @viral_load_date := null
            END AS viral_load_date,
            CASE
								WHEN obs regexp "!!9589=1065!!" then @prior_via_done := 1
								WHEN obs regexp "!!9589=1066!!" then @prior_via_done := 0
								ELSE @prior_via_done := null
            END AS prior_via_done,
            CASE
								WHEN obs regexp "!!7381=664!!" then @prior_via_result := 1
								WHEN obs regexp "!!7381=703!!" then @prior_via_result := 2
								ELSE @prior_via_result := null
            END AS prior_via_result,
            CASE
								WHEN obs regexp "!!000=" then @prior_via_date := replace(replace((substring_index(substring(obs,locate("!!000=",obs)),@sep,1)),"!!000=",""),"!!","")
								ELSE @prior_via_date := null
            END AS prior_via_date,
            CASE
								WHEN obs regexp "!!9590=1115!!" then @cur_via_result := 1
								WHEN obs regexp "!!9590=7469!!" then @cur_via_result := 2
                WHEN obs regexp "!!9590=9593!!" then @cur_via_result := 3
                WHEN obs regexp "!!9590=7472!!" then @cur_via_result := 4
                WHEN obs regexp "!!9590=7293!!" then @cur_via_result := 5
                WHEN obs regexp "!!9590=7470!!" then @cur_via_result := 6
                WHEN obs regexp "!!9590=6497!!" then @cur_via_result := 7
                WHEN obs regexp "!!9590=5245!!" then @cur_via_result := 8
                WHEN obs regexp "!!9590=9591!!" then @cur_via_result := 9
                WHEN obs regexp "!!9590=9592!!" then @cur_via_result := 10
                WHEN obs regexp "!!9590=6497!!" then @cur_via_result := 11
								ELSE @cur_via_resul := null
						END AS cur_via_result,
            CASE
								WHEN obs regexp "!!7484=1115!!" then @visual_impression_cervix := 1
								WHEN obs regexp "!!7484=7507!!" then @visual_impression_cervix := 2
                WHEN obs regexp "!!7484=7508!!" then @visual_impression_cervix := 3
								ELSE @visual_impression_cervix := null
            END AS visual_impression_cervix,
            CASE
								WHEN obs regexp "!!7490=1115!!" then @visual_impression_vagina := 1
								WHEN obs regexp "!!7490=1447!!" then @visual_impression_vagina := 2
                WHEN obs regexp "!!7490=9181!!" then @visual_impression_vagina := 3
								ELSE @visual_impression_vagina := null
            END AS visual_impression_vagina,
            CASE
								WHEN obs regexp "!!7490=1115!!" then @visual_impression_vulva := 1
								WHEN obs regexp "!!7490=1447!!" then @visual_impression_vulva := 2
                WHEN obs regexp "!!7490=9177!!" then @visual_impression_vulva := 3
                ELSE @visual_impression_vulva := null
            END AS visual_impression_vulva,
            CASE
								WHEN obs regexp "!!7479=1107!!" then @via_procedure_done := 1
								WHEN obs regexp "!!7479=6511!!" then @via_procedure_done := 2
                WHEN obs regexp "!!7479=7466!!" then @via_procedure_done := 3
                WHEN obs regexp "!!7479=7147!!" then @via_procedure_done := 4
                WHEN obs regexp "!!7479=9724!!" then @via_procedure_done := 5
                WHEN obs regexp "!!7479=6510!!" then @via_procedure_done := 6
                WHEN obs regexp "!!7479=5622!!" then @via_procedure_done := 7
								ELSE @via_procedure_done := null
						END AS via_procedure_done,
            CASE
								WHEN obs regexp "!!1915=" then @other_via_procedure_done := replace(replace((substring_index(substring(obs,locate("!!1915=",obs)),@sep,1)),"!!1915=",""),"!!","")
								ELSE @other_via_procedure_done := null
						  	END AS other_via_procedure_done,
            CASE
								WHEN obs regexp "!!7500=9725!!" then @via_management_plan := 1
								WHEN obs regexp "!!7500=9178!!" then @via_management_plan := 2
                WHEN obs regexp "!!7500=7497!!" then @via_management_plan := 3
                WHEN obs regexp "!!7500=7383!!" then @via_management_plan := 4
                WHEN obs regexp "!!7500=7499!!" then @via_management_plan := 5
								WHEN obs regexp "!!7500=5622!!" then @via_management_plan := 6
								ELSE @via_management_plan := null
            END AS via_management_plan, 
            CASE
								WHEN obs regexp "!!1915=" then @other_via_management_plan := replace(replace((substring_index(substring(obs,locate("!!1915=",obs)),@sep,1)),"!!1915=",""),"!!","")
								ELSE @other_via_management_plan := null
            END AS other_via_management_plan,
            CASE
								WHEN obs regexp "!!7222=" then @via_assessment_notes := replace(replace((substring_index(substring(obs,locate("!!7222=",obs)),@sep,1)),"!!7222=",""),"!!","")
								ELSE @via_assessment_notes := null
            END AS via_assessment_notes,
            CASE
								WHEN obs regexp "!!5096=" then @via_rtc_date := replace(replace((substring_index(substring(obs,locate("!!5096=",obs)),@sep,1)),"!!5096=",""),"!!","")
								ELSE @via_rtc_date := null
						END AS via_rtc_date, 
            CASE
								WHEN obs regexp "!!7379=1065!!" then @prior_dysplasia_done := 1
								WHEN obs regexp "!!7379=1066!!" then @prior_dysplasia_done := 0
								ELSE @prior_dysplasia_done := null
            END AS prior_dysplasia_done,
            CASE
								WHEN obs regexp "!!7381=664!!" then @previous_via_result := 1
								WHEN obs regexp "!!7381=703!!" then @previous_via_result := 2
								ELSE @previous_via_result := null
						END AS previous_via_result,
            CASE
								WHEN obs regexp "!!000=" then @previous_via_result_date := replace(replace((substring_index(substring(obs,locate("!!000=",obs)),@sep,1)),"!!000=",""),"!!","")
								ELSE @previous_via_result_date := null
            END AS previous_via_result_date,
            CASE
								WHEN obs regexp "!!7423=1115!!" then @prior_papsmear_result := 1
								WHEN obs regexp "!!7423=7417!!" then @prior_papsmear_result := 2
                WHEN obs regexp "!!7423=7418!!" then @prior_papsmear_result := 3
                WHEN obs regexp "!!7423=7419!!" then @prior_papsmear_result := 4
                WHEN obs regexp "!!7423=7420!!" then @prior_papsmear_result := 5
								WHEN obs regexp "!!7423=7421!!" then @prior_papsmear_result := 6
                WHEN obs regexp "!!7423=7422!!" then @prior_papsmear_result := 7
								ELSE @prior_papsmear_result := null
            END AS prior_papsmear_result, 
            CASE
								WHEN obs regexp "!!7426=1115!!" then @prior_biopsy_result := 1
								WHEN obs regexp "!!7426=7424!!" then @prior_biopsy_result := 2
                WHEN obs regexp "!!7426=7425!!" then @prior_biopsy_result := 3
                WHEN obs regexp "!!7426=7216!!" then @prior_biopsy_result := 4
                WHEN obs regexp "!!7426=7421!!" then @prior_biopsy_result := 5
								ELSE @prior_biopsy_result := null
						END AS prior_biopsy_result, 
            CASE
								WHEN obs regexp "!!000=" then @prior_biopsy_result_date := replace(replace((substring_index(substring(obs,locate("!!000=",obs)),@sep,1)),"!!000=",""),"!!","")
								ELSE @prior_biopsy_result_date := null
            END AS prior_biopsy_result_date,
            CASE
								WHEN obs regexp "!!7400=" then @prior_biopsy_result_other := replace(replace((substring_index(substring(obs,locate("!!7400=",obs)),@sep,1)),"!!7400=",""),"!!","")
								ELSE @prior_biopsy_result_other := null
            END AS prior_biopsy_result_other,
            CASE
								WHEN obs regexp "!!000=" then @prior_biopsy_result_date_other := replace(replace((substring_index(substring(obs,locate("!!000=",obs)),@sep,1)),"!!000=",""),"!!","")
								ELSE @prior_biopsy_result_date_other := null
            END AS prior_biopsy_result_date_other,
            CASE
								WHEN obs regexp "!!9706=" then @date_patient_informed_referred := replace(replace((substring_index(substring(obs,locate("!!9706=",obs)),@sep,1)),"!!9706=",""),"!!","")
								ELSE @date_patient_informed_referred := null
            END AS date_patient_informed_referred,
            CASE
								WHEN obs regexp "!!7467=7466!!" then @past_dysplasia_treatment := 1
								WHEN obs regexp "!!7467=7147!!" then @past_dysplasia_treatment := 2
                WHEN obs regexp "!!7467=7465!!" then @past_dysplasia_treatment := 3
                WHEN obs regexp "!!7467=5622!!" then @past_dysplasia_treatment := 4
                ELSE @past_dysplasia_treatment := null
            END AS past_dysplasia_treatment, 
            CASE
								WHEN obs regexp "!!7579=1115!!" then @treatment_specimen_pathology := 1
								WHEN obs regexp "!!7579=149!!" then @treatment_specimen_pathology := 2
                WHEN obs regexp "!!7579=9620!!" then @treatment_specimen_pathology := 3
                WHEN obs regexp "!!7579=7424!!" then @treatment_specimen_pathology := 4
                WHEN obs regexp "!!7579=7425!!" then @treatment_specimen_pathology := 5
                WHEN obs regexp "!!7579=7216!!" then @treatment_specimen_pathology := 6
                WHEN obs regexp "!!7579=7421!!" then @treatment_specimen_pathology := 7
                WHEN obs regexp "!!7579=9618!!" then @treatment_specimen_pathology := 8
								ELSE @treatment_specimen_pathology := null
            END AS treatment_specimen_pathology, 
            CASE
								WHEN obs regexp "!!7428=1065!!" then @satisfactory_colposcopy := 1
								WHEN obs regexp "!!7428=1066!!" then @satisfactory_colposcopy := 0
                WHEN obs regexp "!!7428=1118!!" then @satisfactory_colposcopy := 2
								ELSE @satisfactory_colposcopy := null
            END AS satisfactory_colposcopy, 
            CASE
								WHEN obs regexp "!!7383=1115!!" then @colposcopy_findings := 1
								WHEN obs regexp "!!7383=7469!!" then @colposcopy_findings := 2
                WHEN obs regexp "!!7383=7473!!" then @colposcopy_findings := 3
                WHEN obs regexp "!!7383=7470!!" then @colposcopy_findings := 4
                WHEN obs regexp "!!7383=7471!!" then @colposcopy_findings := 5
                WHEN obs regexp "!!7383=7472!!" then @colposcopy_findings := 6
                ELSE @colposcopy_findings := null
						END AS colposcopy_findings, 
            CASE
								WHEN obs regexp "!!7477=7474!!" then @cervica_lesion_size := 1
								WHEN obs regexp "!!7477=9619!!" then @cervica_lesion_size := 2
                WHEN obs regexp "!!7477=7476!!" then @cervica_lesion_size := 3
								ELSE @cervica_lesion_size := null
            END AS cervica_lesion_size, 
            CASE
								WHEN obs regexp "!!7484=1115!!" then @dysplasia_cervix_impression := 1
								WHEN obs regexp "!!7484=7424!!" then @dysplasia_cervix_impression := 2
                WHEN obs regexp "!!7484=7425!!" then @dysplasia_cervix_impression := 3
                WHEN obs regexp "!!7484=7216!!" then @dysplasia_cervix_impression := 4
                WHEN obs regexp "!!7484=7421!!" then @dysplasia_cervix_impression := 5
                ELSE @dysplasia_cervix_impression := null
						END AS dysplasia_cervix_impression, 
            CASE
								WHEN obs regexp "!!7490=1115!!" then @dysplasia_vagina_impression := 1
								WHEN obs regexp "!!7490=1447!!" then @dysplasia_vagina_impression := 2
                WHEN obs regexp "!!7490=9177!!" then @dysplasia_vagina_impression := 3
								ELSE @dysplasia_vagina_impression := null
            END AS dysplasia_vagina_impression, 
            CASE
								WHEN obs regexp "!!7487=1115!!" then @dysplasia_vulva_impression := 1
								WHEN obs regexp "!!7487=1447!!" then @dysplasia_vulva_impression := 2
                WHEN obs regexp "!!7487=9177!!" then @dysplasia_vulva_impression := 3
								ELSE @dysplasia_vulva_impression := null
            END AS dysplasia_vulva_impression, 
            CASE
								WHEN obs regexp "!!7479=1107!!" then @dysplasia_procedure_done := 1
								WHEN obs regexp "!!7479=6511!!"  then @dysplasia_procedure_done := 2
                WHEN obs regexp "!!7479=7466!!" then @dysplasia_procedure_done := 3
                WHEN obs regexp "!!7479=7147!!" then @dysplasia_procedure_done := 4
                WHEN obs regexp "!!7479=9724!!" then @dysplasia_procedure_done := 5
                WHEN obs regexp "!!7479=6510!!" then @dysplasia_procedure_done := 6
                WHEN obs regexp "!!7479=5622!!" then @dysplasia_procedure_done := 7
								ELSE @dysplasia_procedure_done := null
            END AS dysplasia_procedure_done, 
            CASE
								WHEN obs regexp "!!1915=" then @other_dysplasia_procedure_done := replace(replace((substring_index(substring(obs,locate("!!1915=",obs)),@sep,1)),"!!1915=",""),"!!","")
								ELSE @other_dysplasia_procedure_done := null
            END AS other_dysplasia_procedure_done,
            CASE
								WHEN obs regexp "!!7500=9725!!" then @dysplasia_mgmt_plan := 1
								WHEN obs regexp "!!7500=9178!!" then @dysplasia_mgmt_plan := 2
                WHEN obs regexp "!!7500=7497!!" then @dysplasia_mgmt_plan := 3
                WHEN obs regexp "!!7500=7383!!" then @dysplasia_mgmt_plan := 4
                WHEN obs regexp "!!7500=7499!!" then @dysplasia_mgmt_plan := 5
								WHEN obs regexp "!!7500=5622!!" then @dysplasia_mgmt_plan:= 6
								ELSE @dysplasia_mgmt_plan := null
            END AS dysplasia_mgmt_plan, 
            CASE
								WHEN obs regexp "!!1915=" then @other_dysplasia_mgmt_plan := replace(replace((substring_index(substring(obs,locate("!!1915=",obs)),@sep,1)),"!!1915=",""),"!!","")
								ELSE @other_dysplasia_mgmt_plan := null
            END AS other_dysplasia_mgmt_plan,
            CASE
								WHEN obs regexp "!!7222=" then @dysplasia_assesment_notes := replace(replace((substring_index(substring(obs,locate("!!7222=",obs)),@sep,1)),"!!7222=",""),"!!","")
								ELSE @dysplasia_assesment_notes := null
            END AS dysplasia_assesment_notes,
            CASE
								WHEN obs regexp "!!5096=" then @dysplasia_rtc_date := replace(replace((substring_index(substring(obs,locate("!!5096=",obs)),@sep,1)),"!!5096=",""),"!!","")
								ELSE @dysplasia_rtc_date := null
            END AS dysplasia_rtc_date,
            CASE
								WHEN obs regexp "!!7423=1115!!" then @pap_smear_results := 1
								WHEN obs regexp "!!7423=7417!!" then @pap_smear_results := 2
                WHEN obs regexp "!!7423=7418!!" then @pap_smear_results := 3
                WHEN obs regexp "!!7423=7419!!" then @pap_smear_results := 4
                WHEN obs regexp "!!7423=7420!!" then @pap_smear_results := 5
								WHEN obs regexp "!!7423=7422!!" then @pap_smear_results := 6
								ELSE @pap_smear_results := null
            END AS pap_smear_results,  
            CASE
                WHEN obs regexp "!!10060" then @biopsy_workup_date := GetValues(obs, 10060)
                ELSE @biopsy_workup_date := null
            END AS biopsy_workup_date,
            CASE
                WHEN obs regexp "!!10061" then @date_patient_notified := GetValues(obs, 10061)
                ELSE @date_patient_informed_referred := null
            END AS date_patient_notified,
            CASE
                WHEN obs regexp "!!9728" then @diagnosis_date := GetValues(obs, 9728)
                ELSE @diagnosis_date := null
            END AS diagnosis_date,
            CASE
								WHEN obs regexp "!!8268=8266!!" then @leep_location := 1
								WHEN obs regexp "!!8268=8267!!" then @leep_location := 2
								ELSE @leep_location := null
						END AS leep_location,
            CASE
								WHEN obs regexp "!!7645=1115!!" then @cervix_biopsy_results := 1
								WHEN obs regexp "!!7645=7424!!" then @cervix_biopsy_results := 2
								WHEN obs regexp "!!7645=7425!!" then @cervix_biopsy_results := 3
                WHEN obs regexp "!!7645=7216!!" then @cervix_biopsy_results := 4
                WHEN obs regexp "!!7645=1447!!" then @cervix_biopsy_results := 5
								WHEN obs regexp "!!7645=149!!" then  @cervix_biopsy_results := 6
								WHEN obs regexp "!!7645=8282!!" then @cervix_biopsy_results := 7
								WHEN obs regexp "!!7645=9620!!" then @cervix_biopsy_results := 8
                WHEN obs regexp "!!7645=8276!!" then @cervix_biopsy_results := 9
                WHEN obs regexp "!!7645=9617!!" then @cervix_biopsy_results := 10
                WHEN obs regexp "!!7645=9621!!" then @cervix_biopsy_results := 11
								WHEN obs regexp "!!7645=7421!!" then @cervix_biopsy_results := 12
                WHEN obs regexp "!!7645=7422!!" then @cervix_biopsy_results := 13
								WHEN obs regexp "!!7645=9618!!" then @cervix_biopsy_results := 14
								ELSE @cervix_biopsy_results := null
            END AS cervix_biopsy_results,
            CASE
								WHEN obs regexp "!!7647=1115!!" then @vagina_biopsy_results := 1
								WHEN obs regexp "!!7647=7492!!" then @vagina_biopsy_results := 2
                WHEN obs regexp "!!7647=7491!!" then @vagina_biopsy_results := 3
                WHEN obs regexp "!!7647=7435!!" then @vagina_biopsy_results := 4
                WHEN obs regexp "!!7647=6537!!" then @vagina_biopsy_results := 5
								WHEN obs regexp "!!7647=1447!!" then @vagina_biopsy_results := 6
                WHEN obs regexp "!!7647=8282!!" then @vagina_biopsy_results := 7
								WHEN obs regexp "!!7647=9620!!" then @vagina_biopsy_results := 8
                WHEN obs regexp "!!7647=8276!!" then @vagina_biopsy_results := 9
                WHEN obs regexp "!!7647=9617!!" then @vagina_biopsy_results := 10
                WHEN obs regexp "!!7647=9621!!" then @vagina_biopsy_results := 11
								WHEN obs regexp "!!7647=7421!!" then @vagina_biopsy_results := 12
                WHEN obs regexp "!!7647=7422!!" then @vagina_biopsy_results := 13
								WHEN obs regexp "!!7647=9618!!" then @vagina_biopsy_results := 14
								ELSE @vagina_biopsy_results := null
						END AS vagina_biopsy_results,
            CASE
								WHEN obs regexp "!!7646=1115!!" then @vulva_biopsy_result := 1
								WHEN obs regexp "!!7646=7489!!" then @vulva_biopsy_result := 2
                WHEN obs regexp "!!7646=7488!!" then @vulva_biopsy_result := 3
                WHEN obs regexp "!!7646=7483!!" then @vulva_biopsy_result := 4
                WHEN obs regexp "!!7646=9618!!" then @vulva_biopsy_result := 5
								WHEN obs regexp "!!7646=1447!!" then @vulva_biopsy_result := 6
								WHEN obs regexp "!!7646=8282!!" then @vulva_biopsy_result := 7
								WHEN obs regexp "!!7646=9620!!" then @vulva_biopsy_result := 8
                WHEN obs regexp "!!7646=8276!!" then @vulva_biopsy_result := 9
                WHEN obs regexp "!!7646=9617!!" then @vulva_biopsy_result := 10
                WHEN obs regexp "!!7646=9621!!" then @vulva_biopsy_result := 11
                WHEN obs regexp "!!7646=7421!!" then @vulva_biopsy_result := 12
                WHEN obs regexp "!!7646=7422!!" then @vulva_biopsy_result := 13
								ELSE @vulva_biopsy_result := null
						END AS vulva_biopsy_result,
            CASE
								WHEN obs regexp "!!10207=1115!!" then @endometrium_biopsy := 1
								WHEN obs regexp "!!10207=8276!!" then @endometrium_biopsy := 2
                WHEN obs regexp "!!10207=9617!!" then @endometrium_biopsy := 3
                WHEN obs regexp "!!10207=9621!!" then @endometrium_biopsy := 4
                WHEN obs regexp "!!10207=9618!!" then @endometrium_biopsy := 5
								WHEN obs regexp "!!10207=7421!!" then @endometrium_biopsy := 6
								WHEN obs regexp "!!10207=8282!!" then @endometrium_biopsy := 7
								WHEN obs regexp "!!10207=9620!!" then @endometrium_biopsy := 8
                WHEN obs regexp "!!10207=7422!!" then @endometrium_biopsy := 9
								ELSE @endometrium_biopsy := null
						END AS endometrium_biopsy,
            CASE
								WHEN obs regexp "!!10204=1115!!" then @ECC := 1
								WHEN obs regexp "!!10204=7424!!" then @ECC := 2
								WHEN obs regexp "!!10204=7425!!" then @ECC := 3
                WHEN obs regexp "!!10204=7216!!" then @ECC := 4
                WHEN obs regexp "!!10204=1447!!" then @ECC := 5
								WHEN obs regexp "!!10204=149!!" then  @ECC := 6
								WHEN obs regexp "!!10204=8282!!" then @ECC := 7
								WHEN obs regexp "!!10204=9620!!" then @ECC := 8
                WHEN obs regexp "!!10204=8276!!" then @ECC := 9
                WHEN obs regexp "!!10204=9617!!" then @ECC := 10
                WHEN obs regexp "!!10204=9621!!" then @ECC := 11
								WHEN obs regexp "!!10204=7421!!" then @ECC := 12
                WHEN obs regexp "!!10204=7422!!" then @ECC := 13
								WHEN obs regexp "!!10204=9618!!" then @ECC := 14
								ELSE @ECC := null
            END AS ECC,
            CASE
								WHEN obs regexp "!!7500=9725!!" then @biopsy_results_mngmt_plan := 1
								WHEN obs regexp "!!7500=9178!!" then @biopsy_results_mngmt_plan := 2
                WHEN obs regexp "!!7500=9498!!" then @biopsy_results_mngmt_plan := 2
                WHEN obs regexp "!!7500=7496!!" then @biopsy_results_mngmt_plan := 3
                WHEN obs regexp "!!7500=7497!!" then @biopsy_results_mngmt_plan := 4
                WHEN obs regexp "!!7500=7499!!" then @biopsy_results_mngmt_plan := 5
                WHEN obs regexp "!!7500=6105!!" then @biopsy_results_mngmt_plan := 6
                WHEN obs regexp "!!7500=7147!!" then @biopsy_results_mngmt_plan := 7
                WHEN obs regexp "!!7500=7466!!" then @biopsy_results_mngmt_plan := 8
                WHEN obs regexp "!!7500=10200!!" then @biopsy_results_mngmt_plan := 9
								ELSE @biopsy_results_mngmt_plan := null
            END AS biopsy_results_mngmt_plan,
            CASE
								WHEN obs regexp "!!9868=9852!!" then @cancer_staging := 1
								WHEN obs regexp "!!9868=9856!!" then @cancer_staging := 2
                WHEN obs regexp "!!9868=9860!!" then @cancer_staging := 3
                WHEN obs regexp "!!9868=9864!!" then @cancer_staging := 4
								ELSE @cancer_staging := null
						END AS cancer_staging,
            null as next_app_date
		    FROM 
            flat_cervical_cancer_screening_0 t1
				JOIN 
            amrs.person p using (person_id)
				ORDER BY 
            person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
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

        ALTER TABLE flat_cervical_cancer_screening_1 drop prev_id, drop cur_id;

				DROP TABLE IF EXISTS flat_cervical_cancer_screening_2;
        CREATE TEMPORARY TABLE flat_cervical_cancer_screening_2
        (SELECT
            *,
            @prev_id := @cur_id as prev_id,
            @cur_id := person_id as cur_id,

            CASE
                WHEN @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
								ELSE @prev_encounter_datetime := null
            END AS next_encounter_datetime_cervical_cancer_screening,
            @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,
            CASE
								WHEN @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
								ELSE @next_encounter_type := null
            END AS next_encounter_type_cervical_cancer_screening,
            @cur_encounter_type := encounter_type as cur_encounter_type,
            CASE
								WHEN @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								ELSE @prev_clinical_datetime := null
							END AS next_clinical_datetime_cervical_cancer_screening,
            CASE
								WHEN @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								ELSE @prev_clinical_location_id := null
            END AS next_clinical_location_id_cervical_cancer_screening,
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
            END AS next_clinical_rtc_date_cervical_cancer_screening,
            CASE
                WHEN is_clinical_encounter then @cur_clinical_rtc_date := next_app_date
                WHEN @prev_id = @cur_id then @cur_clinical_rtc_date
                ELSE @cur_clinical_rtc_date:= null
            END AS cur_clinical_rtc_date
        FROM 
            flat_cervical_cancer_screening_1
        ORDER BY 
            person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
				);

				ALTER TABLE flat_cervical_cancer_screening_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;

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

        DROP TEMPORARY TABLE IF EXISTS flat_cervical_cancer_screening_3;
        CREATE TEMPORARY TABLE flat_cervical_cancer_screening_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
        (SELECT
            *,
            @prev_id := @cur_id as prev_id,
            @cur_id := t1.person_id as cur_id,
            CASE
                WHEN @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
                ELSE @prev_encounter_type:=null
            END AS prev_encounter_type_cervical_cancer_screening,	
            @cur_encounter_type := encounter_type as cur_encounter_type,
            CASE
                WHEN @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
                ELSE @prev_encounter_datetime := null
            END AS prev_encounter_datetime_cervical_cancer_screening,
            @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,
            CASE
                WHEN @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
                ELSE @prev_clinical_datetime := null
            END AS prev_clinical_datetime_cervical_cancer_screening,
            CASE
                WHEN @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
                ELSE @prev_clinical_location_id := null
            END AS prev_clinical_location_id_cervical_cancer_screening,
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
            END AS prev_clinical_rtc_date_cervical_cancer_screening,
            CASE
                WHEN is_clinical_encounter then @cur_clinical_rtc_date := next_app_date
                WHEN @prev_id = @cur_id then @cur_clinical_rtc_date
                ELSE @cur_clinical_rtc_date:= null
            END AS cur_clinic_rtc_date
          FROM 
            flat_cervical_cancer_screening_2 t1
          ORDER BY 
            person_id, date(encounter_datetime), encounter_type_sort_index
          );

					SELECT 
              COUNT(*)
          INTO
              @new_encounter_rows
          FROM
              flat_cervical_cancer_screening_3;
                    
          SELECT @new_encounter_rows;                    
					SET @total_rows_written := @total_rows_written + @new_encounter_rows;
          SELECT @total_rows_written;

					SET @dyn_sql := CONCAT('REPLACE INTO ',@write_table,											  
              '(SELECT
                  null,
                  person_id ,
                  encounter_id,
                  encounter_type,
                  encounter_datetime,
                  visit_id,
                  location_id,
                  t2.uuid as location_uuid,
                  uuid,
                  age,
                  encounter_purpose,
                  cur_visit_type,
                  actual_scheduled_date,
                  gravida,
                  parity,
                  menstruation_status,
                  lmp,
                  pregnancy_status,
                  pregnant_edd,
                  reason_not_pregnant,
                  hiv_status,
                  viral_load,
                  viral_load_date,
                  prior_via_done,
                  prior_via_result,
                  prior_via_date,
                  cur_via_result,
                  visual_impression_cervix,
                  visual_impression_vagina,
                  visual_impression_vulva,
                  via_procedure_done,
                  other_via_procedure_done,
                  via_management_plan,
                  other_via_management_plan,
                  via_assessment_notes,
                  via_rtc_date,
                  prior_dysplasia_done,
                  previous_via_result,
                  previous_via_result_date,
                  prior_papsmear_result, 
                  prior_biopsy_result,
                  prior_biopsy_result_date,
                  prior_biopsy_result_other,
                  prior_biopsy_result_date_other,
                  date_patient_informed_referred,
                  past_dysplasia_treatment,
                  treatment_specimen_pathology,
                  satisfactory_colposcopy,
                  colposcopy_findings,
                  cervica_lesion_size,
                  dysplasia_cervix_impression,
                  dysplasia_vagina_impression,
                  dysplasia_vulva_impression,
                  dysplasia_procedure_done,
                  other_dysplasia_procedure_done,
                  dysplasia_mgmt_plan,
                  other_dysplasia_mgmt_plan,
                  dysplasia_assesment_notes,
                  dysplasia_rtc_date,
                  pap_smear_results,
                  biopsy_workup_date,
                  date_patient_notified,
                  diagnosis_date,
                  leep_location,
                  cervix_biopsy_results,
                  vagina_biopsy_results,
                  vulva_biopsy_result,
                  endometrium_biopsy,
                  ECC,
                  biopsy_results_mngmt_plan,
                  cancer_staging,
                  next_app_date,
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
              FROM 
                  flat_cervical_cancer_screening_3 t1
              JOIN 
                  amrs.location t2 using (location_id))'
          );

					PREPARE s1 FROM @dyn_sql; 
          EXECUTE s1; 
					DEALLOCATE PREPARE s1;


					SET @dyn_sql=CONCAT('delete t1 FROM ',@queue_table,' t1 JOIN flat_cervical_cancer_screening_build_queue__0 t2 using (person_id);'); 
					PREPARE s1 FROM @dyn_sql; 
          EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                              
					SET @dyn_sql := CONCAT('SELECT count(*) into @person_ids_count FROM ',@queue_table,';'); 
					PREPARE s1 FROM @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
          SET @cycle_length = timestampdiff(second,@loop_start_time,now());
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
        SET @dyn_sql=CONCAT("Select count(*) into @total_rows_to_write FROM ", @write_table);
        PREPARE s1 FROM @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;
                                            
        SET @start_write := now();
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
END