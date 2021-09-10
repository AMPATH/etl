DELIMITER $$
CREATE  PROCEDURE `generate_flat_cervical_cancer_screening_rc_v1_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
    SET @primary_table := "flat_cervical_cancer_screening_rc";
    SET @query_type := query_type;
    SET @total_rows_written := 0;
    
    SET @encounter_types := "(69,70,147)";
    SET @clinical_encounter_types := "(69,70,147)";
    SET @non_clinical_encounter_types := "(-1)";
    SET @other_encounter_types := "(-1)";
                    
    SET @start := now();
    SET @table_version := "flat_cervical_cancer_screening_rc";

    SET session sort_buffer_size := 512000000;

    SET @sep := " ## ";
    SET @boundary := "!!";
    SET @last_date_created := (select max(max_date_created) from etl.flat_obs);

CREATE TABLE IF NOT EXISTS flat_cervical_cancer_screening_rc (
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
    follow_up_plan TINYINT,
    screening_rtc_date DATETIME,
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
  	select 'BUILDING..........................................';              												
        SET @write_table := concat("flat_cervical_cancer_screening_rc_temp_",queue_number);
        SET @queue_table := concat("flat_cervical_cancer_screening_rc_build_queue_", queue_number);                    												
							
        SET @dyn_sql := CONCAT('create table if not exists ', @write_table,' like ', @primary_table);
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  

        SET @dyn_sql := CONCAT('Create table if not exists ', @queue_table, ' (select * from flat_cervical_cancer_screening_rc_build_queue limit ', queue_size, ');'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
        
        SET @dyn_sql := CONCAT('delete t1 from flat_cervical_cancer_screening_rc_build_queue t1 join ', @queue_table, ' t2 using (person_id);'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
    END IF;
	
    IF (@query_type = "sync") THEN
        select 'SYNCING..........................................';
        SET @write_table := "flat_cervical_cancer_screening_rc";
        SET @queue_table := "flat_cervical_cancer_screening_rc_sync_queue";

		CREATE TABLE IF NOT EXISTS etl.flat_cervical_cancer_screening_rc_sync_queue (
			person_id INT NOT NULL
		);                            
                            
        SET @last_update := null;

SELECT 
    MAX(date_updated)
INTO @last_update FROM
    etl.flat_log
WHERE
    table_name = @table_version;										

        replace into etl.flat_cervical_cancer_screening_rc_sync_queue
        (select distinct person_id
          from etl.flat_obs
          where encounter_type in (69,70,147) and  max_date_created > @last_update
        );

        replace into etl.flat_cervical_cancer_screening_rc_sync_queue
        (select person_id from 
          amrs.person 
          where date_voided > @last_update);

        replace into etl.flat_cervical_cancer_screening_rc_sync_queue
        (select patient_id from 
          amrs.encounter
          where encounter_type in (69,70,147) and date_changed > @last_update);
          
          replace into etl.flat_cervical_cancer_screening_rc_sync_queue
        (select patient_id from 
          amrs.encounter
          where encounter_type in (69,70,147) and date_voided > @last_update);
    END IF;
                      
    -- Remove test patients
    SET @dyn_sql := CONCAT('delete t1 FROM ', @queue_table,' t1
        join amrs.person_attribute t2 using (person_id)
        where t2.person_attribute_type_id=28 and value="true" and voided=0');
    PREPARE s1 from @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  

    SET @person_ids_count := 0;
    SET @dyn_sql := CONCAT('select count(*) into @person_ids_count from ', @queue_table); 
    PREPARE s1 from @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;

SELECT @person_ids_count AS 'num patients to update';

    SET @dyn_sql := CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
    PREPARE s1 from @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
                                    
    SET @total_time := 0;
    SET @cycle_number := 0;
                    
    WHILE @person_ids_count > 0 DO
        SET @loop_start_time = now();
                        
        drop temporary table if exists flat_cervical_cancer_screening_rc_build_queue__0;
						
        SET @dyn_sql=CONCAT('create temporary table flat_cervical_cancer_screening_rc_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
                    
        drop temporary table if exists flat_cervical_cancer_screening_rc_0a;
        SET @dyn_sql = CONCAT(
            'create temporary table flat_cervical_cancer_screening_rc_0a
            (select
              t1.person_id,
              t1.visit_id,
              t1.encounter_id,
              t1.encounter_datetime,
              t1.encounter_type,
              t1.location_id,
              t1.obs,
              t1.obs_datetimes,
              case
                  when t1.encounter_type in ', @clinical_encounter_types,' then 1
                  else null
              end as is_clinical_encounter,
              case
                  when t1.encounter_type in ', @non_clinical_encounter_types,' then 20
                  when t1.encounter_type in ', @clinical_encounter_types,' then 10
                  when t1.encounter_type in', @other_encounter_types, ' then 5
                  else 1
              end as encounter_type_sort_index,
              t2.orders
            from etl.flat_obs t1
              join flat_cervical_cancer_screening_rc_build_queue__0 t0 using (person_id)
              left join etl.flat_orders t2 using(encounter_id)
            where t1.encounter_type in ',@encounter_types,');'
          );
                            
          PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
          DEALLOCATE PREPARE s1;  
  					
          insert into flat_cervical_cancer_screening_rc_0a
          (select
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
              from etl.flat_lab_obs t1
              join flat_cervical_cancer_screening_rc_build_queue__0 t0 using (person_id)
          );


          drop temporary table if exists flat_cervical_cancer_screening_rc_0;
          create temporary table flat_cervical_cancer_screening_rc_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
          (select * from flat_cervical_cancer_screening_rc_0a
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
          SET @follow_up_plan := null;
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
                                                
          drop temporary table if exists flat_cervical_cancer_screening_rc_1;
          create temporary table flat_cervical_cancer_screening_rc_1 #(index encounter_id (encounter_id))
          (select 
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
                  when t1.encounter_type = 69 and obs regexp "!!1839=1246!!" then @cur_visit_type := 1 -- Scheduled visit
                  when t1.encounter_type = 69 and obs regexp "!!1839=1837!!" then @cur_visit_type := 2 -- Unscheduled visit early
                  when t1.encounter_type = 69 and obs regexp "!!1839=1838!!" then @cur_visit_type := 3 -- Unscheduled visit late
                  else @cur_visit_type := null
              end as cur_visit_type,
              case
                 when t1.encounter_type = 69 and obs regexp "!!1834=7850!!" then @reasons_for_current_visit := 1 -- Initial screening
                 when t1.encounter_type = 69 and obs regexp "!!1834=432!!" then @reasons_for_current_visit := 2 -- Routine screening
                 when t1.encounter_type = 69 and obs regexp "!!1834=11755!!" then @reasons_for_current_visit := 3 -- Post-Treatment screening
                 when t1.encounter_type = 69 and obs regexp "!!1834=1185!!" then @reasons_for_current_visit := 4 -- Treatment visit
                 when t1.encounter_type = 69 and obs regexp "!!1834=11758!!" then @reasons_for_current_visit := 5 -- Complications
                  else @reasons_for_current_visit := null
              end as reasons_for_current_visit,
              case
                 when t1.encounter_type = 69 and obs regexp "!!7029=" then @actual_scheduled_visit_date := GetValues(obs, 7029)
                  else @actual_scheduled_visit_date := null
              end as actual_scheduled_visit_date,
              case
                 when t1.encounter_type = 69 and obs regexp "!!5624=" then @gravida := GetValues(obs, 5624)
                  else @gravida := null
              end as gravida,
              case
                 when t1.encounter_type = 69 and obs regexp "!!1053=" then @parity := GetValues(obs, 1053)
                  else @parity := null
              end as parity,
              case
                 when t1.encounter_type = 69 and obs regexp "!!2061=5989!!" then @menstruation_status := 1 -- Menstruating 
                 when t1.encounter_type = 69 and obs regexp "!!2061=6496!!" then @menstruation_status := 2 -- Post-menopausal
                  else @menstruation_status := null
              end as menstruation_status,
              case
                 when t1.encounter_type = 69 and obs regexp "!!1836=" then @last_menstrual_period_date := GetValues(obs, 1836)
                  else @last_menstrual_period_date := null
              end as last_menstrual_period_date,
              case
                 when t1.encounter_type = 69 and obs regexp "!!8351=1065!!" then @pregnancy_status := 1 -- Yes
                 when t1.encounter_type = 69 and obs regexp "!!8351=1066!!" then @pregnancy_status := 0 -- No
                  else @pregnancy_status := null
              end as pregnancy_status, 
              case
                 when t1.encounter_type = 69 and obs regexp "!!5596=" then @estimated_delivery_date := GetValues(obs, 5596)
                  else @estimated_delivery_date := null
              end as estimated_delivery_date,
              case 
                when t1.encounter_type = 69 and obs regexp "!!5599=" then @last_delivery_date := GetValues(obs, 5599)
                else @last_delivery_date := null
              end as last_delivery_date,
              case
                 when t1.encounter_type = 69 and obs regexp "!!9733=9729!!" then @reason_not_pregnant := 1 -- Pregnancy not suspected
                 when t1.encounter_type = 69 and obs regexp "!!9733=9730!!" then @reason_not_pregnant := 2 -- Pregnancy test is negative
                 when t1.encounter_type = 69 and obs regexp "!!9733=9731!!" then @reason_not_pregnant := 3 -- Hormonal contraception
                 when t1.encounter_type = 69 and obs regexp "!!9733=9732!!" then @reason_not_pregnant := 4 -- Postpartum less than six weeks
                  else @reason_not_pregnant := null
              end as reason_not_pregnant,
              case
                 when t1.encounter_type = 69 and obs regexp "!!6709=664!!" then @hiv_status := 1 -- Negative
                 when t1.encounter_type = 69 and obs regexp "!!6709=703!!" then @hiv_status := 2 -- Positive
                 when t1.encounter_type = 69 and obs regexp "!!6709=1067!!" then @hiv_status := 3 -- Unknown
                  else @hiv_status := null
              end as hiv_status,
              case 
                when t1.encounter_type = 69 and obs regexp "!!6152=1065!!" then @enrolled_in_hiv_care := 1 -- Yes
                when t1.encounter_type = 69 and obs regexp "!!6152=1066!!" then @enrolled_in_hiv_care := 2 -- No
                else @enrolled_in_hiv_care := null
              end as enrolled_in_hiv_care,
              case
                when t1.encounter_type = 69 and obs regexp "!!6152=1065!!" then @primary_care_facility := pa.value
                else @primary_care_facility := null
              end as primary_care_facility,
              case
                when t1.encounter_type = 69 and obs regexp "!!6152=1065!!" and obs regexp "!!10000=" then @primary_care_facility_non_ampath := GetValues(obs, 10000)
                else @primary_care_facility_non_ampath := null
              end as primary_care_facility_non_ampath,
              case
                 when t1.encounter_type = 69 and obs regexp "!!9589=1065!!" then @prior_via_done := 1 -- Yes
                 when t1.encounter_type = 69 and obs regexp "!!9589=1066!!" then @prior_via_done := 0 -- No
                  else @prior_via_done := null
              end as prior_via_done,
              case
                 when t1.encounter_type = 69 and obs regexp "!!7381=664!!" then @prior_via_result := 1 -- Positive
                 when t1.encounter_type = 69 and obs regexp "!!7381=703!!" then @prior_via_result := 2 -- Negative
                  else @prior_via_result := null
              end as prior_via_result,
              case
                  when t1.encounter_type = 69 and obs regexp "!!7381=" then @prior_via_date := GetValues(obs_datetimes, 7381) -- Could be buggy!
                  else @prior_via_date := null
              end as prior_via_date,
              case
                 when t1.encounter_type = 69 and obs regexp "!!10402=" then @screening_method := GetValues(obs, 10402) 
                 else @screening_method := null
            end as screening_method,
            case 
                when t1.encounter_type = 69 and obs regexp "!!9434=664!!" then @via_or_via_vili_test_result := 1 -- Negative
                when t1.encounter_type = 69 and obs regexp "!!9434=703!!" then @via_or_via_vili_test_result := 2 -- Positive
                when t1.encounter_type = 69 and obs regexp "!!9434=6971!!" then @via_or_via_vili_test_result := 3 -- Suspicious of cancer
                else @via_or_via_vili_test_result := null
            end as via_or_via_vili_test_result,
            case
              when t1.encounter_type = 69 and obs regexp "!!9590=" then @observations_from_positive_via_or_via_vili_test := GetValues(obs, 9590)
              else @observations_from_positive_via_or_via_vili_test := null
            end as observations_from_positive_via_or_via_vili_test,
            case
                 when t1.encounter_type = 69 and obs regexp "!!7484=1115!!" then @visual_impression_cervix := 1 -- Normal
                 when t1.encounter_type = 69 and obs regexp "!!7484=7507!!" then @visual_impression_cervix := 2 -- Positive VIA with Aceto white area
                 when t1.encounter_type = 69 and obs regexp "!!7484=7508!!" then @visual_impression_cervix := 3 -- Positive VIA with suspicious lesion
                  else @visual_impression_cervix := null
              end as visual_impression_cervix,
              case
                 when t1.encounter_type = 69 and obs regexp "!!7490=1115!!" then @visual_impression_vagina := 1 -- Normal
                 when t1.encounter_type = 69 and obs regexp "!!7490=1447!!" then @visual_impression_vagina := 2 -- Warts, genital
                 when t1.encounter_type = 69 and obs regexp "!!7490=9181!!" then @visual_impression_vagina := 3 -- Suspicious of cancer, vaginal lesion
                  else @visual_impression_vagina := null
              end as visual_impression_vagina,
              case
                 when t1.encounter_type = 69 and obs regexp "!!7490=1115!!" then @visual_impression_vulva := 1 -- Normal
                 when t1.encounter_type = 69 and obs regexp "!!7490=1447!!" then @visual_impression_vulva := 2 -- Warts, genital
                 when t1.encounter_type = 69 and obs regexp "!!7490=9177!!" then @visual_impression_vulva := 3 -- Suspicious of cancer, vulval lesion
                  else @visual_impression_vulva := null
              end as visual_impression_vulva,
                        case 
                when t1.encounter_type = 69 and obs regexp "!!2322=664!!" then @hpv_test_result := 1 -- Negative
                when t1.encounter_type = 69 and obs regexp "!!2322=703!!" then @hpv_test_result := 2 -- Positive
                when t1.encounter_type = 69 and obs regexp "!!2322=1138!!" then @hpv_test_result := 3 -- Indeterminate -> Unknown (different label)
                else @hpv_test_result := null
            end as hpv_test_result,
            case
              when t1.encounter_type = 69 and obs regexp "!!11776=11773!!" then @hpv_type := 1 -- HPV TYPE 16
              when t1.encounter_type = 69 and obs regexp "!!11776=11774!!" then @hpv_type := 2 -- HPV TYPE 18
              when t1.encounter_type = 69 and obs regexp "!!11776=11775!!" then @hpv_type := 3 -- HPV TYPE 45
              else @hpv_type := null
            end as hpv_type,
                        case 
                when t1.encounter_type = 69 and obs regexp "!!7423=1115!!" then @pap_smear_test_result := 1 -- Normal
                when t1.encounter_type = 69 and obs regexp "!!7423=7417!!" then @pap_smear_test_result := 2 -- ASCUS / ASC-H
                when t1.encounter_type = 69 and obs regexp "!!7423=7419!!" then @pap_smear_test_result := 3 -- LSIL
                when t1.encounter_type = 69 and obs regexp "!!7423=7420!!" then @pap_smear_test_result := 4 -- HSIL/CIS
                when t1.encounter_type = 69 and obs regexp "!!7423=7418!!" then @pap_smear_test_result := 5 -- AGUS
                when t1.encounter_type = 69 and obs regexp "!!7423=10055!!" then @pap_smear_test_result := 6 -- Invasive cancer
                else @pap_smear_test_result := null
            end as pap_smear_test_result,
              case
                 when t1.encounter_type = 69 and obs regexp "!!7479=1107!!" then @procedures_done_this_visit := 1 -- None
                 when t1.encounter_type = 69 and obs regexp "!!7479=11769!!" then @procedures_done_this_visit := 2 -- Endometrial biopsy
                 when t1.encounter_type = 69 and obs regexp "!!7479=10202!!" then @procedures_done_this_visit := 3 -- Punch biopsy
                 when t1.encounter_type = 69 and obs regexp "!!7479=9724!!" then @procedures_done_this_visit := 4 -- Polypectomy
                else @procedures_done_this_visit := null
              end as procedures_done_this_visit,
              case
                when t1.encounter_type = 69 and obs regexp "!!10380=1107!!" then @treatment_method := 1 -- None  
                when t1.encounter_type = 69 and obs regexp "!!10380=7466!!" then @treatment_method := 2 -- Cryotherapy
                when t1.encounter_type = 69 and obs regexp "!!10380=7147!!" then @treatment_method := 3 -- LEEP 
                when t1.encounter_type = 69 and obs regexp "!!10380=11757!!" then @treatment_method := 4 -- Thermocoagulation
                when t1.encounter_type = 69 and obs regexp "!!10380=1667!!" then @treatment_method := 5 -- Other treatment methods
                else @treatment_method := null
              end as treatment_method,
              case
                when t1.encounter_type = 69 and obs regexp "!!10039=" then @other_treatment_method_non_coded := GetValues(obs, 10039)
                else @other_treatment_method_non_coded := null
              end as other_treatment_method_non_coded,
              case
                when t1.encounter_type = 69 and obs regexp "!!11761=10756!!" then @cryotherapy_status := 1 -- Done
                when t1.encounter_type = 69 and obs regexp "!!11761=11760!!" then @cryotherapy_status := 2 -- Single Visit Approach
                when t1.encounter_type = 69 and obs regexp "!!11761=11759!!" then @cryotherapy_status := 3 -- Postponed
                when t1.encounter_type = 69 and obs regexp "!!11761=10115!!" then @cryotherapy_status := 4 -- Referred
                else @cryotherapy_status := null
              end as cryotherapy_status,
              case
                when t1.encounter_type = 69 and obs regexp "!!11762=11760!!" then @leep_status := 1 -- Single Visit Approach
                when t1.encounter_type = 69 and obs regexp "!!11762=11759!!" then @leep_status := 2 -- Postponed
                when t1.encounter_type = 69 and obs regexp "!!11762=10115!!" then @leep_status := 3 -- Referred
                else @leep_status := null
              end as leep_status,
              case 
                when t1.encounter_type = 69 and obs regexp "!!11763=10756!!" then @thermocoagulation_status := 1 -- Done  
                when t1.encounter_type = 69 and obs regexp "!!11763=11760!!" then @thermocoagulation_status := 2 -- Single Visit Approach
                when t1.encounter_type = 69 and obs regexp "!!11763=11759!!" then @thermocoagulation_status := 3 -- Postponed
                when t1.encounter_type = 69 and obs regexp "!!11763=10115!!" then @thermocoagulation_status := 4 -- Referred
                else @thermocoagulation_status := null
              end as thermocoagulation_status,
              case
                when t1.encounter_type = 69 and obs regexp "!!7222=" then @screening_assessment_notes := GetValues(obs, 7222)
                else @screening_assessment_notes := null
              end as screening_assessment_notes,
              case
                 when t1.encounter_type = 69 and obs regexp "!!7500=9725!!" then @follow_up_plan := 1 -- Return for results
                 when t1.encounter_type = 69 and obs regexp "!!7500=7498!!" then @follow_up_plan := 2 -- VIA follow-up in three to six months
                 when t1.encounter_type = 69 and obs regexp "!!7500=7496!!" then @follow_up_plan := 3 -- Routine yearly VIA
                 when t1.encounter_type = 69 and obs regexp "!!7500=7497!!" then @follow_up_plan := 4 -- Routine 3 year VIA
                 when t1.encounter_type = 69 and obs regexp "!!7500=7383!!" then @follow_up_plan := 5 -- Colposcopy planned
                 when t1.encounter_type = 69 and obs regexp "!!7500=7499!!" then @follow_up_plan := 6 -- Gynecologic oncology services
                else @follow_up_plan := null
              end as follow_up_plan,
              case
                 when t1.encounter_type = 69 and obs regexp "!!5096=" then @screening_rtc_date := GetValues(obs, 5096)
                  else @screening_rtc_date := null
              end as screening_rtc_date,
              case
                 when t1.encounter_type = 70 and obs regexp "!!1839=1911!!" then @dysp_visit_type := 1 -- New visit
                 when t1.encounter_type = 70 and obs regexp "!!1839=1246!!" then @dysp_visit_type := 1 -- Scheduled visit
                 when t1.encounter_type = 70 and obs regexp "!!1839=1837!!" then @dysp_visit_type := 2 -- Unscheduled visit early
                 when t1.encounter_type = 70 and obs regexp "!!1839=1838!!" then @dysp_visit_type := 3 -- Unscheduled visit late
                else @dysp_visit_type := null
              end as dysp_visit_type,
              case
                  when t1.encounter_type = 70 and obs regexp "!!7379=1065!!" then @dysp_history := 1 -- Yes
                  when t1.encounter_type = 70 and obs regexp "!!7379=1066!!" then @dysp_history := 0 -- No
                  else @dysp_history := null
              end as dysp_history,
      			  case
      			    when t1.encounter_type = 70 and obs regexp "!!7381=664!!" then @dysp_past_via_result := 1 -- Negative
      				  when t1.encounter_type = 70 and obs regexp "!!7381=703!!" then @dysp_past_via_result := 2 -- Positive
      				else @dysp_past_via_result := null
      			  end as dysp_past_via_result,
              case
                  when t1.encounter_type = 70 and obs regexp "!!7381=" then @dysp_past_via_result_date := GetValues(obs_datetimes, 7381)
                  else @dysp_past_via_result_date := null
              end as dysp_past_via_result_date,
              case
                 when t1.encounter_type = 70 and obs regexp "!!7423=" then @dysp_past_pap_smear_result := GetValues(obs, 7423)
                  else @dysp_past_pap_smear_result := null
              end as dysp_past_pap_smear_result,
              case
                  when t1.encounter_type = 70 and obs regexp "!!7426=1115!!" then @dysp_past_biopsy_result := 1 -- Normal
                  when t1.encounter_type = 70 and obs regexp "!!7426=7424!!" then @dysp_past_biopsy_result := 2 -- CIN 1
                  when t1.encounter_type = 70 and obs regexp "!!7426=7425!!" then @dysp_past_biopsy_result := 3 -- CIN 2
                  when t1.encounter_type = 70 and obs regexp "!!7426=7216!!" then @dysp_past_biopsy_result := 4 -- CIN 3
                  when t1.encounter_type = 70 and obs regexp "!!7426=7421!!" then @dysp_past_biopsy_result := 5 -- Carcinoma
                  else @dysp_past_biopsy_result := null
              end as dysp_past_biopsy_result, 
              case
                  when t1.encounter_type = 70 and obs regexp "!!7426=" then @dysp_past_biopsy_result_date := GetValues(obs_datetimes, 7426)
                  else @dysp_past_biopsy_result_date := null
              end as dysp_past_biopsy_result_date,
              case
                  when t1.encounter_type = 70 and obs regexp "!!7400=" then @dysp_past_biopsy_result_non_coded := GetValues(obs, 7400)
                  else @dysp_past_biopsy_result_non_coded := null
              end as dysp_past_biopsy_result_non_coded,
              case
                  when t1.encounter_type = 70 and obs regexp "!!7467=" then @dysp_past_treatment := GetValues(obs, 7467)
                  else @dysp_past_treatment := null
              end as dysp_past_treatment, 
              case
                  when t1.encounter_type = 70 and obs regexp "!!7579=" then @dysp_past_treatment_specimen_pathology := GetValues(obs, 7579)
                  else @dysp_past_treatment_specimen_pathology := null
              end as dysp_past_treatment_specimen_pathology, 
              case
                  when t1.encounter_type = 70 and obs regexp "!!7428=1065!!" then @dysp_satisfactory_colposcopy := 1 -- Yes
                  when t1.encounter_type = 70 and obs regexp "!!7428=1066!!" then @dysp_satisfactory_colposcopy := 2 -- No
                  when t1.encounter_type = 70 and obs regexp "!!7428=1118!!" then @dysp_satisfactory_colposcopy := 3 -- Not done
                  else @dysp_satisfactory_colposcopy := null
              end as dysp_satisfactory_colposcopy,
              case 
                  when t1.encounter_type = 70 and obs regexp "!!7383=" then @dysp_colposcopy_findings := GetValues(obs, 7383)
                  else @dysp_colposcopy_findings := null
              end as dysp_colposcopy_findings,
              case
                  when t1.encounter_type = 70 and obs regexp "!!7477=7474!!" then @dysp_cervical_lesion_size := 1 -- > 50% of cervix
                  when t1.encounter_type = 70 and obs regexp "!!7477=7474!!" then @dysp_cervical_lesion_size := 2 -- < 50% of cervix
                  when t1.encounter_type = 70 and obs regexp "!!7477=7476!!" then @dysp_cervical_lesion_size := 3 -- Cervical lesion extends into endocervical canal
                  when t1.encounter_type = 70 and obs regexp "!!7477=9619!!" then @dysp_cervical_lesion_size := 4 -- Transformation zone > 50% of cervix
                  else @dysp_cervical_lesion_size := null
              end as dysp_cervical_lesion_size, 
              case
                 when t1.encounter_type = 70 and obs regexp "!!7484=1115!!" then @dysp_visual_impression_cervix := 1 -- Normal
                 when t1.encounter_type = 70 and obs regexp "!!7484=7424!!" then @dysp_visual_impression_cervix := 2 -- CIN 1
                 when t1.encounter_type = 70 and obs regexp "!!7484=7425!!" then @dysp_visual_impression_cervix := 3 -- CIN 2
                 when t1.encounter_type = 70 and obs regexp "!!7484=7216!!" then @dysp_visual_impression_cervix := 4 -- CIN 3
                 when t1.encounter_type = 70 and obs regexp "!!7484=7421!!" then @dysp_visual_impression_cervix := 5 -- Carcinoma
                  else @dysp_visual_impression_cervix := null
              end as dysp_visual_impression_cervix,
              case
                 when t1.encounter_type = 70 and obs regexp "!!7490=1115!!" then @dysp_visual_impression_vagina := 1 -- Normal
                 when t1.encounter_type = 70 and obs regexp "!!7490=1447!!" then @dysp_visual_impression_vagina := 2 -- Warts, genital
                 when t1.encounter_type = 70 and obs regexp "!!7490=9181!!" then @dysp_visual_impression_vagina := 3 -- Suspicious of cancer, vaginal lesion
                  else @dysp_visual_impression_vagina := null
              end as dysp_visual_impression_vagina,
              case
                 when t1.encounter_type = 70 and obs regexp "!!7490=1115!!" then @dysp_visual_impression_vulva := 1 -- Normal
                 when t1.encounter_type = 70 and obs regexp "!!7490=1447!!" then @dysp_visual_impression_vulva := 2 -- Warts, genital
                 when t1.encounter_type = 70 and obs regexp "!!7490=9177!!" then @dysp_visual_impression_vulva := 3 -- Suspicious of cancer, vulval lesion
                  else @dysp_visual_impression_vulva := null
              end as dysp_visual_impression_vulva,
              case
                when t1.encounter_type = 70 and obs regexp "!!7479=" then @dysp_procedure_done := GetValues(obs, 7479) -- Consider changing to multi-select
                else @dysp_procedure_done := null
              end as dysp_procedure_done,
              -- FREETEXT FIELD (Problematic!)
              --   case
              --       when t1.encounter_type = 70 and obs regexp "!!1915=" then @other_dysplasia_procedure_done_non_coded := GetValues(obs, 1915)
              --       else @other_dysplasia_procedure_done_non_coded := null
              --   end as other_dysplasia_procedure_done_non_coded,
              case
                  when t1.encounter_type = 70 and obs regexp "!!7500=" then @dysp_management_plan := GetValues(obs, 7500)
                  else @dysp_management_plan := null
              end as dysp_management_plan,
              case
                  when t1.encounter_type = 70 and obs regexp "!!7222=" then @dysp_assessment_notes := GetValues(obs, 7222)
                  else @dysp_assessment_notes := null
              end as dysp_assessment_notes,
              case
                  when t1.encounter_type = 70 and obs regexp "!!5096=" then @dysp_rtc_date := GetValues(obs, 5096)
                  else @dysp_rtc_date := null
              end as dysp_rtc_date,

              -- GYN PATHOLOGY RESULTS FORM (enc type 147)
              case 
                when t1.encounter_type = 147 and obs regexp "!!7423=1115!!" then @gynp_pap_smear_results := 1 -- Normal
                when t1.encounter_type = 147 and obs regexp "!!7423=7417!!" then @gynp_pap_smear_results := 2 -- ASCUS
                when t1.encounter_type = 147 and obs regexp "!!7423=7418!!" then @gynp_pap_smear_results := 3 -- AGUS
                when t1.encounter_type = 147 and obs regexp "!!7423=7419!!" then @gynp_pap_smear_results := 4 -- LSIL
                when t1.encounter_type = 147 and obs regexp "!!7423=7420!!" then @gynp_pap_smear_results := 5 -- HSIL
                when t1.encounter_type = 147 and obs regexp "!!7423=7422!!" then @gynp_pap_smear_results := 6-- Carcinoma
                else @gynp_pap_smear_results := null
              end as gynp_pap_smear_results,
              case 
                when t1.encounter_type = 147 and obs regexp "!!7423=" then @gynp_pap_smear_results_date := GetValues(obs_datetimes, 7423)
                else @gynp_pap_smear_results_date := null
              end as gynp_pap_smear_results_date,
              case 
                when t1.encounter_type = 147 and obs regexp "!!10060=" then @gynp_biopsy_sample_collection_date := GetValues(obs, 10060)
                else @gynp_biopsy_sample_collection_date := null
              end as gynp_biopsy_sample_collection_date,
              case 
                when t1.encounter_type = 147 and obs regexp "!!9728=" then @gynp_diagnosis_date := GetValues(obs, 9728) 
                else @gynp_diagnosis_date := null
              end as gynp_diagnosis_date,
              case 
                when t1.encounter_type = 147 and obs regexp "!!10061=" then @gynp_date_patient_notified_of_results := GetValues(obs, 10061)
                else @gynp_date_patient_notified_of_results := null
              end as gynp_date_patient_notified_of_results,
              case 
                when t1.encounter_type = 147 and obs regexp "!!10127=10202!!" then @gynp_procedure_done := 1 -- Punch biopsy
                when t1.encounter_type = 147 and obs regexp "!!10127=7147!!" then @gynp_procedure_done := 2 -- LEEP
                else @gynp_procedure_done := null
              end as gynp_procedure_done,
              case 
                when t1.encounter_type = 147 and obs regexp "!!7645=1115!!" then @gynp_biopsy_cervix_result := 1 -- Normal
                when t1.encounter_type = 147 and obs regexp "!!7645=7424!!" then @gynp_biopsy_cervix_result := 2 -- CIN 1
                when t1.encounter_type = 147 and obs regexp "!!7645=7425!!" then @gynp_biopsy_cervix_result := 3 -- CIN 2
                when t1.encounter_type = 147 and obs regexp "!!7645=7216!!" then @gynp_biopsy_cervix_result := 4 -- CIN 3
                when t1.encounter_type = 147 and obs regexp "!!7645=1447!!" then @gynp_biopsy_cervix_result := 5 -- Genital warts
                when t1.encounter_type = 147 and obs regexp "!!7645=149!!" then  @gynp_biopsy_cervix_result := 6 -- Cervicitis
                when t1.encounter_type = 147 and obs regexp "!!7645=8282!!" then @gynp_biopsy_cervix_result := 7 -- Cervical squamous metaplasia
                when t1.encounter_type = 147 and obs regexp "!!7645=9620!!" then @gynp_biopsy_cervix_result := 8 -- Condylomata
                when t1.encounter_type = 147 and obs regexp "!!7645=8276!!" then @gynp_biopsy_cervix_result := 9 -- Cervical squamous cell carcinoma
                when t1.encounter_type = 147 and obs regexp "!!7645=9617!!" then @gynp_biopsy_cervix_result := 10 -- Microinvasive carcinoma
                when t1.encounter_type = 147 and obs regexp "!!7645=9621!!" then @gynp_biopsy_cervix_result := 11 -- Adenocarcinoma in situ
                when t1.encounter_type = 147 and obs regexp "!!7645=7421!!" then @gynp_biopsy_cervix_result := 12 -- Squamous cell carcinoma, NOS
                when t1.encounter_type = 147 and obs regexp "!!7645=7422!!" then @gynp_biopsy_cervix_result := 13 -- Adenocarcinoma
                when t1.encounter_type = 147 and obs regexp "!!7645=9618!!" then @gynp_biopsy_cervix_result := 14 -- Invasive Adenocarcinoma
                else @gynp_biopsy_cervix_result := null
              end as gynp_biopsy_cervix_result,
              case
                when t1.encounter_type = 147 and obs regexp "!!8268=8266!!" then @leep_location := 1 -- Superficial 
                when t1.encounter_type = 147 and obs regexp "!!8268=8267!!" then @leep_location := 2 -- Deep
                else @gynp_leep_location := null
              end as gynp_leep_location,
              case 
                  when t1.encounter_type = 147 and obs regexp "!!7647=1115!!" then @gynp_vagina_result := 1 -- Normal
                  when t1.encounter_type = 147 and obs regexp "!!7647=7492!!" then @gynp_vagina_result := 2 -- VAIN 1
                  when t1.encounter_type = 147 and obs regexp "!!7647=7491!!" then @gynp_vagina_result := 3 -- VAIN 2
                  when t1.encounter_type = 147 and obs regexp "!!7647=7435!!" then @gynp_vagina_result := 4 -- VAIN 3
                  when t1.encounter_type = 147 and obs regexp "!!7647=6537!!" then @gynp_vagina_result := 5 -- Cervical cancer
                  when t1.encounter_type = 147 and obs regexp "!!7647=1447!!" then @gynp_vagina_result := 6 -- Genital warts
                  when t1.encounter_type = 147 and obs regexp "!!7647=8282!!" then @gynp_vagina_result := 7 -- Cervical squamous metaplasia
                  when t1.encounter_type = 147 and obs regexp "!!7647=9620!!" then @gynp_vagina_result := 8 -- Condylomata
                  when t1.encounter_type = 147 and obs regexp "!!7647=8276!!" then @gynp_vagina_result := 9 -- Cervical squamous cell carcinoma
                  when t1.encounter_type = 147 and obs regexp "!!7647=9617!!" then @gynp_vagina_result := 10 -- Microinvasive carcinoma
                  when t1.encounter_type = 147 and obs regexp "!!7647=9621!!" then @gynp_vagina_result := 11 -- Adenocarcinoma in situ
                  when t1.encounter_type = 147 and obs regexp "!!7647=7421!!" then @gynp_vagina_result := 12 -- Squamous cell carcinoma, NOS
                  when t1.encounter_type = 147 and obs regexp "!!7647=7422!!" then @gynp_vagina_result := 13 -- Adenocarcinoma
                  when t1.encounter_type = 147 and obs regexp "!!7647=9618!!" then @gynp_vagina_result := 14 -- Invasive adenocarcinoma
                else @gynp_vagina_result := null
              end as gynp_vagina_result,
              case
                  when t1.encounter_type = 147 and obs regexp "!!7646=1115!!" then @gynp_vulva_result := 1 -- Normal
                  when t1.encounter_type = 147 and obs regexp "!!7646=7489!!" then @gynp_vulva_result := 2 -- Condyloma or vulvar intraepithelial neoplasia grade 1
                  when t1.encounter_type = 147 and obs regexp "!!7646=7488!!" then @gynp_vulva_result := 3 -- Vulvar intraepithelial neoplasia grade 2
                  when t1.encounter_type = 147 and obs regexp "!!7646=7483!!" then @gynp_vulva_result := 4 -- Vulvar intraepithelial neoplasia grade 3
                  when t1.encounter_type = 147 and obs regexp "!!7646=9618!!" then @gynp_vulva_result := 5 -- Invasive adenocarcinoma
                  when t1.encounter_type = 147 and obs regexp "!!7646=1447!!" then @gynp_vulva_result := 6 -- Genital warts
                  when t1.encounter_type = 147 and obs regexp "!!7646=8282!!" then @gynp_vulva_result := 7 -- Cervical squamous metaplasia
                  when t1.encounter_type = 147 and obs regexp "!!7646=9620!!" then @gynp_vulva_result := 8 -- Condylomata
                  when t1.encounter_type = 147 and obs regexp "!!7646=8276!!" then @gynp_vulva_result := 9 -- Cervical squamous cell carcinoma
                  when t1.encounter_type = 147 and obs regexp "!!7646=9617!!" then @gynp_vulva_result := 10 -- Microinvasive carcinoma
                  when t1.encounter_type = 147 and obs regexp "!!7646=9621!!" then @gynp_vulva_result := 11 -- Adenocarcinoma in situ
                  when t1.encounter_type = 147 and obs regexp "!!7646=7421!!" then @gynp_vulva_result := 12 -- Squamous cell carcinoma, otherwise not specified
                  when t1.encounter_type = 147 and obs regexp "!!7646=7422!!" then @gynp_vulva_result := 13 -- Adenocarcinoma
                else @gynp_vulva_result := null
              end as gynp_vulva_result,
              case 
                when t1.encounter_type = 147 and obs regexp "!!10207=1115!!" then @gynp_endometrium_result := 1 -- Normal
                when t1.encounter_type = 147 and obs regexp "!!10207=9620!!" then @gynp_endometrium_result := 2 -- Condylomata
                when t1.encounter_type = 147 and obs regexp "!!10207=8282!!" then @gynp_endometrium_result := 3 -- Cervical squamous metaplasia
                when t1.encounter_type = 147 and obs regexp "!!10207=8726!!" then @gynp_endometrium_result := 4 -- Invasive squamous cell carcinoma
                when t1.encounter_type = 147 and obs regexp "!!10207=9617!!" then @gynp_endometrium_result := 5 -- Microinvasive carcinoma
                when t1.encounter_type = 147 and obs regexp "!!10207=9621!!" then @gynp_endometrium_result := 6 -- Adenocarcinoma in situ
                when t1.encounter_type = 147 and obs regexp "!!10207=7421!!" then @gynp_endometrium_result := 7 -- Squamous cell carcinoma
                when t1.encounter_type = 147 and obs regexp "!!10207=7422!!" then @gynp_endometrium_result := 8 -- Adenocarcinoma
                when t1.encounter_type = 147 and obs regexp "!!10207=9618!!" then @gynp_endometrium_result := 9 -- Invasive adenocarcinoma
                when t1.encounter_type = 147 and obs regexp "!!10207=5622!!" then @gynp_endometrium_result := 10 -- Other (non-coded)
                else @gynp_endometrium_result := null
              end as gynp_endometrium_result,
              case
                  when t1.encounter_type = 147 and obs regexp "!!10204=1115!!" then @gynp_ecc_result := 1 -- Normal
                  when t1.encounter_type = 147 and obs regexp "!!10204=7424!!" then @gynp_ecc_result := 2 -- CIN 1
                  when t1.encounter_type = 147 and obs regexp "!!10204=7425!!" then @gynp_ecc_result := 3 -- CIN 2
                  when t1.encounter_type = 147 and obs regexp "!!10204=7216!!" then @gynp_ecc_result := 4 -- CIN 3
                  when t1.encounter_type = 147 and obs regexp "!!10204=149!!" then  @gynp_ecc_result := 5 -- Cervicitis
                  when t1.encounter_type = 147 and obs regexp "!!10204=8282!!" then @gynp_ecc_result := 6 -- Cervical squamous metaplasia
                  when t1.encounter_type = 147 and obs regexp "!!10204=9620!!" then @gynp_ecc_result := 7 -- Condylomata
                  when t1.encounter_type = 147 and obs regexp "!!10204=8276!!" then @gynp_ecc_result := 8 -- Cervical squamous cell carcinoma
                  when t1.encounter_type = 147 and obs regexp "!!10204=9617!!" then @gynp_ecc_result := 9 -- Microinvasive carcinoma
                  when t1.encounter_type = 147 and obs regexp "!!10204=9621!!" then @gynp_ecc_result := 10 -- Adenocarcinoma in situ
                  when t1.encounter_type = 147 and obs regexp "!!10204=7421!!" then @gynp_ecc_result := 11 -- Squamous cell carcinoma, otherwise not specified
                  when t1.encounter_type = 147 and obs regexp "!!10204=7422!!" then @gynp_ecc_result := 12 -- Adenocarcinoma
                  when t1.encounter_type = 147 and obs regexp "!!10204=9618!!" then @gynp_ecc_result := 13 -- Invasive adenocarcinoma
                  else @gynp_ecc_result := null
              end as gynp_ecc_result,
              case
                when t1.encounter_type = 147 and obs regexp "!!9538=" then @gynp_lab_test_non_coded := GetValues(obs, 9538)
                else @gynp_lab_test_non_coded := null
              end as gynp_lab_test_non_coded,
              case
                when t1.encounter_type = 147 and obs regexp "!!9706=" then @gynp_date_patient_informed_and_referred := GetValues(obs, 9706) 
                else @gynp_date_patient_informed_and_referred := null
              end as gynp_date_patient_informed_and_referred,
              case
                  when t1.encounter_type = 147 and obs regexp "!!7500=9725!!" then @gynp_pathology_management_plan := 1 -- Return for result
                  when t1.encounter_type = 147 and obs regexp "!!7500=9178!!" then @gynp_pathology_management_plan := 2 -- VIA follow-up in three months
                  when t1.encounter_type = 147 and obs regexp "!!7500=7498!!" then @gynp_pathology_management_plan := 2 -- Complete VIA or Pap smear in six months
                  when t1.encounter_type = 147 and obs regexp "!!7500=7496!!" then @gynp_pathology_management_plan := 3 -- Complete VIA in one year
                  when t1.encounter_type = 147 and obs regexp "!!7500=7497!!" then @gynp_pathology_management_plan := 4 -- Complete VIA in three years
                  when t1.encounter_type = 147 and obs regexp "!!7500=7499!!" then @gynp_pathology_management_plan := 5 -- Gynecologic oncology services
                  when t1.encounter_type = 147 and obs regexp "!!7500=6105!!" then @gynp_pathology_management_plan := 6 -- Referred to clinician today
                  when t1.encounter_type = 147 and obs regexp "!!7500=7147!!" then @gynp_pathology_management_plan := 7 -- LEEP
                  when t1.encounter_type = 147 and obs regexp "!!7500=7466!!" then @gynp_pathology_management_plan := 8 -- Cryotherapy
                  when t1.encounter_type = 147 and obs regexp "!!7500=10200!!" then @gynp_pathology_management_plan := 9 -- Repeat procedure
                  else @gynp_pathology_management_plan := null
              end as gynp_pathology_management_plan,
              case 
                when t1.encounter_type = 147 and obs regexp "!!7222=" then @gynp_assessment_notes := GetValues(obs, 7222)
                else @gynp_assessment_notes := null
              end as gynp_assessment_notes,
            case
                when t1.encounter_type = 147 and obs regexp "!!5096=" then @gynp_rtc_date := GetValues(obs, 5096)
                else @gynp_rtc_date := null
              end as gynp_rtc_date      
		    FROM flat_cervical_cancer_screening_rc_0 t1
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


						alter table flat_cervical_cancer_screening_rc_1 drop prev_id, drop cur_id;

						drop table if exists flat_cervical_cancer_screening_rc_2;
						create temporary table flat_cervical_cancer_screening_rc_2
						(select *,
							@prev_id := @cur_id as prev_id,
							@cur_id := person_id as cur_id,

							case
                  when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
                  else @prev_encounter_datetime := null
							end as next_encounter_datetime_cervical_cancer_screening,

							@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
                  when @prev_id = @cur_id then @next_encounter_type := @cur_encounter_type
                  else @next_encounter_type := null
							end as next_encounter_type_cervical_cancer_screening,

							@cur_encounter_type := encounter_type as cur_encounter_type,

							case
                  when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
                  else @prev_clinical_datetime := null
							end as next_clinical_datetime_cervical_cancer_screening,

              case
                  when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
                  else @prev_clinical_location_id := null
							end as next_clinical_location_id_cervical_cancer_screening,

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
							end as next_clinical_rtc_date_cervical_cancer_screening,

							case
                  when is_clinical_encounter then @cur_clinical_rtc_date := screening_rtc_date
                  when @prev_id = @cur_id then @cur_clinical_rtc_date
                  else @cur_clinical_rtc_date:= null
							end as cur_clinical_rtc_date

							from flat_cervical_cancer_screening_rc_1
							order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
						);

						alter table flat_cervical_cancer_screening_rc_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;

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

						drop temporary table if exists flat_cervical_cancer_screening_rc_3;
						create temporary table flat_cervical_cancer_screening_rc_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
						(select
							*,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,
							case
                  when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
                  else @prev_encounter_type:=null
							end as prev_encounter_type_cervical_cancer_screening,	
              @cur_encounter_type := encounter_type as cur_encounter_type,
							case
                  when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
                  else @prev_encounter_datetime := null
						  end as prev_encounter_datetime_cervical_cancer_screening,
              @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,
							case
                  when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
                  else @prev_clinical_datetime := null
							end as prev_clinical_datetime_cervical_cancer_screening,
              case
                  when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
                  else @prev_clinical_location_id := null
							end as prev_clinical_location_id_cervical_cancer_screening,
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
							end as prev_clinical_rtc_date_cervical_cancer_screening,
							case
                  when is_clinical_encounter then @cur_clinical_rtc_date := screening_rtc_date
                  when @prev_id = @cur_id then @cur_clinical_rtc_date
                  else @cur_clinical_rtc_date:= null
							end as cur_clinic_rtc_date
							from flat_cervical_cancer_screening_rc_2 t1
							order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
                                        
					SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    flat_cervical_cancer_screening_rc_3;
                    
SELECT @new_encounter_rows;                    
          SET @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

					SET @dyn_sql=CONCAT('replace into ',@write_table,											  
            '(select
              null,
              person_id,
              encounter_id,
              encounter_type,
              encounter_datetime,
              visit_id,
              location_id,
              t2.uuid as location_uuid,
              uuid,
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
              follow_up_plan,
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

              from flat_cervical_cancer_screening_rc_3 t1
              join amrs.location t2 using (location_id))');       

					PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
					DEALLOCATE PREPARE s1;


					SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_cervical_cancer_screening_rc_build_queue__0 t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                              
					SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';'); 
					PREPARE s1 from @dyn_sql; 
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
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
                        
        SET @total_rows_to_write := 0;
        SET @dyn_sql := CONCAT("Select count(*) into @total_rows_to_write from ", @write_table);
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;
                                                
        SET @start_write := now();
SELECT 
    CONCAT(@start_write,
            ' : Writing ',
            @total_rows_to_write,
            ' to ',
            @primary_table);

						SET @dyn_sql := CONCAT('replace into ', @primary_table, '(select * from ',@write_table,');');
            PREPARE s1 from @dyn_sql; 
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
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
    END IF;
                
		SET @ave_cycle_length := ceil(@total_time/@cycle_number);

SELECT 
    CONCAT('Average Cycle Length: ',
            @ave_cycle_length,
            ' second(s)');
                
        SET @end := now();
        insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
				SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');
END$$
DELIMITER ;
