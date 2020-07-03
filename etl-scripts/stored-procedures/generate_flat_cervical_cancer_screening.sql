DELIMITER $$
CREATE  PROCEDURE `generate_flat_cervical_cancer_screening`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
    SET @primary_table := "flat_cervical_cancer_screening";
    SET @query_type := query_type;
    SET @total_rows_written := 0;
    
    SET @encounter_types := "(69,70,147)";
    SET @clinical_encounter_types := "(69,70,147)";
    SET @non_clinical_encounter_types := "(-1)";
    SET @other_encounter_types := "(-1)";
                    
    SET @start := now();
    SET @table_version := "flat_cervical_cancer_screening_v1.1";

    SET session sort_buffer_size := 512000000;

    SET @sep := " ## ";
    SET @boundary := "!!";
    SET @last_date_created := (select max(max_date_created) from etl.flat_obs);

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
        reasons_for_current_visit INT,
        cur_visit_type INT,
        actual_scheduled_date DATETIME,
        gravida INT,
        parity INT,
        menstruation_status INT,
        last_menstrual_period_date DATETIME,
        pregnancy_status INT,
        estimated_delivery_date DATETIME,
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
        history_of_dysplasia INT,
        previous_via_result INT,
        previous_via_result_date DATETIME,
        most_recent_pap_smear_result INT,
        most_recent_biopsy_result INT,
        prior_biopsy_result_date DATETIME,
        most_recent_biopsy_result_other VARCHAR(1000),
        prior_biopsy_result_date_other DATETIME,
        date_patient_informed_referred DATETIME,
        past_dysplasia_treatment INT,
        treatment_specimen_pathology INT,
        satisfactory_colposcopy INT,
        colposcopy_findings INT,
        cervical_lesion_size INT,
        other_dysplasia_procedure_done VARCHAR(1000),
        dysplasia_mgmt_plan INT,
        other_dysplasia_mgmt_plan VARCHAR(1000),
        dysplasia_assesment_notes TEXT,
        dysplasia_rtc_date DATETIME,
        leep_location INT,
        pathology_cervical_exam_findings INT,
        pathology_vaginal_exam_findings INT,
        pathology_vulval_exam_findings INT,
        endometrium_biopsy_exam_findings INT,
        endocervical_curettage_exam_findings INT,
        biopsy_results_mgmt_plan INT,
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
		select 'BUILDING..........................................';              												
        SET @write_table := concat("flat_cervical_cancer_screening_temp_",queue_number);
        SET @queue_table := concat("flat_cervical_cancer_screening_build_queue_", queue_number);                    												
							
        SET @dyn_sql := CONCAT('create table if not exists ', @write_table,' like ', @primary_table);
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  

        SET @dyn_sql := CONCAT('Create table if not exists ', @queue_table, ' (select * from flat_cervical_cancer_screening_build_queue limit ', queue_size, ');'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
        
        SET @dyn_sql := CONCAT('delete t1 from flat_cervical_cancer_screening_build_queue t1 join ', @queue_table, ' t2 using (person_id);'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
    END IF;
	
    IF (@query_type = "sync") THEN
        select 'SYNCING..........................................';
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

        replace into flat_cervical_cancer_screening_sync_queue
        (select distinct patient_id
          from amrs.encounter
          where date_changed > @last_update
        );

        replace into flat_cervical_cancer_screening_sync_queue
        (select distinct person_id
          from etl.flat_obs
          where max_date_created > @last_update
        );

        replace into flat_cervical_cancer_screening_sync_queue
        (select distinct person_id
          from etl.flat_lab_obs
          where max_date_created > @last_update
        );

        replace into flat_cervical_cancer_screening_sync_queue
        (select distinct person_id
          from etl.flat_orders
          where max_date_created > @last_update
        );
                      
        replace into flat_cervical_cancer_screening_sync_queue
        (select person_id from 
          amrs.person 
          where date_voided > @last_update);


        replace into flat_cervical_cancer_screening_sync_queue
        (select person_id from 
          amrs.person 
          where date_changed > @last_update);
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
                        
        drop temporary table if exists flat_cervical_cancer_screening_build_queue__0;
						
        SET @dyn_sql=CONCAT('create temporary table flat_cervical_cancer_screening_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
                    
        drop temporary table if exists flat_cervical_cancer_screening_0a;
        SET @dyn_sql = CONCAT(
            'create temporary table flat_cervical_cancer_screening_0a
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
              join flat_cervical_cancer_screening_build_queue__0 t0 using (person_id)
              left join etl.flat_orders t2 using(encounter_id)
            where t1.encounter_type in ',@encounter_types,');'
          );
                            
          PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
          DEALLOCATE PREPARE s1;  
  					
          insert into flat_cervical_cancer_screening_0a
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
              join flat_cervical_cancer_screening_build_queue__0 t0 using (person_id)
          );


          drop temporary table if exists flat_cervical_cancer_screening_0;
          create temporary table flat_cervical_cancer_screening_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
          (select * from flat_cervical_cancer_screening_0a
              order by person_id, date(encounter_datetime), encounter_type_sort_index
          );

          SET @prev_id := null;
          SET @cur_id := null;
          SET @reasons_for_current_visit := null;
          SET @cur_visit_type := null;
          SET @actual_scheduled_date := null;
          SET @gravida := null;
          SET @parity := null;
          SET @menstruation_status := null;
          SET @last_menstrual_period_date := null;
          SET @pregnancy_status := null;
          SET @estimated_delivery_date := null;
          SET @preason_not_pregnant := null;
          SET @hiv_status = null;
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
          SET @history_of_dysplasia := null;
          SET @previous_via_result := null;
          SET @previous_via_result_date := null;
          SET @most_recent_pap_smear_result := null;
          SET @most_recent_biopsy_result := null;
          SET @prior_biopsy_result_date := null;
          SET @most_recent_biopsy_result_other := null;
          SET @prior_biopsy_result_date_other := null;
          SET @date_patient_informed_referred := null;
          SET @past_dysplasia_treatment := null;
          SET @treatment_specimen_pathology := null;
          SET @satisfactory_colposcopy := null;
          SET @colposcopy_findings := null;
          SET @cervical_lesion_size := null;
          SET @other_dysplasia_procedure_done := null;
          SET @dysplasia_mgmt_plan := null;
          SET @other_dysplasia_mgmt_plan := null;
          SET @dysplasia_assesment_notes := null;
          SET @dysplasia_rtc_date := null;
          SET @most_recent_pap_smear_result := null;
          SET @leep_location := null;
          SET @pathology_cervical_exam_findings := null;
          SET @pathology_vaginal_exam_findings := null;
          SET @pathology_vulval_exam_findings := null;
          SET @endometrium_biopsy_exam_findings := null;
          SET @endocervical_curettage_exam_findings := null;
          SET @biopsy_results_mgmt_plan := null;
          SET @cancer_staging := null;
                                                
          drop temporary table if exists flat_cervical_cancer_screening_1;
          create temporary table flat_cervical_cancer_screening_1 #(index encounter_id (encounter_id))
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
                  when obs regexp "!!1834=9651!!" then @reasons_for_current_visit := 1 -- Annual screening
                  when obs regexp "!!1834=1154!!" then @reasons_for_current_visit := 2 -- New complaints
                  when obs regexp "!!1834=1246!!" then @reasons_for_current_visit := 3 -- Scheduled visit
                  when obs regexp "!!1834=5622!!" then @reasons_for_current_visit := 4 -- Other (non-coded)
                  else @reasons_for_current_visit := null
              end as reasons_for_current_visit,
              case
                  when obs regexp "!!1839=1838!!" then @cur_visit_type := 1 -- Unscheduled visit late
                  when obs regexp "!!1839=1837!!" then @cur_visit_type := 2 -- Unscheduled visit early
                  when obs regexp "!!1839=1246!!" then @cur_visit_type := 3 -- Scheduled visit
                  when obs regexp "!!1839=7850!!" then @cur_visit_type := 4 -- Initial visit
                  when obs regexp "!!1839=9569!!" then @cur_visit_type := 5 -- Peer follow-up visit
                  else @cur_visit_type := null
              end as cur_visit_type,
              case
                  when obs regexp "!!7029=" then @actual_scheduled_date := replace(replace((substring_index(substring(obs,locate("!!7029=",obs)),@sep,1)),"!!7029=",""),"!!","")
                  else @actual_scheduled_date := null
              end as actual_scheduled_date,
              case
                  when obs regexp "!!5624=[0-9]" then @gravida := cast(replace(replace((substring_index(substring(obs,locate("!!5624=",obs)),@sep,1)),"!!5624=",""),"!!","") as unsigned)
                  else @gravida := null
              end as gravida,
              case
                  when obs regexp "!!1053=[0-9]" then @parity := cast(replace(replace((substring_index(substring(obs,locate("!!1053=",obs)),@sep,1)),"!!1053=",""),"!!","") as unsigned)
                  else @parity := null
              end as parity,
              case
                  when obs regexp "!!2061=5989!!" then @menstruation_status := 1 -- Menstruating 
                  when obs regexp "!!2061=6496!!" then @menstruation_status := 2 -- Post-menopausal
                  else @menstruation_status := null
              end as menstruation_status,
              case
                  when obs regexp "!!1836=" then @last_menstrual_period_date := replace(replace((substring_index(substring(obs,locate("!!1836=",obs)),@sep,1)),"!!1836=",""),"!!","")
                  else @last_menstrual_period_date := null
              end as last_menstrual_period_date,
              case
                  when obs regexp "!!8351=1065!!" then @pregnancy_status := 1 -- Yes
                  when obs regexp "!!8351=1066!!" then @pregnancy_status := 0 -- No
                  else @pregnancy_status := null
              end as pregnancy_status, 
              case
                  when obs regexp "!!5596=" then @estimated_delivery_date := replace(replace((substring_index(substring(obs,locate("!!5596=",obs)),@sep,1)),"!!5596=",""),"!!","")
                  else @estimated_delivery_date := null
              end as estimated_delivery_date,
              case
                  when obs regexp "!!9733=9729!!" then @reason_not_pregnant := 1 -- Pregnancy not suspected
                  when obs regexp "!!9733=9730!!" then @reason_not_pregnant := 2 -- Pregnancy test is negative
                  when obs regexp "!!9733=9731!!" then @reason_not_pregnant := 3 -- Hormonal contraception
                  when obs regexp "!!9733=9732!!" then @reason_not_pregnant := 4 -- Postpartum less than six weeks
                  else @reason_not_pregnant := null
              end as reason_not_pregnant,
              case
                  when obs regexp "!!6709=664!!" then @hiv_status := 1 -- Negative
                  when obs regexp "!!6709=703!!" then @hiv_status := 2 -- Positive
                  when obs regexp "!!6709=1067!!" then @hiv_status := 3 -- Unknown
                  else @hiv_status := null
              end as hiv_status,
              case
                  when obs regexp "!!856=[0-9]"  then @viral_load := cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") as unsigned)
                  else @viral_load := null
              end as viral_load,
              case
                  when obs regexp "!!000=" then @viral_load_date := replace(replace((substring_index(substring(obs,locate("!!000=",obs)),@sep,1)),"!!000=",""),"!!","")
                  else @viral_load_date := null
              end as viral_load_date,
              case
                  when obs regexp "!!9589=1065!!" then @prior_via_done := 1 -- Yes
                  when obs regexp "!!9589=1066!!" then @prior_via_done := 0 -- No
                  else @prior_via_done := null
              end as prior_via_done,
              case
                  when obs regexp "!!7381=664!!" then @prior_via_result := 1 -- Positive
                  when obs regexp "!!7381=703!!" then @prior_via_result := 2 -- Negative
                  else @prior_via_result := null
              end as prior_via_result,
              case
                  when obs regexp "!!000=" then @prior_via_date := replace(replace((substring_index(substring(obs,locate("!!000=",obs)),@sep,1)),"!!000=",""),"!!","")
                  else @prior_via_date := null
              end as prior_via_date,
              case
                  when obs regexp "!!9590=1115!!" then @cur_via_result := 1 -- Normal
                  when obs regexp "!!9590=7469!!" then @cur_via_result := 2 -- Acetowhite lesion
                  when obs regexp "!!9590=9593!!" then @cur_via_result := 3 -- Friable tissue
                  when obs regexp "!!9590=7472!!" then @cur_via_result := 4 -- Atypical blood vessels
                  when obs regexp "!!9590=7293!!" then @cur_via_result := 5 -- Ulcer
                  when obs regexp "!!9590=7470!!" then @cur_via_result := 6 -- Punctuated capillaries
                  when obs regexp "!!9590=6497!!" then @cur_via_result := 7 -- Dysfunctional uterine bleeding
                  when obs regexp "!!9590=5245!!" then @cur_via_result := 8 -- Pallor
                  when obs regexp "!!9590=9591!!" then @cur_via_result := 9 -- Oysterwhite lesion
                  when obs regexp "!!9590=9592!!" then @cur_via_result := 10 -- Bright white lesion
                  else @cur_via_result := null
              end as cur_via_result,
              case
                  when obs regexp "!!7484=1115!!" then @visual_impression_cervix := 1 -- Normal
                  when obs regexp "!!7484=7507!!" then @visual_impression_cervix := 2 -- Positive VIA with Aceto white area
                  when obs regexp "!!7484=7508!!" then @visual_impression_cervix := 3 -- Positive VIA with suspicious lesion
                  when obs regexp "!!7484=7424!!" then @visual_impression_cervix := 4 -- Cervical intraepithelial neoplasia grade 1
                  when obs regexp "!!7484=7425!!" then @visual_impression_cervix := 5 -- Cervical intraepithelial neoplasia grade 2
                  when obs regexp "!!7484=7216!!" then @visual_impression_cervix := 6 -- Cervical intraepithelial neoplasia grade 3
                  when obs regexp "!!7484=6537!!" then @visual_impression_cervix := 7 -- Cervical cancer
                  when obs regexp "!!7484=7421!!" then @visual_impression_cervix := 8 -- Squamous cell carcinoma, not otherwise specified
                  when obs regexp "!!7484=5622!!" then @visual_impression_cervix := 9 -- Other (non-coded)
                  else @visual_impression_cervix := null
              end as visual_impression_cervix,
              case
                  when obs regexp "!!7490=1115!!" then @visual_impression_vagina := 1 -- Normal
                  when obs regexp "!!7490=1447!!" then @visual_impression_vagina := 2 -- Warts, genital
                  when obs regexp "!!7490=9181!!" then @visual_impression_vagina := 3 -- Suspicious of cancer, vaginal lesion
                  when obs regexp "!!7490=1116!!" then @visual_impression_vagina := 4 -- Abnormal
                  when obs regexp "!!7490=9177!!" then @visual_impression_vagina := 5 -- Suspicious of cancer, vulva lesion
                  when obs regexp "!!7490=7492!!" then @visual_impression_vagina := 6 -- Vaginal intraepithelial neoplasia grade 1
                  when obs regexp "!!7490=7491!!" then @visual_impression_vagina := 7 -- Vaginal intraepithelial neoplasia grade 2
                  when obs regexp "!!7490=7435!!" then @visual_impression_vagina := 8 -- Vaginal intraepithelial neoplasia grade 3
                  when obs regexp "!!7490=5622!!" then @visual_impression_vagina := 9 -- Other (non-coded)
                  else @visual_impression_vagina := null
              end as visual_impression_vagina,
              case
                  when obs regexp "!!7490=1115!!" then @visual_impression_vulva := 1 -- Normal
                  when obs regexp "!!7490=1447!!" then @visual_impression_vulva := 2 -- Warts, genital
                  when obs regexp "!!7490=9177!!" then @visual_impression_vulva := 3 -- Suspicious of cancer, vulva lesion
                  when obs regexp "!!7490=1116!!" then @visual_impression_vulva := 4 -- Abnormal
                  when obs regexp "!!7490=7489!!" then @visual_impression_vulva := 5 -- Condyloma or vulvar intraepithelial neoplasia grade 1
                  when obs regexp "!!7490=7488!!" then @visual_impression_vulva := 6 -- Vulvar intraepithelial neoplasia grade 2
                  when obs regexp "!!7490=7483!!" then @visual_impression_vulva := 7 -- Vulvar intraepithelial neoplasia grade 3
                  when obs regexp "!!7490=5622!!" then @visual_impression_vulva := 8 -- Other (non-coded)
                  else @visual_impression_vulva := null
              end as visual_impression_vulva,
              case
                  when obs regexp "!!7479=1107!!" then @via_procedure_done := 1 -- None
                  when obs regexp "!!7479=6511!!" then @via_procedure_done := 2 -- Excisional/surgical biopsy
                  when obs regexp "!!7479=7466!!" then @via_procedure_done := 3 -- Cryotherapy
                  when obs regexp "!!7479=7147!!" then @via_procedure_done := 4 -- LEEP
                  when obs regexp "!!7479=9724!!" then @via_procedure_done := 5 -- Cervical polypectomy
                  when obs regexp "!!7479=6510!!" then @via_procedure_done := 6 -- Core needle biopsy
                  when obs regexp "!!7479=885!!" then @via_procedure_done := 7 -- Papanicolaou smear
                  when obs regexp "!!7479=7478!!" then @via_procedure_done := 8 -- Endocervical curettage
                  when obs regexp "!!7479=7553!!" then @via_procedure_done := 9 -- Closure by suture
                  when obs regexp "!!7479=1907!!" then @via_procedure_done := 10 -- Plaster services
                  when obs regexp "!!7479=441!!" then @via_procedure_done := 11 -- Clean and dressing
                  when obs regexp "!!7479=2062!!" then @via_procedure_done := 12 -- Circumcized
                  when obs regexp "!!7479=7648!!" then @via_procedure_done := 13 -- Minor surgical procedure
                  when obs regexp "!!7479=9434!!" then @via_procedure_done := 14 -- Visual inspection with acetic acid
                  when obs regexp "!!7479=5275!!" then @via_procedure_done := 15 -- Intrauterine device
                  when obs regexp "!!7479=6220!!" then @via_procedure_done := 16 -- Contraceptive implant
                  when obs regexp "!!7479=5622!!" then @via_procedure_done := 17 -- Other (non-coded)
                  else @via_procedure_done := null
              end as via_procedure_done,
              case
                  when obs regexp "!!1915=" then @other_via_procedure_done := replace(replace((substring_index(substring(obs,locate("!!1915=",obs)),@sep,1)),"!!1915=",""),"!!","")
                  else @other_via_procedure_done := null
              end as other_via_procedure_done,
              case
                  when obs regexp "!!7500=9725!!" then @via_management_plan := 1 -- Return for result
                  when obs regexp "!!7500=9178!!" then @via_management_plan := 2 -- VIA follow-up in three months
                  when obs regexp "!!7500=7497!!" then @via_management_plan := 3 -- Complete VIA in three years
                  when obs regexp "!!7500=7383!!" then @via_management_plan := 4 -- Colposcopy
                  when obs regexp "!!7500=7499!!" then @via_management_plan := 5 -- Gynecologic oncology services
                  when obs regexp "!!7500=7496!!" then @via_management_plan := 6 -- Complete VIA in one year
                  when obs regexp "!!7500=7498!!" then @via_management_plan := 7 -- Complete VIA or Pap smear in six months
                  when obs regexp "!!7500=7465!!" then @via_management_plan := 8 -- Surgery       
                  when obs regexp "!!7500=7466!!" then @via_management_plan := 9 -- Cryotherapy         
                  when obs regexp "!!7500=1496!!" then @via_management_plan := 10 -- Clinician       
                  when obs regexp "!!7500=1107!!" then @via_management_plan := 11 -- None              
                  when obs regexp "!!7500=6102!!" then @via_management_plan := 12 -- Discontinue             
                  when obs regexp "!!7500=7478!!" then @via_management_plan := 13 -- Endocervical curettage    
                  when obs regexp "!!7500=6511!!" then @via_management_plan := 14 -- Excisional/Surgical biopsy                 
                  when obs regexp "!!7500=5276!!" then @via_management_plan := 15 -- Female sterilization             
                  when obs regexp "!!7500=10200!!" then @via_management_plan := 16 --  Repeat procedure           
                  when obs regexp "!!7500=5622!!" then @via_management_plan := 17 -- Other (non-coded)
                  else @via_management_plan := null
              end as via_management_plan, 
              case
                  when obs regexp "!!1915=" then @other_via_management_plan := replace(replace((substring_index(substring(obs,locate("!!1915=",obs)),@sep,1)),"!!1915=",""),"!!","")
                  else @other_via_management_plan := null
              end as other_via_management_plan,
              case
                  when obs regexp "!!7222=" then @via_assessment_notes := replace(replace((substring_index(substring(obs,locate("!!7222=",obs)),@sep,1)),"!!7222=",""),"!!","")
                  else @via_assessment_notes := null
              end as via_assessment_notes,
              case
                  when obs regexp "!!5096=" then @via_rtc_date := replace(replace((substring_index(substring(obs,locate("!!5096=",obs)),@sep,1)),"!!5096=",""),"!!","")
                  else @via_rtc_date := null
              end as via_rtc_date, 
              case
                  when obs regexp "!!7379=1065!!" then @history_of_dysplasia := 1 -- Yes
                  when obs regexp "!!7379=1066!!" then @history_of_dysplasia := 0 -- No
                  else @history_of_dysplasia := null
              end as history_of_dysplasia,
			  case
			    when obs regexp "!!7381=664!!" then @previous_via_result := 1 -- Negative
				when obs regexp "!!7381=703!!" then @previous_via_result := 2 -- Positive
				else @previous_via_result := null
			  end as previous_via_result,
              case
                  when obs regexp "!!000=" then @previous_via_result_date := replace(replace((substring_index(substring(obs,locate("!!000=",obs)),@sep,1)),"!!000=",""),"!!","")
                  else @previous_via_result_date := null
              end as previous_via_result_date,
              case
                  when obs regexp "!!7423=1115!!" then @most_recent_pap_smear_result := 1 -- Normal
                  when obs regexp "!!7423=7417!!" then @most_recent_pap_smear_result := 2 -- Atypical squamous cells of undetermined significance
                  when obs regexp "!!7423=7418!!" then @most_recent_pap_smear_result := 3 -- Atypical glandular cells of undetermined significance
                  when obs regexp "!!7423=7419!!" then @most_recent_pap_smear_result := 4 -- Low grade squamous intraepithelial lesion
                  when obs regexp "!!7423=7420!!" then @most_recent_pap_smear_result := 5 -- High grade squamous intraepithelial lesion
                  when obs regexp "!!7423=7421!!" then @most_recent_pap_smear_result := 6 -- Squamous cell carcinoma, not otherwise specified
                  when obs regexp "!!7423=7422!!" then @most_recent_pap_smear_result := 7 -- Adenocarcinoma
                  else @most_recent_pap_smear_result := null
              end as most_recent_pap_smear_result, 
              case
                  when obs regexp "!!7426=1115!!" then @most_recent_biopsy_result := 1
                  when obs regexp "!!7426=7424!!" then @most_recent_biopsy_result := 2
                  when obs regexp "!!7426=7425!!" then @most_recent_biopsy_result := 3
                  when obs regexp "!!7426=7216!!" then @most_recent_biopsy_result := 4
                  when obs regexp "!!7426=7421!!" then @most_recent_biopsy_result := 5
                  else @most_recent_biopsy_result := null
              end as most_recent_biopsy_result, 
              case
                  when obs regexp "!!000=" then @prior_biopsy_result_date := replace(replace((substring_index(substring(obs,locate("!!000=",obs)),@sep,1)),"!!000=",""),"!!","")
                  else @prior_biopsy_result_date := null
              end as prior_biopsy_result_date,
              case
                  when obs regexp "!!7400=" then @most_recent_biopsy_result_other := replace(replace((substring_index(substring(obs,locate("!!7400=",obs)),@sep,1)),"!!7400=",""),"!!","")
                  else @most_recent_biopsy_result_other := null
              end as most_recent_biopsy_result_other,
              case
                  when obs regexp "!!000=" then @prior_biopsy_result_date_other := replace(replace((substring_index(substring(obs,locate("!!000=",obs)),@sep,1)),"!!000=",""),"!!","")
                  else @prior_biopsy_result_date_other := null
              end as prior_biopsy_result_date_other,
              case
                  when obs regexp "!!9706=" then @date_patient_informed_referred := replace(replace((substring_index(substring(obs,locate("!!9706=",obs)),@sep,1)),"!!9706=",""),"!!","")
                  else @date_patient_informed_referred := null
              end as date_patient_informed_referred,
              case
                  when obs regexp "!!7467=7466!!" then @past_dysplasia_treatment := 1 -- Crotherapy 
                  when obs regexp "!!7467=7147!!" then @past_dysplasia_treatment := 2 -- LEEP
                  when obs regexp "!!7467=7465!!" then @past_dysplasia_treatment := 3 -- Surgery
                  when obs regexp "!!7467=5622!!" then @past_dysplasia_treatment := 4 -- Other (non-coded)
                  else @past_dysplasia_treatment := null
              end as past_dysplasia_treatment, 
              case
                  when obs regexp "!!7579=1115!!" then @treatment_specimen_pathology := 1
                  when obs regexp "!!7579=149!!" then @treatment_specimen_pathology := 2
                  when obs regexp "!!7579=9620!!" then @treatment_specimen_pathology := 3
                  when obs regexp "!!7579=7424!!" then @treatment_specimen_pathology := 4
                  when obs regexp "!!7579=7425!!" then @treatment_specimen_pathology := 5
                  when obs regexp "!!7579=7216!!" then @treatment_specimen_pathology := 6
                  when obs regexp "!!7579=7421!!" then @treatment_specimen_pathology := 7
                  when obs regexp "!!7579=9618!!" then @treatment_specimen_pathology := 8
                  else @treatment_specimen_pathology := null
              end as treatment_specimen_pathology, 
              case
                  when obs regexp "!!7428=1065!!" then @satisfactory_colposcopy := 1
                  when obs regexp "!!7428=1066!!" then @satisfactory_colposcopy := 0
                  when obs regexp "!!7428=1118!!" then @satisfactory_colposcopy := 2
                  else @satisfactory_colposcopy := null
              end as satisfactory_colposcopy, 
              case
                  when obs regexp "!!7383=1115!!" then @colposcopy_findings := 1
                  when obs regexp "!!7383=7469!!" then @colposcopy_findings := 2
                  when obs regexp "!!7383=7473!!" then @colposcopy_findings := 3
                  when obs regexp "!!7383=7470!!" then @colposcopy_findings := 4
                  when obs regexp "!!7383=7471!!" then @colposcopy_findings := 5
                  when obs regexp "!!7383=7472!!" then @colposcopy_findings := 6
                  else @colposcopy_findings := null
              end as colposcopy_findings, 
              case
                  when obs regexp "!!7477=7474!!" then @cervical_lesion_size := 1
                  when obs regexp "!!7477=9619!!" then @cervical_lesion_size := 2
                  when obs regexp "!!7477=7476!!" then @cervical_lesion_size := 3
                  else @cervical_lesion_size := null
              end as cervical_lesion_size, 
              case
                  when obs regexp "!!1915=" then @other_dysplasia_procedure_done := replace(replace((substring_index(substring(obs,locate("!!1915=",obs)),@sep,1)),"!!1915=",""),"!!","")
                  else @other_dysplasia_procedure_done := null
              end as other_dysplasia_procedure_done,
              case
                  when obs regexp "!!7500=9725!!" then @dysplasia_mgmt_plan := 1
                  when obs regexp "!!7500=9178!!" then @dysplasia_mgmt_plan := 2
                  when obs regexp "!!7500=7497!!" then @dysplasia_mgmt_plan := 3
                  when obs regexp "!!7500=7383!!" then @dysplasia_mgmt_plan := 4
                  when obs regexp "!!7500=7499!!" then @dysplasia_mgmt_plan := 5
                  when obs regexp "!!7500=5622!!" then @dysplasia_mgmt_plan:= 6
                  else @dysplasia_mgmt_plan := null
              end as dysplasia_mgmt_plan, 
              case
                  when obs regexp "!!1915=" then @other_dysplasia_mgmt_plan := replace(replace((substring_index(substring(obs,locate("!!1915=",obs)),@sep,1)),"!!1915=",""),"!!","")
                  else @other_dysplasia_mgmt_plan := null
                  end as other_dysplasia_mgmt_plan,
              case
                  when obs regexp "!!7222=" then @dysplasia_assesment_notes := replace(replace((substring_index(substring(obs,locate("!!7222=",obs)),@sep,1)),"!!7222=",""),"!!","")
                  else @dysplasia_assesment_notes := null
                  end as dysplasia_assesment_notes,
              case
                  when obs regexp "!!5096=" then @dysplasia_rtc_date := replace(replace((substring_index(substring(obs,locate("!!5096=",obs)),@sep,1)),"!!5096=",""),"!!","")
                  else @dysplasia_rtc_date := null
              end as dysplasia_rtc_date,
              case
                  when obs regexp "!!8268=8266!!" then @leep_location := 1 -- Superficial 
                  when obs regexp "!!8268=8267!!" then @leep_location := 2 -- Deep
                  else @leep_location := null
              end as leep_location,
              case
                  when obs regexp "!!7645=1115!!" then @pathology_cervical_exam_findings := 1
                  when obs regexp "!!7645=7424!!" then @pathology_cervical_exam_findings := 2
                  when obs regexp "!!7645=7425!!" then @pathology_cervical_exam_findings := 3
                  when obs regexp "!!7645=7216!!" then @pathology_cervical_exam_findings := 4
                  when obs regexp "!!7645=1447!!" then @pathology_cervical_exam_findings := 5
                  when obs regexp "!!7645=149!!" then  @pathology_cervical_exam_findings := 6
                  when obs regexp "!!7645=8282!!" then @pathology_cervical_exam_findings := 7
                  when obs regexp "!!7645=9620!!" then @pathology_cervical_exam_findings := 8
                  when obs regexp "!!7645=8276!!" then @pathology_cervical_exam_findings := 9
                  when obs regexp "!!7645=9617!!" then @pathology_cervical_exam_findings := 10
                  when obs regexp "!!7645=9621!!" then @pathology_cervical_exam_findings := 11
                  when obs regexp "!!7645=7421!!" then @pathology_cervical_exam_findings := 12
                  when obs regexp "!!7645=7422!!" then @pathology_cervical_exam_findings := 13
                  when obs regexp "!!7645=9618!!" then @pathology_cervical_exam_findings := 14
              else @pathology_cervical_exam_findings := null
              end as pathology_cervical_exam_findings,
              case
                  when obs regexp "!!7647=1115!!" then @pathology_vaginal_exam_findings := 1
                  when obs regexp "!!7647=7492!!" then @pathology_vaginal_exam_findings := 2
                  when obs regexp "!!7647=7491!!" then @pathology_vaginal_exam_findings := 3
                  when obs regexp "!!7647=7435!!" then @pathology_vaginal_exam_findings := 4
                  when obs regexp "!!7647=6537!!" then @pathology_vaginal_exam_findings := 5
                  when obs regexp "!!7647=1447!!" then @pathology_vaginal_exam_findings := 6
                  when obs regexp "!!7647=8282!!" then @pathology_vaginal_exam_findings := 7
                  when obs regexp "!!7647=9620!!" then @pathology_vaginal_exam_findings := 8
                  when obs regexp "!!7647=8276!!" then @pathology_vaginal_exam_findings := 9
                  when obs regexp "!!7647=9617!!" then @pathology_vaginal_exam_findings := 10
                  when obs regexp "!!7647=9621!!" then @pathology_vaginal_exam_findings := 11
                  when obs regexp "!!7647=7421!!" then @pathology_vaginal_exam_findings := 12
                  when obs regexp "!!7647=7422!!" then @pathology_vaginal_exam_findings := 13
                  when obs regexp "!!7647=9618!!" then @pathology_vaginal_exam_findings := 14
              else @pathology_vaginal_exam_findings := null
              end as pathology_vaginal_exam_findings,
              case
                  when obs regexp "!!7646=1115!!" then @pathology_vulval_exam_findings := 1 -- Normal
                  when obs regexp "!!7646=7489!!" then @pathology_vulval_exam_findings := 2 -- Condyloma or vulvar intraepithelial neoplasia grade 1
                  when obs regexp "!!7646=7488!!" then @pathology_vulval_exam_findings := 3 -- Vulvar intraepithelial neoplasia grade 2
                  when obs regexp "!!7646=7483!!" then @pathology_vulval_exam_findings := 4 -- Vulvar intraepithelial neoplasia grade 3
                  when obs regexp "!!7646=9618!!" then @pathology_vulval_exam_findings := 5 -- Invasive adenocarcinoma
                  when obs regexp "!!7646=1447!!" then @pathology_vulval_exam_findings := 6 -- Warts, genital
                  when obs regexp "!!7646=8282!!" then @pathology_vulval_exam_findings := 7 -- Cervical squamous metaplasia
                  when obs regexp "!!7646=9620!!" then @pathology_vulval_exam_findings := 8 -- Condylomata
                  when obs regexp "!!7646=8276!!" then @pathology_vulval_exam_findings := 9 -- Cervical squamous cell carcinoma
                  when obs regexp "!!7646=9617!!" then @pathology_vulval_exam_findings := 10 -- Microinvasive carcinoma
                  when obs regexp "!!7646=9621!!" then @pathology_vulval_exam_findings := 11 -- Adenocarcinoma in situ
                  when obs regexp "!!7646=7421!!" then @pathology_vulval_exam_findings := 12 -- Squamous cell carcinoma, otherwise not specified
                  when obs regexp "!!7646=7422!!" then @pathology_vulval_exam_findings := 13 -- Adenocarcinoma
                  else @pathology_vulval_exam_findings := null
              end as pathology_vulval_exam_findings,
              case
                  when obs regexp "!!10207=1115!!" then @endometrium_biopsy_exam_findings := 1 -- Normal
                  when obs regexp "!!10207=8276!!" then @endometrium_biopsy_exam_findings := 2 -- Cervical squamous cell carcinoma
                  when obs regexp "!!10207=9617!!" then @endometrium_biopsy_exam_findings := 3 -- Microinvasive carcinoma
                  when obs regexp "!!10207=9621!!" then @endometrium_biopsy_exam_findings := 4 -- Adenocarcinoma in situ
                  when obs regexp "!!10207=9618!!" then @endometrium_biopsy_exam_findings := 5 -- Invasive adenocarcinoma
                  when obs regexp "!!10207=7421!!" then @endometrium_biopsy_exam_findings := 6 -- Squamous cell carcinoma, otherwise not specified
                  when obs regexp "!!10207=8282!!" then @endometrium_biopsy_exam_findings := 7 -- Cervical squamous metaplasia
                  when obs regexp "!!10207=9620!!" then @endometrium_biopsy_exam_findings := 8 -- Condylomata
                  when obs regexp "!!10207=7422!!" then @endometrium_biopsy_exam_findings := 9 -- Adenocarcinoma
                  else @endometrium_biopsy_exam_findings := null
              end as endometrium_biopsy_exam_findings,
              case
                  when obs regexp "!!10204=1115!!" then @endocervical_curettage_exam_findings := 1 -- Normal
                  when obs regexp "!!10204=7424!!" then @endocervical_curettage_exam_findings := 2 -- Cervical intraepithelial neoplasia grade 1
                  when obs regexp "!!10204=7425!!" then @endocervical_curettage_exam_findings := 3 -- Cervical intraepithelial neoplasia grade 2
                  when obs regexp "!!10204=7216!!" then @endocervical_curettage_exam_findings := 4 -- Cervical intraepithelial neoplasia grade 3
                  when obs regexp "!!10204=149!!" then  @endocervical_curettage_exam_findings := 5 -- Cervicitis
                  when obs regexp "!!10204=8282!!" then @endocervical_curettage_exam_findings := 6 -- Cervical squamous metaplasia
                  when obs regexp "!!10204=9620!!" then @endocervical_curettage_exam_findings := 7 -- Condylomata
                  when obs regexp "!!10204=8276!!" then @endocervical_curettage_exam_findings := 8 -- Cervical squamous cell carcinoma
                  when obs regexp "!!10204=9617!!" then @endocervical_curettage_exam_findings := 9 -- Microinvasive carcinoma
                  when obs regexp "!!10204=9621!!" then @endocervical_curettage_exam_findings := 10 -- Adenocarcinoma in situ
                  when obs regexp "!!10204=7421!!" then @endocervical_curettage_exam_findings := 11 -- Squamous cell carcinoma, otherwise not specified
                  when obs regexp "!!10204=7422!!" then @endocervical_curettage_exam_findings := 12 -- Adenocarcinoma
                  when obs regexp "!!10204=9618!!" then @endocervical_curettage_exam_findings := 13 -- Invasive adenocarcinoma
                  else @endocervical_curettage_exam_findings := null
              end as endocervical_curettage_exam_findings,
              case
                  when obs regexp "!!7500=9725!!" then @biopsy_results_mgmt_plan := 1 -- Return for result
                  when obs regexp "!!7500=9178!!" then @biopsy_results_mgmt_plan := 2 -- VIA follow-up in three months
                  when obs regexp "!!7500=7498!!" then @biopsy_results_mgmt_plan := 2 -- Complete VIA or Pap smear in six months
                  when obs regexp "!!7500=7496!!" then @biopsy_results_mgmt_plan := 3 -- Complete VIA in one year
                  when obs regexp "!!7500=7497!!" then @biopsy_results_mgmt_plan := 4 -- Complete VIA in three years
                  when obs regexp "!!7500=7499!!" then @biopsy_results_mgmt_plan := 5 -- Gynecologic oncology services
                  when obs regexp "!!7500=6105!!" then @biopsy_results_mgmt_plan := 6 -- Referred to clinician today
                  when obs regexp "!!7500=7147!!" then @biopsy_results_mgmt_plan := 7 -- LEEP
                  when obs regexp "!!7500=7466!!" then @biopsy_results_mgmt_plan := 8 -- Crotherapy
                  when obs regexp "!!7500=10200!!" then @biopsy_results_mgmt_plan := 9 -- Repeat procedure
                  else @biopsy_results_mgmt_plan := null
              end as biopsy_results_mgmt_plan,
              case
                  when obs regexp "!!9868=9852!!" then @cancer_staging := 1 -- Stage I
                  when obs regexp "!!9868=9856!!" then @cancer_staging := 2 -- Stage II
                  when obs regexp "!!9868=9860!!" then @cancer_staging := 3 -- Stage III
                  when obs regexp "!!9868=9864!!" then @cancer_staging := 4 -- Stage IV
                  else @cancer_staging := null
              end as cancer_staging,
              null as next_app_date
		
						from flat_cervical_cancer_screening_0 t1
							join amrs.person p using (person_id)
						order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
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


						alter table flat_cervical_cancer_screening_1 drop prev_id, drop cur_id;

						drop table if exists flat_cervical_cancer_screening_2;
						create temporary table flat_cervical_cancer_screening_2
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
                  when is_clinical_encounter then @cur_clinical_rtc_date := next_app_date
                  when @prev_id = @cur_id then @cur_clinical_rtc_date
                  else @cur_clinical_rtc_date:= null
							end as cur_clinical_rtc_date

							from flat_cervical_cancer_screening_1
							order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
						);

						alter table flat_cervical_cancer_screening_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;

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

						drop temporary table if exists flat_cervical_cancer_screening_3;
						create temporary table flat_cervical_cancer_screening_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
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
                  when is_clinical_encounter then @cur_clinical_rtc_date := next_app_date
                  when @prev_id = @cur_id then @cur_clinical_rtc_date
                  else @cur_clinical_rtc_date:= null
							end as cur_clinic_rtc_date
							from flat_cervical_cancer_screening_2 t1
							order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
                                        
					SELECT 
              COUNT(*)
          INTO @new_encounter_rows FROM
              flat_cervical_cancer_screening_3;
                    
          SELECT @new_encounter_rows;                    
          SET @total_rows_written = @total_rows_written + @new_encounter_rows;
          SELECT @total_rows_written;

					SET @dyn_sql=CONCAT('replace into ',@write_table,											  
            '(select
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
              reasons_for_current_visit,
              cur_visit_type,
              actual_scheduled_date,
              gravida,
              parity,
              menstruation_status,
              last_menstrual_period_date,
              pregnancy_status,
              estimated_delivery_date,
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
              history_of_dysplasia,
              previous_via_result,
              previous_via_result_date,
              most_recent_pap_smear_result, 
              most_recent_biopsy_result,
              prior_biopsy_result_date,
              most_recent_biopsy_result_other,
              prior_biopsy_result_date_other,
              date_patient_informed_referred,
              past_dysplasia_treatment,
              treatment_specimen_pathology,
              satisfactory_colposcopy,
              colposcopy_findings,
              cervical_lesion_size,
              other_dysplasia_procedure_done,
              dysplasia_mgmt_plan,
              other_dysplasia_mgmt_plan,
              dysplasia_assesment_notes,
              dysplasia_rtc_date,
              leep_location,
              pathology_cervical_exam_findings,
              pathology_vaginal_exam_findings,
              pathology_vulval_exam_findings,
              endometrium_biopsy_exam_findings,
              endocervical_curettage_exam_findings,
              biopsy_results_mgmt_plan,
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

              from flat_cervical_cancer_screening_3 t1
              join amrs.location t2 using (location_id))');       

					PREPARE s1 from @dyn_sql; 
          EXECUTE s1; 
					DEALLOCATE PREPARE s1;


					SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_cervical_cancer_screening_build_queue__0 t2 using (person_id);'); 
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