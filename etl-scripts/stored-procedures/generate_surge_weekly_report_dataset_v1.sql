DELIMITER $$
CREATE PROCEDURE `generate_surge_weekly_report_dataset_v1`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int , IN log boolean)
BEGIN
            set @primary_table := "surge_weekly_report_dataset";
            set @query_type = query_type;
            set @queue_table = "";

            set @total_rows_written = 0;
  			set @start = now();
  			set @table_version = "surge_weekly_report_dataset";

            set @last_update = null;
  			set @last_date_created = (select max(max_date_created) from etl.flat_obs);
            set @may_2019 = '2019-05-11';
			set @may_2015 = '2019-05-11';
 			set @october_2018 = '2018-10-01';

             
             
CREATE TABLE IF NOT EXISTS surge_weekly_report_dataset(
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
    elastic_id BIGINT,
    person_uuid CHAR(38),
    person_id INT,
    year_week VARCHAR(10),
    encounter_yw INT,
    encounter_id INT,
    encounter_datetime DATETIME,
    encounter_date DATETIME,
    end_date DATE,
    start_date DATE,
    birthdate DATE,
    age DECIMAL(23 , 2 ),
    gender VARCHAR(100),
    clinical_visit_number INT(11),
    prev_rtc_date DATETIME,
    rtc_date DATETIME,
    visit_this_week INT(1),
    on_schedule INT(1),
    early_appointment INT(1),
    early_appointment_this_week INT(1),
    late_appointment_this_week INT(11),
    days_since_rtc_date BIGINT(21),
    scheduled_this_week INT(1),
    unscheduled_this_week INT(0),
    tx2_visit_this_week INT(11),
    missed_tx2_visit_this_week INT(11),
    tx2_scheduled_this_week_but_came_early INT(11),
    death_date DATETIME,
    missed_appointment_this_week INT(11),
    ltfu INT(1),
    defaulted INT(1),
    missed INT(1),
    next_status BINARY(0),
    active_in_care_this_week INT(11),
    cur_arv_adherence VARCHAR(200),
    cur_who_stage INT(11),
    is_pre_art_this_week INT(11),
    arv_first_regimen_location_id INT(11),
    arv_first_regimen VARCHAR(500),
    arv_first_regimen_names VARCHAR(500),
    arv_first_regimen_start_date DATETIME,
    days_since_starting_arvs BIGINT(21),
    started_art_this_week INT(1),
    enrollment_date DATETIME,
    enrolled_this_week INT(1),
    art_revisit_this_week INT(0),
    cur_arv_meds VARCHAR(500),
    cur_arv_meds_names VARCHAR(500),
    cur_arv_meds_strict VARCHAR(500),
    cur_arv_line INT(11),
    cur_arv_line_strict INT(11),
    cur_arv_line_reported TINYINT(4),
    on_art_this_week BINARY(0),
    vl_1 INT(11),
    vl_1_date DATETIME,
    vl_2 INT(11),
    vl_2_date DATETIME,
    has_vl_this_week INT(1),
    is_suppressed INT(1),
    is_un_suppressed INT(1),
    days_since_last_vl BIGINT(21),
    due_for_vl_this_week INT(0),
    reason_for_needing_vl_this_week BINARY(0),
    cd4_1 DOUBLE,
    cd4_1_date DATETIME,
    child_hiv_status_disclosure_status BINARY(0),
    transfer_in_this_week INT(0),
    transfer_in_location_id INT(11),
    transfer_in_date DATETIME,
    transfer_out_this_week INT(0),
    transfer_out_location_id INT(11),
    transfer_out_date DATETIME,
    status VARCHAR(12),
    dc_eligible_cumulative INT(1),
    started_dc_this_week INT(1),
    location_id INT(11),
    tx2_scheduled_this_week INT(1),
    tx2_scheduled_honored INT(1),
    prev_id BIGINT(20),
    cur_id INT(11),
    prev_enc_id BIGINT(20),
    cur_enc_id INT(11),
    clinical_visit_num BIGINT(63),
    prev_status LONGTEXT,
    cur_status VARCHAR(12),
    cur_prep_this_week INT(11),
    new_prep_this_week INT(11),
    height DOUBLE,
    weight DOUBLE,
    bmi DOUBLE,
    scheduled_this_week_and_due_for_vl INT(0),
    unscheduled_this_week_and_due_for_vl INT(0),
    overdue_for_vl_active INT(0),
    due_for_vl_has_vl_order INT(0),
    due_for_vl_dont_have_order INT(0),
    ltfu_this_week INT(0),
    missed_this_week INT(0),
    all_ltfus INT(0),
    ltfu_surge_baseline INT(0),
    surge_ltfu_and_ltfu_after_may INT(0),
    surge_ltfu_and_still_ltfu INT(0),
    newly_ltfu_this_week INT(0),
    ltfu_cumulative_outcomes_death INT(0),
    ltfu_cumulative_outcomes_transfer_out INT(0),
    ltfu_cumulative_outcomes_active INT(0),
    active_to_ltfu_count INT(0),
    defaulted_this_week INT(0),
    due_for_vl_this_week_active INT(0),
    on_schedule_this_week INT(0),
    ltfu_cumulative_outcomes_total INT(0),
    old_ltfus_to_active_this_week INT(0),
    tx2_unscheduled_this_week INT(0),
    dead_this_week INT(0),
    non_ltfu_dead_this_week INT(0),
    cumulative_dead INT(0),
    dc_eligible_this_week INT(0),
    dc_eligible_and_scheduled_this_week INT(0),
    active_on_dc INT(0),
    is_ltfu_surge_baseline INT(0),
    
    baseline_location VARCHAR(100),
    was_ltfu_may_19 INT(0),
    was_active_october_18 INT(0),
    not_elligible_for_dc INT(0),
    eligible_and_on_dc INT(0),
    eligible_not_on_dc INT(0),
    eligible_not_on_dc_and_scheduled_this_week INT(0),
    eligible_not_on_dc_and_unscheduled_this_week INT(0),
    eligible_and_on_dc_and_scheduled_this_week INT(0),
    eligible_and_on_dc_and_unscheduled_this_week INT(0),
    elligible_total INT(0),
    not_elligible_and_on_dc INT(0),
    elligible_total_revised INT(0),
    ltfu_transfer_out_this_week INT(0),
    ltfu_death_this_week INT(0),
    ltfu_active_this_week INT(0),
    
    is_ltfu_after_may INT(0),
    is_ltfu_after_may_total INT(0),
    
    
    week_patient_became_active  VARCHAR(100),
    med_pickup_rtc_date VARCHAR(100),
    days_since_med_pickup_rtc_date BIGINT(21),
    days_diff_med_rtc_and_next_clinical_date BIGINT(21),
    
    was_ltfu_before_october_2018 INT(0),
    old_ltfus_outcome_total INT(0),
    old_ltfus_to_active INT(0),
    intervention_done_this_week INT(0),
    interventions INT(0),
    due_for_vl_has_vl_order_scheduled INT(0),
    due_for_vl_has_vl_order_unscheduled INT(0),
    due_for_vl_dont_have_order_scheduled INT(0),
    due_for_vl_dont_have_order_unscheduled INT(0),
    has_vl_this_week_and_suppressed INT(0),
    has_vl_this_week_and_unsuppressed INT(0),
    missed_to_active_this_week INT(0),
    defaulter_to_active_this_week INT(0),
    missed_cumulative INT(0),
    med_surge_ltfus_cumulative INT(0),
    med_surge_ltfu_and_med_ltfu_after_may INT(0),
    med_defaulters INT(0),
    newly_med_ltfu_this_week INT(0),
    med_surge_ltfus_outcomes  INT(0),
    med_surge_ltfus_outcomes_this_week  INT(0),
    column_1 INT(0),
    column_2 INT(0),
    column_3 INT(0),
    column_4 VARCHAR(100),
    column_5 VARCHAR(100),
    column_6 VARCHAR(100),
    PRIMARY KEY elastic_id (elastic_id),
    INDEX person_enc_date (person_id , encounter_date),
    INDEX person_year_week (person_id , year_week),
    INDEX date_created (date_created),
    INDEX encounter_id (encounter_id),
    INDEX person_report_date (person_id , end_date),
    INDEX endDate_location_id (end_date , location_id),
    INDEX location_id (location_id),
    INDEX year_week (year_week),
    INDEX person_id (person_id),
    INDEX year_week_location_id (year_week , location_id)
);

  if (query_type = "build") then
        SELECT 'BUILDING.......................';
        set @write_table = concat("surge_weekly_report_dataset_temp",queue_number);
        set @queue_table = concat("surge_weekly_report_dataset_build_queue_",queue_number);   

        SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,'(person_id int primary key) (select * from surge_weekly_report_dataset_build_queue limit ',queue_size, ');'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;

        SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1; 
            
        SET @dyn_sql=CONCAT('delete t1 from surge_weekly_report_dataset_build_queue t1 join ',@queue_table, ' t2 using (person_id)'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
  end if;

  if (@query_type="sync") then
        SELECT 'SYNCING.......................';
        set @write_table = concat("surge_weekly_report_dataset");
        set @queue_table = "surge_weekly_report_dataset_sync_queue";                                       				
        create table if not exists surge_weekly_report_dataset_sync_queue (person_id int primary key);

        select @last_update := (select max(date_updated) from etl.flat_log where table_name=@table_version);

        select "Finding patients in amrs.encounters...";

                replace into surge_weekly_report_dataset_sync_queue
                (select distinct patient_id
                    from amrs.encounter
                    where date_changed > @last_update
                );
            
            
        select "Finding patients in flat_obs...";

                replace into surge_weekly_report_dataset_sync_queue
                (select distinct person_id
                    from etl.flat_obs
                    where max_date_created > @last_update
                );


        select "Finding patients in flat_lab_obs...";
                replace into surge_weekly_report_dataset_sync_queue
                (select distinct person_id
                    from etl.flat_lab_obs
                    where max_date_created > @last_update
                );

        select "Finding patients in flat_orders...";

                replace into surge_weekly_report_dataset_sync_queue
                (select distinct person_id
                    from etl.flat_orders
                    where max_date_created > @last_update
                );
                
                replace into surge_weekly_report_dataset_sync_queue
                (select person_id from 
                    amrs.person 
                    where date_voided > @last_update);


                replace into surge_weekly_report_dataset_sync_queue
                (select person_id from 
                    amrs.person 
                    where date_changed > @last_update);

  end if;

  # Remove test patients
    SET @dyn_sql=CONCAT('delete t1 FROM ',@queue_table,' t1
            join amrs.person_attribute t2 using (person_id)
            where t2.person_attribute_type_id=28 and value="true" and voided=0');
    PREPARE s1 from @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;
  
  SET @person_ids_count = 0;
  SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
  PREPARE s1 from @dyn_sql; 
  EXECUTE s1; 
  DEALLOCATE PREPARE s1;

  SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;
  
SELECT CONCAT('Patients in queue: ', @person_ids_count);
                          
set @total_time=0;
set @cycle_number = 0;

while @person_ids_count > 0 do

            set @loop_start_time = now();                        
            drop temporary table if exists surge_weekly_report_dataset_temporary_build_queue;
            create temporary table surge_weekly_report_dataset_temporary_build_queue (person_id int primary key);
            
            SET @dyn_sql=CONCAT('insert into surge_weekly_report_dataset_temporary_build_queue (select * from ',@queue_table,' limit ',cycle_size,');'); 
            PREPARE s1 from @dyn_sql; 
            EXECUTE s1; 
            DEALLOCATE PREPARE s1;
                
            SELECT CONCAT('creating patient_vl ...');

            CREATE Temporary TABLE if not exists patient_vl_status (
            person_id int(11) DEFAULT NULL,
            vl_1 int(11) DEFAULT NULL,
            vl_2 int(11) DEFAULT NULL,
            vl_2_date datetime DEFAULT NULL,
            vl_1_date datetime DEFAULT NULL,
            end_date date NOT NULL,
            week varchar(10) NOT NULL DEFAULT '',
            is_suppressed int(1) NOT NULL DEFAULT '0',
            is_un_suppressed int(1) NOT NULL DEFAULT '0',
            has_vl_this_week int(1) NOT NULL DEFAULT '0',
            days_since_last_vl int(11) DEFAULT NULL,
            Primary Key person_week (person_id,week),
            KEY person_id (person_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;


            replace into patient_vl_status(
            SELECT
            person_id,
            vl_1,
            vl_2,
            vl_2_date,
            vl_1_date,
            end_date,
            week,
            if(vl_1=0 or (vl_1 >= 0 and vl_1 <= 400),1,0 ) as is_suppressed,
            if(vl_1>400,1,0 ) as is_un_suppressed,
            if(yearweek(vl_1_date)=week,1,0 ) as has_vl_this_week,
            TIMESTAMPDIFF(DAY,
                vl_1_date,
                    end_date) AS days_since_last_vl

            FROM
                surge_week t1
                    JOIN
                (SELECT t1a.person_id, vl_1,vl_2,vl_2_date, vl_1_date,arv_start_date,encounter_datetime FROM
                etl.flat_hiv_summary_v15b t1a  Inner JOIN   surge_weekly_report_dataset_temporary_build_queue t3  on t1a.person_id=t3.person_id
            WHERE     t1a.vl_1_date IS NOT NULL) t2    
                
            WHERE   t2.encounter_datetime < DATE_ADD(t1.end_date, INTERVAL 1 DAY));


            SELECT CONCAT('creating surge_vitals ...');

            CREATE TEMPORARY TABLE if not exists surge_vitals (
            prev_id bigint(20) DEFAULT NULL,
            cur_id int(11) DEFAULT NULL,
            person_id int(11) DEFAULT NULL,
            encounter_id int(11) NOT NULL DEFAULT '0',
            encounter_datetime datetime DEFAULT NULL,
            height double DEFAULT NULL,
            weight double DEFAULT NULL,
            bmi double DEFAULT NULL,
            Primary KEY encounter_id (encounter_id),
            KEY person_id (person_id),
            KEY encounter_id (encounter_id),
            KEY person_id_2 (person_id,encounter_datetime),
            KEY encounter_datetime (encounter_datetime)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

            replace into surge_vitals (
            SELECT 
                @prev_id := @cur_id AS prev_id,
                @cur_id := t1.person_id AS cur_id,
                t1.person_id as person_id,
                t1.encounter_id, 
                encounter_datetime,
                CASE
                    WHEN @prev_id != @cur_id  THEN @height := height 
                    WHEN @prev_id = @cur_id and @height is null and height is not null THEN @height := height
                    WHEN @prev_id = @cur_id and @height  and height  THEN @height := height
                    ELSE @height 
                END AS height,
                CASE
                    WHEN @prev_id != @cur_id  THEN @weight := weight 
                    WHEN @prev_id = @cur_id and @weight is null and weight is not null THEN @weight := weight
                    WHEN @prev_id = @cur_id and @weight  and weight  THEN @weight := weight
                    ELSE @weight 
                END AS weight,
                (@weight / (@height * @height) * 10000)  as bmi
                
                from 
                (select encounter_id, height,weight,encounter_datetime,tv.person_id from  etl.flat_vitals tv 
                inner join surge_weekly_report_dataset_temporary_build_queue t2 on tv.person_id=t2.person_id
            order by t2.person_id , encounter_datetime) t1);

            set @age =null;
            set @status = null;



            SELECT CONCAT('creating surge_weekly_report_dataset_0 ...');    
            drop temporary table if exists surge_weekly_report_dataset_0;
            create temporary table surge_weekly_report_dataset_0               
            (index (person_id), index(start_date), index(end_date),index(encounter_date))
            (SELECT
                concat(t1.week,t2.person_id) as elastic_id,
                t2.uuid AS person_uuid,
                t2.person_id,
                t1.week AS year_week,
                    YEARWEEK(t2.encounter_datetime) encounter_yw,
                        t2.encounter_id,
                t2.encounter_datetime,  
                t2.next_clinical_datetime_hiv,
                if(rtc_date >= @october_2018 <= @may_2019, rtc_date, null) as base_rtc_date,
                DATE(t2.encounter_datetime) AS encounter_date,
                t1.end_date,
                t1.start_date,
                DATE(birthdate) AS birthdate,
                gender,
                CASE
                    WHEN
                        TIMESTAMPDIFF(YEAR, birthdate, end_date) > 0
                    THEN
                        @age:=ROUND(TIMESTAMPDIFF(YEAR, birthdate, end_date),
                                0)
                    ELSE @age:=ROUND(TIMESTAMPDIFF(MONTH,
                                birthdate,
                                end_date) / 12,
                            2)
                END AS age,
                prev_clinical_rtc_date_hiv as prev_rtc_date ,
                rtc_date,
                
                
                CASE
                    WHEN
                        med_pickup_rtc_date IS NOT NULL
                        AND visit_type NOT IN (24)  

                    THEN
                        @med_pickup_rtc_date := med_pickup_rtc_date

                    ELSE @med_pickup_rtc_date := NULL
                END AS med_pickup_rtc_date,
                
                # select med_pickup_rtc_date, year_week from surge_weekly_report_dataset_0 where person_id = 21417
                
                
                IF(YEARWEEK(t2.encounter_datetime) = t1.week,
                    1,
                    0) AS visit_this_week,
                IF(DATE(t2.encounter_datetime) = DATE(prev_clinical_rtc_date_hiv),
                    1,
                    0) AS on_schedule,
                IF((yearweek(prev_clinical_rtc_date_hiv)=t1.week  OR yearweek(rtc_date)=t1.week OR (prev_clinical_rtc_date_hiv is null and yearweek(rtc_date)=t1.week))  and  
                        YEARWEEK(t2.encounter_datetime) = t1.week,
                    1,
                    0) AS on_schedule_this_week,
                IF(DATE(t2.encounter_datetime) < DATE(prev_clinical_rtc_date_hiv),
                    1,
                    0) AS early_appointment,
                    
                IF(YEARWEEK(t2.encounter_datetime) = t1.week AND
                (t1.week < YEARWEEK(prev_clinical_rtc_date_hiv) OR (prev_clinical_rtc_date_hiv is null and t1.week < YEARWEEK(rtc_date)))
                ,1,0) AS early_appointment_this_week,

                IF(YEARWEEK(t2.encounter_datetime) = t1.week 
                AND (t1.week > YEARWEEK(prev_clinical_rtc_date_hiv) OR ( prev_clinical_rtc_date_hiv is null  and t1.week > YEARWEEK(rtc_date)))
                        ,1,0)  AS late_appointment_this_week,
                        
                TIMESTAMPDIFF(DAY,
                    rtc_date,
                    end_date) AS days_since_rtc_date,
                    
                        
                TIMESTAMPDIFF(DAY,
                    prev_clinical_rtc_date_hiv,
                    t2.encounter_datetime) AS days_diff_enc_date_and_prev_rtc,
                    
                TIMESTAMPDIFF(DAY,
                    @med_pickup_rtc_date,
                    end_date) AS days_since_med_pickup_rtc_date,  
                    
                TIMESTAMPDIFF(DAY,
                DATE(med_pickup_rtc_date),
                    DATE(next_clinical_datetime_hiv)
                    ) AS days_diff_med_rtc_and_next_clinical_date, 
                    
                IF(YEARWEEK(prev_clinical_rtc_date_hiv) = t1.week
                        OR  (prev_clinical_rtc_date_hiv is null  and  t1.week = YEARWEEK(rtc_date)) OR yearweek(rtc_date)=t1.week,
                    1,
                    0) AS scheduled_this_week,
                CASE
                    WHEN
                        YEARWEEK(t2.encounter_datetime) = t1.week
                        AND (yearweek(prev_clinical_rtc_date_hiv) != t1.week  OR (prev_clinical_rtc_date_hiv is null and yearweek(rtc_date) != t1.week))
                            AND ((DATE(t2.encounter_datetime) != DATE(prev_clinical_rtc_date_hiv))  OR ( prev_clinical_rtc_date_hiv is null  and (DATE(t2.encounter_datetime) != DATE(rtc_date))))
                    THEN
                        1
                    ELSE 0
                END AS unscheduled_this_week,
                
                #  NULL AS tx2_visit_this_week,
                #  NULL AS missed_tx2_visit_this_week,
                t2.death_date,
                NULL active_in_care_this_week,
                cur_arv_adherence,
                NULL cur_who_stage,
                NULL is_pre_art_this_week,
                arv_first_regimen_location_id,
                arv_first_regimen,
                etl.get_arv_names(arv_first_regimen) as arv_first_regimen_names,
                arv_first_regimen_start_date,
                TIMESTAMPDIFF(DAY,
                    arv_first_regimen_start_date,
                    end_date) AS days_since_starting_arvs,
                IF(YEARWEEK(arv_first_regimen_start_date) = t1.week,
                    1,
                    0) AS started_art_this_week,
                enrollment_date,
                IF(YEARWEEK(enrollment_date) = t1.week,
                    1,
                    0) AS enrolled_this_week,
                CASE
                    WHEN
                        YEARWEEK(t2.encounter_datetime) = t1.week
                            AND YEARWEEK(arv_first_regimen_start_date) != t1.week
                            AND cur_arv_meds IS NOT NULL
                            AND arv_first_regimen_start_date IS NOT NULL
                    THEN
                        1
                    ELSE 0
                END AS art_revisit_this_week,
                cur_arv_meds,
                etl.get_arv_names(cur_arv_meds) AS cur_arv_meds_names,
                cur_arv_meds_strict,
                cur_arv_line,
                cur_arv_line_strict,
                cur_arv_line_reported,
                NULL AS on_art_this_week,
                cd4_1,
                cd4_1_date,
                NULL AS child_hiv_status_disclosure_status,
                CASE
                    WHEN YEARWEEK(transfer_in_date) = t1.week THEN 1
                    ELSE 0
                END AS transfer_in_this_week,
                transfer_in_location_id,
                transfer_in_date,
                CASE
                    WHEN YEARWEEK(transfer_out_date) = t1.week THEN 1
                    ELSE 0
                END AS transfer_out_this_week,
                transfer_out_location_id,
                transfer_out_date,
                            
                t2.location_id,
                IF((DATE(arv_first_regimen_start_date) = DATE(t2.encounter_datetime)
                        AND YEARWEEK(rtc_date) = t1.week) or (DATE(arv_first_regimen_start_date) = DATE(prev_clinical_datetime_hiv)
                        AND YEARWEEK(prev_rtc_date) = t1.week) ,
                    1,
                    0) AS tx2_scheduled_this_week,
                IF(DATE(arv_first_regimen_start_date) = DATE(prev_clinical_datetime_hiv)
                        AND YEARWEEK(t2.encounter_datetime) = t1.week,
                    1,
                    0) AS tx2_visit_this_week,
                IF(YEARWEEK(arv_first_regimen_start_date) = YEARWEEK(prev_clinical_datetime_hiv)
                        AND YEARWEEK(t2.encounter_datetime) = t1.week AND YEARWEEK(prev_rtc_date) = t1.week,
                    1,
                    0) AS tx2_scheduled_honored,
                    
                    
                    is_pregnant,
                    edd,
                    @date_pregnant := if(edd is not null,DATE_ADD(edd, INTERVAL -9 MONTH),null) as date_confirmed_pregnant,
                    if(edd is not null and yearweek(edd) >= yearweek(t1.end_date),1,0) as is_still_pregnant,
                    if(@date_pregnant is not null and YEARWEEK(@date_pregnant) > yearweek(arv_first_regimen_start_date),1,0) as started_arv_b4_pregnancy,
                    if(@date_pregnant is not null and YEARWEEK(@date_pregnant) < yearweek(arv_first_regimen_start_date),1,0) as started_arv_after_pregnancy,
                    if(arv_first_regimen_start_date is not null and yearweek(arv_first_regimen_start_date) <= yearweek(t1.end_date) ,1,0) as patient_is_on_art,
                    breast_feeding,
                    breast_feeding_encounter_date,
                    


                    tb_tx_start_date, 
                        TIMESTAMPDIFF(MONTH,
                    tb_tx_start_date,
                    end_date) AS months_since_tb_tx_start_date,
                    CASE
                    WHEN YEARWEEK(vl_order_date) = t1.week THEN 1
                    ELSE 0
                END AS vl_ordered_this_week,
                if(YEARWEEK(t2.encounter_datetime)= t1.week and encounter_type=21,1,0) intervention_done_this_week,
                if( encounter_type=21,1,0) interventions,
                arv_start_date
            FROM
                surge_week t1
                    JOIN  etl.flat_hiv_summary_v15b t2
                    JOIN  surge_weekly_report_dataset_temporary_build_queue t5 USING (person_id)
                    left join ndwr.patient_breast_feeding bf USING (person_id)#ON(t2.person_id = bf.person_id AND breast_feeding_encounter_date < DATE_ADD(t1.end_date, INTERVAL 1 DAY))
                JOIN amrs.person t3 ON (t3.person_id = t2.person_id AND (t3.voided is null or t3.voided = 0))
                    
            WHERE
                t2.encounter_datetime < DATE_ADD(t1.end_date, INTERVAL 1 DAY)
                    AND ((t2.next_clinical_datetime_hiv IS NULL
                    OR t2.next_clinical_datetime_hiv >= DATE_ADD(t1.end_date, INTERVAL 1 DAY)) || (t2.rtc_date IS NULL
                    OR t2.rtc_date >= DATE_ADD(t1.end_date, INTERVAL 1 DAY)))
                    AND t2.encounter_datetime < DATE_ADD(t1.end_date, INTERVAL 1 DAY)
                    AND t2.is_clinical_encounter = 1
            ORDER BY t2.person_id , t2.encounter_datetime , rtc_date , end_date);

            drop temporary table if exists surge_weekly_report_dataset_01;
            CREATE TEMPORARY TABLE `surge_weekly_report_dataset_01` (
            `elastic_id` varchar(21) CHARACTER SET latin1 DEFAULT NULL,
            `person_uuid` varchar(100) CHARACTER SET latin1 DEFAULT NULL,
            `person_id` int(11) DEFAULT NULL,
            `year_week` varchar(10) CHARACTER SET latin1 NOT NULL DEFAULT '',
            `encounter_yw` int(6) DEFAULT NULL,
            `encounter_id` int(11) NOT NULL DEFAULT '0',
            `encounter_datetime` datetime DEFAULT NULL,
            `next_clinical_datetime_hiv` datetime DEFAULT NULL,
            `base_rtc_date` datetime DEFAULT NULL,
            `encounter_date` date DEFAULT NULL,
            `end_date` date NOT NULL,
            `start_date` date NOT NULL,
            `birthdate` date DEFAULT NULL,
            `gender` varchar(50) DEFAULT '',
            `age` decimal(23,2) DEFAULT NULL,
            `prev_rtc_date` datetime DEFAULT NULL,
            `rtc_date` datetime DEFAULT NULL,
            `med_pickup_rtc_date` varchar(19) DEFAULT NULL,
            `visit_this_week` int(1) NOT NULL DEFAULT '0',
            `on_schedule` int(1) NOT NULL DEFAULT '0',
            `on_schedule_this_week` int(1) NOT NULL DEFAULT '0',
            `early_appointment` int(1) NOT NULL DEFAULT '0',
            `early_appointment_this_week` int(1) NOT NULL DEFAULT '0',
            `late_appointment_this_week` int(1) NOT NULL DEFAULT '0',
            `days_since_rtc_date` bigint(21) DEFAULT NULL,
            `days_diff_enc_date_and_prev_rtc` bigint(21) DEFAULT NULL,
            `days_since_med_pickup_rtc_date` bigint(21) DEFAULT NULL,
            `days_diff_med_rtc_and_next_clinical_date` bigint(21) DEFAULT NULL,
            `scheduled_this_week` int(1) NOT NULL DEFAULT '0',
            `unscheduled_this_week` int(0) DEFAULT NULL,
            `death_date` datetime DEFAULT NULL,
            `active_in_care_this_week` binary(0) DEFAULT NULL,
            `cur_arv_adherence` varchar(200) CHARACTER SET latin1 DEFAULT NULL,
            `cur_who_stage` binary(0) DEFAULT NULL,
            `is_pre_art_this_week` binary(0) DEFAULT NULL,
            `arv_first_regimen_location_id` int(11) DEFAULT NULL,
            `arv_first_regimen` varchar(500) CHARACTER SET latin1 DEFAULT NULL,
            `arv_first_regimen_names` text CHARACTER SET latin1,
            `arv_first_regimen_start_date` datetime DEFAULT NULL,
            `days_since_starting_arvs` bigint(21) DEFAULT NULL,
            `started_art_this_week` int(1) NOT NULL DEFAULT '0',
            `enrollment_date` datetime DEFAULT NULL,
            `enrolled_this_week` int(1) NOT NULL DEFAULT '0',
            `art_revisit_this_week` int(0) DEFAULT NULL,
            `cur_arv_meds` varchar(500) CHARACTER SET latin1 DEFAULT NULL,
            `cur_arv_meds_names` text CHARACTER SET latin1,
            `cur_arv_meds_strict` varchar(500) CHARACTER SET latin1 DEFAULT NULL,
            `cur_arv_line` int(11) DEFAULT NULL,
            `cur_arv_line_strict` int(11) DEFAULT NULL,
            `cur_arv_line_reported` tinyint(4) DEFAULT NULL,
            `on_art_this_week` binary(0) DEFAULT NULL,
            `cd4_1` double DEFAULT NULL,
            `cd4_1_date` datetime DEFAULT NULL,
            `child_hiv_status_disclosure_status` binary(0) DEFAULT NULL,
            `transfer_in_this_week` int(0) DEFAULT NULL,
            `transfer_in_location_id` int(11) DEFAULT NULL,
            `transfer_in_date` datetime DEFAULT NULL,
            `transfer_out_this_week` int(0) DEFAULT NULL,
            `transfer_out_location_id` int(11) DEFAULT NULL,
            `transfer_out_date` datetime DEFAULT NULL,
            `location_id` int(11) DEFAULT NULL,
            `tx2_scheduled_this_week` int(1) NOT NULL DEFAULT '0',
            `tx2_visit_this_week` int(1) NOT NULL DEFAULT '0',
            `tx2_scheduled_honored` int(1) NOT NULL DEFAULT '0',
            `is_pregnant` tinyint(1) DEFAULT NULL,
            `edd` datetime DEFAULT NULL,
            `date_confirmed_pregnant` varchar(19) DEFAULT NULL,
            `is_still_pregnant` int(1) NOT NULL DEFAULT '0',
            `started_arv_b4_pregnancy` int(1) NOT NULL DEFAULT '0',
            `started_arv_after_pregnancy` int(1) NOT NULL DEFAULT '0',
            `patient_is_on_art` int(1) NOT NULL DEFAULT '0',
            `breast_feeding` int(11) DEFAULT NULL,
            `breast_feeding_encounter_date` datetime DEFAULT NULL,
            `tb_tx_start_date` datetime DEFAULT NULL,
            `months_since_tb_tx_start_date` bigint(21) DEFAULT NULL,
            `vl_ordered_this_week` int(0) DEFAULT NULL,
            `intervention_done_this_week` int(1) NOT NULL DEFAULT '0',
            `interventions` int(1) NOT NULL DEFAULT '0',
            `arv_start_date` datetime DEFAULT NULL,
            PRIMARY KEY(person_id, year_week),
            KEY `person_id` (`person_id`),
            KEY `start_date` (`start_date`),
            KEY `end_date` (`end_date`),
            KEY `encounter_date` (`encounter_date`)


            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

            replace into surge_weekly_report_dataset_01(select *
            from surge_weekly_report_dataset_0);
            SELECT 
                COUNT(*) patients_for_24
            FROM
                surge_weekly_report_dataset_0
            WHERE
                year_week = 201924;

            SELECT CONCAT('creating surge_weekly_report_dataset__1 ...');

            drop temporary table if exists surge_weekly_report_dataset_1;

            CREATE temporary TABLE surge_weekly_report_dataset_1 (SELECT sw0.*,
                vl.vl_1,
                vl.vl_1_date,
                vl.vl_2,
                vl.vl_2_date,
                vl.days_since_last_vl,

                CASE
                    WHEN vl_1 > 999
                                AND vl_1_date > arv_start_date
                                AND TIMESTAMPDIFF(MONTH,vl_1_date,sw0.end_date) >=3
                        THEN
                            1
                    #is pregnant and started_arv_b4_pregnancy
                    WHEN is_still_pregnant=1 AND started_arv_b4_pregnancy=1 and vl_1_date is null then 1
                    WHEN is_still_pregnant=1 AND started_arv_b4_pregnancy=1 and TIMESTAMPDIFF(MONTH,vl_1_date,date_confirmed_pregnant)>=1 then 1
                    WHEN is_still_pregnant=1 AND started_arv_b4_pregnancy=1 and yearweek(vl_1_date) >= yearweek(date_confirmed_pregnant) and TIMESTAMPDIFF(MONTH,vl_1_date,vl.end_date)>=6   then 1
                    
                    #is pregnant and started_arv_after_pregnancy
                    WHEN is_still_pregnant=1 AND patient_is_on_art=1 AND started_arv_after_pregnancy=1 and TIMESTAMPDIFF(MONTH,arv_first_regimen_start_date,sw0.end_date)>=3 and vl_1_date is null   then 1
                    WHEN is_still_pregnant=1 AND patient_is_on_art=1 AND started_arv_after_pregnancy=1 and TIMESTAMPDIFF(MONTH,arv_first_regimen_start_date,sw0.end_date)>=3 and TIMESTAMPDIFF(MONTH,vl_1_date,date_confirmed_pregnant)>=1    then 1
                    WHEN is_still_pregnant=1 AND patient_is_on_art=1 AND started_arv_after_pregnancy=1 and TIMESTAMPDIFF(MONTH,arv_first_regimen_start_date,sw0.end_date)>=3 and TIMESTAMPDIFF(MONTH,vl_1_date,vl.end_date)>=6  then 1
                    
                    #is 24 and below years
                    WHEN age<=24 AND patient_is_on_art=1 AND (TIMESTAMPDIFF(MONTH,arv_first_regimen_start_date,sw0.end_date)>=6 or TIMESTAMPDIFF(MONTH,arv_start_date,sw0.end_date)>=6) and vl_1_date is null  then 1
                    WHEN age <=24 AND patient_is_on_art=1 AND TIMESTAMPDIFF(MONTH,vl_1_date,vl.end_date) >=6  then 1
                    
                    #is 25 and above years
                    WHEN age >=25 AND patient_is_on_art=1 AND (TIMESTAMPDIFF(MONTH,arv_first_regimen_start_date,sw0.end_date)>=6 or TIMESTAMPDIFF(MONTH,arv_start_date,sw0.end_date)>=6) and vl_1_date is null  then 1
                    WHEN age >=25 AND patient_is_on_art=1 AND TIMESTAMPDIFF(MONTH,vl_1_date,vl.end_date) >=6 and (TIMESTAMPDIFF(MONTH,arv_first_regimen_start_date,sw0.end_date) <=12 or TIMESTAMPDIFF(MONTH,arv_start_date,sw0.end_date) <= 12 ) then 1
                    WHEN age >=25 AND patient_is_on_art=1 AND TIMESTAMPDIFF(MONTH,vl_1_date,vl.end_date) >=12 and (TIMESTAMPDIFF(MONTH,arv_first_regimen_start_date,sw0.end_date) > 12 or TIMESTAMPDIFF(MONTH,arv_start_date,sw0.end_date) > 12 )then 1
                    
                    # breast feeding      
                    WHEN breast_feeding=1065 AND patient_is_on_art=1 and (TIMESTAMPDIFF(MONTH,arv_first_regimen_start_date,sw0.end_date) >=6
                    or TIMESTAMPDIFF(MONTH,arv_start_date,sw0.end_date) >=6) and vl_1_date is null then 1
                    WHEN breast_feeding=1065 AND patient_is_on_art=1 AND TIMESTAMPDIFF(MONTH,vl_1_date,vl.end_date) >=6 then 1
                    
                    # drug substitution and no vl ever
                    WHEN arv_first_regimen_start_date is not null and arv_start_date is not null 
                    and arv_first_regimen_start_date < arv_start_date 
                    AND TIMESTAMPDIFF(MONTH,arv_start_date,sw0.end_date) >= 3 
                    AND vl_1_date is null then 1
                    
                    # drug substitution and vl was done before change
                    WHEN arv_first_regimen_start_date is not null and arv_start_date is not null 
                    and arv_first_regimen_start_date < arv_start_date 
                    AND TIMESTAMPDIFF(MONTH,arv_start_date,sw0.end_date) >= 3 
                    AND TIMESTAMPDIFF(MONTH,vl_1_date,arv_start_date)>=1  then 1
                    
                    
                    else 0
                
                END AS patients_due_for_vl,
                
                vl.has_vl_this_week,
                vl.is_un_suppressed,
                vl.is_suppressed,
            IF( YEARWEEK(death_date)=year_week , 1,0) as non_ltfu_dead_this_week,
            IF( death_date>=@may_2019, 1,0) as cumulative_dead,
                CASE
                    WHEN death_date IS NOT NULL AND YEARWEEK(sw0.end_date) >= YEARWEEK(death_date)  THEN @status:='dead'
                        WHEN transfer_out_date IS  NOT NULL AND YEARWEEK(sw0.end_date) >= YEARWEEK(transfer_out_date) THEN @status:='transfer_out'
                        
                        WHEN  days_since_rtc_date > 0  AND days_since_rtc_date < 5 THEN  @status:='missed'  
                        
                            WHEN  days_since_rtc_date > 4  AND days_since_rtc_date <= 28 THEN  @status:='defaulter'
                        
                        WHEN  days_since_rtc_date <= 0 THEN  @status:='active' 
                        
                        WHEN  days_since_rtc_date > 28 THEN  @status:='ltfu'         
                        ELSE @status:='unknown'
                END AS status,
                
                CASE
                    WHEN death_date IS NOT NULL AND YEARWEEK(sw0.end_date) >= YEARWEEK(death_date)  THEN @status:='unknown'
                    WHEN transfer_out_date IS  NOT NULL AND YEARWEEK(sw0.end_date) >= YEARWEEK(transfer_out_date) THEN @status:='unknown'
                    WHEN  next_clinical_datetime_hiv IS NULL
                        AND days_since_med_pickup_rtc_date > 0  
                        AND days_since_med_pickup_rtc_date < 5 THEN  @status:='missed'  
                    
                        WHEN
                            next_clinical_datetime_hiv IS NULL
                            AND days_since_med_pickup_rtc_date > 4  
                            AND days_since_med_pickup_rtc_date <= 28 THEN  @status:='defaulter'
                        
                        WHEN  next_clinical_datetime_hiv IS  NULL 
                            AND days_since_med_pickup_rtc_date > 28 THEN  @status:='ltfu'
                            
                    WHEN  next_clinical_datetime_hiv IS NOT NULL 
                            AND  days_diff_med_rtc_and_next_clinical_date > 28
                            AND days_since_med_pickup_rtc_date > 28 THEN  @status:='ltfu'
                        
                    ELSE @status:='unknown'
                END AS med_status,

                
                
                IF(scheduled_this_week =1 and on_schedule_this_week=0 and  days_since_rtc_date >0 and days_since_rtc_date <5
                    
                AND death_date IS NULL AND transfer_out_date IS NULL, 1, 0) AS missed_appointment_this_week,
                
                IF(days_since_rtc_date >=0  and days_since_rtc_date <5 and @status not in('transfer_out','dead'), 1, 0) AS missed_cumulative ,
                
                IF(days_since_rtc_date >4  and days_since_rtc_date <=28
                    AND death_date IS NULL AND transfer_out_date IS NULL, 1, 0) AS defaulted ,
                
                null as ltfu,
                null AS missed,
                IF(YEARWEEK(t4.date_enrolled) = year_week
                        AND (t4.program_id = 3 OR t4.program_id = 9),
                    1,
                    0) AS started_dc_this_week,
                IF(t4.program_id = 3 OR t4.program_id = 9, 1, 0) AS active_on_dc,
                IF(t4.program_id = 3 OR t4.program_id = 9,
                    1,
                    0) AS on_dc_this_week,
                IF(YEARWEEK(t4.date_enrolled) = year_week
                        AND t4.program_id = 10,
                    1,
                    0) AS new_prep_this_week,
                IF(t4.program_id = 10 , 1, 0) AS cur_prep_this_week,
                t6.weight AS weight,
                t6.height AS height,
                fd.encounter_datetime as death_reporting_date,
                fd.next_encounter_datetime as fd_next_encounter_datetime,
                ft.encounter_datetime as transfer_reporting_date,
                ft.next_encounter_datetime as ft_next_encounter_datetime,
                fe.encounter_datetime as exit_reporting_date,
                fe.next_encounter_datetime as fe_next_encounter_datetime,
                t6.bmi FROM
                surge_weekly_report_dataset_01 sw0
                    LEFT OUTER JOIN   patient_vl_status vl ON vl.person_id=sw0.person_id and vl.week = sw0.year_week
                    LEFT OUTER JOIN
                amrs.patient_program t4 ON t4.patient_id = sw0.person_id
                    AND t4.program_id IN (3 , 9, 10, 11)
                    AND t4.date_completed IS NULL
                    LEFT OUTER JOIN
                surge_vitals t6 ON t6.encounter_id = sw0.encounter_id
                LEFT JOIN etl.flat_appointment fd ON (sw0.person_id = fd.person_id and fd.encounter_type = 31)
                LEFT JOIN etl.flat_appointment ft ON (sw0.person_id = ft.person_id and ft.encounter_type = 116)
                LEFT JOIN etl.flat_appointment fe ON (sw0.person_id = fe.person_id and fe.encounter_type = 157)
                ORDER BY sw0.person_id , year_week);


            set @prev_id = -1;
            set @cur_id = -1;
            set @prev_enc_id = -1;
            set @cur_enc_id = -1;
            set @cur_status = '';
            set @prev_status = '';
            set @cur_med_status = '';
            set @prev_med_status = '';
            set @cur_arv_meds = null;
            set @prev_arv_meds = null;
            set @cur_location_id = null;
            set @prev_location_id = null;
            Set @clinic_visit_number = 1;
            set @prev_patients_due_for_vl  = 0;
            set @prev_defaulted = 0;
            set @cur_defaulted  = 0;
            set @prev_missed = 0;
            set @cur_missed  = 0;
            set @prev_ltfu = 0;
            set @cur_ltfu  = 0;
            set @base_rtc = null;


            set @was_active_october_18:= null;
            set @was_ltfu_may_19 := null;
            set @is_ltfu_surge_baseline := null;



            SELECT CONCAT('creating surge_weekly_report_dataset__2 ...');


            drop temporary table if exists surge_weekly_report_dataset__2;
            CREATE temporary TABLE surge_weekly_report_dataset__2 (SELECT *,
                @prev_id:=@cur_id AS prev_id,
                @cur_id:=person_id AS cur_id,
                @prev_enc_id:=@cur_enc_id AS prev_enc_id,
                @cur_enc_id:=encounter_id AS cur_enc_id,
                CASE
                    WHEN
                        @prev_id = @cur_id
                            AND @prev_enc_id != @cur_enc_id
                    THEN
                        @clinic_visit_number:=@clinic_visit_number + 1
                    WHEN
                        @prev_id = @cur_id
                            AND @prev_enc_id = @cur_enc_id
                    THEN
                        @clinic_visit_number
                    ELSE @clinic_visit_number:=1
                END AS clinical_visit_num,
                
                CASE
                    WHEN @prev_id = @cur_id THEN @prev_status:=@cur_status
                    ELSE @prev_status:=''
                END AS prev_status,
                
                @cur_status:=status AS cur_status,
                
                CASE
                    WHEN @prev_id = @cur_id THEN @prev_med_status:=@cur_med_status
                    ELSE @prev_med_status:=''
                END AS prev_med_status,
                
                @cur_med_status:=med_status AS cur_med_status,
                
                CASE
                    WHEN
                        (@prev_id != @cur_id
                            OR @was_active_october_18 IS NULL)
                            AND year_week < 201938
                            AND (@cur_status = 'active' OR @cur_status = 'missed'  OR @cur_status = 'defaulter')
                            AND ((death_date IS NULL OR DATE(death_reporting_date) > '2019-09-30') AND fd_next_encounter_datetime IS NULL)
                            AND (transfer_out_date IS NULL OR (DATE(transfer_reporting_date) > '2019-09-30'))
                            AND ((exit_reporting_date IS NULL OR DATE(exit_reporting_date) > '2019-09-30') AND fe_next_encounter_datetime IS NULL)
                    
                THEN
                        @was_active_october_18:=1
                    WHEN @prev_id != @cur_id THEN @was_active_october_18:=NULL
                    ELSE @was_active_october_18
                END AS was_active_october_18,

                CASE
                    WHEN
                        (@prev_id != @cur_id
                            OR @was_ltfu_may_19 IS NULL)
                            AND year_week = 201918
                            AND @cur_status = 'ltfu'
                            AND ((death_date IS NULL OR DATE(death_reporting_date) > '2019-05-11') AND fd_next_encounter_datetime IS NULL)
                            AND (transfer_out_date IS NULL OR (DATE(transfer_reporting_date) > '2019-05-11'))
                            AND ((exit_reporting_date IS NULL OR DATE(exit_reporting_date) > '2019-05-11') AND fe_next_encounter_datetime IS NULL)
                            THEN
                        @was_ltfu_may_19:=1
                    WHEN @prev_id != @cur_id THEN @was_ltfu_may_19:=NULL
                    ELSE @was_ltfu_may_19
                END AS was_ltfu_may_19,
                
                CASE
                    WHEN
                        (@prev_id != @cur_id
                            OR @is_ltfu_surge_baseline IS NULL)
                            AND @was_active_october_18 = 1
                            AND @was_ltfu_may_19 = 1
                            AND year_week <= 201918
                            
                    THEN
                        @is_ltfu_surge_baseline:=1
                    WHEN
                        (@prev_id != @cur_id
                            OR @is_ltfu_surge_baseline IS NULL)
                            AND NOT (@was_active_october_18 = NULL
                            AND @was_ltfu_may_19 = NULL)
                            AND year_week <= 201918  
                            
                    THEN
                        @is_ltfu_surge_baseline:=0
                    WHEN @prev_id != @cur_id THEN @is_ltfu_surge_baseline:=NULL
                    ELSE @is_ltfu_surge_baseline
                END AS is_ltfu_surge_baseline,
                    
                
                CASE
                    WHEN @prev_id != @cur_id 
                    AND @is_ltfu_surge_baseline = 1
                    AND @baseline_location IS NULL
                    THEN @baseline_location:= location_id
                    
                    WHEN @prev_id != @cur_id 
                    AND @is_ltfu_surge_baseline = 1
                    AND @baseline_location IS NOT NULL
                    THEN @baseline_location
                    
                    WHEN @prev_id = @cur_id 
                    AND @is_ltfu_surge_baseline = 1
                    AND @baseline_location IS NULL
                    THEN @baseline_location:= location_id
                    
                    WHEN @prev_id = @cur_id 
                    AND @is_ltfu_surge_baseline = 1
                    AND @baseline_location IS NOT NULL
                    THEN @baseline_location
                    
                    ELSE @baseline_location:= null
                END as  baseline_location,

                CASE
                    WHEN
                        (@prev_id != @cur_id
                            OR @ever_active_after_sep_19 IS NULL)
                            AND year_week >=201938
                            AND (@cur_status = 'active' OR @cur_status = 'missed' OR @cur_status = 'defaulter')
                            AND ((death_date IS NULL OR DATE(death_reporting_date) > '2019-09-22') AND fd_next_encounter_datetime IS NULL)
                            AND (transfer_out_date IS NULL OR (DATE(transfer_reporting_date) > '2019-09-22'))
                            AND ((exit_reporting_date IS NULL OR DATE(exit_reporting_date) > '2019-09-22') AND fe_next_encounter_datetime IS NULL)
                            
                    THEN
                        @ever_active_after_sep_19:=1
                    WHEN @prev_id != @cur_id THEN @ever_active_after_sep_19:=NULL
                    ELSE @ever_active_after_sep_19
                END AS ever_active_after_sep_19,  #active_after_may_19

                CASE
                    WHEN
                        (@prev_id != @cur_id
                            OR @is_ltfu_after_sep_19 IS NULL) 
                            AND @ever_active_after_sep_19 = 1
                            AND @cur_status = 'ltfu'
                            AND year_week > 201938
                    THEN
                        @is_ltfu_after_sep_19:=1
                    WHEN
                        (@prev_id != @cur_id
                            OR @is_ltfu_after_sep_19 IS NULL)
                            AND NOT (@ever_active_after_sep_19 = NULL)
                            AND year_week > 201938
                            
                    THEN
                        @is_ltfu_after_sep_19:=0
                    WHEN @prev_id != @cur_id THEN @is_ltfu_after_sep_19:=NULL
                    ELSE @is_ltfu_after_sep_19
                END AS is_ltfu_after_sep_19,   #is_ltfu_after_may_revised

                IF((@is_ltfu_after_sep_19 = 1 OR 
                (@prev_status = 'defaulter' AND ((days_since_rtc_date > 28) OR (days_diff_enc_date_and_prev_rtc > 28 and days_since_rtc_date <=0)))),
                    1,
                    0) AS is_ltfu_after_sep_total,

                IF((@is_ltfu_after_sep_19 = 1 OR 
                (@prev_status = 'defaulter' AND ((days_since_rtc_date > 28) OR (days_diff_enc_date_and_prev_rtc > 28 and days_since_rtc_date <=0))))
                AND @cur_status = 'ltfu'
                        AND ((death_date IS NULL OR DATE(death_reporting_date) > end_date) AND fd_next_encounter_datetime IS NULL)
                        AND (transfer_out_date IS NULL OR (DATE(transfer_reporting_date) > end_date))
                        AND ((exit_reporting_date IS NULL OR DATE(exit_reporting_date) > end_date) AND fe_next_encounter_datetime IS NULL),
                    1,
                    0) AS is_ltfu_after_sep_not_resolved,

                
                #MEDICATION Refill indicators

                CASE
                    WHEN
                        (@prev_id != @cur_id
                            OR @is_med_ltfu_after_sep19 IS NULL) 
                            AND @ever_active_after_sep_19 = 1
                            AND @cur_med_status = 'ltfu'
                            AND year_week > 201938
                    THEN
                        @is_med_ltfu_after_sep19:=1
                    WHEN
                        (@prev_id != @cur_id
                            OR @is_med_ltfu_after_sep19 IS NULL)
                            AND NOT (@ever_active_after_sep_19 = NULL)
                            AND year_week > 201938
                            
                    THEN
                        @is_med_ltfu_after_sep19:=0
                    WHEN @prev_id != @cur_id THEN @is_med_ltfu_after_sep19:=NULL
                    ELSE @is_med_ltfu_after_sep19
                END AS is_med_ltfu_after_sep19, #med revised

                IF((@is_med_ltfu_after_sep19 = 1),
                        1, 0) AS is_med_ltfu_after_sep19_cumulative,
                        
                IF(@is_med_ltfu_after_sep19 = 1 AND @cur_med_status = 'ltfu'
                        AND ((death_date IS NULL OR DATE(death_reporting_date) > end_date) AND fd_next_encounter_datetime IS NULL)
                        AND (transfer_out_date IS NULL OR (DATE(transfer_reporting_date) > end_date))
                        AND ((exit_reporting_date IS NULL OR DATE(exit_reporting_date) > end_date) AND fe_next_encounter_datetime IS NULL),
                        1, 0) AS is_med_ltfu_after_sep19_unresolved,

    
                CASE
                    WHEN
                        next_clinical_datetime_hiv IS NULL
                            AND days_since_med_pickup_rtc_date > 4  
                            AND days_since_med_pickup_rtc_date <= 28
                        AND @cur_status not in('transfer_out','dead') 
                            
                    THEN
                        1
                    ELSE 0
                END AS med_defaulters,        
                        
                IF(@prev_med_status = 'defaulter'
                    AND @cur_med_status = 'ltfu'
                    AND ((death_date IS NULL OR DATE(death_reporting_date) > end_date) AND fd_next_encounter_datetime IS NULL)
                    AND (transfer_out_date IS NULL OR (DATE(transfer_reporting_date) > end_date))
                    AND ((exit_reporting_date IS NULL OR DATE(exit_reporting_date) > end_date) AND fe_next_encounter_datetime IS NULL),
                    1,
                    0) AS newly_med_ltfu_this_week,
                    
                IF(@is_med_ltfu_after_sep19 = 1 AND @prev_med_status = 'ltfu'
                    AND (@cur_med_status = 'unknown'
                    OR @cur_med_status = 'missed'
                    OR @cur_med_status = 'defaulter'),
                        1, 0) AS med_surge_ltfus_outcomes_this_week, 
                        
                IF(@is_med_ltfu_after_sep19 = 1 AND (@cur_med_status = 'unknown'
                    OR @cur_med_status = 'missed'
                    OR @cur_med_status = 'defaulter'),
                        1, 0) AS med_surge_ltfus_outcomes,    

                #MEDICATION Refill indicators final 
                    
                    
                CASE
                    WHEN @prev_id = @cur_id 
                    AND @is_ltfu_after_sep19 = 1
                    AND @prev_status = 'ltfu' AND @cur_status = 'active'
                    AND @week_patient_became_active IS NULL
                    AND encounter_datetime > '2019-05-11'
                    THEN @week_patient_became_active:= encounter_datetime
                    
                    WHEN @prev_id = @cur_id 
                    AND @is_ltfu_after_sep19 = 1
                    AND @prev_status = 'defaulter' AND @cur_status = 'ltfu'
                    AND @week_patient_became_active IS NOT NULL
                    THEN @week_patient_became_active:= NULL
                    
                    WHEN @prev_id = @cur_id 
                    AND @is_ltfu_after_sep19 = 1
                    AND @prev_status = 'ltfu' AND @cur_status = 'ltfu'
                    THEN @week_patient_became_active:= NULL
                    
                    WHEN @prev_id != @cur_id 
                    AND @is_ltfu_after_sep19 = 1
                    AND @prev_status = 'ltfu' AND @cur_status = 'active'
                    AND @week_patient_became_active IS NULL
                    AND encounter_datetime > '2019-05-11'
                    THEN @week_patient_became_active:= encounter_datetime
                    
                    WHEN @prev_id != @cur_id 
                    AND @is_ltfu_after_sep19 = 1
                    AND @prev_status = 'defaulter' AND @cur_status = 'ltfu'
                    AND @week_patient_became_active IS NOT NULL
                    THEN @week_patient_became_active:= NULL
                    
                    WHEN @prev_id != @cur_id 
                    AND @is_ltfu_after_sep19 = 1
                    AND @prev_status = 'ltfu' AND @cur_status = 'ltfu'
                    THEN @week_patient_became_active:= NULL
                    
                    ELSE @week_patient_became_active
                END as  week_patient_became_active,
                

                IF(@prev_status = 'missed'
                        AND @cur_status = 'active',
                    1,
                    0) AS missed_to_active_this_week, 
                    
                IF(@prev_status = 'defaulter'
                        AND @cur_status = 'active',
                    1,
                    0) AS defaulter_to_active_this_week,
                IF((@cur_status = 'ltfu' OR days_since_rtc_date > 28 OR (days_diff_enc_date_and_prev_rtc > 28 and days_since_rtc_date <=0))
                    AND ((death_date IS NULL OR DATE(death_reporting_date) > end_date) AND fd_next_encounter_datetime IS NULL)
                    AND (transfer_out_date IS NULL OR (DATE(transfer_reporting_date) > end_date))
                    AND ((exit_reporting_date IS NULL OR DATE(exit_reporting_date) > end_date) AND fe_next_encounter_datetime IS NULL),
                    1,
                    0) AS all_ltfus,    
                        
                IF( YEARWEEK(death_date)=year_week, 1,0) as dead_this_week,
                
                IF(@prev_status = 'defaulter'
                    AND (@cur_status = 'ltfu' OR days_since_rtc_date > 28 OR (days_diff_enc_date_and_prev_rtc > 28 and days_since_rtc_date <=0))
                    
                    AND ((death_date IS NULL OR DATE(death_reporting_date) > end_date) AND fd_next_encounter_datetime IS NULL)
                    AND (transfer_out_date IS NULL OR (DATE(transfer_reporting_date) > end_date))
                    AND ((exit_reporting_date IS NULL OR DATE(exit_reporting_date) > end_date) AND fe_next_encounter_datetime IS NULL),
                    1,
                    0) AS newly_ltfu_this_week,

                
                # OUTCOMES  cumulative outcomes
                IF((@is_ltfu_after_sep_19 = 1 OR 
                (@prev_status = 'defaulter' AND ((days_since_rtc_date > 28) OR (days_diff_enc_date_and_prev_rtc > 28 and days_since_rtc_date <=0))))
                        AND @cur_status = 'dead',
                    1,
                    0) AS ltfu_cumulative_outcomes_death,
                    
                IF((@is_ltfu_after_sep_19 = 1 OR 
                (@prev_status = 'defaulter' AND ((days_since_rtc_date > 28) OR (days_diff_enc_date_and_prev_rtc > 28 and days_since_rtc_date <=0))))
                        AND @cur_status = 'transfer_out',
                    1,
                    0) AS ltfu_cumulative_outcomes_transfer_out,
                    
                IF((@is_ltfu_after_sep_19 = 1 OR 
                (@prev_status = 'defaulter' AND ((days_since_rtc_date > 28) OR (days_diff_enc_date_and_prev_rtc > 28 and days_since_rtc_date <=0))))
                        AND (@cur_status = 'active'
                        OR @cur_status = 'transfer_in'
                        OR @cur_status = 'missed'
                        OR @cur_status = 'defaulter'),
                    1,
                    0) AS ltfu_cumulative_outcomes_active,
                
                IF((@is_ltfu_after_sep_19 = 1 OR 
                (@prev_status = 'defaulter' AND ((days_since_rtc_date > 28) OR (days_diff_enc_date_and_prev_rtc > 28 and days_since_rtc_date <=0))))
                        AND (@cur_status = 'active'
                        OR @cur_status = 'transfer_in'
                        OR @cur_status = 'missed'
                        OR @cur_status = 'defaulter'
                        OR @cur_status = 'transfer_out'
                        OR @cur_status = 'dead'),
                    1,
                    0) AS ltfu_cumulative_outcomes_total,
                    
                    #OUTCOMES weekly outcomes
                IF(((@is_ltfu_after_sep_19 = 1 AND @prev_status = 'ltfu')
                            OR (@prev_status = 'defaulter' AND ((days_since_rtc_date > 28) OR (days_diff_enc_date_and_prev_rtc > 28 and days_since_rtc_date <= 0 ))))
                            AND @cur_status = 'dead',
                    1,
                    0) AS ltfu_death_this_week,
                    
                IF((@is_ltfu_after_sep_19 = 1 AND @prev_status = 'ltfu'
                        OR (@prev_status = 'defaulter' AND ((days_since_rtc_date > 28) OR (days_diff_enc_date_and_prev_rtc > 28 and days_since_rtc_date <= 0 ))))
                        AND @cur_status = 'transfer_out',
                    1,
                    0) AS ltfu_transfer_out_this_week,
                    
                    
                IF(((@is_ltfu_after_sep_19 = 1 AND @prev_status = 'ltfu') 
                        OR (@prev_status = 'defaulter' AND ((days_since_rtc_date > 28) OR (days_diff_enc_date_and_prev_rtc > 28 and days_since_rtc_date <= 0 )))) 
                        AND (@cur_status = 'active'
                        OR @cur_status = 'missed'
                        OR @cur_status = 'defaulter'
                        OR @cur_status = 'transfer_in'),
                    1,
                    0) AS ltfu_active_this_week,
                    
				 IF(((@is_ltfu_after_sep_19 = 1 AND @prev_status = 'ltfu') 
                        OR (@prev_status = 'defaulter' AND ((days_since_rtc_date > 28) OR (days_diff_enc_date_and_prev_rtc > 28 and days_since_rtc_date <= 0 )))) 
                        AND (@cur_status = 'active'
                        OR @cur_status = 'missed'
                        OR @cur_status = 'defaulter'
                        OR @cur_status = 'transfer_in'
                        OR @cur_status = 'dead'
                        OR @cur_status = 'transfer_out'),
                    1,
                    0) AS ltfu_cumulative_outcomes_this_week_total,
                #OUTCOMES weekly outcomes
                    
                CASE
                    WHEN
                        (@prev_id != @cur_id
                            OR @was_ltfu_before_october_2018 IS NULL)
                            AND year_week = 201938
                            AND @cur_status = 'ltfu'
                            AND @was_active_october_18 = 0
                THEN
                        @was_ltfu_before_october_2018:=1
                    WHEN @prev_id != @cur_id THEN @was_ltfu_before_october_2018:=NULL
                    ELSE @was_ltfu_before_october_2018
                END AS was_ltfu_before_october_2018,
                
                
                    
                #self returnies total
                IF(@was_ltfu_before_october_2018 = 1 
                        AND (@cur_status = 'active'
                        OR @cur_status = 'missed'
                        OR @cur_status = 'defaulter'
                        OR @cur_status = 'transfer_in'
                        OR @cur_status = 'transfer_out'
                        OR @cur_status = 'dead'),
                    1,
                    0) AS old_ltfus_outcome_total, 
                    
                IF(@was_ltfu_before_october_2018 = 1 
                    AND (@cur_status = 'active'
                    OR @cur_status = 'missed'
                    OR @cur_status = 'defaulter'
                    OR @cur_status = 'transfer_out'
                    OR @cur_status = 'transfer_in'
                    OR @cur_status = 'dead'),
                1,
                    0) AS old_ltfus_to_active,
                    
                    
                #self returnies this week
                IF(@was_ltfu_before_october_2018 = 1 
                    AND @prev_status = 'ltfu'
                    AND (@cur_status = 'active'
                    OR @cur_status = 'missed'
                    OR @cur_status = 'defaulter'
                    OR @cur_status = 'transfer_in'
                    OR @cur_status = 'transfer_out'),
                1,
                    0) AS old_ltfus_to_active_this_week,
                    

                CASE
                    WHEN
                        @prev_id = @cur_id
                            AND @prev_status = 'active'
                            AND @cur_status = 'ltfu'
                    THEN
                        @active_to_ltfu_count:=@active_to_ltfu_count + 1
                    ELSE @active_to_ltfu_count:=0
                END AS active_to_ltfu_count,
                
                CASE
                    WHEN @prev_id != @cur_id THEN @height:=height
                    WHEN
                        @prev_id = @cur_id AND @height IS NULL
                            AND height IS NOT NULL
                    THEN
                        @height:=height
                    WHEN
                        @prev_id = @cur_id AND @height
                            AND height
                    THEN
                        @height:=height
                    ELSE @height
                END AS revised_height,
                CASE
                    WHEN @prev_id != @cur_id THEN @weight:=weight
                    WHEN
                        @prev_id = @cur_id AND @weight IS NULL
                            AND weight IS NOT NULL
                    THEN
                        @weight:=weight
                    WHEN
                        @prev_id = @cur_id AND @weight
                            AND weight
                    THEN
                        @weight:=weight
                    ELSE @weight
                END AS revised_weight,
                CASE
                    WHEN @prev_id != @cur_id THEN @bmi:=bmi
                    WHEN
                        @prev_id = @cur_id AND @bmi IS NULL
                            AND bmi IS NOT NULL
                    THEN
                        @bmi:=bmi
                    WHEN @prev_id = @cur_id AND @bmi AND bmi THEN @bmi:=bmi
                    ELSE @bmi
                END AS revised_bmi,
                CASE
                    WHEN @prev_id = @cur_id THEN @prev_patients_due_for_vl:=@cur_patients_due_for_vl
                    ELSE @prev_patients_due_for_vl:=0
                END AS prev_patients_due_for_vl,
                @cur_patients_due_for_vl:=patients_due_for_vl AS cur_patients_due_for_vl,
                IF(tx2_scheduled_this_week = 1
                        AND year_week != YEARWEEK(encounter_date) AND prev_rtc_date IS NULL,
                    1,
                    0) AS tx2_missed_this_week,
                IF(tx2_scheduled_this_week = 1
                        AND YEARWEEK(encounter_date) < year_week AND prev_rtc_date IS NOT NULL ,
                    1,
                    0) AS tx2_scheduled_this_week_but_came_early,
                IF(tx2_scheduled_this_week = 0
                        AND tx2_visit_this_week = 1,
                    1,
                    0) AS tx2_unscheduled_this_week,
                
                CASE
                    WHEN days_since_last_vl <= 365
                            AND vl_2_date IS NOT NULL
                            AND (months_since_tb_tx_start_date IS NULL
                            OR months_since_tb_tx_start_date >= 6)
                            AND (is_pregnant = 0 OR is_pregnant IS NULL)
                            AND @bmi >= 18.5
                            AND age >= 20
                            AND @cur_status = 'active'
                            AND days_since_starting_arvs > 364
                            AND vl_1 >= 0
                            AND vl_1 <= 400 
                        THEN @not_elligible_for_dc := 0
                        ELSE @not_elligible_for_dc := 1
                    END AS not_elligible_for_dc,
                    
                    
                CASE
                    WHEN days_since_last_vl <= 365
                            AND vl_2_date IS NOT NULL
                            AND (months_since_tb_tx_start_date IS NULL
                            OR months_since_tb_tx_start_date >= 6)
                            AND on_dc_this_week = 1
                            AND (is_pregnant = 0 OR is_pregnant IS NULL)
                            AND @bmi >= 18.5
                            AND age >= 20
                            AND @cur_status = 'active'
                            AND days_since_starting_arvs > 364
                            AND vl_1 >= 0
                            AND vl_1 <= 400 
                        THEN @eligible_and_on_dc := 1
                        ELSE @eligible_and_on_dc := 0
                    END AS eligible_and_on_dc,
                    
                CASE    
                    WHEN days_since_last_vl <= 365
                        AND vl_2_date IS NOT NULL
                            AND (months_since_tb_tx_start_date IS NULL
                            OR months_since_tb_tx_start_date >= 6)
                            AND on_dc_this_week != 1
                            AND (is_pregnant = 0 OR is_pregnant IS NULL)
                            AND @bmi >= 18.5
                            AND age >= 20
                            AND @cur_status = 'active'
                            AND days_since_starting_arvs > 364
                            AND vl_1 >= 0
                            AND vl_1 <= 400 
                        THEN @eligible_not_on_dc := 1
                            ELSE
                        @eligible_not_on_dc := 0
                    END AS eligible_not_on_dc,
                    
                
                IF(scheduled_this_week = 1 AND @eligible_not_on_dc = 1, 1, 0 ) as eligible_not_on_dc_and_scheduled_this_week,
                IF(scheduled_this_week = 0 AND @eligible_not_on_dc = 1, 1, 0 ) as eligible_not_on_dc_and_unscheduled_this_week,
                IF(scheduled_this_week = 1 AND @eligible_and_on_dc = 1, 1, 0 ) as eligible_and_on_dc_and_scheduled_this_week,
                IF(scheduled_this_week = 0 AND @eligible_and_on_dc = 1, 1, 0 ) as eligible_and_on_dc_and_unscheduled_this_week,
                IF(@eligible_not_on_dc = 1 OR @eligible_and_on_dc = 1, 1, 0 ) as elligible_total,
                IF(@not_elligible_for_dc = 1 AND on_dc_this_week = 1, 1, 0 ) as not_elligible_and_on_dc,
                IF(@eligible_not_on_dc = 1 OR on_dc_this_week = 1, 1, 0 ) as elligible_total_revised,

                CASE 
                WHEN 
                @prev_id != @cur_id
                THEN @previous_dc_eligibility := NULL
                ELSE 
                @previous_dc_eligibility := @dc_eligible_cumulative 
                END as previous_dc_eligibility,
                
                CASE 
                WHEN days_since_last_vl <= 365
                        AND vl_2_date IS NOT NULL
                        AND (months_since_tb_tx_start_date IS NULL
                        OR months_since_tb_tx_start_date >= 6)
                        AND on_dc_this_week != 1
                        AND (is_pregnant = 0 OR is_pregnant IS NULL)
                        AND @bmi >= 18.5
                        AND age >= 20
                        AND @cur_status = 'active'
                        AND days_since_starting_arvs >= 364
                        AND vl_1 >= 0
                        AND vl_1 <= 400 
                    THEN @dc_eligible_cumulative := 1
                        ELSE
                    @dc_eligible_cumulative := 0
                    END AS dc_eligible_cumulative,
                    
                IF(@prev_id = @cur_id AND (@previous_dc_eligibility = 0 OR @previous_dc_eligibility = null) AND @dc_eligible_cumulative = 1, 1, 0 ) as dc_eligible_this_week,
                
                IF(scheduled_this_week = 1 AND @dc_eligible_cumulative = 1, 1, 0 ) as dc_eligible_and_scheduled_this_week,
                IF(patients_due_for_vl = 1 AND @cur_status = 'active'
                        AND scheduled_this_week = 1,
                    1,
                    0) AS scheduled_this_week_and_due_for_vl,
                IF(patients_due_for_vl = 1
                        AND @cur_status = 'active'
                        AND scheduled_this_week = 0,
                    1,
                    0) AS unscheduled_this_week_and_due_for_vl,
                IF(patients_due_for_vl = 1
                        AND vl_ordered_this_week = 1
                        AND @cur_status = 'active',
                    1,
                    0) AS due_for_vl_has_vl_order,
                IF(patients_due_for_vl = 1
                        AND vl_ordered_this_week = 0
                        AND @cur_status = 'active',
                    1,
                    0) AS due_for_vl_dont_have_order,
                IF(patients_due_for_vl = 1 AND @cur_status = 'active',
                    1,
                    0) AS due_for_vl_this_week_active,
                IF( patients_due_for_vl= 1
                        AND @cur_status = 'active',
                    1,
                    0) AS overdue_for_vl_active,
                IF(is_suppressed = 1
                    AND @cur_status = 'active',
                1,
                0) AS is_suppressed_active,
                IF(is_un_suppressed = 1
                    AND @cur_status = 'active',
                1,
                0) AS is_un_suppressed_active,

                IF(patients_due_for_vl = 1
                        AND vl_ordered_this_week = 1
                        AND @cur_status = 'active'
                        AND scheduled_this_week = 1,
                    1,
                    0) AS due_for_vl_has_vl_order_scheduled,
                    
                IF(patients_due_for_vl = 1
                        AND vl_ordered_this_week = 1
                        AND @cur_status = 'active'
                        AND scheduled_this_week = 0,
                    1,
                    0) AS due_for_vl_has_vl_order_unscheduled,
                
                
                IF(patients_due_for_vl = 1
                        AND vl_ordered_this_week = 0
                        AND @cur_status = 'active'
                        AND scheduled_this_week = 1,
                    1,
                    0) AS due_for_vl_dont_have_order_scheduled,
                    
                IF(patients_due_for_vl = 1
                        AND vl_ordered_this_week = 0
                        AND @cur_status = 'active'
                        AND scheduled_this_week = 0,
                    1,
                    0) AS due_for_vl_dont_have_order_unscheduled,
                
                IF(is_suppressed = 1
                    AND has_vl_this_week = 1,
                1,
                0) AS has_vl_this_week_and_suppressed,
                
                IF(is_un_suppressed = 1
                    AND has_vl_this_week = 1,
                1,
                0) AS has_vl_this_week_and_unsuppressed,
                
                
                
                CASE
                    WHEN @prev_id = @cur_id THEN @prev_defaulted:=@cur_defaulted
                    ELSE @prev_defaulted:=0
                END AS prev_defaulted,
                @cur_defaulted:=defaulted AS cur_defaulted,
                IF(@cur_defaulted = 0
                        AND @cur_defaulted = 1,
                    1,
                    0) AS defaulted_this_week,
                CASE
                    WHEN @prev_id = @cur_id THEN @prev_missed:=@cur_missed
                    ELSE @prev_missed:=0
                END AS prev_missed,
                @cur_missed:=missed AS cur_missed,
                IF(@prev_missed = 0 AND @cur_missed = 1,
                    1,
                    0) AS missed_this_week,
                CASE
                    WHEN @prev_id = @cur_id THEN @prev_ltfu:=@cur_ltfu
                    ELSE @prev_ltfu:=0
                END AS prev_ltfu,
                @cur_ltfu:=ltfu AS cur_ltfu,
                IF(@cur_ltfu = 0 AND @cur_ltfu = 1,
                    1,
                    0) AS ltfu_this_week FROM
                surge_weekly_report_dataset_1);
                
                
            SELECT CONCAT('replacing into surge_weekly_report_dataset ...');
            replace into surge_weekly_report_dataset						  
                        (select
                        now(),
                        elastic_id ,
                        person_uuid ,
                        person_id ,
                        year_week ,
                        encounter_yw ,
                        encounter_id ,
                        encounter_datetime,
                        encounter_date ,
                        end_date ,
                        start_date ,
                        birthdate ,
                        age ,
                        gender,
                        clinical_visit_num as clinical_visit_number,
                        prev_rtc_date ,
                        rtc_date ,
                        visit_this_week ,
                        on_schedule ,
                        early_appointment,
                        early_appointment_this_week ,
                        late_appointment_this_week ,
                        days_since_rtc_date ,
                        scheduled_this_week ,
                        unscheduled_this_week ,
                        tx2_visit_this_week,
                        tx2_missed_this_week as missed_tx2_visit_this_week ,
                        tx2_scheduled_this_week_but_came_early,
                        death_date ,
                        missed_appointment_this_week ,
                        ltfu ,
                        defaulted ,
                        missed ,
                        null as next_status ,
                        active_in_care_this_week ,
                        cur_arv_adherence ,
                        cur_who_stage ,
                        is_pre_art_this_week ,
                        arv_first_regimen_location_id ,
                        arv_first_regimen ,
                        arv_first_regimen_names ,
                        arv_first_regimen_start_date ,
                        days_since_starting_arvs ,
                        started_art_this_week ,
                        enrollment_date ,
                        enrolled_this_week,
                        art_revisit_this_week ,
                        cur_arv_meds ,
                        cur_arv_meds_names ,
                        cur_arv_meds_strict ,
                        cur_arv_line ,
                        cur_arv_line_strict ,
                        cur_arv_line_reported ,
                        on_art_this_week ,
                        vl_1 ,
                        vl_1_date ,
                        vl_2 ,
                        vl_2_date,
                        has_vl_this_week ,
                        is_suppressed_active as  is_suppressed ,
                        is_un_suppressed_active as is_un_suppressed ,
                        days_since_last_vl ,
                        null as due_for_vl_this_week,
                        null as reason_for_needing_vl_this_week ,
                        cd4_1 ,
                        cd4_1_date ,
                        child_hiv_status_disclosure_status ,
                        transfer_in_this_week ,
                        transfer_in_location_id ,
                        transfer_in_date ,
                        transfer_out_this_week ,
                        transfer_out_location_id,
                        transfer_out_date ,
                        status ,
                        dc_eligible_cumulative ,
                        started_dc_this_week ,
                        location_id,
                        tx2_scheduled_this_week ,
                        tx2_scheduled_honored ,
                        prev_id,
                        cur_id ,
                        prev_enc_id ,
                        cur_enc_id ,
                        clinical_visit_num ,
                        prev_status ,
                        cur_status ,
                        cur_prep_this_week ,
                        new_prep_this_week,
                        revised_height as height ,
                        revised_weight as weight ,
                        revised_bmi as bmi ,
                        scheduled_this_week_and_due_for_vl ,
                        unscheduled_this_week_and_due_for_vl ,
                        overdue_for_vl_active ,
                        due_for_vl_has_vl_order,
                        due_for_vl_dont_have_order,
                        ltfu_this_week ,
                        missed_this_week ,
                        all_ltfus ,
                        null as ltfu_surge_baseline ,
                        is_ltfu_after_sep_not_resolved as surge_ltfu_and_ltfu_after_may,
                        null as surge_ltfu_and_still_ltfu ,
                        newly_ltfu_this_week ,
                        ltfu_cumulative_outcomes_death ,
                        ltfu_cumulative_outcomes_transfer_out ,
                        ltfu_cumulative_outcomes_active,
                        active_to_ltfu_count ,
                        defaulted_this_week,
                        due_for_vl_this_week_active ,
                        on_schedule_this_week ,
                        ltfu_cumulative_outcomes_total,
                        old_ltfus_to_active_this_week,
                        tx2_unscheduled_this_week ,
                        dead_this_week,
                        non_ltfu_dead_this_week ,
                        cumulative_dead ,
                        dc_eligible_this_week,
                        dc_eligible_and_scheduled_this_week,
                        active_on_dc,
                        is_ltfu_surge_baseline,
                        baseline_location,
                        was_ltfu_may_19 ,
                        was_active_october_18,
                        not_elligible_for_dc,
                        eligible_and_on_dc, 
                        eligible_not_on_dc, 
                        eligible_not_on_dc_and_scheduled_this_week, 
                        eligible_not_on_dc_and_unscheduled_this_week, 
                        eligible_and_on_dc_and_scheduled_this_week, 
                        eligible_and_on_dc_and_unscheduled_this_week, 
                        elligible_total,
                        not_elligible_and_on_dc,
                        elligible_total_revised,
                        ltfu_transfer_out_this_week,
                        ltfu_death_this_week,
                        ltfu_active_this_week,
                    
                        is_ltfu_after_sep_not_resolved AS is_ltfu_after_may,
                        is_ltfu_after_sep_total AS is_ltfu_after_may_total,
                        week_patient_became_active,
                        
                        med_pickup_rtc_date,
                        days_since_med_pickup_rtc_date,
                        days_diff_med_rtc_and_next_clinical_date,

                        was_ltfu_before_october_2018,
                        old_ltfus_outcome_total,
                        old_ltfus_to_active,
                        intervention_done_this_week,
                        interventions,
                        due_for_vl_has_vl_order_scheduled,
                        due_for_vl_has_vl_order_unscheduled,
                        due_for_vl_dont_have_order_scheduled,
                        due_for_vl_dont_have_order_unscheduled,
                        has_vl_this_week_and_suppressed,
                        has_vl_this_week_and_unsuppressed,
                        missed_to_active_this_week,
                        defaulter_to_active_this_week,
                        missed_cumulative,
                        is_med_ltfu_after_sep19_cumulative AS med_surge_ltfus_cumulative,
                        is_med_ltfu_after_sep19_unresolved AS med_surge_ltfu_and_med_ltfu_after_may,
                        med_defaulters, 
                        newly_med_ltfu_this_week,
                        med_surge_ltfus_outcomes,
                        med_surge_ltfus_outcomes_this_week,
                        ltfu_cumulative_outcomes_this_week_total as column_1,
                        null as column_2,
                        null as column_3,
                        null as column_4,
                        null as column_5,
                        null as column_6
                        
                        
                        from surge_weekly_report_dataset__2);


                    SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join surge_weekly_report_dataset_temporary_build_queue t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  

                    #select @person_ids_count := (select count(*) from @queue_table);                        
					SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1; 
                        
                    set @cycle_length = timestampdiff(second,@loop_start_time,now());
                    set @total_time = @total_time + @cycle_length;
                    set @cycle_number = @cycle_number + 1;
                    
                    set @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);
                            
            SELECT 
            @person_ids_count as 'persons remaining',
            @cycle_length AS 'Cycle time (s)',
            CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
            @remaining_time AS 'Est time remaining (min)';

        end while;
  
        if(@query_type="build") then
            SET @dyn_sql=CONCAT('drop table ',@queue_table,';'); 
            PREPARE s1 from @dyn_sql; 
            EXECUTE s1; 
            DEALLOCATE PREPARE s1; 
            
            SET @total_rows_to_write=0;
            SET @dyn_sql=CONCAT("Select count(*) into @total_rows_to_write from ",@write_table);
            PREPARE s1 from @dyn_sql; 
            EXECUTE s1; 
            DEALLOCATE PREPARE s1;
                                    
            set @start_write = now();
            select concat(@start_write, " : Writing ",@total_rows_to_write, ' to ',@primary_table);

            SET @dyn_sql=CONCAT('replace into ', @primary_table,
                '(select * from ',@write_table,');');
            PREPARE s1 from @dyn_sql; 
            EXECUTE s1; 
            DEALLOCATE PREPARE s1;
            
            set @finish_write = now();
            set @time_to_write = timestampdiff(second,@start_write,@finish_write);
            select concat(@finish_write, ' : Completed writing rows. Time to write to primary table: ', @time_to_write, ' seconds ');                        
            
            SET @dyn_sql=CONCAT('drop table ',@write_table,';'); 
            PREPARE s1 from @dyn_sql; 
            EXECUTE s1; 
            DEALLOCATE PREPARE s1;  
                        
        end if; 

    set @ave_cycle_length = ceil(@total_time/@cycle_number);
    select CONCAT('Average Cycle Length: ', @ave_cycle_length, ' second(s)');
             
  set @end = now();
  if(log = true) then
  insert into flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
  end if;
SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');
  
 END$$
DELIMITER ;