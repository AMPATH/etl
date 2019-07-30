CREATE  PROCEDURE `generate_surge_weekly_report_dataset_v1_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int , IN log boolean)
BEGIN

			set @start = now();
			set @table_version = "surge_weekly_report_dataset_v1.0";
			set @last_date_created = (select max(date_created) from etl.flat_hiv_summary_v15b);

            
CREATE TABLE IF NOT EXISTS surge_weekly_report_dataset (
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
    gender VARCHAR(50),
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
    returned_to_care_this_week INT(0),
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
    all_ltfu_this_week INT(0),
   active_october_2018_and_ltfu_may_2019_cohort INT(0),
   active_october_2018_and_ltfu_this_week INT(0),
   ltfu_may_2019_and_ltfu_this_week INT(0),
   newly_ltfu_this_week INT(0),
   ltfu_cumulative_outcomes_death INT(0),
   ltfu_cumulative_outcomes_transfer_out INT(0),
   ltfu_cumulative_outcomes_active INT(0),
   active_to_ltfu_count INT(0),


    defaulted_this_week INT(0),
    due_for_vl_this_week_active INT(0),
    on_schedule_this_week INT(0),
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
					select "BUILDING.......................";
					set @queue_table = concat("surge_weekly_report_dataset_build_queue_",queue_number);                                       				
					SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,'(person_id int primary key) (select * from surge_weekly_report_dataset_build_queue limit ',queue_size, ');'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;
                    
					SET @dyn_sql=CONCAT('delete t1 from surge_weekly_report_dataset_build_queue t1 join ',@queue_table, ' t2 using (person_id)'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
end if;

SET @num_ids := 0;
SET @dyn_sql=CONCAT('select count(*) into @num_ids from ',@queue_table,';'); 
PREPARE s1 from @dyn_sql; 
EXECUTE s1; 
DEALLOCATE PREPARE s1; 

SET @person_ids_count = 0;
SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
PREPARE s1 from @dyn_sql; 
EXECUTE s1; 
DEALLOCATE PREPARE s1;

SELECT CONCAT('Patients in queue: ', @person_ids_count);
            
             
            
set @total_time=0;
set @cycle_number = 0;

while @person_ids_count > 0 do

	set @loop_start_time = now();                        
	drop temporary table if exists surge_weekly_report_dataset_1_test_build_queue__0;
	create temporary table surge_weekly_report_dataset_1_test_build_queue__0 (person_id int primary key);
    
    SET @dyn_sql=CONCAT('insert into surge_weekly_report_dataset_1_test_build_queue__0 (select * from ',@queue_table,' limit ',cycle_size,');'); 
	PREPARE s1 from @dyn_sql; 
	EXECUTE s1; 
	DEALLOCATE PREPARE s1;
    
SELECT CONCAT('creating patient_vl ...');
    
drop table if exists patient_vl;
create temporary table patient_vl(
select t1.person_id, vl_1,vl_1_date from flat_hiv_summary_v15b t1
join surge_weekly_report_dataset_1_test_build_queue__0 t2 USING (person_id) where vl_1_date is not null
ORDER BY t1.person_id , t1.encounter_datetime
 );
 
SELECT CONCAT('creating surge_vitals ...');
 
 drop table  if exists surge_vitals;
 create temporary table surge_vitals(index(person_id), index (person_id,encounter_datetime), index(encounter_datetime))
 SELECT 
    @prev_id := @cur_id AS prev_id,
    @cur_id := t1.person_id AS cur_id,
    t1.person_id as person_id,
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
    FORMAT (@weight / (@height * @height) * 10000,2)  as bmi
    
    from 
    (select height,weight,encounter_datetime,tv.person_id from  etl.flat_vitals tv 
    inner join surge_weekly_report_dataset_1_test_build_queue__0 t2 on tv.person_id=t2.person_id
order by t2.person_id , encounter_datetime) t1;
 
set @age =null;
set @status = null;

SELECT CONCAT('creating surge_weekly_report_dataset_0 ...');
    
drop temporary table if exists surge_weekly_report_dataset_0;
create temporary table surge_weekly_report_dataset_0               
(index (person_id), index(start_date), index(end_date),index(encounter_date))
SELECT
    concat(date_format(t1.end_date,"%Y%m"),t2.person_id) as elastic_id,
    t3.uuid AS person_uuid,
    t2.person_id,
    t1.week AS year_week,
        YEARWEEK(t2.encounter_datetime) encounter_yw,
            encounter_id,
    t2.encounter_datetime,
	DATE(t2.encounter_datetime) AS encounter_date,
    t1.end_date,
    t1.start_date,
    DATE(birthdate) AS birthdate,
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
    t3.gender,
    format(t6.weight,2) as weight,
    format(t6.height,2) as height,
    t6.bmi,
    prev_rtc_date,
    rtc_date,
    IF(YEARWEEK(t2.encounter_datetime) = t1.week,
        1,
        0) AS visit_this_week,
    IF(DATE(t2.encounter_datetime) = DATE(prev_clinical_rtc_date_hiv),
        1,
        0) AS on_schedule,
    IF((yearweek(prev_clinical_rtc_date_hiv,1)=t1.week  OR (prev_clinical_rtc_date_hiv is null and
	yearweek(rtc_date,1)=t1.week))  and  
    (DATE(t2.encounter_datetime) = DATE(prev_clinical_rtc_date_hiv) OR (prev_clinical_rtc_date_hiv is null and DATE(t2.encounter_datetime) = DATE(rtc_date))),
        1,
        0) AS on_schedule_this_week,
    IF(DATE(t2.encounter_datetime) < DATE(prev_clinical_rtc_date_hiv),
        1,
        0) AS early_appointment,
    IF(((DATE(t2.encounter_datetime) < DATE(prev_clinical_rtc_date_hiv)) OR (
	prev_clinical_rtc_date_hiv is null and DATE(t2.encounter_datetime) < DATE(rtc_date)))
            AND YEARWEEK(t2.encounter_datetime,1) = t1.week,
        1,
        0) AS early_appointment_this_week,

    IF(((DATE(t2.encounter_datetime) > DATE(prev_clinical_rtc_date_hiv)) OR ( prev_clinical_rtc_date_hiv is null  and DATE(t2.encounter_datetime) > DATE(rtc_date)))
            AND YEARWEEK(t2.encounter_datetime,1) = t1.week,
        1,
        0)  AS late_appointment_this_week,
    TIMESTAMPDIFF(DAY,
        prev_clinical_rtc_date_hiv,
        end_date) AS days_since_rtc_date,
    IF(YEARWEEK(prev_clinical_rtc_date_hiv,1) = t1.week
            OR  (prev_clinical_rtc_date_hiv is null  and  t1.week = YEARWEEK(rtc_date,1)),
        1,
        0) AS scheduled_this_week,
    CASE
        WHEN
            YEARWEEK(t2.encounter_datetime,1) = t1.week
                AND ((YEARWEEK(t2.encounter_datetime,1) != YEARWEEK(prev_clinical_rtc_date_hiv,1)) OR ( prev_clinical_rtc_date_hiv is null  and YEARWEEK(t2.encounter_datetime,1) != YEARWEEK(rtc_date,1)))
        THEN
            1
        ELSE 0
    END AS unscheduled_this_week,
  #  NULL AS tx2_visit_this_week,
  #  NULL AS missed_tx2_visit_this_week,
    t2.death_date,
    CASE
        WHEN
            YEARWEEK(t2.encounter_datetime,1) != t1.week
                AND (t1.week = YEARWEEK(prev_clinical_rtc_date_hiv,1)
                OR (prev_clinical_rtc_date_hiv IS NULL AND t1.week = YEARWEEK(rtc_date,1)))
        THEN
            1
        ELSE 0
    END AS missed_appointment_this_week,
    IF(TIMESTAMPDIFF(DAY, rtc_date, end_date) > 28,
        1,
        0) AS ltfu,
    IF(TIMESTAMPDIFF(DAY, rtc_date, end_date) > 4
            AND TIMESTAMPDIFF(DAY, rtc_date, end_date) < 28,
        1,
        0) AS defaulted,
    IF(TIMESTAMPDIFF(DAY, rtc_date, end_date) > 0
            AND TIMESTAMPDIFF(DAY, rtc_date, end_date) < 5,
        1,
        0) AS missed,
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
            YEARWEEK(t2.encounter_datetime,1) = t1.week
                AND YEARWEEK(arv_first_regimen_start_date,1) != t1.week
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
    vl.vl_1,
    vl.vl_1_date,
    vl_2,
    vl_2_date,
    IF(YEARWEEK(vl.vl_1_date) = t1.week, 1, 0) AS has_vl_this_week,
    IF(vl.vl_1 IS NOT NULL AND vl.vl_1 < 400,
        1,
        0) AS is_suppressed,
    IF(vl.vl_1 IS NOT NULL AND vl.vl_1 > 400,
        1,
        0) AS is_un_suppressed,
    TIMESTAMPDIFF(DAY, vl.vl_1_date, end_date) AS days_since_last_vl,
    CASE
        WHEN
            vl.vl_1 > 1000
                AND vl.vl_1_date > arv_start_date
                AND TIMESTAMPDIFF(MONTH,
                vl.vl_1_date,
                end_date) BETWEEN 3 AND 11
        THEN
            1
        WHEN
            TIMESTAMPDIFF(MONTH,
                arv_start_date,
                end_date) BETWEEN 6 AND 11
                AND vl.vl_1 IS NULL
        THEN
            1
        WHEN
            TIMESTAMPDIFF(MONTH,
                arv_start_date,
                end_date) BETWEEN 12 AND 18
                AND vl_2 IS NULL
        THEN
            1
        WHEN
            TIMESTAMPDIFF(MONTH,
                arv_start_date,
                end_date) >= 12
                AND IFNULL(TIMESTAMPDIFF(MONTH,
                        vl.vl_1_date,
                        end_date) >= 12,
                    1)
        THEN
            1
    END AS due_for_vl_this_week,
        CASE
        WHEN
            vl.vl_1 > 1000
                AND vl.vl_1_date > arv_start_date
                AND TIMESTAMPDIFF(MONTH,
                vl.vl_1_date,
                end_date) BETWEEN 3 AND 11
        THEN
            1
        WHEN
            TIMESTAMPDIFF(MONTH,
                arv_start_date,
                end_date) BETWEEN 6 AND 11
                AND vl.vl_1 IS NULL
        THEN
            1
        WHEN
            TIMESTAMPDIFF(MONTH,
                arv_start_date,
                end_date) BETWEEN 12 AND 18
                AND vl_2 IS NULL
        THEN
            1
        WHEN
            TIMESTAMPDIFF(MONTH,
                arv_start_date,
                end_date) >= 12
                AND IFNULL(TIMESTAMPDIFF(MONTH,
                        vl.vl_1_date,
                        end_date) >= 12,
                    1)
        THEN
            1
    END AS patients_due_for_vl,
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
    CASE
        WHEN YEARWEEK(t1.end_date) > YEARWEEK(t2.death_date) THEN @status:='dead'
        WHEN YEARWEEK(t1.end_date) > YEARWEEK(transfer_out_date) THEN @status:='transfer_out'
        WHEN
            TIMESTAMPDIFF(DAY,
                IF(rtc_date,
                    rtc_date,
                    DATE_ADD(t2.encounter_datetime,
                        INTERVAL 28 DAY)),
                t1.end_date) <= 28
        THEN
            @status:='active'
        WHEN
            TIMESTAMPDIFF(DAY,
                IF(rtc_date,
                    rtc_date,
                    DATE_ADD(t2.encounter_datetime,
                        INTERVAL 28 DAY)),
                t1.end_date) BETWEEN 29 AND 90
        THEN
            @status:='defaulter'
        WHEN
            TIMESTAMPDIFF(DAY,
                IF(rtc_date,
                    rtc_date,
                    DATE_ADD(t2.encounter_datetime,
                        INTERVAL 28 DAY)),
                t1.end_date) > 90
        THEN
            @status:='ltfu'
        ELSE @status:='unknown'
    END AS status,
    IF(YEARWEEK(t4.date_enrolled) = t1.week and (t4.program_id=3 or t4.program_id=9),
        1,
        0) AS started_dc_this_week, 
         IF(t4.program_id=3 or t4.program_id=9,
        1,
        0) AS on_dc_this_week,
        IF(YEARWEEK(t4.date_enrolled) = t1.week and t4.program_id=10,
        1,
        0) AS new_prep_this_week, 
        IF(t4.program_id=10,
        1,
        0) AS cur_prep_this_week,
    t2.location_id,
    IF((DATE(arv_first_regimen_start_date) = DATE(t2.encounter_datetime)
            AND YEARWEEK(rtc_date,1) = t1.week) or (DATE(arv_first_regimen_start_date) = DATE(prev_clinical_datetime_hiv)
            AND YEARWEEK(prev_rtc_date,1) = t1.week) ,
        1,
        0) AS tx2_scheduled_this_week,
	IF(DATE(arv_first_regimen_start_date) = DATE(prev_clinical_datetime_hiv)
            AND YEARWEEK(t2.encounter_datetime,1) = t1.week,
        1,
        0) AS tx2_visit_this_week,
	IF(DATE(arv_first_regimen_start_date) = DATE(prev_clinical_datetime_hiv)
            AND YEARWEEK(t2.encounter_datetime,1) = t1.week AND YEARWEEK(prev_rtc_date,1) = t1.week,
        1,
        0) AS tx2_scheduled_honored,
		is_pregnant,
        tb_tx_start_date, 
        TIMESTAMPDIFF(MONTH,
        tb_tx_start_date,
        end_date) AS months_since_tb_tx_start_date,
		CASE
        WHEN YEARWEEK(vl_order_date) = t1.week THEN 1
        ELSE 0
    END AS vl_ordered_this_week,
    if(YEARWEEK(t2.encounter_datetime)= t1.week and encounter_type=21,1,0) intervention_done_this_week,
    if( encounter_type=21,1,0) interventions
FROM
    surge_week t1
        JOIN
    flat_hiv_summary_v15b t2
        JOIN
    amrs.person t3 USING (person_id)
        JOIN
    surge_weekly_report_dataset_1_test_build_queue__0 t5 USING (person_id)
    LEFT JOIN patient_vl vl on vl.person_id=t2.person_id and yearweek(vl.vl_1_date)=t1.week
        LEFT JOIN
    amrs.patient_program t4 ON t4.patient_id = t2.person_id AND t4.program_id in(3,9,10,11)  AND t4.date_completed IS NULL
    LEFT JOIN surge_vitals t6 on t6.person_id=t2.person_id and t6.encounter_datetime < DATE_ADD(t1.end_date, INTERVAL 1 DAY)
              and t6.encounter_datetime > DATE_ADD(t1.start_date, INTERVAL -1 DAY)
        
WHERE
    t2.encounter_datetime < DATE_ADD(t1.end_date, INTERVAL 1 DAY)
        AND (t2.next_clinical_datetime_hiv IS NULL
        OR t2.next_clinical_datetime_hiv >= DATE_ADD(t1.end_date, INTERVAL 1 DAY))
        AND t2.is_clinical_encounter = 1
ORDER BY t2.person_id , t2.encounter_datetime , rtc_date , end_date;


set @prev_id = -1;
set @cur_id = -1;
set @prev_enc_id = -1;
set @cur_enc_id = -1;
set @cur_status = '';
set @prev_status = '';
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

SELECT CONCAT('creating surge_weekly_report_dataset__1 ...');


drop temporary table if exists surge_weekly_report_dataset__1;
create temporary table surge_weekly_report_dataset__1
(select 
*,
    @prev_id:=@cur_id AS prev_id,
    @cur_id:=person_id AS cur_id,
    @prev_enc_id:=@cur_enc_id AS prev_enc_id,
    @cur_enc_id:=encounter_id AS cur_enc_id,
    
    CASE
        WHEN @prev_id = @cur_id and @prev_enc_id != @cur_enc_id THEN @clinic_visit_number := @clinic_visit_number  + 1
        WHEN @prev_id = @cur_id and @prev_enc_id = @cur_enc_id THEN @clinic_visit_number
        ELSE @clinic_visit_number := 1
    END AS clinical_visit_num,
    
    CASE
        WHEN @prev_id = @cur_id THEN @prev_status:= @cur_status
        ELSE @prev_status:=''
    END AS prev_status,
	@cur_status := status as cur_status,
    CASE
        WHEN
            @prev_id = @cur_id
                AND @prev_status = 'ltfu'
                AND @cur_status = 'active'
        THEN
            1
        ELSE 0
    END AS returned_to_care_this_week,
	CASE
        WHEN @prev_id != @cur_id  THEN @height := height 
        WHEN @prev_id = @cur_id and @height is null and height is not null THEN @height := height
        WHEN @prev_id = @cur_id and @height  and height  THEN @height := height
        ELSE @height 
    END AS revised_height,
    CASE
        WHEN @prev_id != @cur_id  THEN @weight := weight 
        WHEN @prev_id = @cur_id and @weight is null and weight is not null THEN @weight := weight
        WHEN @prev_id = @cur_id and @weight  and weight  THEN @weight := weight
        ELSE @weight 
    END AS revised_weight,
    CASE
        WHEN @prev_id != @cur_id  THEN @bmi := bmi 
        WHEN @prev_id = @cur_id and @bmi is null and bmi is not null THEN @bmi := bmi
        WHEN @prev_id = @cur_id and @bmi  and bmi  THEN @bmi := bmi
        ELSE @bmi 
    END AS revised_bmi,
    CASE
        WHEN @prev_id = @cur_id THEN @prev_patients_due_for_vl:= @cur_patients_due_for_vl
        ELSE @prev_patients_due_for_vl:=0
    END AS prev_patients_due_for_vl,
     if(tx2_scheduled_this_week=1 and year_week!=yearweek(encounter_date,1),1,0) as    tx2_missed_this_week,
	if(@cur_status = 'ltfu', 1,0) as all_ltfu_this_week,
	if( @is_ltfu_surge_baseline = 1, 1, 0) as active_october_2018_and_ltfu_may_2019_cohort,
    if( @ever_active_after_october_18 = 1 and @cur_status = 'ltfu', 1,0) as active_october_2018_and_ltfu_this_week,
	if( @is_ltfu_surge_baseline = 1 and @cur_status = 'ltfu', 1,0) as ltfu_may_2019_and_ltfu_this_week, 
    if( @prev_status = 'active'   and @cur_status = 'ltfu', 1,0) as newly_ltfu_this_week,
    if( @is_ltfu_surge_baseline = 1 and @cur_status = 'dead', 1,0) as ltfu_cumulative_outcomes_death,
    if( @is_ltfu_surge_baseline = 1  and @cur_status = 'transfer_out', 1,0) as ltfu_cumulative_outcomes_transfer_out ,
    if( @is_ltfu_surge_baseline = 1 and @cur_status = 'active' or @cur_status = 'transfer_in', 1,0) as ltfu_cumulative_outcomes_active,
    CASE
        WHEN @prev_id = @cur_id and @prev_status = 'active' and @cur_status= 'ltfu' THEN @active_to_ltfu_count := @active_to_ltfu_count  + 1
        ELSE @active_to_ltfu_count := 0
    END AS active_to_ltfu_count,
    
    
     
     
     
     
     
    
     if((months_since_tb_tx_start_date is null or months_since_tb_tx_start_date >=6) and  on_dc_this_week != 1 and (is_pregnant =0 or is_pregnant is null) 
     and @bmi>18.5 and age>= 20 and @cur_status = 'active' and days_since_starting_arvs>364 and vl_1 >= 0 and vl_1 <= 400,1,0) as dc_eligible_cumulative,
     if(patients_due_for_vl=1 and @prev_patients_due_for_vl=0 and scheduled_this_week=1, 1,0) as scheduled_this_week_and_due_for_vl,
     if(patients_due_for_vl=1 and @prev_patients_due_for_vl=0 and scheduled_this_week=0, 1,0) as unscheduled_this_week_and_due_for_vl,
	 if(patients_due_for_vl=1 and @prev_patients_due_for_vl=0 and vl_ordered_this_week=1 and @cur_status='active', 1,0) as due_for_vl_has_vl_order,
     if(patients_due_for_vl=1 and @prev_patients_due_for_vl=0 and vl_ordered_this_week=0 and @cur_status='active', 1,0) as due_for_vl_dont_have_order,
     if(patients_due_for_vl=1 and @prev_patients_due_for_vl=0 and @cur_status='active', 1,0) as due_for_vl_this_week_active,
     if(patients_due_for_vl=1 and @cur_status='active', 1,0) as overdue_for_vl_active,
     CASE
        WHEN @prev_id = @cur_id THEN @prev_defaulted:= @cur_defaulted
        ELSE @prev_defaulted:=0
    END AS prev_defaulted,
	@cur_defaulted := defaulted as cur_defaulted,
    if( @cur_defaulted=0 and  @cur_defaulted =1, 1,0) as defaulted_this_week,
     CASE
        WHEN @prev_id = @cur_id THEN @prev_missed:= @cur_missed
        ELSE @prev_missed:=0
    END AS prev_missed,
	@cur_missed := missed as cur_missed,
   if( @cur_missed=0 and  @cur_missed =1, 1,0) as missed_this_week,
     CASE
        WHEN @prev_id = @cur_id THEN @prev_ltfu:= @cur_ltfu
        ELSE @prev_ltfu:=0
    END AS prev_ltfu,
	@cur_ltfu := ltfu as cur_ltfu,
    if( @cur_ltfu=0 and  @cur_ltfu =1, 1,0) as ltfu_this_week 
FROM
    surge_weekly_report_dataset_0 
     );
     
SELECT CONCAT('replacing into surge_weekly_report_dataset ...');
     										  
replace into surge_weekly_report_dataset									  
			(select
				 null,
                 elastic_id,
				 person_uuid,
				 person_id,
				 year_week,
				 encounter_yw,
				 encounter_id,
				 encounter_datetime,
				 encounter_date,
				 end_date,
				 start_date,
				 birthdate,
				 age,
				 gender,
				 clinical_visit_num as clinical_visit_number,
				 prev_rtc_date,
				 rtc_date,
				 visit_this_week,
				 on_schedule,
				 early_appointment,
				 early_appointment_this_week,
				 late_appointment_this_week,
				 days_since_rtc_date,
				 scheduled_this_week,
				 unscheduled_this_week,
				 tx2_visit_this_week,
				 tx2_missed_this_week as missed_tx2_visit_this_week,
				 death_date,
				 missed_appointment_this_week,
				 ltfu,
				 defaulted,
				 missed,
				 null as next_status,
				 active_in_care_this_week,
				 cur_arv_adherence,
				 cur_who_stage,
				 is_pre_art_this_week,
				 arv_first_regimen_location_id,
				 arv_first_regimen,
				 arv_first_regimen_names,
				 arv_first_regimen_start_date,
				 days_since_starting_arvs,
				 started_art_this_week,
				 enrollment_date,
				 enrolled_this_week,
				 art_revisit_this_week,
				 cur_arv_meds,
				 cur_arv_meds_names,
				cur_arv_meds_strict,
				cur_arv_line,
				cur_arv_line_strict,
				cur_arv_line_reported,
				on_art_this_week,
				vl_1,
				vl_1_date,
				vl_2,
				vl_2_date,
				has_vl_this_week,
				is_suppressed,
				is_un_suppressed,
				days_since_last_vl,
				due_for_vl_this_week,
				null as reason_for_needing_vl_this_week,
				cd4_1,
				cd4_1_date,
				child_hiv_status_disclosure_status,
				transfer_in_this_week,
				transfer_in_location_id,
				transfer_in_date,
				transfer_out_this_week,
				transfer_out_location_id,
				transfer_out_date,
				status,
				dc_eligible_cumulative,
				started_dc_this_week,
				location_id,
				tx2_scheduled_this_week,
				tx2_scheduled_honored,
				prev_id,
				cur_id,
				prev_enc_id,
				cur_enc_id,
				clinical_visit_num,
				prev_status,
				cur_status,
				returned_to_care_this_week ,
				cur_prep_this_week,
				new_prep_this_week,
				revised_height as height,
				revised_weight as weight,
				revised_bmi as bmi,
                scheduled_this_week_and_due_for_vl,
                unscheduled_this_week_and_due_for_vl,
                overdue_for_vl_active,

				due_for_vl_has_vl_order,
				due_for_vl_dont_have_order,
				ltfu_this_week,
	            missed_this_week,
                all_ltfu_this_week,
                active_october_2018_and_ltfu_may_2019_cohort,
                active_october_2018_and_ltfu_this_week,
                ltfu_may_2019_and_ltfu_this_week,
                newly_ltfu_this_week,
                ltfu_cumulative_outcomes_death,
                ltfu_cumulative_outcomes_transfer_out,
                ltfu_cumulative_outcomes_active,
                active_to_ltfu_count,

	            defaulted_this_week,
                due_for_vl_this_week_active,
                on_schedule_this_week
                
                from surge_weekly_report_dataset__1);
                
                
				SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join surge_weekly_report_dataset_1_test_build_queue__0 t2 using (person_id);'); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;  
				
				SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';'); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;  
                
                
				set @cycle_length = timestampdiff(second,@loop_start_time,now());
				set @total_time = @total_time + @cycle_length;
				set @cycle_number = @cycle_number + 1;
				
				set @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);
                
                
SELECT 
    @num_in_hmrd AS num_in_hmrd,
    @person_ids_count AS num_remaining,
    @cycle_length AS 'Cycle time (s)',
    CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
    @remaining_time AS 'Est time remaining (min)';






end while;

if(query_type = "build") then
					SET @dyn_sql=CONCAT('drop table ',@queue_table,';'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
end if;  
            
           
set @end = now();
if(log = true) then
insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
end if;
SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');

END