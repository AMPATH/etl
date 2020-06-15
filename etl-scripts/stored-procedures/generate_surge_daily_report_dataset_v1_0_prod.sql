DELIMITER $$
CREATE  PROCEDURE `generate_surge_daily_report_dataset_v1_0_prod`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int , IN log boolean)
BEGIN
			set @start = now();
			set @table_version = "surge_daily_report_dataset_v1.0";
			set @last_date_created = (select max(date_created) from etl.flat_hiv_summary_v15b);
CREATE TABLE IF NOT EXISTS `surge_daily_report_dataset` (
  `elastic_id` varchar(19) CHARACTER SET utf8 NOT NULL DEFAULT '',
  `location_id` int(11) DEFAULT NULL,
  `person_id` int(11) NOT NULL,
  `person_uuid` varchar(100) DEFAULT NULL, 
  `birthdate` date DEFAULT NULL,
  `death_date` datetime DEFAULT NULL,
  `age` decimal(23,2) DEFAULT NULL,
  `gender` varchar(50) CHARACTER SET utf8 DEFAULT '',
  `encounter_id` int(11) NOT NULL DEFAULT '0',
  `encounter_datetime` datetime DEFAULT NULL,
  `_date` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `prev_clinical_rtc_date_hiv` datetime DEFAULT NULL,
  `prev_rtc_date` datetime DEFAULT NULL,
  `rtc_date` datetime DEFAULT NULL,
  `visit_today` int(1) DEFAULT '0',
  `appointment_today` int(1)  DEFAULT '0',
  `scheduled_visit_today` int(1)  DEFAULT '0',
  `early_appointment_today` int(1)  DEFAULT '0',
  `late_appointment_today` int(1)  DEFAULT '0',
  `missed_appointment_today` int(1) DEFAULT '0',
  `days_since_rtc_date` varchar(21) CHARACTER SET utf8 DEFAULT NULL,
  `status` varchar(12) CHARACTER SET utf8 DEFAULT NULL,
  `cur_arv_adherence` varchar(200) DEFAULT NULL,
  `arv_first_regimen_location_id` int(11) DEFAULT NULL,
  `arv_first_regimen` varchar(500) DEFAULT NULL,
  `arv_first_regimen_names` text,
  `arv_first_regimen_start_date` datetime DEFAULT NULL,
  `days_since_starting_arvs` bigint(21) DEFAULT NULL,
  `started_art_today` int(1) DEFAULT '0',
  `enrollment_date` datetime DEFAULT NULL,
  `enrolled_today` int(1) DEFAULT '0',
  `art_revisit_today` int(1) DEFAULT '0',
  `arv_start_date` datetime DEFAULT NULL,
  `cur_arv_meds` varchar(500) DEFAULT NULL,
  `cur_arv_meds_names` text,
  `cur_arv_meds_strict` varchar(500) DEFAULT NULL,
  `cur_arv_line` int(11) DEFAULT NULL,
  `cur_arv_line_strict` int(11) DEFAULT NULL,
  `cur_arv_line_reported` tinyint(4) DEFAULT NULL,
  `transfer_in_today` int(1) DEFAULT '0',
  `transfer_in_location_id` int(11) DEFAULT NULL,
  `transfer_in_date` datetime DEFAULT NULL,
  `transfer_out_today` int(1) DEFAULT '0',
  `transfer_out_location_id` int(11) DEFAULT NULL,
  `transfer_out_date` datetime DEFAULT NULL,
  `tx2_appointment_today` int(1) DEFAULT '0',
  `tx2_visit_today` int(1) DEFAULT '0',
  `tx2_appointment_honored` int(1) DEFAULT '0',
  `tx2_appointment_missed` int(1)  DEFAULT '0',
  `tx2_unscheduled_today`  int(1)  DEFAULT '0',
  `is_pregnant` tinyint(1) DEFAULT NULL,
  `tb_tx_start_date` datetime DEFAULT NULL,
  `months_since_tb_tx_start_date` bigint(21) DEFAULT NULL,
  `intervention_done_today` int(1)  DEFAULT '0',
  `interventions` int(1) DEFAULT '0',
  `vl_ordered_today` int(0) DEFAULT NULL,
  `vl_encounter_datetime` datetime DEFAULT NULL,
  `last_viralload` varchar(200) DEFAULT NULL,
  `last_viralload_date` datetime DEFAULT NULL,
  `second_last_viralload` varchar(200) DEFAULT NULL,
  `second_last_viralload_date` datetime DEFAULT NULL,
  `is_suppressed` int(1) DEFAULT '0',
  `is_unsuppressed` int(1)  DEFAULT '0',
  `has_vl_today` int(1)  DEFAULT '0',
  `days_since_last_vl` bigint(21) DEFAULT NULL,
  `due_for_vl_today` int(1) DEFAULT '0',
  `cd4_1` double DEFAULT NULL,
  `cd4_1_date` datetime DEFAULT NULL,
  `weight` decimal(65,30) DEFAULT NULL,
  `height` decimal(65,30) DEFAULT NULL,
  `bmi` decimal(65,30) DEFAULT NULL,
  `started_dc_today` int(1)  DEFAULT '0',
  `on_dc_today` int(1)  DEFAULT '0',
  `prev_id` bigint(20) DEFAULT NULL,
  `cur_id` int(11) DEFAULT NULL,
  `prev_enc_id` bigint(20) DEFAULT NULL,
  `cur_enc_id` int(11) NOT NULL DEFAULT '0',
  `clinical_visit_num` bigint(63) DEFAULT NULL,
  `prev_status` longtext CHARACTER SET utf8,
  `cur_status` varchar(12) CHARACTER SET utf8 DEFAULT NULL,
  `ltfu_to_active_today` int(1)  DEFAULT '0',
  `ltfu_outcome_today` varchar(200) DEFAULT NULL,
  `ltfu_outcome_dead_today` varchar(200) DEFAULT NULL,
  `ltfu_returned_to_care_today` varchar(200) DEFAULT NULL,
  `ltfu_outcome_tranfer_out_today` varchar(200) DEFAULT NULL,
  `revised_height` decimal(65,30) DEFAULT NULL,
  `revised_weight` decimal(65,30) DEFAULT NULL,
  `revised_bmi` decimal(65,30) DEFAULT NULL,
  `eligible_and_on_dc` int(1)  DEFAULT '0',
  `eligible_not_on_dc` int(1)  DEFAULT '0',
  `eligible_not_on_dc_and_appointment_today` int(1)  DEFAULT '0',
  `eligible_not_on_dc_and_unscheduled_today` int(1)  DEFAULT '0',
  `eligible_and_on_dc_and_appointment_today` int(1)  DEFAULT '0',
  `eligible_and_on_dc_and_unscheduled_today` int(1)  DEFAULT '0',
  `elligible_total` int(1)  DEFAULT '0',
  `dc_eligible_cumulative` int(1)  DEFAULT '0',
  `scheduled_today_and_due_for_vl` int(1)  DEFAULT '0',
  `unscheduled_today_and_due_for_vl` int(1) DEFAULT '0',
  `due_for_vl_today_has_vl_order` int(1)  DEFAULT '0',
  `due_for_vl_today_dont_have_order` int(1)  DEFAULT '0',
  `due_for_vl_today_active` int(1)  DEFAULT '0',
  `overdue_for_vl_active` int(1)  DEFAULT '0',
  `is_suppressed_active` int(1)  DEFAULT '0',
  `is_unsuppressed_active` int(1)  DEFAULT '0',
  `was_active_october_18` int(1)  DEFAULT '0',
  `ever_active_after_october_18` int(1)  DEFAULT '0',
  `was_ltfu_may_19`  int(1)  DEFAULT '0',
  `is_ltfu_surge_baseline`  int(1)  DEFAULT '0',
  `active_after_may_19` int(1)  DEFAULT '0',
  `is_ltfu_after_may_revised` int(1)  DEFAULT '0',
  `is_ltfu_after_may`  int(1)  DEFAULT '0',
  `surge_ltfu_and_ltfu_after_may`  int(1)  DEFAULT '0',
  `ltfu_death_today` int(1)  DEFAULT '0',
  `ltfu_transfer_out_today`  int(1)  DEFAULT '0', 
  `ltfu_active_today`  int(1)  DEFAULT '0',
  `ltfu_outcome_total_today`  int(1)  DEFAULT '0',
  `dead_today`   int(1)  DEFAULT '0',
  `newly_ltfu_today` int(1)  DEFAULT '0',
  `defaulters_today`  int(1)  DEFAULT '0',
  PRIMARY KEY (`elastic_id`),
  KEY `location_id` (`location_id`),
  KEY `person_id` (`person_id`),
  KEY `_date` (`_date`),
  KEY `location_id_2` (`location_id`,`person_id`,`_date`),
  KEY `encounter_datetime` (`encounter_datetime`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

if (query_type = "build") then
					select "BUILDING.......................";
					set @queue_table = concat("surge_daily_report_dataset_build_queue_",queue_number);                                       				
					SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,'(person_id int primary key) (select * from surge_daily_report_dataset_build_queue limit ',queue_size, ');'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;
                    
					SET @dyn_sql=CONCAT('delete t1 from surge_daily_report_dataset_build_queue t1 join ',@queue_table, ' t2 using (person_id)'); 
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
	drop temporary table if exists surge_daily_report_dataset_temporary_build_queue;
	create temporary table surge_daily_report_dataset_temporary_build_queue (person_id int primary key);
    
    SET @dyn_sql=CONCAT('insert into surge_daily_report_dataset_temporary_build_queue (select * from ',@queue_table,' limit ',cycle_size,');'); 
	PREPARE s1 from @dyn_sql; 
	EXECUTE s1; 
	DEALLOCATE PREPARE s1;
    
    -- BEGIN HERE
			set @age =null;
			set @appointment_today = -1;
			set @visit_today = -1;
			set @days_since_rtc = null;
			set @status = null;
			set @min_date = (select min(_date) from surge_days);

			drop temporary table if exists hiv_summary_in_queue;
			create temporary table hiv_summary_in_queue               
			(index (person_id), index(person_id, encounter_datetime),  index(encounter_id), index(encounter_datetime), index(rtc_date))
			(select hs.*, p.birthdate, p.gender,
			 etl.get_arv_names(arv_first_regimen) as arv_first_regimen_names,  
			 etl.get_arv_names(cur_arv_meds) AS cur_arv_meds_names
			 from 
			 etl.flat_hiv_summary_v15b hs
					INNER JOIN
				amrs.person p USING (person_id)
					INNER JOIN
				surge_daily_report_dataset_temporary_build_queue q USING (person_id)
				
				Where 
				hs.encounter_datetime >= '2018-01-01'
				AND hs.is_clinical_encounter = 1
				order by person_id, encounter_datetime
			);
            
            drop temporary table if exists active_enrollments_in_queue;
			create temporary table active_enrollments_in_queue               
			(index (person_id), index(person_id, date_enrolled), index(date_enrolled))
			(select 
					q.person_id,
					pe.program_id,
					pe.date_enrolled,
					pe.location_id
					from 
					amrs.patient_program pe 
					INNER JOIN
					surge_daily_report_dataset_temporary_build_queue q on (person_id = patient_id)
					Where 
					pe.date_completed is null and 
					(pe.voided = null or pe.voided = 0) and 
					pe.program_id IN (3 , 9)
					order by person_id, date_enrolled
			);
            
            set @prev_id = -1;
			set @cur_id = -1;
			set @height = null;
			set @weight = null;

			drop temporary table if exists vitals_in_queue;
			create temporary table vitals_in_queue
			(index (person_id), index(encounter_datetime), index(person_id, encounter_datetime)) 
			(
			   SELECT 
				  @prev_id := @cur_id AS prev_id,
				  @cur_id := person_id AS cur_id,
				  person_id,
				  encounter_id, 
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
					  (select 
					  v.person_id, 
					  encounter_id,
					  encounter_datetime,
					  height,
					  weight 
					  from  
					  etl.flat_vitals v 
					  inner join 
					  surge_daily_report_dataset_temporary_build_queue q on v.person_id=q.person_id
					  where v.encounter_datetime >= '2018-01-01'
					  order by v.person_id , encounter_datetime) v
			);
            
            drop temporary table if exists viralload_in_queue;
			create temporary table viralload_in_queue               
			(index (person_id),  index(encounter_datetime), index(person_id, encounter_datetime))
			(select 
			  hs.person_id,
			  encounter_datetime,
			  vl_1,
			  vl_2,
			  vl_2_date,
			  vl_1_date,
			  arv_start_date 
			 from 
			 etl.flat_hiv_summary_v15b hs
					INNER JOIN
				surge_daily_report_dataset_temporary_build_queue q USING (person_id)
				Where 
				hs.encounter_datetime >= '2018-01-01'
				order by person_id, encounter_datetime
			);

			drop temporary table if exists patient_date_encounters;
			create temporary table patient_date_encounters               
			(index (person_id), index(person_id, _date, encounter_id))
			( 
			 select person_id, _date, encounter_id from (select 
				h.person_id,
				 d._date, 
				 encounter_datetime,
				 encounter_id
				 from 
				 surge_days d
				 join
				 hiv_summary_in_queue h
				 WHERE
				h.encounter_datetime < DATE_ADD(_date, INTERVAL 1 DAY)
			ORDER BY h.person_id , d._date, h.encounter_datetime desc , rtc_date
			) p group by person_id, _date);
            
			drop temporary table if exists patient_date_vitals;
			create temporary table patient_date_vitals               
			(index (v_person_id), index (v_date),  UNIQUE v_person_id_date(v_person_id, v_date))
			( 
			 select 
				person_id as v_person_id, 
				_date as v_date, 
				encounter_datetime as vt_encounter_datetime,
				height,
				weight,
				bmi
				from 
					(select 
					h.person_id,
					 d._date, 
					 encounter_datetime,
					 height,
					 weight,
					 bmi
					 from 
					 surge_days d
					 join
					 vitals_in_queue h
					 WHERE
					h.encounter_datetime < DATE_ADD(_date, INTERVAL 1 DAY)
					ORDER BY h.person_id , d._date, h.encounter_datetime desc
					) p 
			group by person_id, _date);
            
            drop temporary table if exists patient_date_enrollments;
			create temporary table patient_date_enrollments               
			(index (e_person_id), index (e_date),  UNIQUE e_person_id_date(e_person_id, e_date))
			( 
			 select 
				person_id as e_person_id, 
				_date as e_date, 
				date_enrolled,
				program_id
				from 
					(select 
					h.person_id,
					 d._date, 
					 date_enrolled,
					 program_id,
					 location_id
					 from 
					 surge_days d
					 join
					 active_enrollments_in_queue h
					 WHERE
					h.date_enrolled < DATE_ADD(_date, INTERVAL 1 DAY)
					ORDER BY h.person_id , d._date, h.date_enrolled desc
					) p 
			group by person_id, _date);

			drop temporary table if exists patient_date_viralload;
			create temporary table patient_date_viralload               
			(index (vl_person_id), index(vl_date), UNIQUE pde_person_id_date (vl_person_id, vl_date))
			( 
			 select 
				person_id as vl_person_id,
				_date as vl_date, 
				concat(person_id, date_format(_date,"%Y%m%d")) as vl_person_date,
				encounter_datetime as vl_encounter_datetime,
				vl_1 as last_viralload,
				vl_1_date as last_viralload_date,
				vl_2 as second_last_viralload,
				vl_1_date as second_last_viralload_date,
				
				 if(vl_1 >= 0 and vl_1 < 400,1,0 ) as is_suppressed,
				 if(vl_1>=400,1,0 ) as is_unsuppressed,
				 if(DATE(vl_1_date)=_date,1,0 ) as has_vl_today,
				 
				 TIMESTAMPDIFF(DAY,vl_1_date, _date) AS days_since_last_vl,
				 
					 CASE
						WHEN
							vl_1 > 1000
								AND vl_1_date > arv_start_date
								AND TIMESTAMPDIFF(MONTH,
								vl_1_date,
								_date) BETWEEN 3 AND 11
						THEN
							1
						WHEN
							TIMESTAMPDIFF(MONTH,
								arv_start_date,
								_date) BETWEEN 6 AND 11
								AND vl_1 IS NULL
						THEN
							1
						WHEN
							TIMESTAMPDIFF(MONTH,
								arv_start_date,
								_date) BETWEEN 12 AND 18
								AND vl_2 IS NULL
						THEN
							1
						WHEN
							TIMESTAMPDIFF(MONTH,
								arv_start_date,
								_date) >= 12
								AND IFNULL(TIMESTAMPDIFF(MONTH,
										vl_1_date,
										_date) >= 12,
									1)
						THEN
							1
					END AS due_for_vl_today
				 from (select 
							 h.person_id,
							 d._date, 
							 encounter_datetime,
							 vl_1,
							 vl_2,
							 vl_2_date,
							 vl_1_date,
							 arv_start_date
							 from 
							 surge_days d
							 join
							 viralload_in_queue h
							 WHERE
							h.encounter_datetime < DATE_ADD(_date, INTERVAL 1 DAY)
						ORDER BY h.person_id , d._date, h.encounter_datetime desc
						) p 
			group by vl_person_id, vl_date);
				 
			-- select  * from patient_date_encounters  where person_id = 3080;


			drop temporary table if exists surge_daily_report_dataset_0;
			create temporary table surge_daily_report_dataset_0               
			(index (person_id), index(_date),index(encounter_datetime))
			(SELECT
			   concat(date_format(_date,"%Y%m%d"),hs.person_id) as elastic_id,
				location_id,
				hs.person_id, 
				hs.uuid AS person_uuid,
				DATE(birthdate) AS birthdate,
				death_date,
				CASE
					WHEN
						TIMESTAMPDIFF(YEAR, birthdate, _date) > 0
					THEN
						@age:=ROUND(TIMESTAMPDIFF(YEAR, birthdate, _date),
								0)
					ELSE @age:=ROUND(TIMESTAMPDIFF(MONTH,
								birthdate,
								_date) / 12,
							2)
				END AS age,
				gender,

				hs.encounter_id, 
				hs.encounter_datetime,
				pde._date,
				prev_clinical_rtc_date_hiv,
				prev_rtc_date,
				rtc_date,
				
				CASE
					WHEN DATE(hs.encounter_datetime) = DATE(_date) THEN @visit_today := 1
					ELSE @visit_today := 0 
				END AS visit_today,
				
				CASE
					WHEN DATE(prev_clinical_rtc_date_hiv)= DATE(_date) THEN @appointment_today := 1
					WHEN DATE(rtc_date)= DATE(_date) THEN @appointment_today := 1
					ELSE @appointment_today := 0 
				END AS appointment_today,
				
				IF(@visit_today = 1 AND @appointment_today = 1, 1, 0)  AS scheduled_visit_today,
				
				IF(@visit_today = 1  AND @appointment_today <> 1 AND DATE(hs.encounter_datetime) < DATE(prev_clinical_rtc_date_hiv)
					,1,0) AS early_appointment_today,
					
				IF(@visit_today = 1  AND @appointment_today <> 1 AND DATE(hs.encounter_datetime) > DATE(prev_clinical_rtc_date_hiv)
					,1,0) AS late_appointment_today,
					
				IF(@visit_today = 0  AND @appointment_today = 1,1,0) AS missed_appointment_today,

				 CASE 
				 WHEN @visit_today = 1  THEN @days_since_rtc := TIMESTAMPDIFF(DAY, rtc_date, _date)
				 WHEN @visit_today <> 1  THEN @days_since_rtc := TIMESTAMPDIFF(DAY, rtc_date, _date)
				 WHEN @visit_today <> 1  THEN @days_since_rtc := TIMESTAMPDIFF(DAY, prev_clinical_rtc_date_hiv, _date) 
				 ELSE @days_since_rtc := null
				 END AS days_since_rtc_date,
				 
				 CASE
					WHEN 
						DATE(_date) > DATE(death_date) 
					THEN @status:='dead'
					WHEN 
						DATE(_date) > DATE(transfer_out_date) 
					THEN 
						@status:='transfer_out'
					WHEN
						@days_since_rtc <= 0
					THEN
						@status:='active'
					WHEN
						@days_since_rtc > 0 AND @days_since_rtc <= 4
					THEN
						@status:='missed'
					WHEN
						@days_since_rtc > 4 and @days_since_rtc <= 28
					THEN
						@status:='defaulter'
					WHEN
						@days_since_rtc > 28
					THEN
						@status:='ltfu'
					ELSE @status:='unknown'
				END AS status,
				
				cur_arv_adherence,
				arv_first_regimen_location_id,
				arv_first_regimen,
				arv_first_regimen_names,
				arv_first_regimen_start_date,
				
				TIMESTAMPDIFF(DAY,
					arv_first_regimen_start_date,
					_date) AS days_since_starting_arvs,

				IF(DATE(arv_first_regimen_start_date) = _date,
					1,
					0) AS started_art_today,
					
				enrollment_date,
				IF(DATE(enrollment_date) = _date,
					1,
					0) AS enrolled_today,
				CASE
					WHEN
						DATE(encounter_datetime) = _date
							AND DATE(arv_first_regimen_start_date) != _date
							AND cur_arv_meds IS NOT NULL
							AND arv_first_regimen_start_date IS NOT NULL
					THEN
						1
					ELSE 0
				END AS art_revisit_today,
				
				arv_start_date,
				cur_arv_meds,
				cur_arv_meds_names,
				cur_arv_meds_strict,
				cur_arv_line,
				cur_arv_line_strict,
				cur_arv_line_reported,
				
				 CASE
					WHEN DATE(transfer_in_date) = _date THEN 1
					ELSE 0
				END AS transfer_in_today,
				transfer_in_location_id,
				transfer_in_date,

				CASE
					WHEN DATE(transfer_out_date) = _date THEN 1
					ELSE 0
				END AS transfer_out_today,
				transfer_out_location_id,
				transfer_out_date,
				
				CASE
					WHEN (DATE(arv_first_regimen_start_date) = DATE(encounter_datetime)
						AND DATE(rtc_date) = _date) or (DATE(arv_first_regimen_start_date) = DATE(prev_clinical_datetime_hiv)
						AND DATE(prev_rtc_date) = _date) 
					THEN @tx2_appointment_today := 1
					ELSE  @tx2_appointment_today := 0
				END as tx2_appointment_today,
				
				CASE
					WHEN DATE(arv_first_regimen_start_date) = DATE(prev_clinical_datetime_hiv)
						AND DATE(encounter_datetime) = _date
					THEN @tx2_visit_today := 1
					ELSE  @tx2_visit_today := 0
				END as tx2_visit_today,
				
				IF(@tx2_appointment_today = 1 AND @tx2_visit_today = 1,1,0) AS tx2_appointment_honored,
				
				IF(@tx2_appointment_today = 1 AND @tx2_visit_today = 0,1,0) AS tx2_appointment_missed,

                IF(@tx2_appointment_today = 0 AND @tx2_visit_today = 1,1,0) AS tx2_unscheduled_today,
				
				is_pregnant,
				tb_tx_start_date, 
				
				TIMESTAMPDIFF(MONTH,
					tb_tx_start_date,
					_date) AS months_since_tb_tx_start_date,

				if(YEARWEEK(encounter_datetime)= _date and encounter_type=21,1,0) intervention_done_today,
				if( encounter_type=21,1,0) interventions,
                
                 	CASE
					WHEN DATE(vl_order_date) = _date THEN 1
						ELSE 0
					END AS vl_ordered_today,
					
					vl_encounter_datetime, 
					last_viralload,
					last_viralload_date,
					second_last_viralload,
					second_last_viralload_date,
					is_suppressed,
					is_unsuppressed,
					has_vl_today,
					days_since_last_vl,
					due_for_vl_today,
					cd4_1,
					cd4_1_date,
					
					weight,
					height,
					bmi,
					
					IF(Date(date_enrolled) = _date
							 AND (program_id = 3 OR program_id = 9),
						 1,
						 0) AS started_dc_today,
					 IF(program_id = 3 OR program_id = 9,
						 1,
						 0) AS on_dc_today
			FROM
				patient_date_encounters pde
					INNER JOIN
				hiv_summary_in_queue hs using (encounter_id)
                		INNER JOIN
				patient_date_viralload vl force index (pde_person_id_date) on (pde.person_id = vl.vl_person_id AND pde._date = vl.vl_date) 
					INNER JOIN
				patient_date_vitals v force index (v_person_id_date) on (pde.person_id = v.v_person_id AND pde._date = v.v_date) 
					LEFT OUTER JOIN
				patient_date_enrollments e force index (e_person_id_date) on (pde.person_id = e.e_person_id AND pde._date = e.e_date) 
			);
			-- ORDER BY hs.person_id , _date, hs.encounter_datetime, rtc_date);

			-- select  * from surge_daily_report_dataset_0  where person_id = 3080; 

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

			set @was_active_october_18:= null;
			set @ever_active_after_october_18:= null;
			set @was_ltfu_may_19 := null;
			set @is_ltfu_surge_baseline := null;
			set @ltfu_outcome_this_week := null;
			set @surge_baseline_ltfu_outcome_this_week := null;

			SELECT CONCAT('creating surge_weekly_report_dataset__2 ...');

			drop temporary table if exists surge_daily_report_dataset_2;
			create temporary table surge_daily_report_dataset_2
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
				END AS ltfu_to_active_today,
				
				CASE
				  WHEN
					 @prev_id = @cur_id
							AND @prev_status = 'ltfu'
							AND @cur_status = 'active'
					then 'returned_to_care'
					WHEN
					 @prev_id = @cur_id
							AND @prev_status = 'ltfu'
							AND @cur_status = 'dead'
					then 'dead'
					WHEN
					 @prev_id = @cur_id
							AND @prev_status = 'ltfu'
							AND @cur_status = 'transfer_out'
					then 'transfer_out'
				END AS ltfu_outcome_today,

            CASE
                WHEN
                    @prev_id = @cur_id
                        AND @prev_status = 'ltfu'
                        AND @cur_status = 'dead'
                then 'dead'
            END AS ltfu_outcome_dead_today,
				
			CASE
				  WHEN
					 @prev_id = @cur_id
							AND @prev_status = 'ltfu'
							AND @cur_status = 'active'
					then 'returned_to_care'
				END AS ltfu_returned_to_care_today,
				
		     CASE
					WHEN
					 @prev_id = @cur_id
							AND @prev_status = 'ltfu'
							AND @cur_status = 'transfer_out'
					then 'transfer_out'
				END AS ltfu_outcome_tranfer_out_today,
				
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
				     WHEN days_since_last_vl <= 365
			            AND second_last_viralload IS NOT NULL
						 AND (months_since_tb_tx_start_date IS NULL
						 OR months_since_tb_tx_start_date >= 6)
						 AND on_dc_today = 1
						 AND (is_pregnant = 0 OR is_pregnant IS NULL)
						 AND @bmi >= 18.5
						 AND age >= 20
						 AND @cur_status = 'active'
						 AND days_since_starting_arvs > 364
						 AND last_viralload >= 0
						 AND last_viralload <= 400 
				         THEN @eligible_and_on_dc := 1
				         ELSE @eligible_and_on_dc := 0
			         END AS eligible_and_on_dc,
		        
		       CASE
				     WHEN days_since_last_vl <= 365
			            AND second_last_viralload IS NOT NULL
						 AND (months_since_tb_tx_start_date IS NULL
						 OR months_since_tb_tx_start_date >= 6)
						 AND on_dc_today != 1
						 AND (is_pregnant = 0 OR is_pregnant IS NULL)
						 AND @bmi >= 18.5
						 AND age >= 20
						 AND @cur_status = 'active'
						 AND days_since_starting_arvs > 364
						 AND last_viralload >= 0
						 AND last_viralload <= 400 
				         THEN @eligible_not_on_dc := 1
				         ELSE @eligible_not_on_dc := 0
			         END AS eligible_not_on_dc,
			         
		    	IF(appointment_today = 1 AND @eligible_not_on_dc = 1, 1, 0 ) as eligible_not_on_dc_and_appointment_today,
				IF(appointment_today = 0 AND @eligible_not_on_dc = 1, 1, 0 ) as eligible_not_on_dc_and_unscheduled_today,
				IF(appointment_today = 1 AND @eligible_and_on_dc = 1, 1, 0 ) as eligible_and_on_dc_and_appointment_today,
				IF(appointment_today = 0 AND @eligible_and_on_dc = 1, 1, 0 ) as eligible_and_on_dc_and_unscheduled_today,
				IF(@eligible_not_on_dc = 1 OR @eligible_and_on_dc = 1, 1, 0 ) as elligible_total,
					 
				 IF(days_since_last_vl <= 365
						 AND second_last_viralload IS NOT NULL
						 AND (months_since_tb_tx_start_date IS NULL
						 OR months_since_tb_tx_start_date >= 6)
						 AND on_dc_today != 1
						 AND (is_pregnant = 0 OR is_pregnant IS NULL)
						 AND @bmi >= 18.5
						 AND age >= 20
						 AND @cur_status = 'active'
						 AND days_since_starting_arvs > 364
						 AND last_viralload >= 0
						 AND last_viralload < 400,
					 1,
					 0) AS dc_eligible_cumulative,
					 
				 IF(due_for_vl_today = 1 AND @cur_status = 'active'
						 AND appointment_today = 1,
					 1,
					 0) AS scheduled_today_and_due_for_vl,
					 
				 IF(due_for_vl_today = 1
						 AND @cur_status = 'active'
						 AND appointment_today = 0,
					 1,
					 0) AS unscheduled_today_and_due_for_vl,
					 
				 IF(due_for_vl_today = 1
						 AND vl_ordered_today = 1
						 AND @cur_status = 'active',
					 1,
					 0) AS due_for_vl_today_has_vl_order,
					 
				 IF(due_for_vl_today = 1
						 AND vl_ordered_today = 0
						 AND @cur_status = 'active',
					 1,
					 0) AS due_for_vl_today_dont_have_order,
					 
				 IF(due_for_vl_today = 1 AND @cur_status = 'active',
					 1,
					 0) AS due_for_vl_today_active,
					 
				 IF(due_for_vl_today = 1
						 AND @cur_status = 'active',
					 1,
					 0) AS overdue_for_vl_active,
					 
				 IF(is_suppressed = 1
						 AND @cur_status = 'active',
					 1,
					 0) AS is_suppressed_active,
					 
				  IF(is_unsuppressed = 1
						 AND @cur_status = 'active',
					 1,
					 0) AS is_unsuppressed_active,
					 
					 
					 
				 CASE
			         WHEN
			             (@prev_id != @cur_id
			                 OR @was_active_october_18 IS NULL)
			                 AND _date >= '2018-10-01'
			                 AND _date < '2019-05-11'
			                 AND @cur_status = 'active'
			         THEN
			             @was_active_october_18:=1
			         WHEN @prev_id != @cur_id THEN @was_active_october_18:=NULL
			         ELSE @was_active_october_18
			     END AS was_active_october_18,
			     
			     CASE
			         WHEN
			             (@prev_id != @cur_id
			                 OR @ever_active_after_october_18 IS NULL)
			                 AND _date >= '2018-10-01'
			                 AND @cur_status = 'active'
			         THEN
			             @ever_active_after_october_18:=1
			         WHEN @prev_id != @cur_id THEN @ever_active_after_october_18:=NULL
			         ELSE @ever_active_after_october_18
			     END AS ever_active_after_october_18,
			     
			     
			     CASE
			         WHEN
			             (@prev_id != @cur_id
			                 OR @was_ltfu_may_19 IS NULL)
			                 AND _date = '2019-05-11'
			                 AND @cur_status = 'ltfu'
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
			                 AND _date <= '2019-05-11'
			         THEN
			             @is_ltfu_surge_baseline:=1
			         WHEN
			             (@prev_id != @cur_id
			                 OR @is_ltfu_surge_baseline IS NULL)
			                 AND NOT (@was_active_october_18 = NULL
			                 AND @was_ltfu_may_19 = NULL)
			                 AND _date <= '2019-05-11'
			         THEN
			             @is_ltfu_surge_baseline:=0
			         WHEN @prev_id != @cur_id THEN @is_ltfu_surge_baseline:=NULL
			         ELSE @is_ltfu_surge_baseline
			     END AS is_ltfu_surge_baseline,
					
			     
			     CASE
			      WHEN
			             (@prev_id != @cur_id
			                 OR @active_after_may_19 IS NULL)
			                 AND _date > '2019-05-11'
			                 AND @cur_status = 'active'
			         THEN
			             @active_after_may_19:=1
			         WHEN @prev_id != @cur_id THEN @active_after_may_19:=NULL
			         ELSE @active_after_may_19
			     END AS active_after_may_19,
			     
			    CASE
			         WHEN
			             (@prev_id != @cur_id
			                 OR @is_ltfu_after_may_revised IS NULL)
			                 AND @active_after_may_19 = 1
			                 AND @cur_status = 'ltfu'
			                 AND _date > '2019-05-11'
			         THEN
			             @is_ltfu_after_may_revised:=1
			         WHEN
			             (@prev_id != @cur_id
			                 OR @is_ltfu_after_may_revised IS NULL)
			                 AND NOT (@active_after_may_19 = NULL)
			                 AND _date > '2019-05-11'
			         THEN
			             @is_ltfu_after_may_revised:=0
			         WHEN @prev_id != @cur_id THEN @is_ltfu_after_may_revised:=NULL
			         ELSE @is_ltfu_after_may_revised
			     END AS is_ltfu_after_may_revised,
			     
			     
			     IF(@is_ltfu_after_may_revised = 1 AND @cur_status = 'ltfu',
			         1,
			         0) AS is_ltfu_after_may,
			         
			         
	        IF((@is_ltfu_surge_baseline = 1 AND @cur_status = 'ltfu') OR (@is_ltfu_after_may_revised = 1 AND @cur_status = 'ltfu'), 1, 0) AS surge_ltfu_and_ltfu_after_may,
	        
	        IF((@is_ltfu_surge_baseline = 1 OR (@is_ltfu_after_may_revised = 1 AND @prev_status = 'ltfu'))
             AND @prev_status = 'ltfu'
                 AND @cur_status = 'dead',
		         1,
		         0) AS ltfu_death_today,
		         
	     IF((@is_ltfu_surge_baseline = 1 OR (@is_ltfu_after_may_revised = 1 AND @prev_status = 'ltfu'))
	     		AND @prev_status = 'ltfu'
	             AND @cur_status = 'transfer_out',
	         1,
	         0) AS ltfu_transfer_out_today,
	         
	         
	     IF((@is_ltfu_surge_baseline = 1 OR (@is_ltfu_after_may_revised = 1 AND @prev_status = 'ltfu'))
	    		 AND @prev_status = 'ltfu'
	             AND (@cur_status = 'active'
	             OR @cur_status = 'transfer_in'),
	         1,
	         0) AS ltfu_active_today,
	         
	     IF((@is_ltfu_surge_baseline = 1 OR (@is_ltfu_after_may_revised = 1 AND @prev_status = 'ltfu'))
	    		 AND @prev_status = 'ltfu'
	             AND (@cur_status = 'active'
	             OR @cur_status = 'transfer_in'
	             OR @cur_status = 'transfer_out'
	             OR @cur_status = 'dead'),
	         1,
	         0) AS ltfu_outcome_total_today,
	         
         IF((@prev_status = 'ltfu' OR @prev_status = 'active' OR @prev_status = 'transfer_out' OR @prev_status = 'transfer_in') AND @cur_status = 'dead', 1, 0) AS dead_today,
         
         IF(@prev_status = 'defaulter' AND @cur_status = 'ltfu',
         1,
         0) AS newly_ltfu_today,
         
         IF(days_since_rtc_date > 4   and days_since_rtc_date <= 28,
         1,
         0) AS defaulters_today	     
				
			 FROM
				surge_daily_report_dataset_0 
				 );

    -- END HERE 										  
replace into surge_daily_report_dataset								  
			(select 
*
from surge_daily_report_dataset_2);
                
                
				SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join surge_daily_report_dataset_temporary_build_queue t2 using (person_id);'); 
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
insert into flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
end if;
SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');

END$$
DELIMITER ;
