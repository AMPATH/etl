DELIMITER $$
CREATE PROCEDURE `generate_prep_monthly_report_v1_prod`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int , IN log boolean)
BEGIN
	select @start := now();
	select @table_version := "prep_monthly_report_dataset";
	set @primary_table := "prep_monthly_report_dataset";
	set @query_type = query_type;
	
	set @total_rows_written = 0;
	
	set session sort_buffer_size=512000000;

	select @sep := " ## ";
	select @last_date_created := (select max(max_date_created) from etl.flat_obs);
	
	CREATE TABLE IF NOT EXISTS `prep_monthly_report_dataset` (
		`elastic_id` varchar(21) NOT NULL DEFAULT '',
		`location_id` bigint(60) DEFAULT NULL,
		`person_id` int(11) DEFAULT NULL,
		`person_uuid` char(38) CHARACTER SET utf8 NOT NULL,
		`birthdate` date DEFAULT NULL,
		`death_date` longtext CHARACTER SET utf8,
		`age` decimal(23,2) DEFAULT NULL,
		`gender` varchar(50) CHARACTER SET utf8 DEFAULT '',
		`encounter_id` int(11) NOT NULL DEFAULT '0',
		`encounter_datetime` datetime DEFAULT NULL,
		`encounter_month` int(6) DEFAULT NULL,
		`endDate` date DEFAULT NULL,
		`prev_rtc_date` longtext,
		`prev_rtc_month` int(6) DEFAULT NULL,
		`rtc_date` varchar(10) CHARACTER SET utf8 DEFAULT NULL,
		`rtc_month` int(6) DEFAULT NULL,
		`cur_prep_meds_names` text,
		`first_prep_regimen` longtext,
		`prep_start_date` varbinary(10) DEFAULT NULL,
		`is_breastfeeding` INT DEFAULT NULL,
		`is_pregnant` INT DEFAULT NULL,
		`population_type` INT  DEFAULT NULL,
		`key_population_type` INT DEFAULT NULL,
		`visit_this_month` int(3) DEFAULT NULL,
		`appointment_this_month` int(3) DEFAULT NULL,
		`scheduled_visit_this_month` int(1) NOT NULL DEFAULT '0',
		`early_appointment_this_month` int(1) NOT NULL DEFAULT '0',
		`late_appointment_this_month` int(1) NOT NULL DEFAULT '0',
		`missed_appointment_this_month` int(1) NOT NULL DEFAULT '0',
		`days_since_rtc_date` varchar(23) CHARACTER SET utf8 DEFAULT NULL,
		`status` varchar(12) CHARACTER SET utf8 DEFAULT NULL,
		`active_on_prep_this_month` int(1) NOT NULL DEFAULT '0',
		`prep_defaulter_this_month` int(1) NOT NULL DEFAULT '0',
        `cumulative_prep_ltfu_this_month` int(1) NOT NULL DEFAULT '0',
		`prep_ltfu_this_month` int(1) NOT NULL DEFAULT '0',
		`prep_discontinued_this_month` int(1) NOT NULL DEFAULT '0',
        `cumulative_prep_discontinued_this_month` int(1) NOT NULL DEFAULT '0',
		`enrolled_in_prep_this_month` int(1) NOT NULL DEFAULT '0',
		`discontinued_from_prep_this_month` int(1) NOT NULL DEFAULT '0',
		`turned_positive_this_month` int(1) NOT NULL DEFAULT '0',
        `cumulative_turned_positive_this_month` int(1) NOT NULL DEFAULT '0',
		`prev_on_prep_and_turned_positive` int(1) NOT NULL DEFAULT '0',
		PRIMARY KEY (`elastic_id`),
		KEY `person_id` (`person_id`),
		KEY `person_id_2` (`person_id`,`endDate`),
		KEY `endDate` (`endDate`),
		KEY `location_id` (`location_id`,`endDate`),
		KEY `encounter_datetime` (`encounter_datetime`)
	) ENGINE=InnoDB DEFAULT CHARSET=latin1;


	if(@query_type="build") then
			select 'BUILDING..........................................';
			set @write_table = concat("prep_monthly_report_temp_",queue_number);
			set @queue_table = concat("prep_monthly_report_build_queue_",queue_number);                                                                    

			SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1;  

			SET @dyn_sql=CONCAT('drop table if exists ',@queue_table,';'); 
			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1; 
			
			SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select person_id from prep_monthly_report_build_queue limit ', queue_size, ');'); 
			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1;  
			
			SET @dyn_sql=CONCAT('delete t1 from prep_monthly_report_build_queue t1 join ',@queue_table, ' t2 on (t1.person_id = t2.person_id);'); 
			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1;  

	end if;

	
		
	select @queue_table;
	select @primary_table;
	select @write_table;
		
	# Remove test patients
	SET @dyn_sql=CONCAT('delete t1 FROM ',@queue_table,' t1
			join amrs.person_attribute t2 using (person_id)
			where t2.person_attribute_type_id=28 and value="true" and voided=0');
	PREPARE s1 from @dyn_sql; 
	EXECUTE s1; 
	DEALLOCATE PREPARE s1;
	
	SET @dyn_sql=CONCAT('select count(*) as queue_size from ',@queue_table); 
	PREPARE s1 from @dyn_sql; 
	EXECUTE s1; 
	DEALLOCATE PREPARE s1; 

	SET @person_ids_count = 0;
	SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
	PREPARE s1 from @dyn_sql; 
	EXECUTE s1; 
	DEALLOCATE PREPARE s1;  

	select @person_ids_count as 'num patients to sync';
	
	
	SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
	PREPARE s1 from @dyn_sql; 
	EXECUTE s1; 
	DEALLOCATE PREPARE s1;  

	set @total_time=0;
	set @cycle_number = 0;
	while @person_ids_count > 0 do
	
		set @loop_start_time = now();
		
		drop temporary table if exists prep_monthly_report_temp_queue;
		SET @dyn_sql=CONCAT('create temporary table prep_monthly_report_temp_queue (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
		PREPARE s1 from @dyn_sql; 
		EXECUTE s1; 
		DEALLOCATE PREPARE s1;
		
		drop temporary table if exists prep_patients_temp_queue;
		create temporary table prep_patients_temp_queue (person_id int primary key) 
		(
			select distinct q.person_id from prep_monthly_report_temp_queue q
			inner join etl.flat_obs t0 using (person_id)
			where t0.encounter_type in (133,134)
		);
		
		drop temporary table if exists prep_summary_in_queue;
		create temporary table prep_summary_in_queue               
		(index (person_id), index(person_id, encounter_datetime),  index(encounter_id), index(encounter_datetime), index(rtc_date))
		(select * 
			from 
			etl.flat_prep_summary_v1_1
			where
			encounter_datetime >= '2018-01-01'
			AND is_prep_clinical_encounter = 1
			order by person_id, encounter_datetime
		);
		
		drop temporary table if exists prep_patient_encounters;
		create temporary table prep_patient_encounters               
		(index (person_id), index(person_id, endDate, encounter_id))
		( 
			select * from (select 
				*
				from 
				etl.dates m
				join
				prep_summary_in_queue h
				WHERE
			h.encounter_datetime < DATE_ADD(endDate, INTERVAL 1 DAY)
		ORDER BY h.person_id , month(endDate), h.encounter_datetime desc , rtc_date
		) p group by person_id, month(endDate));
		
		
		drop temporary table if exists stage_1;
		create temporary table stage_1               
		( 
			primary key elastic_id (elastic_id),
			index (person_id),  
			index (person_id, endDate), 
			index(endDate), 
			index(location_id, endDate), 
			index(encounter_datetime))
		(SELECT
			concat(month(endDate), person_id) as elastic_id,
			location_id,
			person_id, 
			uuid AS person_uuid,
			DATE(birthdate) AS birthdate,
			death_date,
			CASE
				WHEN
					TIMESTAMPDIFF(YEAR, birthdate, endDate) > 0
				THEN
					@age:=ROUND(TIMESTAMPDIFF(YEAR, birthdate, endDate),
							0)
				ELSE @age:=ROUND(TIMESTAMPDIFF(MONTH,
							birthdate,
							endDate) / 12,
						2)
			END AS age,
			gender,
			encounter_id, 
			encounter_datetime,
			@encounter_month := month(encounter_datetime) as encounter_month,
			endDate,
			prev_rtc_date,
			@prev_rtc_month := month(prev_rtc_date) as prev_rtc_month,
			rtc_date,
			@rtc_month := month(rtc_date) as rtc_month,
			cur_prep_meds_names,
			first_prep_regimen,
			prep_start_date,
			is_breastfeeding,
			is_pregnant,
			population_type,
			key_population_type,
			
			CASE 
				WHEN  encounter_datetime between date_format(endDate,"%Y-%m-01")  and endDate THEN @visit_this_month := 1
				ELSE @visit_this_month := 0 
			END AS visit_this_month,
			
			CASE
				WHEN prev_rtc_date between date_format(endDate,"%Y-%m-01")  and endDate THEN @appointment_this_month := 1
				WHEN rtc_date between date_format(endDate,"%Y-%m-01")  and endDate THEN @appointment_this_month := 1
				ELSE @appointment_this_month := 0 
			END AS appointment_this_month,
			
			IF(@visit_this_month = 1 AND @appointment_this_month = 1, 1, 0)  AS scheduled_visit_this_month,
			
			IF(@visit_this_month = 1  AND @appointment_this_month <> 1 AND @encounter_month < @prev_rtc_month
				,1,0) AS early_appointment_this_month,
				
			IF(@visit_this_month = 1  AND @appointment_this_month <> 1 AND @encounter_month > @prev_rtc_month
				,1,0) AS late_appointment_this_month,
				
			IF(@visit_this_month = 0  AND @appointment_this_month = 1,1,0) AS missed_appointment_this_month,

			timestampdiff(day,rtc_date, endDate) as days_since_rtc_date,
				
			CASE
				WHEN 
					DATE(endDate) > DATE(death_date) 
				THEN @status:='dead'
				WHEN 
					discontinued_prep_date between date_format(endDate,"%Y-%m-01")  and endDate
					THEN 
					@status:='discontinued'
				WHEN
					timestampdiff(day,rtc_date, endDate) <= 0
				THEN
					@status:='active'
				WHEN
					timestampdiff(day,rtc_date, endDate) > 0
				THEN
					@status:='ltfu'
				ELSE @status:='unknown'
			END AS status,
			
			if( @status = 'active', 1, 0) as active_on_prep_this_month,
			null as prep_defaulter_this_month,
			if( @status = 'ltfu', 1, 0) as cumulative_prep_ltfu_this_month,
			if( @status = 'ltfu' AND rtc_date between date_format(endDate,"%Y-%m-01") and endDate , 1, 0) as prep_ltfu_this_month,
			if( @status = 'discontinued' and discontinued_prep_date between date_format(endDate,"%Y-%m-01") and endDate, 1, 0) as prep_discontinued_this_month,
			if( @status = 'discontinued', 1, 0) as cumulative_prep_discontinued_this_month,

			if(enrollment_date between date_format(endDate,"%Y-%m-01")  and endDate, 1, 0) as enrolled_in_prep_this_month,
			if(discontinued_prep_date between date_format(endDate,"%Y-%m-01")  and endDate, 1, 0) as discontinued_from_prep_this_month,
			if(turned_positive_date between date_format(endDate,"%Y-%m-01")  and endDate, 1, 0) as turned_positive_this_month,
            if(turned_positive_date is not null, 1, 0) as cumulative_turned_positive_this_month,
			if((@turned_positive_this_month = 1 and @status = 'discontinued'), 1, 0) as prev_on_prep_and_turned_positive
			
			from 
			prep_patient_encounters
			);
			
			replace into prep_monthly_report_dataset
		(select
			*
			from stage_1);
		


		SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join prep_monthly_report_temp_queue t2 using (person_id);'); 

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
			@person_ids_count AS 'persons remaining',
			@cycle_length AS 'Cycle time (s)',
			CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
			@remaining_time AS 'Est time remaining (min)';

	end while;

	select @end := now();
	insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
	select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");

END$$
DELIMITER ;