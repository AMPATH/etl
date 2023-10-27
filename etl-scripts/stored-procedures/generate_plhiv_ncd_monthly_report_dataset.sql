DELIMITER $$
CREATE  PROCEDURE `generate_plhiv_ncd_monthly_report_dataset`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int , IN log boolean)
BEGIN
	select @start := now();
	select @table_version := "plhiv_ncd_monthly_report_dataset";
	set @primary_table := "plhiv_ncd_monthly_report_dataset";
	set @query_type = query_type;
	
	set @total_rows_written = 0;
	
	set session sort_buffer_size=512000000;

	select @sep := " ## ";
	select @last_date_created := (select max(max_date_created) from etl.flat_obs);
	
	CREATE TABLE IF NOT EXISTS `plhiv_ncd_monthly_report_dataset` (
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
		`visit_this_month` int(3) DEFAULT NULL,
		`appointment_this_month` int(3) DEFAULT NULL,
		`scheduled_visit_this_month` int(1) NOT NULL DEFAULT '0',
		`early_appointment_this_month` int(1) NOT NULL DEFAULT '0',
		`late_appointment_this_month` int(1) NOT NULL DEFAULT '0',
		`missed_appointment_this_month` int(1) NOT NULL DEFAULT '0',
		`days_since_rtc_date` varchar(23) CHARACTER SET utf8 DEFAULT NULL,
		`has_comorbidity` TINYINT(1),
		`has_mental_disorder_comorbidity` TINYINT(1),
		`has_diabetic_comorbidity` TINYINT(1),
		`has_hypertension_comorbidity` TINYINT(1),
		`has_other_comorbidity` TINYINT(1),
		PRIMARY KEY (`elastic_id`),
		KEY `person_id` (`person_id`),
		KEY `person_id_2` (`person_id`,`endDate`),
		KEY `endDate` (`endDate`),
		KEY `location_id` (`location_id`,`endDate`),
		KEY `encounter_datetime` (`encounter_datetime`)
	) ENGINE=InnoDB DEFAULT CHARSET=latin1;


	if(@query_type="build") then
			select 'BUILDING..........................................';
			set @write_table = concat("plhiv_ncd_monthly_report_temp_",queue_number);
			set @queue_table = concat("plhiv_ncd_monthly_report_build_queue_",queue_number);                                                                    

			SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1;  

			SET @dyn_sql=CONCAT('drop table if exists ',@queue_table,';'); 
			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1; 
			
			SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select person_id from plhiv_ncd_monthly_report_build_queue limit ', queue_size, ');'); 
			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1;  
			
			SET @dyn_sql=CONCAT('delete t1 from plhiv_ncd_monthly_report_build_queue t1 join ',@queue_table, ' t2 on (t1.person_id = t2.person_id);'); 
			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1;  

	end if;

	# Display the important tables names
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
		
		drop temporary table if exists plhiv_ncd_monthly_report_temp_queue;
		SET @dyn_sql=CONCAT('create temporary table plhiv_ncd_monthly_report_temp_queue (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
		PREPARE s1 from @dyn_sql; 
		EXECUTE s1; 
		DEALLOCATE PREPARE s1;
		
		drop temporary table if exists plhiv_ncd_summary_in_queue;
		create temporary table plhiv_ncd_summary_in_queue               
		(index (person_id), index(person_id, encounter_datetime),  index(encounter_id), index(encounter_datetime), index(rtc_date))
		(select * 
			from 
			etl.plhiv_ncd_summary_v1
			where
			encounter_datetime >= '2018-01-01'
			order by person_id, encounter_datetime
		);
		
		drop table if exists plhiv_ncd_patient_encounters;
		create table plhiv_ncd_patient_encounters               
		(index (person_id), index(person_id, endDate, encounter_id))
		( 
			select 
				* 
			from (
				select 
					*
				from  etl.dates m
				join plhiv_ncd_summary_in_queue h
				WHERE 	h.encounter_datetime < DATE_ADD(endDate, INTERVAL 1 DAY)
						and m.endDate BETWEEN '2018-01-01' AND DATE_ADD(now(), INTERVAL 2 YEAR)
				ORDER BY h.person_id , month(endDate), h.encounter_datetime desc , rtc_date
		 	) p 
			group by person_id, month(endDate)
		 );
		
		
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
			concat(month(endDate), t1.person_id) as elastic_id,
			location_id,
			t1.person_id, 
			t1.uuid AS person_uuid,
			DATE(t1.birthdate) AS birthdate,
			t1.death_date,
			CASE
				WHEN
					TIMESTAMPDIFF(YEAR, t1.birthdate, endDate) > 0
				THEN
					@age:=ROUND(TIMESTAMPDIFF(YEAR, t1.birthdate, endDate),
							0)
				ELSE @age:=ROUND(TIMESTAMPDIFF(MONTH,
							t1.birthdate,
							endDate) / 12,
						2)
			END AS age,
			t1.gender,
			encounter_id, 
			encounter_datetime,
			@encounter_month := month(encounter_datetime) as encounter_month,
			endDate,
			prev_rtc_date,
			@prev_rtc_month := month(prev_rtc_date) as prev_rtc_month,
			rtc_date,
			@rtc_month := month(rtc_date) as rtc_month,
			
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
			t1.has_comorbidity,
			t1.has_mental_disorder_comorbidity,
			t1.has_diabetic_comorbidity,
			t1.has_hypertension_comorbidity,
			t1.has_other_comorbidity
			from 
			plhiv_ncd_patient_encounters t1
            inner join amrs.person t2 on (t1.person_id = t2.person_id and t2.voided = 0)
			);
			
		replace into plhiv_ncd_monthly_report_dataset
		(
			select
				*
			from stage_1
		);
		
		SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join plhiv_ncd_monthly_report_temp_queue t2 using (person_id);'); 
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