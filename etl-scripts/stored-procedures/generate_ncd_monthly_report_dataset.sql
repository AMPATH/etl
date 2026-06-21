CREATE DEFINER=`hkorir`@`%` PROCEDURE `generate_ncd_monthly_report_dataset`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int, IN start_date varchar(50))
BEGIN

	set @start = now();
	set @table_version = "ncd_monthly_report_dataset_v1.0";
	set @last_date_created = (select max(date_created) from etl.flat_ncd);

	set @sep = " ## ";
	set @lab_encounter_type = 99999;
	set @death_encounter_type = 31;
			
	CREATE TABLE IF NOT EXISTS ncd_monthly_report_dataset (
		`date_created` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
		`elastic_id` BIGINT,
		`endDate` DATE,
		`encounter_id` INT,
		`person_id` INT,
		`person_uuid` VARCHAR(100),
		`birthdate` DATE,
		`age` DOUBLE,
		`gender` VARCHAR(1),
		`location_id` INT,
		`location_uuid` VARCHAR(100),
		`clinic` VARCHAR(250),
		`encounter_datetime` DATETIME,
		`visit_this_month` TINYINT,
		`is_hypertensive` TINYINT,
		`htn_state` TINYINT,
		`is_diabetic` TINYINT,
		`dm_state` TINYINT,
		`has_mhd` TINYINT,
		`is_depressive_mhd` TINYINT,
		`is_anxiety_mhd` TINYINT,
		`is_bipolar_and_related_mhd` TINYINT,
		`is_personality_mhd` TINYINT,
		`is_feeding_and_eating_mhd` TINYINT,
		`is_ocd_mhd` TINYINT,
		`has_kd` TINYINT,
		`is_ckd` TINYINT,
		`ckd_stage` INT,
		`has_cvd` TINYINT,
		`is_heart_failure_cvd` TINYINT,
		`is_myocardinal_infarction` TINYINT,
		`has_neurological_disorder` TINYINT,
		`has_stroke` TINYINT,
		`is_stroke_haemorrhagic` TINYINT,
		`is_stroke_ischaemic` TINYINT,
		`has_migraine` TINYINT,
		`has_seizure` TINYINT,
		`has_epilepsy` TINYINT,
		`has_convulsive_disorder` TINYINT,
		`has_rheumatologic_disorder` TINYINT,
		`has_arthritis` TINYINT,
		`has_SLE` TINYINT,
		PRIMARY KEY elastic_id (elastic_id),
		INDEX person_enc_date (person_id , encounter_datetime),
		INDEX person_report_date (person_id , endDate),
		INDEX endDate_location_id (endDate , location_id),
		INDEX date_created (date_created),
		INDEX status_change (location_id , endDate)
	);

	if (query_type = "build") then
		select "BUILDING.......................";
		set @queue_table = concat("ncd_monthly_report_dataset_build_queue_",queue_number);                    

		SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,'(person_id int primary key) (select * from ncd_monthly_report_dataset_build_queue limit ', queue_size, ');'); 
		PREPARE s1 from @dyn_sql; 
		EXECUTE s1; 
		DEALLOCATE PREPARE s1;
		
		SET @dyn_sql=CONCAT('delete t1 from ncd_monthly_report_dataset_build_queue t1 join ',@queue_table, ' t2 using (person_id)'); 
		PREPARE s1 from @dyn_sql; 
		EXECUTE s1; 
		DEALLOCATE PREPARE s1;  
	end if;

		
	if (query_type = "sync") then
		set @queue_table = "ncd_monthly_report_dataset_sync_queue";
		CREATE TABLE IF NOT EXISTS ncd_monthly_report_dataset_sync_queue (
			person_id INT PRIMARY KEY
		);
				
		SELECT @last_update:=(SELECT MAX(date_updated) FROM etl.flat_log WHERE table_name = @table_version);
        SELECT @last_update := IF(@last_update,@last_update,'1900-01-01');

		replace into ncd_monthly_report_dataset_sync_queue
		(select distinct person_id from flat_ncd where date_created >= @last_update);
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
	
	
	SET @dyn_sql=CONCAT('delete t1 from ncd_monthly_report_dataset t1 join ',@queue_table,' t2 using (person_id);'); 
	PREPARE s1 from @dyn_sql; 
	EXECUTE s1; 
	DEALLOCATE PREPARE s1;  
	
	set @total_time=0;
	set @cycle_number = 0;
				
	while @person_ids_count > 0 do
	
		set @loop_start_time = now();                        

		drop temporary table if exists ncd_monthly_report_dataset_build_queue__0;
		create temporary table ncd_monthly_report_dataset_build_queue__0 (person_id int primary key);                

		SET @dyn_sql=CONCAT('insert into ncd_monthly_report_dataset_build_queue__0 (select * from ',@queue_table,' limit ',cycle_size,');'); 
		PREPARE s1 from @dyn_sql; 
		EXECUTE s1; 
		DEALLOCATE PREPARE s1;
		
		
		set @age =null;
		set @status = null;
		drop temporary table if exists ncd_monthly_report_dataset_0;
		create temporary table ncd_monthly_report_dataset_0
		(
			select 
				concat(date_format(endDate,"%Y%m"),person_id) as elastic_id,
				endDate,
				encounter_id,
				person_id,
				t3.uuid as person_uuid,
				date(birthdate) as birthdate,
				case
					when timestampdiff(year,birthdate,endDate) > 0 then @age := round(timestampdiff(year,birthdate,endDate),0)
					else @age :=round(timestampdiff(month,birthdate,endDate)/12,2)
				end as age,
				t3.gender,
				t2.location_id,
				t2.location_uuid,
				encounter_datetime, 
				
				if(encounter_datetime between date_format(endDate,"%Y-%m-01")  and endDate,1,0) as visit_this_month,
				
				encounter_type,
				
				case
					when htn_status = 7285 or htn_status = 7286 then 1
					when (comorbidities regexp '[[:<:]]903[[:>:]]') then 1
					when (prev_hbp_findings regexp '[[:<:]]1065[[:>:]]') then 1
					when (htn_meds is not null) then 1
					when (problems regexp '[[:<:]]903[[:>:]]') then 1
					when (review_of_med_history regexp '[[:<:]]903[[:>:]]') then 1
					else 0
				end as is_hypertensive,

				case
					when ((sbp < 130) and (dbp < 80)) then 1 
					when ((sbp >= 130) and (dbp >= 80)) then 2
					else 3
				end as htn_state,

				case
					when dm_status = 7281 or dm_status = 7282 then 1
					when (comorbidities regexp '[[:<:]]175[[:>:]]') then 1
					when (dm_meds is not null) then 1
					when (problems regexp '[[:<:]]9324|175[[:>:]]') then 1
					when (review_of_med_history regexp '[[:<:]]175[[:>:]]') then 1
					else 0
				end as is_diabetic,

				case
					when (hb_a1c <= 7) then 1 
					when (hb_a1c > 7) then 2
					else 3
				end as dm_state,

				case
					when (comorbidities regexp '[[:<:]]10860[[:>:]]') then 1
					when (indicated_mhd_tx is not null) then 1
					when (has_past_mhd_tx regexp '[[:<:]]1065[[:>:]]') then 1
					when (review_of_med_history regexp '[[:<:]]77|207[[:>:]]') then 1
					when (eligible_for_depression_care regexp '[[:<:]]1065[[:>:]]') then 1
					when (mood_disorder is not null) AND (mood_disorder not regexp '[[:<:]]1115[[:>:]]') then 1
					when (anxiety_condition is not null) AND (anxiety_condition not regexp '[[:<:]]1115[[:>:]]')then 1
					when (psychiatric_exam_findings is not null) AND (psychiatric_exam_findings not regexp '[[:<:]]1115[[:>:]]') then 1
					else 0
				end as has_mhd,

				case
					when (eligible_for_depression_care regexp '[[:<:]]1065[[:>:]]') then 1
					when (mood_disorder regexp '[[:<:]]11278[[:>:]]') then 1
					when (indicated_mhd_tx regexp '[[:<:]]207[[:>:]]') then 1
					when (review_of_med_history regexp '[[:<:]]207[[:>:]]') then 1
					when (psychiatric_exam_findings regexp '[[:<:]]207[[:>:]]') then 1
					else 0
				end as is_depressive_mhd,

				case
					when (anxiety_condition is not null) AND (anxiety_condition not regexp '[[:<:]]1115[[:>:]]') then 1
					when (indicated_mhd_tx regexp '[[:<:]]1443[[:>:]]') then 1
					when (review_of_med_history regexp '[[:<:]]207[[:>:]]') then 1
					when (psychiatric_exam_findings regexp '[[:<:]]1443[[:>:]]') then 1
					else 0
				end as is_anxiety_mhd,

				case
					when (mood_disorder regexp '[[:<:]]7763[[:>:]]') then 1
					when (indicated_mhd_tx regexp '[[:<:]]7763[[:>:]]') then 1
					else 0
				end as is_bipolar_and_related_mhd,

				case
					when (mood_disorder regexp '[[:<:]]7763[[:>:]]') then 1
					when (indicated_mhd_tx regexp '[[:<:]]11281[[:>:]]') then 1
					when (problems regexp '[[:<:]]467[[:>:]]') then 1
					else 0
				end as is_personality_mhd,

				null as is_feeding_and_eating_mhd,

				null as is_ocd_mhd,

				case
					when (comorbidities regexp '[[:<:]]77[[:>:]]') then 1
					when (kidney_disease regexp '[[:<:]]1065[[:>:]]') then 1
					when (problems regexp '[[:<:]]8078|11684[[:>:]]') then 1
					when (review_of_med_history regexp '[[:<:]]6033|8078[[:>:]]') then 1
					else 0
				end as has_kd,

				case
					when (problems regexp '[[:<:]]8078[[:>:]]') then 1
					when (review_of_med_history regexp '[[:<:]]8078[[:>:]]') then 1
					when (ckd_staging is not null) then 1
					else 0
				end as is_ckd,

				ckd_staging as ckd_stage,

				case
					when (cardiovascular_disorder is not null) AND (cardiovascular_disorder not regexp '[[:<:]]1115[[:>:]]') then 1
					when (comorbidities regexp '[[:<:]]7971[[:>:]]') then 1
					when (review_of_med_history regexp '[[:<:]]7971|6237[[:>:]]') then 1
					else 0
				end as has_cvd,

				case
					when (cardiovascular_disorder regexp '[[:<:]]1456[[:>:]]') then 1
					when (indicated_mhd_tx regexp '[[:<:]]1456[[:>:]]') then 1
					when (review_of_med_history regexp '[[:<:]]7971[[:>:]]') then 1
					else 0
				end as is_heart_failure_cvd,

				case
					when (cardiovascular_disorder regexp '[[:<:]]1535[[:>:]]') then 1
					else 0
				end as is_myocardinal_infarction,

				case
					when (neurological_disease is not null) AND (neurological_disease not regexp '[[:<:]]1115[[:>:]]') then 1
					when (indicated_mhd_tx regexp '[[:<:]]1456[[:>:]]') then 1
					when (review_of_med_history regexp '[[:<:]]7971[[:>:]]') then 1
					else 0
				end as has_neurological_disorder,

				case
					when (cardiovascular_disorder regexp '[[:<:]]1878[[:>:]]') AND (cardiovascular_disorder not regexp '[[:<:]]1115[[:>:]]') then 1
					when (indicated_mhd_tx regexp '[[:<:]]1456[[:>:]]') then 1
					when (review_of_med_history regexp '[[:<:]]7971[[:>:]]') then 1
					else 0
				end as has_stroke,

				null as is_stroke_haemorrhagic,

				null as is_stroke_ischaemic,

				case
					when (problems regexp '[[:<:]]1477[[:>:]]') then 1
					when (neurological_disease regexp '[[:<:]]1477[[:>:]]') then 1
					else 0
				end as has_migraine,

				case
					when (problems regexp '[[:<:]]206[[:>:]]') then 1
					when (neurological_disease regexp '[[:<:]]206[[:>:]]') then 1
					when (convulsive_disorder regexp '[[:<:]]206[[:>:]]') then 1
					else 0
				end as has_seizure,

				case
					when (problems regexp '[[:<:]]155|11687[[:>:]]') then 1
					when (neurological_disease regexp '[[:<:]]155[[:>:]]') then 1
					when (convulsive_disorder regexp '[[:<:]]155[[:>:]]') then 1
					when (indicated_mhd_tx regexp '[[:<:]]155[[:>:]]') then 1
					else 0
				end as has_epilepsy,

				case
					when (neurological_disease regexp '[[:<:]]10806[[:>:]]') then 1
					when (convulsive_disorder regexp '[[:<:]]155|10806[[:>:]]') then 1
					else 0
				end as has_convulsive_disorder,

				case
					when (rheumatologic_disorder is not null) AND (rheumatologic_disorder not regexp '[[:<:]]1115[[:>:]]') then 1
					when (comorbidities regexp '[[:<:]]12293[[:>:]]') then 1
					else 0
				end as has_rheumatologic_disorder,

				case
					when (rheumatologic_disorder regexp '[[:<:]]116[[:>:]]') then 1
					else 0
				end as has_arthritis,

				case
					when (rheumatologic_disorder regexp '[[:<:]]12292[[:>:]]') then 1
					else 0
				end as has_SLE

			from etl.dates t1
			join etl.flat_ncd t2 
			join amrs.person t3 using (person_id)
			join etl.ncd_monthly_report_dataset_build_queue__0 t5 using (person_id)
			
			where  
				t2.encounter_datetime < date_add(endDate, interval 1 day)
				-- and (t2.next_clinical_datetime_ncd is null or t2.next_clinical_datetime_ncd >= date_add(t1.endDate, interval 1 day) )
				-- and t2.is_clinical_encounter=1 
				and t1.endDate between start_date and date_add(now(),interval 2 year)
			order by person_id, endDate, sbp desc, dbp desc, hb_a1c desc
		);


		set @prev_id = null;
		set @cur_id = null;
		
		set @is_hypertensive = null;
		set @has_mhd = null;
		set @is_depressive_mhd = null;
		set @is_anxiety_mhd = null;
		set @is_bipolar_and_related_mhd = null;
		set @is_personality_mhd = null;
		set @is_feeding_and_eating_mhd = null;
		set @is_ocd_mhd = null;
		set @has_kd = null;
        set @is_ckd = null;
        set @ckd_stage = null;
        set @has_cvd = null;
        set @is_heart_failure_cvd = null;
        set @is_myocardinal_infarction = null;
        set @has_neurological_disorder = null;
        set @has_stroke = null;
        set @is_stroke_haemorrhagic = null;
        set @is_stroke_ischaemic = null;
        set @has_migraine = null;
        set @has_seizure = null;
        set @has_epilepsy = null;
        set @has_convulsive_disorder = null;
        set @has_rheumatologic_disorder = null;
        set @has_arthritis = null;
        set @has_SLE = null;

		drop temporary table if exists ncd_monthly_report_dataset_1;
		create temporary table ncd_monthly_report_dataset_1
		(
			select
				@prev_id := @cur_id as prev_id,
				@cur_id := person_id as cur_id,
				elastic_id,
				endDate,
				encounter_id,
				person_id,
				person_uuid,
				birthdate,
				age,
				gender,
				location_id,
				location_uuid,
				encounter_datetime, 
				
				visit_this_month,
				
				encounter_type,
				case
				    when (@prev_id = @cur_id AND (is_hypertensive = 1 OR @is_hypertensive = 1)) then @is_hypertensive := 1
					else @is_hypertensive := is_hypertensive
				end as is_hypertensive,
				htn_state,
				case
					when (@prev_id = @cur_id AND (is_diabetic = 1 OR @is_diabetic = 1)) then @is_diabetic := 1
					else @is_diabetic := is_diabetic
				end as is_diabetic,
				dm_state,
				case
					when (@prev_id = @cur_id AND (has_mhd = 1 OR @has_mhd = 1)) then @has_mhd := 1
					else @has_mhd := has_mhd
				end as has_mhd,
				case
					when (@prev_id = @cur_id AND (is_depressive_mhd = 1 OR @is_depressive_mhd = 1)) then @is_depressive_mhd := 1
					else @is_depressive_mhd := is_depressive_mhd
				end as is_depressive_mhd,
				case
					when (@prev_id = @cur_id AND (is_anxiety_mhd = 1 OR @is_anxiety_mhd = 1)) then @is_anxiety_mhd := 1
					else @is_anxiety_mhd := is_anxiety_mhd
				end as is_anxiety_mhd,

				case
					when (@prev_id = @cur_id AND (is_bipolar_and_related_mhd = 1 OR @is_bipolar_and_related_mhd = 1)) then @is_bipolar_and_related_mhd := 1
					else @is_bipolar_and_related_mhd := is_bipolar_and_related_mhd
				end as is_bipolar_and_related_mhd,

				case
					when (@prev_id = @cur_id AND (is_personality_mhd = 1 OR @is_personality_mhd = 1)) then @is_personality_mhd := 1
					else @is_personality_mhd := is_personality_mhd
				end as is_personality_mhd,

				null as is_feeding_and_eating_mhd,

				null as is_ocd_mhd,

				case
					when (@prev_id = @cur_id AND (has_kd = 1 OR @has_kd = 1)) then @has_kd := 1
					else @has_kd := has_kd
				end as has_kd,

				case
					when (@prev_id = @cur_id AND (is_ckd = 1 OR @is_ckd = 1)) then @is_ckd := 1
					else @is_ckd := is_ckd
				end as is_ckd,

				case	
					when (@prev_id = @cur_id AND (@ckd_stage is null) AND (ckd_stage is not null)) then @ckd_stage := ckd_stage
					when (@prev_id = @cur_id AND (@ckd_stage is not null) AND (ckd_stage is  null)) then @ckd_stage
					else @ckd_stage := ckd_stage
				end as ckd_stage,

				case
					when (@prev_id = @cur_id AND (has_cvd = 1 OR @has_cvd = 1)) then @has_cvd := 1
					else @has_cvd := has_cvd
				end as has_cvd,

				case
					when (@prev_id = @cur_id AND (is_heart_failure_cvd = 1 OR @is_heart_failure_cvd = 1)) then @is_heart_failure_cvd := 1
					else @is_heart_failure_cvd := is_heart_failure_cvd
				end as is_heart_failure_cvd,

				case
					when (@prev_id = @cur_id AND (is_myocardinal_infarction = 1 OR @is_myocardinal_infarction = 1)) then @is_myocardinal_infarction := 1
					else @is_myocardinal_infarction := is_myocardinal_infarction
				end as is_myocardinal_infarction,

				case
					when (@prev_id = @cur_id AND (has_neurological_disorder = 1 OR @has_neurological_disorder = 1)) then @has_neurological_disorder := 1
					else @has_neurological_disorder := has_neurological_disorder
				end as has_neurological_disorder,

				case
					when (@prev_id = @cur_id AND (has_stroke = 1 OR @has_stroke = 1)) then @has_stroke := 1
					else @has_stroke := has_stroke
				end as has_stroke,

				is_stroke_haemorrhagic,

				is_stroke_ischaemic,

				case
					when (@prev_id = @cur_id AND (has_migraine = 1 OR @has_migraine = 1)) then @has_migraine := 1
					else @has_migraine := has_migraine
				end as has_migraine,

				case
					when (@prev_id = @cur_id AND (has_seizure = 1 OR @has_seizure = 1)) then @has_seizure := 1
					else @has_seizure := has_seizure
				end as has_seizure,

				case
					when (@prev_id = @cur_id AND (has_epilepsy = 1 OR @has_epilepsy = 1)) then @has_epilepsy := 1
					else @has_epilepsy := has_epilepsy
				end as has_epilepsy,

				case
					when (@prev_id = @cur_id AND (has_convulsive_disorder = 1 OR @has_convulsive_disorder = 1)) then @has_convulsive_disorder := 1
					else @has_convulsive_disorder := has_convulsive_disorder
				end as has_convulsive_disorder,

				case
					when (@prev_id = @cur_id AND (has_rheumatologic_disorder = 1 OR @has_rheumatologic_disorder = 1)) then @has_rheumatologic_disorder := 1
					else @has_rheumatologic_disorder := has_rheumatologic_disorder
				end as has_rheumatologic_disorder,

				case
					when (@prev_id = @cur_id AND (has_arthritis = 1 OR @has_arthritis = 1)) then @has_arthritis := 1
					else @has_arthritis := has_arthritis
				end as has_arthritis,

				case
					when (@prev_id = @cur_id AND (has_SLE = 1 OR @has_SLE = 1)) then @has_SLE := 1
					else @has_SLE := has_SLE
				end as has_SLE

				from ncd_monthly_report_dataset_0
				order by person_id, encounter_datetime, endDate
		);

		alter table ncd_monthly_report_dataset_1 drop prev_id, drop cur_id;

		set @prev_id = null;
		set @cur_id = null;
		set @prev_location_id = null;
		set @cur_location_id = null;
		drop temporary table if exists ncd_monthly_report_dataset_2;
		create temporary table ncd_monthly_report_dataset_2
		(select
			*,
			@prev_id := @cur_id as prev_id,
			@cur_id := person_id as cur_id,

			case
				when @prev_id=@cur_id then @prev_location_id := @cur_location_id
				else @prev_location_id := null
			end as next_location_id,
			
			@cur_location_id := location_id as cur_location_id

			from ncd_monthly_report_dataset_1
			order by person_id, endDate desc
		);

		alter table ncd_monthly_report_dataset_2 drop prev_id, drop cur_id, drop cur_location_id;

		set @prev_id = null;
		set @cur_id = null;
		set @cur_location_id = null;
		set @prev_location_id = null;
		drop temporary table if exists ncd_monthly_report_dataset_3;
		create temporary table ncd_monthly_report_dataset_3
		(select
			*,
			@prev_id := @cur_id as prev_id,
			@cur_id := person_id as cur_id,
			
			case
				when @prev_id=@cur_id then @prev_location_id := @cur_location_id
				else @prev_location_id := null
			end as prev_location_id,
			
			@cur_location_id := location_id as cur_location_id
				
			from ncd_monthly_report_dataset_2
			order by person_id, endDate
		);
								
		SELECT NOW();
		SELECT COUNT(*) AS num_rows_to_be_inserted FROM ncd_monthly_report_dataset_3;
	
				#add data to table									  
				replace into ncd_monthly_report_dataset											  
				(select
					null,
				elastic_id,
				endDate,
				encounter_id,
				person_id,
				person_uuid,
				birthdate,
				age,
				gender,
				location_id,
				location_uuid,
				t2.name as clinic,
				encounter_datetime,
				visit_this_month,

				is_hypertensive,
				htn_state,

				is_diabetic,
				dm_state,

				has_mhd,
				is_depressive_mhd,
				is_anxiety_mhd,
				is_bipolar_and_related_mhd,
				is_personality_mhd,
				is_feeding_and_eating_mhd,
				is_ocd_mhd,
				
				has_kd,
				is_ckd,
				ckd_stage,

				has_cvd,
				is_heart_failure_cvd,
				is_myocardinal_infarction,

				has_neurological_disorder,
				has_stroke,
				is_stroke_haemorrhagic,
				is_stroke_ischaemic,

				has_migraine,
				has_seizure,
				has_epilepsy,
				has_convulsive_disorder,

				has_rheumatologic_disorder,
				has_arthritis,
				has_SLE

					from ncd_monthly_report_dataset_3 t1
					join amrs.location t2 using (location_id)
				);
				

				SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ncd_monthly_report_dataset_build_queue__0 t2 using (person_id);'); 
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
				set @num_in_nmrd := (select count(*) from ncd_monthly_report_dataset);
			
            SELECT 
				@num_in_nmrd AS num_in_nmrd,
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
	
	#log the operation for next starting point
	insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
	
	SELECT CONCAT(@table_version, ' : Time to complete: ',TIMESTAMPDIFF(MINUTE, @start, @end),' minutes');

END