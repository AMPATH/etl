
DELIMITER $$

CREATE PROCEDURE `etl`.`generate_plhiv_ncd_summary`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
	set @primary_table := "plhiv_ncd_summary_v1";
	set @query_type = query_type;

	set @total_rows_written = 0;

	set @encounter_types = "(54,55,75,76,77,78,79,83,96,99,100,104,107,108,109,131,171,172)";
	set @clinical_encounter_types = "(54,55,75,76,77,78,79,83,96,104,107,108,109,171,172)";
	set @non_clinical_encounter_types = "(131)";
	set @other_encounter_types = "(-1)";

	set @start = now();
	set @table_version = "plhiv_ncd_summary_v1.0";

	set session sort_buffer_size=512000000;

	set @sep = " ## ";
	set @boundary = "!!";
	set @last_date_created = (select max(max_date_created) from etl.flat_obs);

	CREATE TABLE IF NOT EXISTS plhiv_ncd_summary_v1 (
		date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		person_id INT,
		uuid VARCHAR(100),
		visit_id INT,
		encounter_id INT,
		encounter_datetime DATETIME,
		encounter_type INT,
		is_clinical_encounter INT,
		location_id INT,
		location_uuid VARCHAR(100),
		death_date DATETIME,
		prev_rtc_date DATETIME,
		rtc_date DATETIME,
		lmp DATE,
		sbp SMALLINT,
		dbp SMALLINT,
		pulse SMALLINT,
		fbs DECIMAL,
		rbs DECIMAL,
		hb_a1c DECIMAL,
		hb_a1c_date DATETIME,
		creatinine DECIMAL,
		creatinine_date DATETIME,
		total_cholesterol DECIMAL,
		hdl DECIMAL,
		ldl DECIMAL,
		triglycerides DECIMAL,
		lipid_panel_date DATETIME,
		dm_status MEDIUMINT,
		htn_status MEDIUMINT,
		dm_meds VARCHAR(500),
		htn_meds VARCHAR(500),
		prescriptions TEXT,
		problems TEXT,
		has_comorbidity TINYINT(1),
		has_mental_disorder_comorbidity TINYINT(1),
		has_diabetic_comorbidity TINYINT(1),
		has_hypertension_comorbidity TINYINT(1),
		has_other_comorbidity TINYINT(1),
		prev_encounter_datetime_plhiv_ncd DATETIME,
		next_encounter_datetime_plhiv_ncd DATETIME,
		prev_encounter_type_plhiv_ncd MEDIUMINT,
		next_encounter_type_plhiv_ncd MEDIUMINT,
		prev_clinical_datetime_plhiv_ncd DATETIME,
		next_clinical_datetime_plhiv_ncd DATETIME,
		prev_clinical_location_id_plhiv_ncd MEDIUMINT,
		next_clinical_location_id_plhiv_ncd MEDIUMINT,
		prev_clinical_rtc_date_plhiv_ncd DATETIME,
		next_clinical_rtc_date_plhiv_ncd DATETIME,
		PRIMARY KEY encounter_id (encounter_id),
		INDEX person_date (person_id , encounter_datetime),
		INDEX person_uuid (uuid),
		INDEX location_enc_date (location_uuid , encounter_datetime),
		INDEX enc_date_location (encounter_datetime , location_uuid),
		INDEX location_id_rtc_date (location_id , rtc_date),
		INDEX location_uuid_rtc_date (location_uuid , rtc_date),
		INDEX loc_id_enc_date_next_clinical (location_id , encounter_datetime , next_clinical_datetime_plhiv_ncd),
		INDEX encounter_type (encounter_type),
		INDEX date_created (date_created)
	);

	if(@query_type="build") then
		select 'BUILDING..........................................';

		set @write_table = concat("plhiv_ncd_summary_temp_",queue_number);
		set @queue_table = concat("plhiv_ncd_summary_build_queue_",queue_number);

		select @queue_table;
		SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
		PREPARE s1 from @dyn_sql;
		EXECUTE s1;
		DEALLOCATE PREPARE s1;

		#create  table if not exists @queue_table (person_id int, primary key (person_id));
		SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from plhiv_ncd_summary_build_queue limit ', queue_size, ');');
		#SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from plhiv_ncd_summary_build_queue limit 500);');
		PREPARE s1 from @dyn_sql;
		EXECUTE s1;
		DEALLOCATE PREPARE s1;

		#delete t1 from plhiv_ncd_summary_build_queue t1 join @queue_table t2 using (person_id)
		SET @dyn_sql=CONCAT('delete t1 from plhiv_ncd_summary_build_queue t1 join ',@queue_table, ' t2 using (person_id);');
		PREPARE s1 from @dyn_sql;
		EXECUTE s1;
		DEALLOCATE PREPARE s1;
	end if;

	if(@query_type="sync") then
		select 'SYNCING..........................................';
		set @write_table = "plhiv_ncd_summary_v1";
		set @queue_table = "plhiv_ncd_summary_sync_queue";
		CREATE TABLE IF NOT EXISTS plhiv_ncd_summary_sync_queue (
			person_id INT PRIMARY KEY
		);

		set @last_update = null;

		SELECT
			MAX(date_updated)
		INTO @last_update FROM
			etl.flat_log
		WHERE
			table_name = @table_version;

		replace into plhiv_ncd_summary_sync_queue
		(select distinct patient_id
			from amrs.encounter
			where date_changed > @last_update
		);

		replace into plhiv_ncd_summary_sync_queue
		(select distinct person_id
			from etl.flat_obs
			where max_date_created > @last_update
		);

		replace into plhiv_ncd_summary_sync_queue
		(select distinct person_id
			from etl.flat_lab_obs
			where max_date_created > @last_update
		);

		replace into plhiv_ncd_summary_sync_queue
		(select distinct person_id
			from etl.flat_orders
			where max_date_created > @last_update
		);

		replace into plhiv_ncd_summary_sync_queue
		(select person_id from
			amrs.person
			where date_voided > @last_update);


		replace into plhiv_ncd_summary_sync_queue
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

	SELECT @person_ids_count AS 'num patients to update';


	#delete t1 from plhiv_ncd_summary_v1.0 t1 join @queue_table t2 using (person_id);
	SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);');
	PREPARE s1 from @dyn_sql;
	EXECUTE s1;
	DEALLOCATE PREPARE s1;

	-- drop temporary table if exists prescriptions;
	-- create temporary table prescriptions (encounter_id int primary key, prescriptions text)
	-- 	(
	-- 	select
	-- 		encounter_id,
	-- 		group_concat(obs separator ' $ ') as prescriptions
	-- 	from
	-- 	(
	-- 		select
	-- 			t2.encounter_id,
	-- 			obs_group_id,
	-- 			group_concat(
	-- 				case
	-- 					when value_coded is not null then concat(@boundary,o.concept_id,'=',value_coded,@boundary)
	-- 					when value_numeric is not null then concat(@boundary,o.concept_id,'=',value_numeric,@boundary)
	-- 					when value_datetime is not null then concat(@boundary,o.concept_id,'=',date(value_datetime),@boundary)
	-- 					when value_text is not null then concat(@boundary,o.concept_id,'=',value_text,@boundary)
	-- 					when value_drug is not null then concat(@boundary,o.concept_id,'=',value_drug,@boundary)
	-- 					when value_modifier is not null then concat(@boundary,o.concept_id,'=',value_modifier,@boundary)
	-- 				end
	-- 				order by o.concept_id,value_coded
	-- 				separator ' ## '
	-- 			) as obs

	-- 		from amrs.obs o
	-- 		join (select encounter_id, obs_id, concept_id as grouping_concept from amrs.obs where concept_id in (7307,7334)) t2 on o.obs_group_id = t2.obs_id
	-- 		group by obs_group_id
	-- 	) t
	-- 	group by encounter_id
	-- );

	set @total_time=0;
	set @cycle_number = 0;

	while @person_ids_count > 0 do

		set @loop_start_time = now();

		drop temporary table if exists plhiv_ncd_summary_build_queue__0;

		SET @dyn_sql=CONCAT('create temporary table plhiv_ncd_summary_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');');
		PREPARE s1 from @dyn_sql;
		EXECUTE s1;
		DEALLOCATE PREPARE s1;



		drop temporary table if exists plhiv_ncd_summary_0a;
		SET @dyn_sql = CONCAT(
				'create temporary table plhiv_ncd_summary_0a
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
				from etl.flat_obs t1
					join plhiv_ncd_summary_build_queue__0 t0 using (person_id)
					left join etl.flat_orders t2 using(encounter_id)
				where t1.encounter_type in ',@encounter_types,');'
		);

		PREPARE s1 from @dyn_sql;
		EXECUTE s1;
		DEALLOCATE PREPARE s1;

		insert into plhiv_ncd_summary_0a
		(
			select
				t1.person_id,
				null,
				t1.encounter_id,
				t1.test_datetime,
				t1.encounter_type,
				null, #t1.location_id,
				t1.obs,
				null, #obs_datetimes
				# in any visit, there many be multiple encounters. for this dataset, we want to include only clinical encounters (e.g. not lab or triage visit)
				0 as is_clinical_encounter,
				1 as encounter_type_sort_index,
				null
			from etl.flat_lab_obs t1
			join plhiv_ncd_summary_build_queue__0 t0 using (person_id)
		);

		drop temporary table if exists plhiv_ncd_summary_0;
		create temporary table plhiv_ncd_summary_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
		(
			select 
				* 
			from plhiv_ncd_summary_0a
			order by person_id, date(encounter_datetime), encounter_type_sort_index
		);

		set @prev_id = null;
		set @cur_id = null;
		set @prev_encounter_date = null;
		set @cur_encounter_date = null;
		set @enrollment_date = null;
		set @cur_location = null;
		set @cur_rtc_date = null;
		set @prev_rtc_date = null;

		set @death_date = null;

		#TO DO
		# screened for cervical ca
		# exposed infant

		drop temporary table if exists plhiv_ncd_summary_1;
		CREATE temporary TABLE plhiv_ncd_summary_1 (INDEX encounter_id (encounter_id)) 
		(
			SELECT 
				obs,
				encounter_type_sort_index,
				@prev_id:=@cur_id AS prev_id,
				@cur_id:=t1.person_id AS cur_id,
				t1.person_id,
				p.uuid,
				t1.visit_id,
				t1.encounter_id,
				@prev_encounter_date:=DATE(@cur_encounter_date) AS prev_encounter_date,
				@cur_encounter_date:=DATE(encounter_datetime) AS cur_encounter_date,
				t1.encounter_datetime,
				t1.encounter_type,
				t1.is_clinical_encounter,
				death_date,
				CASE
					WHEN location_id THEN @cur_location:=location_id
					WHEN @prev_id = @cur_id THEN @cur_location
					ELSE NULL
				END AS location_id,
				CASE
					WHEN @prev_id = @cur_id THEN @prev_rtc_date:=@cur_rtc_date
					ELSE @prev_rtc_date:=NULL
				END AS prev_rtc_date,
				CASE
					WHEN obs REGEXP '!!5096=' THEN @cur_rtc_date:=GETVALUES(obs, 5096)
					WHEN
						@prev_id = @cur_id
					THEN
						IF(@cur_rtc_date > encounter_datetime,
							@cur_rtc_date,
							NULL)
					ELSE @cur_rtc_date:=NULL
				END AS cur_rtc_date,
				@lmp:=GETVALUES(obs, 1836) AS lmp,
				CASE
					WHEN obs REGEXP '!!5085=' THEN @sbp:=GETVALUES(obs, 5085)
				END AS sbp,
				@dbp:=GETVALUES(obs, 5086) AS dbp,
				@pulse:=GETVALUES(obs, 5087) AS pulse,
				@fbs:=GETVALUES(obs, 6252) AS fbs,
				@rbs:=GETVALUES(obs, 887) AS rbs,
				CASE
					WHEN obs REGEXP '!!6126=' THEN @hb_a1c:=GETVALUES(obs, 6126)
					WHEN @prev_id = @cur_id THEN @hb_a1c
					ELSE @hb_a1c:=NULL
				END AS hb_a1c,
				CASE
					WHEN
						obs REGEXP '!!6126='
					THEN
						@hb_a1c_date:=IFNULL(GETVALUES(obs_datetimes, 6126),
								encounter_datetime)
					WHEN @prev_id = @cur_id THEN @hb_a1c_date
					ELSE @hb_a1c_date:=NULL
				END AS hb_a1c_date,
				CASE
					WHEN obs REGEXP '!!790=' THEN @creatinine:=GETVALUES(obs, 790)
					WHEN @prev_id = @cur_id THEN @creatinine
					ELSE @creatinine:=NULL
				END AS creatinine,
				CASE
					WHEN
						obs REGEXP '!!790='
					THEN
						@creatinine_date:=IFNULL(GETVALUES(obs_datetimes, 790),
								encounter_datetime)
					WHEN @prev_id = @cur_id THEN @creatinine_date
					ELSE @creatinine_date:=NULL
				END AS creatinine_date,
				CASE
					WHEN obs REGEXP '!!1006=' THEN @total_cholesterol:=GETVALUES(obs, 1006)
					WHEN @prev_id = @cur_id THEN @total_cholesterol
					ELSE @total_cholesterol:=NULL
				END AS total_cholesterol,
				CASE
					WHEN obs REGEXP '!!1007=' THEN @hdl:=GETVALUES(obs, 1007)
					WHEN @prev_id = @cur_id THEN @hdl
					ELSE @hdl:=NULL
				END AS hdl,
				CASE
					WHEN obs REGEXP '!!1008=' THEN @ldl:=GETVALUES(obs, 1008)
					WHEN @prev_id = @cur_id THEN @ldl
					ELSE @ldl:=NULL
				END AS ldl,
				CASE
					WHEN obs REGEXP '!!1009=' THEN @triglycerides:=GETVALUES(obs, 1009)
					WHEN @prev_id = @cur_id THEN @triglycerides
					ELSE @triglycerides:=NULL
				END AS triglycerides,
				CASE
					WHEN
						obs REGEXP '!!1006='
					THEN
						@lipid_panel_date:=IFNULL(GETVALUES(obs_datetimes, 1006),
								encounter_datetime)
					WHEN @prev_id = @cur_id THEN @lipid_panel_date
					ELSE @lipid_panel_date:=NULL
				END AS lipid_panel_date,
				@dm_status:=GETVALUES(obs, 7287) AS dm_status,
				@htn_status:=GETVALUES(obs, 7288) AS htn_status,
				@dm_meds:=GETVALUES(obs, 7290) AS dm_meds,
				@htn_meds:=GETVALUES(obs, 10241) AS htn_meds,
				-- t2.prescriptions AS prescriptions,
				NULL AS prescriptions,
				@problems:=GETVALUES(obs, 6042) AS problems,
				case
					when obs regexp '!!11200=10239!!' then 1
					when (obs regexp '!!10239=') = 1 and (obs not regexp '!!10239=1066!!') = 1 then 1
					when obs regexp '!!10239=7281!!' then 1
					when obs regexp '!!10239=7282!!' then 1
					when obs regexp '!!10239=1066!!' then 0
					else NULL
				end as has_comorbidity,
				case
					when obs regexp '!!10239=175!!' then 1
					else NULL
				end as has_diabetic_comorbidity,
				case
					when obs regexp '!!10239=7971!!' then 1
					when obs regexp '!!10239=903!!' then 1
					when obs regexp '!!9328=903!!' then 1
					when obs regexp '!!9328=7971!!' then 1
					else NULL
				end as has_hypertension_comorbidity,
				case
					when obs regexp '!!10239=10860!!' then 1
					when obs regexp '!!10239=77!!' then 1
					else NULL
				end as has_mental_disorder_comorbidity,
				case
					when obs regexp '!!10239=5622!!' then 1
					when (obs regexp '!!10239=')= 1  and (obs not regexp '!!10239=(77|175|903|7281|7282|7971|10860)!!')=1 then 1
					else NULL
				end as has_other_comorbidity,
			FROM plhiv_ncd_summary_0 t1
			JOIN amrs.person p USING (person_id)
		);
			--     LEFT OUTER JOIN
			-- prescriptions t2 USING (encounter_id)
		-- );

		set @prev_id = null;
		set @cur_id = null;
		set @prev_encounter_datetime = null;
		set @cur_encounter_datetime = null;

		set @prev_clinical_datetime = null;
		set @cur_clinical_datetime = null;

		set @next_encounter_type = null;
		set @cur_encounter_type = null;

		set @prev_clinical_location_id = null;
		set @cur_clinical_location_id = null;


		alter table plhiv_ncd_summary_1 drop prev_id, drop cur_id;

		drop table if exists plhiv_ncd_summary_2;
		create temporary table plhiv_ncd_summary_2
		(
			select 
				*,
				@prev_id := @cur_id as prev_id,
				@cur_id := person_id as cur_id,

				case
					when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
					else @prev_encounter_datetime := null
				end as next_encounter_datetime_plhiv_ncd,

				@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

				case
					when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
					else @next_encounter_type := null
				end as next_encounter_type_plhiv_ncd,

				@cur_encounter_type := encounter_type as cur_encounter_type,

				case
					when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
					else @prev_clinical_datetime := null
				end as next_clinical_datetime_plhiv_ncd,

				case
					when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
					else @prev_clinical_location_id := null
				end as next_clinical_location_id_plhiv_ncd,

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
				end as next_clinical_rtc_date_plhiv_ncd,

				case
					when is_clinical_encounter then @cur_clinical_rtc_date := cur_rtc_date
					when @prev_id = @cur_id then @cur_clinical_rtc_date
					else @cur_clinical_rtc_date:= null
				end as cur_clinical_rtc_date

			from plhiv_ncd_summary_1
			order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
		);
		
		alter table plhiv_ncd_summary_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;


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

		drop temporary table if exists plhiv_ncd_summary_3;
		create temporary table plhiv_ncd_summary_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
		(
			select
				*,
				@prev_id := @cur_id as prev_id,
				@cur_id := t1.person_id as cur_id,

				-- if same person then cur enc type is same as prev encounter type else null
				case
					when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
					else @prev_encounter_type:=null
				end as prev_encounter_type_plhiv_ncd,
				@cur_encounter_type := encounter_type as cur_encounter_type,

				-- if same person then cur enc dt is same as prev enc dt else null
				case
					when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
					else @prev_encounter_datetime := null
				end as prev_encounter_datetime_plhiv_ncd,

				@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

				-- if same person then cur clc dt is same as prev clc dt else null
				case
					when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
					else @prev_clinical_datetime := null
				end as prev_clinical_datetime_plhiv_ncd,

				-- if same person then cur clc locid is same as prev clc locid else null
				case
					when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
					else @prev_clinical_location_id := null
				end as prev_clinical_location_id_plhiv_ncd,

				-- if clc enc or same person then cur clc dt is enc_dt else null
				case
					when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
					when @prev_id = @cur_id then @cur_clinical_datetime
					else @cur_clinical_datetime := null
				end as cur_clinical_datetime,

				-- if clc enc or same person then cur clc loc is loc_id else null
				case
					when is_clinical_encounter then @cur_clinical_location_id := location_id
					when @prev_id = @cur_id then @cur_clinical_location_id
					else @cur_clinical_location_id := null
				end as cur_clinical_location_id,

				-- if same person then prev clc rtc is cur rtc else null
				case
					when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
					else @prev_clinical_rtc_date := null
				end as prev_clinical_rtc_date_plhiv_ncd,

				-- if clc enc or same person then cur clc rtc is cur rtc else null
				case
					when is_clinical_encounter then @cur_clinical_rtc_date := cur_rtc_date
					when @prev_id = @cur_id then @cur_clinical_rtc_date
					else @cur_clinical_rtc_date:= null
				end as cur_clinic_rtc_date

			from plhiv_ncd_summary_2 t1
			order by person_id, date(encounter_datetime), encounter_type_sort_index
		);

		SELECT COUNT(*) INTO @new_encounter_rows FROM plhiv_ncd_summary_3;

		SELECT @new_encounter_rows;
		set @total_rows_written = @total_rows_written + @new_encounter_rows;
		SELECT @total_rows_written;

		select
			null,
			person_id,
			t1.uuid,
			visit_id,
			encounter_id,
			encounter_datetime,
			encounter_type,
			is_clinical_encounter,
			location_id,
			t2.uuid as location_uuid,
			death_date,
			prev_rtc_date,
			cur_rtc_date,
			lmp,
			sbp,
			dbp,
			pulse,
			fbs,
			rbs,
			hb_a1c,
			hb_a1c_date,
			creatinine,
			creatinine_date,
			total_cholesterol,
			hdl,
			ldl,
			triglycerides,
			lipid_panel_date,
			dm_status,
			htn_status,
			dm_meds,
			htn_meds,
			prescriptions,
			problems,
			has_comorbidity,
			has_mental_disorder_comorbidity,
			has_diabetic_comorbidity,
			has_hypertension_comorbidity,
			has_other_comorbidity,
			prev_encounter_datetime_plhiv_ncd,
			next_encounter_datetime_plhiv_ncd,
			prev_encounter_type_plhiv_ncd,
			next_encounter_type_plhiv_ncd,
			prev_clinical_datetime_plhiv_ncd,
			next_clinical_datetime_plhiv_ncd,
			prev_clinical_location_id_plhiv_ncd,
			next_clinical_location_id_plhiv_ncd,
			prev_clinical_rtc_date_plhiv_ncd,
			next_clinical_rtc_date_plhiv_ncd
		from plhiv_ncd_summary_3 t1
		join amrs.location t2 using (location_id);

		#add data to table
		SET @dyn_sql=CONCAT('replace into ',@write_table,
			'(select
			null,
			person_id,
			t1.uuid,
			visit_id,
			encounter_id,
			encounter_datetime,
			encounter_type,
			is_clinical_encounter,
			location_id,
			t2.uuid as location_uuid,
			death_date,
			prev_rtc_date,
			cur_rtc_date,
			lmp,
			sbp,
			dbp,
			pulse,
			fbs,
			rbs,
			hb_a1c,
			hb_a1c_date,
			creatinine,
			creatinine_date,
			total_cholesterol,
			hdl,
			ldl,
			triglycerides,
			lipid_panel_date,
			dm_status,
			htn_status,
			dm_meds,
			htn_meds,
			prescriptions,
			problems,
			has_comorbidity,
			has_mental_disorder_comorbidity,
			has_diabetic_comorbidity,
			has_hypertension_comorbidity,
			has_other_comorbidity,
			prev_encounter_datetime_plhiv_ncd,
			next_encounter_datetime_plhiv_ncd,
			prev_encounter_type_plhiv_ncd,
			next_encounter_type_plhiv_ncd,
			prev_clinical_datetime_plhiv_ncd,
			next_clinical_datetime_plhiv_ncd,
			prev_clinical_location_id_plhiv_ncd,
			next_clinical_location_id_plhiv_ncd,
			prev_clinical_rtc_date_plhiv_ncd,
			next_clinical_rtc_date_plhiv_ncd
			
			from plhiv_ncd_summary_3 t1
			join amrs.location t2 using (location_id))');

		PREPARE s1 from @dyn_sql;
		EXECUTE s1;
		DEALLOCATE PREPARE s1;


		#delete from @queue_table where person_id in (select person_id from plhiv_ncd_summary_build_queue__0);

		SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join plhiv_ncd_summary_build_queue__0 t2 using (person_id);');
		PREPARE s1 from @dyn_sql;
		EXECUTE s1;
		DEALLOCATE PREPARE s1;


		#select @person_ids_count := (select count(*) from plhiv_ncd_summary_build_queue_2);
		SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';');
		PREPARE s1 from @dyn_sql;
		EXECUTE s1;
		DEALLOCATE PREPARE s1;

		#select @person_ids_count as remaining_in_build_queue;

		set @cycle_length = timestampdiff(second,@loop_start_time,now());
		#select concat('Cycle time: ',@cycle_length,' seconds');
		set @total_time = @total_time + @cycle_length;
		set @cycle_number = @cycle_number + 1;

		#select ceil(@person_ids_count / cycle_size) as remaining_cycles;
		set @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);
		
		SELECT
			@person_ids_count AS 'persons remaining',
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

			SELECT
    			CONCAT(@start_write,
            	' : Writing ',
           		@total_rows_to_write,
            	' to ',
            	@primary_table);

			SET @dyn_sql=CONCAT('replace into ', @primary_table,
				'(select * from ',@write_table,');');
			PREPARE s1 from @dyn_sql;
			EXECUTE s1;
			DEALLOCATE PREPARE s1;

			set @finish_write = now();
			set @time_to_write = timestampdiff(second,@start_write,@finish_write);

			SELECT
    			CONCAT(@finish_write,
            	' : Completed writing rows. Time to write to primary table: ',
            	@time_to_write,
            	' seconds ');

			SET @dyn_sql=CONCAT('drop table ',@write_table,';');
			PREPARE s1 from @dyn_sql;
			EXECUTE s1;
			DEALLOCATE PREPARE s1;
	end if;

	set @ave_cycle_length = ceil(@total_time/@cycle_number);
	SELECT
		CONCAT('Average Cycle Length: ',
		@ave_cycle_length,
		' second(s)');

	set @end = now();
	insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
	SELECT
    	CONCAT(@table_version,
        ' : Time to complete: ',
        TIMESTAMPDIFF(MINUTE, @start, @end),
        ' minutes');
END$$
DELIMITER ;