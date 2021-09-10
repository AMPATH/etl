DELIMITER $$
CREATE  PROCEDURE `generate_flat_obs_v_4_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN

    SET @primary_table := "flat_obs";
    SET @query_type = query_type;
    SET @queue_table = "";
    SET @total_rows_written = 0;
    SET @start = now();
    SET @table_version = "flat_obs_v1.5";
    SET SESSION sort_buffer_size = 512000000;
    SELECT @fake_visit_id := 10000000;
    SELECT @boundary := '!!';
    set @drug_separator := ' ## ';

    CREATE TABLE IF NOT EXISTS flat_obs (
        person_id INT,
        visit_id INT,
        encounter_id INT,
        encounter_datetime DATETIME,
        encounter_type INT,
        location_id INT,
        obs TEXT,
        obs_datetimes TEXT,
        max_date_created DATETIME,
        INDEX encounter_id (encounter_id),
        INDEX person_date (person_id, encounter_datetime),
        INDEX person_enc_id (person_id, encounter_id),
        INDEX date_created (max_date_created),
        PRIMARY KEY (encounter_id)
    );
    SELECT CONCAT('Created flat_obs_table');

    IF(@query_type = "build") THEN
		SELECT 'BUILDING..........................................';
		SET @write_table = concat("flat_obs_temp_", queue_number);
		SET @queue_table = concat("flat_obs_build_queue_", queue_number);
		SET @dyn_sql = CONCAT(
				'Create table if not exists ',
				@write_table,
				' like ',
				@primary_table
			);
		PREPARE s1
		FROM @dyn_sql;
		EXECUTE s1;
		DEALLOCATE PREPARE s1;
		SET @dyn_sql = CONCAT(
				'Create table if not exists ',
				@queue_table,
				' (select person_id from flat_obs_build_queue limit ',
				queue_size,
				');'
			);
		PREPARE s1
		FROM @dyn_sql;
		EXECUTE s1;
		DEALLOCATE PREPARE s1;
        
		SET @dyn_sql = CONCAT(
				'delete t1 from flat_obs_build_queue t1 join ',
				@queue_table,
				' t2 using (person_id);'
			);
		PREPARE s1
		FROM @dyn_sql;
		EXECUTE s1;
		DEALLOCATE PREPARE s1;
    END IF;

    IF (@query_type = "sync") THEN
		SELECT 'SYNCING..........................................';
		SET @write_table := @primary_table;
		SET @queue_table := "flat_obs_sync_queue";
		CREATE TABLE IF NOT EXISTS flat_obs_sync_queue (person_id INT PRIMARY KEY);

		SET @last_update := null;
		SELECT MAX(date_updated) INTO @last_update FROM etl.flat_log WHERE table_name = @table_version;

		SELECT 'Finding patients to sync in amrs.obs...';
		REPLACE INTO flat_obs_sync_queue (
			SELECT distinct person_id
			FROM amrs.obs
			where date_created > @last_update
		);
    END IF;

    SET @person_ids_count = 0;
    SET @dyn_sql = CONCAT(
            'select count(*) into @person_ids_count from ',
            @queue_table
        );
    PREPARE s1
    FROM @dyn_sql;
    EXECUTE s1;
    DEALLOCATE PREPARE s1;
    SELECT @person_ids_count AS 'num patients to sync/build';
    SET @dyn_sql = CONCAT(
            'delete t1 from ',
            @primary_table,
            ' t1 join ',
            @queue_table,
            ' t2 using (person_id);'
        );
    PREPARE s1
    FROM @dyn_sql;
    EXECUTE s1;
    DEALLOCATE PREPARE s1;

    SET @total_time = 0;
    SET @cycle_number = 0;
    WHILE @person_ids_count > 0 do
        SET @loop_start_time = now();
        DROP temporary table if exists flat_obs_build_queue__0;
        SET @dyn_sql = CONCAT(
                'create temporary table IF NOT EXISTS flat_obs_build_queue__0 (person_id int primary key) (select * from ',
                @queue_table,
                ' limit ',
                cycle_size,
                ');'
            );
        PREPARE s1
        FROM @dyn_sql;
        EXECUTE s1;
        DEALLOCATE PREPARE s1;
        CREATE temporary TABLE IF NOT EXISTS flat_person_encounters__0 (
            person_id INT,
            visit_id INT,
            encounter_id INT,
            encounter_datetime DATETIME,
            encounter_type INT,
            location_id INT
        );

        SELECT CONCAT('Created flat person encounters table');
        SELECT CONCAT('replace into flat_person_encounters__0');
        replace into flat_person_encounters__0(
            SELECT p.person_id,
                e.visit_id,
                e.encounter_id,
                e.encounter_datetime,
                e.encounter_type,
                e.location_id
            FROM etl.flat_obs_build_queue__0 `p`
                 JOIN amrs.encounter `e` on (e.patient_id = p.person_id)
        );

        DROP  TABLE if exists flat_obs__0;
        CREATE  table flat_obs__0 (
            select o.person_id,
                case
                    when e.visit_id is not null then e.visit_id
                    else @fake_visit_id := @fake_visit_id + 1
                end as visit_id,
                o.encounter_id,
                e.encounter_datetime,
                e.encounter_type,
                e.location_id,
                group_concat(
                    case
                        when value_drug is not null then concat(
                        @boundary,
                        value_coded,
                        '_drug',
                        '=',
                        value_drug,
                        @boundary,
                        @drug_separator,
                        @boundary,
                        o.concept_id,
                        '=',
                        value_coded,
                        @boundary
                        )
                        when value_coded is not null then concat(
                            @boundary,
                            o.concept_id,
                            '=',
                            value_coded,
                            @boundary
                        )
                        when value_numeric is not null then concat(
                            @boundary,
                            o.concept_id,
                            '=',
                            value_numeric,
                            @boundary
                        )
                        when value_datetime is not null then concat(
                            @boundary,
                            o.concept_id,
                            '=',
                            date(value_datetime),
                            @boundary
                        ) -- when value_boolean is not null then concat(@boundary,o.concept_id,'=',value_boolean,@boundary)
                        when value_text is not null then concat(
                            @boundary,
                            o.concept_id,
                            '=',
                            value_text,
                            @boundary
                        )
                        when value_drug is not null then concat(
                            @boundary,
                            o.concept_id,
                            '=',
                            value_drug,
                            @boundary
                        )
                        when value_modifier is not null then concat(
                            @boundary,
                            o.concept_id,
                            '=',
                            value_modifier,
                            @boundary
                        )
                    end
                    order by o.concept_id,
                        value_coded separator ' ## '
                ) as obs,
                group_concat(
                    case
                        when value_coded is not null
                        or value_numeric is not null
                        or value_datetime is not null
                        or value_text is not null
                        or value_drug is not null
                        or value_modifier is not null then concat(
                            @boundary,
                            o.concept_id,
                            '=',
                            date(o.obs_datetime),
                            @boundary
                        )
                    end
                    order by o.concept_id,
                        value_coded separator ' ## '
                ) as obs_datetimes,
                max(o.date_created) as max_date_created
                from amrs.obs o
                join flat_person_encounters__0 `e` on (e.encounter_id = o.encounter_id)
            where o.encounter_id > 1
                and o.voided = 0
            group by e.encounter_id
        );

        # Add back obs sets without encounter_ids with voided obs removed
        replace into flat_obs__0 (
            select o.person_id,
                @fake_visit_id := @fake_visit_id + 1,
                min(o.obs_id) + 100000000 as encounter_id,
                o.obs_datetime,
                99999 as encounter_type,
                null as location_id,
                group_concat(
                    case
                        when value_drug is not null then concat(
                        @boundary,value_coded,
                        '_drug',
                        '=',
                        value_drug,
                        @boundary,
                        @drug_separator,
                        @boundary,
                        o.concept_id,
                        '=',
                        value_coded,
                        @boundary
                        )
                        when value_coded is not null then concat(
                            @boundary,
                            o.concept_id,
                            '=',
                            value_coded,
                            @boundary
                        )
                        when value_numeric is not null then concat(
                            @boundary,
                            o.concept_id,
                            '=',
                            value_numeric,
                            @boundary
                        )
                        when value_datetime is not null then concat(
                            @boundary,
                            o.concept_id,
                            '=',
                            date(value_datetime),
                            @boundary
                        ) -- when value_boolean is not null then concat(@boundary,o.concept_id,'=',value_boolean,@boundary)
                        when value_text is not null then concat(
                            @boundary,
                            o.concept_id,
                            '=',
                            value_text,
                            @boundary
                        )
                        when value_modifier is not null then concat(
                            @boundary,
                            o.concept_id,
                            '=',
                            value_modifier,
                            @boundary
                        )
                    end
                    order by o.concept_id,
                        value_coded separator ' ## '
                ) as obs,
                group_concat(
                    case
                        when value_coded is not null
                        or value_numeric is not null
                        or value_datetime is not null
                        or value_text is not null
                        or value_drug is not null
                        or value_modifier is not null then concat(
                            @boundary,
                            o.concept_id,
                            '=',
                            date(o.obs_datetime),
                            @boundary
                        )
                    end
                    order by o.concept_id,
                        value_coded separator ' ## '
                ) as obs_datetimes,
                max(o.date_created) as max_date_created
            from amrs.obs o
                join etl.flat_obs_build_queue__0 `e` using (person_id)
            where o.encounter_id is null
                and o.voided = 0
            group by person_id,
                o.obs_datetime
        );

        DROP temporary table if exists flat_person_encounters__0;
        SET @dyn_sql = CONCAT(
                'replace into ',
                @write_table,
                '(select
                    person_id,
                    visit_id,
                    encounter_id,
                    encounter_datetime,
                    encounter_type,
                    location_id,
                    obs,
                    obs_datetimes,
                    max_date_created
                    from flat_obs__0 t1)'
            );
        PREPARE s1
        FROM @dyn_sql;
        EXECUTE s1;
        DEALLOCATE PREPARE s1;
        
		SELECT 'Copy build patients to hiv summary sync queue...';
		SET @dyn_sql = CONCAT(
				'replace into etl.flat_hiv_summary_sync_queue(select distinct person_id from flat_obs_build_queue__0);'
			);
		PREPARE s1
		FROM @dyn_sql;
		EXECUTE s1;
		DEALLOCATE PREPARE s1;
        
        SET @dyn_sql = CONCAT(
                'delete t1 from ',
                @queue_table,
                ' t1 join flat_obs_build_queue__0 t2 using (person_id);'
            );
        
        PREPARE s1
        FROM @dyn_sql;
        EXECUTE s1;
        DEALLOCATE PREPARE s1;
        SET @dyn_sql = CONCAT(
                'select count(*) into @person_ids_count from ',
                @queue_table,
                ';'
            );
        PREPARE s1
        FROM @dyn_sql;
        EXECUTE s1;
        DEALLOCATE PREPARE s1;
        SET @cycle_length = timestampdiff(second, @loop_start_time, now());
        SET @total_time = @total_time + @cycle_length;
        SET @cycle_number = @cycle_number + 1;
        SET @remaining_time = ceil(
                (@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60
            );

        SELECT @person_ids_count AS 'persons remaining',
            @cycle_length AS 'Cycle time (s)',
            CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
            @remaining_time AS 'Est time remaining (min)';
    END WHILE;

    IF(@query_type = "build") THEN
		SET @dyn_sql = CONCAT(
				'delete t1 from ',
				@queue_table,
				' t1 join flat_obs_build_queue__0 t2 using (person_id);'
			);

		PREPARE s1 from @dyn_sql;
		EXECUTE s1;
		DEALLOCATE PREPARE s1;
		SET @dyn_sql = CONCAT('drop table ', @queue_table, ';');

        PREPARE s1 from @dyn_sql;
		EXECUTE s1;
		DEALLOCATE PREPARE s1;

        SET @total_rows_to_write = 0;
		SET @dyn_sql = CONCAT(
				"Select count(*) into @total_rows_to_write from ",
				@write_table
			);

		PREPARE s1 from @dyn_sql;
		EXECUTE s1;
		DEALLOCATE PREPARE s1;

		SET @start_write = now();
		SELECT CONCAT(@start_write, ' : Writing ', @total_rows_to_write, ' to ', @primary_table);
		SET @dyn_sql = CONCAT('replace into ', @primary_table, '(select * from ', @write_table,');');
        PREPARE s1 from @dyn_sql;
		EXECUTE s1;
		DEALLOCATE PREPARE s1;

		SET @finish_write = now();
		SET @time_to_write = timestampdiff(second, @start_write, @finish_write);
		SELECT CONCAT(
				@finish_write,
				' : Completed writing rows. Time to write to primary table: ',
				@time_to_write,
				' seconds '
			);
        
        PREPARE s1 from @dyn_sql;
        EXECUTE s1;
        DEALLOCATE PREPARE s1;
        SET @dyn_sql = CONCAT('drop table ', @write_table, ';');
        PREPARE s1
        from @dyn_sql;
        EXECUTE s1;
        DEALLOCATE PREPARE s1;
    END IF;

    SET @end := NOW();
    SET @date_created := NOW();
    # INSERT INTO etl.flat_log VALUES (@start, @date_created, @table_version, TIMESTAMPDIFF(SECOND, @start, @end));
    SELECT CONCAT(@table_version, ': Time to complete: ', TIMESTAMPDIFF(MINUTE, @start, @end), ' minutes');

END$$
DELIMITER ;
