CREATE DEFINER=`openmrs_user`@`%` PROCEDURE `etl`.`generate_flat_moh_706_v1`(
    IN query_type   VARCHAR(50),
    IN queue_number INT,
    IN queue_size   INT,
    IN cycle_size   INT
)
BEGIN
    # MOH 706 Report ETL - v1.0
    # Reads from flat_lab_obs_v2 and extracts lab columns for MOH 706

    SET session sort_buffer_size     = 512000000;
    SET session group_concat_max_len = 100000;

    SET @start              = NOW();
    SET @primary_table      = 'flat_moh_706_report';
    SET @table_version      = 'flat_moh_706_report_v1.0';
    SET @total_rows_written = 0;
    SET @query_type         = query_type;
    SET @queue_number       = queue_number;
    SET @queue_size         = queue_size;
    SET @cycle_size         = cycle_size;
    SET @boundary           = '!!';
    SET @sep                = ' ## ';

    -- ------------------------------------------------------------------
    -- Primary table (all VARCHAR to avoid CAST failures on dirty data)
    -- ------------------------------------------------------------------
    SET @dyn_sql = CONCAT('CREATE TABLE IF NOT EXISTS ', @primary_table, ' (
        date_created        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        person_id           INT,
        uuid                VARCHAR(100),
        encounter_id        INT,
        test_datetime       DATETIME,
        encounter_type      INT,
        location_id         INT,
        blood_sugar         VARCHAR(100),
        ogtt                VARCHAR(100),
        creatinine          VARCHAR(100),
        urea                VARCHAR(100),
        direct_bili         VARCHAR(100),
        total_bili          VARCHAR(100),
        ast                 VARCHAR(100),
        alt                 VARCHAR(100),
        total_protein       VARCHAR(100),
        alk_phos            VARCHAR(100),
        lipid_profile       VARCHAR(100),
        total_cholesterol   VARCHAR(100),
        triglycerides       VARCHAR(100),
        ldl                 VARCHAR(100),
        t3                  VARCHAR(100),
        t4                  VARCHAR(100),
        tsh                 VARCHAR(100),
        total_psa           VARCHAR(100),
        urine_glucose       VARCHAR(100),
        urinalysis          VARCHAR(100),
        urine_ketones       VARCHAR(100),
        urine_proteins      VARCHAR(100),
        urine_pus_cells     VARCHAR(100),
        urine_s_haematobium VARCHAR(100),
        urine_t_vaginalis   VARCHAR(100),
        urine_yeast_cells   VARCHAR(100),
        malaria_bs          VARCHAR(100),
        malaria_rdt         VARCHAR(100),
        hookworm            VARCHAR(100),
        roundworms          VARCHAR(100),
        fbc                 VARCHAR(100),
        blood_group         VARCHAR(100),
        hiv                 VARCHAR(100),
        hep_b               VARCHAR(100),
        hep_c               VARCHAR(100),
        syphilis            VARCHAR(100),
        salmonella          VARCHAR(100),
        vdrl                VARCHAR(100),
        brucella            VARCHAR(100),
        rheumatoid_factor   VARCHAR(100),
        h_pylori            VARCHAR(100),
        hcg                 VARCHAR(100),
        crag                VARCHAR(100),
        cd4                 VARCHAR(100),
        viral_load          VARCHAR(100),
        tb_culture          VARCHAR(100),
        PRIMARY KEY encounter_id (encounter_id),
        INDEX person_date (person_id, test_datetime),
        INDEX person_uuid (uuid),
        INDEX location (location_id)
    )');
    PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

    -- ------------------------------------------------------------------
    -- BUILD mode
    -- Requires a table named `<primary_table>_build_queue` to exist and
    -- contain the person_ids to process.  For this procedure that's
    -- `flat_moh_706_report_build_queue`.
    -- ------------------------------------------------------------------
    IF (@query_type = 'build') THEN
        SELECT CONCAT('BUILDING ', @primary_table, '..........................................');
        SET @write_table = CONCAT(@primary_table, '_temp_', @queue_number);
        SET @build_queue = CONCAT(@primary_table, '_build_queue');
        SET @queue_table = CONCAT(@build_queue, '_', @queue_number);

        SET @dyn_sql = CONCAT('CREATE TABLE IF NOT EXISTS ', @write_table, ' LIKE ', @primary_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT(
            'CREATE TABLE IF NOT EXISTS ', @queue_table,
            ' (person_id INT PRIMARY KEY) AS (SELECT * FROM ', @build_queue,
            ' LIMIT ', @queue_size, ')'
        );
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT('DELETE t1 FROM ', @build_queue, ' t1 JOIN ', @queue_table, ' t2 USING (person_id)');
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT('SELECT COUNT(*) INTO @queue_count FROM ', @queue_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;
    END IF;

    -- ------------------------------------------------------------------
    -- SYNC mode
    -- FIX: every reference to `flat_blood_chemistry_sync_queue` is now
    --      `flat_moh_706_report_sync_queue`, matching @queue_table.
    --      In the original renamed version, the CREATE/REPLACE wrote
    --      to the wrong table, so the downstream @queue_count was 0
    --      and the WHILE loop was skipped (nothing built).
    -- ------------------------------------------------------------------
    IF (@query_type = 'sync') THEN
        SELECT CONCAT('SYNCING ', @primary_table, '..........................................');
        SET @write_table = @primary_table;
        SET @queue_table = CONCAT(@primary_table, '_sync_queue');
        SET @last_update = NULL;

        SELECT MAX(date_updated) INTO @last_update
        FROM etl.flat_log
        WHERE table_name = @table_version;

        -- If no prior run, treat every row as new
        SET @last_update = IFNULL(@last_update, '1900-01-01');

        CREATE TABLE IF NOT EXISTS flat_moh_706_report_sync_queue (
            person_id INT PRIMARY KEY
        );

        REPLACE INTO flat_moh_706_report_sync_queue
            (SELECT DISTINCT patient_id FROM amrs.encounter      WHERE date_changed    > @last_update);
        REPLACE INTO flat_moh_706_report_sync_queue
            (SELECT DISTINCT person_id  FROM etl.flat_lab_obs_v2 WHERE max_date_created > @last_update);
        REPLACE INTO flat_moh_706_report_sync_queue
            (SELECT person_id FROM amrs.person WHERE date_voided  > @last_update);
        REPLACE INTO flat_moh_706_report_sync_queue
            (SELECT person_id FROM amrs.person WHERE date_changed > @last_update);
    END IF;

    -- Delete stale rows for queued persons
    SET @dyn_sql = CONCAT('DELETE t1 FROM ', @primary_table, ' t1 JOIN ', @queue_table, ' t2 USING (person_id)');
    PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

    SET @dyn_sql = CONCAT('SELECT COUNT(*) INTO @queue_count FROM ', @queue_table);
    PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

    SET @total_time   = 0;
    SET @cycle_number = 0;

    -- ------------------------------------------------------------------
    -- Main processing loop
    -- ------------------------------------------------------------------
    WHILE @queue_count > 0 DO

        SET @loop_start_time = NOW();

        DROP TEMPORARY TABLE IF EXISTS temp_queue_table;
        SET @dyn_sql = CONCAT('CREATE TEMPORARY TABLE temp_queue_table LIKE ', @queue_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT('REPLACE INTO temp_queue_table (SELECT * FROM ', @queue_table, ' LIMIT ', @cycle_size, ')');
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        -- FIX: rename temp table from flat_blood_chemistry_0 → flat_moh_706_report_0
        DROP TEMPORARY TABLE IF EXISTS flat_moh_706_report_0;

        SET @dyn_sql = CONCAT(
            'CREATE TEMPORARY TABLE flat_moh_706_report_0
                (PRIMARY KEY (encounter_id), INDEX person_test (person_id, test_datetime))
            AS (SELECT
                t1.person_id,
                t2.uuid,
                t1.encounter_id,
                t1.test_datetime,
                t1.encounter_type,
                t1.location_id,

                -- Blood Sugar
                IF(t1.obs REGEXP \"!!887=\",   SUBSTRING_INDEX(getValues(t1.obs, 887),   \" ## \", 1), NULL) AS blood_sugar,
                IF(t1.obs REGEXP \"!!10627=\", SUBSTRING_INDEX(getValues(t1.obs, 10627), \" ## \", 1), NULL) AS ogtt,

                -- Renal Function
                IF(t1.obs REGEXP \"!!790=\", SUBSTRING_INDEX(getValues(t1.obs, 790), \" ## \", 1), NULL) AS creatinine,
                IF(t1.obs REGEXP \"!!857=\", SUBSTRING_INDEX(getValues(t1.obs, 857), \" ## \", 1), NULL) AS urea,

                -- Liver Function
                IF(t1.obs REGEXP \"!!1297=\", SUBSTRING_INDEX(getValues(t1.obs, 1297), \" ## \", 1), NULL) AS direct_bili,
                IF(t1.obs REGEXP \"!!655=\",  SUBSTRING_INDEX(getValues(t1.obs, 655),  \" ## \", 1), NULL) AS total_bili,
                IF(t1.obs REGEXP \"!!653=\",  SUBSTRING_INDEX(getValues(t1.obs, 653),  \" ## \", 1), NULL) AS ast,
                IF(t1.obs REGEXP \"!!654=\",  SUBSTRING_INDEX(getValues(t1.obs, 654),  \" ## \", 1), NULL) AS alt,
                IF(t1.obs REGEXP \"!!717=\",  SUBSTRING_INDEX(getValues(t1.obs, 717),  \" ## \", 1), NULL) AS total_protein,
                IF(t1.obs REGEXP \"!!785=\",  SUBSTRING_INDEX(getValues(t1.obs, 785),  \" ## \", 1), NULL) AS alk_phos,

                -- Lipid Profile
                IF(t1.obs REGEXP \"!!1010=\", SUBSTRING_INDEX(getValues(t1.obs, 1010), \" ## \", 1), NULL) AS lipid_profile,
                IF(t1.obs REGEXP \"!!1006=\", SUBSTRING_INDEX(getValues(t1.obs, 1006), \" ## \", 1), NULL) AS total_cholesterol,
                IF(t1.obs REGEXP \"!!1009=\", SUBSTRING_INDEX(getValues(t1.obs, 1009), \" ## \", 1), NULL) AS triglycerides,
                IF(t1.obs REGEXP \"!!1008=\", SUBSTRING_INDEX(getValues(t1.obs, 1008), \" ## \", 1), NULL) AS ldl,

                -- Hormonal
                IF(t1.obs REGEXP \"!!7881=\", SUBSTRING_INDEX(getValues(t1.obs, 7881), \" ## \", 1), NULL) AS t3,
                IF(t1.obs REGEXP \"!!7882=\", SUBSTRING_INDEX(getValues(t1.obs, 7882), \" ## \", 1), NULL) AS t4,
                IF(t1.obs REGEXP \"!!7880=\", SUBSTRING_INDEX(getValues(t1.obs, 7880), \" ## \", 1), NULL) AS tsh,

                -- Tumor Markers
                IF(t1.obs REGEXP \"!!10249=\", SUBSTRING_INDEX(getValues(t1.obs, 10249), \" ## \", 1), NULL) AS total_psa,

                -- Urine Chemistry
                IF(t1.obs REGEXP \"!!2340=\",  SUBSTRING_INDEX(getValues(t1.obs, 2340),  \" ## \", 1), NULL) AS urine_glucose,
                IF(t1.obs REGEXP \"!!302=\",   SUBSTRING_INDEX(getValues(t1.obs, 302),   \" ## \", 1), NULL) AS urinalysis,
                IF(t1.obs REGEXP \"!!7276=\",  SUBSTRING_INDEX(getValues(t1.obs, 7276),  \" ## \", 1), NULL) AS urine_ketones,
                IF(t1.obs REGEXP \"!!2339=\",  SUBSTRING_INDEX(getValues(t1.obs, 2339),  \" ## \", 1), NULL) AS urine_proteins,
                IF(t1.obs REGEXP \"!!1984=\",  SUBSTRING_INDEX(getValues(t1.obs, 1984),  \" ## \", 1), NULL) AS urine_pus_cells,
                IF(t1.obs REGEXP \"!!1985=\",  SUBSTRING_INDEX(getValues(t1.obs, 1985),  \" ## \", 1), NULL) AS urine_s_haematobium,
                IF(t1.obs REGEXP \"!!15086=\", SUBSTRING_INDEX(getValues(t1.obs, 15086), \" ## \", 1), NULL) AS urine_t_vaginalis,
                IF(t1.obs REGEXP \"!!15084=\", SUBSTRING_INDEX(getValues(t1.obs, 15084), \" ## \", 1), NULL) AS urine_yeast_cells,

                -- Malaria
                IF(t1.obs REGEXP \"!!32=\",   SUBSTRING_INDEX(getValues(t1.obs, 32),   \" ## \", 1), NULL) AS malaria_bs,
                IF(t1.obs REGEXP \"!!9187=\", SUBSTRING_INDEX(getValues(t1.obs, 9187), \" ## \", 1), NULL) AS malaria_rdt,

                -- Stool
                IF(t1.obs REGEXP \"!!1524=\", SUBSTRING_INDEX(getValues(t1.obs, 1524), \" ## \", 1), NULL) AS hookworm,
                IF(t1.obs REGEXP \"!!1525=\", SUBSTRING_INDEX(getValues(t1.obs, 1525), \" ## \", 1), NULL) AS roundworms,

                -- Haematology
                IF(t1.obs REGEXP \"!!1019=\", SUBSTRING_INDEX(getValues(t1.obs, 1019), \" ## \", 1), NULL) AS fbc,
                IF(t1.obs REGEXP \"!!300=\",  SUBSTRING_INDEX(getValues(t1.obs, 300),  \" ## \", 1), NULL) AS blood_group,

                -- Blood Screening
                IF(t1.obs REGEXP \"!!6709=\", SUBSTRING_INDEX(getValues(t1.obs, 6709), \" ## \", 1), NULL) AS hiv,
                IF(t1.obs REGEXP \"!!1322=\", SUBSTRING_INDEX(getValues(t1.obs, 1322), \" ## \", 1), NULL) AS hep_b,
                IF(t1.obs REGEXP \"!!1325=\", SUBSTRING_INDEX(getValues(t1.obs, 1325), \" ## \", 1), NULL) AS hep_c,
                IF(t1.obs REGEXP \"!!299=\",  SUBSTRING_INDEX(getValues(t1.obs, 299),  \" ## \", 1), NULL) AS syphilis,

                -- Bacteriology & Serology
                IF(t1.obs REGEXP \"!!12881=\", SUBSTRING_INDEX(getValues(t1.obs, 12881), \" ## \", 1), NULL) AS salmonella,
                IF(t1.obs REGEXP \"!!11868=\", SUBSTRING_INDEX(getValues(t1.obs, 11868), \" ## \", 1), NULL) AS vdrl,
                IF(t1.obs REGEXP \"!!305=\",   SUBSTRING_INDEX(getValues(t1.obs, 305),   \" ## \", 1), NULL) AS brucella,
                IF(t1.obs REGEXP \"!!12880=\", SUBSTRING_INDEX(getValues(t1.obs, 12880), \" ## \", 1), NULL) AS rheumatoid_factor,
                IF(t1.obs REGEXP \"!!12875=\", SUBSTRING_INDEX(getValues(t1.obs, 12875), \" ## \", 1), NULL) AS h_pylori,
                IF(t1.obs REGEXP \"!!45=\",    SUBSTRING_INDEX(getValues(t1.obs, 45),    \" ## \", 1), NULL) AS hcg,
                IF(t1.obs REGEXP \"!!9812=\",  SUBSTRING_INDEX(getValues(t1.obs, 9812),  \" ## \", 1), NULL) AS crag,

                -- Reference Labs
                IF(t1.obs REGEXP \"!!12078=\", SUBSTRING_INDEX(getValues(t1.obs, 12078), \" ## \", 1), NULL) AS cd4,
                IF(t1.obs REGEXP \"!!856=\",   SUBSTRING_INDEX(getValues(t1.obs, 856),   \" ## \", 1), NULL) AS viral_load,
                IF(t1.obs REGEXP \"!!2311=\",  SUBSTRING_INDEX(getValues(t1.obs, 2311),  \" ## \", 1), NULL) AS tb_culture

            FROM flat_lab_obs_v2  t1
            JOIN temp_queue_table t3 USING (person_id)
            JOIN amrs.person      t2 USING (person_id)
            )'
        );
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        -- FIX: read from flat_moh_706_report_0 (was flat_blood_chemistry_0)
        SET @dyn_sql = CONCAT(
            'INSERT INTO ', @write_table,
            ' (person_id, uuid, encounter_id, test_datetime, encounter_type, location_id,
               blood_sugar, ogtt, creatinine, urea, direct_bili, total_bili,
               ast, alt, total_protein, alk_phos,
               lipid_profile, total_cholesterol, triglycerides, ldl,
               t3, t4, tsh, total_psa,
               urine_glucose, urinalysis, urine_ketones, urine_proteins,
               urine_pus_cells, urine_s_haematobium, urine_t_vaginalis, urine_yeast_cells,
               malaria_bs, malaria_rdt, hookworm, roundworms, fbc, blood_group,
               hiv, hep_b, hep_c, syphilis,
               salmonella, vdrl, brucella, rheumatoid_factor, h_pylori, hcg, crag,
               cd4, viral_load, tb_culture)
            SELECT
                person_id, uuid, encounter_id, test_datetime, encounter_type, location_id,
                blood_sugar, ogtt, creatinine, urea, direct_bili, total_bili,
                ast, alt, total_protein, alk_phos,
                lipid_profile, total_cholesterol, triglycerides, ldl,
                t3, t4, tsh, total_psa,
                urine_glucose, urinalysis, urine_ketones, urine_proteins,
                urine_pus_cells, urine_s_haematobium, urine_t_vaginalis, urine_yeast_cells,
                malaria_bs, malaria_rdt, hookworm, roundworms, fbc, blood_group,
                hiv, hep_b, hep_c, syphilis,
                salmonella, vdrl, brucella, rheumatoid_factor, h_pylori, hcg, crag,
                cd4, viral_load, tb_culture
            FROM flat_moh_706_report_0'
        );
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT('DELETE t1 FROM ', @queue_table, ' t1 JOIN temp_queue_table t2 USING (person_id)');
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT('SELECT COUNT(*) INTO @queue_count FROM ', @queue_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @cycle_length   = TIMESTAMPDIFF(SECOND, @loop_start_time, NOW());
        SET @total_time     = @total_time + @cycle_length;
        SET @cycle_number   = @cycle_number + 1;
        SET @remaining_time = CEIL((@total_time / @cycle_number) * CEIL(@queue_count / @cycle_size) / 60);

        SELECT
            @queue_count                     AS '# in queue',
            @cycle_length                    AS 'Cycle Time (s)',
            CEIL(@queue_count / @cycle_size) AS remaining_cycles,
            @remaining_time                  AS 'Est time remaining (min)';

    END WHILE;

    -- Remove voided persons
    SET @dyn_sql = CONCAT(
        'DELETE t1 FROM ', @primary_table, ' t1
         JOIN amrs.person t2 USING (person_id)
         WHERE t2.voided = 1'
    );
    PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

    -- ------------------------------------------------------------------
    -- BUILD mode: merge temp table → primary
    -- ------------------------------------------------------------------
    IF (@query_type = 'build') THEN
        SET @dyn_sql = CONCAT('DROP TABLE ', @queue_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @total_rows_to_write = 0;
        SET @dyn_sql = CONCAT('SELECT COUNT(*) INTO @total_rows_to_write FROM ', @write_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @start_write = NOW();
        SELECT CONCAT(@start_write, ' : Writing ', @total_rows_to_write, ' to ', @primary_table);

        SET @dyn_sql = CONCAT('REPLACE INTO ', @primary_table, ' (SELECT * FROM ', @write_table, ')');
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @finish_write  = NOW();
        SET @time_to_write = TIMESTAMPDIFF(SECOND, @start_write, @finish_write);
        SELECT CONCAT(@finish_write, ' : Completed. Time to write: ', @time_to_write, ' seconds');

        SET @dyn_sql = CONCAT('DROP TABLE ', @write_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;
    END IF;

    SELECT @end := NOW();

    -- Advance the sync watermark so next sync run only processes new rows.
    -- Positional INSERT — matches the pattern used in generate_flat_lab_obs_v2
    -- which writes: (start_time, date_updated, table_name, completion_seconds).
    -- If your flat_log has different columns, adjust accordingly.
    INSERT INTO etl.flat_log
    VALUES (@start, NOW(), @table_version, TIMESTAMPDIFF(SECOND, @start, @end));

    SELECT CONCAT(@table_version, ' : Time to complete: ', TIMESTAMPDIFF(MINUTE, @start, @end), ' minutes');

END