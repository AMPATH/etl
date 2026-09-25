CREATE DEFINER=`openmrs_user`@`%` PROCEDURE `etl`.`generate_flat_moh_710_v1`(
    IN query_type   VARCHAR(50),
    IN queue_number INT,
    IN queue_size   INT,
    IN cycle_size   INT
)
BEGIN
    # MOH 710 Report ETL - v1.0
    # Reads from flat_obs and extracts indicator columns for MOH 710
    # (Immunization, Tetanus/HPV).
    #
    # Indicator logic translated from the etl-rest-server json-report
    # definitions at:
    #   app/reporting-framework/json-reports/moh-710/aggregations/*.json
    #
    # Translation rules (flat_obs packs each encounter's obs as
    # !!concept_id=value!! tokens joined by " ## "):
    #   o.concept_id = C AND o.value_coded = V    -> obs REGEXP "!!C=V!!"
    #   o.concept_id = C AND o.value_coded != V   -> obs REGEXP "!!C=" AND obs NOT REGEXP "!!C=V!!"
    #   o.concept_id = C AND o.value_coded IN(..) -> obs REGEXP "!!C=V1!!|!!C=V2!!|..."
    #   o.concept_id = C                          -> obs REGEXP "!!C="
    #   numeric comparisons use the first packed value of the concept,
    #   guarded by a numeric-format check (dirty values -> not counted)
    #   age bands are computed at encounter_datetime instead of the
    #   upstream CURDATE() so rows stay deterministic once written
    #
    # Each indicator column holds a per-encounter 0/1 flag (NULL when the
    # upstream base reports "1, NULL").  SUM(<col>) grouped by location_id
    # and period downstream reproduces the MOH 710 aggregate report.
    #
    # Columns whose upstream expression is a "null" placeholder are emitted
    # as NULL columns (they appear as null in the live report output too).
    # Upstream quirks are preserved verbatim and flagged with NOTE comments
    # so this table can be reconciled 1:1 against the etl-rest-server report.
    SET session sort_buffer_size     = 512000000;
    SET session group_concat_max_len = 100000;

    SET @start              = NOW();
    SET @primary_table      = 'flat_moh_710_report';
    SET @table_version      = 'flat_moh_710_report_v1.0';
    SET @total_rows_written = 0;
    SET @query_type         = query_type;
    SET @queue_number       = queue_number;
    SET @queue_size         = queue_size;
    SET @cycle_size         = cycle_size;
    SET @boundary           = '!!';
    SET @sep                = ' ## ';

    -- ------------------------------------------------------------------
    -- Primary table (all VARCHAR to avoid CAST failures on dirty data).
    -- Indicator columns are 0/1 flags, so VARCHAR(5) is used instead of
    -- the house-style VARCHAR(100): hundreds of VARCHAR(100) columns
    -- would exceed the MySQL row-size limit on utf8mb4 schemas.
    -- ------------------------------------------------------------------
    SET @dyn_sql = CONCAT('CREATE TABLE IF NOT EXISTS ', @primary_table, ' (
        date_created                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        person_id                      INT,
        uuid                           VARCHAR(100),
        encounter_id                   INT,
        encounter_datetime             DATETIME,
        encounter_type                 INT,
        location_id                    INT,
        birth_date                     DATE,
        gender                         VARCHAR(10),


        -- Immunization (encounter types 105, 106, 115, 195, 313)
        bcg_vaccine_age_less_than_1yr                   VARCHAR(5),
        bcg_vaccine_age_greater_than_1yr                VARCHAR(5),
        opv_vaccine_age_less_than_1yr                   VARCHAR(5),
        opv_vaccine_age_greater_than_1yr                VARCHAR(5),
        opv1_vaccine_age_less_than_1yr                  VARCHAR(5),
        opv1_vaccine_age_greater_than_1yr               VARCHAR(5),
        opv2_vaccine_age_less_than_1yr                  VARCHAR(5),
        opv2_vaccine_age_greater_than_1yr               VARCHAR(5),
        opv3_vaccine_age_less_than_1yr                  VARCHAR(5),
        opv3_vaccine_age_greater_than_1yr               VARCHAR(5),
        ipv1_vaccine_age_less_than_1yr                  VARCHAR(5),
        ipv1_vaccine_age_greater_than_1yr               VARCHAR(5),
        ipv2_vaccine_age_less_than_1yr                  VARCHAR(5),
        ipv2_vaccine_age_greater_than_1yr               VARCHAR(5),
        dpt_hep_vaccine1_age_less_than_1yr              VARCHAR(5),
        dpt_hep_vaccine1_age_greater_than_1yr           VARCHAR(5),
        dpt_hep_vaccine2_age_less_than_1yr              VARCHAR(5),
        dpt_hep_vaccine2_age_greater_than_1yr           VARCHAR(5),
        dpt_hep_vaccine3_age_less_than_1yr              VARCHAR(5),
        dpt_hep_vaccine3_age_greater_than_1yr           VARCHAR(5),
        pneumococal_vaccine1_age_less_than_1yr          VARCHAR(5),
        pneumococal_vaccine1_age_greater_than_1yr       VARCHAR(5),
        pneumococal_vaccine2_age_less_than_1yr          VARCHAR(5),
        pneumococal_vaccine2_age_greater_than_1yr       VARCHAR(5),
        pneumococal_vaccine3_age_less_than_1yr          VARCHAR(5),
        pneumococal_vaccine3_age_greater_than_1yr       VARCHAR(5),
        rotavirus_vaccine1_age_less_than_1yr            VARCHAR(5),
        rotavirus_vaccine1_age_greater_than_1yr         VARCHAR(5),
        rotavirus_vaccine2_age_less_than_1yr            VARCHAR(5),
        rotavirus_vaccine2_age_greater_than_1yr         VARCHAR(5),
        rotavirus_vaccine3_age_less_than_1yr            VARCHAR(5),
        rotavirus_vaccine3_age_greater_than_1yr         VARCHAR(5),
        vitaminA_vaccine_age_less_than_1yr              VARCHAR(5),
        vitaminA_vaccine_age_greater_than_1yr           VARCHAR(5),
        yellow_fever_vaccine_age_greater_than_1yr       VARCHAR(5),
        yellow_fever_vaccine_age_less_than_1yr          VARCHAR(5),
        measles_vaccine_age_greater_than_1yr            VARCHAR(5),
        measles_vaccine_age_less_than_1yr               VARCHAR(5),
        typhoid_conjugate_vaccine_age_greater_than_1yr  VARCHAR(5),
        typhoid_conjugate_vaccine_age_less_than_1yr     VARCHAR(5),
        fully_immunized_children                        VARCHAR(5),
        vitaminA_12_59_months                           VARCHAR(5),
        measles_rubella_1_2_years                       VARCHAR(5),
        measles_rubella_greater_2_years                 VARCHAR(5),
        covid_19_vaccine_12_17                          VARCHAR(5),
        covid_19_vaccine_18_59                          VARCHAR(5),
        covid_19_vaccine_60_above                       VARCHAR(5),

        -- Tetanus / HPV (encounter types 265, 313)
        tetanus_toxoid1                                 VARCHAR(5),
        tetanus_toxoid2                                 VARCHAR(5),
        tetanus_toxoid3                                 VARCHAR(5),
        tetanus_toxoid4                                 VARCHAR(5),
        tetanus_toxoid5                                 VARCHAR(5),
        hpv_vaccine1_10_14_years                        VARCHAR(5),
        hpv_vaccine2                                    VARCHAR(5),
        hpv_vaccine1_10_years                           VARCHAR(5),
        hpv_vaccine2_10_years                           VARCHAR(5),
        hpv_vaccine1_greater_10_years                   VARCHAR(5),
        hpv_vaccine2_greater_10_years                   VARCHAR(5),

        PRIMARY KEY encounter_id (encounter_id),
        INDEX person_date (person_id, encounter_datetime),
        INDEX person_uuid (uuid),
        INDEX location (location_id)
    )');
    PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

    -- ------------------------------------------------------------------
    -- BUILD mode
    -- Requires a table named `<primary_table>_build_queue` to exist and
    -- contain the person_ids to process.  For this procedure that's
    -- `flat_moh_710_report_build_queue`.
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

        CREATE TABLE IF NOT EXISTS flat_moh_710_report_sync_queue (
            person_id INT PRIMARY KEY
        );

        REPLACE INTO flat_moh_710_report_sync_queue
            (SELECT DISTINCT patient_id FROM amrs.encounter WHERE date_changed > @last_update);
        REPLACE INTO flat_moh_710_report_sync_queue
            (SELECT DISTINCT person_id FROM etl.flat_obs WHERE max_date_created > @last_update);
        REPLACE INTO flat_moh_710_report_sync_queue
            (SELECT person_id FROM amrs.person WHERE date_voided > @last_update);
        REPLACE INTO flat_moh_710_report_sync_queue
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
    -- Rows are restricted to the union of the MOH 710 section encounter
    -- types: Immunization (105,106,115,195,313), Tetanus/HPV (265,313).
    -- flat_obs synthetic obs-set rows (encounter_type 99999) are excluded.
    -- ------------------------------------------------------------------
    WHILE @queue_count > 0 DO

        SET @loop_start_time = NOW();

        DROP TEMPORARY TABLE IF EXISTS temp_queue_table;
        SET @dyn_sql = CONCAT('CREATE TEMPORARY TABLE temp_queue_table LIKE ', @queue_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT('REPLACE INTO temp_queue_table (SELECT * FROM ', @queue_table, ' LIMIT ', @cycle_size, ')');
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        DROP TEMPORARY TABLE IF EXISTS flat_moh_710_report_0;

        SET @dyn_sql = CONCAT(
            'CREATE TEMPORARY TABLE flat_moh_710_report_0
                (PRIMARY KEY (encounter_id), INDEX person_date (person_id, encounter_datetime))
            AS (SELECT
                t1.person_id,
                t2.uuid,
                t1.encounter_id,
                t1.encounter_datetime,
                t1.encounter_type,
                t1.location_id,
                t2.birthdate AS birth_date,
                t2.gender,

                -- Immunization (encounter types 105, 106, 115, 195, 313)
                -- Vaccine dose numbers upstream come from an obs self-join on
                -- concept 10888 linked by obs_group_id, which the flat_obs packing
                -- loses; here a dose = vaccine and dose number co-occurring in the
                -- same encounter (accurate unless one encounter records the same
                -- vaccine at two different doses).
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=886!!\" AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS bcg_vaccine_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=886!!\" AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, NULL) AS bcg_vaccine_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=783!!\" AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS opv_vaccine_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=783!!\" AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS opv_vaccine_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=783!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 1 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS opv1_vaccine_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=783!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 1 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS opv1_vaccine_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=783!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 2 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS opv2_vaccine_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=783!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 2 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS opv2_vaccine_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=783!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 3 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS opv3_vaccine_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=783!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 3 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS opv3_vaccine_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=10587!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 1 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS ipv1_vaccine_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=10587!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 1 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS ipv1_vaccine_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=10587!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 2 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS ipv2_vaccine_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=10587!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 2 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS ipv2_vaccine_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=781!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 1 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS dpt_hep_vaccine1_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=781!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 1 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS dpt_hep_vaccine1_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=781!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 2 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS dpt_hep_vaccine2_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=781!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 2 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS dpt_hep_vaccine2_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=781!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 3 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS dpt_hep_vaccine3_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=781!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 3 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS dpt_hep_vaccine3_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=6957!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 1 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS pneumococal_vaccine1_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=6957!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 1 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS pneumococal_vaccine1_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=6957!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 2 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS pneumococal_vaccine2_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=6957!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 2 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS pneumococal_vaccine2_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=6957!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 3 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS pneumococal_vaccine3_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=6957!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 3 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS pneumococal_vaccine3_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=10976!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 1 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS rotavirus_vaccine1_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=10976!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 1 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS rotavirus_vaccine1_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=10976!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 2 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS rotavirus_vaccine2_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=10976!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 2 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS rotavirus_vaccine2_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=10976!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 3 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS rotavirus_vaccine3_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=10976!!\" AND t1.obs REGEXP \"!!10888=\" AND SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 10888), \" ## \", 1) AS DECIMAL(10,4)) = 3 AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS rotavirus_vaccine3_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!10936=1065!!\" AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS vitaminA_vaccine_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!10936=1065!!\" AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS vitaminA_vaccine_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=5864!!\" AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS yellow_fever_vaccine_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=5864!!\" AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS yellow_fever_vaccine_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=10977!!\" AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS measles_vaccine_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=10977!!\" AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS measles_vaccine_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=12878!!\" AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) >= 12, 1, 0) AS typhoid_conjugate_vaccine_age_greater_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=12878!!\" AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 12, 1, 0) AS typhoid_conjugate_vaccine_age_less_than_1yr,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!2300=1065!!\", 1, 0) AS fully_immunized_children,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!10936=1065!!\" AND TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 12 AND 59, 1, 0) AS vitaminA_12_59_months,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=10977!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 1 AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 2, 1, 0) AS measles_rubella_1_2_years,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!984=10977!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 2, 1, 0) AS measles_rubella_greater_2_years,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!9612=12256!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 12 AND 17, 1, 0) AS covid_19_vaccine_12_17,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!9612=12256!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 18 AND 59, 1, 0) AS covid_19_vaccine_18_59,
                IF(t1.encounter_type IN (105,106,115,195,313) AND t1.obs REGEXP \"!!9612=12256!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 60, 1, 0) AS covid_19_vaccine_60_above,

                -- Tetanus / HPV (encounter types 265, 313)
                -- NOTE (upstream quirk preserved): hpv_vaccine1 and hpv_vaccine2
                -- have identical expressions upstream (concept 12059 = 1065, no
                -- dose discriminator).
                IF(t1.encounter_type IN (265,313) AND t1.obs REGEXP \"!!10404=8009!!\", 1, 0) AS tetanus_toxoid1,
                IF(t1.encounter_type IN (265,313) AND t1.obs REGEXP \"!!10404=8010!!\", 1, 0) AS tetanus_toxoid2,
                IF(t1.encounter_type IN (265,313) AND t1.obs REGEXP \"!!10404=8011!!\", 1, 0) AS tetanus_toxoid3,
                IF(t1.encounter_type IN (265,313) AND t1.obs REGEXP \"!!10404=8012!!\", 1, 0) AS tetanus_toxoid4,
                IF(t1.encounter_type IN (265,313) AND t1.obs REGEXP \"!!10404=8013!!\", 1, 0) AS tetanus_toxoid5,
                IF(t1.encounter_type IN (265,313) AND t1.obs REGEXP \"!!12059=1065!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 10 AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 15, 1, 0) AS hpv_vaccine1_10_14_years,
                IF(t1.encounter_type IN (265,313) AND t1.obs REGEXP \"!!12059=1065!!\", 1, 0) AS hpv_vaccine2,
                IF(t1.encounter_type IN (265,313) AND t1.obs REGEXP \"!!12059=1065!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) = 10, 1, 0) AS hpv_vaccine1_10_years,
                IF(t1.encounter_type IN (265,313) AND t1.obs REGEXP \"!!12059=1065!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) = 10, 1, 0) AS hpv_vaccine2_10_years,
                IF(t1.encounter_type IN (265,313) AND t1.obs REGEXP \"!!12059=1065!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) > 10, 1, 0) AS hpv_vaccine1_greater_10_years,
                IF(t1.encounter_type IN (265,313) AND t1.obs REGEXP \"!!12059=1065!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) > 10, 1, 0) AS hpv_vaccine2_greater_10_years

            FROM flat_obs t1
            JOIN temp_queue_table t3 USING (person_id)
            JOIN amrs.person t2 USING (person_id)
            WHERE t1.encounter_type IN (105,106,115,195,265,313)
            )'
        );
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT(
            'INSERT INTO ', @write_table,
            ' (person_id, uuid, encounter_id, encounter_datetime, encounter_type, location_id, birth_date, gender,
               bcg_vaccine_age_less_than_1yr, bcg_vaccine_age_greater_than_1yr, opv_vaccine_age_less_than_1yr, opv_vaccine_age_greater_than_1yr, opv1_vaccine_age_less_than_1yr,
               opv1_vaccine_age_greater_than_1yr, opv2_vaccine_age_less_than_1yr, opv2_vaccine_age_greater_than_1yr, opv3_vaccine_age_less_than_1yr, opv3_vaccine_age_greater_than_1yr,
               ipv1_vaccine_age_less_than_1yr, ipv1_vaccine_age_greater_than_1yr, ipv2_vaccine_age_less_than_1yr, ipv2_vaccine_age_greater_than_1yr, dpt_hep_vaccine1_age_less_than_1yr,
               dpt_hep_vaccine1_age_greater_than_1yr, dpt_hep_vaccine2_age_less_than_1yr, dpt_hep_vaccine2_age_greater_than_1yr, dpt_hep_vaccine3_age_less_than_1yr, dpt_hep_vaccine3_age_greater_than_1yr,
               pneumococal_vaccine1_age_less_than_1yr, pneumococal_vaccine1_age_greater_than_1yr, pneumococal_vaccine2_age_less_than_1yr, pneumococal_vaccine2_age_greater_than_1yr, pneumococal_vaccine3_age_less_than_1yr,
               pneumococal_vaccine3_age_greater_than_1yr, rotavirus_vaccine1_age_less_than_1yr, rotavirus_vaccine1_age_greater_than_1yr, rotavirus_vaccine2_age_less_than_1yr, rotavirus_vaccine2_age_greater_than_1yr,
               rotavirus_vaccine3_age_less_than_1yr, rotavirus_vaccine3_age_greater_than_1yr, vitaminA_vaccine_age_less_than_1yr, vitaminA_vaccine_age_greater_than_1yr, yellow_fever_vaccine_age_greater_than_1yr,
               yellow_fever_vaccine_age_less_than_1yr, measles_vaccine_age_greater_than_1yr, measles_vaccine_age_less_than_1yr, typhoid_conjugate_vaccine_age_greater_than_1yr, typhoid_conjugate_vaccine_age_less_than_1yr,
               fully_immunized_children, vitaminA_12_59_months, measles_rubella_1_2_years, measles_rubella_greater_2_years, covid_19_vaccine_12_17,
               covid_19_vaccine_18_59, covid_19_vaccine_60_above, tetanus_toxoid1, tetanus_toxoid2, tetanus_toxoid3,
               tetanus_toxoid4, tetanus_toxoid5, hpv_vaccine1_10_14_years, hpv_vaccine2, hpv_vaccine1_10_years,
               hpv_vaccine2_10_years, hpv_vaccine1_greater_10_years, hpv_vaccine2_greater_10_years)
            SELECT
                person_id, uuid, encounter_id, encounter_datetime, encounter_type, location_id, birth_date, gender,
               bcg_vaccine_age_less_than_1yr, bcg_vaccine_age_greater_than_1yr, opv_vaccine_age_less_than_1yr, opv_vaccine_age_greater_than_1yr, opv1_vaccine_age_less_than_1yr,
               opv1_vaccine_age_greater_than_1yr, opv2_vaccine_age_less_than_1yr, opv2_vaccine_age_greater_than_1yr, opv3_vaccine_age_less_than_1yr, opv3_vaccine_age_greater_than_1yr,
               ipv1_vaccine_age_less_than_1yr, ipv1_vaccine_age_greater_than_1yr, ipv2_vaccine_age_less_than_1yr, ipv2_vaccine_age_greater_than_1yr, dpt_hep_vaccine1_age_less_than_1yr,
               dpt_hep_vaccine1_age_greater_than_1yr, dpt_hep_vaccine2_age_less_than_1yr, dpt_hep_vaccine2_age_greater_than_1yr, dpt_hep_vaccine3_age_less_than_1yr, dpt_hep_vaccine3_age_greater_than_1yr,
               pneumococal_vaccine1_age_less_than_1yr, pneumococal_vaccine1_age_greater_than_1yr, pneumococal_vaccine2_age_less_than_1yr, pneumococal_vaccine2_age_greater_than_1yr, pneumococal_vaccine3_age_less_than_1yr,
               pneumococal_vaccine3_age_greater_than_1yr, rotavirus_vaccine1_age_less_than_1yr, rotavirus_vaccine1_age_greater_than_1yr, rotavirus_vaccine2_age_less_than_1yr, rotavirus_vaccine2_age_greater_than_1yr,
               rotavirus_vaccine3_age_less_than_1yr, rotavirus_vaccine3_age_greater_than_1yr, vitaminA_vaccine_age_less_than_1yr, vitaminA_vaccine_age_greater_than_1yr, yellow_fever_vaccine_age_greater_than_1yr,
               yellow_fever_vaccine_age_less_than_1yr, measles_vaccine_age_greater_than_1yr, measles_vaccine_age_less_than_1yr, typhoid_conjugate_vaccine_age_greater_than_1yr, typhoid_conjugate_vaccine_age_less_than_1yr,
               fully_immunized_children, vitaminA_12_59_months, measles_rubella_1_2_years, measles_rubella_greater_2_years, covid_19_vaccine_12_17,
               covid_19_vaccine_18_59, covid_19_vaccine_60_above, tetanus_toxoid1, tetanus_toxoid2, tetanus_toxoid3,
               tetanus_toxoid4, tetanus_toxoid5, hpv_vaccine1_10_14_years, hpv_vaccine2, hpv_vaccine1_10_years,
               hpv_vaccine2_10_years, hpv_vaccine1_greater_10_years, hpv_vaccine2_greater_10_years
            FROM flat_moh_710_report_0'
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
    -- BUILD mode: merge temp table -> primary
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
    -- Positional INSERT — matches the pattern used in generate_flat_moh_706_v1
    -- which writes: (start_time, date_updated, table_name, completion_seconds).
    INSERT INTO etl.flat_log
    VALUES (@start, NOW(), @table_version, TIMESTAMPDIFF(SECOND, @start, @end));

    SELECT CONCAT(@table_version, ' : Time to complete: ', TIMESTAMPDIFF(MINUTE, @start, @end), ' minutes');

END
