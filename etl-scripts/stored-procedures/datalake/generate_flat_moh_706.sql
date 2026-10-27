CREATE DEFINER=`openmrs_user`@`%` PROCEDURE `etl`.`generate_flat_moh_706_v1`(
    IN query_type   VARCHAR(50),
    IN queue_number INT,
    IN queue_size   INT,
    IN cycle_size   INT
)
BEGIN
    # MOH 706 laboratory register ETL - v3.0 (grain + raw facts only)
    #
    # Grain: one row per amrs.orders.order_id.  lab_no is the order's
    # order_number.  Results are read from the obs linked to that order
    # (obs.order_id), falling back to the encounter-level obs for the same
    # concept.  Lab obs that have no order at all are added as synthetic rows
    # (negative report_id, lab_no NULL, encounter_id NULL when the obs itself
    # has no encounter) so legacy data is not lost.
    #
    # v3.0 keeps only what nobody else can rebuild: the grain, the result
    # linkage chain, and the raw facts (test concept_id, raw result triple,
    # sample collection datetime, fhir report stamps).  Names, coded-answer
    # translation, test slotting and register layout moved to the retrieval
    # layer (lab-706-base.json / lab-706-patient-list.json in etl-rest-server),
    # which translates through etl.concept_value_map at request time.
    #
    # The per-order correlated subqueries of v2.0 were replaced by one grouped
    # pass over obs per cycle (flat_moh_706_obs_latest): a single obs scan
    # yields the latest order-linked obs, the latest obs of the same concept
    # for the order, the latest encounter-level obs, and the specimen
    # collection obs (10253, order- first then encounter-scoped).
    #
    # Prerequisites:
    #   create_concept_value_map.sql   (translation is consumed at retrieval)
    #   create_flat_moh_706.sql        (v3.0 order-grain definition)
    #   create_flat_moh_706_results.sql (v3.1 member results + concept_set_map)
    #
    # NOTE: the v1/v2 presentation columns are gone from this table; see
    #       create_flat_moh_706.sql for the full list and the rationale.
    #
    # v3.1: panel tests (CBC 1019, URINALYSIS 302, LIPID PANEL 1010) result
    # as ObsTrees - a valueless group-head obs plus one obs per analyte under
    # obs_group_id; heads/members almost never carry order_id, so the head is
    # found through ro/re (order-linked first, encounter-level fallback) and
    # kept as anchor_obs_id.  Per cycle, member capture runs grouped scans:
    # children of the anchor (Path G), plus ungrouped encounter-level analyte
    # obs for panel orders with no head (Path U, the lipid-panel recording
    # pattern).  Every report row with a value also writes a self-row, so
    # consumers read etl.flat_moh_706_report_results uniformly.  Member rows
    # are raw facts like the parent; translation stays at retrieval.
    # Migration: v3.1 has its own flat_log watermark and sync REFUSES to run
    # without one (a cold v3.1 sync would date-range scan all of amrs.obs) -
    # run build mode once after installing, with the build queue populated
    # (every build drains it; an empty slice SIGNALs rather than silently
    # logging a watermark that unlocks sync with nothing built).
    #
    # v3.2: register blank-columns release (see etl-rest-server/
    # MOH-706-REGISTER-BLANK-COLUMNS-PLAN.md):
    #   F1 - obs-only rows adopt the best-matching order (same person+concept
    #        +encounter, else closest-in-time person+concept) so the FHIR
    #        stamps, testing_officer and instructions can be populated;
    #        report_id stays -obs_id and lab_no stays NULL (additive, D8).
    #   F2 - the FHIR diagnostic report is picked deterministically
    #        (issued DESC, id DESC) instead of MAX(id).  The encounter
    #        fallback of plan F2.1 needs the fhir_diagnostic_report extract's
    #        column list confirmed first and is NOT implemented yet.
    #   F3 - Path U member capture also fires for obs-only rows and for
    #        order-backed rows without an encounter; an anti-overlap guard
    #        keeps one obs under exactly one report row.
    #   F6 - result_creator (amrs.users.user_id of the obs that carried the
    #        result) rides the report row; the register resolves it to the
    #        analysing officer name at retrieval (D11).
    #
    # v3.3: a member obs no longer also prints as its own register line -
    #       synthetic rows whose obs is captured as a member of another
    #       report row are dropped after member capture (F3b; the v3.2 grain
    #       double-counted panel analytes in the *_with_members indicators,
    #       ketones/proteins exceeded the urinalysis total on the MOH 706
    #       form).  Row counts drop for panel-heavy data; full build
    #       required.
    #
    # v3.4: nested-dictionary panels (panel -> sub-panel -> leaf) - Path G
    #       captures leaf members at any depth (skipping valueless
    #       intermediate heads via G2), refresh_concept_set_map carries
    #       transitive leaf edges only, and map members that are themselves
    #       sets are dropped.  Full build required.

    SET SESSION sort_buffer_size     = 512000000;
    SET SESSION group_concat_max_len = 100000;

    SET @start              = NOW();
    SET @primary_table      = 'flat_moh_706_report';
    SET @table_version      = 'flat_moh_706_report_v3.4';
    SET @total_rows_written = 0;
    SET @query_type         = query_type;
    SET @queue_number       = queue_number;
    SET @queue_size         = queue_size;
    SET @cycle_size         = cycle_size;

    -- ------------------------------------------------------------------
    -- Primary table (idempotent; see create_flat_moh_706.sql)
    -- ------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS flat_moh_706_report (
        date_created            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        report_id               BIGINT       NOT NULL,
        order_id                INT          DEFAULT NULL,
        lab_no                  VARCHAR(50)  DEFAULT NULL,
        person_id               INT          DEFAULT NULL,
        encounter_id            INT          DEFAULT NULL,
        visit_id                INT          DEFAULT NULL,
        test_datetime           DATETIME     DEFAULT NULL,
        encounter_type          INT          DEFAULT NULL,
        location_id             INT          DEFAULT NULL,
        concept_id              INT          DEFAULT NULL,
        result_concept_id       INT          DEFAULT NULL,
        result_numeric          DOUBLE       DEFAULT NULL,
        result_text             TEXT,
        result_datetime         DATETIME     DEFAULT NULL,
        obs_datetime            DATETIME     DEFAULT NULL,
        sample_collection_datetime DATETIME  DEFAULT NULL,
        instructions            VARCHAR(255) DEFAULT NULL,
        diagnostic_report_id    BIGINT       DEFAULT NULL,
        report_date_created     DATETIME     DEFAULT NULL,
        report_issued           DATETIME     DEFAULT NULL,
        report_date_changed     DATETIME     DEFAULT NULL,
        testing_officer         VARCHAR(255) DEFAULT NULL,
        result_creator          INT          DEFAULT NULL,
        PRIMARY KEY (report_id),
        KEY order_id (order_id),
        KEY person_date (person_id, test_datetime),
        KEY location (location_id),
        KEY encounter (encounter_id)
    );

    CREATE TABLE IF NOT EXISTS flat_moh_706_report_results (
        date_created      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                                         ON UPDATE CURRENT_TIMESTAMP,
        report_id         BIGINT NOT NULL,
        member_concept_id INT    NOT NULL,
        result_obs_id     BIGINT DEFAULT NULL,
        result_concept_id INT    DEFAULT NULL,
        result_numeric    DOUBLE DEFAULT NULL,
        result_text       TEXT,
        result_datetime   DATETIME DEFAULT NULL,
        obs_datetime      DATETIME DEFAULT NULL,
        PRIMARY KEY (report_id, member_concept_id),
        KEY member_concept (member_concept_id),
        KEY result_obs (result_obs_id)
    );

    CREATE TABLE IF NOT EXISTS etl.concept_set_map (
        concept_id  INT    NOT NULL,
        concept_set INT    NOT NULL,
        sort_weight DOUBLE DEFAULT NULL,
        PRIMARY KEY (concept_id, concept_set),
        KEY concept_set_idx (concept_set)
    );

    -- Refuse to run against a pre-v3.0 definition.  CREATE TABLE IF NOT
    -- EXISTS above is a no-op when an older definition is still present, and
    -- pre-v3.0 tables have no concept_id column.
    SELECT COUNT(*) INTO @has_concept_id
      FROM information_schema.columns
     WHERE table_schema = 'etl'
       AND table_name   = 'flat_moh_706_report'
       AND column_name  = 'concept_id';

    IF @has_concept_id = 0 THEN
        SIGNAL SQLSTATE '45000'
           SET MESSAGE_TEXT = 'flat_moh_706_report is a pre-v3.0 definition. DROP it, run create_flat_moh_706.sql, then rebuild.';
    END IF;

    -- The retrieval layer translates through this map; keep it seeded.
    SELECT COUNT(*) INTO @concept_map_rows FROM etl.concept_value_map;
    IF @concept_map_rows = 0 THEN
        CALL etl.refresh_concept_value_map();
    END IF;

    -- Panel -> member map (empirical, heads-first); keep it seeded too.
    SELECT COUNT(*) INTO @set_map_rows FROM etl.concept_set_map;
    IF @set_map_rows = 0 THEN
        CALL etl.refresh_concept_set_map(NULL);
    END IF;

    -- Concept ids that make an order a reportable lab test.  Also drives the
    -- obs-only fallback.
    DROP TEMPORARY TABLE IF EXISTS flat_moh_706_lab_concepts;
    CREATE TEMPORARY TABLE flat_moh_706_lab_concepts (
        concept_id INT NOT NULL,
        PRIMARY KEY (concept_id)
    );
    INSERT INTO flat_moh_706_lab_concepts (concept_id) VALUES
        (887), (10627), (790), (857), (1297), (655), (653), (654), (717), (785),
        (1010), (1006), (1009), (1008), (7881), (7882), (7880), (10249),
        (2340), (302), (7276), (2339), (1984), (1985), (15086), (15084),
        (6337), (9307),
        (32), (9187), (1524), (1525), (1019), (300),
        (6709), (1322), (1325), (299),
        (12881), (11868), (305), (12880), (12875), (45), (9812),
        (12078), (856), (2311),
        -- v3.2 additions: electrolytes and tumour markers (1132, 1133, 1134,
        -- 848, 10251, 10250, 10570, 6132, 6124), parasitology/haematology/
        -- serology tests (1526, 21, 9008, 1327, 1032, 15092, 12019) and the
        -- bacteriology / cytology specimen concepts (49547, 1005, 10051,
        -- 7190, 49245, 6902).  6132 serves both 2.29 Proteins and 5.16 CSF.
        (1132), (1133), (1134), (848), (10251), (10250), (10570),
        (6132), (6124), (1526), (21), (9008), (1327),
        (49547), (1005), (10051), (7190), (49245), (6902),
        (1032), (15092), (12019),
        -- New reportable concepts (2026-10-23, additional_concepts.tsv):
        -- 1.10 Bacteria (53247), 2.24/2.26 tumour markers CA 15-3 / CA 125
        -- (53248, 53249), 4.8 Coagulation profile (53250), 5.2-5.8
        -- bacteriology specimens (53311 Pus swabs, 53312 Throat swab, 53313
        -- Rectal swab, 53314 Water, 53315 Food), 5.12-5.28 epidemic-prone
        -- organisms (53269 Shigella dysenteriae type 1, 53270
        -- E. coli O157:H7, 53316 V. cholerae O1, 53279-53284
        -- N. meningitidis A/B/C/W135/Y/indeterminate, 53285
        -- S. pneumoniae, 53286 H. influenzae b, 53287 B. anthracis, 53288
        -- Y. pestis), 6.4-6.25 histology/cytology specimen sites
        -- (53289-53307; 49245 6.10 CSF is already listed above) and 7.8
        -- Hepatitis A (53309).  The TSV also maps 5.15 V. cholerae O139 to
        -- 53316 - one concept id, one register line, so O139 stays unlisted
        -- until its own concept exists.  Historical orders of these concepts
        -- need one full build backfill (sync will not requeue them).
        (53247),
        (53248), (53249), (53250),
        (53311), (53312), (53313), (53314), (53315),
        (53269), (53270), (53316),
        (53279), (53280), (53281), (53282), (53283), (53284),
        (53285), (53286), (53287), (53288),
        (53289), (53290), (53291), (53292), (53293), (53294), (53295),
        (53296), (53297), (53298), (53299), (53300), (53301), (53302),
        (53303), (53304), (53305), (53306), (53307),
        (53309),
        -- 5.29 TB SMEAR (coded; graded answers - SCANTY/1+/2+/3+ - are all
        -- reportable findings, so positivity is "not negative" downstream)
        (307),
        -- 4.3 Hemoglobin A1c (2026-10-27, register recheck): numeric HbA1c
        -- results for the haematology register row; the Pre-diabetes /
        -- Diabetes banding is applied downstream in the lab-706 templates.
        -- Historical orders need one full build backfill (sync will not
        -- requeue them).
        (6126),
        -- 1.5 Urine microscopy (2026-10-27, register recheck): coded
        -- URINALYSIS, MICROSCOPIC.  Results surface through the widened
        -- total_urine_microscopy / positive_urine_microscopy flags -
        -- standalone rows via this concept and member rows under 302
        -- through the results table.  Historical orders need one full
        -- build backfill (sync will not requeue them).
        (1986),
        -- 2.3 Renal function test (2026-10-27, register recheck): results
        -- recorded as numeric values (urea-style in the oncology flows);
        -- the downstream total flag uses the 4-way has-result check so any
        -- answer datatype counts.  Historical orders need one full build
        -- backfill (sync will not requeue them).
        (6898),
        -- 2.8 Liver function test (2026-10-27, register recheck): datatype
        -- unused elsewhere in the codebase, so the downstream slot carries
        -- COALESCE(result_concept_id, result_numeric) (tb_culture
        -- precedent) and the total flag uses the 4-way has-result check;
        -- treated as a probable panel like 6898 (its member analytes 653,
        -- 654, 655, 1297, 717, 785, 848 are already reportable), so the
        -- total also counts any member result.  Historical orders need one
        -- full build backfill (sync will not requeue them).
        (953),
        -- Stool examination (2026-10-27, register recheck): concept 53310.
        -- The owner correction replaces 304, which was erroneously tied to
        -- 5.16 CSF in the dictionary and is not stool.  Datatype unused
        -- elsewhere in the codebase, so the downstream slot carries
        -- COALESCE(result_concept_id, result_numeric) (tb_culture
        -- precedent) and the total flag uses the 4-way has-result check;
        -- treated as a probable panel like 6898/953 (stool results are
        -- commonly recorded as grouped analyte obs), so the total also
        -- counts any member result.  Historical orders need one full build
        -- backfill (sync will not requeue them).
        (53310);

    -- ------------------------------------------------------------------
    -- BUILD mode
    -- ------------------------------------------------------------------
    IF (@query_type = 'build') THEN
        SELECT CONCAT('BUILDING ', @primary_table, '..........................................');
        SET @write_table = CONCAT(@primary_table, '_temp_', @queue_number);
        SET @build_queue = CONCAT(@primary_table, '_build_queue');
        SET @queue_table = CONCAT(@build_queue, '_', @queue_number);

        -- A crashed earlier run leaves its per-worker temp table behind;
        -- DROP first so a rerun starts clean instead of inheriting stale
        -- rows or an outdated definition.
        SET @dyn_sql = CONCAT('DROP TABLE IF EXISTS ', @write_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT('CREATE TABLE ', @write_table, ' LIKE ', @primary_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @results_write = CONCAT(@primary_table, '_results_temp_', @queue_number);
        SET @dyn_sql = CONCAT('DROP TABLE IF EXISTS ', @results_write);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;
        SET @dyn_sql = CONCAT('CREATE TABLE ', @results_write, ' LIKE flat_moh_706_report_results');
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        -- A crashed earlier run also leaves its per-worker queue slice
        -- behind, drained empty by the cycle loop; CREATE TABLE IF NOT
        -- EXISTS would then no-op and the empty-slice SIGNAL below would
        -- fire even though the build queue still has rows.  DROP first so
        -- a rerun takes a fresh slice, exactly as for the write tables.
        SET @dyn_sql = CONCAT('DROP TABLE IF EXISTS ', @queue_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT(
            'CREATE TABLE ', @queue_table,
            ' (person_id INT PRIMARY KEY) AS (SELECT * FROM ', @build_queue,
            ' LIMIT ', @queue_size, ')'
        );
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        -- An empty slice must fail loudly, not no-op: a silent empty pass
        -- still writes the v3.1 watermark and unlocks sync, while the
        -- historical obs-only panel groups never change again - sync can
        -- never requeue them, so the results table would stay empty forever.
        SET @dyn_sql = CONCAT('SELECT COUNT(*) INTO @slice_count FROM ', @queue_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        IF @slice_count = 0 THEN
            SIGNAL SQLSTATE '45000'
               SET MESSAGE_TEXT = 'flat_moh_706_report build queue slice is empty - INSERT person_ids INTO etl.flat_moh_706_report_build_queue first (every build drains it).';
        END IF;

        SET @dyn_sql = CONCAT('DELETE t1 FROM ', @build_queue, ' t1 JOIN ', @queue_table, ' t2 USING (person_id)');
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;
    END IF;

    -- ------------------------------------------------------------------
    -- SYNC mode
    -- ------------------------------------------------------------------
    IF (@query_type = 'sync') THEN
        SELECT CONCAT('SYNCING ', @primary_table, '..........................................');
        SET @write_table   = @primary_table;
        SET @results_write = 'flat_moh_706_report_results';
        SET @queue_table   = CONCAT(@primary_table, '_sync_queue');
        SET @last_update = NULL;

        SELECT MAX(date_updated) INTO @last_update
          FROM etl.flat_log
         WHERE table_name = @table_version;

        -- v3.1+ migration guard.  A fresh install has no watermark and
        -- syncing from 1900 would date-range scan all of amrs.obs (member
        -- obs carry no order_id and must be watched by group link instead).
        -- Run build mode once; it logs a v3.4 row and unlocks sync.
        IF @last_update IS NULL THEN
            SIGNAL SQLSTATE '45000'
               SET MESSAGE_TEXT = 'No watermark for flat_moh_706_report_v3.4 - run generate_flat_moh_706_v1 in build mode first.';
        END IF;

        -- The comparisons use >= rather than >.  flat_log.date_updated has
        -- second precision, so a row written in the same second the previous
        -- run finished would otherwise be skipped forever.  Re-processing the
        -- boundary second is safe: the person's rows are deleted and rebuilt.

        CREATE TABLE IF NOT EXISTS flat_moh_706_report_sync_queue (
            person_id INT PRIMARY KEY
        );

        -- Orders created/stopped/voided since the watermark.  The analytics
        -- replica's amrs.orders extract has no date_changed column (OpenMRS
        -- core has it, the extract does not), so watch the mutation columns
        -- it does have: date_stopped catches discontinuations and
        -- date_voided catches voided orders.  Plain edits (e.g. instruction
        -- text changes) leave no trace in the extract and cannot requeue.
        REPLACE INTO flat_moh_706_report_sync_queue
            (SELECT DISTINCT patient_id
               FROM amrs.orders
              WHERE date_created >= @last_update
                 OR date_stopped >= @last_update
                 OR date_voided >= @last_update);

        -- Lab obs changed since the watermark: result obs carry an order_id,
        -- and specimen-collection obs (10253) feed test_datetime.  Everything
        -- else the v2.0 sync watched (person/address/telephone, encounter,
        -- encounter_diagnosis) is derived at retrieval time now, so those
        -- sources were trimmed from this queue.
        REPLACE INTO flat_moh_706_report_sync_queue
            (SELECT DISTINCT person_id
               FROM amrs.obs
              WHERE date_created >= @last_update
                AND (order_id IS NOT NULL OR concept_id = 10253));

        -- Panel member obs and group heads carry no order_id (v3.1): watch
        -- grouped obs and panel-head concepts or panel results never
        -- requeue.  Both are date_created range scans, small windows.
        REPLACE INTO flat_moh_706_report_sync_queue
            (SELECT DISTINCT person_id
               FROM amrs.obs
              WHERE date_created >= @last_update
                AND obs_group_id IS NOT NULL);

        REPLACE INTO flat_moh_706_report_sync_queue
            (SELECT DISTINCT x.person_id
               FROM amrs.obs x
               JOIN etl.concept_set_map cs ON cs.concept_id = x.concept_id
              WHERE x.date_created >= @last_update);

        REPLACE INTO flat_moh_706_report_sync_queue
            (SELECT DISTINCT subject_id
               FROM amrs.fhir_diagnostic_report
              WHERE date_changed >= @last_update);
    END IF;

    -- Children first: member results hang off report rows.
    SET @dyn_sql = CONCAT('DELETE rr FROM flat_moh_706_report_results rr
         JOIN ', @primary_table, ' r ON rr.report_id = r.report_id
         JOIN ', @queue_table, ' q USING (person_id)');
    PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

    -- Delete stale rows for queued persons.
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

        -- --------------------------------------------------------------
        -- One obs scan per cycle: for every queued lab order, the latest
        -- order-linked obs of the same concept (ro), the latest obs of the
        -- same concept regardless of link (ra), the latest encounter-level
        -- obs with no order link (re), and the SPECIMEN COLLECTION DATE
        -- (10253) obs, order-matched first then encounter-scoped.
        --
        -- v2.0 ran these as three correlated MAX(obs_id) subqueries per
        -- order row; grouping the same scan once is the single biggest ETL
        -- win in this procedure.
        -- --------------------------------------------------------------
        DROP TEMPORARY TABLE IF EXISTS flat_moh_706_obs_latest;
        CREATE TEMPORARY TABLE flat_moh_706_obs_latest (
            order_id            BIGINT NOT NULL,
            ro_obs_id           BIGINT DEFAULT NULL,
            ra_obs_id           BIGINT DEFAULT NULL,
            re_obs_id           BIGINT DEFAULT NULL,
            sample_order_obs_id BIGINT DEFAULT NULL,
            sample_enc_obs_id   BIGINT DEFAULT NULL,
            PRIMARY KEY (order_id)
        );

        INSERT INTO flat_moh_706_obs_latest
            (order_id, ro_obs_id, ra_obs_id, re_obs_id,
             sample_order_obs_id, sample_enc_obs_id)
        SELECT
            o.order_id,
            MAX(IF(x.order_id = o.order_id AND x.concept_id = o.concept_id,
                   x.obs_id, NULL)) AS ro_obs_id,
            MAX(IF(x.order_id = o.order_id,
                   x.obs_id, NULL)) AS ra_obs_id,
            MAX(IF(x.order_id IS NULL
                   AND x.encounter_id = o.encounter_id
                   AND x.concept_id = o.concept_id,
                   x.obs_id, NULL)) AS re_obs_id,
            MAX(IF(x.concept_id = 10253 AND x.order_id = o.order_id,
                   x.obs_id, NULL)) AS sample_order_obs_id,
            MAX(IF(x.concept_id = 10253 AND x.order_id IS NULL
                   AND x.encounter_id = o.encounter_id,
                   x.obs_id, NULL)) AS sample_enc_obs_id
        FROM amrs.orders o
        JOIN temp_queue_table q ON q.person_id = o.patient_id
        JOIN flat_moh_706_lab_concepts lc ON lc.concept_id = o.concept_id
        JOIN amrs.obs x ON x.person_id = o.patient_id AND x.voided = 0
        WHERE o.voided = 0
        GROUP BY o.order_id;

        -- --------------------------------------------------------------
        -- Order rows + resolved raw result
        -- --------------------------------------------------------------
        -- Explicit column list rather than CREATE TABLE ... AS SELECT: CTAS
        -- inherits the NOT NULL attribute from amrs.orders columns, so the
        -- inherited encounter_id NOT NULL made the obs-only insert below fail
        -- with ERROR 1048 on lab obs that genuinely have no encounter
        -- (encounter-less results).  Encounter-less obs are kept, with
        -- encounter_id NULL, exactly like the report table allows.
        DROP TEMPORARY TABLE IF EXISTS flat_moh_706_order_0;
        CREATE TEMPORARY TABLE flat_moh_706_order_0 (
            order_id                   BIGINT       NOT NULL,
            order_number               VARCHAR(50)  DEFAULT NULL,
            person_id                  INT          DEFAULT NULL,
            encounter_id               INT          DEFAULT NULL,
            concept_id                 INT          DEFAULT NULL,
            date_activated             DATETIME     DEFAULT NULL,
            instructions               VARCHAR(255) DEFAULT NULL,
            value_coded                INT          DEFAULT NULL,
            value_numeric              DOUBLE       DEFAULT NULL,
            value_text                 TEXT,
            value_datetime             DATETIME     DEFAULT NULL,
            obs_datetime               DATETIME     DEFAULT NULL,
            sample_collection_datetime DATETIME     DEFAULT NULL,
            anchor_obs_id              BIGINT       DEFAULT NULL,
            diagnostic_report_id       BIGINT       DEFAULT NULL,
            report_date_created        DATETIME     DEFAULT NULL,
            report_issued              DATETIME     DEFAULT NULL,
            report_date_changed        DATETIME     DEFAULT NULL,
            testing_officer            VARCHAR(255) DEFAULT NULL,
            result_creator             INT          DEFAULT NULL,
            adopted_order_id           BIGINT       DEFAULT NULL,
            PRIMARY KEY (order_id),
            INDEX person_enc (person_id, encounter_id)
        );

        INSERT INTO flat_moh_706_order_0
            (order_id, order_number, person_id, encounter_id, concept_id,
             date_activated, instructions, value_coded, value_numeric,
             value_text, value_datetime, obs_datetime,
             sample_collection_datetime, anchor_obs_id, diagnostic_report_id,
             report_date_created, report_issued, report_date_changed,
             testing_officer, result_creator, adopted_order_id)
        SELECT
                o.order_id,
                o.order_number,
                o.patient_id AS person_id,
                o.encounter_id,
                o.concept_id,
                o.date_activated,
                o.instructions,

                -- Result priority: obs tied to this order and concept, then any
                -- obs tied to this order, then the encounter-level obs for the
                -- same concept (legacy rows with no order link).
                COALESCE(ro.value_coded, ra.value_coded, re.value_coded) AS value_coded,
                COALESCE(ro.value_numeric, ra.value_numeric, re.value_numeric) AS value_numeric,
                COALESCE(ro.value_text, ra.value_text, re.value_text) AS value_text,
                COALESCE(ro.value_datetime, ra.value_datetime, re.value_datetime) AS value_datetime,
                COALESCE(ro.obs_datetime, ra.obs_datetime, re.obs_datetime) AS obs_datetime,

                -- SPECIMEN COLLECTION DATE (10253): order-matched first.
                COALESCE(s_o.value_datetime, s_e.value_datetime) AS sample_collection_datetime,

                -- v3.1 group-head obs anchoring panel-member capture.  ro
                -- almost never fires for panels (heads rarely carry the
                -- order_id); the encounter-level head (re) does the work.
                COALESCE(latest.ro_obs_id, latest.re_obs_id) AS anchor_obs_id,

                f.diagnostic_report_id,
                f.date_created  AS report_date_created,
                f.issued        AS report_issued,
                f.date_changed  AS report_date_changed,

                (SELECT CONCAT_WS(' ', pn.given_name,
                                  NULLIF(pn.middle_name, ''),
                                  pn.family_name)
                   FROM amrs.fhir_diagnostic_report_performers fp
                   JOIN amrs.provider pr
                     ON pr.provider_id = fp.provider_id
                   JOIN amrs.person_name pn
                     ON pn.person_id = pr.person_id
                    AND pn.voided = 0
                  WHERE fp.diagnostic_report_id = f.diagnostic_report_id
                  ORDER BY pn.preferred DESC, pn.person_name_id DESC
                  LIMIT 1) AS testing_officer,

                -- v3.2 (F6/D11): who entered the result.  Same obs lineage
                -- as the value COALESCE above; for panels this is almost
                -- always the head-obs creator (re).
                COALESCE(ro.creator, ra.creator, re.creator) AS result_creator,

                NULL AS adopted_order_id

            FROM amrs.orders o
            JOIN temp_queue_table q ON q.person_id = o.patient_id
            JOIN flat_moh_706_lab_concepts lc ON lc.concept_id = o.concept_id

            LEFT JOIN flat_moh_706_obs_latest latest
              ON latest.order_id = o.order_id
            LEFT JOIN amrs.obs ro ON ro.obs_id = latest.ro_obs_id
            LEFT JOIN amrs.obs ra ON ra.obs_id = latest.ra_obs_id
            LEFT JOIN amrs.obs re ON re.obs_id = latest.re_obs_id
            LEFT JOIN amrs.obs s_o ON s_o.obs_id = latest.sample_order_obs_id
            LEFT JOIN amrs.obs s_e ON s_e.obs_id = latest.sample_enc_obs_id

            -- v3.2 (F2): deterministic pick instead of MAX(id) - a report
            -- edited later must not lose to an older sibling on id order.
            -- The encounter fallback (plan F2.1) is deliberately not here:
            -- its column names on the amrs.fhir_diagnostic_report extract
            -- are unconfirmed; add it after Q12-style verification.
            LEFT JOIN amrs.fhir_diagnostic_report f
              ON f.diagnostic_report_id = (
                    SELECT fsr.diagnostic_report_id
                      FROM amrs.fhir_diagnostic_report_service_request fsr
                      JOIN amrs.fhir_diagnostic_report f2
                        ON f2.diagnostic_report_id = fsr.diagnostic_report_id
                     WHERE fsr.order_id = o.order_id
                     ORDER BY f2.issued DESC, fsr.diagnostic_report_id DESC
                     LIMIT 1)

            WHERE o.voided = 0;

        -- Legacy lab obs with no order at all.  Synthetic negative order_id
        -- keeps report_id unique; lab_no stays NULL (no order exists), and so
        -- does encounter_id when the obs itself has no encounter.
        --
        -- v3.2 (F1, D8 additive): the synthetic row ADOPTS the best-matching
        -- order so the FHIR stamps, testing_officer and instructions have a
        -- source.  report_id stays -obs_id and lab_no stays NULL - the obs
        -- has no order number of its own, and the adopted order's number
        -- must not be misattributed to it.  Adoption preference: a non-voided
        -- order of the same person + concept + encounter (NULL-safe; for
        -- encounter-bearing obs the NOT EXISTS guard above already excluded
        -- those, so this tier only fires for encounter-less obs), else the
        -- same person + concept closest in time to the obs (tie: highest
        -- order id).  Double-count risk with the order row's own register
        -- entry is handled at retrieval: billing aggregates the whole
        -- encounter (plan F5).
        DROP TEMPORARY TABLE IF EXISTS flat_moh_706_obs_only;
        CREATE TEMPORARY TABLE flat_moh_706_obs_only (
            obs_id           BIGINT NOT NULL,
            person_id        INT    DEFAULT NULL,
            encounter_id     INT    DEFAULT NULL,
            concept_id       INT    DEFAULT NULL,
            value_coded      INT    DEFAULT NULL,
            value_numeric    DOUBLE DEFAULT NULL,
            value_text       TEXT,
            value_datetime   DATETIME DEFAULT NULL,
            obs_datetime     DATETIME DEFAULT NULL,
            creator          INT    DEFAULT NULL,
            adopted_order_id BIGINT DEFAULT NULL,
            PRIMARY KEY (obs_id),
            INDEX adopted (adopted_order_id)
        );

        INSERT INTO flat_moh_706_obs_only
            (obs_id, person_id, encounter_id, concept_id,
             value_coded, value_numeric, value_text, value_datetime,
             obs_datetime, creator, adopted_order_id)
        SELECT
            x.obs_id,
            x.person_id,
            x.encounter_id,
            x.concept_id,
            x.value_coded,
            x.value_numeric,
            x.value_text,
            x.value_datetime,
            x.obs_datetime,
            x.creator,
            COALESCE(
                (SELECT o2.order_id
                   FROM amrs.orders o2
                  WHERE o2.patient_id    = x.person_id
                    AND o2.concept_id    = x.concept_id
                    AND o2.voided        = 0
                    AND o2.encounter_id <=> x.encounter_id
                  ORDER BY o2.date_activated DESC, o2.order_id DESC
                  LIMIT 1),
                (SELECT o3.order_id
                   FROM amrs.orders o3
                  WHERE o3.patient_id = x.person_id
                    AND o3.concept_id = x.concept_id
                    AND o3.voided     = 0
                  ORDER BY ABS(TIMESTAMPDIFF(SECOND, o3.date_activated,
                                             x.obs_datetime)) IS NULL,
                           ABS(TIMESTAMPDIFF(SECOND, o3.date_activated,
                                             x.obs_datetime)),
                           o3.order_id DESC
                  LIMIT 1))
        FROM amrs.obs x
        JOIN temp_queue_table q ON q.person_id = x.person_id
        JOIN flat_moh_706_lab_concepts lc ON lc.concept_id = x.concept_id
        WHERE x.voided = 0
          AND x.order_id IS NULL
          AND NOT EXISTS (
                SELECT 1
                  FROM amrs.orders o2
                 WHERE o2.patient_id = x.person_id
                   AND o2.concept_id = x.concept_id
                   AND o2.voided = 0
                   AND (o2.encounter_id = x.encounter_id
                        OR (o2.encounter_id IS NULL AND x.encounter_id IS NULL)));

        INSERT INTO flat_moh_706_order_0
            (order_id, order_number, person_id, encounter_id, concept_id,
             date_activated, instructions, value_coded, value_numeric,
             value_text, value_datetime, obs_datetime,
             sample_collection_datetime, anchor_obs_id, diagnostic_report_id,
             report_date_created, report_issued, report_date_changed,
             testing_officer, result_creator, adopted_order_id)
        SELECT
            -x.obs_id,
            NULL,
            x.person_id,
            x.encounter_id,
            x.concept_id,
            x.obs_datetime,
            ao.instructions,
            x.value_coded,
            x.value_numeric,
            x.value_text,
            x.value_datetime,
            x.obs_datetime,
            x.obs_datetime,
            -- v3.2 (F3): anchor only when this obs really is a group head
            -- (has children) - otherwise leave NULL so Path U can capture the
            -- ungrouped encounter-level analyte pattern for obs-only panels.
            CASE WHEN EXISTS (SELECT 1
                                FROM amrs.obs ch
                               WHERE ch.obs_group_id = x.obs_id)
                 THEN x.obs_id ELSE NULL END,
            f.diagnostic_report_id,
            f.date_created  AS report_date_created,
            f.issued        AS report_issued,
            f.date_changed  AS report_date_changed,
            (SELECT CONCAT_WS(' ', pn.given_name,
                              NULLIF(pn.middle_name, ''),
                              pn.family_name)
               FROM amrs.fhir_diagnostic_report_performers fp
               JOIN amrs.provider pr
                 ON pr.provider_id = fp.provider_id
               JOIN amrs.person_name pn
                 ON pn.person_id = pr.person_id
                AND pn.voided = 0
              WHERE fp.diagnostic_report_id = f.diagnostic_report_id
              ORDER BY pn.preferred DESC, pn.person_name_id DESC
              LIMIT 1),
            x.creator,
            x.adopted_order_id
        FROM flat_moh_706_obs_only x
        LEFT JOIN amrs.orders ao ON ao.order_id = x.adopted_order_id
        LEFT JOIN amrs.fhir_diagnostic_report f
          ON f.diagnostic_report_id = (
                SELECT fsr.diagnostic_report_id
                  FROM amrs.fhir_diagnostic_report_service_request fsr
                  JOIN amrs.fhir_diagnostic_report f2
                    ON f2.diagnostic_report_id = fsr.diagnostic_report_id
                 WHERE fsr.order_id = x.adopted_order_id
                 ORDER BY f2.issued DESC, fsr.diagnostic_report_id DESC
                 LIMIT 1);

        -- --------------------------------------------------------------
        -- Panel members (v3.1): grain report_id x analyte.
        --
        -- Path G: children of the anchored group-head obs (one hop of the
        -- ObsTree; depth-3 nesting was ruled out by discovery Q1 - all
        -- member concepts are leaf datatypes).  Path U: ungrouped
        -- encounter-level analyte obs for panel orders with NO head obs
        -- at all (the LIPID PANEL recording pattern; gated on anchor NULL
        -- so it never fires for CBC).  Both are latest-per-member via
        -- MAX(obs_id); INSERT IGNORE makes the two paths disjoint-safe.
        -- --------------------------------------------------------------
        DROP TEMPORARY TABLE IF EXISTS flat_moh_706_members;
        CREATE TEMPORARY TABLE flat_moh_706_members (
            report_id         BIGINT NOT NULL,
            member_concept_id INT    NOT NULL,
            obs_id            BIGINT NOT NULL,
            PRIMARY KEY (report_id, member_concept_id)
        );

        INSERT IGNORE INTO flat_moh_706_members (report_id, member_concept_id, obs_id)
        SELECT t.order_id, c.concept_id, MAX(c.obs_id)
        FROM flat_moh_706_order_0 t
        JOIN amrs.obs c ON c.obs_group_id = t.anchor_obs_id AND c.voided = 0
        WHERE t.anchor_obs_id IS NOT NULL
          -- v3.4 (F3c): leaf members only - intermediate sub-panel heads are
          -- valueless and their leaves arrive via G2 below; capturing heads
          -- produced empty "Not done" members and starved F3b of the real
          -- leaf memberships (nested-dictionary panels never released their
          -- standalone analyte rows, inflating *_with_members past the
          -- panel total).
          AND NOT EXISTS (SELECT 1 FROM amrs.concept hc
                           WHERE hc.concept_id = c.concept_id AND hc.is_set = 1)
        GROUP BY t.order_id, c.concept_id;

        -- v3.4 (F3c) Path G2: leaves under intermediate sub-panel heads
        -- (the dictionary nests panels - panel -> sub-panel -> leaf - and
        -- one obs_group_id hop never reached them).
        INSERT IGNORE INTO flat_moh_706_members (report_id, member_concept_id, obs_id)
        SELECT t.order_id, c2.concept_id, MAX(c2.obs_id)
        FROM flat_moh_706_order_0 t
        JOIN amrs.obs sub ON sub.obs_group_id = t.anchor_obs_id AND sub.voided = 0
        JOIN amrs.obs c2 ON c2.obs_group_id = sub.obs_id AND c2.voided = 0
        WHERE t.anchor_obs_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM amrs.concept hc2
                           WHERE hc2.concept_id = c2.concept_id AND hc2.is_set = 1)
        GROUP BY t.order_id, c2.concept_id;

        INSERT IGNORE INTO flat_moh_706_members (report_id, member_concept_id, obs_id)
        SELECT t.order_id, x.concept_id, MAX(x.obs_id)
        FROM flat_moh_706_order_0 t
        JOIN etl.concept_set_map cs ON cs.concept_id = t.concept_id
        JOIN amrs.obs x ON x.person_id   = t.person_id
                      AND x.voided       = 0
                      AND x.concept_id   = cs.concept_set
                      AND x.obs_group_id IS NULL
                      AND (x.encounter_id = t.encounter_id
                           OR x.order_id = t.order_id)
        WHERE t.anchor_obs_id IS NULL
          -- v3.2 (F3): also fire for obs-only rows (their anchor is their
          -- own obs id, so this gate used to exclude them) and for
          -- order-backed rows without an encounter (the x.order_id arm of
          -- the join covers those).  Encounter-less obs-only rows still
          -- cannot resolve - both join arms compare NULL - and stay out.
          AND (t.encounter_id IS NOT NULL OR t.order_id > 0)
        GROUP BY t.order_id, x.concept_id;

        -- v3.2 (F3): anti-overlap guard.  Paths G and U probe different
        -- anchors, so the same member obs can land under two report rows
        -- (e.g. a synthetic panel row and its adopted order's row).  Keep
        -- the order-backed report (positive id); among same-sign ids keep
        -- the earliest order / earliest obs.
        DELETE m FROM flat_moh_706_members m
        JOIN flat_moh_706_members k
          ON k.obs_id = m.obs_id
         AND k.report_id <> m.report_id
         AND (   (k.report_id > 0 AND m.report_id < 0)
              OR (SIGN(k.report_id) = SIGN(m.report_id)
                  AND ABS(k.report_id) < ABS(m.report_id)));

        -- v3.3 (F3b): a member obs must not ALSO print as its own register
        -- line.  Grouped panel members and Path-U ungrouped analytes are
        -- reportable concepts with no order, so the obs-only branch above
        -- gives each of them a standalone synthetic row as well - the same
        -- result then surfaces twice (panel row Results cell + own row) and
        -- the *_with_members indicators double-count it (ketones/proteins
        -- totals exceeded the urinalysis panel total on the MOH 706 form).
        -- Drop synthetic rows whose obs was captured as a member of a
        -- different report row; the panel row keeps the result.
        DELETE t FROM flat_moh_706_order_0 t
        JOIN flat_moh_706_members m
          ON m.obs_id = -t.order_id
         AND m.report_id <> t.order_id
        WHERE t.order_id < 0;

        SET @dyn_sql = CONCAT('REPLACE INTO ', @results_write, '
            (report_id, member_concept_id, result_obs_id, result_concept_id,
             result_numeric, result_text, result_datetime, obs_datetime)
        SELECT m.report_id, m.member_concept_id, m.obs_id, x.value_coded,
               x.value_numeric, x.value_text, x.value_datetime, x.obs_datetime
        FROM flat_moh_706_members m
        JOIN amrs.obs x ON x.obs_id = m.obs_id');
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        -- Self-row: every report row carrying a value also writes
        -- (report_id, own concept), so plain tests and panels consume
        -- identically.  result_obs_id stays NULL here - lineage for the
        -- self value is the parent row's resolved result.
        SET @dyn_sql = CONCAT('REPLACE INTO ', @results_write, '
            (report_id, member_concept_id, result_obs_id, result_concept_id,
             result_numeric, result_text, result_datetime, obs_datetime)
        SELECT t.order_id, t.concept_id, NULL, t.value_coded,
               t.value_numeric, t.value_text, t.value_datetime, t.obs_datetime
        FROM flat_moh_706_order_0 t
        WHERE t.value_coded     IS NOT NULL
           OR t.value_numeric  IS NOT NULL
           OR t.value_text     IS NOT NULL
           OR t.value_datetime IS NOT NULL');
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        -- --------------------------------------------------------------
        -- Assemble and write the report rows
        -- --------------------------------------------------------------
        SET @dyn_sql = CONCAT('INSERT INTO ', @write_table, '
            (report_id, order_id, lab_no, person_id, encounter_id, visit_id,
             test_datetime, encounter_type, location_id, concept_id,
             result_concept_id, result_numeric, result_text, result_datetime,
             obs_datetime, sample_collection_datetime, instructions,
             diagnostic_report_id, report_date_created, report_issued,
             report_date_changed, testing_officer, result_creator)
        SELECT
            o.order_id,
            -- v3.2 (F1): synthetic rows carry their adopted order here (the
            -- order-keyed retrieval fallbacks - billing, FHIR dates, orderer
            -- - resolve through it); report_id stays negative.
            IF(o.order_id > 0, o.order_id, o.adopted_order_id),
            o.order_number,
            o.person_id,
            o.encounter_id,
            e.visit_id,
            COALESCE(o.sample_collection_datetime, o.date_activated, e.encounter_datetime),
            e.encounter_type,
            e.location_id,
            o.concept_id,
            o.value_coded,
            o.value_numeric,
            o.value_text,
            o.value_datetime,
            o.obs_datetime,
            o.sample_collection_datetime,
            o.instructions,
            o.diagnostic_report_id,
            o.report_date_created,
            o.report_issued,
            o.report_date_changed,
            o.testing_officer,
            o.result_creator
        FROM flat_moh_706_order_0 o
        LEFT JOIN amrs.encounter e ON e.encounter_id = o.encounter_id');
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

    -- Remove voided persons (children first).
    -- This sweep is GLOBAL (whole primary table), so in parallel builds every
    -- worker would otherwise serialise on each other's locks -> ER_LOCK_WAIT_
    -- TIMEOUT (1205).  Only the queue_number = 1 worker sweeps on build; sync
    -- always sweeps.
    IF @query_type <> 'build' OR @queue_number = 1 THEN
        SET @dyn_sql = CONCAT(
            'DELETE rr FROM flat_moh_706_report_results rr
             JOIN ', @primary_table, ' r ON rr.report_id = r.report_id
             JOIN amrs.person t2 ON t2.person_id = r.person_id
             WHERE t2.voided = 1'
        );
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT(
        'DELETE t1 FROM ', @primary_table, ' t1
         JOIN amrs.person t2 USING (person_id)
         WHERE t2.voided = 1'
        );
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;
    END IF;

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

        SET @dyn_sql = CONCAT('REPLACE INTO flat_moh_706_report_results (SELECT * FROM ', @results_write, ')');
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @finish_write  = NOW();
        SET @time_to_write = TIMESTAMPDIFF(SECOND, @start_write, @finish_write);
        SELECT CONCAT(@finish_write, ' : Completed. Time to write: ', @time_to_write, ' seconds');

        SET @dyn_sql = CONCAT('DROP TABLE ', @write_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT('DROP TABLE ', @results_write);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;
    END IF;

    SET @end := NOW();

    INSERT INTO etl.flat_log
    VALUES (@start, NOW(), @table_version, TIMESTAMPDIFF(SECOND, @start, @end));

    SELECT CONCAT(@table_version, ' : Time to complete: ', TIMESTAMPDIFF(MINUTE, @start, @end), ' minutes');

END
