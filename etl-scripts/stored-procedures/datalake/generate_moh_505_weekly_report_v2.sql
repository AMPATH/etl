CREATE DEFINER=`openmrs_user`@`%` PROCEDURE `etl`.`generate_moh_505_weekly_report_v2`(
    IN query_type varchar(50),
    IN queue_number int,
    IN queue_size int,
    IN cycle_size int,
    IN log tinyint(1)
)
BEGIN
    # MOH 505 (IDSR weekly epidemic monitoring) flat table builder - v2.
    #
    # Populates etl.flat_moh_505_weekly_v2 (see create_flat_moh_505_weekly_v2.sql
    # for the table + build/sync queue DDL).  Build mode slices
    # flat_moh_505_weekly_v2_build_queue per worker; sync mode refills
    # flat_moh_505_weekly_v2_sync_queue from the flat_log watermark and runs
    # the same cycle loop over it.
    #
    # KNOWN LIMITATION (owner decision 2026-11): the flat_obs join is NOT
    # scoped by encounter type.  Sibling procedures (generate_flat_moh_710/
    # 711/717) restrict rows with WHERE t1.encounter_type IN (...) - the MOH
    # 505 form's encounter types are not pinned yet, so matching obs may be
    # attributed across every form the person has.  Add the IN (...) filter
    # here AND in the etl.flat_obs join below once the form's encounter types
    # are confirmed.
    #
    # Deaths: the register prints a Deaths column per disease.  Only
    # maternal_deaths / neonatal_deaths have confirmed concepts (7257/10881)
    # and are captured; the per-disease death concepts still need to be
    # counterchecked against the 705 A/B procedures (DB-only, not in this
    # repo) before *_death columns can be added here.
    set @primary_table := "flat_moh_505_weekly_v2";
    set @query_type = query_type;
    set @queue_table = "";
    set @write_table = "";
    set @total_rows_written = 0;
    set @start = now();
    set @table_version = "flat_moh_505_weekly_v2";

    # A cycle_size of 0 makes the loop below insert nothing while the queue
    # never drains - the WHILE @person_ids_count > 0 condition would spin
    # forever.  Clamp it to the sibling default instead.
    if (cycle_size is null or cycle_size <= 0) then
        set cycle_size = 1000;
    end if;

    if (@query_type not in ("build", "sync")) then
        SIGNAL SQLSTATE '45000'
           SET MESSAGE_TEXT = 'generate_moh_505_weekly_report_v2: query_type must be "build" or "sync".';
    end if;

    CREATE TABLE IF NOT EXISTS flat_moh_505_weekly_v2 (
        date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
        elastic_id VARCHAR(50),
        person_uuid CHAR(38),
        person_id INT,
        encounter_id INT,
        encounter_datetime DATETIME,
        location_id INT,
        year_week VARCHAR(10),
        age DECIMAL(23 , 2 ),
        gender VARCHAR(50),
        aefi INT(1) DEFAULT 0,
        acute_jaundice INT(1) DEFAULT 0,
        acute_malnutrition INT(1) DEFAULT 0,
        anthrax INT(1) DEFAULT 0,
        cholera INT(1) DEFAULT 0,
        dengue INT(1) DEFAULT 0,
        dysentery_bacillary INT(1) DEFAULT 0,
        measles INT(1) DEFAULT 0,
        maternal_deaths INT(1) DEFAULT 0,
        neonatal_deaths INT(1) DEFAULT 0,
        neonatal_tetanus INT(1) DEFAULT 0,
        plague INT(1) DEFAULT 0,
        rabies INT(1) DEFAULT 0,
        typhoid INT(1) DEFAULT 0,
        yellow_fever INT(1) DEFAULT 0,
        malaria_microscopy_tested INT(1) DEFAULT 0,
        malaria_microscopy_positive INT(1) DEFAULT 0,
        malaria_mrdt_tested INT(1) DEFAULT 0,
        malaria_mrdt_positive INT(1) DEFAULT 0,
        bacterial_meningitis INT(1) DEFAULT 0,
        meningitis_no_csf INT(1) DEFAULT 0,
        typhoid_lab_tested INT(1) DEFAULT 0,
        typhoid_lab_positive INT(1) DEFAULT 0,
        PRIMARY KEY elastic_id (elastic_id),
        INDEX encounter_id (encounter_id),
        INDEX person_id (person_id),
        INDEX year_week (year_week),
        INDEX location_id (location_id),
        INDEX encounter_datetime (encounter_datetime)
    );

    if (@query_type = "build") then
        SELECT 'BUILDING.......................';
        set @write_table = concat("flat_moh_505_weekly_v2_temp_", queue_number);
        set @build_queue = "flat_moh_505_weekly_v2_build_queue";
        set @queue_table = concat(@build_queue, "_", queue_number);

        SET @dyn_sql=CONCAT('drop table if exists ', @write_table);
        PREPARE s1 from @dyn_sql;
        EXECUTE s1;
        DEALLOCATE PREPARE s1;

        # A crashed earlier run leaves its per-worker queue slice behind,
        # drained empty by the cycle loop; CREATE TABLE IF NOT EXISTS would
        # then no-op and the run would silently process nothing.  DROP first
        # so a rerun takes a fresh slice (generate_flat_moh_710 pattern).
        SET @dyn_sql=CONCAT('drop table if exists ', @queue_table);
        PREPARE s1 from @dyn_sql;
        EXECUTE s1;
        DEALLOCATE PREPARE s1;

        SET @dyn_sql=CONCAT('Create table ', @queue_table, ' (person_id int primary key) (select person_id from ', @build_queue, ' limit ', queue_size, ');');
        PREPARE s1 from @dyn_sql;
        EXECUTE s1;
        DEALLOCATE PREPARE s1;

        # An empty slice must fail loudly, not no-op: a silent empty pass
        # still writes the watermark and unlocks sync while nothing was
        # processed (generate_flat_moh_706 pattern).
        SET @dyn_sql=CONCAT('select count(*) into @slice_count from ', @queue_table);
        PREPARE s1 from @dyn_sql;
        EXECUTE s1;
        DEALLOCATE PREPARE s1;

        if (@slice_count = 0) then
            SIGNAL SQLSTATE '45000'
               SET MESSAGE_TEXT = 'flat_moh_505_weekly_v2 build queue slice is empty - INSERT person_ids INTO etl.flat_moh_505_weekly_v2_build_queue first (every build drains it).';
        end if;

        SET @dyn_sql=CONCAT('Create table if not exists ', @write_table, ' like ', @primary_table);
        PREPARE s1 from @dyn_sql;
        EXECUTE s1;
        DEALLOCATE PREPARE s1;

        SET @dyn_sql=CONCAT('delete t1 from ', @build_queue, ' t1 join ', @queue_table, ' t2 using (person_id)');
        PREPARE s1 from @dyn_sql;
        EXECUTE s1;
        DEALLOCATE PREPARE s1;
    end if;

    if (@query_type="sync") then
        SELECT 'SYNCING.......................';
        set @write_table = concat("flat_moh_505_weekly_v2");
        set @queue_table = "flat_moh_505_weekly_v2_sync_queue";
        create table if not exists flat_moh_505_weekly_v2_sync_queue (person_id int primary key);

        SELECT MAX(date_updated) INTO @last_update FROM etl.flat_log WHERE table_name = @table_version;

        # v2 migration guard.  A fresh install has no watermark and syncing
        # from 1900 would requeue every person in flat_obs in one date scan.
        # Run build mode once; it logs a watermark row and unlocks sync
        # (generate_flat_moh_706 pattern).
        IF @last_update IS NULL THEN
            SIGNAL SQLSTATE '45000'
               SET MESSAGE_TEXT = 'No watermark for flat_moh_505_weekly_v2 - run generate_moh_505_weekly_report_v2 in build mode first.';
        END IF;

        replace into flat_moh_505_weekly_v2_sync_queue
        (select distinct person_id from etl.flat_obs where max_date_created > @last_update);

        # Catch persons whose non-obs records changed too: renamed/voided
        # persons and edited encounters need a rebuild or their flat rows go
        # stale (generate_flat_moh_710 sync-refill pattern).
        replace into flat_moh_505_weekly_v2_sync_queue
        (select distinct person_id from amrs.encounter where date_changed > @last_update);
        replace into flat_moh_505_weekly_v2_sync_queue
        (select person_id from amrs.person where date_voided > @last_update);
        replace into flat_moh_505_weekly_v2_sync_queue
        (select person_id from amrs.person where date_changed > @last_update);
    end if;

    SET @person_ids_count = 0;
    SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table);
    PREPARE s1 from @dyn_sql;
    EXECUTE s1;
    DEALLOCATE PREPARE s1;

    SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);');
    PREPARE s1 from @dyn_sql;
    EXECUTE s1;
    DEALLOCATE PREPARE s1;

    SELECT CONCAT('Patients in queue: ', @person_ids_count);

    set @total_time=0;
    set @cycle_number = 0;

    while @person_ids_count > 0 do
        set @loop_start_time = now();
        drop temporary table if exists flat_moh_505_weekly_v2_temp_queue;
        create temporary table flat_moh_505_weekly_v2_temp_queue (person_id int primary key);

        SET @dyn_sql=CONCAT('insert into flat_moh_505_weekly_v2_temp_queue (select * from ',@queue_table,' limit ',cycle_size,');');
        PREPARE s1 from @dyn_sql;
        EXECUTE s1;
        DEALLOCATE PREPARE s1;

        drop temporary table if exists flat_moh_505_weekly_v2_0;
        create temporary table flat_moh_505_weekly_v2_0 (
            encounter_id INT PRIMARY KEY,
            person_id INT,
            person_uuid CHAR(38),
            encounter_datetime DATETIME,
            location_id INT,
            year_week VARCHAR(10),
            age DECIMAL(23, 2),
            gender VARCHAR(50),
            aefi INT(1) DEFAULT 0,
            acute_jaundice INT(1) DEFAULT 0,
            acute_malnutrition INT(1) DEFAULT 0,
            anthrax INT(1) DEFAULT 0,
            cholera INT(1) DEFAULT 0,
            dengue INT(1) DEFAULT 0,
            dysentery_bacillary INT(1) DEFAULT 0,
            measles INT(1) DEFAULT 0,
            maternal_deaths INT(1) DEFAULT 0,
            neonatal_deaths INT(1) DEFAULT 0,
            neonatal_tetanus INT(1) DEFAULT 0,
            plague INT(1) DEFAULT 0,
            rabies INT(1) DEFAULT 0,
            typhoid INT(1) DEFAULT 0,
            yellow_fever INT(1) DEFAULT 0,
            malaria_microscopy_tested INT(1) DEFAULT 0,
            malaria_microscopy_positive INT(1) DEFAULT 0,
            malaria_mrdt_tested INT(1) DEFAULT 0,
            malaria_mrdt_positive INT(1) DEFAULT 0,
            bacterial_meningitis INT(1) DEFAULT 0,
            meningitis_no_csf INT(1) DEFAULT 0,
            typhoid_lab_tested INT(1) DEFAULT 0,
            typhoid_lab_positive INT(1) DEFAULT 0
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

        insert into flat_moh_505_weekly_v2_0
        SELECT
            t1.encounter_id,
            t1.person_id,
            p.uuid AS person_uuid,
            t1.encounter_datetime,
            t1.location_id,
            YEARWEEK(t1.encounter_datetime, 4) AS year_week,
            ROUND(TIMESTAMPDIFF(MONTH, p.birthdate, t1.encounter_datetime) / 12, 2) AS age,
            p.gender,
            IF(t1.obs REGEXP "!!(1284|11679|6042)=6839!!", 1, 0) AS aefi,
            IF(t1.obs REGEXP "!!(1284|11679|6042)=7384!!", 1, 0) AS acute_jaundice,
            IF(t1.obs REGEXP "!!(1284|11679|6042)=9471!!", 1, 0) AS acute_malnutrition,
            IF(t1.obs REGEXP "!!(1284|11679|6042)=12950!!", 1, 0) AS anthrax,
            IF(t1.obs REGEXP "!!(1284|11679|6042)=12947!!", 1, 0) AS cholera,
            IF(t1.obs REGEXP "!!(1284|11679|6042)=12974!!", 1, 0) AS dengue,
            IF(t1.obs REGEXP "!!(1284|11679|6042)=15030!!", 1, 0) AS dysentery_bacillary,
            IF(t1.obs REGEXP "!!(1284|11679|6042)=63!!", 1, 0) AS measles,
            IF(t1.obs REGEXP "!!(1284|11679|6042)=7257!!", 1, 0) AS maternal_deaths,
            IF(t1.obs REGEXP "!!(1284|11679|6042)=10881!!", 1, 0) AS neonatal_deaths,
            IF(t1.obs REGEXP "!!(1284|11679|6042)=12925!!", 1, 0) AS neonatal_tetanus,
            IF(t1.obs REGEXP "!!(1284|11679|6042)=12987!!", 1, 0) AS plague,
            IF(t1.obs REGEXP "!!(1284|11679|6042)=12918!!", 1, 0) AS rabies,
            IF(t1.obs REGEXP "!!(1284|11679|6042)=141!!", 1, 0) AS typhoid,
            IF(t1.obs REGEXP "!!(1284|11679|6042)=57!!", 1, 0) AS yellow_fever,
            IF(t1.obs REGEXP "!!32=[^!]+!!", 1, 0) AS malaria_microscopy_tested,
            IF(t1.obs REGEXP "!!32=703!!", 1, 0) AS malaria_microscopy_positive,
            IF(t1.obs REGEXP "!!9187=[^!]+!!", 1, 0) AS malaria_mrdt_tested,
            IF(t1.obs REGEXP "!!9187=703!!", 1, 0) AS malaria_mrdt_positive,
            IF(t1.obs REGEXP "!!(1284|11679|6042)=14354!!", 1, 0) AS bacterial_meningitis,
            IF(t1.obs REGEXP "!!(1284|11679|6042)=6124!!", 1, 0) AS meningitis_no_csf,
            IF(t1.obs REGEXP "!!12881=[^!]+!!", 1, 0) AS typhoid_lab_tested,
            IF(t1.obs REGEXP "!!12881=703!!", 1, 0) AS typhoid_lab_positive
        FROM flat_moh_505_weekly_v2_temp_queue tq
        JOIN etl.flat_obs t1 USING (person_id)
        JOIN amrs.person p ON (t1.person_id = p.person_id AND p.voided=0);

        SET @dyn_sql=CONCAT('replace into ',@write_table,' (select now() as date_created, CONCAT_WS("_", IFNULL(year_week, "000000"), person_id, encounter_id) as elastic_id, person_uuid, person_id, encounter_id, encounter_datetime, location_id, year_week, age, gender, aefi, acute_jaundice, acute_malnutrition, anthrax, cholera, dengue, dysentery_bacillary, measles, maternal_deaths, neonatal_deaths, neonatal_tetanus, plague, rabies, typhoid, yellow_fever, malaria_microscopy_tested, malaria_microscopy_positive, malaria_mrdt_tested, malaria_mrdt_positive, bacterial_meningitis, meningitis_no_csf, typhoid_lab_tested, typhoid_lab_positive FROM flat_moh_505_weekly_v2_0);');
        PREPARE s1 from @dyn_sql;
        EXECUTE s1;
        DEALLOCATE PREPARE s1;

        SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_moh_505_weekly_v2_temp_queue t2 using (person_id);');
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

        # Per-cycle progress - each SELECT streams its result set to the
        # client as it is produced, so an interactive CALL shows the queue
        # drain cycle by cycle (generate_flat_moh_710 pattern).
        SELECT
            @person_ids_count                    AS '# in queue',
            @cycle_length                        AS 'Cycle Time (s)',
            ceil(@person_ids_count / cycle_size) AS remaining_cycles,
            @remaining_time                      AS 'Est time remaining (min)';

        if(log = 1) then
            insert into etl.flat_log values (@loop_start_time, now(), @table_version, @cycle_length);
        end if;
    end while;

    # Persons voided since their last build keep their flat rows forever
    # otherwise - purge them once per run (generate_flat_moh_710 pattern).
    SET @dyn_sql=CONCAT('delete t1 from ', @primary_table, ' t1 join amrs.person t2 using (person_id) where t2.voided = 1');
    PREPARE s1 from @dyn_sql;
    EXECUTE s1;
    DEALLOCATE PREPARE s1;

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
        SELECT CONCAT(@start_write, " : Writing ",@total_rows_to_write, ' to ',@primary_table);

        SET @dyn_sql=CONCAT('replace into ', @primary_table, '(select * from ',@write_table,');');
        PREPARE s1 from @dyn_sql;
        EXECUTE s1;
        DEALLOCATE PREPARE s1;

        set @finish_write = now();
        set @time_to_write = timestampdiff(second,@start_write,@finish_write);
        SELECT CONCAT(@finish_write, " : Completed writing... ", @time_to_write, ' seconds');

        SET @dyn_sql=CONCAT('drop table ',@write_table,';');
        PREPARE s1 from @dyn_sql;
        EXECUTE s1;
        DEALLOCATE PREPARE s1;
    end if;

    # Advance the sync watermark so the next sync run only processes new
    # rows.  Positional INSERT - flat_log columns are
    # (date_created, date_updated, table_name, seconds_to_complete).
    set @end = now();
    insert into etl.flat_log values (@start, @end, @table_version, timestampdiff(second,@start,@end));

    SELECT CONCAT(@table_version, ' : Time to complete: ', timestampdiff(minute,@start,@end), ' minutes');

END
