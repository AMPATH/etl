use etl;
drop procedure if exists generate_patient_monthly_enrollment;
DELIMITER $$ 
CREATE PROCEDURE `generate_patient_monthly_enrollment`(
    IN query_type varchar(50),
    IN queue_number int,
    IN queue_size int,
    IN cycle_size int
) BEGIN


set @start = now();
set @table_version = "patient_monthly_enrollment_v1.0";
set @last_date_created = (
        select max(date_created)
        from amrs.patient_program
    );
set @sep = " ## ";


CREATE TABLE if not exists `patient_monthly_enrollment` (
    `person_id` int(11) NOT NULL DEFAULT '0',
    `endDate` datetime DEFAULT NULL,
    `retention_date_enrolled` datetime DEFAULT NULL,
    `retention_date_completed` datetime DEFAULT NULL,
    `retention_location_id` int(11) DEFAULT NULL,
    `retention_patient_program_id` int(11) DEFAULT '0',
    `newly_enrolled_retention_this_month` int(4) DEFAULT NULL,
    `exited_retention_this_month` int(4) DEFAULT NULL,
    `in_retention_this_month` int(0) DEFAULT NULL,
    `ovc_date_enrolled` datetime DEFAULT NULL,
    `ovc_date_completed` datetime DEFAULT NULL,
    `ovc_location_id` int(11) DEFAULT NULL,
    `ovc_patient_program_id` int(11) DEFAULT '0',
    `newly_enrolled_ovc_this_month` int(4) DEFAULT NULL,
    `exited_ovc_this_month` int(4) DEFAULT NULL,
    `in_ovc_this_month` int(0) DEFAULT NULL,
    `pmtct_date_enrolled` datetime DEFAULT NULL,
    `pmtct_date_completed` datetime DEFAULT NULL,
    `pmtct_location_id` int(11) DEFAULT NULL,
    `pmtct_patient_program_id` int(11) DEFAULT '0',
    `newly_enrolled_pmtct_this_month` int(4) DEFAULT NULL,
    `exited_pmtct_this_month` int(4) DEFAULT NULL,
    `in_pmtct_this_month` int(0) DEFAULT NULL,
    KEY `person_id` (`person_id`),
    KEY `person_id_2` (`person_id`, `endDate`),
    KEY `endDate` (`endDate`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;

if (query_type = "build") then
select "BUILDING.......................";
set @queue_table = concat(
        "patient_monthly_enrollment_build_queue_",
        queue_number
    );
  
select @queue_table;
SET @dyn_sql = CONCAT(
        'Create table if not exists ',
        @queue_table,
        '(person_id int primary key) (select * from patient_monthly_enrollment_build_queue limit ',
        queue_size,
        ');'
    );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
select count(*) from patient_monthly_enrollment_build_queue_1;

SET @dyn_sql = CONCAT(
        'delete t1 from patient_monthly_enrollment_build_queue t1 join ',
        @queue_table,
        ' t2 using (person_id)'
    );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;

end if;


if (query_type = "sync") then
set @queue_table = "patient_monthly_enrollment_sync_queue";
create table if not exists patient_monthly_enrollment_sync_queue (person_id int primary key);
select @last_update := (
        select max(date_updated)
        from etl.flat_log
        where table_name = @table_version
    );
replace into patient_monthly_enrollment_sync_queue (
    select distinct patient_id
    from amrs.patient_program
    where date_created >= @last_update
);
end if;


SET @num_ids := 0;
SET @dyn_sql = CONCAT(
        'select count(*) into @num_ids from ',
        @queue_table,
        ';'
    );
PREPARE s1
from @dyn_sql;
-- select @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;

SET @person_ids_count = 0;
SET @dyn_sql = CONCAT(
        'select count(*) into @person_ids_count from ',
        @queue_table
    );
PREPARE s1
from @dyn_sql;
-- select @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;

SET @dyn_sql = CONCAT(
        'delete t1 from patient_monthly_enrollment t1 join ',
        @queue_table,
        ' t2 using (person_id);'
    );
PREPARE s1
from @dyn_sql;
-- select @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;


-- select @person_ids_count as total_patients;
set @total_time = 0;
set @cycle_number = 0;
while @person_ids_count > 0 do
    set @loop_start_time = now();

    drop temporary table if exists program_enrollment_temporary_build_queue;
    create temporary table program_enrollment_temporary_build_queue (person_id int primary key);
	-- select @queue_table as queue_table;
    -- select cycle_size as cycle_size;
    SET @dyn_sql = CONCAT('replace into program_enrollment_temporary_build_queue (select * from ', @queue_table, ' limit ',cycle_size, ');'); 
	-- select @dyn_sql as loop_table;
    PREPARE s1 from @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;
    select count(*) as patients_in_cycle_queue from program_enrollment_temporary_build_queue;

    drop temporary table if exists enrollments_in_queue;
    create temporary table enrollments_in_queue               
    (index (person_id), index(person_id, date_enrolled), index(date_enrolled))
    (select 
                        q.person_id,
                        pe.program_id,
                        pe.patient_program_id,
                        pe.date_enrolled,
                        pe.date_completed,
                        pe.location_id
                        from 
                        amrs.patient_program pe 
                        INNER JOIN
                        program_enrollment_temporary_build_queue q on (person_id = patient_id)
                        Where 
                        (pe.voided = null or pe.voided = 0)
                        order by person_id, date_enrolled
    );
    -- select * from enrollments_in_queue limit 5;

    set @minEnrolmentDate = (select min(date_enrolled) from enrollments_in_queue);
    set @maxEnrolmentDate = (select max(date_enrolled) from enrollments_in_queue);

    drop temporary table if exists ovc_patient_date_enrollments;
    create temporary table ovc_patient_date_enrollments               
    (index (person_id), index (endDate),  UNIQUE e_person_id_date(person_id, endDate))
                ( 
                select 
                    person_id, 
                    endDate, 
                    date_enrolled as ovc_date_enrolled,
                    date_completed as ovc_date_completed,
                    program_id as ovc_program_id,
                    location_id as ovc_location_id,
                    patient_program_id as ovc_patient_program_id
                    from 
                        (select 
                        h.person_id,
                        d.endDate, 
                        date_enrolled,
                        date_completed,
                        patient_program_id,
                        program_id,
                        location_id
                        from 
                        dates d
                        join
                        enrollments_in_queue h
                        WHERE
                        h.date_enrolled < DATE_ADD(endDate, INTERVAL 1 DAY)
                        and program_id IN (2)
                        ORDER BY h.person_id , d.endDate, h.date_enrolled desc
                        ) p 
                group by person_id, endDate);
    select count(*) from ovc_patient_date_enrollments;

    set @prev_id = -1;
    set @cur_id = -1;
    drop temporary table if exists ovc_patient_enrollments_1;
    create temporary table ovc_patient_enrollments_1               
    (index (person_id), index (endDate),  UNIQUE e_person_id_date(person_id, endDate))
    ( 
    select
    e.*,
    @prev_id := @cur_id AS prev_id,
    @cur_id := person_id AS cur_id,
    case
        when EXTRACT( YEAR_MONTH FROM endDate ) = EXTRACT( YEAR_MONTH FROM ovc_date_enrolled ) then 
        @newly_enrolled_ovc_this_month := 1
        else
        @newly_enrolled_ovc_this_month := 0
    end as newly_enrolled_ovc_this_month,
    case
        when EXTRACT( YEAR_MONTH FROM endDate ) = EXTRACT( YEAR_MONTH FROM ovc_date_completed ) then 
        @exited_ovc_this_month := 1
        else
        @exited_ovc_this_month := 0
    end as exited_ovc_this_month,
    case
        when EXTRACT( YEAR_MONTH FROM ovc_date_enrolled ) <= EXTRACT( YEAR_MONTH FROM endDate ) and
        (!(EXTRACT( YEAR_MONTH FROM ovc_date_completed ) <= EXTRACT( YEAR_MONTH FROM endDate )) or ovc_date_completed is null)  then
        1
        else
        0
    end as in_ovc_this_month
    from
    ovc_patient_date_enrollments e
    );
    select count(*) from ovc_patient_enrollments_1;
    
    drop temporary table if exists pmtct_patient_date_enrollments;
    create temporary table pmtct_patient_date_enrollments               
    (index (person_id), index (endDate),  UNIQUE e_person_id_date(person_id, endDate))
                ( 
                select 
                    person_id, 
                    endDate, 
                    date_enrolled as pmtct_date_enrolled,
                    date_completed as pmtct_date_completed,
                    program_id as pmtct_program_id,
                    location_id as pmtct_location_id,
                    patient_program_id as pmtct_patient_program_id
                    from 
                        (select 
                        h.person_id,
                        d.endDate, 
                        date_enrolled,
                        date_completed,
                        patient_program_id,
                        program_id,
                        location_id
                        from 
                        dates d
                        join
                        enrollments_in_queue h
                        WHERE
                        h.date_enrolled < DATE_ADD(endDate, INTERVAL 1 DAY)
                        and program_id IN (4)
                        ORDER BY h.person_id , d.endDate, h.date_enrolled desc
                        ) p 
                group by person_id, endDate);
    
	set @prev_id = -1;
    set @cur_id = -1;
    drop temporary table if exists pmtct_patient_enrollments_1;
    create temporary table pmtct_patient_enrollments_1               
    (index (person_id), index (endDate),  UNIQUE e_person_id_date(person_id, endDate))
    ( 
    select
    e.*,
    @prev_id := @cur_id AS prev_id,
    @cur_id := person_id AS cur_id,
    case
        when EXTRACT( YEAR_MONTH FROM endDate ) = EXTRACT( YEAR_MONTH FROM pmtct_date_enrolled ) then 
        @newly_enrolled_pmtct_this_month := 1
        else
        @newly_enrolled_pmtct_this_month := 0
    end as newly_enrolled_pmtct_this_month,
    case
        when EXTRACT( YEAR_MONTH FROM endDate ) = EXTRACT( YEAR_MONTH FROM pmtct_date_completed ) then 
        @exited_pmtct_this_month := 1
        else
        @exited_pmtct_this_month := 0
    end as exited_pmtct_this_month,
    case
        when EXTRACT( YEAR_MONTH FROM pmtct_date_enrolled ) <= EXTRACT( YEAR_MONTH FROM endDate ) and
        (!(EXTRACT( YEAR_MONTH FROM pmtct_date_completed ) <= EXTRACT( YEAR_MONTH FROM endDate )) or pmtct_date_completed is null)  then
        1
        else
        0
    end as in_pmtct_this_month
    from
    pmtct_patient_date_enrollments e
    );

    drop temporary table if exists retention_patient_date_enrollments;
    create temporary table retention_patient_date_enrollments               
    (index (person_id), index (endDate),  UNIQUE e_person_id_date(person_id, endDate))
                ( 
                select 
                    person_id, 
                    endDate, 
                    date_enrolled as retention_date_enrolled,
                    date_completed as retention_date_completed,
                    program_id as retention_program_id,
                    location_id as retention_location_id,
                    patient_program_id as retention_patient_program_id
                    from 
                        (select 
                        h.person_id,
                        d.endDate, 
                        date_enrolled,
                        date_completed,
                        patient_program_id,
                        program_id,
                        location_id
                        from 
                        dates d
                        join
                        enrollments_in_queue h
                        WHERE
                        h.date_enrolled < DATE_ADD(endDate, INTERVAL 1 DAY)
                        and program_id IN (20)
                        ORDER BY h.person_id , d.endDate, h.date_enrolled desc
                        ) p 
                group by person_id, endDate);
    select count(*) from retention_patient_date_enrollments;

    set @prev_id = -1;
    set @cur_id = -1;
    drop temporary table if exists retention_patient_enrollments_1;
    create temporary table retention_patient_enrollments_1               
    (index (person_id), index (endDate),  UNIQUE e_person_id_date(person_id, endDate))
    ( 
    select
    e.*,
    @prev_id := @cur_id AS prev_id,
    @cur_id := person_id AS cur_id,
    case
        when EXTRACT( YEAR_MONTH FROM endDate ) = EXTRACT( YEAR_MONTH FROM retention_date_enrolled ) then 
        @newly_enrolled_retention_this_month := 1
        else
        @newly_enrolled_retention_this_month := 0
    end as newly_enrolled_retention_this_month,
    case
        when EXTRACT( YEAR_MONTH FROM endDate ) = EXTRACT( YEAR_MONTH FROM retention_date_completed ) then 
        @exited_retention_this_month := 1
        else
        @exited_retention_this_month := 0
    end as exited_retention_this_month,
    case
        when EXTRACT( YEAR_MONTH FROM retention_date_enrolled ) <= EXTRACT( YEAR_MONTH FROM endDate ) and
        (!(EXTRACT( YEAR_MONTH FROM retention_date_completed ) <= EXTRACT( YEAR_MONTH FROM endDate )) or retention_date_completed is null)  then
        1
        else
        0
    end as in_retention_this_month
    from
    retention_patient_date_enrollments e
    );
    select count(*) from retention_patient_enrollments_1;

    drop temporary table if exists patient_dates;
    create temporary table patient_dates               
    (index (person_id), index(person_id, endDate), index(endDate))
    (select * from
    (select person_id, endDate from retention_patient_enrollments_1
    union
    select person_id, endDate from ovc_patient_enrollments_1
    union
    select person_id, endDate from pmtct_patient_enrollments_1) pd
    );

    replace into patient_monthly_enrollment
    (
        select 
        pd.person_id,
        pd.endDate,
        retention_date_enrolled,
        retention_date_completed,
        retention_location_id,
        retention_patient_program_id,
        newly_enrolled_retention_this_month,
        exited_retention_this_month,
        in_retention_this_month,
        ovc_date_enrolled,
        ovc_date_completed,
        ovc_location_id,
        ovc_patient_program_id,
        newly_enrolled_ovc_this_month,
        exited_ovc_this_month,
        in_ovc_this_month,
        pmtct_date_enrolled,
		pmtct_date_completed,
		pmtct_location_id,
		pmtct_patient_program_id,
		newly_enrolled_pmtct_this_month,
		exited_pmtct_this_month,
		in_pmtct_this_month
        from 
        patient_dates  pd
        left outer join retention_patient_enrollments_1 re on (pd.person_id = re.person_id and pd.endDate = re.endDate)
        left outer join ovc_patient_enrollments_1 ovc on (pd.person_id = ovc.person_id and pd.endDate = ovc.endDate)
        left outer join pmtct_patient_enrollments_1 pmtct on (pd.person_id = pmtct.person_id and pd.endDate = pmtct.endDate)
        order by pd.person_id, pd.endDate
    );

    SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join program_enrollment_temporary_build_queue t2 using (person_id);'); 
	PREPARE s1 from @dyn_sql; 
	EXECUTE s1; 
	DEALLOCATE PREPARE s1;
                         
    SET @dyn_sql = CONCAT(
            'select count(*) into @person_ids_count from ',
            @queue_table,
            ';'
        );
    PREPARE s1
    from @dyn_sql;
    EXECUTE s1;
    DEALLOCATE PREPARE s1;

    set @cycle_length = timestampdiff(second, @loop_start_time, now());
    set @total_time = @total_time + @cycle_length;
    set @cycle_number = @cycle_number + 1;
    set @remaining_time = ceil(
            (@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60
        );
    select @num_in_hmrd as num_in_hmrd,
        @person_ids_count as num_remaining,
        @cycle_length as 'Cycle time (s)',
        ceil(@person_ids_count / cycle_size) as remaining_cycles,
        @remaining_time as 'Est time remaining (min)';
end while;


if(query_type = "build") then
SET @dyn_sql = CONCAT('drop table ', @queue_table, ';');
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
end if;

set @end = now();
-- not sure why we need last date_created, I've replaced this with @start
insert into etl.flat_log
values (
        @start,
        @last_date_created,
        @table_version,
        timestampdiff(second, @start, @end)
    );
select concat(
        @table_version,
        " : Time to complete: ",
        timestampdiff(minute, @start, @end),
        " minutes"
    );
END $$ 
DELIMITER ;