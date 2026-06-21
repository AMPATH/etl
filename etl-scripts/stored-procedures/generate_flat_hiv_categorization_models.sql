CREATE DEFINER=`analytics`@`%` PROCEDURE `etl`.`generate_flat_hiv_categorization_models`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int, IN log BOOLEAN)
BEGIN
set @primary_table := "flat_hiv_categorization_models";
set @query_type = query_type;
set @queue_table = "";
set @total_rows_written = 0;

set @start = now();
set @table_version = "flat_hiv_categorization_models_v1.0";

set session sort_buffer_size=512000000;                    
set @last_date_created = (select max(max_date_created) from etl.flat_obs);
             
CREATE TABLE IF NOT EXISTS etl.flat_hiv_categorization_models (
    person_id INT,
    encounter_id INT,
    location_id INT,
    encounter_datetime DATETIME,
    next_encounter_datetime DATETIME,
    encounter_type INT,
    referrals_ordered VARCHAR(800),
    patient_categorization VARCHAR(800),
    service_delivery_model VARCHAR(800),
    dsd_model VARCHAR(800),
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY encounter_id (encounter_id),
    INDEX person_date (person_id , encounter_datetime),
    INDEX person_location (person_id , location_id),
    INDEX location_id (location_id),
    INDEX encounter_type (encounter_type),
    INDEX referrals_ordered (referrals_ordered),
	INDEX patient_categorization (patient_categorization),
	INDEX service_delivery_model (service_delivery_model),
	INDEX dsd_model (dsd_model),
    INDEX date_created (date_created),
    INDEX next_encounter_datetime(next_encounter_datetime)
);
                    
                    
                                        
if(@query_type="build") then
        select 'BUILDING..........................................';
        set @write_table = concat("flat_hiv_categorization_models_temp_",queue_number);
        set @queue_table = concat("flat_hiv_categorization_models_build_queue_",queue_number);                                                                    

        SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  

        
        SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_hiv_categorization_models_build_queue limit ', queue_size, ');'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
        
        
        SET @dyn_sql=CONCAT('delete t1 from flat_hiv_categorization_models_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  

end if;
    
                    
if (@query_type="sync") then
select 'SYNCING..........................................';
set @write_table = "flat_hiv_categorization_models";
set @queue_table = "flat_hiv_categorization_models_sync_queue";
CREATE TABLE IF NOT EXISTS etl.flat_hiv_categorization_models_sync_queue (
	person_id INT PRIMARY KEY
);                            
                            


set @last_update = null;
SELECT 
    MAX(date_updated)
INTO @last_update FROM
    etl.flat_log
WHERE
    table_name = @table_version;

    replace into etl.flat_hiv_categorization_models_sync_queue(
    select distinct patient_id
        from amrs.encounter
        where encounter_type in (1, 2, 3, 105)
        and date_changed > @last_update
    );

    replace into etl.flat_hiv_categorization_models_sync_queue
    (select distinct person_id
        from etl.flat_obs
        where encounter_type in (1, 2, 3, 105)
        and max_date_created > @last_update
    );
                            
    replace into etl.flat_hiv_categorization_models_sync_queue
    (select person_id from 
        amrs.person 
        where 
        date_voided > @last_update);


    replace into etl.flat_hiv_categorization_models_sync_queue
    (select person_id from 
        amrs.person 
        where date_changed > @last_update);
                                

  end if;
                      
                    
                    
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

SELECT @person_ids_count AS 'num patients to sync';

                    
SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
PREPARE s1 from @dyn_sql; 
EXECUTE s1; 
DEALLOCATE PREPARE s1;  

set @total_time=0;
set @cycle_number = 0;


while @person_ids_count > 0 do

    set @loop_start_time = now();
    
    
    drop temporary table if exists flat_hiv_categorization_models_build_queue__0;
    

    
    SET @dyn_sql=CONCAT('create temporary table flat_hiv_categorization_models_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
    PREPARE s1 from @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
    
    SELECT CONCAT('Building flat_hiv_categorization_models_stage_1... ');
    

    drop temporary table if exists flat_hiv_categorization_models_stage_1;
    create temporary table flat_hiv_categorization_models_stage_1(index tr_person_id (person_id))(
        select
        t1.person_id,
        t1.encounter_id,
        t1.encounter_type,
        t1.encounter_datetime,
        t1.location_id,
            CASE
            WHEN t1.obs REGEXP '!!1272=' THEN etl.getValues(t1.obs,
            1272)
            ELSE NULL
        END AS `referrals_ordered`,
        CASE
            WHEN t1.obs REGEXP '!!12108=' THEN etl.getValues(t1.obs,
            12108)
            WHEN t1.obs REGEXP '!!12127=' THEN etl.getValues(t1.obs,
            12127)
            ELSE NULL
        END AS `patient_categorization`,
        CASE
            WHEN t1.obs REGEXP '!!10988=' THEN etl.getValues(t1.obs,
            10988)
            ELSE NULL
        END AS `service_delivery_model`,
        CASE
            WHEN t1.obs REGEXP '!!11931=' THEN etl.getValues(t1.obs,
            11931)
            WHEN t1.obs REGEXP '!!11932=' THEN etl.getValues(t1.obs,
            11932)
        END AS `dsd_model`,
        null as date_created
        from etl.flat_obs t1
        join flat_hiv_categorization_models_build_queue__0 t0 on (t1.person_id = t0.person_id)
        where t1.encounter_type in (1, 2, 3, 105)
            
    );
                

    SELECT CONCAT('Building flat_hiv_categorization_models_interim... ');
    
    set @prev_id = -1;
    set @cur_id = -1;
    set @prev_encounter_datetime = null;
    set @cur_encounter_datetime = null;

    drop temporary table if exists flat_hiv_categorization_models_interim;
    create temporary table flat_hiv_categorization_models_interim (index encounter_id (encounter_id),index person_id (person_id))
    (select
            @prev_id := @cur_id as prev_id,
            @cur_id := t1.person_id as cur_id,
            @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,
            t1.person_id,
            t1.encounter_id,
            t1.location_id,
            t1.encounter_datetime,
            t1.encounter_type,
            t1.referrals_ordered,
            t1.patient_categorization,
            t1.service_delivery_model,
            t1.dsd_model,
            case
            when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
            else @prev_encounter_datetime := null
            end as next_encounter_datetime,
        null as date_created
        from flat_hiv_categorization_models_stage_1 t1
        order by t1.person_id, date(t1.encounter_datetime) desc
    );


SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    flat_hiv_categorization_models_interim;
                    
SELECT @new_encounter_rows;                    
                    set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;
    
                    
                    
    SET @dyn_sql=CONCAT('replace into flat_hiv_categorization_models',                                              
        '(select person_id,encounter_id,location_id,encounter_datetime,
    next_encounter_datetime,
	encounter_type,
    referrals_ordered,
    patient_categorization,
    service_delivery_model,
    dsd_model,
    date_created from flat_hiv_categorization_models_interim);');

    PREPARE s1 from @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;  
    

    

    SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_hiv_categorization_models_build_queue__0 t2 using (person_id);'); 

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
                 -- if (log="true") then
                 insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
                --  end if;
SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');

                END