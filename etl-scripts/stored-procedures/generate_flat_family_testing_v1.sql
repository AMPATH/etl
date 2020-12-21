CREATE PROCEDURE `generate_flat_family_testing`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int , IN log boolean)
BEGIN
	set @primary_table := "flat_family_testing";
	set @query_type = query_type;
	set @queue_table = "";
	set @total_rows_written = 0;
	
	set @start = now();
	set @table_version = "flat_family_testing_v1";

	set @sep = " ## ";
	set @last_date_created = (select max(max_date_created) from etl.flat_obs);

CREATE TABLE IF NOT EXISTS `flat_family_testing` (
  `patient_id` int(11) NOT NULL DEFAULT '0',
  `patient_uuid` varchar(200) DEFAULT NULL,
  `obs_group_id` int(11) NOT NULL DEFAULT '0',
  `identifiers` varchar(200) DEFAULT NULL,
  `person_name` varchar(200) DEFAULT NULL,
  `age` int(11) NOT NULL DEFAULT '0',
  `phone_number` varchar(200) DEFAULT NULL,
  `nearest_center` varchar(200) DEFAULT NULL,
  `patient_program_name` varchar(200) DEFAULT NULL,
  `fm_name` varchar(200) DEFAULT NULL,
  `relationship_type` varchar(200) DEFAULT NULL,
  `fm_age` int(11) DEFAULT NULL,
  `fm_status` varchar(100) DEFAULT NULL,
  `fm_gender` varchar(100) DEFAULT NULL,
  `fm_dob` varchar(100) DEFAULT NULL,
  `fm_phone` varchar(100) DEFAULT NULL,
  `fm_address` varchar(100) DEFAULT NULL,
  `marital_status` varchar(100) DEFAULT NULL,
  `living_with_client` varchar(100) DEFAULT NULL,
  `reported_test_date` varchar(100) DEFAULT NULL,
  `eligible_for_testing` varchar(100) DEFAULT NULL,
  `in_care` varchar(100) DEFAULT NULL,
  `ccc_number` varchar(100) DEFAULT NULL,
  `in_hei_program` varchar(100) DEFAULT NULL,
  `hei_number` varchar(100) DEFAULT NULL,
  `prefered_pns` varchar(100) DEFAULT NULL,
  `location_id` int(11) DEFAULT NULL,
  `location_uuid` varchar(200) DEFAULT NULL,
    
    primary key elastic_id (`patient_id`,`obs_group_id`)
);
    
      if (query_type = "build") then
        SELECT 'BUILDING.......................';
        set @write_table = concat("flat_family_testing_temp",queue_number);
        set @queue_table = concat("flat_family_testing_build_queue_",queue_number);   

        SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,'(person_id int primary key) (select * from flat_family_testing_build_queue limit ',queue_size, ');'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;

        SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1; 
            
        SET @dyn_sql=CONCAT('delete t1 from flat_family_testing_build_queue t1 join ',@queue_table, ' t2 using (person_id)'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
	end if;
    
    if (query_type = "sync") then
		set @write_table = concat("flat_family_testing");
		set @queue_table = "flat_family_testing_sync_queue";                                
		create table if not exists flat_family_testing_sync_queue (person_id int primary key);
		
		select @last_update := (select max(date_updated) from etl.flat_log where table_name=@table_version);

		replace into flat_family_testing_sync_queue
		(select distinct person_id from amrs.obs where date_created >= @last_update);
	end if;
    
  # Remove test patients
    -- SET @dyn_sql=CONCAT('delete t1 FROM ',@queue_table,' t1
    --         join amrs.person_attribute t2 using (person_id)
    --         where t2.person_attribute_type_id=28 and value="true" and voided=0');
    -- PREPARE s1 from @dyn_sql; 
    -- EXECUTE s1; 
    -- DEALLOCATE PREPARE s1;
  
  SET @person_ids_count = 0;
  SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
  PREPARE s1 from @dyn_sql; 
  EXECUTE s1; 
  DEALLOCATE PREPARE s1;

  SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 on t1.patient_id = t2.person_id;'); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;
  
SELECT CONCAT('Patients in queue: ', @person_ids_count);
                          
set @total_time=0;
set @cycle_number = 0;

        set @total_time=0;
        set @cycle_number = 0;
                
        while @person_ids_count > 0 do
        
            set @loop_start_time = now();                        

            drop temporary table if exists flat_family_testing_cycle_queue;
            create temporary table flat_family_testing_cycle_queue (person_id int primary key);                

            SET @dyn_sql=CONCAT('insert into flat_family_testing_cycle_queue (select * from ',@queue_table,' limit ',cycle_size,');'); 
            PREPARE s1 from @dyn_sql; 
            EXECUTE s1; 
            DEALLOCATE PREPARE s1;
                            
                            
            drop temporary table if exists family_testing_obs_stage_0;
            create temporary table family_testing_obs_stage_0 (
                SELECT 
                    t1.*
                FROM
                    amrs.obs t1
                        JOIN
                    etl.flat_family_testing_cycle_queue t2 ON (t1.person_id = t2.person_id
                        AND t1.obs_group_id IS NOT NULL));
            
            drop table if exists family_testing_obs_stage_1;
            create table family_testing_obs_stage_1(
                SELECT 
                    t3.*
                FROM
                    etl.family_testing_obs_stage_0 t3
                        INNER JOIN
                    amrs.encounter t4 ON (t3.encounter_id = t4.encounter_id
                        AND t4.encounter_type = 243));
                       
                
            
        SELECT CONCAT('creating flat_family_testing_stage_2 ...');
        DROP temporary table if exists flat_family_testing_stage_2;

        CREATE temporary TABLE flat_family_testing_stage_2 
        ( SELECT t1.person_id                                                   AS 
                patient_id, 
                p.uuid as `patient_uuid`,
                t1.obs_group_id, 
                Group_concat(DISTINCT id.identifier SEPARATOR ', ')            AS 
                `identifiers`, 
                Concat(Coalesce(person_name.given_name, ''), ' ', Coalesce( 
                person_name.middle_name, ''), ' ', Coalesce( 
                person_name.family_name, ''))                                  AS 
                `person_name`, 
                Extract(year FROM ( From_days(Datediff(Now(), p.birthdate)) )) AS `age`, 
                Group_concat(DISTINCT contacts.value SEPARATOR ', ')           AS 
                `phone_number`, 
                pa.address3                                                    AS 
                `nearest_center`, 
                pt.name                                                        AS 
                `patient_program_name`, 
                t2.value_text                                                  AS 
                `fm_name`, 
                cn3.name                                                       AS 
                `relationship_type`, 
                age.value_numeric                                              AS 
                `fm_age`, 
                cn4.name                                                       AS 
                `fm_status`, 
                cn5.name                  
                AS 
                `fm_gender`, 
                date.value_datetime                                            AS 
                `fm_dob`, 
                phone.value_text                                               AS 
                `fm_phone`, 
                address.value_text                                             AS 
                `fm_address`, 
                cnms.name                                                      AS 
                `marital_status`, 
                cnlwc.name                                                     AS 
                `living_with_client`, 
                rt_date.value_datetime                                         AS 
                `reported_test_date`, 
                cnte.name                                                      AS 
                `eligible_for_testing`, 
                cn_in_care.name AS `in_care`,
                ccc.value_text AS `ccc_number`,
                cn_in_hei.name AS `in_hei_program`,
                hei_no.value_text AS `hei_number`,
                cn_pns.name AS `prefered_pns`,
                t1.location_id,
                l.uuid AS `location_uuid`
            FROM   etl.family_testing_obs_stage_1 t1 
                LEFT JOIN (SELECT * 
                            FROM   etl.family_testing_obs_stage_1 
                            WHERE  concept_id = 7069) `t2` 
                        ON ( t1.obs_group_id = t2.obs_group_id ) 
                LEFT JOIN (SELECT * 
                            FROM   etl.family_testing_obs_stage_1 
                            WHERE  concept_id = 11730) `date` 
                        ON ( t1.obs_group_id = date.obs_group_id ) 
                LEFT JOIN (SELECT * 
                            FROM   etl.family_testing_obs_stage_1 
                            WHERE  concept_id = 10790) `rt_date` 
                        ON ( t1.obs_group_id = rt_date.obs_group_id ) 
                LEFT JOIN (SELECT * 
                            FROM   etl.family_testing_obs_stage_1 
                            WHERE  concept_id = 11729) `age` 
                        ON ( t1.obs_group_id = age.obs_group_id ) 
                LEFT JOIN (SELECT * 
                            FROM   etl.family_testing_obs_stage_1 
                            WHERE  concept_id = 10966) `phone` 
                        ON ( t1.obs_group_id = phone.obs_group_id ) 
                LEFT JOIN (SELECT * 
                            FROM   etl.family_testing_obs_stage_1 
                            WHERE  concept_id = 11737) `address` 
                        ON ( t1.obs_group_id = address.obs_group_id ) 
                LEFT JOIN (SELECT * 
                            FROM   etl.family_testing_obs_stage_1 
                            WHERE  concept_id = 1675) `t3` 
                        ON ( t1.obs_group_id = t3.obs_group_id ) 
                LEFT JOIN amrs.concept_name cn3 
                        ON ( cn3.concept_id = t3.value_coded ) 
                LEFT JOIN (SELECT * 
                            FROM   etl.family_testing_obs_stage_1 
                            WHERE  concept_id = 1054) `ms` 
                        ON ( t1.obs_group_id = ms.obs_group_id ) 
                LEFT JOIN amrs.concept_name cnms 
                        ON ( cnms.concept_id = ms.value_coded ) 
                LEFT JOIN (SELECT * 
                            FROM   etl.family_testing_obs_stage_1 
                            WHERE  concept_id = 6709) `t4` 
                        ON ( t1.obs_group_id = t4.obs_group_id ) 
                LEFT JOIN amrs.concept_name cn4 
                        ON ( cn4.concept_id = t4.value_coded ) 
                LEFT JOIN (SELECT * 
                            FROM   etl.family_testing_obs_stage_1 
                            WHERE  concept_id = 10981) `t6` 
                        ON ( t1.obs_group_id = t6.obs_group_id ) 
                LEFT JOIN amrs.concept_name cn5 
                        ON ( cn5.concept_id = t6.value_coded ) 
                LEFT JOIN (SELECT * 
                            FROM   etl.family_testing_obs_stage_1 
                            WHERE  concept_id = 11674) `lwc` 
                        ON ( t1.obs_group_id = lwc.obs_group_id ) 
                LEFT JOIN amrs.concept_name cnlwc 
                        ON ( cnlwc.concept_id = lwc.value_coded ) 
                LEFT JOIN (SELECT * 
                            FROM   etl.family_testing_obs_stage_1 
                            WHERE  concept_id = 11726) `te` 
                        ON ( t1.obs_group_id = te.obs_group_id ) 
                LEFT JOIN amrs.concept_name cnte 
                        ON ( cnte.concept_id = te.value_coded )
                LEFT JOIN (SELECT * 
                            FROM   etl.family_testing_obs_stage_1 
                            WHERE  concept_id = 9775) `pt_date` 
                        ON ( t1.obs_group_id = pt_date.obs_group_id ) #here
                LEFT JOIN (SELECT * 
                            FROM   etl.family_testing_obs_stage_1 
                            WHERE  concept_id = 6152) `t7` 
                        ON ( t1.obs_group_id = t7.obs_group_id ) 
                LEFT JOIN amrs.concept_name cn_in_care 
                        ON ( cn_in_care.concept_id = t7.value_coded )
                LEFT JOIN (SELECT * 
                        FROM   etl.family_testing_obs_stage_1 
                        WHERE  concept_id = 9775) `ccc` 
                    ON ( t1.obs_group_id = ccc.obs_group_id )
                LEFT JOIN (SELECT * 
                            FROM   etl.family_testing_obs_stage_1 
                            WHERE  concept_id = 9701) `t9` 
                        ON ( t1.obs_group_id = t9.obs_group_id ) 
                LEFT JOIN amrs.concept_name cn_in_hei 
                        ON ( cn_in_hei.concept_id = t7.value_coded )
                LEFT JOIN (SELECT * 
                        FROM   etl.family_testing_obs_stage_1 
                        WHERE  concept_id = 11738) `hei_no` 
                    ON ( t1.obs_group_id = hei_no.obs_group_id )
                LEFT JOIN (SELECT * 
                            FROM   etl.family_testing_obs_stage_1 
                            WHERE  concept_id = 11735) `t8` 
                        ON ( t1.obs_group_id = t8.obs_group_id ) 
                LEFT JOIN amrs.concept_name cn_pns
                        ON ( cn_pns.concept_id = t8.value_coded )
                INNER JOIN amrs.person `p` 
                        ON ( t1.person_id = p.person_id ) 
                LEFT JOIN amrs.person_name `person_name` 
                        ON ( t1.person_id = person_name.person_id 
                            AND ( person_name.voided IS NULL 
                                    || person_name.voided = 0 ) 
                            AND person_name.preferred = 1 ) 
                LEFT JOIN amrs.patient_identifier `id` 
                        ON ( t1.person_id = id.patient_id 
                            AND ( id.voided IS NULL 
                                    || id.voided = 0 ) ) 
                LEFT JOIN amrs.person_attribute `contacts` 
                        ON ( t1.person_id = contacts.person_id 
                            AND ( contacts.voided IS NULL 
                                    || contacts.voided = 0 ) 
                            AND contacts.person_attribute_type_id IN ( 10, 48 ) ) 
                LEFT JOIN amrs.person_address `pa` 
                        ON ( t1.person_id = pa.person_id ) 
                LEFT JOIN amrs.patient_program `pp` 
                        ON ( pp.patient_id = p.person_id 
                            AND pp.date_completed IS NULL ) 
                LEFT JOIN amrs.program `pt` 
                        ON ( pp.program_id = pt.program_id )
                LEFT JOIN amrs.location l ON (l.location_id = t1.location_id) 
            WHERE  t1.obs_group_id IS NOT NULL
            GROUP BY t1.person_id, 
                    t1.obs_group_id);
        
       
		SET @dyn_sql=CONCAT('replace into flat_family_testing'                                             
				'(select 
				patient_id,
                patient_uuid,
                obs_group_id,
                identifiers,
                person_name,
                age,
                phone_number,
                nearest_center,
                patient_program_name,
                fm_name,
                relationship_type,
                fm_age,
                fm_status,
                fm_gender,
                fm_dob,
                fm_phone,
                fm_address,
                marital_status,
                living_with_client,
                reported_test_date,
                eligible_for_testing,
                in_care,
                ccc_number,
                in_hei_program,
                hei_number,
                prefered_pns,
                location_id,
                location_uuid
				
				from flat_family_testing_stage_2 t1)');
        
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
						
                        -- delete already built patients
						SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_family_testing_cycle_queue t2 using (person_id);'); 
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
		  if(log = true) then
		  insert into flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
		  end if;
		SELECT 
			CONCAT(@table_version,
					' : Time to complete: ',
					TIMESTAMPDIFF(MINUTE, @start, @end),
					' minutes');
   END