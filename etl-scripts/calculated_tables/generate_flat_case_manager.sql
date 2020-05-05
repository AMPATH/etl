use etl;
drop procedure if exists generate_flat_case_manager;
DELIMITER $$
CREATE DEFINER=`etl_user`@`%` PROCEDURE `generate_flat_case_manager`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int,  IN person_ids varchar(500))
BEGIN
                    set @primary_table := "flat_case_manager";
                    set @query_type = query_type;
                    set @queue_table = "";
                    set @total_rows_written = 0;
                    
                    set @start = now();
                    set @table_version = "flat_case_manager_v1";

                    set @sep = " ## ";
                    set @last_date_created = (select max(max_date_created) from etl.flat_obs);

                       -- drop TABLE IF EXISTS flat_case_manager;
						CREATE TABLE IF NOT EXISTS flat_case_manager (
							date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
							person_id INT,
							uuid VARCHAR(100),
							visit_id INT,
							visit_type SMALLINT,
							encounter_id INT,
							encounter_datetime DATETIME,
							encounter_type INT,
							is_clinical_encounter INT,
							location_id INT,
							location_uuid VARCHAR(100),
							enrollment_date DATETIME,
							transfer_out TINYINT,
							rtc_date DATETIME,
							med_pickup_rtc_date DATETIME,
							arv_first_regimen_start_date DATETIME,
							arv_start_date DATETIME,
							prev_arv_start_date DATETIME,
							prev_arv_adherence VARCHAR(200),
							cur_arv_adherence VARCHAR(200),
							is_pregnant BOOLEAN,
							edd DATETIME,
							vl_resulted INT,
							vl_resulted_date DATETIME,
							vl_1 INT,
							vl_1_date DATETIME,
							vl_order_date DATETIME,
                            effective_rtc DATETIME,
                            phone_encounter_datetime DATETIME,
                            next_phone_appointment DATETIME,
							phone_planned_rtc DATETIME,
                            gender varchar(50),
                            birthdate DATETIME,
                            case_manager_user_id INT,
                            case_manager_person_id INT,
                            case_manager_user_name varchar(100),
                            case_manager_name varchar(250),
                            person_name varchar(250),
                            identifiers varchar(250),
							PRIMARY KEY person_id (person_id),
							INDEX person_uuid (uuid),
							INDEX location_id (location_id),
							INDEX encounter_datetime (encounter_datetime),
							INDEX rtc_date (rtc_date),
                            INDEX next_phone_appointment (location_id, next_phone_appointment),
							INDEX med_pickup_rtc_date (med_pickup_rtc_date),
							INDEX location_enc_date (location_id , encounter_datetime),
							INDEX location_id_rtc_date (location_id , rtc_date),
                            INDEX location_id_effective_rtc (location_id , effective_rtc),
							INDEX location_id_med_rtc_date (location_id , med_pickup_rtc_date),
							INDEX encounter_type (encounter_type),
							INDEX date_created (date_created)
						);
                    
                    
                                        
                    if(@query_type="build") then
                            select 'BUILDING..........................................';
                            set @write_table = concat("flat_case_manager_temp_",queue_number);
                            set @queue_table = concat("flat_case_manager_build_queue_",queue_number);                                                                    

                            SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  

                            
                            SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_case_manager_build_queue limit ', queue_size, ');'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  
                            
                            
                            SET @dyn_sql=CONCAT('delete t1 from flat_case_manager_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  

                    end if;
    
				   if (@query_type="sync_ids") then
                            select 'SYNCING IDS..........................................';
                            set @write_table = "flat_case_manager";
                            set @queue_table = "flat_case_manager_sync_ids_queue";
							CREATE TEMPORARY TABLE IF NOT EXISTS flat_case_manager_sync_ids_queue (
								person_id INT PRIMARY KEY
							); 
                            
                            SET @sql = CONCAT('replace into flat_case_manager_sync_ids_queue(SELECT person_id FROM amrs.person WHERE person_id IN (', person_ids, '))');
                            select @sql;
							PREPARE stmt FROM @sql;
							EXECUTE stmt;
							DEALLOCATE PREPARE stmt;
                            
                    end if;

                    if (@query_type="sync") then
                            select 'SYNCING..........................................';
                            set @write_table = "flat_case_manager";
                            set @queue_table = "flat_case_manager_sync_queue";
							CREATE TABLE IF NOT EXISTS flat_case_manager_sync_queue (
								person_id INT PRIMARY KEY
							);                            
                            
                            set @last_update = null;
							SELECT 
								MAX(date_updated)
							INTO @last_update FROM
								etl.flat_log
							WHERE
								table_name = @table_version;
                            -- set @last_update = "1900-01-01";
                            replace into flat_case_manager_sync_queue
                            (select distinct person_id
                                from etl.flat_obs
                                where max_date_created > @last_update
                            );
                            
                            /*replace into flat_case_manager_sync_queue
                            (select distinct person_id
                                from etl.flat_obs
                                where max_date_created > @last_update
                            );

                            replace into flat_case_manager_sync_queue
                            (select distinct person_id
                                from etl.flat_lab_obs
                                where max_date_created > @last_update
                            );

                            replace into flat_case_manager_sync_queue
                            (select distinct person_id
                                from etl.flat_orders
                                where max_date_created > @last_update
                            );

                            replace into flat_case_manager_sync_queue
                            (select person_id from 
                                amrs.person 
                                where date_changed > @last_update);
							
                            replace into flat_case_manager_sync_queue
                            (select person_id from 
                                amrs.person_attribute 
                                where date_changed > @last_update and person_attribute_type_id=68); */
                                
							replace into flat_case_manager_sync_queue   
							(SELECT 
									distinct person_id
								FROM
									etl.flat_hiv_summary_v15b
								WHERE
									encounter_datetime > '2019-08-31' and date_created > @last_update);
                            

                      end if;
                      

                    -- Delete test patients from queue
                    SET @dyn_sql=CONCAT('delete t1 FROM ',@queue_table,' t1
                            join amrs.person_attribute t2 using (person_id)
                            where t2.person_attribute_type_id=68 and value="true" and voided=0');
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  

                    -- count patients in queue
                    SET @person_ids_count = 0;
                    SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;
                    SELECT @person_ids_count AS 'num patients to sync';

                    set @total_time=0;
                    set @cycle_number = 0;
                    
                    while @person_ids_count > 0 do
                        set @loop_start_time = now();
                        
                        drop temporary table if exists flat_case_manager_cycle_queue;
                        SET @dyn_sql=CONCAT('create temporary table flat_case_manager_cycle_queue (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;  

                        drop temporary table if exists stage_1;
                        create temporary table stage_1(key person_id(person_id)) -- creates one patient per row with the latest values for that patient
                        ( SELECT 
								person_id,
								uuid,
								visit_id,
								visit_type,
								encounter_id,
								encounter_datetime,
								encounter_type ,
								is_clinical_encounter,
								location_id,
								location_uuid,
								enrollment_date,
								transfer_out,
								rtc_date,
								med_pickup_rtc_date,
								arv_first_regimen_start_date,
								arv_start_date,
								prev_arv_start_date,
								prev_arv_adherence,
								cur_arv_adherence,
								is_pregnant,
								edd,
								vl_resulted,
								vl_resulted_date,
								vl_1,
								vl_1_date,
								vl_order_date
							FROM
								(SELECT 
									*
								FROM
									etl.flat_hiv_summary_v15b
                                    join flat_case_manager_cycle_queue q using (person_id)
								WHERE
									encounter_datetime > '2019-08-31' and (is_clinical_encounter = 1 or encounter_type = 212)
								ORDER BY person_id DESC, encounter_datetime DESC) hiv
							GROUP BY person_id order by person_id desc
                        );
                        
                        
                        
                        drop temporary table if exists telecare_encounters;
                         create temporary table telecare_encounters(key person_id(person_id))
						( 
							SELECT 
								person_id,
								encounter_id,
								encounter_datetime AS phone_encounter_datetime,
								CASE
									WHEN
										obs REGEXP '!!9166='
									THEN
										REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(obs, LOCATE('!!9166=', obs)),
														@sep,
														1)),
												'!!9166=',
												''),
											'!!',
											'')
									ELSE NULL
								END AS next_phone_appointment,
								CASE
									WHEN
										obs REGEXP '!!5096='
									THEN
										REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(obs, LOCATE('!!5096=', obs)),
														@sep,
														1)),
												'!!5096=',
												''),
											'!!',
											'')
									ELSE NULL
								END AS phone_planned_rtc
							FROM
								(SELECT 
									fo.person_id, fo.encounter_id, fo.obs, fo.encounter_datetime
								FROM
									etl.flat_obs fo
                                    join flat_case_manager_cycle_queue q using (person_id)
								WHERE
									fo.encounter_type = 212
								ORDER BY fo.encounter_datetime DESC) o
							GROUP BY person_id
                        );
                        
                        drop temporary table if exists stage_2;
                        create temporary table stage_2(key person_id(person_id))
						(SELECT 
							h.*, 
                            te.phone_planned_rtc,
                            te.next_phone_appointment,
                            te.phone_encounter_datetime,
                            case 
                                when h.med_pickup_rtc_date is not null then h.med_pickup_rtc_date
								when te.phone_planned_rtc is not null then te.phone_planned_rtc
                                else h.rtc_date
                           end as effective_rtc,
							p.gender, 
							p.birthdate, 
							pa.value AS case_manager_user_id, 
							u.person_id AS case_manager_person_id, 
							u.username as case_manager_user_name,
							CONCAT(COALESCE(un.given_name, ''),
									' ',
									COALESCE(un.middle_name, ''),
									' ',
									COALESCE(un.family_name, '')) AS case_manager_name,
							CONCAT(COALESCE(pn.given_name, ''),
									' ',
									COALESCE(pn.middle_name, ''),
									' ',
									COALESCE(pn.family_name, '')) AS person_name,
							GROUP_CONCAT(DISTINCT id.identifier
								SEPARATOR ', ') AS `identifiers`
						FROM
							stage_1 h
                            left outer join 
                            telecare_encounters te on (h.person_id = te.person_id)
								left outer join
							amrs.person_attribute pa ON (h.person_id = pa.person_id
								AND pa.person_attribute_type_id = 68)
								left outer join
							amrs.users u ON (u.user_id = pa.value)
								JOIN
							amrs.person p on (h.person_id = p.person_id) 
							 INNER JOIN
							amrs.person_name pn ON (h.person_id = pn.person_id
								AND (pn.voided IS NULL
								|| pn.voided = 0))
							left outer join
						   amrs.person_name un ON (u.person_id = un.person_id
								AND (un.voided IS NULL
								|| un.voided = 0))
							LEFT JOIN
							amrs.patient_identifier `id` ON (h.person_id = id.patient_id
								AND (id.voided IS NULL || id.voided = 0))
								group by h.person_id);                        
         
						SET @dyn_sql=CONCAT('replace into ',@write_table,                                              
							'(select
                            null,
							person_id,
							uuid,
							visit_id,
							visit_type,
							encounter_id,
							encounter_datetime,
							encounter_type,
							is_clinical_encounter,
							location_id,
							location_uuid,
							enrollment_date,
							transfer_out,
							rtc_date,
							med_pickup_rtc_date,
							arv_first_regimen_start_date,
							arv_start_date,
							prev_arv_start_date,
							prev_arv_adherence,
							cur_arv_adherence,
							is_pregnant,
							edd,
							vl_resulted,
							vl_resulted_date,
							vl_1,
							vl_1_date,
							vl_order_date,
                            effective_rtc,
                            phone_encounter_datetime,
                            next_phone_appointment,
							phone_planned_rtc,
                            gender,
                            birthdate,
                            case_manager_user_id,
                            case_manager_person_id,
                            case_manager_user_name,
                            case_manager_name,
                            person_name,
                            identifiers
							from stage_2)');
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
						
                        -- delete already built patients
						SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_case_manager_cycle_queue t2 using (person_id);'); 
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
                 
                if(@query_type="build") then -- for build, queue tables and insert records from temp writing table to primary table
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
                 #if (@query_type="sync") then
                 insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
                 #end if;
				SELECT 
					CONCAT(@table_version,
							' : Time to complete: ',
							TIMESTAMPDIFF(MINUTE, @start, @end),
							' minutes');

        END$$
DELIMITER ;
