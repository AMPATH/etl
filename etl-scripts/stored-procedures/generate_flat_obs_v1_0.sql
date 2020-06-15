DELIMITER $$
CREATE PROCEDURE `generate_flat_obs_v_1_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
                    set @primary_table := "flat_obs";
                    set @query_type = query_type;
                    set @queue_table = "";
                    set @total_rows_written = 0;
                    
                    set @start = now();
                    set @table_version = "flat_obs_v1.8";

                    set session sort_buffer_size=512000000;

					SELECT @fake_visit_id:=10000000;
					SELECT @boundary:='!!';

                    
                    
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
						INDEX person_date (person_id , encounter_datetime),
						INDEX person_enc_id (person_id , encounter_id),
						INDEX date_created (max_date_created),
						PRIMARY KEY (encounter_id)
					);

					SELECT CONCAT('Created flat_obs_table');

                    if(@query_type="build") then
                            select 'BUILDING..........................................';
                            set @write_table = concat("flat_obs_test_temp_",queue_number);
                            set @queue_table = concat("flat_obs_test_build_queue_",queue_number);                                                                    

                            SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  

                            
                            SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select person_id from flat_obs_test_build_queue limit ', queue_size, ');'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  
                            
                            SET @dyn_sql=CONCAT('delete t1 from flat_obs_test_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  

                    end if;


                    SET @person_ids_count = 0;
                    SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;

                    SELECT @person_ids_count AS 'num patients to sync/build';
                    
                    SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  


                    set @total_time=0;
                    set @cycle_number = 0;
                    

                    while @person_ids_count > 0 do

                        set @loop_start_time = now();
                        
						drop temporary table if exists flat_obs_test_build_queue__0;
                        

                        SET @dyn_sql=CONCAT('create temporary table IF NOT EXISTS flat_obs_test_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;
                        
                        

						CREATE temporary  TABLE IF NOT EXISTS flat_person_encounters__0 (
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
                        SELECT 
                            p.person_id,
                            e.visit_id,
                            e.encounter_id,
                            e.encounter_datetime,
                            e.encounter_type,
                            e.location_id
                        FROM
                            etl.flat_obs_test_build_queue__0 `p`
                        INNER JOIN amrs.encounter `e` on (e.patient_id = p.person_id)  
                        );
                        
                        
                        drop temporary table if exists flat_obs_test__0;

                        create temporary table flat_obs_test__0
                    (select
                        o.person_id,
                        case
							when e.visit_id is not null then e.visit_id else @fake_visit_id :=@fake_visit_id + 1
						end as visit_id,
                        o.encounter_id,
                        e.encounter_datetime,
                        e.encounter_type,
                        e.location_id,
                        group_concat(
                            case
                                when value_coded is not null then concat(@boundary,o.concept_id,'=',value_coded,@boundary)
                                when value_numeric is not null then concat(@boundary,o.concept_id,'=',value_numeric,@boundary)
                                when value_datetime is not null then concat(@boundary,o.concept_id,'=',date(value_datetime),@boundary)
                                -- when value_boolean is not null then concat(@boundary,o.concept_id,'=',value_boolean,@boundary)
                                when value_text is not null then concat(@boundary,o.concept_id,'=',value_text,@boundary)
                                when value_drug is not null then concat(@boundary,o.concept_id,'=',value_drug,@boundary)
                                when value_modifier is not null then concat(@boundary,o.concept_id,'=',value_modifier,@boundary)
                            end
                            order by o.concept_id,value_coded
                            separator ' ## '
                        ) as obs,

                        group_concat(
                            case
                                when value_coded is not null or value_numeric is not null or value_datetime is not null or  value_text is not null or value_drug is not null or value_modifier is not null
                                then concat(@boundary,o.concept_id,'=',date(o.obs_datetime),@boundary)
                            end
                            order by o.concept_id,value_coded
                            separator ' ## '
                        ) as obs_datetimes,
                        max(o.date_created) as max_date_created

                        from amrs.obs o
                        join flat_person_encounters__0 `e` on (e.encounter_id = o.encounter_id)
                        where
		                      o.encounter_id > 1 and o.voided=0
                        group by e.encounter_id
                    );

                    # Add back obs sets without encounter_ids with voided obs removed
                    replace into flat_obs_test__0
                    (select
                        o.person_id,
                        @fake_visit_id :=@fake_visit_id + 1,
                        min(o.obs_id) + 100000000 as encounter_id,
                        o.obs_datetime,
                        99999 as encounter_type,
                        null as location_id,
                        group_concat(
                            case
                                when value_coded is not null then concat(@boundary,o.concept_id,'=',value_coded,@boundary)
                                when value_numeric is not null then concat(@boundary,o.concept_id,'=',value_numeric,@boundary)
                                when value_datetime is not null then concat(@boundary,o.concept_id,'=',date(value_datetime),@boundary)
                                -- when value_boolean is not null then concat(@boundary,o.concept_id,'=',value_boolean,@boundary)
                                when value_text is not null then concat(@boundary,o.concept_id,'=',value_text,@boundary)
                                when value_drug is not null then concat(@boundary,o.concept_id,'=',value_drug,@boundary)
                                when value_modifier is not null then concat(@boundary,o.concept_id,'=',value_modifier,@boundary)
                            end
                            order by o.concept_id,value_coded
                            separator ' ## '
                        ) as obs,

                        group_concat(
                            case
                                when value_coded is not null or value_numeric is not null or value_datetime is not null  or value_text is not null or value_drug is not null or value_modifier is not null
                                then concat(@boundary,o.concept_id,'=',date(o.obs_datetime),@boundary)
                            end
                            order by o.concept_id,value_coded
                            separator ' ## '
                        ) as obs_datetimes,
                        max(o.date_created) as max_date_created

                        from amrs.obs o 
                            join flat_person_encounters__0 `e` using (person_id)
                        where
                            o.encounter_id is null and o.voided=0
                        group by person_id, o.obs_datetime
                    );
                    
                    drop temporary table if exists flat_person_encounters__0;


                    SET @dyn_sql=CONCAT('replace into ',@write_table,                                              
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
                        from flat_obs_test__0 t1)');
                        
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  
                    
                    SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_obs_test_build_queue__0 t2 using (person_id);'); 
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
                      
                        SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_obs_test_build_queue__0 t2 using (person_id);'); 

							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  
                      
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
                        
                        
                        
                        
                        
                      end if;

                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  
                    
                    
                     SET @dyn_sql=CONCAT('drop table ',@write_table,';'); 
					 PREPARE s1 from @dyn_sql; 
					 EXECUTE s1; 
					 DEALLOCATE PREPARE s1;  


 END$$
DELIMITER ;
