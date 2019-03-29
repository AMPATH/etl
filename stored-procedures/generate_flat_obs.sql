DELIMITER $$
CREATE PROCEDURE `generate_flat_obs_v2_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN

				set session group_concat_max_len=100000;
				set @start = now();
				set @primary_table := "flat_obs_2";
				set @table_version = "flat_obs_v2.0";
				set @query_type = query_type;
				set @total_rows_written = 0;
                set @queue_number = queue_number;
                set @queue_size = queue_size;
                set @cycle_size = cycle_size;
                
#set @queue_number = 1;
#set @queue_size = 10000;
#set @cycle_size = 1000;
                
				select max(date_created) into @last_date_created_enc from amrs.encounter;
				select max(date_created) into @last_date_created_obs from amrs.obs;
				set @last_date_created = if(@last_date_created_enc > @last_date_created_obs,@last_date_created_enc,@last_date_created_obs);


				set @boundary = "!!";

				SET @dyn_sql = CONCAT('create table if not exists ', @primary_table,
						'(date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,						
                        person_id int,
						visit_id int,
						encounter_id int,
						encounter_datetime datetime,
						encounter_type int,
						location_id int,
						obs text,
						obs_datetimes text,
						max_date_created datetime,
						index encounter_id (encounter_id),
						index person_date (person_id, encounter_datetime),
						index person_enc_id (person_id,encounter_id),
						index max_date_created (max_date_created),
						index date_created (date_created),
						primary key (encounter_id)                                                
						);');
                PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;  

				if(@query_type="build") then
						select CONCAT('BUILDING ',@primary_table,'..........................................');
						set @write_table = concat(@primary_table,"_temp_",@queue_number);
						set @queue_table = concat("flat_obs_build_queue_",@queue_number);                    												

						SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                        

						#create  table if not exists @queue_table (person_id int, primary key (person_id));
						SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,'(encounter_id int primary key) (select * from flat_obs_build_queue limit ', @queue_size, ');'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
						
						#delete t1 from flat_obs_build_queue t1 join @queue_table t2 using (person_id)
						SET @dyn_sql=CONCAT('delete t1 from flat_obs_build_queue t1 join ',@queue_table, ' t2 using (encounter_id);'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  

						SET @dyn_sql=CONCAT('select count(*) into @queue_count from ',@queue_table); 

				end if;
	
					
				if (@query_type="sync") then
						select 'SYNCING FLAT_OBS..........................................';
						set @write_table = @primary_table;
						set @queue_table = "flat_obs_sync_queue";
						create table if not exists flat_obs_sync_queue (encounter_id int primary key);                            
						
						select max(date_updated) into @last_update from etl.flat_log where table_name=@table_version;	
set @last_update = "2018-01-26";						

						# find all encounters that have date_created, date_voided, or date_changed after @last_update
                        SET @dyn_sql=CONCAT('replace into ',@queue_table,
								'(select encounter_id 
									from amrs.encounter t1
                                    where 									
										CASE
											when t1.date_created >= @last_update and t1.voided=0 then 1
                                            when t1.date_created <= @last_update and t1.date_voided > @last_update then 1
                                            when t1.date_created <= @last_update and t1.date_changed > @last_update then 1
                                            else 0
										end
								)');
                        PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                        

						#find all encounters which have a voided obs since last update
						SET @dyn_sql=CONCAT('replace into ',@queue_table,
								'(select encounter_id
									from amrs.obs t1
                                    where t1.date_created <= @last_update
										AND t1.date_voided > @last_updated
                                        AND t1.encounter_id > 0
								)');
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  

				end if;

                    
				# delete all rows in primary table in the queue
				SET @dyn_sql=CONCAT('delete t1 from ',@primary_table,' t1 join ',@queue_table, ' t2 using (encounter_id)');
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;  
															
				SET @dyn_sql=CONCAT('select count(*) into @queue_count from ',@queue_table); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;                        

				set @total_time=0;
				set @cycle_number = 0;
				
				while @queue_count > 0 do

					set @loop_start_time = now();
					
					#create temp table with a set of person ids
					drop temporary table if exists temp_queue_table;

                    SET @dyn_sql = CONCAT('create temporary table temp_queue_table like ',@queue_table);
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1; 
                    
					SET @dyn_sql= CONCAT('replace into temp_queue_table
						(select * from ',@queue_table,' limit ', @cycle_size,')');

					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1; 
										                    
					select count(*) as '# rows to write' from temp_queue_table;

					SET @dyn_sql = CONCAT('replace into ',@write_table,
						'(select
							null,
							o.person_id,
							e.visit_id,
							o.encounter_id,
							e.encounter_datetime,
							e.encounter_type,
							e.location_id,
							group_concat(
								case
									when value_coded is not null then concat(@boundary,o.concept_id,\'=\',value_coded,@boundary)
									when value_numeric is not null then concat(@boundary,o.concept_id,\'=\',value_numeric,@boundary)
									when value_datetime is not null then concat(@boundary,o.concept_id,\'=\',date(value_datetime),@boundary)
									when value_text is not null then concat(@boundary,o.concept_id,\'=\',value_text,@boundary)
									when value_drug is not null then concat(@boundary,o.concept_id,\'=\',value_drug,@boundary)
									when value_modifier is not null then concat(@boundary,o.concept_id,\'=\',value_modifier,@boundary)
								end
								order by o.concept_id,value_coded
								separator \' ## \'
							) as obs,

							group_concat(
								case
									when value_coded is not null or value_numeric is not null or value_datetime is not null or value_boolean is not null or value_text is not null or value_drug is not null or value_modifier is not null
									then concat(@boundary,o.concept_id,\'=\',date(o.obs_datetime),@boundary)
								end
								order by o.concept_id,value_coded
								separator \' ## \'
							) as obs_datetimes,
							max(o.date_created) as max_date_created

							from temp_queue_table
								join amrs.obs o using (encounter_id)
								join amrs.encounter e using (encounter_id)
							where
								o.encounter_id > 0 and o.voided=0
							group by encounter_id
						);');
                        
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1; 


					SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join temp_queue_table t2 using (encounter_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
					SET @dyn_sql=CONCAT('select count(*) into @queue_count from ',@queue_table,';'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  

					set @cycle_length = timestampdiff(second,@loop_start_time,now());
                    set @total_time = @total_time + @cycle_length;
                    set @cycle_number = @cycle_number + 1;
                    
                    #select ceil(@person_ids_count / cycle_size) as remaining_cycles;
                    set @remaining_time = ceil((@total_time / @cycle_number) * ceil(@queue_count / @cycle_size) / 60);
                    #select concat("Estimated time remaining: ", @remaining_time,' minutes');

					select @queue_count as '# in queue', @cycle_length as 'Cycle Time (s)', ceil(@queue_count / @cycle_size) as remaining_cycles, @remaining_time as 'Est time remaining (min)';

			end while;

			SET @dyn_sql = CONCAT(
					'delete t1
					from ',@primary_table, ' t1
					join amrs.person t2 using (person_id)
					where t2.voided=1;');
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
						select concat(@start_write, " : Writing ",@total_rows_to_write, ' to ',@primary_table);

						SET @dyn_sql=CONCAT('replace into ', @primary_table,
							'(select * from ',@write_table,');');
                        PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;
						
                        set @finish_write = now();
                        set @time_to_write = timestampdiff(second,@start_write,@finish_write);
                        select concat(@finish_write, ' : Completed writing rows. Time to write to primary table: ', @time_to_write, ' seconds ');                        
                        
                        SET @dyn_sql=CONCAT('drop table ',@write_table,';'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  											
				end if;
				
				select @end := now();
				insert into flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
				select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");

	END$$
DELIMITER ;
