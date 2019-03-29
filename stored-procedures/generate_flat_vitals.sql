DELIMITER $$
CREATE PROCEDURE `generate_flat_vitals_v2_1`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
				set session sort_buffer_size=512000000;
				set session group_concat_max_len=100000;
				set @start = now();
				set @primary_table := "flat_vitals_2";
				select @table_version := "flat_vitals_v2.1";
				set @total_rows_written = 0;
				set @query_type = query_type;
                set @queue_number = queue_number;
                set @queue_size = queue_size;
                set @cycle_size = cycle_size;
                
#set @queue_number = 1;
#set @queue_size = 10000;
#set @cycle_size = 1000;
				set @last_date_created = (select max(max_date_created) from etl.flat_obs);


				set @boundary = "!!";
				set @sep = " ## ";

				SET @dyn_sql = CONCAT('create table if not exists ', @primary_table,
						' (date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,						
						person_id int,
						uuid varchar(100),
                        visit_id int,
						encounter_id int,
						encounter_datetime datetime,
						location_id int,
						weight decimal,
						height decimal,
						temp decimal(4,1),
						oxygen_sat int,
						systolic_bp int,
						diastolic_bp int,
						pulse int,
						primary key encounter_id (encounter_id),
						index person_date (person_id, encounter_datetime),
						index person_uuid (uuid),
                        index date_created (date_created)
					)');


                PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;  

				if(@query_type="build") then
						select CONCAT('BUILDING ',@primary_table,'..........................................');
						set @write_table = concat(@primary_table,"_temp_",@queue_number);
						set @queue_table = concat("flat_vitals_build_queue_",@queue_number);                    												

						SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                        
						#create  table if not exists @queue_table (person_id int, primary key (person_id));
						SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,'(encounter_id int primary key) (select * from flat_vitals_build_queue limit ', @queue_size, ');'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
						
						#delete t1 from flat_obs_build_queue t1 join @queue_table t2 using (person_id)
						SET @dyn_sql=CONCAT('delete t1 from flat_vitals_build_queue t1 join ',@queue_table, ' t2 using (encounter_id);'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  

						SET @dyn_sql=CONCAT('select count(*) into @queue_count from ',@queue_table); 

				end if;
	
					
				if (@query_type="sync") then
						select CONCAT('SYNCING ',@primary_table,'..........................................');
						set @write_table = @primary_table;
						set @queue_table = "flat_vitals_sync_queue";
						create table if not exists flat_vitals_sync_queue (encounter_id int primary key);                            
						
						select max(date_updated) into @last_update from etl.flat_log where table_name=@table_version;	
#set @last_update = "2018-01-26";						

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
                    
					SET @dyn_sql= CONCAT('replace into temp_queue_table (select * from ',@queue_table,' limit ', @cycle_size,')');
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1; 
										                    

					drop temporary table if exists flat_vitals_0;
					create temporary table flat_vitals_0 (encounter_id int, primary key (encounter_id), index person_enc_date (person_id,encounter_datetime))
					(select
						t1.person_id,
                        t3.uuid as person_uuid,
                        visit_id,
						t1.encounter_id,
						t1.encounter_datetime,
						t1.encounter_type,
						t1.location_id,
						t1.obs,
						t1.obs_datetimes,

						# 5089 = WEIGHT
						# 5090 = HEIGHT (CM)
						# 5088 = TEMPERATURE (C)
						# 5092 = BLOOD OXYGEN SATURATION
						# 5085 = SYSTOLIC BLOOD PRESSURE
						# 5086 = DIASTOLIC BLOOD PRESSURE
						# 5087 = PULSE

						if(obs regexp "!!5089=",cast(replace(replace((substring_index(substring(obs,locate("!!5089=",obs)),@sep,1)),"!!5089=",""),"!!","") as decimal(4,1)),null) as weight,
						if(obs regexp "!!5090=",cast(replace(replace((substring_index(substring(obs,locate("!!5090=",obs)),@sep,1)),"!!5090=",""),"!!","") as decimal(4,1)),null) as height,
						if(obs regexp "!!5088=",cast(replace(replace((substring_index(substring(obs,locate("!!5088=",obs)),@sep,1)),"!!5088=",""),"!!","") as decimal(4,1)),null) as temp,
						if(obs regexp "!!5092=",cast(replace(replace((substring_index(substring(obs,locate("!!5092=",obs)),@sep,1)),"!!5092=",""),"!!","") as unsigned),null) as oxygen_sat,
						if(obs regexp "!!5085=",cast(replace(replace((substring_index(substring(obs,locate("!!5085=",obs)),@sep,1)),"!!5085=",""),"!!","") as unsigned),null) as systolic_bp,
						if(obs regexp "!!5086=",cast(replace(replace((substring_index(substring(obs,locate("!!5086=",obs)),@sep,1)),"!!5086=",""),"!!","") as unsigned),null) as diastolic_bp,
						if(obs regexp "!!5087=",cast(replace(replace((substring_index(substring(obs,locate("!!5087=",obs)),@sep,1)),"!!5087=",""),"!!","") as unsigned),null) as pulse

						from flat_obs t1
							join temp_queue_table t0 using (encounter_id)
                            join amrs.person t3 using (person_id)
                            where obs regexp '!!(5089|5090|5088|5092|5085|5086|5087)='
					);
                    
					select count(*) as '# rows to write' from flat_vitals_0;

					SET @dyn_sql = CONCAT('replace into ',@write_table,
						'(select
							null,
							person_id,
							person_uuid,
                            visit_id,
							encounter_id,
							encounter_datetime,
							location_id,
							weight,
							height,
							temp,
							oxygen_sat,
							systolic_bp,
							diastolic_bp,
							pulse
						from flat_vitals_0)');                        
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
