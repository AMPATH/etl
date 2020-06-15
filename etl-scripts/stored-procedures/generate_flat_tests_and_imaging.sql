#********************************************************************************************************
#* CREATION OF LABS AND IMAGING FLAT TABLE ****************************************************************************
#********************************************************************************************************

# Need to first create this temporary table to sort the data by person,encounterdateime.
# This allows us to use the previous row's data when making calculations.
# It seems that if you don't create the temporary table first, the sort is applied
# to the final result. Any references to the previous row will not an ordered row.

# v2.1 Notes:
#      Added encounter types for GENERALNOTE (112), CLINICREVIEW (113), MOH257BLUECARD (114), HEIFOLLOWUP (115), TRANSFERFORM (116)
#      Added timestamp to log

# v2.2 Notes:
#      Add ability to handle error messages. Add columns for has_errors, vl_error, cd4_error, hiv_dna_pcr_error
#      Delete all rows for a patient before inserting new data

# v2.5 Notes:
#		use date(t1.test_datetime) = date(t2.encounter_datetime) joining column for flat_lab_obs table instead of encounter_id


use etl;
drop procedure if exists generate_flat_labs_and_imaging_v3_0;

DELIMITER $$
	CREATE PROCEDURE generate_flat_labs_and_imaging_v3_0(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
		BEGIN
				set session sort_buffer_size=512000000;
				set session group_concat_max_len=100000;
				set @start = now();
				set @primary_table := "flat_labs_and_imaging";
				select @table_version := 'flat_labs_and_imaging_v3.0';
				set @total_rows_written = 0;
				set @query_type = query_type;
                set @queue_number = queue_number;
                set @queue_size = queue_size;
                set @cycle_size = cycle_size;

				# 1030 = HIV DNA PCR
				# 1040 = HIV Rapid test
				# 856 = HIV VIRAL LOAD, QUANTITATIVE
				# 5497 = CD4, BY FACS
				# 730 = CD4%, BY FACS
				# 21 = HEMOGLOBIN
				# 653 = AST
				# 790 = SERUM CREATININE
				# 12 = X-RAY, CHEST, PRELIMINARY FINDINGS
				# 6126 = hba1c
				# 887 = rbs
				# 6252 = fbs
				# 1537 = ecg
                # 857 = SERUM BLOOD UREA NITROGEN
				# 1271 = TESTS ORDERED
				# 9239 = LABORATORY TEST WITH EXCEPTION
				# 9020 = LAB ERROR
                set @concept_ids = '(1030, 1040, 856, 5497, 730, 21,653,790,12,6126,887,6252,1537,1271,9239,9020,857)';
                
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
							encounter_id int,
							test_datetime datetime,
							encounter_type int,
							hiv_dna_pcr int,
							hiv_rapid_test int,
							hiv_viral_load int,
							cd4_count int,
							cd4_percent decimal,
							hemoglobin decimal,
							ast int,
							creatinine decimal,
							chest_xray int,
							hba1c decimal,
							rbs decimal,
							fbs decimal,
							ecg int,
							urea decimal,
							has_errors text,
							vl_error boolean,
							cd4_error boolean,
							hiv_dna_pcr_error boolean,

							tests_ordered varchar(1000),
							primary key encounter_id (encounter_id),
							index person_date (person_id, test_datetime),
							index person_uuid (uuid)
					)');


                PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;  

				if(@query_type="build") then
		select CONCAT('BUILDING ',@primary_table,'..........................................');
						set @write_table = concat(@primary_table,"_temp_",@queue_number);
                        set @build_queue = concat(@primary_table,'_build_queue');
						set @queue_table = concat(@build_queue,'_',@queue_number);                    												


						SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                        
						#create  table if not exists @queue_table (person_id int, primary key (person_id));
						SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,'(encounter_id int primary key) (select * from ',@build_queue,' limit ', @queue_size, ');'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
						
						#delete t1 from flat_obs_build_queue t1 join @queue_table t2 using (person_id)
						SET @dyn_sql=CONCAT('delete t1 from ',@build_queue,' t1 join ',@queue_table, ' t2 using (encounter_id);'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  

						SET @dyn_sql=CONCAT('select count(*) into @queue_count from ',@queue_table); 

				end if;
	
					
				if (@query_type="sync") then
		select CONCAT('SYNCING ',@primary_table,'..........................................');
						set @write_table = @primary_table;
						set @queue_table = concat(@primary_table,'_sync_queue');
						
						select max(date_updated) into @last_update from etl.flat_log where table_name=@table_version;	
#set @last_update = "2018-01-26";						
create table flat_labs_and_imaging_sync_queue (person_id int, obs_datetime datetime, index test_datetime (person_id, obs_datetime));

						# find all encounters that have date_created, date_voided, or date_changed after @last_update
                        SET @dyn_sql=CONCAT(
									'replace into ',@queue_table,
									'(select person_id, test_datetime 
										from flat_lab_obs
										where date_created > @last_update
									)'
                                );
                        PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                        
                        

						#find all voided obs since last update
						SET @dyn_sql=CONCAT('replace into ',@queue_table,
								'(select person_id, obs_datetime
									from amrs.obs t1
                                    where t1.date_voided > @last_updated
										and concept_id in ',@concept_ids,'
									group by person_id, obs_datetime)'
								);
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
										                    


                    
					select count(*) as '# rows to write' from flat_vitals_0;

					SET @dyn_sql = CONCAT('replace into ',@write_table,
						'(
                        
                        
                        )');                  
											                        


replace into flat_labs_and_imaging_sync_queue
(select person_id, test_datetime from flat_test_obs);


drop table if exists flat_labs_and_imaging_0;
set @queue_table = 'flat_labs_and_imaging_sync_queue';

					SET @dyn_sql= CONCAT(
									'create temporary table flat_labs_and_imaging_0(primary key (encounter_id), index encounter_id (encounter_id), index person_test (person_id,test_datetime))
									(select t1.person_id,
										t1.encounter_id,
										t1.test_datetime,
										t1.encounter_type,
										t1.location_id,
										t1.obs,
										t3.orders,
										if(obs regexp "!!1030=",cast(replace(replace((substring_index(substring(obs,locate("1030=",obs)),@sep,1)),"1030=",""),"!!","") as unsigned),null) as hiv_dna_pcr,
										if(obs regexp "!!1040=",cast(replace(replace((substring_index(substring(obs,locate("1040=",obs)),@sep,1)),"1040=",""),"!!","") as unsigned),null) as hiv_rapid_test,
										case
											when obs regexp "!!856=" then cast(replace(replace((substring_index(substring(obs,locate("856=",obs)),@sep,1)),"856=",""),"!!","") as unsigned)
										end as hiv_viral_load,
										if(obs regexp "!!5497=",cast(replace(replace((substring_index(substring(obs,locate("5497=",obs)),@sep,1)),"5497=",""),"!!","") as unsigned),null) as cd4_count,
										if(obs regexp "!!730=",cast(replace(replace((substring_index(substring(obs,locate("730=",obs)),@sep,1)),"730=",""),"!!","") as decimal(3,1)),null) as cd4_percent,
										if(obs regexp "!!21=",cast(replace(replace((substring_index(substring(obs,locate("21=",obs)),@sep,1)),"21=",""),"!!","") as decimal(4,1)),null) as hemoglobin,
										if(obs regexp "!!653=",cast(replace(replace((substring_index(substring(obs,locate("653=",obs)),@sep,1)),"653=",""),"!!","") as unsigned),null) as ast,
										if(obs regexp "!!790=",cast(replace(replace((substring_index(substring(obs,locate("790=",obs)),@sep,1)),"790=",""),"!!","") as decimal(4,1)),null) as creatinine,
										if(obs regexp "!!12=" and not obs regexp "!!12=1107",cast(replace(replace((substring_index(substring(obs,locate("12=",obs)),@sep,1)),"12=",""),"!!","") as unsigned),null) as chest_xray,
										if(obs regexp "!!6126=",cast(replace(replace((substring_index(substring(obs,locate("6126=",obs)),@sep,1)),"6126=",""),"!!","") as decimal(4,1)),null) as hba1c,
										if(obs regexp "!!887=",cast(replace(replace((substring_index(substring(obs,locate("887=",obs)),@sep,1)),"887=",""),"!!","") as decimal(4,1)),null) as rbs,
										if(obs regexp "!!6252=",cast(replace(replace((substring_index(substring(obs,locate("6252=",obs)),@sep,1)),"6252=",""),"!!","") as decimal(4,1)),null) as fbs,
										if(obs regexp "!!1537=",cast(replace(replace((substring_index(substring(obs,locate("1537=",obs)),@sep,1)),"1537=",""),"!!","") as unsigned),null) as ecg,
										if(obs regexp "!!857=",cast(replace(replace((substring_index(substring(obs,locate("857=",obs)),@sep,1)),"857=",""),"!!","") as decimal(4,1)),null) as urea,
										if(obs regexp "!!9239=",obs,null) as has_errors,
										if(obs regexp "!!9239=856!!",1,null) as vl_error,
										if(obs regexp "!!9239=5497!!",1,null) as cd4_error,
										if(obs regexp "!!9239=1030",1,null) as hiv_dna_pcr_error,
										CONCAT(
											case
												when obs regexp "!!1271=" then
													replace(replace((substring_index(substring(obs,locate("!!1271=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!1271=", "") ) ) / LENGTH("!!1271=") ))),"!!1271=",""),"!!","")
												else ""
											end,
                                            ifnull(orders,"")
										) as tests_ordered
                                        
										from flat_test_obs t1 ##NEED TO UPDATE 
											join ', @queue_table,' t2 on t1.person_id = t2.person_id and t1.test_datetime = t2.obs_datetime
											left join flat_orders t3 on t1.test_datetime = t3.encounter_datetime and t1.person_id = t3.person_id
									)'
								);
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1; 	


					set @last_update = '2016-03-01';
					SET @dyn_sql= CONCAT(
									'insert into flat_labs_and_imaging_0 (person_id, encounter_id, test_datetime, encounter_type, location_id, orders,tests_ordered)
									( select t1.person_id,
										t1.encounter_id,
										t1.encounter_datetime as test_datetime,
										t3.encounter_type,
										t3.location_id,
										t1.orders,
                                        0 as tests_ordered
										from flat_orders t1
                                            join amrs.encounter t2 using (encounter_id)
											left outer join flat_test_obs t3 ON t1.person_id = t3.person_id AND t1.encounter_datetime = t3.test_datetime 
											where t1.encounter_datetime >= @last_update and t3.person_id is null
									)'
								);
					PREPARE s1 from @dyn_sql;
					EXECUTE s1;
					DEALLOCATE PREPARE s1;
                    
                    SET @dyn_sql = CONCAT(
						'insert into ',@write_table,
						'(select
							person_id,
							t1.uuid,
							encounter_id,
							test_datetime,
							encounter_type,
							hiv_dna_pcr,
							hiv_rapid_test,
							hiv_viral_load,
							cd4_count,
							cd4_percent,
							hemoglobin,
							ast,
							creatinine,
							chest_xray,
							hba1c,
							rbs,
							fbs,
							ecg,
							urea,
							has_errors,
							vl_error,
							cd4_error,
							hiv_dna_pcr_error,
							tests_ordered
						from flat_labs_and_imaging_0
						)'
					);
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  



					SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join temp_queue_table t2 using (person_id, obs_datetime);'); 
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

	END $$
DELIMITER ;


set session sort_buffer_size=512000000;
set session group_concat_max_len=100000;

select @sep := " ## ";
select @unknown_encounter_type := 99999;


select @start := now();
select @last_date_created := (select max(max_date_created) from flat_lab_obs);

select @last_update := (select max(date_updated) from flat_log where table_name=@table_version);

# then use the max_date_created from amrs.encounter. This takes about 10 seconds and is better to avoid.
select @last_update :=
	if(@last_update is null,
		(select max(date_created) from amrs.encounter e join flat_labs_and_imaging using (encounter_id)),
		@last_update);

#otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
select @last_update := if(@last_update,@last_update,'1900-01-01');
#select @last_update := "2015-05-14";


delete t1
from flat_labs_and_imaging t1
join new_data_person_ids_0 t2 using (person_id);

drop table if exists flat_labs_and_imaging_0;
create temporary table flat_labs_and_imaging_0(index encounter_id (encounter_id), index person_test (person_id,test_datetime))
(select * from
((select t1.person_id,
	t1.encounter_id,
	t1.test_datetime,
	t1.encounter_type,
	t1.location_id,
	t1.obs,
	t2.orders
	from flat_lab_obs t1
		join new_data_person_ids_0 t0 using (person_id)
		left join flat_orders t2 on date(t1.test_datetime) = date(t2.encounter_datetime) and t1.person_id = t2.person_id)
        UNION ALL
( select t1.person_id,
	t1.encounter_id,
	t1.encounter_datetime as test_datetime,
	t2.encounter_type,
	t2.location_id,
	t2.obs,
	t1.orders
	from flat_orders t1
		left join flat_lab_obs t2 on date(t2.test_datetime) = date(t1.encounter_datetime) and t1.person_id = t2.person_id
        where t1.encounter_datetime >= @last_update and t2.person_id is null

) order by person_id, test_datetime) as derived );


select @prev_id := null;
select @cur_id := null;
select @cur_location := null;
select @vl := null;
select @cd4_count := null;
Select @cd4_percent := null;
select @hemoglobin := null;
select @ast := null;
select @creatinine := null;
select @chest_xray := null;

drop temporary table if exists flat_labs_and_imaging_1;
create temporary table flat_labs_and_imaging_1 (index encounter_id (encounter_id))
(select
	@prev_id := @cur_id as prev_id,
	@cur_id := t1.person_id as cur_id,
	t1.person_id,
	p.uuid,
	t1.encounter_id,
	t1.test_datetime,
	t1.encounter_type,

	case
		when location_id then @cur_location := location_id
		when @prev_id = @cur_id then @cur_location
		else null
	end as location_id,

	# 1030 = HIV DNA PCR
	# 1040 = HIV Rapid test
	# 856 = HIV VIRAL LOAD, QUANTITATIVE
	# 5497 = CD4, BY FACS
	# 730 = CD4%, BY FACS
	# 21 = HEMOGLOBIN
	# 653 = AST
	# 790 = SERUM CREATININE
	# 12 = X-RAY, CHEST, PRELIMINARY FINDINGS
	# 6126 = hba1c
	# 887 = rbs
	# 6252 = fbs
	# 1537 = ecg
	# 1537 = urea
	# 1271 = TESTS ORDERED
	# 9239 = LABORATORY TEST WITH EXCEPTION
    # 9020 = LAB ERROR

	if(obs regexp "!!1030=",cast(replace(replace((substring_index(substring(obs,locate("1030=",obs)),@sep,1)),"1030=",""),"!!","") as unsigned),null) as hiv_dna_pcr,
	if(obs regexp "!!1040=",cast(replace(replace((substring_index(substring(obs,locate("1040=",obs)),@sep,1)),"1040=",""),"!!","") as unsigned),null) as hiv_rapid_test,

	case
		when obs regexp "!!856=" then cast(replace(replace((substring_index(substring(obs,locate("856=",obs)),@sep,1)),"856=",""),"!!","") as unsigned)
	end as hiv_viral_load,
	if(obs regexp "!!5497=",cast(replace(replace((substring_index(substring(obs,locate("5497=",obs)),@sep,1)),"5497=",""),"!!","") as unsigned),null) as cd4_count,
	if(obs regexp "!!730=",cast(replace(replace((substring_index(substring(obs,locate("730=",obs)),@sep,1)),"730=",""),"!!","") as decimal(3,1)),null) as cd4_percent,
	if(obs regexp "!!21=",cast(replace(replace((substring_index(substring(obs,locate("21=",obs)),@sep,1)),"21=",""),"!!","") as decimal(4,1)),null) as hemoglobin,
	if(obs regexp "!!653=",cast(replace(replace((substring_index(substring(obs,locate("653=",obs)),@sep,1)),"653=",""),"!!","") as unsigned),null) as ast,
	if(obs regexp "!!790=",cast(replace(replace((substring_index(substring(obs,locate("790=",obs)),@sep,1)),"790=",""),"!!","") as decimal(4,1)),null) as creatinine,
	if(obs regexp "!!12=" and not obs regexp "!!12=1107",cast(replace(replace((substring_index(substring(obs,locate("12=",obs)),@sep,1)),"12=",""),"!!","") as unsigned),null) as chest_xray,
	if(obs regexp "!!6126=",cast(replace(replace((substring_index(substring(obs,locate("6126=",obs)),@sep,1)),"6126=",""),"!!","") as decimal(4,1)),null) as hba1c,
	if(obs regexp "!!887=",cast(replace(replace((substring_index(substring(obs,locate("887=",obs)),@sep,1)),"887=",""),"!!","") as decimal(4,1)),null) as rbs,
	if(obs regexp "!!6252=",cast(replace(replace((substring_index(substring(obs,locate("6252=",obs)),@sep,1)),"6252=",""),"!!","") as decimal(4,1)),null) as fbs,
	if(obs regexp "!!1537=",cast(replace(replace((substring_index(substring(obs,locate("1537=",obs)),@sep,1)),"1537=",""),"!!","") as unsigned),null) as ecg,
	if(obs regexp "!!857=",cast(replace(replace((substring_index(substring(obs,locate("857=",obs)),@sep,1)),"857=",""),"!!","") as decimal(4,1)),null) as urea,
	if(obs regexp "!!9239=",obs,null) as has_errors,
    if(obs regexp "!!9239=856!!",1,null) as vl_error,
    if(obs regexp "!!9239=5497!!",1,null) as cd4_error,
    if(obs regexp "!!9239=1030",1,null) as hiv_dna_pcr_error,
    CASE
        WHEN
            (obs REGEXP '!!1271='
                AND NOT obs REGEXP '!!1271=1107')
        THEN
            CONCAT(REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(obs, LOCATE('!!1271=', obs)),
                                    @sep,
                                    ROUND((LENGTH(obs) - LENGTH(REPLACE(obs, '1271=', ''))) / LENGTH('!!1271=')))),
                            '!!1271=',
                            ''),
                        '!!',
                        ''),
                    ' ## ',
                    IFNULL(orders, ''))
        ELSE orders
    END AS tests_ordered

from flat_labs_and_imaging_0 t1
	join amrs.person p using (person_id)
);



# Remove test patients
delete t1
from flat_labs_and_imaging t1
join amrs.person_attribute t2 using (person_id)
where t2.person_attribute_type_id=28 and value='true' and voided=0;

#select * from flat_labs_and_imaging;

select @end := now();
insert into flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");
