DELIMITER $$
CREATE PROCEDURE `generate_flat_labs_and_imaging_v4_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
				set session sort_buffer_size=512000000;
				set session group_concat_max_len=100000;
				set @start = now();
				set @primary_table := "flat_labs_and_imaging";
				select @table_version := 'flat_labs_and_imaging_v4.0';
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
                /*
				679	RBC
				21	HGB
				851	MCV
				1018	MCH
				1017	MCHC
				1016	RDW
				729	PLT
				678	SERUM WBC
				1330	ANC					
				6134	Uric acid
				790	Creatinine
				1132	Sodium
				1133	Potassium
				1134	Chloride					
				655	Total Bili
				1297	Direct Bili
				6123	GGT
				653	AST
				654	ALT
				717	Total Protein
				848	Albumin
				785	ALP
				1014	LDH					
				10249	Total PSA					
				10250	CEA					
				10251	(CA 19-9)					
				9010	HBF
				9011	HBA
				9699	HbS
				9012	HBA2
			*/
            
                set @concept_ids = '(1030, 1040, 856, 5497, 730, 21,653,790,12,6126,887,6252,1537,1271,9239,9020,857
													679,21,851,1018,1017,1016,729,678,1330,6134,790,1132,1133,1134,655,1297,6123,
                                                    653,654,717,848,785,1014,10249,10250,10251,9010,9011,9699,9012                
                )';
                
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
                            rbc decimal,
							hemoglobin decimal,
							mcv int,
							mch decimal,
							mchc decimal,
							rdw decimal,
							plt int,
							wbc decimal,
							anc decimal,
							uric_acid decimal,
							creatinine decimal,
							na decimal,
							k decimal,
							cl decimal,
							total_bili decimal,
							direct_bili decimal,
							ggt decimal,
							ast decimal,
							alt decimal,
							total_protein decimal,
							albumin decimal,
							alk_phos decimal,
							ldh decimal,
							total_psa decimal,
							cea decimal,
							ca_19_9 decimal,
							hbf decimal,
							hba decimal,
							hbs decimal,
							hba2 decimal,

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
#                        set @build_queue = concat(@primary_table,'_build_queue');
                        set @build_queue = concat('flat_labs_and_imaging_build_queue');
						set @queue_table = concat(@build_queue,'_',@queue_number);                    												


						SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                        
						#create  table if not exists @queue_table (person_id int, primary key (person_id));
						SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,'(person_id int primary key) (select * from ',@build_queue,' limit ', @queue_size, ');'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
						
						#delete t1 from flat_obs_build_queue t1 join @queue_table t2 using (person_id)
						SET @dyn_sql=CONCAT('delete t1 from ',@build_queue,' t1 join ',@queue_table, ' t2 using (person_id);'); 
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
						create table if not exists flat_labs_and_imaging_sync_queue (person_id int primary key);
                                                
                        
						set @last_update = null;
						select max(date_updated) into @last_update from etl.flat_log where table_name=@table_version;

						replace into flat_labs_and_imaging_sync_queue
						(select distinct patient_id
							from amrs.encounter
							where date_changed > @last_update
						);

						replace into flat_labs_and_imaging_sync_queue
						(select distinct person_id
							from etl.flat_lab_obs
							where max_date_created > @last_update
						);


						replace into flat_labs_and_imaging_sync_queue
						(select person_id from 
							amrs.person 
							where date_voided > @last_update);


						replace into flat_labs_and_imaging_sync_queue
						(select person_id from 
							amrs.person 
							where date_changed > @last_update);
                        												
				end if;

				# delete all rows in primary table in the queue
				SET @dyn_sql=CONCAT('delete t1 from ',@primary_table,' t1 join ',@queue_table, ' t2 using (person_id)');
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
										                    


					drop table if exists flat_labs_and_imaging_0;

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
										if(obs regexp "!!679=",cast(getValues(obs,679) as decimal(6,2)),null) as rbc,
										if(obs regexp "!!21=",cast(getValues(obs,21) as decimal(6,2)),null) as hemoglobin,
										if(obs regexp "!!851=",cast(getValues(obs,851) as unsigned),null) as mcv,
										if(obs regexp "!!1018=",cast(getValues(obs,1018) as decimal(6,2)),null) as mch,
										if(obs regexp "!!1017=",cast(getValues(obs,1017) as decimal(6,2)),null) as mchc,
										if(obs regexp "!!1016=",cast(getValues(obs,1016) as decimal(6,2)),null) as rdw,
										if(obs regexp "!!729=",cast(getValues(obs,729) as unsigned),null) as plt,
										if(obs regexp "!!678=",cast(getValues(obs,678) as decimal(6,2)),null) as wbc,
										if(obs regexp "!!1330=",cast(getValues(obs,1330) as decimal(6,2)),null) as anc,

										if(obs regexp "!!6134=",cast(getValues(obs,6134) as decimal(6,2)),null) as uric_acid,
										if(obs regexp "!!790=",cast(getValues(obs,790) as decimal(6,2)),null) as creatinine,
										if(obs regexp "!!1132=",cast(getValues(obs,1132) as decimal(6,2)),null) as na,
										if(obs regexp "!!1133=",cast(getValues(obs,1133) as decimal(6,2)),null) as k,
										if(obs regexp "!!1134=",cast(getValues(obs,1134) as decimal(6,2)),null) as cl,

										if(obs regexp "!!655=",cast(getValues(obs,655) as decimal(6,2)),null) as total_bili,
										if(obs regexp "!!1297=",cast(getValues(obs,1297) as decimal(6,2)),null) as direct_bili,
										if(obs regexp "!!6123=",cast(getValues(obs,6123) as decimal(6,2)),null) as ggt,
										if(obs regexp "!!653=",cast(getValues(obs,653) as decimal(6,2)),null) as ast,
										if(obs regexp "!!654=",cast(getValues(obs,654) as decimal(6,2)),null) as alt,
										if(obs regexp "!!717=",cast(getValues(obs,717) as decimal(6,2)),null) as total_protein,
										if(obs regexp "!!848=",cast(getValues(obs,848) as decimal(6,2)),null) as albumin,
										if(obs regexp "!!785=",cast(getValues(obs,785) as decimal(6,2)),null) as alk_phos,
										if(obs regexp "!!1014=",cast(getValues(obs,1014) as decimal(6,2)),null) as ldh,

										if(obs regexp "!!10249=",cast(getValues(obs,10249) as decimal(6,2)),null) as total_psa,

										if(obs regexp "!!10250=",cast(getValues(obs,10250) as decimal(6,2)),null) as cea,

										if(obs regexp "!!10251=",cast(getValues(obs,10251) as decimal(6,2)),null) as ca_19_9,

										if(obs regexp "!!9010=",cast(getValues(obs,9010) as decimal(6,2)),null) as hbf,
										if(obs regexp "!!9011=",cast(getValues(obs,9011) as decimal(6,2)),null) as hba,
										if(obs regexp "!!9699=",cast(getValues(obs,9699) as decimal(6,2)),null) as hbs,
										if(obs regexp "!!9012=",cast(getValues(obs,9012) as decimal(6,2)),null) as hba2,
 
                                       
										CONCAT(
											case
												when obs regexp "!!1271=" then getValues(obs,1271)
													# replace(replace((substring_index(substring(obs,locate("!!1271=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!1271=", "") ) ) / LENGTH("!!1271=") ))),"!!1271=",""),"!!","")
												else ""
											end,
                                            ifnull(orders,"")
										) as tests_ordered
                                        
										from flat_lab_obs t1  
											join temp_queue_table t2 using(person_id)
											left outer join flat_orders t3 using(encounter_id) 
									)'
								);
                                
					
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1; 	


/*
#					set @last_update = '2016-03-01';
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
											join ', @queue_table,' t2 using(person_id)
                                            join amrs.encounter t2 using (encounter_id)
											left outer join flat_lab_obs t3 ON t1.person_id = t3.person_id AND t1.encounter_datetime = t3.test_datetime 
											where 
												t3.person_id is null                                                
                                                # and t1.encounter_datetime >= @last_update and 
									)'
								);
					PREPARE s1 from @dyn_sql;
					EXECUTE s1;
					DEALLOCATE PREPARE s1;
*/				

                    SET @dyn_sql = CONCAT(
						'insert into ',@write_table,
						'(select
							null,
                            person_id,
							uuid,
							encounter_id,
							test_datetime,
							encounter_type ,
							hiv_dna_pcr,
							hiv_rapid_test,
							hiv_viral_load,
							cd4_count,
							cd4_percent,
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
                            rbc,
							hemoglobin,
							mcv,
							mch,
							mchc,
							rdw,
							plt,
							wbc,
							anc,
							uric_acid,
							creatinine,
							na,
							k,
							cl,
							total_bili,
							direct_bili,
							ggt,
							ast,
							alt,
							total_protein,
							albumin,
							alk_phos,
							ldh,
							total_psa,
							cea,
							ca_19_9,
							hbf,
							hba,
							hbs,
							hba2,
							tests_ordered
						from flat_labs_and_imaging_0 t1
							join amrs.person t2 using (person_id)
						)'
					);
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  



					SET @dyn_sql=CONCAT('delete t1.* from ',@queue_table,' t1 join temp_queue_table t2 using (person_id);'); 
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
