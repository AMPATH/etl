DELIMITER $$
CREATE PROCEDURE `generate_flat_breast_cancer_screening_v1_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
					set @primary_table := "flat_breast_cancer_screening";
					set @query_type = query_type;
#set @query_type = "build";
                     
                    set @total_rows_written = 0;
                    
                    set @encounter_types = "(86,145,146,160)";
                    set @clinical_encounter_types = "(86,145,146,160)";
                    set @non_clinical_encounter_types = "(-1)";
                    set @other_encounter_types = "(-1)";
                    
					set @start = now();
					set @table_version = "flat_breast_cancer_screening_v1.0";

					set session sort_buffer_size=512000000;

					set @sep = " ## ";
                    set @boundary = "!!";
					set @last_date_created = (select max(max_date_created) from etl.flat_obs);

					#delete from etl.flat_log where table_name like "%flat_breast_cancer_screening%";
					#drop table etl.flat_breast_cancer_screening;


					#drop table if exists flat_breast_cancer_screening;
					create table if not exists flat_breast_cancer_screening (
							date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            person_id int,
							encounter_id int,
							encounter_type int,
							encounter_datetime datetime,
							visit_id int,
							location_id int,
							location_uuid varchar (100),
							#location_name char (100),
							gender char (100),
							age int,
							encounter_purpose int,
							other_encounter_purpose varchar(1000),
							menstruation_before_12 char,
							menses_stopped_permanently char,
							menses_stop_age int,
							hrt_use char,
							hrt_start_age int,
							hrt_end_age int,
							hrt_use_years int,
							hrt_type_used int,
							given_birth int,
							age_first_birth int,
							gravida int,
							parity int,
							cigarette_smoking int,
							cigarette_smoked_day int,
							tobacco_use int,
							tobacco_use_duration_yrs int,
							alcohol_drinking int,
							alcohol_type int,
							alcohol_use_period_yrs int,
							breast_complaints_3moths int,
							prev_exam_results int,
							fam_brca_history_bf50 int,
							fam_brca_history_aft50 int,
							fam_male_brca_history int,
							fam_ovarianca_history int,
							fam_relatedca_history int,
							fam_otherca_specify int,
							cur_physical_findings int,
							lymph_nodes_findings int,
							cur_screening_findings int,
							#cur_screening_findings_date int,
							patient_education int,
							patient_education_other varchar(1000),
							referred_orderd int,
							referred_date datetime,
							procedure_done int,
							next_app_date datetime,
							
							cbe_imaging_concordance int,
							mammogram_results int,
							mammogram_workup_date datetime,
							date_patient_notified_of_mammogram_results datetime,
							Ultrasound_results int,
							ultrasound_workup_date datetime,
							date_patient_notified_of_ultrasound_results datetime,
							fna_results int,
							fna_tumor_size int,
							fna_degree_of_malignancy int,
							fna_workup_date datetime,
							date_patient_notified_of_fna_results datetime,
							breast_biopsy_results int,
							biopsy_tumor_size int,
							biopsy_degree_of_malignancy int,
							biopsy_workup_date datetime,
							date_patient_notified_of_biopsy_results datetime,
							date_patient_informed_and_referred_for_management datetime,
							screening_mode int,
							diagnosis int,
							diagnosis_date datetime,
							cancer_staging int,
                            
							prev_encounter_datetime_breast_cancer_screening datetime,
							next_encounter_datetime_breast_cancer_screening datetime,
							prev_encounter_type_breast_cancer_screening mediumint,
							next_encounter_type_breast_cancer_screening mediumint,
							prev_clinical_datetime_breast_cancer_screening datetime,
							next_clinical_datetime_breast_cancer_screening datetime,
							prev_clinical_location_id_breast_cancer_screening mediumint,
							next_clinical_location_id_breast_cancer_screening mediumint,
							prev_clinical_rtc_date_breast_cancer_screening datetime,
							next_clinical_rtc_date_breast_cancer_screening datetime,

							primary key encounter_id (encounter_id),
							index person_date (person_id, encounter_datetime),
							index location_enc_date (location_uuid,encounter_datetime),
							index enc_date_location (encounter_datetime, location_uuid),
							index location_id_rtc_date (location_id,next_app_date),
							index location_uuid_rtc_date (location_uuid,next_app_date),
							index loc_id_enc_date_next_clinical (location_id, encounter_datetime, next_clinical_datetime_breast_cancer_screening),
							index encounter_type (encounter_type),
							index date_created (date_created)
							
						);
                        
							
					
                        if(@query_type="build") then
							select 'BUILDING..........................................';
							
#set @write_table = concat("flat_breast_cancer_screening_temp_",1);
#set @queue_table = concat("flat_breast_cancer_screening_build_queue_",1);                    												

                            set @write_table = concat("flat_breast_cancer_screening_temp_",queue_number);
							set @queue_table = concat("flat_breast_cancer_screening_build_queue_",queue_number);                    												
							

#drop table if exists flat_breast_cancer_screening_temp_1;							
							SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

							#create  table if not exists @queue_table (person_id int, primary key (person_id));
							SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_breast_cancer_screening_build_queue limit ', queue_size, ');'); 
#SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_breast_cancer_screening_build_queue limit 500);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  
							
							#delete t1 from flat_breast_cancer_screening_build_queue t1 join @queue_table t2 using (person_id)
							SET @dyn_sql=CONCAT('delete t1 from flat_breast_cancer_screening_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  

					end if;
	
					
					if (@query_type="sync") then
							select 'SYNCING..........................................';
							set @write_table = "flat_breast_cancer_screening";
							set @queue_table = "flat_breast_cancer_screening_sync_queue";
                            create table if not exists flat_breast_cancer_screening_sync_queue (person_id int primary key);                            
                            
							set @last_update = null;

                            select max(date_updated) into @last_update from etl.flat_log where table_name=@table_version;

#select max(date_created) into @last_update from etl.flat_log where table_name like "%breast_cancer_screening%";

#select @last_update;														
select "Finding patients in amrs.encounters...";

							replace into flat_breast_cancer_screening_sync_queue
							(select distinct patient_id
								from amrs.encounter
								where date_changed > @last_update
							);
						
                        
select "Finding patients in flat_obs...";

							replace into flat_breast_cancer_screening_sync_queue
							(select distinct person_id
								from etl.flat_obs
								where max_date_created > @last_update
							);


select "Finding patients in flat_lab_obs...";
							replace into flat_breast_cancer_screening_sync_queue
							(select distinct person_id
								from etl.flat_lab_obs
								where max_date_created > @last_update
							);

select "Finding patients in flat_orders...";

							replace into flat_breast_cancer_screening_sync_queue
							(select distinct person_id
								from etl.flat_orders
								where max_date_created > @last_update
							);
                            
                            replace into flat_breast_cancer_screening_sync_queue
                            (select person_id from 
								amrs.person 
								where date_voided > @last_update);


                            replace into flat_breast_cancer_screening_sync_queue
                            (select person_id from 
								amrs.person 
								where date_changed > @last_update);
                                

					  end if;
                      

					# Remove test patients
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

					select @person_ids_count as 'num patients to update';


					#delete t1 from flat_breast_cancer_screening t1 join @queue_table t2 using (person_id);
					SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
                    set @total_time=0;
                    set @cycle_number = 0;
                    

					while @person_ids_count > 0 do

						set @loop_start_time = now();
                        
						#create temp table with a set of person ids
						drop temporary table if exists flat_breast_cancer_screening_build_queue__0;
						
                        #create temporary table flat_breast_cancer_screening_build_queue__0 (select * from flat_breast_cancer_screening_build_queue_2 limit 5000); #TODO - change this when data_fetch_size changes

#SET @dyn_sql=CONCAT('create temporary table flat_breast_cancer_screening_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit 100);'); 
						SET @dyn_sql=CONCAT('create temporary table flat_breast_cancer_screening_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                    
						drop temporary table if exists flat_breast_cancer_screening_0a;
						SET @dyn_sql = CONCAT(
								'create temporary table flat_breast_cancer_screening_0a
								(select
									t1.person_id,
									t1.visit_id,
									t1.encounter_id,
									t1.encounter_datetime,
									t1.encounter_type,
									t1.location_id,
									t1.obs,
									t1.obs_datetimes,
									case
										when t1.encounter_type in ',@clinical_encounter_types,' then 1
										else null
									end as is_clinical_encounter,

									case
										when t1.encounter_type in ',@non_clinical_encounter_types,' then 20
										when t1.encounter_type in ',@clinical_encounter_types,' then 10
										when t1.encounter_type in', @other_encounter_types, ' then 5
										else 1
									end as encounter_type_sort_index,
									t2.orders
								from etl.flat_obs t1
									join flat_breast_cancer_screening_build_queue__0 t0 using (person_id)
									left join etl.flat_orders t2 using(encounter_id)
								where t1.encounter_type in ',@encounter_types,');');
                            
                        PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                    
                    

                        
					
						insert into flat_breast_cancer_screening_0a
						(select
							t1.person_id,
							null,
							t1.encounter_id,
							t1.test_datetime,
							t1.encounter_type,
							null, #t1.location_id,
							t1.obs,
							null, #obs_datetimes
							# in any visit, there many be multiple encounters. for this dataset, we want to include only clinical encounters (e.g. not lab or triage visit)
							0 as is_clinical_encounter,
							1 as encounter_type_sort_index,
							null
							from etl.flat_lab_obs t1
								join flat_breast_cancer_screening_build_queue__0 t0 using (person_id)
						);


						drop temporary table if exists flat_breast_cancer_screening_0;
						create temporary table flat_breast_cancer_screening_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
						(select * from flat_breast_cancer_screening_0a
						order by person_id, date(encounter_datetime), encounter_type_sort_index
						);


						set @encounter_purpose = null;
                        set @other_encounter_purpose = null;
						set @menstruation_before_12 = null;
						set @menses_stopped_permanently = null;
						set @menses_stop_age = null;
						set @hrt_use = null;
						set @hrt_start_age = null;
						set @hrt_end_age = null;
						set @hrt_use_years = null;
						set @hrt_type_used =null;
						set @given_birth = null;
						set @age_first_birth = null;
						set @gravida = null;
						set @parity = null;
						set @cigarette_smoking = null;
						set @cigarette_smoked_day =null;
						set @tobacco_use =null;
						set @tobacco_use_duration_yrs = null;
						set @alcohol_drinking =null;
						set @alcohol_type = null;
						set @alcohol_use_period_yrs = null;
						set @breast_complaints_3moths = null;
						set @prev_exam_results = null;
						set @fam_brca_history_bf50 = null;
						set @fam_brca_history_aft50 = null;
						set @fam_male_brca_history = null;
						set @fam_ovarianca_history =null;
						set @fam_relatedca_history =null;
						set @fam_otherca_specify = null;
						set @cur_physical_findings = null;
						set @lymph_nodes_findings =null;
						set @cur_screening_findings = null;
						#set @cur_screening_findings_date = null;
						set @patient_education = null;
						set @patient_education_other = null;
						set @referred_orderd = null; 
						set @procedure_done =null;
						set @referred_date =null;
						set @next_app_date = null;

						set @cbe_imaging_concordance =null;
						set @mammogram_results =null;
						set @mammogram_workup_date =null; 
						set @date_patient_notified_of_mammogram_results =null;
						set @Ultrasound_results =null;
                        set @ultrasound_workup_date =null;
						set @date_patient_notified_of_ultrasound_results =null;
						set @fna_results =null;
                        set @fna_tumor_size =null;
                        set @fna_degree_of_malignancy =null;
                        set @fna_workup_date =null;
						set @date_patient_notified_of_fna_results =null;
						set @breast_biopsy_results =null;
						set @biopsy_tumor_size =null;
						set @biopsy_degree_of_malignancy =null;
						set @biopsy_workup_date =null;
						set @date_patient_notified_of_biopsy_results =null;
						set @date_patient_informed_and_referred_for_management =null;
                        set @screening_mode =null;
						set @diagnosis =null;
						set @diagnosis_date =null;
                        set @cancer_staging =null;
                                                
						drop temporary table if exists flat_breast_cancer_screening_1;
						create temporary table flat_breast_cancer_screening_1 #(index encounter_id (encounter_id))
						(select 
							obs,
							encounter_type_sort_index,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,
							t1.person_id,
							t1.encounter_id,
							t1.encounter_type,
							t1.encounter_datetime,
							t1.visit_id,
							#t4.name as location_name,
						    t1.location_id,
                            t1.is_clinical_encounter,
							p.gender,
                            p.death_date,
                            
                            
							case
								when timestampdiff(year,birthdate,curdate()) > 0 then round(timestampdiff(year,birthdate,curdate()),0)
								else round(timestampdiff(month,birthdate,curdate())/12,2)
							end as age,
							
                            case
								when obs regexp "!!1834=9651!!" then @encounter_purpose := 1
								when obs regexp "!!1834=1154!!" then @encounter_purpose := 2
                                when obs regexp "!!1834=1246!!" then @encounter_purpose := 3
                                when obs regexp "!!1834=5622!!" then @encounter_purpose := 4
								else @encounter_purpose := null
							end as encounter_purpose,
                            
                            case
								when obs regexp "!!1915=" then @other_encounter_purpose := GetValues(obs,1915) 
								else @other_encounter_purpose := null
							end as other_encounter_purpose,

							case
								when obs regexp "!!9560=1065!!" then @menstruation_before_12 := 1
								when obs regexp "!!9560=1066!!" then @menstruation_before_12 := 0
								else @menstruation_before_12 := null
							end as menstruation_before_12,
                            
                            case
								when obs regexp "!!9561=1065!!" then @menstruation_before_12 := 1
								when obs regexp "!!9561=1066!!" then @menstruation_before_12 := 0
                                when obs regexp "!!9561=9568!!" then @menstruation_before_12 := 2
								else @menses_stopped_permanently := null
							end as menses_stopped_permanently,

							
                            case
								when obs regexp "!!9562=[0-9]" and t1.encounter_type = @lab_encounter_type then @menses_stop_age:=cast(GetValues(obs,9562) as unsigned)
								when @prev_id=@cur_id then@menses_stop_age
								else @menses_stop_age:=null
							end as menses_stop_age,
                            
							case
								when obs regexp "!!9626=1065!!" then @hrt_use := 1
								when obs regexp "!!9626=1066!!" then @hrt_use := 0
                                when obs regexp "!!9626=9568!!" then @hrt_use := 2
								else @hrt_use := null
							end as hrt_use,
                            
                            
                            case
								when obs regexp "!!9627=[0-9]"  then @hrt_start_age:=cast(GetValues(obs,9627) as unsigned)
								else @hrt_start_age := null
							end as hrt_start_age,
                            
                            
                            case
								when obs regexp "!!9723=[0-9]"  then @hrt_end_age:=cast(GetValues(obs,9723) as unsigned) 
                                else @hrt_end_age := null
							end as hrt_end_age,
							
                            
                            case
								when obs regexp "!!97629=[0-9]"  then @hrt_use_years:=cast(GetValues(obs,9629) as unsigned) 
								else @hrt_use_years := null
							end as hrt_use_years,
                            
                            
                            case
								when obs regexp "!!9630=9573!!" then @hrt_type_used := 1
								when obs regexp "!!9630=6217!!" then @hrt_type_used := 2
								when obs regexp "!!9630=6218!!" then @hrt_type_used := 3
								else @hrt_type_used := null
							end as hrt_type_used,
                            
                            case
								when obs regexp "!!9563=1065!!" then @given_birth := 1
								when obs regexp "!!9563=1066!!" then @given_birth := 0
								else @given_birth := null
							end as given_birth,
							
                            
                            case
								when obs regexp "!!5574=[0-9]"  then @age_first_birth:=cast(GetValues(obs,5574) as unsigned) 
								else @age_first_birth := null
							end as age_first_birth,
							
                            
                            case
								when obs regexp "!!5624=[0-9]"  then @gravida:= cast(GetValues(obs,5624) as unsigned)
								else @gravida := null
							end as gravida,
                            
                            
                            case
								when obs regexp "!!1053=[0-9]"  then @parity:=cast(GetValues(obs,1053) as unsigned) 
								else @parity := null
							end as parity,
                            
                            
                            case
								when obs regexp "!!9333=" then @cigarette_smoking := GetValues(obs,9333)
								else @cigarette_smoking := null
							end as cigarette_smoking,
                            
                            
							case
								when obs regexp "!!2069=[0-9]"  then @cigarette_smoked_day:=cast(GetValues(obs,2069) as unsigned) 
								else @cigarette_smoked_day := null
							end as cigarette_smoked_day,
                            
                            
                            case
								when obs regexp "!!7973=1065!!" then @tobacco_use := 1
								when obs regexp "!!7973=1066!!" then @tobacco_use := 0
                                when obs regexp "!!7973=1679!!" then @tobacco_use := 2
								else @hrt_use := null
							end as tobacco_use,
                            
                            
                            case
								when obs regexp "!!8144=[0-9]"  then @tobacco_use_duration_yrs:= cast(GetValues(obs,8144) as unsigned) 
								else @tobacco_use_duration_yrs := null
							end as tobacco_use_duration_yrs,
                            
                            
							case
								when obs regexp "!!1684=1065!!" then @alcohol_drinking := 1
								when obs regexp "!!1684=1066!!" then @alcohol_drinking := 0
                                when obs regexp "!!1684=1679!!" then @alcohol_drinking := 2
								else @alcohol_drinking := null
							end as alcohol_drinking,
							
                            
                            case
								when obs regexp "!!1685=1682!!" then @alcohol_type := 1
								when obs regexp "!!1685=1681!!" then @alcohol_type := 2
								when obs regexp "!!1685=1680!!" then @alcohol_type := 3
                                when obs regexp "!!1685=1683!!" then @alcohol_type := 4
								when obs regexp "!!1685=2059!!" then @alcohol_type := 5
                                when obs regexp "!!1685=5622!!" then @alcohol_type := 6
								else @hrt_type_used := null
							end as alcohol_type,
                            
                            
                            case
								when obs regexp "!!8170=[0-9]"  then @alcohol_use_period_yrs:= cast(GetValues(obs,8170) as unsigned) 
								else @alcohol_use_period_yrs := null
							end as alcohol_use_period_yrs,
                            
                            
                            case
								when obs regexp "!!9553=5006!!" then @breast_complaints_3moths := 1
								when obs regexp "!!9553=1068!!" then @breast_complaints_3moths := 2
								else @breast_complaints_3moths := null
							end as breast_complaints_3moths,
                            
                            
                            case
								when obs regexp "!!9694=1115!!" then @prev_exam_results := 1
								when obs regexp "!!9694=1116!!" then @prev_exam_results := 0
                                when obs regexp "!!9694=1067!!" then @prev_exam_results := 2
								else @prev_exam_results := null
							end as prev_exam_results,
                            
                            
							case
								when obs regexp "!!9631=1672!!" then @fam_brca_history_bf50 := 1
								when obs regexp "!!9631=1107!!" then @fam_brca_history_bf50 := 0
								when obs regexp "!!9631=978!!" then @fam_brca_history_bf50 := 2
                                when obs regexp "!!9631=972!!" then @fam_brca_history_bf50 := 3
								when obs regexp "!!9631=1671!!" then @fam_brca_history_bf50 := 4
                                when obs regexp "!!9631=1393!!" then @fam_brca_history_bf50 := 5
								when obs regexp "!!9631=1392!!" then @fam_brca_history_bf50 := 6
								when obs regexp "!!9631=1395!!" then @fam_brca_history_bf50 := 7
                                when obs regexp "!!9631=1394!!" then @fam_brca_history_bf50 := 8
								else @fam_brca_history_bf50 := null
							end as fam_brca_history_bf50,
                            
                            
							case
								when obs regexp "!!9632=978!!" then @fam_brca_history_aft50 := 1
								when obs regexp "!!9632=1672!!" then @fam_brca_history_aft50 := 2
                                when obs regexp "!!9632=972!!" then @fam_brca_history_aft50 := 3
                                when obs regexp "!!9632=1671!!" then @fam_brca_history_aft50 := 4
                                when obs regexp "!!9632=1393!!" then @fam_brca_history_aft50 := 5
                                 when obs regexp "!!9632=1392!!" then @fam_brca_history_aft50 := 6
								when obs regexp "!!9632=1395!!" then @fam_brca_history_aft50 := 7
                                when obs regexp "!!9632=1394!!" then @fam_brca_history_aft50 := 8
								else @fam_brca_history_aft50 := null
							end as fam_brca_history_aft50,
                            
                            
                            case
								when obs regexp "!!9633=978!!" then @fam_male_brca_history := 1
								when obs regexp "!!9633=1672!!" then @fam_male_brca_history := 2
                                when obs regexp "!!9633=972!!" then @fam_male_brca_history := 3
                                when obs regexp "!!9633=1671!!" then @fam_male_brca_history := 4
                                when obs regexp "!!9633=1393!!" then @fam_male_brca_history := 5
                                 when obs regexp "!!9633=1392!!" then @fam_male_brca_history := 6
								when obs regexp "!!9633=1395!!" then @fam_male_brca_history := 7
                                when obs regexp "!!9633=1394!!" then @fam_male_brca_history := 8
								else @fam_male_brca_history := null
							end as fam_male_brca_history,
                            
                            
                            case
								when obs regexp "!!9634=978!!" then @fam_ovarianca_history := 1
								when obs regexp "!!9634=1672!!" then @fam_ovarianca_history := 2
                                when obs regexp "!!9634=972!!" then @fam_ovarianca_history := 3
                                when obs regexp "!!9634=1671!!" then @fam_ovarianca_history := 4
                                when obs regexp "!!9634=1393!!" then @fam_ovarianca_history := 5
                                 when obs regexp "!!9634=1392!!" then @fam_ovarianca_history := 6
								when obs regexp "!!9634=1395!!" then @fam_ovarianca_history := 7
                                when obs regexp "!!9634=1394!!" then @fam_ovarianca_history := 8
								else @fam_ovarianca_history := null
							end as fam_ovarianca_history,
                            
                            
                            case
								when obs regexp "!!9635=978!!" then @fam_relatedca_history:= 1
								when obs regexp "!!9635=1672!!" then @fam_relatedca_history := 2
                                when obs regexp "!!9635=972!!" then @fam_relatedca_history := 3
                                when obs regexp "!!9635=1671!!" then @fam_relatedca_history := 4
                                when obs regexp "!!9635=1393!!" then @fam_relatedca_history := 5
                                 when obs regexp "!!9635=1392!!" then @fam_relatedca_history := 6
								when obs regexp "!!9635=1395!!" then @fam_relatedca_history := 7
                                when obs regexp "!!9635=1394!!" then @fam_relatedca_history := 8
								else @fam_relatedca_history := null
							end as fam_relatedca_history,
                            
                            
                            case
								when obs regexp "!!7176=6529!!" then @fam_otherca_specify := 1
								when obs regexp "!!7176=9636!!" then @fam_otherca_specify := 2
								when obs regexp "!!7176=9637!!" then @fam_otherca_specify := 3
                                when obs regexp "!!7176=6485!!" then @fam_otherca_specify := 4
								when obs regexp "!!7176=9638!!" then @fam_otherca_specify := 5
                                when obs regexp "!!7176=9639!!" then @fam_otherca_specify := 6
                                when obs regexp "!!7176=6522!!" then @fam_otherca_specify := 5
                                when obs regexp "!!7176=216!!" then @fam_otherca_specify := 6
								else @fam_otherca_specify := null
							end as fam_otherca_specify,
                            
                            
                            case
								when obs regexp "!!6251=1115!!" then @cur_physical_findings:= 0
								when obs regexp "!!6251=115!!" then @cur_physical_findings := 1
                                when obs regexp "!!6251=6250!!" then @cur_physical_findings := 2
                                when obs regexp "!!6251=6249!!" then @cur_physical_findings := 3
                                when obs regexp "!!6251=5622!!" then @cur_physical_findings := 4
                                 when obs regexp "!!6251=582!!" then @cur_physical_findings := 5
								when obs regexp "!!6251=6493!!" then @cur_physical_findings := 6
                                when obs regexp "!!6251=6499!!" then @cur_physical_findings := 7
                                when obs regexp "!!6251=1118!!" then @cur_physical_findings := 8
                                 when obs regexp "!!6251=1481!!" then @cur_physical_findings := 9
								when obs regexp "!!6251=6729!!" then @cur_physical_findings := 10
                                when obs regexp "!!6251=1116!!" then @cur_physical_findings := 11
                                when obs regexp "!!6251=8188!!" then @cur_physical_findings := 12
                                when obs regexp "!!6251=8189!!" then @cur_physical_findings := 13
                                when obs regexp "!!6251=1067!!" then @cur_physical_findings := 14
								when obs regexp "!!6251=9689!!" then @cur_physical_findings := 15
								when obs regexp "!!6251=9690!!" then @cur_physical_findings := 16
                                when obs regexp "!!6251=9687!!" then @cur_physical_findings := 17
                                when obs regexp "!!6251=9688!!" then @cur_physical_findings := 18
								when obs regexp "!!6251=5313!!" then @cur_physical_findings := 19
								when obs regexp "!!6251=9691!!" then @cur_physical_findings := 20
								else @cur_physical_findings := null
							end as cur_physical_findings,
                            
                            
                            case
								when obs regexp "!!1121=1115!!" then @lymph_nodes_findings := 1
								when obs regexp "!!1121=161!!" then @lymph_nodes_findings := 2
                                when obs regexp "!!1121=9675!!" then @lymph_nodes_findings:= 3
                                when obs regexp "!!1121=9676!!" then @lymph_nodes_findings:= 4
								else @lymph_nodes_findings := null
							end as lymph_nodes_findings,
                            
                            
                            case
								when obs regexp "!!9748=1115!!" then @cur_screening_findings := 1
								when obs regexp "!!9748=9691!!" then @cur_screening_findings := 3
                                when obs regexp "!!9748=1116!!" then @cur_screening_findings := 2
								else @cur_screening_findings := null
							end as cur_screening_findings,
                            
                            
                            case
								when obs regexp "!!6327=9651!!" then @patient_education := 1
								when obs regexp "!!6327=2345!!" then @patient_education := 2
                                when obs regexp "!!6327=9692!!" then @patient_education:= 3
                                when obs regexp "!!6327=5622!!" then @patient_education:= 4
								else @patient_education := null
							end as patient_education,
                            
                            
							case
								when obs regexp "!!1915=" then @patient_education_other := GetValues(obs,1915) 
								else @patient_education_other := null
							end as patient_education_other,
                            
                            
                            case
								when obs regexp "!!1272=1107!!" then @referred_orderd := 0
								when obs regexp "!!1272=1496!!" then @referred_orderd:= 1
								else @referred_orderd := null
							end as referred_orderd,
                            
                            case
								when obs regexp "!!1272=9596!!" then @procedure_done := 1
								when obs regexp "!!1272=9595!!" then @procedure_done:= 2
                                when obs regexp "!!1272=6510!!" then @procedure_done := 3
								when obs regexp "!!1272=7190!!" then @procedure_done:= 4
                                when obs regexp "!!1272=6511!!" then @procedure_done := 5
								when obs regexp "!!1272=9997!!" then @procedure_done:= 6
								else @procedure_done := null
							end as procedure_done,
                            
							case
								when obs regexp "!!9158=" then @referred_date := GetValues(obs,9158) 
								else @referred_date := null
							end as referred_date,
                            
                            
							case
								when obs regexp "!!5096=" then @next_app_date := GetValues(obs,5096) 
								else @next_app_date := null
							end as next_app_date,
                            
                            
                            case
								when obs regexp "!!9702=9703!!" then @cbe_imaging_concordance := 1
								when obs regexp "!!9702=9704!!" then @cbe_imaging_concordance := 2
                                when obs regexp "!!9702=1175!!" then @cbe_imaging_concordance := 3
								else @cbe_imaging_concordance := null
							end as cbe_imaging_concordance,
                            
                            
                            case
								when obs regexp "!!9595=1115!!" then @mammogram_results := 1
								when obs regexp "!!9595=1116!!" then @mammogram_results := 2
                                when obs regexp "!!9595=1118!!" then @mammogram_results := 3
                                when obs regexp "!!9595=1138!!" then @mammogram_results := 4
                                when obs regexp "!!9595=1267!!" then @mammogram_results := 5
                                when obs regexp "!!9595=1067!!" then @mammogram_results := 6
								else @mammogram_results := null
							end as mammogram_results,
                            
                            
							case
								when obs regexp "!!9708=" then @mammogram_workup_date := GetValues(obs,9708)
                                else @mammogram_workup_date := null
							end as mammogram_workup_date,
                            
                            
							case
								when obs regexp "!!9705=" then @date_patient_notified_of_mammogram_results := GetValues(obs,9705) 
								else @date_patient_notified_of_mammogram_results := null
							end as date_patient_notified_of_mammogram_results,
                            
							case
								when obs regexp "!!9596=1115!!" then @Ultrasound_results := 1
								when obs regexp "!!9596=1116!!" then @Ultrasound_results := 2
                                when obs regexp "!!9596=1067!!" then @Ultrasound_results := 3
                                when obs regexp "!!9596=1118!!" then @Ultrasound_results := 4
								else @Ultrasound_results := null
							end as Ultrasound_results,
                            
							case
								when obs regexp "!!9708=" then @ultrasound_workup_date := GetValues(obs,9708) 
								else @ultrasound_workup_date := null
							end as ultrasound_workup_date,
                            
                            
                            case
								when obs regexp "!!10047=" then @date_patient_notified_of_ultrasound_results := GetValues(obs,10047) 
								else @date_patient_notified_of_ultrasound_results := null
							end as date_patient_notified_of_ultrasound_results,
                            
                            
                            case
								when obs regexp "!!10051=9691!!" then @fna_results := 1
								when obs regexp "!!10051=10052!!" then @fna_results := 2
                                when obs regexp "!!9596=1118!!" then @fna_results := 3
								else @fna_results := null
							end as fna_results,
                            
                            
							case
								when obs regexp "!!10053=[0-9]"  then @fna_tumor_size:= cast(GetValues(obs,10053) as unsigned) 
								else @fna_tumor_size := null
							end as fna_tumor_size,
                            
                            
                            case
								when obs regexp "!!10054=10055!!" then @fna_degree_of_malignancy := 1
								when obs regexp "!!10054=10056!!" then @fna_degree_of_malignancy := 2
								else @fna_degree_of_malignancy := null
							end as fna_degree_of_malignancy,
                            
                            
							case
								when obs regexp "!!10057=" then @fna_workup_date := GetValues(obs,10057) 
								else @fna_workup_date := null
							end as fna_workup_date,
                            
                            
                            case
								when obs regexp "!!10059=" then @date_patient_notified_of_fna_results := GetValues(obs,10059)
								else @date_patient_notified_of_fna_results := null
							end as date_patient_notified_of_fna_results,
                            
                            
                            case
								when obs regexp "!!8184=1115!!" then @breast_biopsy_results := 1
								when obs regexp "!!8184=1116!!" then @breast_biopsy_results := 2
                                when obs regexp "!!8184=1118!!" then @breast_biopsy_results := 3
                                when obs regexp "!!8184=1067!!" then @breast_biopsy_results := 4
                                when obs regexp "!!8184=9691!!" then @breast_biopsy_results := 5
                                when obs regexp "!!8184=10052!!" then @breast_biopsy_results := 6
								else @breast_biopsy_results := null
							end as breast_biopsy_results,
                            
                            
                            case
								when obs regexp "!!10053=[0-9]"  then @biopsy_tumor_size:= cast(GetValues(obs,10053) as unsigned) 
								else @biopsy_tumor_size := null
							end as biopsy_tumor_size,
                            
                            
							 case
								when obs regexp "!!10054=10055!!" then @biopsy_degree_of_malignancy := 1
								when obs regexp "!!10054=10056!!" then @biopsy_degree_of_malignancy := 2
								else @biopsy_degree_of_malignancy := null
							end as biopsy_degree_of_malignancy,
                            
                            
                          case
								when obs regexp "!!10060=" then @biopsy_workup_date := GetValues(obs,10060) 
								else @biopsy_workup_date := null
							end as biopsy_workup_date,
                            
                            
                              case
								when obs regexp "!!10061=" then @date_patient_notified_of_biopsy_resultse := GetValues(obs,10061) 
								else @date_patient_notified_of_biopsy_results := null
							end as date_patient_notified_of_biopsy_results,
                            
                            
                             case
								when obs regexp "!!9706=" then @date_patient_informed_and_referred_for_management := GetValues(obs,10061) 
								else @date_patient_informed_and_referred_for_management := null
							end as date_patient_informed_and_referred_for_management,


                            case
								when obs regexp "!!10068=9595!!" then @screening_mode := 1
								when obs regexp "!!10068=10067!!" then @screening_mode := 2
                                when obs regexp "!!10068=9596!!" then @screening_mode := 3
								else @screening_mode := null
							end as screening_mode,
                            
                            
							case
								when obs regexp "!!6042=" then @diagnosis := GetValues(obs,6042) 
								else @diagnosis := null
							end as diagnosis,
                            
                             /**case
								#when obs regexp "!!9728=" then @diagnosis_date := replace(replace((substring_index(substring(obs,locate("!!9728=",obs)),@sep,1)),"!!9728=",""),"!!","")
								when obs regexp "!!9728=" then @diagnosis_date := encounter_datetime
                                else @diagnosis_date := null
							end as diagnosis_date**/
                            
                            
                            case
                                when obs regexp "!!9728=" then @diagnosis_date := GetValues(obs,9728) 
                                else @diagnosis_date := null
							end as diagnosis_date,
							
                            
                            case
								when obs regexp "!!9868=9852!!" then @cancer_staging := 1
								when obs regexp "!!9868=9856!!" then @cancer_staging := 2
                                when obs regexp "!!9868=9860!!" then @cancer_staging := 3
                                when obs regexp "!!9868=9864!!" then @cancer_staging := 4
								else @cancer_staging := null
							end as cancer_staging
		
						from flat_breast_cancer_screening_0 t1
							join amrs.person p using (person_id)
						order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
						);# limit 100;

						
						set @prev_id = null;
						set @cur_id = null;
						set @prev_encounter_datetime = null;
						set @cur_encounter_datetime = null;

						set @prev_clinical_datetime = null;
						set @cur_clinical_datetime = null;

						set @next_encounter_type = null;
						set @cur_encounter_type = null;

                        set @prev_clinical_location_id = null;
						set @cur_clinical_location_id = null;


						alter table flat_breast_cancer_screening_1 drop prev_id, drop cur_id;

						drop table if exists flat_breast_cancer_screening_2;
						create temporary table flat_breast_cancer_screening_2
						(select *,
							@prev_id := @cur_id as prev_id,
							@cur_id := person_id as cur_id,

							case
								when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
								else @prev_encounter_datetime := null
							end as next_encounter_datetime_breast_cancer_screening,

							@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
								else @next_encounter_type := null
							end as next_encounter_type_breast_cancer_screening,

							@cur_encounter_type := encounter_type as cur_encounter_type,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as next_clinical_datetime_breast_cancer_screening,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as next_clinical_location_id_breast_cancer_screening,

							case
								when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
								when @prev_id = @cur_id then @cur_clinical_datetime
								else @cur_clinical_datetime := null
							end as cur_clinic_datetime,

                            case
								when is_clinical_encounter then @cur_clinical_location_id := location_id
								when @prev_id = @cur_id then @cur_clinical_location_id
								else @cur_clinical_location_id := null
							end as cur_clinic_location_id,

						    case
								when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
								else @prev_clinical_rtc_date := null
							end as next_clinical_rtc_date_breast_cancer_screening,

							case
								when is_clinical_encounter then @cur_clinical_rtc_date := next_app_date
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinical_rtc_date

							from flat_breast_cancer_screening_1
							order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
						);

						alter table flat_breast_cancer_screening_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;


						set @prev_id = null;
						set @cur_id = null;
						set @prev_encounter_type = null;
						set @cur_encounter_type = null;
						set @prev_encounter_datetime = null;
						set @cur_encounter_datetime = null;
						set @prev_clinical_datetime = null;
						set @cur_clinical_datetime = null;
                        set @prev_clinical_location_id = null;
						set @cur_clinical_location_id = null;

						drop temporary table if exists flat_breast_cancer_screening_3;
						create temporary table flat_breast_cancer_screening_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
						(select
							*,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,

							case
						        when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
						        else @prev_encounter_type:=null
							end as prev_encounter_type_breast_cancer_screening,	
                            @cur_encounter_type := encounter_type as cur_encounter_type,

							case
						        when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
						        else @prev_encounter_datetime := null
						    end as prev_encounter_datetime_breast_cancer_screening,

                            @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as prev_clinical_datetime_breast_cancer_screening,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as prev_clinical_location_id_breast_cancer_screening,

							case
								when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
								when @prev_id = @cur_id then @cur_clinical_datetime
								else @cur_clinical_datetime := null
							end as cur_clinical_datetime,

                            case
								when is_clinical_encounter then @cur_clinical_location_id := location_id
								when @prev_id = @cur_id then @cur_clinical_location_id
								else @cur_clinical_location_id := null
							end as cur_clinical_location_id,

							case
								when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
								else @prev_clinical_rtc_date := null
							end as prev_clinical_rtc_date_breast_cancer_screening,

							case
								when is_clinical_encounter then @cur_clinical_rtc_date := next_app_date
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinic_rtc_date

							from flat_breast_cancer_screening_2 t1
							order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
                                        


					select count(*) into @new_encounter_rows from flat_breast_cancer_screening_3;
                    
                    select @new_encounter_rows;                    
					set @total_rows_written = @total_rows_written + @new_encounter_rows;
                    select @total_rows_written;

					SET @dyn_sql=CONCAT('replace into ',@write_table,											  
						'(select
								null,
								person_id,
								encounter_id,
								encounter_type,
								encounter_datetime,
								visit_id,
							   location_id,
							   t2.uuid as location_uuid,
								gender,
								age,
								encounter_purpose,	
								other_encounter_purpose,
								menstruation_before_12,
								menses_stopped_permanently,
								menses_stop_age,
								hrt_use,
								hrt_start_age,
								hrt_end_age,
								hrt_use_years,
								hrt_type_used,
								given_birth,
								age_first_birth,
								gravida,
								parity,
								cigarette_smoking,
								cigarette_smoked_day,
								tobacco_use,
								tobacco_use_duration_yrs,
								alcohol_drinking,
								alcohol_type,
								alcohol_use_period_yrs,
								breast_complaints_3moths,
								prev_exam_results,
								fam_brca_history_bf50,
								fam_brca_history_aft50,
								fam_male_brca_history,
								fam_ovarianca_history,
								fam_relatedca_history,
								fam_otherca_specify,
								cur_physical_findings,
								lymph_nodes_findings,
								cur_screening_findings,
								#cur_screening_findings_date,
								patient_education,
								patient_education_other,
								referred_orderd,
								referred_date,
								procedure_done,
								next_app_date,
								cbe_imaging_concordance,
								mammogram_results,
								mammogram_workup_date,
								date_patient_notified_of_mammogram_results,
								Ultrasound_results,
								ultrasound_workup_date,
								date_patient_notified_of_ultrasound_results,
								fna_results,
								fna_tumor_size,
								fna_degree_of_malignancy,
								fna_workup_date,
								date_patient_notified_of_fna_results,
								breast_biopsy_results,
								biopsy_tumor_size,
								biopsy_degree_of_malignancy,
								biopsy_workup_date,
								date_patient_notified_of_biopsy_results,
								date_patient_informed_and_referred_for_management,
								screening_mode,
								diagnosis,
								diagnosis_date,
								cancer_staging,
                                
                                prev_encounter_datetime_breast_cancer_screening,
								next_encounter_datetime_breast_cancer_screening,
								prev_encounter_type_breast_cancer_screening,
								next_encounter_type_breast_cancer_screening,
								prev_clinical_datetime_breast_cancer_screening,
								next_clinical_datetime_breast_cancer_screening,
								prev_clinical_location_id_breast_cancer_screening,
								next_clinical_location_id_breast_cancer_screening,
								prev_clinical_rtc_date_breast_cancer_screening,
								next_clinical_rtc_date_breast_cancer_screening

						from flat_breast_cancer_screening_3 t1
                        join amrs.location t2 using (location_id))');

					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;


					SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_breast_cancer_screening_build_queue__0 t2 using (person_id);'); 
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    
					#select @person_ids_count := (select count(*) from flat_breast_cancer_screening_build_queue_2);                        
					SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
                    
                    #select @person_ids_count as remaining_in_build_queue;

					set @cycle_length = timestampdiff(second,@loop_start_time,now());
					#select concat('Cycle time: ',@cycle_length,' seconds');                    
                    set @total_time = @total_time + @cycle_length;
                    set @cycle_number = @cycle_number + 1;
                    
                    #select ceil(@person_ids_count / cycle_size) as remaining_cycles;
                    set @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);
                    #select concat("Estimated time remaining: ", @remaining_time,' minutes');

					select @person_ids_count as 'persons remaining', @cycle_length as 'Cycle time (s)', ceil(@person_ids_count / cycle_size) as remaining_cycles, @remaining_time as 'Est time remaining (min)';

				 end while;
                 
                 if(@query_type="build") then

#select 1;
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
                
									
				set @ave_cycle_length = ceil(@total_time/@cycle_number);
                select CONCAT('Average Cycle Length: ', @ave_cycle_length, ' second(s)');
                
				 set @end = now();
				 insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
				 select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");

		END$$
DELIMITER ;
