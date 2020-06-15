DELIMITER $$
CREATE  PROCEDURE `generate_hiv_summary_v15`()
BEGIN
                    select @query_type := "rebuild"; 
					select @start := now();
					select @start := now();
					select @table_version := "flat_hiv_summary_v2.15";

					set session sort_buffer_size=512000000;

					select @sep := " ## ";
					select @lab_encounter_type := 99999;
					select @death_encounter_type := 31;
					select @last_date_created := (select max(max_date_created) from etl.flat_obs);

					
					
					create table if not exists flat_hiv_summary_v15 (
						person_id int,
						uuid varchar(100),
						visit_id int,
						encounter_id int,
						encounter_datetime datetime,
						encounter_type int,
						is_clinical_encounter int,

						location_id int,
						location_uuid varchar(100),

						visit_num int,
					
						enrollment_date datetime,                        
						enrollment_location_id int,
                                                
                        hiv_start_date datetime,

						death_date datetime,
						scheduled_visit int,
                        
						transfer_in tinyint,
                        transfer_in_location_id int,
                        transfer_in_date datetime,
                        
						transfer_out tinyint,
						transfer_out_location_id int,
						transfer_out_date datetime,

						patient_care_status int,
						out_of_care int,
						prev_rtc_date datetime,
						rtc_date datetime,
						
						arv_first_regimen varchar(500),
                        arv_first_regimen_location_id int,
						arv_first_regimen_start_date datetime,

                        prev_arv_meds varchar(500),
						cur_arv_meds varchar(500),
						arv_start_date datetime,						
                        arv_start_location_id int,
						prev_arv_start_date datetime,
						prev_arv_end_date datetime,						
						
                        prev_arv_line int,
						cur_arv_line int,

						
						prev_arv_adherence varchar(200),
						cur_arv_adherence varchar(200),
						hiv_status_disclosed int,

						first_evidence_patient_pregnant datetime,
						edd datetime,

						screened_for_tb boolean,

						ipt_start_date datetime,
						ipt_stop_date datetime,
						ipt_completion_date datetime,

						tb_tx_start_date datetime,
						tb_tx_end_date datetime,
						
                        pcp_prophylaxis_start_date datetime,
                        
						condoms_provided_date datetime,
						modern_contraceptive_method_start_date datetime,
						cur_who_stage int,

						cd4_resulted double,
						cd4_resulted_date datetime,
						cd4_1 double,
						cd4_1_date datetime,
						cd4_2 double,
						cd4_2_date datetime,
						cd4_percent_1 double,
						cd4_percent_1_date datetime,
						cd4_percent_2 double,
						cd4_percent_2_date datetime,
						vl_resulted int,
						vl_resulted_date datetime,
						vl_1 int,
						vl_1_date datetime,
						vl_2 int,
						vl_2_date datetime,
						vl_order_date datetime,
						cd4_order_date datetime,

						
						hiv_dna_pcr_order_date datetime,
						hiv_dna_pcr_resulted int,
						hiv_dna_pcr_resulted_date datetime,
						hiv_dna_pcr_1 int,
						hiv_dna_pcr_1_date datetime,
						hiv_dna_pcr_2 int,
						hiv_dna_pcr_2_date datetime,

						
						hiv_rapid_test_resulted int,
						hiv_rapid_test_resulted_date datetime,

						prev_encounter_datetime_hiv datetime,
						next_encounter_datetime_hiv datetime,
						prev_encounter_type_hiv mediumint,
						next_encounter_type_hiv mediumint,
						prev_clinical_datetime_hiv datetime,
						next_clinical_datetime_hiv datetime,
                        prev_clinical_location_id mediumint,
                        next_clinical_location_id mediumint,
						prev_clinical_rtc_date_hiv datetime,
                        next_clinical_rtc_date_hiv datetime,


                        
                        outreach_date_bncd datetime,
						outreach_death_date_bncd datetime,
						outreach_patient_care_status_bncd int,
	                    transfer_date_bncd datetime,
						transfer_transfer_out_bncd datetime,

                        primary key encounter_id (encounter_id),
                        index person_date (person_id, encounter_datetime),
						index location_rtc (location_uuid,rtc_date),
						index person_uuid (uuid),
						index location_enc_date (location_uuid,encounter_datetime),
						index enc_date_location (encounter_datetime, location_uuid),
						index location_id_rtc_date (location_id,rtc_date),
                        index location_uuid_rtc_date (location_uuid,rtc_date),
                        index loc_id_enc_date_next_clinical (location_id, encounter_datetime, next_clinical_datetime_hiv),
                        index encounter_type (encounter_type)
					);

					select @last_update := (select max(date_updated) from etl.flat_log where table_name=@table_version);

					
					select @last_update :=
						if(@last_update is null,
							(select max(date_created) from amrs.encounter e join etl.flat_hiv_summary_v15 using (encounter_id)),
							@last_update);

					
					select @last_update := if(@last_update,@last_update,'1900-01-01');
					
					

					
					create  table if not exists flat_hiv_summary_build_queue(person_id int, primary key (person_id));

					
					
					select @num_ids := (select count(*) from flat_hiv_summary_build_queue limit 1);

						
					if (@num_ids=0 or @query_type="sync") then

                        replace into flat_hiv_summary_build_queue
                        (select distinct patient_id 
                            from amrs.encounter
                            where date_changed > @last_update
                        );


                        replace into flat_hiv_summary_build_queue
                        (select distinct person_id 
                            from etl.flat_obs
                            where max_date_created > @last_update
                        
                        
                        );

                        replace into flat_hiv_summary_build_queue
                        (select distinct person_id
                            from etl.flat_lab_obs
                            where max_date_created > @last_update
                        );

                        replace into flat_hiv_summary_build_queue
                        (select distinct person_id
                            from etl.flat_orders
                            where max_date_created > @last_update
                        );
					  end if;

					select @person_ids_count := (select count(*) from flat_hiv_summary_build_queue);

					delete t1 from flat_hiv_summary_v15 t1 join flat_hiv_summary_build_queue t2 using (person_id);

					while @person_ids_count > 0 do

						
						drop table if exists flat_hiv_summary_build_queue_0;

						create temporary table flat_hiv_summary_build_queue_0 (select * from flat_hiv_summary_build_queue limit 100); 


						select @person_ids_count := (select count(*) from flat_hiv_summary_build_queue);

						drop temporary table if exists flat_hiv_summary_0a;
						create temporary table flat_hiv_summary_0a
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
								when t1.encounter_type in (1,2,3,4,10,14,15,17,19,26,32,33,34,47,105,106,112,113,114,117,120,127,128,129,138,153,154,158) then 1
								else null
							end as is_clinical_encounter,

						    case
						        when t1.encounter_type in (116) then 20
								when t1.encounter_type in (1,2,3,4,10,14,15,17,19,26,32,33,34,47,105,106,112,113,114,115,117,120,127,128,138,129,153,154,158) then 10
								else 1
							end as encounter_type_sort_index,

							t2.orders
							from etl.flat_obs t1
								join flat_hiv_summary_build_queue_0 t0 using (person_id)
								left join etl.flat_orders t2 using(encounter_id)
							where t1.encounter_type in (1,2,3,4,10,14,15,17,19,22,23,26,32,33,43,47,21,105,106,110,111,112,113,114,116,117,120,127,128,129,138,153,154,158)
							AND NOT obs regexp "!!5303=(822|664)!!"
						);

						insert into flat_hiv_summary_0a
						(select
							t1.person_id,
							null,
							t1.encounter_id,
							t1.test_datetime,
							t1.encounter_type,
							null, 
							t1.obs,
							null, 
							
							0 as is_clinical_encounter,
							1 as encounter_type_sort_index,
							null
							from etl.flat_lab_obs t1
								join flat_hiv_summary_build_queue_0 t0 using (person_id)
						);

						drop temporary table if exists flat_hiv_summary_0;
						create temporary table flat_hiv_summary_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
						(select * from flat_hiv_summary_0a
						order by person_id, date(encounter_datetime), encounter_type_sort_index
						);


												select @prev_id := null;
						select @cur_id := null;
                        select @prev_encounter_date := null;
						select @cur_encounter_date := null;
						select @enrollment_date := null;
						select @hiv_start_date := null;
						select @cur_location := null;
						select @cur_rtc_date := null;
						select @prev_rtc_date := null;
						select @hiv_start_date := null;
						select @prev_arv_start_date := null;
						select @arv_start_date := null;
						select @prev_arv_end_date := null;
						select @arv_start_location_id := null;
						select @art_first_regimen_start_date := null;
						select @arv_first_regimen := null;
						select @prev_arv_line := null;
						select @cur_arv_line := null;
						select @prev_arv_adherence := null;
						select @cur_arv_adherence := null;
						select @hiv_status_disclosed := null;
						select @first_evidence_pt_pregnant := null;
						select @edd := null;
						select @prev_arv_meds := null;
						select @cur_arv_meds := null;
						select @ipt_start_date := null;
						select @ipt_end_date := null;
                        select @ipt_completion_date := null;
						select @tb_treatment_start_date := null;
						select @tb_treatment_end_date := null;
						select @pcp_prophylaxis_start_date := null;
						select @screened_for_tb := null;
						select @death_date := null;
                        
						select @patient_care_status:=null;

						select @condoms_provided_date := null;
						select @modern_contraceptive_method_start_date := null;

						
						select @cur_who_stage := null;

						select @vl_1:=null;
						select @vl_2:=null;
						select @vl_1_date:=null;
						select @vl_2_date:=null;
						select @vl_resulted:=null;
						select @vl_resulted_date:=null;

						select @cd4_resulted:=null;
						select @cd4_resulted_date:=null;
						select @cd4_1:=null;
						select @cd4_1_date:=null;
						select @cd4_2:=null;
						select @cd4_2_date:=null;
						select @cd4_percent_1:=null;
						select @cd4_percent_1_date:=null;
						select @cd4_percent_2:=null;
						select @cd4_percent_2_date:=null;
						select @vl_order_date := null;
						select @cd4_order_date := null;

						select @hiv_dna_pcr_order_date := null;
						select @hiv_dna_pcr_1:=null;
						select @hiv_dna_pcr_2:=null;
						select @hiv_dna_pcr_1_date:=null;
						select @hiv_dna_pcr_2_date:=null;

						select @hiv_rapid_test_resulted:=null;
						select @hiv_rapid_test_resulted_date:= null;


						
						
						

						drop temporary table if exists flat_hiv_summary_1;
						create temporary table flat_hiv_summary_1 (index encounter_id (encounter_id))
						(select
							obs,
							encounter_type_sort_index,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,
							t1.person_id,
							p.uuid,
							t1.visit_id,
							t1.encounter_id,
							@prev_encounter_date := date(@cur_encounter_date) as prev_encounter_date,
                            @cur_encounter_date := date(encounter_datetime) as cur_encounter_date,
							t1.encounter_datetime,                            
							t1.encounter_type,
							t1.is_clinical_encounter,
													
							case
								when @enrollment_date is null and obs regexp "!!7013=" then @enrollment_date :=  replace(replace((substring_index(substring(obs,locate("!!7013=",obs)),@sep,1)),"!!7013=",""),"!!","")
								when obs regexp "!!7015=" and @enrollment_date is null then @enrollmen_date := "1900-01-01"
								when t1.encounter_type not in (21,@lab_encounter_type) and @enrollment_date is null then @enrollment_date := date(encounter_datetime)
								when @prev_id = @cur_id then @enrollment_date                                
								else @enrollment_date := null
							end as enrollment_date,
                            													                            
                            case
								when @enrollment_location_id is null and obs regexp "!!7030=5622" then @enrollment_location_id := 9999
								when obs regexp "!!7015=" and @enrollment_location_id is null then @enrollmen_location_id := 9999
								when t1.encounter_type not in (21,@lab_encounter_type) and @enrollment_location_id is null then @enrollment_location_id := location_id
								when @prev_id = @cur_id then @enrollment_location_id                               
								else @enrollment_location_id := null
							end as enrollment_location_id,

							
							
							
							
							if(obs regexp "!!1839="
								,replace(replace((substring_index(substring(obs,locate("!!1839=",obs)),@sep,1)),"!!1839=",""),"!!","")
								,null) as scheduled_visit,

							case
								when location_id then @cur_location := location_id
								when @prev_id = @cur_id then @cur_location
								else null
							end as location_id,

							case
						        when @prev_id=@cur_id and t1.encounter_type not in (5,6,7,8,9,21) then @visit_num:= @visit_num + 1
						        when @prev_id != @cur_id then @visit_num := 1
							end as visit_num,

							case
						        when @prev_id=@cur_id then @prev_rtc_date := @cur_rtc_date
						        else @prev_rtc_date := null
							end as prev_rtc_date,

							
							case
								when obs regexp "!!5096=" then @cur_rtc_date := replace(replace((substring_index(substring(obs,locate("!!5096=",obs)),@sep,1)),"!!5096=",""),"!!","")
								when @prev_id = @cur_id then if(@cur_rtc_date > encounter_datetime,@cur_rtc_date,null)
								else @cur_rtc_date := null
							end as cur_rtc_date,
                                                        
                            
							
							case
								when obs regexp "!!1946=1065!!" then 1
								when obs regexp "!!1285=(1287|9068)!!" then 1
								when obs regexp "!!1596=" then 1
								when obs regexp "!!9082=(159|9036|9083|1287|9068|9079|9504|1285)!!" then 1
								when t1.encounter_type = @death_encounter_type then 1
								else null
							end as out_of_care,

							
							
							
							
							case
								when obs regexp "!!1946=1065!!" then @patient_care_status := 9036
								when obs regexp "!!1285=" then @patient_care_status := replace(replace((substring_index(substring(obs,locate("!!1285=",obs)),@sep,1)),"!!1285=",""),"!!","")
								when obs regexp "!!1596=" then @patient_care_status := replace(replace((substring_index(substring(obs,locate("!!1596=",obs)),@sep,1)),"!!1596=",""),"!!","")
								when obs regexp "!!9082=" then @patient_care_status := replace(replace((substring_index(substring(obs,locate("!!9082=",obs)),@sep,1)),"!!9082=",""),"!!","")

								when t1.encounter_type = @death_encounter_type then @patient_care_status := 159
								when t1.encounter_type = @lab_encounter_type and @cur_id != @prev_id then @patient_care_status := null
								when t1.encounter_type = @lab_encounter_type and @cur_id = @prev_id then @patient_care_status
								else @patient_care_status := 6101
							end as patient_care_status,

							
							case
								when obs regexp "!!9203=" then @hiv_start_date := replace(replace((substring_index(substring(obs,locate("!!9203=",obs)),@sep,1)),"!!9203=",""),"!!","")
								when obs regexp "!!7015=" then @hiv_start_date := "1900-01-01"
                                when @hiv_start_date is null then @hiv_start_date := date(encounter_datetime)
								when @prev_id = @cur_id then @hiv_start_date
								else @hiv_start_date := null
							end as hiv_start_date,

							case
								when obs regexp "!!1255=1256!!" or (obs regexp "!!1255=(1257|1259|981|1258|1849|1850)!!" and @arv_start_date is null ) then @arv_start_location_id := location_id
								when @prev_id = @cur_id and obs regexp "!!(1250|1088|2154)=" and @arv_start_date is null then @arv_start_location_id := location_id
								when @prev_id != @cur_id then @arv_start_location_id := null
								else @arv_start_location_id
						    end as arv_start_location_id,

							case
						        when @prev_id=@cur_id and @cur_arv_meds is not null then @prev_arv_meds := @cur_arv_meds
								when @prev_id=@cur_id then @prev_arv_meds
						        else @prev_arv_meds := null
							end as prev_arv_meds,

							
							
							
							
							case
								when obs regexp "!!1255=(1107|1260)!!" then @cur_arv_meds := null
								when obs regexp "!!1250=" then @cur_arv_meds :=
									replace(replace((substring_index(substring(obs,locate("!!1250=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!1250=", "") ) ) / LENGTH("!!1250=") ))),"!!1250=",""),"!!","")
								when obs regexp "!!1088=" then @cur_arv_meds :=
									replace(replace((substring_index(substring(obs,locate("!!1088=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!1088=", "") ) ) / LENGTH("!!1088=") ))),"!!1088=",""),"!!","")
								when obs regexp "!!2154=" then @cur_arv_meds :=
									replace(replace((substring_index(substring(obs,locate("!!2154=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!2154=", "") ) ) / LENGTH("!!2154=") ))),"!!2154=",""),"!!","")
								else @cur_arv_meds:= null
							end as cur_arv_meds,


							case
								when @arv_first_regimen is null and obs regexp "!!2157=" then
										@arv_first_regimen := replace(replace((substring_index(substring(obs,locate("!!2157=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!2157=", "") ) ) / LENGTH("!!2157=") ))),"!!2157=",""),"!!","")
								when @arv_first_regimen is null and @cur_arv_meds is not null then @arv_first_regimen := @cur_arv_meds
								when @prev_id = @cur_id then @arv_first_regimen
								when @prev_id != @cur_id then @arv_first_regimen := null
								else "-1"
							end as arv_first_regimen,


							
							case
								when @arv_first_regimen is null and obs regexp "!!1499=" then @arv_first_regimen_start_date := replace(replace((substring_index(substring(obs,locate("!!1499=",obs)),@sep,1)),"!!1499=",""),"!!","")
								when @arv_first_regimen_start_date is null and @cur_arv_meds is not null then @arv_first_regimen_start_date := date(t1.encounter_datetime)
								when @prev_id = @cur_id then @arv_first_regimen_start_date
								when @prev_id != @cur_id then @arv_first_regimen_start_date := null
								else @arv_first_regimen_start_date
							end as arv_first_regimen_start_date,
                            
                            case
								when @arv_first_regimen is null and obs regexp "!!1499=" then  @arv_first_regimen_location_id := 9999
								when @arv_first_regimen_location_id is null and @cur_arv_meds is not null then @arv_first_regimen_location_id := location_id
								when @prev_id = @cur_id then @arv_first_regimen_location_id
								when @prev_id != @cur_id then @arv_first_regimen_location_id := null
								else "-1"
							end as arv_first_regimen_location_id,


							case
						        when @prev_id=@cur_id then @prev_arv_line := @cur_arv_line
						        else @prev_arv_line := null
							end as prev_arv_line,

							case
								when obs regexp "!!1255=(1107|1260)!!" then @cur_arv_line := null
								when obs regexp "!!1250=(6467|6964|792|633|631)!!" then @cur_arv_line := 1
								when obs regexp "!!1250=(794|635|6160|6159)!!" then @cur_arv_line := 2
								when obs regexp "!!1250=(6156|9759)!!" then @cur_arv_line := 3
								when obs regexp "!!1088=(6467|6964|792|633|631)!!" then @cur_arv_line := 1
								when obs regexp "!!1088=(794|635|6160|6159)!!" then @cur_arv_line := 2
								when obs regexp "!!1088=(6156|9759)!!" then @cur_arv_line := 3
								when obs regexp "!!2154=(6467|6964|792|633|631)!!" then @cur_arv_line := 1
								when obs regexp "!!2154=(794|635|6160|6159)!!" then @cur_arv_line := 2
								when obs regexp "!!2154=(6156|9759)!!" then @cur_arv_line := 3
								when @prev_id = @cur_id then @cur_arv_line

								else @cur_arv_line := null
							end as cur_arv_line,

							case
								when @prev_id=@cur_id then @prev_arv_start_date := @arv_start_date
								else @prev_arv_start_date := null
							end as prev_arv_start_date,

							
							
							
							
							

							case
								when obs regexp "!!1255=(1256|1259|1850)" or (obs regexp "!!1255=(1257|1259|981|1258|1849|1850)!!" and @arv_start_date is null ) then @arv_start_date := date(t1.encounter_datetime)
								when obs regexp "!!1255=(1107|1260)!!" then @arv_start_date := null
								when @cur_arv_meds != @prev_arv_meds and @cur_arv_line != @prev_arv_line then @arv_start_date := date(t1.encounter_datetime)
								when @prev_id != @cur_id then @arv_start_date := null
								else @arv_start_date
							end as arv_start_date,


							case
								when @prev_arv_start_date != @arv_start_date then @prev_arv_end_date  := date(t1.encounter_datetime)
								else @prev_arv_end_date
							end as prev_arv_end_date,

							case
						        when @prev_id=@cur_id then @prev_arv_adherence := @cur_arv_adherence
						        else @prev_arv_adherence := null
							end as prev_arv_adherence,

							
							
							
							
							case
								when obs regexp "!!8288=6343!!" then @cur_arv_adherence := 'GOOD'
								when obs regexp "!!8288=6655!!" then @cur_arv_adherence := 'FAIR'
								when obs regexp "!!8288=6656!!" then @cur_arv_adherence := 'POOR'
								when @prev_id = @cur_id then @cur_arv_adherence
								else @cur_arv_adherence := null
							end as cur_arv_adherence,

							case
								when obs regexp "!!6596=(6594|1267|6595)!!" then  @hiv_status_disclosed := 1
								when obs regexp "!!6596=1118!!" then 0
								when obs regexp "!!6596=" then @hiv_status_disclosed := null
								when @prev_id != @cur_id then @hiv_status_disclosed := null
								else @hiv_status_disclosed
							end as hiv_status_disclosed,


							
							
							
							
							case
								when @prev_id != @cur_id then
									case
										when t1.encounter_type in (32,33,44,10) or obs regexp "!!(1279|5596)=" then @first_evidence_pt_pregnant := encounter_datetime
										else @first_evidence_pt_pregnant := null
									end
								when @first_evidence_pt_pregnant is null and (t1.encounter_type in (32,33,44,10) or obs regexp "!!(1279|5596)=") then @first_evidence_pt_pregnant := encounter_datetime
								when @first_evidence_pt_pregnant and (t1.encounter_type in (11,47,34) or timestampdiff(week,@first_evidence_pt_pregnant,encounter_datetime) > 40 or timestampdiff(week,@edd,encounter_datetime) > 40 or obs regexp "!!5599=|!!1156=1065!!") then @first_evidence_pt_pregnant := null
								else @first_evidence_pt_pregnant
							end as first_evidence_patient_pregnant,

							
							
							
							
							

							case
								when @prev_id != @cur_id then
									case
										when @first_evidence_patient_pregnant and obs regexp "!!1836=" then @edd :=
											date_add(replace(replace((substring_index(substring(obs,locate("!!1836=",obs)),@sep,1)),"!!1836=",""),"!!",""),interval 280 day)
										when obs regexp "!!1279=" then @edd :=
											date_add(encounter_datetime,interval (40-replace(replace((substring_index(substring(obs,locate("!!1279=",obs)),@sep,1)),"!!1279=",""),"!!","")) week)
										when obs regexp "!!5596=" then @edd :=
											replace(replace((substring_index(substring(obs,locate("!!5596=",obs)),@sep,1)),"!!5596=",""),"!!","")
										when @first_evidence_pt_pregnant then @edd := date_add(@first_evidence_pt_pregnant,interval 6 month)
										else @edd := null
									end
								when @edd is null or @edd = @first_evidence_pt_pregnant then
									case
										when @first_evidence_pt_pregnant then @edd := date_add(@first_evidence_pt_pregnant,interval 6 month)
										when @first_evidence_patient_pregnant and obs regexp "!!1836=" then @edd :=
											date_add(replace(replace((substring_index(substring(obs,locate("!!1836=",obs)),@sep,1)),"!!1836=",""),"!!",""),interval 280 day)
										when obs regexp "!!1279=" then @edd :=
											date_add(encounter_datetime,interval (40-replace(replace((substring_index(substring(obs,locate("!!1279=",obs)),@sep,1)),"!!1279=",""),"!!","")) week)
										when obs regexp "!!5596=" then @edd :=
											replace(replace((substring_index(substring(obs,locate("!!5596=",obs)),@sep,1)),"!!5596=",""),"!!","")
										when @first_evidence_pt_pregnant then @edd := date_add(@first_evidence_pt_pregnant,interval 6 month)
										else @edd
									end
								when @edd and (t1.encounter_type in (11,47,34) or timestampdiff(week,@edd,encounter_datetime) > 4 or obs regexp "!!5599|!!1145=1065!!") then @edd := null
								else @edd
							end as edd,

							
							
							
							
							
							
							
							case
								when obs regexp "!!6174=" then @screened_for_tb := true 
								when obs regexp "!!2022=1065!!" then @screened_for_tb := true 
								when obs regexp "!!307=" then @screened_for_tb := true 
								when obs regexp "!!12=" then @screened_for_tb := true 
								when obs regexp "!!1271=(12|307|8064|2311|2323)!!" then @screened_for_tb := true 
								when orders regexp "(12|307|8064|2311|2323)" then @screened_for_tb := true 
								when obs regexp "!!1866=(12|307|8064|2311|2323)!!" then @screened_for_tb := true 
								when obs regexp "!!5958=1077!!" then @screened_for_tb := true 
								when obs regexp "!!2020=1065!!" then @screened_for_tb := true 
								when obs regexp "!!2021=1065!!" then @screened_for_tb := true 
								when obs regexp "!!2028=" then @screened_for_tb := true 
								when obs regexp "!!1268=(1256|1850)!!" then @screened_for_tb := true
								when obs regexp "!!5959=(1073|1074)!!" then @screened_for_tb := true 
								when obs regexp "!!5971=(1073|1074)!!" then @screened_for_tb := true 
								when obs regexp "!!1492=107!!" then @screened_for_tb := true 
								when obs regexp "!!1270=" and obs not regexp "!!1268=1257!!" then @screened_for_tb := true
							end as screened_for_tb,


							case
								when obs regexp "!!1265=(1107|1260)!!" then @on_ipt := 0
								when obs regexp "!!1265=!!" then @on_ipt := 1
								when obs regexp "!!1110=656!!" then @on_ipt := 1
								when @prev_id = @cur_id then @on_ipt
								else null
							end as on_ipt,

							
							
							
							
							case
								when obs regexp "!!1265=(1256|1850)!!" then @ipt_start_date := encounter_datetime
								when obs regexp "!!1265=(1257|981|1406|1849)!!" and @ipt_start_date is null then @ipt_start_date := encounter_datetime
								when @cur_id = @prev_id then @ipt_start_date
								else @ipt_start_date := null
							end as ipt_start_date,


							
							
							
							
							case
								when obs regexp "!!1266=!!" then @ipt_stop_date :=  encounter_datetime
								when @cur_id = @prev_id then @ipt_stop_date
								else @ipt_stop_date
							end as ipt_stop_date,
                            
                            case
								when obs regexp "!!1266=1267!!" then @ipt_completion_date :=  encounter_datetime
								when @cur_id = @prev_id then @ipt_completion_date
								else @ipt_completion_date
							end as ipt_completion_date,
                            

							case
								when obs regexp "!!1268=(1107|1260)!!" then @on_tb_treatment := 0
                                when obs regexp "!!1268=" then @on_tb_treatment := 1 
                                when obs regexp "!!1111=" and obs not regexp "!!1111=(1267|1107)!!" then @on_tb_treatment := 1
                                else 0
                            end as on_tb_tx,

							
							
							
							
                            
							case
								when obs regexp "!!1113=" then @tb_treatment_start_date := date(replace(replace((substring_index(substring(obs,locate("!!1113=",obs)),@sep,1)),"!!1113=",""),"!!",""))
								when obs regexp "!!1268=1256!!" then @tb_treatment_start_date := encounter_datetime
								when obs regexp "!!1268=(1257|1259|1849|981)!!" and obs regexp "!!7015=" and @tb_treatment_start_date is null then @tb_treatment_start_date := null
								when obs regexp "!!1268=(1257|1259|1849|981)!!" and @tb_treatment_start_date is null then @tb_treatment_start_date := encounter_datetime
								
								when obs regexp "!!1111=" and obs not regexp "!!1111=(1267|1107)!!" and @tb_treatment_start_date is null then @tb_treatment_start_date := encounter_datetime
								when @cur_id = @prev_id then @tb_treatment_start_date
								else @tb_treatment_start_date := null
							end as tb_tx_start_date,

                            
							
							
							case
									when obs regexp "!!2041=" then @tb_treatment_end_date := date(replace(replace((substring_index(substring(obs,locate("!!2041=",obs)),@sep,1)),"!!2041=",""),"!!",""))
									when obs regexp "!!1268=1260!!" then @tb_treatment_end_date := encounter_datetime
									when @cur_id = @prev_id then @tb_treatment_end_date
									else @tb_treatment_end_date := null
							end as tb_tx_end_date,


							
							
							
							case
								when obs regexp "!!1261=(1107|1260)!!" then @pcp_prophylaxis_start_date := null
								when obs regexp "!!1261=(1256|1850)!!" then @pcp_prophylaxis_start_date := encounter_datetime
								when obs regexp "!!1261=1257!!" and @pcp_prophylaxis_start_date is null then @pcp_prophylaxis_start_date := encounter_datetime
								when obs regexp "!!1109=(916|92)!!" and @pcp_prophylaxis_start_date is null then @pcp_prophylaxis_start_date := encounter_datetime
								when obs regexp "!!1193=916!!" and @pcp_prophylaxis_start_date is null then @pcp_prophylaxis_start_date := encounter_datetime
								when @prev_id=@cur_id then @pcp_prophylaxis_start_date
								else @pcp_prophylaxis_start_date := null
							end as pcp_prophylaxis_start_date,

							
							
							
							
							
							

							case
								when p.dead or p.death_date then @death_date := p.death_date
								when obs regexp "!!1570=" then @death_date := replace(replace((substring_index(substring(obs,locate("!!1570=",obs)),@sep,1)),"!!1570=",""),"!!","")
								when @prev_id != @cur_id or @death_date is null then
									case
										when obs regexp "!!(1734|1573)=" then @death_date := encounter_datetime
										when obs regexp "!!(1733|9082|6206)=159!!" or t1.encounter_type=31 then @death_date := encounter_datetime
										else @death_date := null
									end
								else @death_date
							end as death_date,

							
							case
								when @prev_id=@cur_id then
									case
										when t1.encounter_type = @lab_encounter_type and obs regexp "!!5497=[0-9]" and @cd4_1 >= 0 and date(encounter_datetime)<>@cd4_1_date then @cd4_2:= @cd4_1
										else @cd4_2
									end
								else @cd4_2:=null
							end as cd4_2,

							case
								when @prev_id=@cur_id then
									case
										when t1.encounter_type=@lab_encounter_type and obs regexp "!!5497=[0-9]" and @cd4_1 >= 0 then @cd4_2_date:= @cd4_1_date
										else @cd4_2_date
									end
								else @cd4_2_date:=null
							end as cd4_2_date,

							case
								when t1.encounter_type = @lab_encounter_type and obs regexp "!!5497=[0-9]" then @cd4_date_resulted := date(encounter_datetime)
								when @prev_id = @cur_id and date(encounter_datetime) = @cd4_date_resulted then @cd4_date_resulted
							end as cd4_resulted_date,

							case
								when t1.encounter_type = @lab_encounter_type and obs regexp "!!5497=[0-9]" then @cd4_resulted := cast(replace(replace((substring_index(substring(obs,locate("!!5497=",obs)),@sep,1)),"!!5497=",""),"!!","") as unsigned)
								when @prev_id = @cur_id and date(encounter_datetime) = @cd4_date_resulted then @cd4_resulted
							end as cd4_resulted,



							case
								when t1.encounter_type = @lab_encounter_type and obs regexp "!!5497=[0-9]" then @cd4_1:= cast(replace(replace((substring_index(substring(obs,locate("!!5497=",obs)),@sep,1)),"!!5497=",""),"!!","") as unsigned)
								when @prev_id=@cur_id then @cd4_1
								else @cd4_1:=null
							end as cd4_1,


							case
								when t1.encounter_type = @lab_encounter_type and obs regexp "!!5497=[0-9]" then @cd4_1_date:=date(encounter_datetime)
								when @prev_id=@cur_id then @cd4_1_date
								else @cd4_1_date:=null
							end as cd4_1_date,

							
							case
								when @prev_id=@cur_id then
									case
										when t1.encounter_type=@lab_encounter_type and obs regexp "!!730=[0-9]" and @cd4_percent_1 >= 0
											then @cd4_percent_2:= @cd4_percent_1
										else @cd4_percent_2
									end
								else @cd4_percent_2:=null
							end as cd4_percent_2,

							case
								when @prev_id=@cur_id then
									case
										when obs regexp "!!730=[0-9]" and t1.encounter_type = @lab_encounter_type and @cd4_percent_1 >= 0 then @cd4_percent_2_date:= @cd4_percent_1_date
										else @cd4_percent_2_date
									end
								else @cd4_percent_2_date:=null
							end as cd4_percent_2_date,


							case
								when t1.encounter_type = @lab_encounter_type and obs regexp "!!730=[0-9]"
									then @cd4_percent_1:= cast(replace(replace((substring_index(substring(obs,locate("!!730=",obs)),@sep,1)),"!!730=",""),"!!","") as unsigned)
								when @prev_id=@cur_id then @cd4_percent_1
								else @cd4_percent_1:=null
							end as cd4_percent_1,

							case
								when obs regexp "!!730=[0-9]" and t1.encounter_type = @lab_encounter_type then @cd4_percent_1_date:=date(encounter_datetime)
								when @prev_id=@cur_id then @cd4_percent_1_date
								else @cd4_percent_1_date:=null
							end as cd4_percent_1_date,


							
							case
									when @prev_id=@cur_id then
										case
											when obs regexp "!!856=[0-9]" and @vl_1 >= 0
												and (replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!","")) <>date(@vl_1_date) then @vl_2:= @vl_1
											else @vl_2
										end
									else @vl_2:=null
							end as vl_2,

							case
									when @prev_id=@cur_id then
										case
											when obs regexp "!!856=[0-9]" and @vl_1 >= 0
												and (replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!","")) <>date(@vl_1_date) then @vl_2_date:= @vl_1_date
											else @vl_2_date
										end
									else @vl_2_date:=null
							end as vl_2_date,

							case
								when t1.encounter_type = @lab_encounter_type and obs regexp "!!856=[0-9]" then @vl_date_resulted := date(encounter_datetime)
								when @prev_id = @cur_id and date(encounter_datetime) = @vl_date_resulted then @vl_date_resulted
							end as vl_resulted_date,

							case
								when t1.encounter_type = @lab_encounter_type and obs regexp "!!856=[0-9]" then @vl_resulted := cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") as unsigned)
								when @prev_id = @cur_id and date(encounter_datetime) = @vl_date_resulted then @vl_resulted
							end as vl_resulted,

							case
									when obs regexp "!!856=[0-9]" and t1.encounter_type = @lab_encounter_type then @vl_1:=cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") as unsigned)
									when obs regexp "!!856=[0-9]"
											and (@vl_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!",""),@vl_1_date)) > 30)
											and (@vl_1_date is null or (replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!","")) > @vl_1_date)
										then @vl_1 := cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") as unsigned)
									when @prev_id=@cur_id then @vl_1
									else @vl_1:=null
							end as vl_1,

                            case
                                when obs regexp "!!856=[0-9]" and t1.encounter_type = @lab_encounter_type then @vl_1_date:= encounter_datetime
                                when obs regexp "!!856=[0-9]"
                                        and (@vl_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!",""),@vl_1_date)) > 30)
                                        and (@vl_1_date is null or (replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!","")) > @vl_1_date)
                                    then @vl_1_date := replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!","")
                                when @prev_id=@cur_id then @vl_1_date
                                else @vl_1_date:=null
                            end as vl_1_date,



							
							
							case
								when obs regexp "!!1271=856!!" then @vl_order_date := date(encounter_datetime)
								when orders regexp "856" then @vl_order_date := date(encounter_datetime)
								when @prev_id=@cur_id and (@vl_1_date is null or @vl_1_date < @vl_order_date) then @vl_order_date
								else @vl_order_date := null
							end as vl_order_date,

							
							case
								when obs regexp "!!1271=657!!" then @cd4_order_date := date(encounter_datetime)
								when orders regexp "657" then @cd4_order_date := date(encounter_datetime)
								when @prev_id=@cur_id then @cd4_order_date
								else @cd4_order_date := null
							end as cd4_order_date,

								
							case
							  when obs regexp "!!1271=1030!!" then @hiv_dna_pcr_order_date := date(encounter_datetime)
							  when orders regexp "1030" then @hiv_dna_pcr_order_date := date(encounter_datetime)
							  when @prev_id=@cur_id then @hiv_dna_pcr_order_date
							  else @hiv_dna_pcr_order_date := null
							end as hiv_dna_pcr_order_date,

							case
							  when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" then encounter_datetime
							  when obs regexp "!!1030=[0-9]"
								  and (@hiv_dna_pcr_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!1030=",obs_datetimes)),@sep,1)),"!!1030=",""),"!!",""),@hiv_dna_pcr_1_date)) > 30)
								then replace(replace((substring_index(substring(obs_datetimes,locate("1030=",obs_datetimes)),@sep,1)),"1030=",""),"!!","")
							end as hiv_dna_pcr_resulted_date,

							case
							  when @prev_id=@cur_id then
								case
								  when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" and @hiv_dna_pcr_1 >= 0 and date(encounter_datetime)<>@hiv_dna_pcr_1_date then @hiv_dna_pcr_2:= @hiv_dna_pcr_1
								  when obs regexp "!!1030=[0-9]" and @hiv_dna_pcr_1 >= 0
									and abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!1030=",obs_datetimes)),@sep,1)),"!!1030=",""),"!!",""),@hiv_dna_pcr_1_date)) > 30 then @hiv_dna_pcr_2 := @hiv_dna_pcr_1
								  else @hiv_dna_pcr_2
								end
							  else @hiv_dna_pcr_2:=null
							end as hiv_dna_pcr_2,

							case
							  when @prev_id=@cur_id then
								case
								  when t1.encounter_type=@lab_encounter_type and obs regexp "!!1030=[0-9]" and @hiv_dna_pcr_1 >= 0 and date(encounter_datetime)<>@hiv_dna_pcr_1_date then @hiv_dna_pcr_2_date:= @hiv_dna_pcr_1_date
								  when obs regexp "!!1030=[0-9]" and @hiv_dna_pcr_1 >= 0
									and abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("1030=",obs_datetimes)),@sep,1)),"1030=",""),"!!",""),@hiv_dna_pcr_1_date)) > 30 then @hiv_dna_pcr_2_date:= @hiv_dna_pcr_1_date
								  else @hiv_dna_pcr_2_date
								end
							  else @hiv_dna_pcr_2_date:=null
							end as hiv_dna_pcr_2_date,

							case
							  when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" then cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
							  when obs regexp "!!1030=[0-9]"
								and (@hiv_dna_pcr_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!1030=",obs_datetimes)),@sep,1)),"!!1030=",""),"!!",""),@hiv_dna_pcr_1_date)) > 30)
								then cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
							end as hiv_dna_pcr_resulted,

							case
							  when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" then @hiv_dna_pcr_1:= cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
							  when obs regexp "!!1030=[0-9]"
								  and (@hiv_dna_pcr_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!1030=",obs_datetimes)),@sep,1)),"!!1030=",""),"!!","") ,@hiv_dna_pcr_1_date)) > 30)
								then @hiv_dna_pcr_1 := cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
							  when @prev_id=@cur_id then @hiv_dna_pcr_1
							  else @hiv_dna_pcr_1:=null
							end as hiv_dna_pcr_1,


							case
							  when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" then @hiv_dna_pcr_1_date:=date(encounter_datetime)
							  when obs regexp "!!1030=[0-9]"
								  and (@hiv_dna_pcr_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!1030=",obs_datetimes)),@sep,1)),"!!1030=",""),"!!","") ,@hiv_dna_pcr_1_date)) > 30)
								then @hiv_dna_pcr_1_date := replace(replace((substring_index(substring(obs_datetimes,locate("!!1030=",obs_datetimes)),@sep,1)),"!!1030=",""),"!!","")
							  when @prev_id=@cur_id then @hiv_dna_pcr_1_date
							  else @hiv_dna_pcr_1_date:=null
							end as hiv_dna_pcr_1_date,

							
							case
							  when t1.encounter_type = @lab_encounter_type and obs regexp "!!(1040|1042)=[0-9]" then encounter_datetime
							end as hiv_rapid_test_resulted_date,

							case
							  when t1.encounter_type = @lab_encounter_type and obs regexp "!!(1040|1042)=[0-9]" then cast(replace(replace((substring_index(substring(obs,locate("!!(1040|1042)=",obs)),@sep,1)),"!!(1040|1042)=",""),"!!","") as unsigned)
							end as hiv_rapid_test_resulted,
                            
							case
								when obs regexp "!!8302=8305!!" then @condoms_provided_date := encounter_datetime
								when obs regexp "!!374=(190|6717|6718)!!" then @condoms_provided_date := encounter_datetime
								when obs regexp "!!6579=" then @condoms_provided_date := encounter_datetime
                                when @prev_id = @cur_id then @condoms_provided_date
								else @condoms_provided_date := null
                            end as condoms_provided_date,
								
                                
                            
                            
                            
                            
                            
							

							case
								when obs regexp "!!7240=(5275|6220|780|5279)!!"
									then @modern_contraceptive_method_start_date := encounter_datetime
								when obs regexp "!!374=(5275|6220|780|5279)!!" and obs regexp "!!1190="
									then @modern_contraceptive_method_start_date :=  replace(replace((substring_index(substring(obs,locate("!!1190=",obs)),@sep,1)),"!!1190=",""),"!!","")
								when obs regexp "!!374=(5275|6220|780|5279)!!"
									then @modern_contraceptive_method_start_date :=  encounter_datetime
								when @prev_id = @cur_id then @modern_contraceptive_method_start_date
								else @modern_contraceptive_method_start_date := null
							end as modern_contraceptive_method_start_date,
                            

							
							
							
							
							

							
							
							
							
							
							case
								when obs regexp "!!5356=(1204)!!" then @cur_who_stage := 1
								when obs regexp "!!5356=(1205)!!" then @cur_who_stage := 2
								when obs regexp "!!5356=(1206)!!" then @cur_who_stage := 3
								when obs regexp "!!5356=(1207)!!" then @cur_who_stage := 4
								when obs regexp "!!1224=(1220)!!" then @cur_who_stage := 1
								when obs regexp "!!1224=(1221)!!" then @cur_who_stage := 2
								when obs regexp "!!1224=(1222)!!" then @cur_who_stage := 3
								when obs regexp "!!1224=(1223)!!" then @cur_who_stage := 4
								when @prev_id = @cur_id then @cur_who_stage
								else @cur_who_stage := null
							end as cur_who_stage

						from flat_hiv_summary_0 t1
							join amrs.person p using (person_id)
						);
                        
				

						select @prev_id := null;
						select @cur_id := null;
						select @prev_encounter_datetime := null;
						select @cur_encounter_datetime := null;

						select @prev_clinical_datetime := null;
						select @cur_clinical_datetime := null;

						select @next_encounter_type := null;
						select @cur_encounter_type := null;

                        select @prev_clinical_location_id := null;
						select @cur_clinical_location_id := null;


						alter table flat_hiv_summary_1 drop prev_id, drop cur_id;

						drop table if exists flat_hiv_summary_2;
						create temporary table flat_hiv_summary_2
						(select *,
							@prev_id := @cur_id as prev_id,
							@cur_id := person_id as cur_id,

							case
								when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
								else @prev_encounter_datetime := null
							end as next_encounter_datetime_hiv,

							@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
								else @next_encounter_type := null
							end as next_encounter_type_hiv,

							@cur_encounter_type := encounter_type as cur_encounter_type,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as next_clinical_datetime_hiv,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as next_clinical_location_id,

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
							end as next_clinical_rtc_date_hiv,

							case
								when is_clinical_encounter then @cur_clinical_rtc_date := cur_rtc_date
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinical_rtc_date,

							case
							    when @prev_id != @cur_id then null
                                when is_clinical_encounter then @outreach_date_bncd
                                else null
                            end as outreach_date_bncd,

                            case
                                when encounter_type=21 and @outreach_date_bncd is null then @outreach_date_bncd := encounter_datetime
								when is_clinical_encounter then @outreach_date_bncd := null
								when @prev_id != @cur_id then @outreach_date_bncd := null
                                else @outreach_date_bncd
                            end as next_outreach_date_bncd,

                            case
                                when @prev_id != @cur_id then null
                                when is_clinical_encounter then @outreach_death_date_bncd
                                else null
                            end as outreach_death_date_bncd,

                            case
                                when encounter_type=21 and @outreach_death_date_bncd  is null then @outreach_death_date_bncd  := death_date
                                when is_clinical_encounter then @outreach_death_date_bncd  := null
                                when @prev_id != @cur_id then @outreach_death_date_bncd := null
                                else @outreach_death_date_bncd
                            end as next_outreach_death_date_bncd,


                            case
                                when @prev_id != @cur_id then null
                                when is_clinical_encounter then cast(@outreach_patient_care_status_bncd as unsigned)
                                else null
                            end as outreach_patient_care_status_bncd,

                            case
                                when encounter_type=21 and @outreach_patient_care_status_bncd is null then @outreach_patient_care_status_bncd := patient_care_status
                                when is_clinical_encounter then @outreach_patient_care_status_bncd := null
                                when @prev_id != @cur_id then @outreach_patient_care_status_bncd := null
                                else @outreach_patient_care_status_bncd
                            end as next_outreach_patient_care_status_bncd,

                            case
							    when @prev_id != @cur_id then null
                                when is_clinical_encounter then @transfer_date_bncd
                                else null
                            end as transfer_date_bncd,

                            case
                                when encounter_type=116 and @transfer_date_bncd is null then @transfer_date_bncd := encounter_datetime
								when is_clinical_encounter then @transfer_date_bncd := null
								when @prev_id != @cur_id then @transfer_date_bncd := null
                                else @transfer_date_bncd
                            end as next_transfer_date_bncd,

							case
							    when @prev_id != @cur_id then null
                                when is_clinical_encounter then @transfer_transfer_out_bncd
                                else null
                            end as transfer_transfer_out_bncd,

                            case
                                when encounter_type=116 and @transfer_transfer_out_bncd is null then @transfer_transfer_out_bncd := encounter_datetime
								when is_clinical_encounter then @transfer_transfer_out_bncd := null
								when @prev_id != @cur_id then @transfer_transfer_out_bncd := null
                                else @transfer_transfer_out_bncd
                            end as next_transfer_transfer_out_bncd

							from flat_hiv_summary_1
							order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
						);

						alter table flat_hiv_summary_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;

						select @prev_id := null;
						select @cur_id := null;
						select @prev_encounter_type := null;
						select @cur_encounter_type := null;
						select @prev_encounter_datetime := null;
						select @cur_encounter_datetime := null;
						select @prev_clinical_datetime := null;
						select @cur_clinical_datetime := null;
                        select @prev_clinical_location_id := null;
						select @cur_clinical_location_id := null;

						drop temporary table if exists flat_hiv_summary_3;
						create temporary table flat_hiv_summary_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
						(select
							*,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,

							case
						        when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
						        else @prev_encounter_type:=null
							end as prev_encounter_type_hiv,	
                            @cur_encounter_type := encounter_type as cur_encounter_type,

							case
						        when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
						        else @prev_encounter_datetime := null
						    end as prev_encounter_datetime_hiv,

                            @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as prev_clinical_datetime_hiv,

                            case
								when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
								else @prev_clinical_location_id := null
							end as prev_clinical_location_id,

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
							end as prev_clinical_rtc_date_hiv,

							case
								when is_clinical_encounter then @cur_clinical_rtc_date := cur_rtc_date
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinic_rtc_date

							from flat_hiv_summary_2 t1
							order by person_id, date(encounter_datetime), encounter_type_sort_index
						);
                                        
						alter table flat_hiv_summary_3 drop prev_id, drop cur_id;

						select @prev_id := null;
						select @cur_id := null;
						select @transfer_in := null;
                        select @transfer_in_date := null;
                        select @transfer_in_location_id := null;
						select @transfer_out := null;
                        select @transfer_out_date := null;
                        select @transfer_out_location_id := null;
				
						drop temporary table if exists flat_hiv_summary_4;

						create temporary table flat_hiv_summary_4 ( index person_enc (person_id, encounter_datetime))
						(select
							*,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,
                                                    
                            
							
							case
								when obs regexp "!!7015=" then @transfer_in := 1
								when prev_clinical_location_id != location_id then @transfer_in := 1
								else @transfer_in := null
							end as transfer_in,

							case 
								when obs regexp "!!7015=" then @transfer_in_date := encounter_datetime
								when prev_clinical_location_id != location_id then @transfer_in_date := encounter_datetime
                                when @cur_id = @prev_id then @transfer_in_date
								else @transfer_in_date := null
							end transfer_in_date,
                            
							case 
								when obs regexp "!!7015=1287" then @transfer_in_location_id := 9999
								when prev_clinical_location_id != location_id then @transfer_in_location_id := prev_clinical_location_id
								when @cur_id = @prev_id then @transfer_in_location_id
								else @transfer_in_location_id := null
							end transfer_in_location_id,



							
							
							
							
							
							
							
                            
                            case
									when obs regexp "!!1285=!!" then @transfer_out := 1
									when obs regexp "!!1596=1594!!" then @transfer_out := 1
									when obs regexp "!!9082=(1287|1594|9068|9504|1285)!!" then @transfer_out := 1
                                    when next_clinical_location_id != location_id then @transfer_out := 1
									else @transfer_out := null
							end as transfer_out,

							case 
								when obs regexp "!!1285=(1287|9068|2050)!!" and next_clinical_datetime_hiv is null then @transfer_out_location_id := 9999
								when obs regexp "!!1285=1286!!" and next_clinical_datetime_hiv is null then @transfer_out_location_id := 9998
								when next_clinical_location_id != location_id then @transfer_out_location_id := next_clinical_location_id
								else @transfer_out_location_id := null
							end transfer_out_location_id,

                            
                            case 
								when @transfer_out and next_clinical_datetime_hiv is null then @transfer_out_date := cur_rtc_date                                
								when next_clinical_location_id != location_id then @transfer_out_date := next_clinical_datetime_hiv
                                when transfer_transfer_out_bncd then @transfer_out_date := transfer_transfer_out_bncd
								else @transfer_out_date := null
							end transfer_out_date

                                                        
						
							from flat_hiv_summary_3 t1
							order by person_id, date(encounter_datetime), encounter_type_sort_index
						);

						  
                        replace into flat_hiv_summary_v15
						(select                        
								person_id,
								t1.uuid,
								visit_id,
								encounter_id,
								encounter_datetime,
								encounter_type,
								is_clinical_encounter,

								location_id,
								t2.uuid as location_uuid,

								visit_num,
							
								enrollment_date,                        
								enrollment_location_id,
														
								hiv_start_date,

								death_date,
								scheduled_visit,
								
								transfer_in,
								transfer_in_location_id ,
								transfer_in_date,
								
								transfer_out,
								transfer_out_location_id,
								transfer_out_date,

								patient_care_status,
								out_of_care,
								prev_rtc_date,
								cur_rtc_date as rtc_date,
								
								arv_first_regimen ,
								arv_first_regimen_location_id,
								arv_first_regimen_start_date,

								prev_arv_meds,
								cur_arv_meds,
								arv_start_date ,						
								arv_start_location_id,
								prev_arv_start_date,
								prev_arv_end_date,						
								
								prev_arv_line,
								cur_arv_line,

								
								prev_arv_adherence,
								cur_arv_adherence,
								hiv_status_disclosed,

								first_evidence_patient_pregnant,
								edd,

								screened_for_tb,

								ipt_start_date,
								ipt_stop_date,
                                ipt_completion_date,

								tb_tx_start_date,
								tb_tx_end_date,
								
								pcp_prophylaxis_start_date,
								
								condoms_provided_date,
								modern_contraceptive_method_start_date,
								cur_who_stage,

								cd4_resulted,
								cd4_resulted_date,
								cd4_1,
								cd4_1_date,
								cd4_2,
								cd4_2_date,
								cd4_percent_1,
								cd4_percent_1_date,
								cd4_percent_2,
								cd4_percent_2_date,
								vl_resulted,
								vl_resulted_date,
								vl_1,
								vl_1_date,
								vl_2,
								vl_2_date,
								vl_order_date,
								cd4_order_date,

								
								hiv_dna_pcr_order_date,
								hiv_dna_pcr_resulted,
								hiv_dna_pcr_resulted_date,
								hiv_dna_pcr_1,
								hiv_dna_pcr_1_date,
								hiv_dna_pcr_2,
								hiv_dna_pcr_2_date,

								
								hiv_rapid_test_resulted,
								hiv_rapid_test_resulted_date,

								prev_encounter_datetime_hiv,
								next_encounter_datetime_hiv,
								prev_encounter_type_hiv,
								next_encounter_type_hiv,
								prev_clinical_datetime_hiv,
								next_clinical_datetime_hiv,
								prev_clinical_location_id,
								next_clinical_location_id,
								prev_clinical_rtc_date_hiv,
								next_clinical_rtc_date_hiv,

								
								outreach_date_bncd,
								outreach_death_date_bncd,
								outreach_patient_care_status_bncd,
								transfer_date_bncd,
								transfer_transfer_out_bncd

							from flat_hiv_summary_4 t1
								join amrs.location t2 using (location_id));

				    delete from flat_hiv_summary_build_queue where person_id in (select person_id from flat_hiv_summary_build_queue_0);

				 end while;

				
				delete t1
				from flat_hiv_summary_v15 t1
				join amrs.person_attribute t2 using (person_id)
				where t2.person_attribute_type_id=28 and value='true' and voided=0;

				 select @end := now();
				 insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
				 select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");

		END$$
DELIMITER ;
