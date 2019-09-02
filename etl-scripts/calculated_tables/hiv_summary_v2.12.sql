#********************************************************************************************************
#* CREATION OF MOH INDICATORS TABLE ****************************************************************************
#********************************************************************************************************

# Need to first create this temporary table to sort the data by person,encounterdateime.
# This allows us to use the previous row's data when making calculations.
# It seems that if you don't create the temporary table first, the sort is applied
# to the final result. Any references to the previous row will not an ordered row.
# v2.1 Notes:
#     Updated out_of_care to include untraceable
#     Added tb_prophylaxis_start_date
#     Updated patient_care_status to be more inclusive of other status questions
#     Fixed problem with next_clinic_datetime_hiv

# v2.2 Notes:
#      Added encounter types for GENERALNOTE (112), CLINICREVIEW (113), MOH257BLUECARD (114), HEIFOLLOWUP (115), TRANSFERFORM (116)

# v2.3 Notes:
#      Added arv_first_regimen_start_date and arv_start_location. This makes it easier to query for the cumulative ever indicator
#	   Added visit_id This makes it easier to query  visits related indicators eg scheduled, unscheduled
#	   Added prev_clinical_rtc_date_hiv and next_clinical_rtc_date_hiv this required for creating outreach dataset


# v2.4 Notes:
#Corrected Errors with definations for vl_1 and vl_2

# v2.5 Notes:
#Changed the definition arv_start_date  to account for regimen changes
#Added prev_arv_start_date track when they stared the previous regimen
#Added prev_arv_end_date to track when they stopped the previous regimen
#Added prev_arv_line to track the arv line the patient was previously on
#Added prev_arv_meds to track the arv medications the patient was previously on
#Fixed arv_start_date and arv_first_regimen_start_date

# v2.7 Notes:
#This indicators were added inorder to produce vl supression assesment report
#Added hiv_status_disclosed indicator
#Added prev_arv_adherence, cur_arv_adherence indicators
#Added prev_vl_1_date and prev_vl_1
#fixed cur_clinic_datetime to include NONCLINICALENCOUNTER Encounter type

# v2.8 Notes:
#Removed prev_vl_1_date and prev_vl_1
#Fixed vl_2 and vl_2_date

# v2.9 Notes:
#Added tb_prophylaxis_end_date and modified tb_prophylaxis_start_date
#fixed tb_tx_start_date defintion

# v2.10 Notes:
# added encounter type 120 to is_clinical_encounter
# added encounter type index so that flat_defaulters can execute much faster
# added tb_screening_result indicator
# added index for location_uuid_rtc_date
# added hiv_exposed_occupational and pep_start_date

# v2.11 Notes:
# Added encounter date_change tracking to update patients whose encounter
# data changes without affecting obs or orders eg a change in encounter location
# removed PEP indicators (hiv_exposed_occupational and pep_start_date) from this flat table: this was affecting hiv active in care.

#v2.12 Notes:
# added encounter type 127 and 128 to is_clinical_encounter
# Added Concept TRANSFER CARE TO OTHER CENTER to transfer_out indicator
# Added ability to rebuild/sync at any day of the week
# Added ability to continue rebuilding in case of an error
# Renamed new_data_person_ids table to flat_hiv_summary_queue and made it permanent
# Added 1594 (PATIENT TRANSFERRED OUT) to transfer_out indicator

drop procedure if exists generate_hiv_summary;
DELIMITER $$
	CREATE PROCEDURE generate_hiv_summary()
		BEGIN
                    select @query_type := "sync"; # this can be either sync or rebuild
					select @start := now();
					select @start := now();
					select @table_version := "flat_hiv_summary_v2.12";

					set session sort_buffer_size=512000000;

					select @sep := " ## ";
					select @lab_encounter_type := 99999;
					select @death_encounter_type := 31;
					select @last_date_created := (select max(max_date_created) from etl.flat_obs);

					#drop table if exists flat_hiv_summary;
					#delete from flat_log where table_name="flat_hiv_summary";
					create table if not exists flat_hiv_summary (
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
						hiv_start_date datetime,
						death_date datetime,
						scheduled_visit int,
						transfer_out int,
						transfer_in int,

						patient_care_status int,
						out_of_care int,
						prev_rtc_date datetime,
						rtc_date datetime,

							arv_start_location int,
							arv_first_regimen_start_date datetime,
						arv_start_date datetime,
							prev_arv_start_date datetime,
							prev_arv_end_date datetime,

						arv_first_regimen varchar(500),
							prev_arv_meds varchar(500),
						cur_arv_meds varchar(500),
							prev_arv_line int,
						cur_arv_line int,

						#ARV adherence
						prev_arv_adherence varchar(200),
						cur_arv_adherence varchar(200),
						hiv_status_disclosed int,

						first_evidence_patient_pregnant datetime,
						edd datetime,
						screened_for_tb boolean,
						tb_screening_result boolean,
						tb_prophylaxis_start_date datetime,
						tb_prophylaxis_end_date datetime,
						tb_tx_start_date datetime,
						tb_tx_end_date datetime,
						pcp_prophylaxis_start_date datetime,
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

						# 1030 dna pcr
						hiv_dna_pcr_order_date datetime,
						hiv_dna_pcr_resulted int,
						hiv_dna_pcr_resulted_date datetime,
						hiv_dna_pcr_1 int,
						hiv_dna_pcr_1_date datetime,
						hiv_dna_pcr_2 int,
						hiv_dna_pcr_2_date datetime,

						# 1040 hiv rapid test
						hiv_rapid_test_resulted int,
						hiv_rapid_test_resulted_date datetime,

						condoms_provided int,
						using_modern_contraceptive_method int,
						#Current WHO Stage 5356
						cur_who_stage int,
						prev_encounter_datetime_hiv datetime,
						next_encounter_datetime_hiv datetime,
						prev_encounter_type_hiv mediumint,
						next_encounter_type_hiv mediumint,
						prev_clinical_datetime_hiv datetime,
						next_clinical_datetime_hiv datetime,

						prev_clinical_rtc_date_hiv datetime,
                        next_clinical_rtc_date_hiv datetime,
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

					# then use the max_date_created from amrs.encounter. This takes about 10 seconds and is better to avoid.
					select @last_update :=
						if(@last_update is null,
							(select max(date_created) from amrs.encounter e join etl.flat_hiv_summary using (encounter_id)),
							@last_update);

					#otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
					select @last_update := if(@last_update,@last_update,'1900-01-01');
					#select @last_update := "2016-09-12"; #date(now());
					#select @last_date_created := "2015-11-17"; #date(now());

					# drop table if exists flat_hiv_summary_queue;
					create  table if not exists flat_hiv_summary_queue(person_id int, primary key (person_id));

					# we will add new patient id to be rebuilt when either we  are in sync mode or if the existing table is empty
					# this will allow us to restart rebuilding the table if it crashes in the middle of a rebuild
					select @num_ids := (select count(*) from flat_hiv_summary_queue limit 1);

					if (@num_ids=0 or @query_type="sync") then

                        replace into flat_hiv_summary_queue
                        (select distinct patient_id #, min(encounter_datetime) as start_date
                            from amrs.encounter
                            where date_changed > @last_update
                        );


                        replace into flat_hiv_summary_queue
                        (select distinct person_id #, min(encounter_datetime) as start_date
                            from etl.flat_obs
                            where max_date_created > @last_update
                        #	group by person_id
                        # limit 10
                        );

                        replace into flat_hiv_summary_queue
                        (select distinct person_id
                            from etl.flat_lab_obs
                            where max_date_created > @last_update
                        );

                        replace into flat_hiv_summary_queue
                        (select distinct person_id
                            from etl.flat_orders
                            where max_date_created > @last_update
                        );
					  end if;

					select @person_ids_count := (select count(*) from flat_hiv_summary_queue);

					delete t1 from flat_hiv_summary t1 join flat_hiv_summary_queue t2 using (person_id);

					while @person_ids_count > 0 do

						#create temp table with a set of person ids
						drop table if exists flat_hiv_summary_queue_0;

						create temporary table flat_hiv_summary_queue_0 (select * from flat_hiv_summary_queue limit 5000); #TODO - change this when data_fetch_size changes


						select @person_ids_count := (select count(*) from flat_hiv_summary_queue);

						drop table if exists flat_hiv_summary_0a;
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
							# in any visit, there many be multiple encounters. for this dataset, we want to include only clinical encounters (e.g. not lab or triage visit)
							case
								when t1.encounter_type in (1,2,3,4,10,14,15,17,19,26,32,33,34,47,105,106,112,113,114,115,117,120,127,128,129) then 1
								else null
							end as is_clinical_encounter,

						    case
						        when t1.encounter_type in (116) then 20
								when t1.encounter_type in (1,2,3,4,10,14,15,17,19,26,32,33,34,47,105,106,112,113,114,115,117,120,127,128,129) then 10
								else 1
							end as encounter_type_sort_index,

							t2.orders
							from etl.flat_obs t1
								join flat_hiv_summary_queue_0 t0 using (person_id)
								left join etl.flat_orders t2 using(encounter_id)
						#		join flat_hiv_summary_queue t0 on t1.person_id=t0.person_id and t1.encounter_datetime >= t0.start_date
							where t1.encounter_type in (1,2,3,4,10,14,15,17,19,22,23,26,32,33,43,47,21,105,106,110,111,112,113,114,115,116,117,120,127,128,129)
						);

						insert into flat_hiv_summary_0a
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
								join flat_hiv_summary_queue_0 t0 using (person_id)
						);

						drop table if exists flat_hiv_summary_0;
						create temporary table flat_hiv_summary_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
						(select * from flat_hiv_summary_0a
						order by person_id, date(encounter_datetime), encounter_type_sort_index
						);


						select @prev_id := null;
						select @cur_id := null;
						select @enrollment_date := null;
						select @hiv_start_date := null;
						select @cur_location := null;
						select @cur_rtc_date := null;
						select @prev_rtc_date := null;
						select @hiv_start_date := null;
						select @prev_arv_start_date := null;
						select @arv_start_date := null;
						select @prev_arv_end_date := null;
						select @arv_start_location := null;
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
						select @tb_prophylaxis_start_date := null;
						select @tb_prophylaxis_end_date := null;
						select @tb_treatment_start_date := null;
						select @tb_treatment_end_date := null;
						select @pcp_prophylaxis_start_date := null;
						select @screened_for_tb := null;
						select @tb_screening_result := null;
						select @death_date := null;
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

						select @patient_care_status:=null;

						select @condoms_provided := null;
						select @using_modern_contraceptive_method := null;

						#Current WHO Stage
						select @cur_who_stage := null;

						#TO DO
						# screened for cervical ca
						# exposed infant

						drop temporary table if exists flat_hiv_summary_1;
						create temporary table flat_hiv_summary_1 (index encounter_id (encounter_id))
						(select
							encounter_type_sort_index,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,
							t1.person_id,
							p.uuid,
							t1.visit_id,
							t1.encounter_id,
							t1.encounter_datetime,
							t1.encounter_type,
							t1.is_clinical_encounter,

							case
								when @prev_id != @cur_id and t1.encounter_type in (21,@lab_encounter_type) then @enrollment_date := null
								when @prev_id != @cur_id then @enrollment_date := encounter_datetime
								when t1.encounter_type not in (21,@lab_encounter_type) and @enrollment_date is null then @enrollment_date := encounter_datetime
								else @enrollment_date
							end as enrollment_date,

							#1839 = CURRENT VISIT TYPE
							#1246 = SCHEDULED VISIT
							#1838 = UNSCHEDULED VISIT LATE
							#1837 = UNSCHEDULED VISIT EARLY
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
						        when @prev_id!=@cur_id then @visit_num := 1
							end as visit_num,

							case
						        when @prev_id=@cur_id then @prev_rtc_date := @cur_rtc_date
						        else @prev_rtc_date := null
							end as prev_rtc_date,

							# 5096 = return visit date
							case
								when obs regexp "!!5096=" then @cur_rtc_date := replace(replace((substring_index(substring(obs,locate("!!5096=",obs)),@sep,1)),"!!5096=",""),"!!","")
								when @prev_id = @cur_id then if(@cur_rtc_date > encounter_datetime,@cur_rtc_date,null)
								else @cur_rtc_date := null
							end as cur_rtc_date,

							# 7015 = TRANSFER IN CARE FROM OTHER CENTER
							case
								when obs regexp "!!7015=" then @transfer_in := replace(replace((substring_index(substring(obs,locate("!!7015=",obs)),@sep,1)),"!!7015=",""),"!!","")
								else @transfer_in := null
							end as transfer_in,

							# 1285 = TRANSFER CARE TO OTHER CENTER
							# 1596 = REASON EXITED CARE
							# 9082 = PATIENT CARE STATUS

							case
								when obs regexp "!!1285=(1287|9068)!!" then 1
								when obs regexp "!!1596=1594!!" then 1
								when obs regexp "!!9082=(1287|1594|9068|9504|1285)!!" then 1
								else null
							end as transfer_out,

							# 1946 = DISCONTINUE FROM CLINIC, HIV NEGATIVE
							case
								when obs regexp "!!1946=1065!!" then 1
								when obs regexp "!!1285=(1287|9068)!!" then 1
								when obs regexp "!!1596=" then 1
								when obs regexp "!!9082=(159|9036|9083|1287|9068|9079|9504|1285)!!" then 1
								when t1.encounter_type = @death_encounter_type then 1
								else null
							end as out_of_care,

							# 9082 = PATIENT CARE STATUS
							# 6101 = CONTINUE
							# 1946 = DISCONTINUE FROM CLINIC, HIV NEGATIVE (this is a question)
							# 9036 = HIV NEGATIVE, NO LONGER AT RISK
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

							# 1946 = DISCONTINUE FROM CLINIC, HIV NEGATIVE
							# 1088 = CURRENT ANTIRETROVIRAL DRUGS USED FOR TREATMENT
							# 1255 = ANTIRETROVIRAL PLAN
							# 1040 = HIV RAPID TEST, QUALITATIVE
							# 1030 = HIV DNA POLYMERASE CHAIN REACTION, QUALITATIVE
							# 664 = POSITIVE
							case
								when obs regexp "!!1946=1065!!" then @hiv_start_date := null
								when t1.encounter_type=@lab_encounter_type and obs regexp "!!(1040|1030)=664!!" then @hiv_start_date:=null
								when @prev_id != @cur_id or @hiv_start_date is null then
									case
										when obs regexp "!!(1040|1030)=664!!" then @hiv_start_date := date(encounter_datetime)
										when obs regexp "!!(1088|1255)=" then @hiv_start_date := date(t1.encounter_datetime)
										else @hiv_start_date := null
									end
								else @hiv_start_date
							end as hiv_start_date,

							case
								when obs regexp "!!1255=1256!!" or (obs regexp "!!1255=(1257|1259|981|1258|1849|1850)!!" and @arv_start_date is null ) then @arv_start_location := location_id
								when @prev_id = @cur_id and obs regexp "!!(1250|1088|2154)=" and @arv_start_date is null then @arv_start_location := location_id
								when @prev_id != @cur_id then @arv_start_location := null
								else @arv_start_location
						    end as arv_start_location,

							case
						        when @prev_id=@cur_id then @prev_arv_meds := @cur_arv_meds
						        else @prev_arv_meds := null
							end as prev_arv_meds,
							# 1255 = ANTIRETROVIRAL PLAN
							# 1250 = ANTIRETROVIRALS STARTED
							# 1088 = CURRENT ANTIRETROVIRAL DRUGS USED FOR TREATMENT
							# 2154 = PATIENT REPORTED CURRENT ANTIRETROVIRAL TREATMENT
							case
								when obs regexp "!!1255=(1107|1260)!!" then @cur_arv_meds := null
								when obs regexp "!!1250=" then @cur_arv_meds :=
									replace(replace((substring_index(substring(obs,locate("!!1250=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!1250=", "") ) ) / LENGTH("!!1250=") ))),"!!1250=",""),"!!","")
								when obs regexp "!!1088=" then @cur_arv_meds :=
									replace(replace((substring_index(substring(obs,locate("!!1088=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!1088=", "") ) ) / LENGTH("!!1088=") ))),"!!1088=",""),"!!","")
								when obs regexp "!!2154=" then @cur_arv_meds :=
									replace(replace((substring_index(substring(obs,locate("!!2154=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!2154=", "") ) ) / LENGTH("!!2154=") ))),"!!2154=",""),"!!","")
								when @prev_id=@cur_id then @cur_arv_meds
								else @cur_arv_meds:= null
							end as cur_arv_meds,


							case
								when @arv_first_regimen is null and @cur_arv_meds is not null then @arv_first_regimen := @cur_arv_meds
								when @prev_id = @cur_id then @arv_first_regimen
								else @arv_first_regimen := null
							end as arv_first_regimen,

							case
								when @arv_first_regimen_start_date is null and (obs regexp "!!1255=(1256|1259|1850)" or obs regexp "!!1255=(1257|1259|981|1258|1849|1850)!!") then @arv_first_regimen_start_date := date(t1.encounter_datetime)
								when @prev_id != @cur_id then @arv_first_regimen_start_date := null
                                else @arv_first_regimen_start_date
							end as arv_first_regimen_start_date,

							case
						        when @prev_id=@cur_id then @prev_arv_line := @cur_arv_line
						        else @prev_arv_line := null
							end as prev_arv_line,

							case
								when obs regexp "!!1255=(1107|1260)!!" then @cur_arv_line := null
								when obs regexp "!!1250=(6467|6964|792|633|631)!!" then @cur_arv_line := 1
								when obs regexp "!!1250=(794|635|6160|6159)!!" then @cur_arv_line := 2
								when obs regexp "!!1250=6156!!" then @cur_arv_line := 3
								when obs regexp "!!1088=(6467|6964|792|633|631)!!" then @cur_arv_line := 1
								when obs regexp "!!1088=(794|635|6160|6159)!!" then @cur_arv_line := 2
								when obs regexp "!!1088=6156!!" then @cur_arv_line := 3
								when obs regexp "!!2154=(6467|6964|792|633|631)!!" then @cur_arv_line := 1
								when obs regexp "!!2154=(794|635|6160|6159)!!" then @cur_arv_line := 2
								when obs regexp "!!2154=6156!!" then @cur_arv_line := 3
								when @prev_id = @cur_id then @cur_arv_line
								else @cur_arv_line := null
							end as cur_arv_line,

							case
				        when @prev_id=@cur_id then @prev_arv_start_date := @arv_start_date
				        else @prev_arv_start_date := null
							end as prev_arv_start_date,

							# 1255 = ANTIRETROVIRAL PLAN
							# 1250 = ANTIRETROVIRALS STARTED
							# 1088 = CURRENT ANTIRETROVIRAL DRUGS USED FOR TREATMENT
							# 2154 = PATIENT REPORTED CURRENT ANTIRETROVIRAL TREATMENT
							# 1260 = STOP ALL MEDICATIONS

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

							# 8288 = ANTIRETROVIRAL ADHERENCE SINCE LAST VISIT
							# 6343 = GOOD
							# 6655 = FAIR
							# 6656 = POOR
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


							# 1279 = NUMBER OF WEEKS PREGNANT
							# 5596 = ESTIMATED DATE OF CONFINEMENT
							# 5599 = DATE OF CONFINEMENT
							# 1146 = CONCEPTION SINCE LAST VISIT
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

							# 1836 = LAST MENSTRUAL PERIOD DATE
							# 1279 = NUMBER OF WEEKS PREGNANT
							# 5596 = ESTIMATED DATE OF CONFINEMENT
							# 5599 = DATE OF CONFINEMENT
							# 1146 = CONCEPTION SINCE LAST VISIT

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

							# 6174 = REVIEW OF TUBERCULOSIS SCREENING QUESTIONS
							# 2022 = TUBERCULOSIS DIAGNOSED THIS VISIT
							# 1268 = TUBERCULOSIS TREATMENT PLAN
							# 1270 = TUBERCULOSIS TREATMENT STARTED
							# 1257 = CONTINUE REGIMEN
							# 1256 = START,
							# 1850 = RESTART
							case
								when obs regexp "!!6174=" then @screened_for_tb := true #there is an obs for "any symptoms of tb?"
								when obs regexp "!!2022=1065!!" then @screened_for_tb := true #1065 = yes
								when obs regexp "!!307=" then @screened_for_tb := true #test result for tb
								when obs regexp "!!12=" then @screened_for_tb := true #test result for tb
								when obs regexp "!!1271=(12|307|8064|2311|2323)!!" then @screened_for_tb := true #test ordered for tb
								when orders regexp "(12|307|8064|2311|2323)" then @screened_for_tb := true #test ordered for tb
								when obs regexp "!!1866=(12|307|8064|2311|2323)!!" then @screened_for_tb := true #test results for tb
								when obs regexp "!!5958=1077!!" then @screened_for_tb := true #means cough is bloody
								when obs regexp "!!2020=1065!!" then @screened_for_tb := true #means a familiy member was diagnosed for tb
								when obs regexp "!!2021=1065!!" then @screened_for_tb := true #means a familiy member was diagnosed for tb
								when obs regexp "!!2028=" then @screened_for_tb := true #TB DIAGNOSIS MADE ON THE BASIS OF
								when obs regexp "!!1268=(1256|1850)!!" then @screened_for_tb := true
								when obs regexp "!!5959=(1073|1074)!!" then @screened_for_tb := true #COUGH DURATION, CODED
								when obs regexp "!!5971=(1073|1074)!!" then @screened_for_tb := true #CHEST PAIN DURATION, CODED
								when obs regexp "!!1492=107!!" then @screened_for_tb := true #REVIEW OF SYSTEM, EXPRESS CARE(1492)=COUGH (107)
								when obs regexp "!!1270=" and obs not regexp "!!1268=1257!!" then @screened_for_tb := true
							end as screened_for_tb,

							# 6174 = REVIEW OF TUBERCULOSIS SCREENING QUESTIONS
							# 2022 = TUBERCULOSIS DIAGNOSED THIS VISIT
							# 1268 = TUBERCULOSIS TREATMENT PLAN
							# 1270 = TUBERCULOSIS TREATMENT STARTED
							# 1257 = CONTINUE REGIMEN
							# 1256 = START,
							# 1850 = RESTART
							case

								when obs regexp "!!2022=1065!!" then @tb_screening_result := true #1065 = yes
								when obs regexp "!!307=" then @tb_screening_result := true #test result for tb
								when obs regexp "!!12=" then @tb_screening_result := true #test result for tb
								when obs regexp "!!1271=(12|307|8064|2311|2323)!!" then @tb_screening_result := true #test ordered for tb
								when orders regexp "(12|307|8064|2311|2323)" then @tb_screening_result := true #test ordered for tb
								when obs regexp "!!1866=(12|307|8064|2311|2323)!!" then @tb_screening_result := true #test results for tb
								when obs regexp "!!5958=1077!!" then @tb_screening_result := true #means cough is bloody
								when obs regexp "!!2020=1065!!" then @tb_screening_result := true #means a familiy member was diagnosed for tb
								when obs regexp "!!2021=1065!!" then @tb_screening_result := true #means a familiy member was diagnosed for tb
								when obs regexp "!!2028=" then @tb_screening_result := true #TB DIAGNOSIS MADE ON THE BASIS OF
								when obs regexp "!!1268=(1256|1850)!!" then @tb_screening_result := true
								when obs regexp "!!5959=(1073|1074)!!" then @tb_screening_result := true #COUGH DURATION, CODED
								when obs regexp "!!5971=(1073|1074)!!" then @tb_screening_result := true #CHEST PAIN DURATION, CODED
								when obs regexp "!!1492=107!!" then @tb_screening_result := true #REVIEW OF SYSTEM, EXPRESS CARE(1492)=COUGH (107)
								when obs regexp "!!1270=" and obs not regexp "!!1268=1257!!" then @tb_screening_result := true
                                when obs not regexp "!!6174=1107" then @tb_screening_result := true
                                  else @tb_screening_result := false
							  end as tb_screening_result,

							case
								when obs regexp "!!1265=(1256|1257|1850)!!" then @on_tb_prophylaxis := 1
								when obs regexp "!!1110=656!!" then @on_tb_prophylaxis := 1
								when @prev_id = @cur_id then @on_tb_prophylaxis
								else null
							end as on_tb_prophylaxis,

							# 1265 = TUBERCULOSIS PROPHYLAXIS PLAN
							# 1268 = TUBERCULOSIS TREATMENT PLAN
							# 1110 = PATIENT REPORTED CURRENT TUBERCULOSIS PROPHYLAXIS
							# 656 = ISONIAZID
							case
								when @cur_id != @prev_id then
									case
                                        when obs regexp "!!1265=(1256|1850)!!" then @tb_prophylaxis_start_date := encounter_datetime
                                        when obs regexp "!!1265=(1257|981|1406|1849)!!" then @tb_prophylaxis_start_date := encounter_datetime
										when obs regexp "!!1110=656!!" then @tb_prophylaxis_start_date := encounter_datetime
                                        else @tb_prophylaxis_start_date := null
									end
								when @cur_id = @prev_id then
									case
										when obs regexp "!!1265=(1256|1850)!!" then @tb_prophylaxis_start_date := encounter_datetime
                                        when @tb_prophylaxis_start_date is not null then @tb_prophylaxis_start_date
                                        when obs regexp "!!1265=(1257|981|1406|1849)!!" then @tb_prophylaxis_start_date := encounter_datetime
                                        when obs regexp "!!1110=656!!" then @tb_prophylaxis_start_date := encounter_datetime
									end
							end as tb_prophylaxis_start_date,

							# 1265 = TUBERCULOSIS PROPHYLAXIS PLAN
							# 1268 = TUBERCULOSIS TREATMENT PLAN
							# 1110 = PATIENT REPORTED CURRENT TUBERCULOSIS PROPHYLAXIS
							# 656 = ISONIAZID
							case
								when @cur_id != @prev_id then
									case
										when obs regexp "!!1265=1260!!" then @tb_prophylaxis_end_date :=  encounter_datetime
                                        else @tb_prophylaxis_end_date := null
                                    end
								when @cur_id = @prev_id then
									case
										when obs regexp "!!1265=1260!!" then @tb_prophylaxis_end_date :=  encounter_datetime
                                        when  @tb_prophylaxis_end_date is not null then @tb_prophylaxis_end_date
										when @tb_prophylaxis_start_date is not null and obs regexp "!!1110=1107!!" and obs regexp "!!1265=" and obs regexp "!!1265=(1107|1260)!!" then @tb_prophylaxis_end_date := encounter_datetime
									end
							end as tb_prophylaxis_end_date,


							# 1111 = PATIENT REPORTED CURRENT TUBERCULOSIS TREATMENT
							# 1267 = COMPLETED
							# 1107 = NONE
							case
								when @cur_id != @prev_id then
									case
										when obs regexp "!!1113=" then @tb_treatment_start_date := date(replace(replace((substring_index(substring(obs,locate("!!1113=",obs)),@sep,1)),"!!1113=",""),"!!",""))
                                        when obs regexp "!!1268=1256!!" then @tb_treatment_start_date := encounter_datetime
                                        when obs regexp "!!1268=(1257|1259|1849|981)!!" then @tb_treatment_start_date := encounter_datetime
                                        when obs regexp "!!1111=" and obs not regexp "!!1111=(1267|1107)!!" then @tb_treatment_start_date := encounter_datetime
                                        else @tb_treatment_start_date := null
									end
								when @cur_id = @prev_id then
									case
										when obs regexp "!!1113=" then @tb_treatment_start_date := date(replace(replace((substring_index(substring(obs,locate("!!1113=",obs)),@sep,1)),"!!1113=",""),"!!",""))
                                        when @tb_treatment_start_date is not null then @tb_treatment_start_date
										when obs regexp "!!1268=1256!!" then @tb_treatment_start_date := encounter_datetime
                                        when obs regexp "!!1268=(1257|1259|1849|981)!!" then @tb_treatment_start_date := encounter_datetime
                                        when obs regexp "!!1111=" and obs not regexp "!!1111=(1267|1107)!!" then @tb_treatment_start_date := encounter_datetime
									end
							end as tb_tx_start_date,

                            # 1111 = PATIENT REPORTED CURRENT TUBERCULOSIS TREATMENT
							# 1267 = COMPLETED
							# 1107 = NONE
							case
								when @cur_id != @prev_id then
									case
										when obs regexp "!!2041=" then @tb_treatment_end_date := date(replace(replace((substring_index(substring(obs,locate("!!2041=",obs)),@sep,1)),"!!2041=",""),"!!",""))
                                        when obs regexp "!!1268=1260!!" then @tb_treatment_end_date := encounter_datetime
                                        else @tb_treatment_end_date := null
									end
								when @cur_id = @prev_id then
									case
										when obs regexp "!!2041=" then @tb_treatment_end_date := date(replace(replace((substring_index(substring(obs,locate("!!2041=",obs)),@sep,1)),"!!2041=",""),"!!",""))
                                        when obs regexp "!!1268=1260!!" then @tb_treatment_end_date := encounter_datetime
										when @tb_treatment_end_date is not null then @tb_treatment_end_date
                                        when @tb_treatment_start_date is not null and obs regexp "!!6176=1066!!" and !(obs regexp "!!1268=(1256|1257|1259|1849|981)!!") then @tb_treatment_end_date := encounter_datetime
									end
							end as tb_tx_end_date,

							# 1109 = PATIENT REPORTED CURRENT PCP PROPHYLAXIS
							# 1261 = PCP PROPHYLAXIS PLAN
							# 1193 = CURRENT MEDICATIONS
							case
								when obs regexp "!!1261=(1107|1260)!!" then @pcp_prophylaxis_start_date := null
								when obs regexp "!!1261=(1256|1850)!!" then @pcp_prophylaxis_start_date := encounter_datetime
								when obs regexp "!!1261=1257!!" then
									case
										when @prev_id!=@cur_id or @pcp_prophylaxis_start_date is null then @pcp_prophylaxis_start_date := encounter_datetime
										else @pcp_prophylaxis_start_date
									end
								when obs regexp "!!1109=(916|92)!!" and @pcp_prophylaxis_start_date is null then @pcp_prophylaxis_start_date := encounter_datetime
								when obs regexp "!!1193=916!!" and @pcp_prophylaxis_start_date is null then @pcp_prophylaxis_start_date := encounter_datetime
								when @prev_id=@cur_id then @pcp_prophylaxis_start_date
								else @pcp_prophylaxis_start_date := null
							end as pcp_prophylaxis_start_date,

							# 1570 = DATE OF DEATH
							# 1734 = DEATH REPORTED BY
							# 1573 = CAUSE FOR DEATH
							# 1733 = REASON FOR MISSED VISIT
							# 9082 = PATIENT CARE STATUS
							# 6206 = OUTCOME AT END OF TUBERCULOSIS TREATMENT

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

							# 5497 = CD4, BY FACS
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

							# 730 = CD4%, BY FACS
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


							# 856 = HIV VIRAL LOAD, QUANTITATIVE
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



							# 1271 = TESTS ORDERED
							# 856 = HIV VIRAL LOAD, QUANTITATIVE
							case
								when obs regexp "!!1271=856!!" then @vl_order_date := date(encounter_datetime)
								when orders regexp "856" then @vl_order_date := date(encounter_datetime)
								when @prev_id=@cur_id and (@vl_1_date is null or @vl_1_date < @vl_order_date) then @vl_order_date
								else @vl_order_date := null
							end as vl_order_date,

							# 657 = CD4 PANEL
							case
								when obs regexp "!!1271=657!!" then @cd4_order_date := date(encounter_datetime)
								when orders regexp "657" then @cd4_order_date := date(encounter_datetime)
								when @prev_id=@cur_id then @cd4_order_date
								else @cd4_order_date := null
							end as cd4_order_date,

								# 1030 = HIV DNA PCR
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

							#1040|1042 hiv rapid test
							case
							  when t1.encounter_type = @lab_encounter_type and obs regexp "!!(1040|1042)=[0-9]" then encounter_datetime
							end as hiv_rapid_test_resulted_date,

							case
							  when t1.encounter_type = @lab_encounter_type and obs regexp "!!(1040|1042)=[0-9]" then cast(replace(replace((substring_index(substring(obs,locate("!!(1040|1042)=",obs)),@sep,1)),"!!(1040|1042)=",""),"!!","") as unsigned)
							end as hiv_rapid_test_resulted,


							case
								when obs regexp "!!8302=8305!!" then @condoms_provided := 1
								when obs regexp "!!374=(190|6717|6718)!!" then @condoms_provided := 1
								when obs regexp "!!6579=" then @condoms_provided := 1
								else null
							end as condoms_provided,

							case
								when obs regexp "!!374=(5275|6220|780|5279)!!" then @using_modern_conctaceptive_method := 1
								else null
							end as using_modern_contraceptive_method,

							#Current WHO Stage 5356 - Adults
							#Adult Who Stage 1 - 1204
							#Adult Who Stage 2 - 1205
							#Adult Who Stage 3 - 1206
							#Adult Who Stage 4 - 1207

							#Current WHO Stage 1224 - Peds
							#Adult Who Stage 1 - 1220
							#Adult Who Stage 2 - 1221
							#Adult Who Stage 3 - 1222
							#Adult Who Stage 4 - 1223
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
								when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
								when @prev_id = @cur_id then @cur_clinical_datetime
								else @cur_clinical_datetime := null
							end as cur_clinic_datetime,

						    case
								when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
								else @prev_clinical_rtc_date := null
							end as next_clinical_rtc_date_hiv,

							case
								when is_clinical_encounter then @cur_clinical_rtc_date := cur_rtc_date
								when @prev_id = @cur_id then @cur_clinical_rtc_date
								else @cur_clinical_rtc_date:= null
							end as cur_clinical_rtc_date

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

						drop temporary table if exists flat_hiv_summary_3;
						create temporary table flat_hiv_summary_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
						(select
							*,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,

							case
						        when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
						        else @prev_encounter_type:=null
							end as prev_encounter_type_hiv,	@cur_encounter_type := encounter_type as cur_encounter_type,

							case
						        when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
						        else @prev_encounter_datetime := null
						    end as prev_encounter_datetime_hiv, @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

							case
								when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
								else @prev_clinical_datetime := null
							end as prev_clinical_datetime_hiv,

							case
								when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
								when @prev_id = @cur_id then @cur_clinical_datetime
								else @cur_clinical_datetime := null
							end as cur_clinical_datetime,

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

						replace into flat_hiv_summary
						(select
							person_id,
							t1.uuid,
							t1.visit_id,
						    encounter_id,
							encounter_datetime,
							encounter_type,
							is_clinical_encounter,
							location_id,
							t2.uuid as location_uuid,
							visit_num,
							enrollment_date,
							hiv_start_date,
							death_date,
							scheduled_visit,
							transfer_out,
							transfer_in,
						    patient_care_status,
							out_of_care,
							prev_rtc_date,
							cur_rtc_date,
						    arv_start_location,
						    arv_first_regimen_start_date,
							arv_start_date,
							prev_arv_start_date,
						    prev_arv_end_date,
							arv_first_regimen,
						    prev_arv_meds,
							cur_arv_meds,
						    prev_arv_line,
							cur_arv_line,
							prev_arv_adherence,
							cur_arv_adherence,
							hiv_status_disclosed,
						    first_evidence_patient_pregnant,
						    edd,
							screened_for_tb,
							tb_screening_result,
							tb_prophylaxis_start_date,
                            tb_prophylaxis_end_date,
							tb_tx_start_date,
							tb_tx_end_date,
							pcp_prophylaxis_start_date,
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
							condoms_provided,
							using_modern_contraceptive_method,
							cur_who_stage,
							prev_encounter_datetime_hiv,
							next_encounter_datetime_hiv,
							prev_encounter_type_hiv,
							next_encounter_type_hiv,
							prev_clinical_datetime_hiv,
							next_clinical_datetime_hiv,
							prev_clinical_rtc_date_hiv,
						    next_clinical_rtc_date_hiv
							from flat_hiv_summary_3 t1
								join amrs.location t2 using (location_id));

				    delete from flat_hiv_summary_queue where person_id in (select person_id from flat_hiv_summary_queue_0);

				 end while;

				 select @end := now();
				 insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
				 select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");

		END $$
	DELIMITER ;

call generate_hiv_summary();
