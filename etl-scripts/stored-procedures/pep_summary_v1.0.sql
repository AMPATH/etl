#********************************************************************************************************
#* CREATION OF PEP INDICATORS TABLE ****************************************************************************
#********************************************************************************************************

# Need to first create this temporary table to sort the data by person,encounterdateime.
# This allows us to use the previous row's data when making calculations.
# It seems that if you don't create the temporary table first, the sort is applied
# to the final result. Any references to the previous row will not an ordered row.
# v1 Notes:
# added hiv_exposed_occupational and pep_start_date

#drop procedure if exists generate_pep_summary;
DELIMITER $$
	CREATE PROCEDURE generate_pep_summary()
		BEGIN

					select @start := now();
					select @start := now();
					select @table_version := "flat_pep_summary_v1.0";

					set session sort_buffer_size=512000000;

					select @sep := " ## ";
					select @last_date_created := (select max(max_date_created) from etl.flat_obs);

					#drop table if exists flat_pep_summary;
					#delete from flat_log where table_name="flat_pep_summary";
					create table if not exists flat_pep_summary (
						person_id int,
						uuid varchar(100),
						visit_id int,
						encounter_id int,
						encounter_datetime datetime,
						encounter_type int,
						location_id int,
						location_uuid varchar(100),
						enrollment_date datetime,
						prev_rtc_date datetime,
						rtc_date datetime,
                        hiv_exposed_occupational boolean,
                        pep_start_date date,

                        primary key encounter_id (encounter_id),
                        index person_date (person_id, encounter_datetime),
						index location_rtc (location_uuid,rtc_date),
						index person_uuid (uuid),
						index location_enc_date (location_uuid,encounter_datetime),
						index enc_date_location (encounter_datetime, location_uuid),
						index location_id_rtc_date (location_id,rtc_date),
                        index location_uuid_rtc_date (location_uuid,rtc_date),
                        index encounter_type (encounter_type)
					);

					select @last_update := (select max(date_updated) from etl.flat_log where table_name=@table_version);

					# then use the max_date_created from amrs.encounter. This takes about 10 seconds and is better to avoid.
					select @last_update :=
						if(@last_update is null,
							(select max(date_created) from amrs.encounter e join etl.flat_pep_summary using (encounter_id)),
							@last_update);

					#otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
					select @last_update := if(@last_update,@last_update,'1900-01-01');
					#select @last_update := "2016-09-12"; #date(now());
					#select @last_date_created := "2015-11-17"; #date(now());

					drop table if exists new_data_person_ids;
					create temporary table new_data_person_ids(person_id int, primary key (person_id));
                      replace into new_data_person_ids
                        (select distinct patient_id #, min(encounter_datetime) as start_date
                            from amrs.encounter
                            where date_changed > @last_update
                        );

                        replace into new_data_person_ids
                        (select distinct person_id #, min(encounter_datetime) as start_date
                            from etl.flat_obs
                            where max_date_created > @last_update
                        #	group by person_id
                        # limit 10
                        );

                        # This is for terminal display only
                        (select distinct person_id #, min(encounter_datetime) as start_date
                            from etl.flat_obs
                            where max_date_created > @last_update
                        #	group by person_id
                        # limit 10
                        );

					select @person_ids_count := (select count(*) from new_data_person_ids);

					delete t1 from flat_pep_summary t1 join new_data_person_ids t2 using (person_id);

					while @person_ids_count > 0 do

						#create temp table with a set of person ids
						drop table if exists new_data_person_ids_0;

						create temporary table new_data_person_ids_0 (select * from new_data_person_ids limit 10000); #TODO - change this when data_fetch_size changes

						delete from new_data_person_ids where person_id in (select person_id from new_data_person_ids_0);

						select @person_ids_count := (select count(*) from new_data_person_ids);

						drop table if exists flat_pep_summary_0a;
						create temporary table flat_pep_summary_0a
						(select
							t1.person_id,
							t1.visit_id,
							t1.encounter_id,
							t1.encounter_datetime,
							t1.encounter_type,
							t1.location_id,
							t1.obs,
							t1.obs_datetimes
							from etl.flat_obs t1
								join new_data_person_ids_0 t0 using (person_id)
							where t1.encounter_type in (56,57)
						);

						drop table if exists flat_pep_summary_0;
						create temporary table flat_pep_summary_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
						(select * from flat_pep_summary_0a
						order by person_id, date(encounter_datetime)
						);


						select @prev_id := null;
						select @cur_id := null;
						select @enrollment_date := null;
						select @cur_location := null;
						select @cur_rtc_date := null;
						select @prev_rtc_date := null;

						#OCCUPATIONAL PEP
						select @hiv_exposed_occupational := null;
						select @pep_start_date := null;

						drop temporary table if exists flat_pep_summary_1;
						create temporary table flat_pep_summary_1 (index encounter_id (encounter_id))
						(select
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,
							t1.person_id,
							p.uuid as uuid,
							t1.visit_id,
							t1.encounter_id,
							t1.encounter_datetime,
							t1.encounter_type,

							case
								when @prev_id != @cur_id then @enrollment_date := encounter_datetime
								when  @enrollment_date is null then @enrollment_date := encounter_datetime
								else @enrollment_date
							end as enrollment_date,

							case
								when location_id then @cur_location := location_id
								when @prev_id = @cur_id then @cur_location
								else null
							end as location_id,

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

							case
								when  obs regexp "!!1972=(5619|5507|1496|6280|8710|8713|8714)" then @hiv_exposed_occupational := 1
                                when !(obs regexp "!!1972=(5619|5507|1496|6280|8710|8713|8714)") then @hiv_exposed_occupational := 0
                                when @prev_id = @cur_id then @hiv_exposed_occupational
                                else @hiv_exposed_occupational := null
							end as hiv_exposed_occupational,
                            case
								when encounter_type = 56 and (obs regexp "!!1705=1149" or obs regexp  "!!7123=") then @pep_start_date := encounter_datetime
                                when @prev_id = @cur_id then @pep_start_date
                                else @pep_start_date := null
							end as pep_start_date

						from flat_pep_summary_0 t1
							join amrs.person p using (person_id)
						);

						replace into flat_pep_summary
						(select
							person_id,
							t1.uuid,
							t1.visit_id,
						    encounter_id,
							encounter_datetime,
							encounter_type,
							location_id,
							t2.uuid as location_uuid,
							enrollment_date,
							prev_rtc_date,
							cur_rtc_date,
							hiv_exposed_occupational,
							pep_start_date
							from flat_pep_summary_1 t1
								join amrs.location t2 using (location_id));

				 end while;

				 select @end := now();
				 insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
				 select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");

		END $$
	DELIMITER ;

call generate_pep_summary();
