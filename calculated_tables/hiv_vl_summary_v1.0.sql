# This script will create a table for viral load summarry. Each row is a vl test results (value and dates). 
# For each row you are able to get the last 4 results (vl_1 being the latest and vl_4 being the oldest)
		select @start := now();
		select @start := now();
		select @table_version := "hiv_vl_summary_v1.0";

		set session sort_buffer_size=512000000;

		select @sep := " ## ";
		select @lab_encounter_type := 99999;
		select @death_encounter_type := 31;
		select @last_date_created := (select max(max_date_created) from etl.flat_lab_obs);

		#drop table if exists flat_hiv_vl_summary;
		#delete from flat_log where table_name="flat_hiv_summary";
		create table if not exists flat_hiv_vl_summary (
			person_id int,
			uuid varchar(100),
			vl_1 int,
			vl_1_date datetime,
			vl_2 int,
			vl_2_date datetime,
			vl_3 int,
			vl_3_date datetime,
			vl_4 int,
			vl_4_date datetime,

            next_vl_datetime datetime,

#			primary key encounter_id (test),
			index person_date (person_id, vl_1_date),
			index person_uuid (uuid),
			index person_next_vl_datetime (person_id, next_vl_datetime)
		);

		select @last_update := (select max(date_updated) from etl.flat_log where table_name=@table_version);

		#otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
		select @last_update := if(@last_update,@last_update,'1900-01-01');
		#select @last_update := "2016-09-12"; #date(now());
		#select @last_date_created := "2015-11-17"; #date(now());

		drop table if exists new_data_person_ids;
		create temporary table new_data_person_ids(person_id int, primary key (person_id));

		replace into new_data_person_ids
		(select distinct person_id #, min(encounter_datetime) as start_date
			from etl.flat_lab_obs
			where max_date_created > @last_update   

		#	group by person_id
		# limit 100									
		);


		#while @person_ids_count > 0 do


		#create temp table with a set of person ids
		#drop table if exists new_data_person_ids_0;


			#create temporary table new_data_person_ids_0 (select * from new_data_person_ids limit 10000); #TODO - change this when data_fetch_size changes
			#delete from new_data_person_ids where person_id in (select person_id from new_data_person_ids_0);
			#select @person_ids_count := (select count(*) from new_data_person_ids);

			drop table if exists flat_hiv_vl_summary_0;
			create temporary table flat_hiv_vl_summary_0(index person_test (person_id,test_datetime))
			(select
				t1.person_id,
				t1.test_datetime,
                t1.obs
#				group_concat(t1.obs) as obs
#				t2.orders
				from etl.flat_lab_obs t1
					join new_data_person_ids t0 using (person_id)

#					left join flat_orders t2 using(encounter_id)
					where obs regexp "!!856="
				group by person_id, date(test_datetime)
				order by t1.person_id, test_datetime
			);

#            select * from flat_hiv_vl_summary_0;

			select @prev_id := null;
			select @cur_id := null;
			select @vl_1:=null;
			select @vl_1_date:=null;
			select @vl_2:=null;
			select @vl_2_date:=null;
			select @vl_3:=null;
			select @vl_3_date:=null;
			select @vl_4:=null;
			select @vl_4_date:=null;


			drop temporary table if exists flat_hiv_vl_summary_1;
			create temporary table flat_hiv_vl_summary_1 (index person_test_date (person_id,test_datetime))
			(select
				@prev_id := @cur_id as prev_id,
				@cur_id := t1.person_id as cur_id,
				t1.person_id,
				p.uuid,
				t1.test_datetime,

				# 856 = HIV VIRAL LOAD, QUANTITATIVE
				case
					when @prev_id=@cur_id then
						case
							when obs regexp "!!856=[0-9]" then @vl_4:= @vl_3
							else @vl_4
						end
					else @vl_4:=null
				end as vl_4,

				case
					when @prev_id=@cur_id then
						case
							when obs regexp "!!856=[0-9]" then @vl_4_date:= @vl_3_date
							else @vl_4_date
						end
					else @vl_4_date:=null
				end as vl_4_date,



				# 856 = HIV VIRAL LOAD, QUANTITATIVE
				case
					when @prev_id=@cur_id then
						case
							when obs regexp "!!856=[0-9]" then @vl_3:= @vl_2
							else @vl_3
						end
					else @vl_3:=null
				end as vl_3,

				case
					when @prev_id=@cur_id then
						case
							when obs regexp "!!856=[0-9]" then @vl_3_date:= @vl_2_date
							else @vl_3_date
						end
					else @vl_3_date:=null
				end as vl_3_date,




				# 856 = HIV VIRAL LOAD, QUANTITATIVE
				case
					when @prev_id=@cur_id then
						case
							when obs regexp "!!856=[0-9]" then @vl_2:= @vl_1
							else @vl_2
						end
					else @vl_2:=null
				end as vl_2,

				case
					when @prev_id=@cur_id then
						case
							when obs regexp "!!856=[0-9]" then @vl_2_date:= @vl_1_date
							else @vl_2_date
						end
					else @vl_2_date:=null
				end as vl_2_date,

				case
					when obs regexp "!!856=[0-9]" then @vl_1:=cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") as unsigned)
					when @prev_id=@cur_id then @vl_1
					else @vl_1:=null
				end as vl_1,

				case
					when obs regexp "!!856=[0-9]" then @vl_1_date:= test_datetime
					when @prev_id=@cur_id then @vl_1_date
					else @vl_1_date:=null
				end as vl_1_date


			from flat_hiv_vl_summary_0 t1
			join amrs.person p using (person_id)
		);

            #select * from flat_hiv_vl_summary_1;



			select @prev_id := null;
			select @cur_id := null;
			select @next_vl_datetime := null;
			select @cur_vl_datetime := null;

			alter table flat_hiv_vl_summary_1 drop prev_id, drop cur_id;

			drop table if exists flat_hiv_vl_summary_2;
			create temporary table flat_hiv_vl_summary_2
			(select *,
				@prev_id := @cur_id as prev_id,
				@cur_id := person_id as cur_id,

				case
					when @prev_id = @cur_id then @next_vl_datetime := @cur_test_datetime
					else @next_vl_datetime := null
				end as next_vl_datetime,

				@cur_test_datetime := test_datetime as cur_test_datetime

				from flat_hiv_vl_summary_1
				order by person_id, date(test_datetime) desc
			);

        #select * from flat_hiv_vl_summary_2 order by person_id, test_datetime;

		replace into flat_hiv_vl_summary
        (select
			person_id,
			uuid,
			vl_1,
			vl_1_date,
			vl_2,
			vl_2_date,
			vl_3,
			vl_3_date,
			vl_4,
			vl_4_date,
            next_vl_datetime
            from flat_hiv_vl_summary_2
		);

        #select * from flat_hiv_vl_summary;

        select @end := now();
	    insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
		select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");

