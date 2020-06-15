use etl;
DELIMITER $$
CREATE PROCEDURE `generate_flat_covid_screening_v1_0`()
BEGIN

select @start:= now();
select @table_version := "flat_covid_screening_v1.0";

set session sort_buffer_size=512000000;

select @sep := " ## ";

#delete from flat_log where table_name="flat_vitals";
drop table if exists flat_covid_screening;
create table if not exists flat_covid_screening (
	person_id int,
	uuid varchar(100),
    gender  varchar(50) ,
    birthdate DATETIME,
    encounter_id int,
	encounter_datetime datetime,
    encounter_type INT,
	location_id int,
    location_name varchar(100),
	fever int,
    cough int,
    difficulty_breathing int,
    travelled_to_covid_location_last_14_days int,
    close_contact_covid_case_last_14_days int,
    close_contact_risky_individual_last_30_days int,
    been_in_hospital_with_covid_cases_last_14_days int,
    screened_positive_covid int,
    last_vl_result int,
	last_vl_date datetime,
	next_hiv_appointment_date datetime,
	last_hiv_appointment_date datetime,
    date_created datetime,
    primary key encounter_id (encounter_id),
    index person_date (person_id, encounter_datetime),
	index person_uuid (uuid)
);


select @start := now();
select @last_date_created := (select max(max_date_created) from flat_obs);


select @last_update := (select max(date_updated) from flat_log where table_name=@table_version);

# then use the max_date_created from amrs.encounter. This takes about 10 seconds and is better to avoid.
select @last_update :=
	if(@last_update is null,
		(select max(e.date_created) from amrs.encounter e join flat_covid_screening using (encounter_id)),
		@last_update);

#otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
select @last_update := if(@last_update,@last_update,'1900-01-01');
#select @last_update := '1900-01-01';

drop temporary table if exists new_data_person_ids;
create temporary table new_data_person_ids(person_id int, primary key (person_id))
(select distinct patient_id as person_id
	from amrs.encounter
	where encounter_type in (208) and (date_created > @last_update or date_changed > @last_update or date_voided > @last_update) 
);


drop temporary table if exists flat_covid_0;
create temporary table flat_covid_0(encounter_id int, primary key (encounter_id), index person_enc_date (person_id,encounter_datetime))
(select
	t1.person_id,
	t1.encounter_id,
	t1.encounter_datetime,
	t1.encounter_type,
	t1.location_id,
	t1.obs,
	t1.obs_datetimes,
    l.name as location_name
	from flat_obs t1
		join new_data_person_ids t0 using (person_id)
        join amrs.location l using (location_id)
	where encounter_type in (208)
	order by person_id, encounter_datetime
);

drop temporary table if exists latest_hiv_summary;
create temporary table latest_hiv_summary(encounter_id int, primary key (encounter_id), index i_person_id (person_id,encounter_datetime))
(
SELECT 
   encounter_id,
            encounter_datetime,
            person_id,
            vl_1,
            vl_1_date,
            rtc_date,
            prev_rtc_date
FROM
    (SELECT 
        encounter_id,
            encounter_datetime,
            location_id,
            h.person_id,
            vl_1,
            vl_1_date,
            rtc_date,
            prev_rtc_date
    FROM
        etl.flat_hiv_summary_v15b h
    inner join new_data_person_ids t0 using (person_id) 
    ORDER BY encounter_datetime DESC) p
GROUP BY person_id
);


drop temporary table if exists flat_covid_1;
create temporary table flat_covid_1 (index encounter_id (encounter_id))
(select
	t1.person_id,
	p.uuid,
    p.gender,
    p.birthdate,
	t1.encounter_id,
	t1.encounter_datetime,
    t1.encounter_type,
	location_id,
    location_name,
	case
		when obs regexp "!!5945=1065!!" then 1
        when obs regexp "!!5945=1066!!" then 0
	    else null
	 end as fever,
     case
		when obs regexp "!!107=1065!!" then 1
        when obs regexp "!!107=1066!!" then 0
	    else null
	 end as cough,
     case
		when obs regexp "!!2296=1065!!" then 1
        when obs regexp "!!2296=1066!!" then 0
	    else null
	 end as difficulty_breathing,
     case
		when obs regexp "!!11090=1065!!" then 1
        when obs regexp "!!11090=1066!!" then 0
	    else null
	 end as travelled_to_covid_location_last_14_days,
     case
		when obs regexp "!!11091=1065!!" then 1
        when obs regexp "!!11091=1066!!" then 0
	    else null
	 end as close_contact_covid_case_last_14_days,
      case
		when obs regexp "!!11092=1065!!" then 1
        when obs regexp "!!11092=1066!!" then 0
	    else null
	 end as close_contact_risky_individual_last_30_days, 
     case
		when obs regexp "!!11093=1065!!" then 1
        when obs regexp "!!11093=1066!!" then 0
	    else null
	 end as been_in_hospital_with_covid_cases_last_14_days,
	 case
		when obs regexp "!!11140=703!!" then 1
        when obs regexp "!!11140=664!!" then 0
	    else null
	 end as screened_positive_covid,
      vl_1 as last_vl_result,
	  vl_1_date as last_vl_date,
	  rtc_date as next_hiv_appointment_date,
	  prev_rtc_date as last_hiv_appointment_date
from flat_covid_0 t1
	join amrs.person p using (person_id)
    join latest_hiv_summary h using (person_id)
);



delete t1
from flat_covid_screening t1
join new_data_person_ids t2 using (person_id);

replace into flat_covid_screening
(select
	person_id,
	uuid,
	gender ,
    birthdate,
    encounter_id,
	encounter_datetime,
    encounter_type,
	location_id,
    location_name,
	fever,
    cough,
    difficulty_breathing,
    travelled_to_covid_location_last_14_days,
    close_contact_covid_case_last_14_days,
    close_contact_risky_individual_last_30_days,
    been_in_hospital_with_covid_cases_last_14_days,
    screened_positive_covid,
     last_vl_result,
	last_vl_date,
	next_hiv_appointment_date,
	last_hiv_appointment_date,
    now() as date_created
from flat_covid_1);

select @end := now();
insert into flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");

END$$
DELIMITER ;
