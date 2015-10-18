#********************************************************************************************************
#* CREATION OF MOH 731 INDICATORS TABLE ****************************************************************************
#********************************************************************************************************
#
# Need to first create this temporary table to sort the data by person,encounterdateime. 
# This allows us to use the previous row's data when making calculations.
# It seems that if you don't create the temporary table first, the sort is applied 
# to the final result. Any references to the previous row will not an ordered row. 

#drop table if exists derived_encounter;
#delete from flat_log where table_name="derived_encounter";
create table if not exists derived_encounter(
	person_id int,
    encounter_id int,
	uuid varchar(50),
	prev_encounter_datetime datetime,
	next_encounter_datetime datetime,
	prev_clinic_datetime datetime,
	next_clinic_datetime datetime,
	prev_encounter_type int,
	next_encounter_type int,
    primary key encounter_id (encounter_id),
    index person_next_enc (person_id,next_encounter_datetime),
	index person_uuid (uuid)
);


select @start := now();
select @last_date_created := (select max(max_date_created) from flat_obs);

select @last_update := (select max(date_updated) from flat_log where table_name="derived_encounter");

# then use the max_date_created from amrs.encounter. This takes about 10 seconds and is better to avoid.
select @last_update :=
	if(@last_update is null, 
		(select max(date_created) from amrs.encounter e join derived_encounter using (encounter_id)),
		@last_update);

#otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
select @last_update := if(@last_update,@last_update,'1900-01-01');
#select @last_update := "2015-05-14";


drop table if exists new_data_person_ids;
create temporary table new_data_person_ids(person_id int, primary key (person_id))
(select distinct person_id 
	from flat_obs
	where max_date_created > @last_update
);


drop table if exists derived_encounter_0;
create temporary table derived_encounter_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
(select *
	from amrs.encounter e
		join new_data_person_ids t0 on e.patient_id = t0.person_id
	where encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,32,33,43,47,21)
		and voided=0
	order by t0.person_id, e.encounter_datetime
);

drop table if exists derived_encounter_0;
create temporary table derived_encounter_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
(select *
	from amrs.encounter t1
		join new_data_person_ids t2 on t1.patient_id = t2.person_id
	where voided = 0
	order by t1.patient_id, encounter_datetime
);



select @prev_id := null;
select @cur_id := null;
select @prev_encounter_datetime := null;
select @cur_encounter_datetime := null;
select @prev_clinic_datetime := null;
select @cur_clinic_datetime := null;

select @next_encounter_type := null;
select @cur_encounter_type := null;

drop table if exists derived_encounter_1;
create temporary table derived_encounter_1(
	next_encounter_type int,
	next_encounter_datetime datetime,
	index encounter_id (encounter_id), 
	index person_enc (person_id,encounter_datetime))
(select
	*,
	@prev_id := @cur_id as prev_id,
	@cur_id := person_id as cur_id,

	case
		when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
		else @prev_encounter_datetime := null
	end as next_encounter_datetime,

	@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

	case
		when @prev_id = @cur_id then @prev_clinic_datetime := @cur_clinic_datetime
		else @prev_clinic_datetime := null
	end as next_clinic_datetime,

	case
		when encounter_type != 21 then @cur_clinic_datetime := encounter_datetime 
		else @cur_clinic_datetime := null
	end as cur_clinic_datetime,

	case
		when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
		else @next_encounter_type := null
	end as next_encounter_type,

	@cur_encounter_type := encounter_type as cur_encounter_type

	from derived_encounter_0
	order by person_id, encounter_datetime desc
);

alter table derived_encounter_1 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinic_datetime;

select @prev_id := null;
select @cur_id := null;
select @prev_encounter_type := null;
select @cur_encounter_type := null;
select @prev_encounter_datetime := null;
select @cur_encounter_datetime := null;
select @prev_clinic_datetime := null;
select @cur_clinic_datetime := null;

drop temporary table if exists derived_encounter_2;
create temporary table derived_encounter_2 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
(select 
	*,
	@prev_id := @cur_id as prev_id, 
	@cur_id := t1.person_id as cur_id,

	case
        when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
        else @prev_encounter_type:=null
	end as prev_encounter_type,

	@cur_encounter_type := encounter_type as cur_encounter_type,

	case
        when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
        else @prev_encounter_datetime := null
	end as prev_encounter_datetime,
	@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

	case
		when @prev_id = @cur_id then @prev_clinic_datetime := @cur_clinic_datetime
		else @prev_clinic_datetime := null
	end as prev_clinic_datetime,

	case
		when encounter_type != 21 then @cur_clinic_datetime := encounter_datetime 
		else @cur_clinic_datetime := null
	end as cur_clinic_datetime


	from derived_encounter_1 t1
	order by person_id, encounter_datetime
);		
	


delete t1
from derived_encounter t1
join new_data_person_ids t2 using (person_id);

replace into derived_encounter
(select 
	person_id,
    encounter_id,
	t1.uuid,
	prev_encounter_datetime,
	next_encounter_datetime,
	prev_clinic_datetime,
	next_clinic_datetime,
	prev_encounter_type,
	next_encounter_type
from derived_encounter_2 t1
	join amrs.person t2 using (person_id)
order by person_id, encounter_datetime);

insert into flat_log values (@last_date_created,"derived_encounter");

select concat("Time to complete: ",timestampdiff(minute, @start, now())," minutes");

/*
select person_id, prev_encounter_datetime, encounter_datetime, next_encounter_datetime, prev_encounter_type, encounter_type, next_encounter_type 
	from derived_encounter 
	join amrs.encounter using (encounter_id)
	order by person_id, encounter_datetime;
*/
