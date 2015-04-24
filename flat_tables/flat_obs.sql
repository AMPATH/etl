# This is the ETL table for flat_obs
# obs concept_ids: 

# encounter types: 1,2,3,4,5,6,7,8,9,10,13,14,15,17,19,22,23,26,43,47,21
# 1. Replace flat_obs with flat_obs_name
# 2. Replace concept_id in () with concept_id in (obs concept_ids)
# 3. Add column definitions 
# 4. Add obs_set column definitions

set session group_concat_max_len=100000;
select @now := now();


#delete from flat_log where table_name="flat_obs";
#drop table if exists flat_obs;
create table if not exists flat_obs
(person_id int,
encounter_id int,
encounter_datetime datetime,
obs text,
obs_datetimes text,
index encounter_id (encounter_id),
index person_date (person_id, encounter_datetime),
primary key (encounter_id)
);


select @last_update := (select max(date_updated) from flat_log where table_name="flat_obs");

# then use the max_date_created from amrs.encounter. This takes about 10 seconds and is better to avoid.
select @last_update :=
	if(@last_update is null, 
		(select max(date_created) from amrs.encounter e join flat_obs using (encounter_id)),
		@last_update);

#otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
select @last_update := if(@last_update,@last_update,'1900-01-01');

#select @last_update := "2015-01-01";

drop table if exists voided_obs;
create table voided_obs (index encounter_id (encounter_id), index obs_id (obs_id), index person_datetime (person_id, obs_datetime))
(select person_id, encounter_id, obs_id, obs_datetime, date_voided, concept_id, date_created
from amrs.obs where voided=1 and date_voided > @last_update and date_created <= @last_update);


# remove test patients
delete t1 
from voided_obs t1 
join amrs.person_attribute t2 using (person_id) 
where t2.person_attribute_type_id=28 and value='true';

drop table if exists new_data;
create temporary table if not exists new_data
(person_id int,
encounter_id int,
encounter_datetime datetime,
obs text,
obs_datetimes text,
index encounter_id (encounter_id),
index person_date (person_id, encounter_datetime),
primary key (encounter_id)
);

# add back encounters with voided obs removed
insert ignore into new_data
(select
	o.person_id,
	o.encounter_id, 
	e.encounter_datetime,
	group_concat(
		case 
			when value_coded then concat(o.concept_id,'=',value_coded)
			when value_numeric then concat(o.concept_id,'=',value_numeric)
			when value_datetime then concat(o.concept_id,'=',value_datetime)
			when value_boolean then concat(o.concept_id,'=',value_boolean)
			when value_text then concat(o.concept_id,'=',value_text)
			when value_drug then concat(o.concept_id,'=',value_drug)
			when value_modifier then concat(o.concept_id,'=',value_modifier)
		end
		order by o.concept_id,value_coded
		separator ' ## '
	) as obs,

	group_concat(
		case 
			when value_coded or value_numeric or value_datetime or value_boolean or value_text or value_drug or value_modifier
			then concat(o.concept_id,'=',o.obs_datetime)
		end
		order by o.concept_id,value_coded
		separator ' ## '
	) as obs_datetimes

	from voided_obs v
		join amrs.obs o using (encounter_id)
		join amrs.encounter e using (encounter_id)
	where 
		o.encounter_id > 1 and o.voided=0
	group by encounter_id
);


# Add back obs sets without encounter_ids with voided obs removed
insert ignore into new_data
(select
	o.person_id,
	min(o.obs_id) + 100000000 as encounter_id, 
	o.obs_datetime,
	group_concat(
		case 
			when value_coded then concat(o.concept_id,'=',value_coded)
			when value_numeric then concat(o.concept_id,'=',value_numeric)
			when value_datetime then concat(o.concept_id,'=',value_datetime)
			when value_boolean then concat(o.concept_id,'=',value_boolean)
			when value_text then concat(o.concept_id,'=',value_text)
			when value_drug then concat(o.concept_id,'=',value_drug)
			when value_modifier then concat(o.concept_id,'=',value_modifier)
		end
		order by o.concept_id,value_coded
		separator ' ## '
	) as obs,

	group_concat(
		case 
			when value_coded or value_numeric or value_datetime or value_boolean or value_text or value_drug or value_modifier
			then concat(o.concept_id,'=',o.obs_datetime)
		end
		order by o.concept_id,value_coded
		separator ' ## '
	) as obs_datetimes

	from voided_obs v
		join amrs.obs o using (person_id, obs_datetime)
	where 
		o.encounter_id is null and voided=0
	group by person_id, o.obs_datetime
);

# Insert newly creatred obs with encounter_ids
insert ignore into new_data
(select 
	o.person_id,
	o.encounter_id, 
	encounter_datetime,
	group_concat(
		case 
			when value_coded then concat(o.concept_id,'=',value_coded)
			when value_numeric then concat(o.concept_id,'=',value_numeric)
			when value_datetime then concat(o.concept_id,'=',value_datetime)
			when value_boolean then concat(o.concept_id,'=',value_boolean)
			when value_text then concat(o.concept_id,'=',value_text)
			when value_drug then concat(o.concept_id,'=',value_drug)
			when value_modifier then concat(o.concept_id,'=',value_modifier)
		end
		order by concept_id,value_coded
		separator ' ## '
	) as obs,

	group_concat(
		case 
			when value_coded or value_numeric or value_datetime or value_boolean or value_text or value_drug or value_modifier
			then concat(o.concept_id,'=',o.obs_datetime)
		end
		order by o.concept_id,value_coded
		separator ' ## '
	) as obs_datetimes

	from amrs.obs o
		join amrs.encounter e using (encounter_id)
	where o.encounter_id > 0 
		and o.voided=0 and o.date_created > @last_update
	group by o.encounter_id
);

# Insert newly creatred obs without encounter_ids
insert ignore into new_data
(select 
	o.person_id,	
	min(o.obs_id) + 100000000 as encounter_id, 		
	o.obs_datetime,
	group_concat(
		case 
			when value_coded then concat(o.concept_id,'=',value_coded)
			when value_numeric then concat(o.concept_id,'=',value_numeric)
			when value_datetime then concat(o.concept_id,'=',value_datetime)
			when value_boolean then concat(o.concept_id,'=',value_boolean)
			when value_text then concat(o.concept_id,'=',value_text)
			when value_drug then concat(o.concept_id,'=',value_drug)
			when value_modifier then concat(o.concept_id,'=',value_modifier)
		end
		order by concept_id,value_coded
		separator ' ## '
	) as obs,

	group_concat(
		case 
			when value_coded or value_numeric or value_datetime or value_boolean or value_text or value_drug or value_modifier
			then concat(o.concept_id,'=',o.obs_datetime)
		end
		order by o.concept_id,value_coded
		separator ' ## '
	) as obs_datetimes

	from amrs.obs o use index (date_created)
	where 
		o.encounter_id is null 
		and voided=0 and o.date_created > @last_update
	group by person_id, o.obs_datetime
);


# delete any rows that have voided obs with encounter_id
delete t1
from flat_obs t1
join voided_obs t2 using (encounter_id);

# delete any rows that have a voided obs with no encounter_id
delete t1
from flat_obs t1
join voided_obs t2 on t1.encounter_datetime = t2.obs_datetime and t1.person_id=t2.person_id
where t2.encounter_id is null;


replace into flat_obs
(select * from new_data);

## UPDATE data for derived tables

#remove any encounters that have been voided.
# delete from flat_new_person_data;
insert ignore into flat_new_person_data
(select distinct person_id from new_data);

insert ignore into flat_new_person_data
(select patient_id as person_id from amrs.encounter where voided=1 and date_created <= @last_update and date_voided > @last_update);

drop table voided_obs;

insert into flat_log values (@last_update,"flat_obs");

select concat("Time to complete: ",timestampdiff(minute, @now, now())," minutes") as "Time to complete";