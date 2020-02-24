# This is the ETL table for flat_obs
# obs concept_ids:

# encounter types: 1,2,3,4,5,6,7,8,9,10,13,14,15,17,19,22,23,26,43,47,21
# 1. Replace flat_obs with flat_obs_name
# 2. Replace concept_id in () with concept_id in (obs concept_ids)
# 3. Add column definitions
# 4. Add obs_set column definitions

# v1.1 Notes:
#      Added visit_id. This makes it easier to query for visits related indicators
# v1.3 Notes:
#   1. Added updated encounter tracking when updating flat_obs
#   2. Removed voided patients data from flat_obs


select @table_version := "flat_obs_v1.3";
select @start := now();

set session group_concat_max_len=100000;
select @last_date_created_enc := (select max(date_created) from amrs.encounter);
select @last_date_created_obs := (select max(date_created) from amrs.obs);
select @last_date_created := if(@last_date_created_enc > @last_date_created_obs,@last_date_created_enc,@last_date_created_obs);
select @fake_visit_id := 10000000;


select @boundary := "!!";

#delete from flat_log where table_name="flat_obs";
#drop table if exists flat_obs;
create table if not exists flat_obs
(person_id int,
visit_id int,
encounter_id int,
encounter_datetime datetime,
encounter_type int,
location_id int,
obs text,
obs_datetimes text,
max_date_created datetime,
index encounter_id (encounter_id),
index person_date (person_id, encounter_datetime),
index person_enc_id (person_id,encounter_id),
index date_created (max_date_created),
primary key (encounter_id)
);


select @last_date_created_enc := (select max(date_created) from amrs.encounter);
select @last_date_created_obs := (select max(date_created) from amrs.obs);
select @last_date_created := if(@last_date_created_enc > @last_date_created_obs,@last_date_created_enc,@last_date_created_obs);
select @fake_visit_id := 10000000;


# this breaks when replication is down
select @last_update := (select max(date_updated) from flat_log where table_name=@table_version);

# then use the max_date_created from amrs.encounter. This takes about 10 seconds and is better to avoid.
select @last_update :=
	if(@last_update is null,
		(select max(date_created) from amrs.encounter e join flat_obs using (encounter_id)),
		@last_update);

#otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
select @last_update := if(@last_update,@last_update,'1900-01-01');

#select @last_update := "2016-07-06";

drop table if exists voided_obs_by_flat_obs;
create table voided_obs_by_flat_obs (index encounter_id (encounter_id), index obs_id (obs_id), index person_datetime (person_id, obs_datetime))
(select person_id, encounter_id, obs_id, obs_datetime, date_voided, concept_id, date_created
from amrs.obs where voided=1 and date_voided > @last_update and date_created <= @last_update);

# find all encounters that were changed after @last_update
drop table if exists encounters_with_updated_data;
create temporary table encounters_with_updated_data
(select
	distinct t1.encounter_id
from amrs.encounter t1
join flat_obs t2 using(encounter_id)
	where t1.voided=0 and t1.date_changed > @last_update
);

# remove test patients
delete t1
from voided_obs_by_flat_obs t1
join amrs.person_attribute t2 using (person_id)
where t2.person_attribute_type_id=28 and value='true';


# delete any rows that have voided obs with encounter_id
delete t1
from flat_obs t1
join voided_obs_by_flat_obs t2 using (encounter_id);

# delete any rows whose encounter changed after last update
delete t1
from flat_obs t1
join encounters_with_updated_data using (encounter_id);


# delete any rows that have a voided obs with no encounter_id
delete t1
from flat_obs t1
join voided_obs_by_flat_obs t2 on t1.encounter_datetime = t2.obs_datetime and t1.person_id=t2.person_id
where t2.encounter_id is null;

replace into flat_obs
(select
	o.person_id,
	e.visit_id,
	o.encounter_id,
	e.encounter_datetime,
	e.encounter_type,
	e.location_id,
	group_concat(
		case
			when value_coded is not null then concat(@boundary,o.concept_id,'=',value_coded,@boundary)
			when value_numeric is not null then concat(@boundary,o.concept_id,'=',value_numeric,@boundary)
			when value_datetime is not null then concat(@boundary,o.concept_id,'=',date(value_datetime),@boundary)
			-- when value_boolean is not null then concat(@boundary,o.concept_id,'=',value_boolean,@boundary)
			when value_text is not null then concat(@boundary,o.concept_id,'=',value_text,@boundary)
			when value_drug is not null then concat(@boundary,o.concept_id,'=',value_drug,@boundary)
			when value_modifier is not null then concat(@boundary,o.concept_id,'=',value_modifier,@boundary)
		end
		order by o.concept_id,value_coded
		separator ' ## '
	) as obs,

	group_concat(
		case
			when value_coded is not null or value_numeric is not null or value_datetime is not null or  value_text is not null or value_drug is not null or value_modifier is not null
			then concat(@boundary,o.concept_id,'=',date(o.obs_datetime),@boundary)
		end
		order by o.concept_id,value_coded
		separator ' ## '
	) as obs_datetimes,
	max(o.date_created) as max_date_created

	from voided_obs_by_flat_obs v
		join amrs.obs o using (encounter_id)
		join amrs.encounter e using (encounter_id)
	where
		o.encounter_id > 1 and o.voided=0
	group by encounter_id
);


# Add back obs sets without encounter_ids with voided obs removed
replace into flat_obs
(select
	o.person_id,
	@fake_visit_id :=@fake_visit_id + 1,
	min(o.obs_id) + 100000000 as encounter_id,
	o.obs_datetime,
	99999 as encounter_type,
	null as location_id,
	group_concat(
		case
			when value_coded is not null then concat(@boundary,o.concept_id,'=',value_coded,@boundary)
			when value_numeric is not null then concat(@boundary,o.concept_id,'=',value_numeric,@boundary)
			when value_datetime is not null then concat(@boundary,o.concept_id,'=',date(value_datetime),@boundary)
			-- when value_boolean is not null then concat(@boundary,o.concept_id,'=',value_boolean,@boundary)
			when value_text is not null then concat(@boundary,o.concept_id,'=',value_text,@boundary)
			when value_drug is not null then concat(@boundary,o.concept_id,'=',value_drug,@boundary)
			when value_modifier is not null then concat(@boundary,o.concept_id,'=',value_modifier,@boundary)
		end
		order by o.concept_id,value_coded
		separator ' ## '
	) as obs,

	group_concat(
		case
			when value_coded is not null or value_numeric is not null or value_datetime is not null  or value_text is not null or value_drug is not null or value_modifier is not null
			then concat(@boundary,o.concept_id,'=',date(o.obs_datetime),@boundary)
		end
		order by o.concept_id,value_coded
		separator ' ## '
	) as obs_datetimes,
	max(o.date_created) as max_date_created

	from voided_obs_by_flat_obs v
		join amrs.obs o using (person_id, obs_datetime)
	where
		o.encounter_id is null and voided=0
	group by person_id, o.obs_datetime
);



# find all encounters which have new obs after @last_update
drop table if exists encounters_with_new_obs;
create temporary table encounters_with_new_obs
(select
	distinct encounter_id
from amrs.obs o
	where o.encounter_id > 0
		and o.voided=0
        and o.date_created > @last_update
);

# Insert newly created obs with encounter_ids
replace into flat_obs
(select
	o.person_id,
	case
     	when e.visit_id is not null then e.visit_id else @fake_visit_id :=@fake_visit_id + 1
    end as visit_id,
	o.encounter_id,
	encounter_datetime,
	encounter_type,
	e.location_id,
	group_concat(
		case
			when value_coded is not null then concat(@boundary,o.concept_id,'=',value_coded,@boundary)
			when value_numeric is not null then concat(@boundary,o.concept_id,'=',value_numeric,@boundary)
			when value_datetime is not null then concat(@boundary,o.concept_id,'=',date(value_datetime),@boundary)
			-- when value_boolean is not null then concat(@boundary,o.concept_id,'=',value_boolean,@boundary)
			when value_text is not null then concat(@boundary,o.concept_id,'=',value_text,@boundary)
			when value_drug is not null then concat(@boundary,o.concept_id,'=',value_drug,@boundary)
			when value_modifier is not null then concat(@boundary,o.concept_id,'=',value_modifier,@boundary)
		end
		order by concept_id,value_coded
		separator ' ## '
	) as obs,

	group_concat(
		case
			when value_coded is not null or value_numeric is not null or value_datetime is not null or  value_text is not null or value_drug is not null or value_modifier is not null
			then concat(@boundary,o.concept_id,'=',date(o.obs_datetime),@boundary)
		end
		order by o.concept_id,value_coded
		separator ' ## '
	) as obs_datetimes,
	max(o.date_created) as max_date_created

	from amrs.obs o
		join encounters_with_new_obs e1 using (encounter_id)
        join amrs.encounter e using (encounter_id)
	where o.voided=0
	group by o.encounter_id
);

# Insert newly creatred obs without encounter_ids
replace into flat_obs
(select
	o.person_id,
	@fake_visit_id :=@fake_visit_id + 1 as visit_id,
	min(o.obs_id) + 100000000 as encounter_id,
	o.obs_datetime,
	99999 as encounter_type,
	null as location_id,
	group_concat(
		case
			when value_coded is not null then concat(@boundary,o.concept_id,'=',value_coded,@boundary)
			when value_numeric is not null then concat(@boundary,o.concept_id,'=',value_numeric,@boundary)
			when value_datetime is not null then concat(@boundary,o.concept_id,'=',date(value_datetime),@boundary)
			-- when value_boolean is not null then concat(@boundary,o.concept_id,'=',value_boolean,@boundary)
			when value_text is not null then concat(@boundary,o.concept_id,'=',value_text,@boundary)
			when value_drug is not null then concat(@boundary,o.concept_id,'=',value_drug,@boundary)
			when value_modifier is not null then concat(@boundary,o.concept_id,'=',value_modifier,@boundary)
		end
		order by concept_id,value_coded
		separator ' ## '
	) as obs,

	group_concat(
		case
			when value_coded is not null or value_numeric is not null or value_datetime is not null or value_text is not null or value_drug is not null or value_modifier is not null
			then concat(@boundary,o.concept_id,'=',date(o.obs_datetime),@boundary)
		end
		order by o.concept_id,value_coded
		separator ' ## '
	) as obs_datetimes,
	max(o.date_created) as max_date_created

	from amrs.obs o use index (date_created)
	where
		o.encounter_id is null
		and voided=0 and o.date_created > @last_update
	group by person_id, o.obs_datetime
);


# Insert obs whose  encounters changed after last flat_obs update
replace into flat_obs
(select
	o.person_id,
	case
     	when e.visit_id is not null then e.visit_id else @fake_visit_id :=@fake_visit_id + 1
    end as visit_id,
	o.encounter_id,
	encounter_datetime,
	encounter_type,
	e.location_id,
	group_concat(
		case
			when value_coded is not null then concat(@boundary,o.concept_id,'=',value_coded,@boundary)
			when value_numeric is not null then concat(@boundary,o.concept_id,'=',value_numeric,@boundary)
			when value_datetime is not null then concat(@boundary,o.concept_id,'=',date(value_datetime),@boundary)
			-- when value_boolean is not null then concat(@boundary,o.concept_id,'=',value_boolean,@boundary)
			when value_text is not null then concat(@boundary,o.concept_id,'=',value_text,@boundary)
			when value_drug is not null then concat(@boundary,o.concept_id,'=',value_drug,@boundary)
			when value_modifier is not null then concat(@boundary,o.concept_id,'=',value_modifier,@boundary)
		end
		order by concept_id,value_coded
		separator ' ## '
	) as obs,

	group_concat(
		case
			when value_coded is not null or value_numeric is not null or value_datetime is not null or  value_text is not null or value_drug is not null or value_modifier is not null
			then concat(@boundary,o.concept_id,'=',date(o.obs_datetime),@boundary)
		end
		order by o.concept_id,value_coded
		separator ' ## '
	) as obs_datetimes,
	max(o.date_created) as max_date_created

	from amrs.obs o
		join encounters_with_updated_data e1 using (encounter_id)
        join amrs.encounter e using (encounter_id)
	where o.voided=0
	group by o.encounter_id
);


# remove voided patients
delete t1
from flat_obs t1
join amrs.person t2 using (person_id)
where t2.voided=1;

drop table voided_obs_by_flat_obs;

select @end := now();
insert into flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");
