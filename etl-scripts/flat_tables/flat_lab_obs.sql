# This is the ETL table for flat_lab_obs
# obs concept_ids: 

# lab concept_ids : 
# 856 = HIV VIRAL LOAD, QUANTITATIVE
# 5497 = CD4, BY FACS
# 730 = CD4%, BY FACS
# 21 = HEMOGLOBIN
# 653 = AST
# 790 = SERUM CREATININE
# 12 = X-RAY, CHEST, PRELIMINARY FINDINGS
# 1030 = HIV DNA PCR
# 1040 = HIV Rapid Test
# 1271 = TESTS ORDERED


# 1. Replace flat_lab_obs with flat_lab_obs_name
# 2. Replace concept_id in () with concept_id in (obs concept_ids)
# 3. Add column definitions 
# 4. Add obs_set column definitions

set session group_concat_max_len=100000;
select @start := now();
select @last_date_created_enc := (select max(date_created) from amrs.encounter);
select @last_date_created_obs := (select max(date_created) from amrs.obs);
select @last_date_created := if(@last_date_created_enc > @last_date_created_obs,@last_date_created_enc,@last_date_created_obs);


select @boundary := "!!";

#delete from flat_log where table_name="flat_lab_obs";
# delete from flat_lab_obs;
#drop table if exists flat_lab_obs;
create table if not exists flat_lab_obs
(person_id int,
encounter_id int,
test_datetime datetime,
encounter_type int,
location_id int,
obs text,
max_date_created datetime,
encounter_ids text,
obs_ids text,
index encounter_id (encounter_id),
index person_date (person_id, test_datetime),
index person_enc_id (person_id,encounter_id),
index date_created (max_date_created),
primary key (encounter_id)
);


select @last_date_created_enc := (select max(date_created) from amrs.encounter);
select @last_date_created_obs := (select max(date_created) from amrs.obs);
select @last_date_created := if(@last_date_created_enc > @last_date_created_obs,@last_date_created_enc,@last_date_created_obs);


# this breaks when replication is down
select @last_update := (select max(date_updated) from flat_log where table_name="flat_lab_obs");

# then use the max_date_created from amrs.encounter. This takes about 10 seconds and is better to avoid.
select @last_update :=
	if(@last_update is null, 
		(select max(date_created) from amrs.encounter e join flat_lab_obs using (encounter_id)),
		@last_update);

#otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
select @last_update := if(@last_update,@last_update,'1900-01-01');

#select @last_update := "2016-02-16";

drop table if exists voided_obs;
create table voided_obs (index person_datetime (person_id, obs_datetime))
(select distinct person_id, obs_datetime
	from amrs.obs 
	where voided=1 
		and date_voided > @last_update 
		and date_created <= @last_update
		and concept_id in (856, 5497, 730,21,653,790,12,1030,1040,1271)
);

# delete any rows that have a voided obs
delete t1
from flat_lab_obs t1
join voided_obs t2 on t1.test_datetime = t2.obs_datetime and t1.person_id=t2.person_id;

# Add back obs sets with voided obs removed
replace into flat_lab_obs
(select
	o.person_id,
	min(o.obs_id) + 100000000 as encounter_id, 
	date(o.obs_datetime),
	99999 as encounter_type,
	null as location_id,
	group_concat( distinct
		case 
			when value_coded is not null then concat(@boundary,o.concept_id,'=',value_coded,@boundary)
			when value_numeric is not null then concat(@boundary,o.concept_id,'=',value_numeric,@boundary)
			when value_datetime is not null then concat(@boundary,o.concept_id,'=',date(value_datetime),@boundary)
			when value_text is not null then concat(@boundary,o.concept_id,'=',value_text,@boundary)
			when value_drug is not null then concat(@boundary,o.concept_id,'=',value_drug,@boundary)
			when value_modifier is not null then concat(@boundary,o.concept_id,'=',value_modifier,@boundary)
		end
		order by o.concept_id,value_coded
		separator ' ## '
	) as obs,
	max(o.date_created) as max_date_created,
	group_concat(concat(@boundary,o.concept_id,'=',o.value_coded,"=",if(encounter_id is not null,encounter_id,""),@boundary)) as encounter_ids,
	group_concat(concat(@boundary,o.concept_id,'=',o.obs_id,@boundary)) as obs_ids


	from voided_obs v
		join amrs.obs o using (person_id, obs_datetime)
	where 
		o.concept_id in (856,5497, 730,21,653,790,12,1030,1040,1271)
		and if(concept_id=1271 and value_coded=1107,false,true)
		and voided=0
	group by person_id, o.obs_datetime
);
#select * from flat_lab_obs;

# Insert newly created obs 
replace into flat_lab_obs
(select 
	o.person_id,	
	min(o.obs_id) + 100000000 as encounter_id, 		
	date(o.obs_datetime),
	99999 as encounter_type,
	null as location_id,
	group_concat(
		case 
			when value_coded is not null then concat(@boundary,o.concept_id,'=',value_coded,@boundary)
			when value_numeric is not null then concat(@boundary,o.concept_id,'=',value_numeric,@boundary)
			when value_datetime is not null then concat(@boundary,o.concept_id,'=',date(value_datetime),@boundary)
			when value_text is not null then concat(@boundary,o.concept_id,'=',value_text,@boundary)
			when value_drug is not null then concat(@boundary,o.concept_id,'=',value_drug,@boundary)
			when value_modifier is not null then concat(@boundary,o.concept_id,'=',value_modifier,@boundary)
		end
		order by concept_id,value_coded
		separator ' ## '
	) as obs,
	max(o.date_created) as max_date_created,
	group_concat(concat(@boundary,o.concept_id,'=',o.value_coded,"=",if(encounter_id is not null,encounter_id,""),@boundary)) as encounter_ids,
	group_concat(concat(@boundary,o.concept_id,'=',o.obs_id,@boundary)) as obs_ids

	from amrs.obs o use index (date_created)
	where voided=0
		and o.date_created > @last_update
		and concept_id in (856,5497,730,21,653,790,12,1030,1040,1271)
		and if(concept_id=1271 and value_coded=1107,false,true) # DO NOT INCLUDE ROWS WHERE ORDERS=NONE
	group by person_id, o.obs_datetime
);
#select * from flat_lab_obs


# Remove test patients
delete t1 
from flat_lab_obs t1 
join amrs.person_attribute t2 using (person_id) 
where t2.person_attribute_type_id=28 and value='true';


drop table voided_obs;
insert into flat_log values (@last_date_created,"flat_lab_obs");
select concat("Time to complete: ",timestampdiff(minute, @start, now())," minutes") as "Time to complete";
