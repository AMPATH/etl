# HIV encounter_type_ids : (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) 
# HIV pmtct encounter_type_ids : (10,11,12,44,46,47)
# HIV adult visit : (1,2,14,17,19)
# HIV peds visit : (3,4,15,26)
# non-lab encounters : (1,2,3,4,10,11,12,16,17,19,25,27,28,29,32,33,34,37,43,47,49,40,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,73,75,76,78,79,80,83)
# lab encounters : (5,6,7,8,9,45,87)


# required indexes: obs.date_voided, obs.date_created, encounter.date_voided, encounter.date_created

/*
drop table if exists flat_lab_data;
create table flat_lab_data (encounter_datetime datetime, person_id int, vl1 int, vl1_date datetime, vl2 int, vl2_date datetime,cd4_1_date datetime, cd4_1 mediumint, cd4_2 mediumint, cd4_2_date datetime, index encounter_id (encounter_id), index person_enc (person_id, encounter_datetime))
(select * from flat_lab_data_old);
*/

select @last_enc_date_created := max(enc_date_created) from flat_lab_data;
select @last_enc_date_created := if(@last_enc_date_created,@last_enc_date_created,'1900-01-01');

select @last_obs_date_created := max(obs_date_created) from flat_lab_data;
select @last_obs_date_created := if(@last_obs_date_created,@last_obs_date_created,'1900-01-01');

drop table if exists voided_obs;
create table voided_obs (index encounter_id (encounter_id), index obs_id (obs_id))
(select person_id, encounter_id, obs_id, obs_datetime, date_voided, concept_id, date_created
from amrs.obs where voided=1 and date_voided > @last_obs_date_created and date_created <= @last_obs_date_created and concept_id in (1042,1040,856,5497,730) );




/*
build a dataset of all relevant encounter data : 
	(1) new encounters, 
	(2) encounters with voided obs
	(3) encounters with new obs
	(4) encounters with the same datetime as voided obs with no encounter_ids
*/
drop temporary table if exists enc;
create temporary table enc (encounter_id int, encounter_datetime datetime, index encounter_id (encounter_id), index person_id (person_id))

(select encounter_id,encounter_datetime,person_id,encounter_type,enc_date_created
 from
(
# new encounter_data
(select e.encounter_id, e.encounter_datetime, e.provider_id, e.patient_id as person_id, e.location_id,e.encounter_type, e.date_created as enc_date_created
from amrs.encounter e
where e.voided=0
and e.date_created > @last_enc_date_created
)

union
# add to the enc dataset all existing encounters with voided obs. 
(select e.encounter_id, e.encounter_datetime, e.provider_id, e.patient_id as person_id, e.location_id,e.encounter_type, e.date_created as enc_date_created
from amrs.encounter e
join voided_obs v using (encounter_id)
where e.date_created <= @last_enc_date_created and e.voided=0 and v.encounter_id is not null
)

union

# add in encounters which have new obs attached to them
(select e.encounter_id, e.encounter_datetime, e.provider_id, e.patient_id as person_id, e.location_id,e.encounter_type, e.date_created as enc_date_created
from amrs.encounter e
join amrs.obs o using (encounter_id)
where o.date_created > @last_obs_date_created and o.voided=0 and e.date_created <= @last_enc_date_created and e.voided=0 and concept_id in (1042,1040,856,5497,730) 
)
) t3 group by encounter_id);

delete t1 
from enc t1 
join amrs.person_attribute t2 using (person_id) 
where t2.person_attribute_type_id=28 and value='true';


# create a dataset of the new obs. 
drop table if exists obs_subset;
create temporary table obs_subset (index encounter_id (encounter_id))
(select * from
	(
		# add new obs 
		(select * from amrs.obs o
			where concept_id in (1042,1040,856,5497,730) 
				and o.voided=0 and date_created > @last_obs_date_created
		)
		union
		# add obs of encounters with voided obs
		(select t1.* from amrs.obs t1 
			join voided_obs t2 using (encounter_id)
			where t1.voided=0 and t1.date_created <= @last_obs_date_created and t2.encounter_id is not null
		)
		union
		# add obs for encounters which have new obs
		(select o.*
			from amrs.encounter e
			join amrs.obs o using (encounter_id)
			where o.date_created > @last_obs_date_created and o.voided=0 and e.date_created <= @last_enc_date_created and e.voided=0
		)
	) t3 group by obs_id
);

drop temporary table if exists enc_obs;
create temporary table enc_obs (index encounter_id (encounter_id))
(select 
	encounter_id,
	max(if(concept_id in (1042,1040),value_coded,null)) as enc_hiv_test,
	max(if(concept_id=856, value_numeric,null)) as enc_vl,
	max(if(concept_id=5497,value_numeric,null)) as enc_cd4_count, 
	max(if(concept_id=730,value_numeric,null)) as enc_cd4_percent,
	max(date_created) as obs_date_created
	from obs_subset
	where encounter_id is not null
	group by encounter_id 
);


drop temporary table if exists lab_obs;
create temporary table lab_obs (encounter_id integer, index encounter_id (encounter_id))
(select 
	person_id, 
	date(obs_datetime) as encounter_datetime, 
	date_created as obs_lab_date_created,
	if(concept_id in (1042,1040),value_coded,null) as lab_hiv_test,
	if(concept_id=856, value_numeric,null) as lab_vl,
	if(concept_id=5497,value_numeric,null) as lab_cd4_count, 
	if(concept_id=730,value_numeric,null) as lab_cd4_percent,
	(obs_id + 100000000) as encounter_id
from obs_subset
where encounter_id is null 
order by person_id, encounter_datetime
);


# create fake encounters for the lab encounters which don't (BUT SHOULD!!!) have an associated encounter_id
insert into enc
(select encounter_id,
encounter_datetime, 
person_id, 
999999 as encounter_type,
obs_lab_date_created as enc_date_created
from lab_obs
);

# get rid of person_id and encounter_datetime so they don't interfere with join to make denormalized table. they are no longer necesseary now
# that there is an associated encounter_id
alter table lab_obs drop person_id, drop encounter_datetime;

/*
insert into flat_lab_data
(select e1.*,enc_hiv_test,enc_vl,enc_cd4_count,enc_cd4_percent,obs_date_created,obs_lab_date_created,lab_hiv_test,lab_vl,lab_cd4_count,null,null,null,null,null,null,null,null,null,null,null,null,null
from enc e1 
left outer join enc_obs n1 using (encounter_id)
left outer join lab_obs l using (encounter_id)
order by e1.person_id, e1.encounter_datetime
);
*/

# subtract out all data for any person who has new data to be entered. We require all previous data for a given person to make the new derived table. 
# we will then add this back into the final table in the end.

drop table if exists new_person_data;
create temporary table new_person_data(index person_id (person_id))
(select distinct person_id from
(
	(select distinct person_id from enc)
	union
	(select patient_id as person_id from amrs.encounter where date_created <= @last_enc_date_created and date_voided > @last_enc_date_created)
	union
	(select person_id from amrs.obs where date_created <= @last_obs_date_created and date_voided > @last_obs_date_created and concept_id in (1042,1040,856,5497,730))
) t0);

drop table if exists new_dataset;
create table new_dataset (index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
(select encounter_id,encounter_datetime,person_id,encounter_type,enc_date_created,enc_hiv_test,enc_vl,enc_cd4_count,enc_cd4_percent,obs_date_created,obs_lab_date_created,lab_hiv_test,lab_vl,lab_cd4_count,lab_cd4_percent
from flat_lab_data t1 join new_person_data t2 using(person_id));


delete t1
from new_dataset t1
join
(select distinct encounter_id
	from
	(
		(select t1.encounter_id
			from new_dataset t1 
			join amrs.encounter t2 using (encounter_id) 
			where t2.voided=1
		)

		union

		# remove all rows that have a voided obs. we will rebuild this later. 
		
		(select  distinct t1.encounter_id
			from new_dataset t1
			join voided_obs t2 using (encounter_id)
			where t2.encounter_id is not null
		)
			
		union

		# delete any encounters that have new obs. we will add these back in later. 
		(select t1.encounter_id
			from new_dataset t1
			join  
			(select e.encounter_id
				from amrs.encounter e
				join amrs.obs o using (encounter_id)
				where o.date_created > @last_obs_date_created and o.voided=0 and e.date_created <= @last_enc_date_created and concept_id in (1042,1040,856,5497,730) 
			) t2 using (encounter_id)
		)

		union
		# remove those rows associated with voided obs with no encounter_ids. obs with no encounter_ids use the obs_id as a temporary encounter_id
		(select t1.encounter_id
			from new_dataset t1
			join voided_obs t2 on (t1.encounter_id - 100000000) = t2.obs_id
			where t2.encounter_id is null
		)
	)t3 
) t4 using (encounter_id)
;


insert into new_dataset
(select e1.*,enc_hiv_test,enc_vl,enc_cd4_count,enc_cd4_percent,obs_date_created,obs_lab_date_created,lab_hiv_test,lab_vl,lab_cd4_count,lab_cd4_percent
from enc e1 
left outer join enc_obs n1 using (encounter_id)
left outer join lab_obs l using (encounter_id)
order by e1.person_id, e1.encounter_datetime
);

drop table voided_obs;


# for those encounters with the same datetime, we need to make sure the encounters we are removing are ordered ahead of the encounters we are keeping
# we will set the encounter_id to a high number for lab encounters and then sort by encounter_id desc.
# update new_dataset set encounter_id = encounter_id + 10000000 where encounter_type in (5,6,7,8,9,45,87) and encounter_id < 10000000;

drop table if exists flat_lab_data_2;
create temporary table flat_lab_data_2
(select *,
if(lab_hiv_test,lab_hiv_test,if(enc_hiv_test,enc_hiv_test,null)) as hiv_test,
if(lab_cd4_count >= 0,lab_cd4_count,if(enc_cd4_count >= 0,enc_cd4_count,null)) as cd4_count,
if(lab_vl >= 0,lab_vl,if(enc_vl >= 0,enc_vl,null)) as vl,
if(lab_cd4_percent >= 0,lab_cd4_percent,if(enc_cd4_percent >= 0,enc_cd4_percent,null)) as cd4_percent,
case 
	when encounter_type in (5,6,7,8,9,45,87,999999) then 1
	else 2
end as sort_order,
if(encounter_type=21,1,2) as encounter_type_sort_order
from new_dataset
order by person_id, encounter_datetime
);


select @prev_id := null;
select @cur_id := null;
select @vl1:= null;
select @vl2:= null;
select @vl1_date:= null;
select @vl2_date:= null;
select @cd4_1:= null;
select @cd4_2:= null;
select @cd4_1_date:= null;
select @cd4_2_date:= null;


drop table if exists flat_lab_data_3;
create temporary table flat_lab_data_3 (encounter_datetime datetime, person_id int, vl1_date datetime, vl2_date datetime,cd4_1_date datetime, cd4_1 mediumint, cd4_2 mediumint, cd4_2_date datetime,index person_enc (person_id, encounter_datetime))
(select *,
@prev_id := @cur_id as prev_id, 
@cur_id := person_id as cur_id,

if(@prev_id=@cur_id,if(cd4_count >= 0 and @cd4_1 and date(encounter_datetime)<>@cd4_1_date, @cd4_2:= @cd4_1, @cd4_2),@cd4_2:=null) as cd4_2,
if(@prev_id=@cur_id,if(cd4_count >= 0 and @cd4_1 and date(encounter_datetime)<>date(@cd4_1_date), @cd4_2_date:= @cd4_1_date, @cd4_2_date),@cd4_2_date:=null) as cd4_2_date,

if(cd4_count >= 0, @cd4_1:=cd4_count,if(@prev_id=@cur_id,@cd4_1,@cd4_1:=null)) as cd4_1,
if(cd4_count >= 0, @cd4_1_date:=encounter_datetime,if(@prev_id=@cur_id,@cd4_1_date,@cd4_1_date:=null)) as cd4_1_date,

if(@prev_id=@cur_id,if(vl >= 0 and @vl1 and date(encounter_datetime)<>@vl1_date, @vl2:= @vl1, @vl2),@vl2:=null) as vl2,
if(@prev_id=@cur_id,if(vl >= 0 and @vl1 and date(encounter_datetime)<>date(@vl1_date), @vl2_date:= @vl1_date, @vl2_date),@vl2_date:=null) as vl2_date,

if(vl >= 0, @vl1:=vl,if(@prev_id=@cur_id,@vl1,@vl1:=null)) as vl1,
if(vl >= 0, @vl1_date:=encounter_datetime,if(@prev_id=@cur_id,@vl1_date,@vl1_date:=null)) as vl1_date

from flat_lab_data_2 order by person_id, encounter_datetime, sort_order,encounter_type_sort_order,encounter_id
);

# we have updated the encounters to have the most recent lab data. we will now delete the pseudo "lab" encounters which include
# encounters with encounter_id's > 10,000,000 or encounter_types of (5,6,7,8,9,45,87,999). Note that 999 was inserted above and represents a lab encounter. 
alter table flat_lab_data_3 drop prev_id, drop cur_id, drop sort_order, drop encounter_type_sort_order;


# delete all data currently in lost_to_follow_up for any person in the new dataset
delete t1
from flat_lab_data t1
join new_person_data t3 using (person_id);
# add the new dataset into lost_to_follow_up
insert into flat_lab_data
(select * from flat_lab_data_3 order by person_id, encounter_datetime, encounter_id);
