# HIV encounter_type_ids : (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) 
# HIV pmtct encounter_type_ids : (10,11,12,44,46,47)
# HIV adult visit : (1,2,14,17,19)
# HIV peds visit : (3,4,15,26)
# non-lab encounters : (1,2,3,4,10,11,12,16,17,19,25,27,28,29,32,33,34,37,43,47,49,40,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,73,75,76,78,79,80,83)
# lab encounters : (5,6,7,8,9,45,87)

# Build LTFU table

select @start_time := now();
select @start_date := '2014-04-16';

drop temporary table if exists enc;
create temporary table enc (encounter_datetime datetime, index encounter_id (encounter_id), index person_id (person_id))
(select e.encounter_id, e.patient_id as person_id, encounter_datetime, e.date_created as enc_date_created, encounter_type, provider_id, location_id
from amrs.encounter e
where (e.voided=0 or e.date_voided >= @start_date) and e.date_created <= @start_date
);
update enc set voided = 0 where date_voided >= @start_date;
update enc set date_voided = null where voided=0;


# delete test patients
delete t1 
from enc t1 
join amrs.person_attribute t2 using (person_id) 
where t2.person_attribute_type_id=28 and value='true';


select @interval_time := now() as 'start_obs_subset';
drop table if exists obs_subset;
create temporary table obs_subset (index encounter_id (encounter_id))
(select obs_id, obs_datetime, person_id, encounter_id, concept_id,value_coded, value_numeric,value_datetime,date_created,voided,date_voided
	from amrs.obs o
	where (voided=0 or date_voided >= @start_date) and o.date_created <= @start_date and concept_id in (1042,1040,856,5497,730) 
);
update obs_subset set voided = 0 where date_voided >= @start_date;
update obs_subset set date_voided = null where voided=0;

select timestampdiff(second,@interval_time,now()) as 'time to get obs';

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
	min(if(concept_id in (1042,1040),value_coded,null)) as lab_hiv_test,
	min(if(concept_id=856, value_numeric,null)) as lab_vl,
	min(if(concept_id=5497,value_numeric,null)) as lab_cd4_count, 
	min(if(concept_id=730,value_numeric,null)) as lab_cd4_percent,
	(obs_id + 100000000) as encounter_id
	from obs_subset
	where encounter_id is null 
	order by person_id, encounter_datetime
);

# create fake encounters for the lab encounters which don't (BUT SHOULD!!!) have an associated encounter_id
insert into enc
(select 
	encounter_id,
	encounter_datetime, 
	999999 as provider_id, 
	person_id, 
	999999 as location_id,
	999999 as encounter_type,
	obs_lab_date_created as enc_date_created
	from lab_obs
);

alter table lab_obs drop encounter_datetime, drop person_id;

drop table if exists flat_lab_data_1;
create temporary table flat_lab_data_1 (index encounter_id (encounter_id))
(select *
from enc e1 
left outer join enc_obs n1 using (encounter_id)
left outer join lab_obs l using (encounter_id)
order by e1.person_id, e1.encounter_datetime);

# for those encounters with the same datetime, we need to make sure the encounters we are removing are ordered ahead of the encounters we are keeping
# we will set the encounter_id to a high number for lab encounters and then sort by encounter_id desc.
#update ltfu_1 set encounter_id = encounter_id + 10000000 where encounter_type in (5,6,7,8,9,45,87);

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
from flat_lab_data_1
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


drop table if exists flat_lab_data;
create table flat_lab_data (encounter_datetime datetime, person_id int, vl1 int, vl1_date datetime, vl2 int, vl2_date datetime,cd4_1_date datetime, cd4_1 mediumint, cd4_2 mediumint, cd4_2_date datetime,index person_enc (person_id, encounter_datetime))
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
alter table flat_lab_data drop prev_id, drop cur_id, drop sort_order, drop encounter_type_sort_order, drop provider_id, drop location_id;
