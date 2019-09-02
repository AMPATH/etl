
create table if not exists flat_lab_data (
	encounter_id int,
    person_id int,
    encounter_datetime datetime,
	encounter_type int,
	enc_date_created datetime,
    enc_hiv_test int,
    enc_vl int,
    enc_cd4_count double,
    enc_cd4_percent double,
    enc_obs_date_created datetime,
    lab_hiv_test int,
    lab_vl int,
    lab_cd4_count double,
    lab_cd4_percent double,
	lab_obs_date_created datetime,
    cd4_2 double,
    cd4_2_date datetime,
    cd4_1 double,
    cd4_1_date datetime,
    cd4_percent_2 double,
    cd4_percent_1 double,
    vl2 int,
    vl2_date datetime,
    vl1 int,
    vl1_date datetime,
    primary key encounter_id (encounter_id),
    index person_enc (person_id , encounter_datetime),
	index enc_obs_date_created (enc_obs_date_created),
	index lab_obs_date_created (lab_obs_date_created),
	index enc_date_created (enc_date_created)	
);

select @init_count:= count(*) from flat_lab_data;
select @last_enc_date_created:=max(enc_date_created) from flat_lab_data;
select @last_enc_date_created:=if(@last_enc_date_created,@last_enc_date_created,'1900-01-01');

select @last_enc_obs_date_created:=max(enc_obs_date_created) from flat_lab_data;
select @last_enc_obs_date_created:=if(@last_enc_obs_date_created,@last_enc_obs_date_created,'1900-01-01');

select @last_lab_obs_date_created:=max(lab_obs_date_created) from flat_lab_data;
select @last_lab_obs_date_created:=if(@last_lab_obs_date_created,@last_lab_obs_date_created,'1900-01-01');

select @last_obs_date_created := if(@last_lab_obs_date_created > @last_enc_obs_date_created,@last_lab_obs_date_created,@last_enc_obs_date_created);

drop table if exists voided_obs;
create table voided_obs (index encounter_id (encounter_id),index obs_id (obs_id)) 
(select 
    encounter_id,
	person_id,
    obs_id,
    obs_datetime,
    date_voided,
    concept_id,
    date_created 
from amrs.obs
where voided = 1
	and date_voided > @last_obs_date_created
	and date_created <= @last_obs_date_created
	and concept_id in (1042 , 1040, 856, 5497, 730));


/*
build a dataset of all relevant encounter data : 
	(1) new encounters, 
	(2) encounters with voided obs
	(3) encounters with new obs
*/
# new encounter_data

drop temporary table if exists enc;
create temporary table enc (encounter_id int, primary key encounter_id (encounter_id), index person_id (person_id))
(select e.encounter_id, e.patient_id as person_id, e.encounter_datetime, encounter_type, e.date_created as enc_date_created
from amrs.encounter e 
where e.voided=0 and e.date_created > @last_enc_date_created
);

# add to the enc dataset all existing encounters with voided obs. 
insert ignore into enc
(select e.encounter_id, e.patient_id as person_id, e.encounter_datetime, encounter_type, e.date_created as enc_date_created
from amrs.encounter e
join voided_obs v using (encounter_id)
where e.date_created <= @last_enc_date_created and e.voided=0 and v.encounter_id is not null
);

# add in encounters which have new obs attached to them
insert ignore into enc
(select e.encounter_id, e.patient_id as person_id, e.encounter_datetime, encounter_type, e.date_created as enc_date_created
from amrs.encounter e
join amrs.obs o using (encounter_id)
where o.date_created > @last_obs_date_created and o.voided=0 and e.date_created <= @last_enc_date_created and e.voided=0 and concept_id in (1042,1040,856,5497,730) 
);

#remove test patients that exist in amrs
delete t1 
from enc t1
join amrs.person_attribute t2 USING (person_id) 
where t2.person_attribute_type_id = 28 and value = 'true';


# create a dataset of the new obs. 
drop table if exists obs_subset;
create temporary table obs_subset (primary key obs_id (obs_id),index encounter_id (encounter_id))
(select * from amrs.obs o where concept_id in (1042,1040,856,5497,730) and o.voided=0 and date_created > @last_obs_date_created);

# add obs of encounters with voided obs
insert ignore into obs_subset
(select t1.* 
	from amrs.obs t1 
	join voided_obs t2 using (encounter_id)
	where t1.voided=0 and t1.date_created <= @last_obs_date_created and t2.encounter_id is not null and t1.concept_id in (1042,1040,856,5497,730)
);

# add obs for encounters which have new obs
insert ignore into obs_subset
(select o.*
	from amrs.encounter e
	join amrs.obs o using (encounter_id)
	where o.date_created > @last_obs_date_created and o.voided=0 and e.date_created <= @last_enc_date_created and e.voided=0 and o.concept_id in (1042,1040,856,5497,730)
);

drop temporary table if exists enc_obs;
create temporary table enc_obs (encounter_id int, index encounter_id (encounter_id))
(select 
	encounter_id,
	max(if(concept_id in (1042,1040),value_coded,null)) as enc_hiv_test,
	max(if(concept_id=856, value_numeric,null)) as enc_vl,
	max(if(concept_id=5497,value_numeric,null)) as enc_cd4_count, 
	max(if(concept_id=730,value_numeric,null)) as enc_cd4_percent,
	max(date_created) as enc_obs_date_created
	from obs_subset
	where encounter_id is not null
	group by encounter_id 
);


drop temporary table if exists lab_obs;
create temporary table lab_obs (encounter_id integer, index encounter_id (encounter_id))
(select 
	person_id, 
	(obs_id + 100000000) as encounter_id,
	date(obs_datetime) as encounter_datetime, 
	if(concept_id in (1042,1040),value_coded,null) as lab_hiv_test,
	if(concept_id=856, value_numeric,null) as lab_vl,
	if(concept_id=5497,value_numeric,null) as lab_cd4_count, 
	if(concept_id=730,value_numeric,null) as lab_cd4_percent,
	date_created as lab_obs_date_created
from obs_subset
where encounter_id is null 
order by person_id, encounter_datetime
);

# create fake encounters for the lab encounters which don't (BUT SHOULD!!!) have an associated encounter_id

insert into enc
(select 
encounter_id,
person_id, 
encounter_datetime, 
999999 as encounter_type,
lab_obs_date_created as enc_date_created
from lab_obs
);

alter table lab_obs drop person_id, drop encounter_datetime;

drop table if exists new_person_data;
create temporary table new_person_data(primary key person_id (person_id))
(select distinct person_id from enc);

insert ignore into new_person_data
(select patient_id as person_id from amrs.encounter where date_created <= @last_enc_date_created and date_voided > @last_enc_date_created);

insert ignore into new_person_data
(select person_id from amrs.obs where date_created <= @last_obs_date_created and date_voided > @last_obs_date_created and concept_id in (1042,1040,856,5497,730));

drop table if exists new_dataset;
create temporary table new_dataset (index encounter_id (encounter_id),index person_enc (person_id , encounter_datetime))
(select 
	encounter_id,
	person_id,
    encounter_datetime,
    encounter_type,
    enc_date_created,
    enc_hiv_test,
    enc_vl,
    enc_cd4_count,
    enc_cd4_percent,
    enc_obs_date_created,
    lab_hiv_test,
    lab_vl,
    lab_cd4_count,
    lab_cd4_percent,
	lab_obs_date_created
from flat_lab_data t1
join new_person_data t2 USING (person_id));


#remove any encounters that have a voided obs. 
drop table if exists encounters_to_be_removed;
create temporary table encounters_to_be_removed (primary key encounter_id (encounter_id))
(select distinct if(encounter_id,encounter_id,obs_id+100000000) as encounter_id from voided_obs);

#remove any encounters that have been voided.
insert ignore into encounters_to_be_removed
(select encounter_id from amrs.encounter where voided=1 and date_created <= @last_enc_date_created and date_voided > @last_enc_date_created);

#remove any encounters with new obs as the entire encounter will be rebuilt and added back
insert ignore into encounters_to_be_removed
(select if(encounter_id,encounter_id,obs_id+100000000) from obs_subset);

delete t1
from new_dataset t1
join encounters_to_be_removed t2 using (encounter_id);

insert into new_dataset
(select *
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


select @prev_id:=null;
select @cur_id:=null;
select @vl1:=null;
select @vl2:=null;
select @vl1_date:=null;
select @vl2_date:=null;
select @cd4_1:=null;
select @cd4_2:=null;
select @cd4_percent_1:=null;
select @cd4_percent_2:=null;
select @cd4_1_date:=null;
select @cd4_2_date:=null;
select @cd4_percent_1_date:=null;


drop table if exists flat_lab_data_3;
create temporary table flat_lab_data_3 (encounter_datetime datetime, person_id int, vl1_date datetime, vl2_date datetime,cd4_1_date datetime, cd4_1 mediumint, cd4_2 mediumint, cd4_2_date datetime,index person_enc (person_id, encounter_datetime))
(select *,
@prev_id := @cur_id as prev_id, 
@cur_id := person_id as cur_id,

if(@prev_id=@cur_id,if(cd4_count >= 0 and @cd4_1 and date(encounter_datetime)<>@cd4_1_date, @cd4_2:= @cd4_1, @cd4_2),@cd4_2:=null) as cd4_2,
if(@prev_id=@cur_id,if(cd4_count >= 0 and @cd4_1 and date(encounter_datetime)<>date(@cd4_1_date), @cd4_2_date:= @cd4_1_date, @cd4_2_date),@cd4_2_date:=null) as cd4_2_date,

if(cd4_count >= 0, @cd4_1:=cd4_count,if(@prev_id=@cur_id,@cd4_1,@cd4_1:=null)) as cd4_1,
if(cd4_count >= 0, @cd4_1_date:=encounter_datetime,if(@prev_id=@cur_id,@cd4_1_date,@cd4_1_date:=null)) as cd4_1_date,

if(@prev_id=@cur_id,if(cd4_percent >= 0 and @cd4_percent_1 and date(encounter_datetime)<>@cd4_percent_1_date, @cd4_percent_2:= @cd4_percent_1, @cd4_percent_2),@cd4_percent_2:=null) as cd4_percent_2,

if(cd4_percent >= 0, @cd4_percent_1:=cd4_percent,if(@prev_id=@cur_id,@cd4_percent_1,@cd4_percent_1:=null)) as cd4_percent_1,
if(cd4_percent >= 0, @cd4_percent_1_date:=encounter_datetime,if(@prev_id=@cur_id,@cd4_percent_1_date,@cd4_percent_1_date:=null)) as cd4_percent_1_date,


if(@prev_id=@cur_id,if(vl >= 0 and @vl1 and date(encounter_datetime)<>@vl1_date, @vl2:= @vl1, @vl2),@vl2:=null) as vl2,
if(@prev_id=@cur_id,if(vl >= 0 and @vl1 and date(encounter_datetime)<>date(@vl1_date), @vl2_date:= @vl1_date, @vl2_date),@vl2_date:=null) as vl2_date,

if(vl >= 0, @vl1:=vl,if(@prev_id=@cur_id,@vl1,@vl1:=null)) as vl1,
if(vl >= 0, @vl1_date:=encounter_datetime,if(@prev_id=@cur_id,@vl1_date,@vl1_date:=null)) as vl1_date

from flat_lab_data_2 order by person_id, encounter_datetime, sort_order,encounter_type_sort_order,encounter_id
);

# we have updated the encounters to have the most recent lab data. we will now delete the pseudo "lab" encounters which include
# encounters with encounter_id's > 10,000,000 or encounter_types of (5,6,7,8,9,45,87,999). Note that 999 was inserted above and represents a lab encounter. 
alter table flat_lab_data_3 drop prev_id, drop cur_id, drop sort_order, drop encounter_type_sort_order, drop cd4_count, drop vl, drop hiv_test,drop cd4_percent, drop cd4_percent_1_date;

delete t1 
from flat_lab_data t1
join new_person_data t3 USING (person_id);

insert into flat_lab_data
(select * from flat_lab_data_3 order by person_id, encounter_datetime, encounter_id);

select (count(*) - @init_count) as 'flat_lab_data new rows' from flat_lab_data;