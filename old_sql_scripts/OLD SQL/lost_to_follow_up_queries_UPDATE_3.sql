# HIV encounter_type_ids : (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) 
# HIV pmtct encounter_type_ids : (10,11,12,44,46,47)
# HIV adult visit : (1,2,14,17,19)
# HIV peds visit : (3,4,15,26)
# non-lab encounters : (1,2,3,4,10,11,12,16,17,19,25,27,28,29,32,33,34,37,43,47,49,40,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,73,75,76,78,79,80,83)
# lab encounters : (5,6,7,8,9,45,87)


# required indexes: obs.date_voided, obs.date_created, encounter.date_voided, encounter.date_created

/*
Variables:
	1. concept_ids for obs dataset
	2. final denormalized table name
	3. 

*/

# Remove previously voided data from existing denormalized_dataset
# 1. For voided encounteres, we will remove the whole row from the dataset
# 2. For voided obs, we will delete the value of the cell in a row to null


drop table if exists ltfu_1;
create table ltfu_1 (index encounter_id (encounter_id), index person_id (person_id))
(select * from amrs.ltfu_1);

select @last_enc_date_created := max(enc_date_created) from ltfu_1;
select @last_obs_date_created := max(obs_date_created) from ltfu_1;


drop table if exists voided_obs;
create table voided_obs
(select person_id, encounter_id, obs_id, obs_datetime, date_voided, concept_id
from amrs.obs where voided=1 and date_voided > @last_obs_date_created and date_created <= @last_obs_date_created);

# delete any rows that are voided in amrs.encounter
delete t1
from ltfu_1 t1
join
(select distinct encounter_id 
	from
	(
		(select t1.encounter_id
			from ltfu_1 t1 
			join amrs.encounter t2 using (encounter_id) 
			where t2.voided=1
		)

		union

		# remove all rows that have a voided obs. we will rebuild this later. 
		(select t1.encounter_id
			from ltfu_1 t1
			join voided_obs t2 using (encounter_id)
			where t2.encounter_id is not null)

		union

		# delete any encounters that have new obs. we will add these back in later. 
		(select t1.encounter_id
			from ltfu_1 t1
			join  
			(select e.encounter_id
				from amrs.encounter e
				join amrs.obs o using (encounter_id)
				where o.date_created > @last_obs_date_created and o.voided=0 and e.date_created <= @last_enc_date_created
			) t2 using (encounter_id)
		)

		union

		# remove those rows associated with obs with no associated encounter_ids. Namely this will remove the "pseudo" lab encounters that were created
		(select t1.encounter_id
			from ltfu_1 t1
			join voided_obs t2 on t1.encounter_datetime = t2.obs_datetime and t1.person_id = t2.person_id
			where t2.encounter_id is null)
	)t3 
) t4 using (encounter_id)
;


/*
build a dataset of all relevant encounter data : 
	(1) new encounters, 
	(2) encounters with voided obs
	(3) encounters with new obs
	(4) encounters with the same datetime as voided obs with no encounter_ids
*/
drop temporary table if exists enc;
create temporary table enc (encounter_id int, encounter_datetime datetime, index encounter_id (encounter_id), index person_id (person_id))

(select * from
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

# add to the enc dataset voided obs with no encounter_ids that have been attached to an encounter because of the same datetime
(select e.encounter_id, e.encounter_datetime, e.provider_id, e.patient_id as person_id, e.location_id,e.encounter_type, e.date_created as enc_date_created
from amrs.encounter e
join voided_obs v on e.patient_id= v.person_id and e.encounter_datetime = v.obs_datetime
where e.date_created <= @last_enc_date_created and e.voided=0 and v.encounter_id is null
)

union

# add in encounters which have new obs attached to them
(select e.encounter_id, e.encounter_datetime, e.provider_id, e.patient_id as person_id, e.location_id,e.encounter_type, e.date_created as enc_date_created
from amrs.encounter e
join amrs.obs o using (encounter_id)
where o.date_created > @last_obs_date_created and o.voided=0 and e.date_created <= @last_enc_date_created and e.voided=0
)
) t3 group by encounter_id);


# create a dataset of the new obs. 
drop table if exists obs_dataset;
create temporary table obs_dataset (index encounter_id (encounter_id))
(select * from
	(
		(select * from amrs.obs o
			where concept_id in (1088,1941,5096,1502,1777,1285,1596,6206,1733,1042,1559,1725,1040,1579,1499,1255,856,5497,730,1250) 
				and o.voided=0 and date_created > @last_obs_date_created
		)
		union
		# add obs for old encounters which had voided obs
		(select o.*
			from amrs.encounter e
			join voided_obs v on e.patient_id= v.person_id and e.encounter_datetime = v.obs_datetime
			join amrs.obs o on o.person_id= v.person_id and o.obs_datetime = v.obs_datetime
			where e.date_created <= @last_enc_date_created and e.voided=0 and v.encounter_id is null and o.voided=0
		)
		union
		# add obs without encounter_ids with matching person_ids and encounter_datetimes of obs which were voided
		(select o.*
			from amrs.obs o
			join voided_obs v on o.person_id= v.person_id and date(o.obs_datetime) = date(v.obs_datetime) 
			where o.date_created <= @last_obs_date_created and o.voided=0 and v.encounter_id is null and o.encounter_id is null
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

drop temporary table if exists n_obs;
create temporary table n_obs (encounter_id int, rtc_date datetime, on_lopinavir tinyint, on_nnrti tinyint, transfer_out tinyint, transfer_in tinyint, outreach_found_patient tinyint, reason_for_missed_appt varchar(300), index encounter_id (encounter_id))
(select 
encounter_id,
min(if(concept_id in (1941,1088) and value_coded in (794),1,null)) as on_lopinavir, 
min(if(concept_id in (1941,1088) and value_coded in (6467,6964,792,633,631),1,null)) as on_nnrti, 
min(if(concept_id in (5096,1502,1777),value_datetime,null)) as rtc_date,
max(if( (concept_id = 1285 and value_coded=1287) or (concept_id=1596 and value_coded=1594) or (concept_id=6206 and value_coded=1595) or (concept_id=1579 and value_coded=1066),1,0)) as transfer_out,
max(if( (concept_id = 1285 and value_coded=1286) or (concept_id=1733 and value_coded=1732),1,0)) as transfer_in,
max(if(concept_id in (1042,1040) and value_coded=703,1,0)) as enc_tested_hiv_positive,
max(if(concept_id=1559 and value_coded=1065,1,if((concept_id=1559 and value_coded=1066) or concept_id=1725,0,null))) as outreach_found_patient,
group_concat(if(concept_id=1733,value_coded,null) separator ' ') as reason_for_missed_appt,
min(if(concept_id=1255,value_coded,null)) as arv_plan,
max(if(concept_id=856, value_numeric,null)) as enc_vl,
max(if(concept_id=5497,value_numeric,null)) as enc_cd4_count, 
max(if(concept_id=730,value_numeric,null)) as enc_cd4_percent,
min(case
		when concept_id=1250 and value_coded in (6467,6964,792,633,631) then 1
		when concept_id=1250 and value_coded=794 then 2	
		when concept_id=1250 and value_coded=6156 then 3
		else null
	end) as arv_regimen_plan,
max(date_created) as obs_date_created
from obs_dataset 
where encounter_id is not null
group by encounter_id 
);



select @temp_index := max(encounter_id) from amrs.ltfu_1;
select if(@temp_index > 10000000,@index:= @temp_index + 1,@index:=@temp_index + 10000000);

drop temporary table if exists lab_obs;
create temporary table lab_obs (encounter_id integer, index encounter_id (encounter_id))
(select person_id, 
date(obs_datetime) as encounter_datetime, 
date_created as obs_lab_date_created,
max(if(concept_id in (1042,1040) and value_coded=703,1,0)) as lab_tested_hiv_positive,
max(if(concept_id=856, value_numeric,null)) as lab_vl,
max(if(concept_id=5497,value_numeric,null)) as lab_cd4_count, 
max(if(concept_id=730,value_numeric,null)) as lab_cd4_percent,
@index := @index + 1 as encounter_id
from obs_dataset
where encounter_id is null and concept_id in (1042,1040,856,5497,730)
group by person_id, encounter_datetime
order by person_id, encounter_datetime
);


# create fake encounters for the lab encounters which don't (BUT SHOULD!!!) have an associated encounter_id
insert into enc
(select encounter_id,
encounter_datetime, 
999999 as provider_id, 
person_id, 
999999 as location_id,
999999 as encounter_type,
obs_lab_date_created as enc_date_created
from lab_obs
);

# get rid of person_id and encounter_datetime so they don't interfere with join to make denormalized table. they are no longer necesseary now
# that there is an associated encounter_id
alter table lab_obs drop person_id, drop encounter_datetime;


/*
# query to create table of patients who have died and associated dates based on concept defintion in wiki. 
# Note the query (used from the wiki) does not map to the definition on the wiki. This should be addressed at a later date (7/1/2013). For now will use the
# earliest date as the death date
drop temporary table if exists death_dates;
create temporary table death_dates (person_id int, death_date datetime, index person_id (person_id))
(select person_id, min(death_date) as death_date from
(
select person_id,obs_datetime as death_date, 'o' as type from amrs.obs o where o.voided=0 and ((o.concept_id=6206 and o.value_coded=159) or  o.concept_id in(1570,1734,1573) or (o.concept_id=1733 and o.value_coded=159) or (o.concept_id=1596 and o.value_coded=1593)) 
union
select e.patient_id as person_id, encounter_datetime, 'e' as type from amrs.encounter e where e.voided=0 and e.encounter_type=31 
union 
select p.person_id, p.death_date, 'p' as type from amrs.person p where p.voided=0 and p.death_date is not null) t0
where person_id in (select patient_id from amrs.patient where voided=0)
group by person_id);


drop temporary table if exists person;
create temporary table person
(select p.person_id, p.gender, p.birthdate from amrs.person where p.voided=0);

 
drop temporary table if exists address;
create temporary table address (person_id int, index person_id (person_id))
(select a.person_id, a.state_province as province, a.address6 as location, a.address5 as sublocation
from amrs.person_address a
where a.voided=0
group by person_id); 



insert into ltfu_1
(select *
from enc e1 
left outer join n_obs n1 using (encounter_id)
left outer join lab_obs l using (encounter_id)
left outer join address a using (person_id)
left outer join death_dates d using (person_id)
order by e1.person_id, e1.encounter_datetime);
*/

insert into ltfu_1
(select *
from enc e1 
left outer join n_obs n1 using (encounter_id)
left outer join lab_obs l using (encounter_id)
order by e1.person_id, e1.encounter_datetime
);

drop table voided_obs;


# subtract out all data for any person who has new data to be entered. We require all previous data for a given person to make the new derived table. 
# we will then add this back into the final table in the end.

drop table if exists lost_to_follow_up;
create table lost_to_follow_up (index encounter_id (encounter_id), index person_id (person_id), index encounter_datetime (encounter_datetime))
(select * from amrs.lost_to_follow_up);

drop table if exists new_person_data;
create temporary table new_person_data(index person_id (person_id))
(select distinct person_id from
(
(select distinct person_id from enc)
union
(select patient_id as person_id from amrs.encounter where date_created <= @last_enc_date_created and date_voided > @last_enc_date_created)

union
(select person_id from amrs.obs where date_created <= @last_enc_date_created and date_voided > @last_enc_date_created)
) t0);

drop table if exists new_dataset;
create temporary table new_dataset (index encounter_id (encounter_id))
(select t1.* from ltfu_1 t1 join new_person_data t2 using(person_id));


# for those encounters with the same datetime, we need to make sure the encounters we are removing are ordered ahead of the encounters we are keeping
# we will set the encounter_id to a high number for lab encounters and then sort by encounter_id desc.
# update new_dataset set encounter_id = encounter_id + 10000000 where encounter_type in (5,6,7,8,9,45,87) and encounter_id < 10000000;


drop table if exists ltfu_2;
create temporary table ltfu_2
(select *,
if(lab_tested_hiv_positive >= 0,lab_tested_hiv_positive,if(enc_tested_hiv_positive >= 0,enc_tested_hiv_positive,null)) as tested_hiv_positive,
if(lab_cd4_count >= 0,lab_cd4_count,if(enc_cd4_count >= 0,enc_cd4_count,null)) as cd4_count,
if(lab_vl >= 0,lab_vl,if(enc_vl >= 0,enc_vl,null)) as vl,
if(lab_cd4_percent >= 0,lab_cd4_percent,if(enc_cd4_percent >= 0,enc_cd4_percent,null)) as cd4_percent,
case 
	when encounter_type in (5,6,7,8,9,45,87,999999) then 1
	else 2
end as sort_order
from new_dataset
);



select @vl1:= null;
select @vl2:= null;
select @vl1_date:= null;
select @vl2_date:= null;
select @cd4_1:= null;
select @cd4_2:= null;
select @cd4_1_date:= null;
select @cd4_2_date:= null;

drop temporary table if exists ltfu_2a;
create temporary table ltfu_2a (encounter_datetime datetime, person_id int, 
								vl1_date datetime, vl2_date datetime,cd4_1_date datetime, cd4_1 mediumint, cd4_2 mediumint, cd4_2_date datetime,
								index person_enc (person_id, encounter_datetime))
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

from ltfu_2 order by person_id, encounter_datetime, sort_order, encounter_id
);


# we have updated the encounters to have the most recent lab data. we will now delete the pseudo "lab" encounters which include
# encounters with encounter_id's > 10,000,000 or encounter_types of (5,6,7,8,9,45,87,999). Note that 999 was inserted above and represents a lab encounter. 
alter table ltfu_2a drop prev_id, drop cur_id, drop vl, drop enc_cd4_count, drop lab_cd4_count, drop cd4_count, drop cd4_percent;


delete from ltfu_2a where encounter_type in (5,6,7,8,9,45,87,999999) or encounter_type is null;


select @prev_id := null;
select @cur_id := null;
select @prev_encounter_type := null;
select @cur_encounter_type := null;
select @prev_plan := null;
select @cur_plan := null;
select @prev_appt_date :=null;
select @cur_appt_date :=null;
select @prev_rtc_date :=null;
select @cur_rtc_date :=null;
select @prev_hiv_start_date := null;
select @hiv_start_date := null;
select @prev_arv_start_date := null;
select @arv_start_date := null;

drop temporary table if exists ltfu_2b;
create temporary table ltfu_2b (encounter_datetime datetime, person_id int, prev_appt_date datetime, cur_appt_date datetime, prev_rtc_date datetime, 
                               cur_rtc_date datetime, plan tinyint, prev_plan tinyint, prev_encounter_type tinyint, arv_start_date datetime, prev_arv_start_date datetime,
								hiv_start_date datetime, plan_start_date datetime,
								index person_enc (person_id, encounter_datetime desc))
(select *,
@prev_id := @cur_id as prev_id, 
@cur_id := person_id as cur_id,

if(@prev_id = @cur_id,@prev_encounter_type := @cur_encounter_type, @prev_encounter_type:=encounter_type) as prev_encounter_type,
@cur_encounter_type := encounter_type as cur_encounter_type,


if(@prev_id = @cur_id, @prev_plan := @cur_plan, @prev_plan:=null) as prev_plan,

case
	when arv_plan in (1107,1260) then @cur_plan := if(@prev_plan is null,null,0)
	when arv_regimen_plan in (1,2,3) then @cur_plan := arv_regimen_plan
	when on_lopinavir then @cur_plan := 2
	when on_nnrti then @cur_plan := 1	
	else @cur_plan := @prev_plan
end as plan,


cast(if(@prev_id = @cur_id, if(@prev_encounter_type not in (5,6,7,8,9,45,87,999999),@prev_appt_date := @cur_appt_date,@prev_appt_date), @prev_appt_date:=null) as datetime) as prev_appt_date,
@cur_appt_date := encounter_datetime as cur_appt_date,

cast(if(@prev_id = @cur_id, @prev_rtc_date := @cur_rtc_date, @prev_rtc_date:=null) as datetime) as prev_rtc_date,
@cur_rtc_date := rtc_date as cur_rtc_date,

case
	when @prev_id = @cur_id then @prev_hiv_start_date := @hiv_start_date
	else @prev_hiv_start_date := null
end as prev_hiv_start_date,

if(@prev_id = @cur_id,
	if((encounter_datetime < @prev_hiv_start_date or @prev_hiv_start_date is null) and (tested_hiv_positive or encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) or on_nnrti or on_lopinavir), @hiv_start_date := encounter_datetime,@prev_hiv_start_date),
	if(tested_hiv_positive or encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) or on_nnrti or on_lopinavir,@hiv_start_date := encounter_datetime, @hiv_start_date:=null)) as hiv_start_date,

case
	when @prev_id = @cur_id then @prev_arv_start_date := @arv_start_date
	else @prev_arv_start_date := null
end as prev_arv_start_date,
											
case
	when arv_plan = 1256 then @arv_start_date := encounter_datetime
	when arv_plan in (1107,1260) then @arv_start_date := null
	when (on_nnrti or on_lopinavir) and @prev_arv_start_date is null then @arv_start_date := encounter_datetime
	else @arv_start_date := @prev_arv_start_date
end as arv_start_date

from ltfu_2a 
);

alter table ltfu_2b drop prev_id, drop cur_id, drop cur_appt_date, drop cur_encounter_type, drop cur_rtc_date, drop prev_arv_start_date, drop prev_hiv_start_date;
drop temporary table if exists ltfu_2a;


drop temporary table if exists ltfu_2c;
create temporary table ltfu_2c
(select * from ltfu_2b order by person_id, encounter_datetime desc, encounter_id desc);
drop temporary table if exists ltfu_2b;


select @prev_id := null;
select @cur_id := null;
select @prev_appt_date := null;
select @cur_appt_date := null;
select @next_encounter_type := null;
select @cur_encounter_type := null;

drop temporary table if exists ltfu_3;
create temporary table ltfu_3 (id int, next_appt_date datetime)
(select *,
@prev_id := @cur_id as prev_id, 
@cur_id := person_id as cur_id,

if(@prev_id = @cur_id,@next_encounter_type := @cur_encounter_type, @next_encounter_type:=encounter_type) as next_encounter_type,
@cur_encounter_type := encounter_type as cur_encounter_type,

cast(if(@prev_id = @cur_id, if(@next_encounter_type not in (5,6,7,8,9,45,87,999999),@prev_appt_date := @cur_appt_date,@prev_appt_date),@prev_appt_date:=null) as datetime) as next_appt_date,
@cur_appt_date := encounter_datetime as cur_appt_date

from ltfu_2c);


alter table ltfu_3 drop prev_id, drop cur_id, drop cur_appt_date, drop cur_encounter_type;
drop temporary table if exists ltfu_2c;

# delete all data currently in lost_to_follow_up for any person in the new dataset
delete t1
from lost_to_follow_up t1
join new_person_data t3 using (person_id);

# add the new dataset into lost_to_follow_up
insert into lost_to_follow_up
(select * from ltfu_3 order by person_id, encounter_datetime, encounter_id);

drop temporary table if exists ltfu_3;

