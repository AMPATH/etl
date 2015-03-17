# HIV encounter_type_ids : (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) 
# HIV pmtct encounter_type_ids : (10,11,12,44,46,47)
# HIV adult visit : (1,2,14,17,19)
# HIV peds visit : (3,4,15,26)
# non-lab encounters : (1,2,3,4,10,11,12,16,17,19,25,27,28,29,32,33,34,37,43,47,49,40,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,73,75,76,78,79,80,83)
# lab encounters : (5,6,7,8,9,45,87)

# Build LTFU table

select @start_time := now();
select @prev_time := now();

drop temporary table if exists enc;
create temporary table enc (encounter_id int, encounter_datetime datetime, index encounter_id (encounter_id), index person_id (person_id))
(select e.encounter_id, e.encounter_datetime, e.provider_id, e.patient_id as person_id, e.location_id,e.encounter_type,e.date_created as enc_date_created
from amrs.encounter e
where e.voided=0
);



select timestampdiff(second,@start_time,now()) as 'total time elapsed';
select timestampdiff(second,@prev_time,now()) as 'time elapsed';
select @prev_time := now();

drop temporary table if exists n_obs;
create temporary table n_obs (encounter_id int, rtc_date datetime, on_lopinavir tinyint, on_nnrti tinyint, transfer_out tinyint, transfer_in tinyint, outreach_found_patient tinyint, reason_for_missed_appt varchar(300), index encounter_id (encounter_id))
(select 
encounter_id,
if(concept_id in (1941,1088) and value_coded in (794),1,null) as on_lopinavir, 
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
max(date_created) as obs_date_created,
if(concept_id=1569,value_coded,null) as method_of_outreach_contact
from amrs.obs o
where concept_id in (1088,1941,5096,1502,1777,1285,1596,6206,1733,1042,1559,1725,1040,1579,1499,1255,856,5497,730,1250,1569) and o.voided=0 and encounter_id is not null
group by encounter_id 
);


select timestampdiff(second,@start_time,now()) as 'total time elapsed';
select timestampdiff(second,@prev_time,now()) as 'time elapsed';
select @prev_time := now();


drop temporary table if exists lab_obs;
create temporary table lab_obs (encounter_id integer, index encounter_id (encounter_id))
(select person_id, 
date(obs_datetime) as encounter_datetime, 
date_created as obs_lab_date_created,
if(concept_id in (1042,1040) and value_coded=703,1,0) as lab_tested_hiv_positive,
if(concept_id=856, value_numeric,null) as lab_vl,
if(concept_id=5497,value_numeric,null) as lab_cd4_count, 
if(concept_id=730,value_numeric,null) as lab_cd4_percent,
(obs_id + 100000000) as encounter_id
from amrs.obs o
where concept_id in (1042,1040,856,5497,730) and o.voided=0 and encounter_id is null 
order by person_id, encounter_datetime
);

select timestampdiff(second,@start_time,now()) as 'total time elapsed';
select timestampdiff(second,@prev_time,now()) as 'time elapsed';
select @prev_time := now();

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

alter table lab_obs drop encounter_datetime, drop person_id;

select timestampdiff(second,@start_time,now()) as 'total time elapsed';
select timestampdiff(second,@prev_time,now()) as 'time elapsed';
select @prev_time := now();

drop table if exists person_info;
create temporary table person_info (index person_id (person_id))
(select p.person_id, p.gender, p.birthdate
	from amrs.person p 
	where p.voided=0
);

drop table if exists person_name;
create temporary table person_name (index person_id (person_id))
(select person_id, group_concat(concat(`given_name`,' ',`family_name`) separator ' / ') as person_name
	from amrs.person_name n 
	where n.voided =0
	group by person_id
);

drop table if exists person_phone_number;
create temporary table person_phone_number (index person_id (person_id))
(select person_id, group_concat(if(value != '',value,null) separator ' / ') as phone_number
	from amrs.person_attribute
	where voided=0 and person_attribute_type_id=10
	group by person_id

);

drop table if exists person_identifier;
create temporary table person_identifier (index person_id (person_id))
(select patient_id as person_id, group_concat(if(identifier != '',identifier,null) separator ' / ') as identifier
	from amrs.patient_identifier
	where voided=0 and preferred=1
	group by person_id
);


drop table if exists person;
create temporary table person (index person_id (person_id))
(select * 
	from person_info p
	left outer join person_name n using (person_id)
	left outer join person_phone_number pn using (person_id)
	left outer join person_identifier pi using (person_id)
);

drop table person_info;
drop table person_name;
drop table person_phone_number;
drop table person_identifier;

select timestampdiff(second,@start_time,now()) as 'total time elapsed';
select timestampdiff(second,@prev_time,now()) as 'time elapsed';
select @prev_time := now();


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
select timestampdiff(second,@start_time,now()) as 'total time elapsed';
select timestampdiff(second,@prev_time,now()) as 'time elapsed';
select @prev_time := now();

drop table if exists ltfu_1;
create table ltfu_1 (person_id int, encounter_datetime datetime, index person_enc (person_id, encounter_datetime))
(select *
from enc e1 
left outer join n_obs n1 using (encounter_id)
left outer join lab_obs l using (encounter_id)
left outer join person p using (person_id)
left outer join death_dates d using (person_id)
order by e1.person_id, e1.encounter_datetime);


select timestampdiff(second,@start_time,now()) as 'total time elapsed';
select timestampdiff(second,@prev_time,now()) as 'time elapsed';
select @prev_time := now();

# for those encounters with the same datetime, we need to make sure the encounters we are removing are ordered ahead of the encounters we are keeping
# we will set the encounter_id to a high number for lab encounters and then sort by encounter_id desc.
#update ltfu_1 set encounter_id = encounter_id + 10000000 where encounter_type in (5,6,7,8,9,45,87);

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
from ltfu_1
);

select timestampdiff(second,@start_time,now()) as 'total time elapsed';
select timestampdiff(second,@prev_time,now()) as 'time elapsed';
select @prev_time := now();

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


drop temporary table if exists ltfu_2a;
create temporary table ltfu_2a (encounter_datetime datetime, person_id int, vl1_date datetime, vl2_date datetime,cd4_1_date datetime, cd4_1 mediumint, cd4_2 mediumint, cd4_2_date datetime,index person_enc (person_id, encounter_datetime))
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

from ltfu_2 order by person_id, encounter_datetime, sort_order,encounter_id
);





select timestampdiff(second,@start_time,now()) as 'total time elapsed';
select timestampdiff(second,@prev_time,now()) as 'time elapsed';
select @prev_time := now();

# we have updated the encounters to have the most recent lab data. we will now delete the pseudo "lab" encounters which include
# encounters with encounter_id's > 10,000,000 or encounter_types of (5,6,7,8,9,45,87,999). Note that 999 was inserted above and represents a lab encounter. 
alter table ltfu_2a drop prev_id, drop cur_id, drop vl, drop enc_cd4_count, drop lab_cd4_count, drop cd4_count, drop cd4_percent;

select timestampdiff(second,@start_time,now()) as 'total time elapsed';
select timestampdiff(second,@prev_time,now()) as 'time elapsed';
select @prev_time := now();

delete from ltfu_2a where encounter_type in (5,6,7,8,9,45,87,999999) or encounter_type is null;

select timestampdiff(second,@start_time,now()) as 'total time elapsed';
select timestampdiff(second,@prev_time,now()) as 'time elapsed';
select @prev_time := now();

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


cast(if(@prev_id = @cur_id, if(@prev_encounter_type not in (5,6,7,8,9,45,87,999),@prev_appt_date := @cur_appt_date,@prev_appt_date), @prev_appt_date:=null) as datetime) as prev_appt_date,
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

select timestampdiff(second,@start_time,now()) as 'total time elapsed';
select timestampdiff(second,@prev_time,now()) as 'time elapsed';
select @prev_time := now();

alter table ltfu_2b drop prev_id, drop cur_id, drop cur_appt_date, drop cur_encounter_type, drop cur_rtc_date, drop prev_arv_start_date, drop prev_hiv_start_date;

select timestampdiff(second,@start_time,now()) as 'total time elapsed';
select timestampdiff(second,@prev_time,now()) as 'time elapsed';
select @prev_time := now();

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
create temporary table ltfu_3 (next_appt_date datetime)
(select *,
@prev_id := @cur_id as prev_id, 
@cur_id := person_id as cur_id,

if(@prev_id = @cur_id,@next_encounter_type := @cur_encounter_type, @next_encounter_type:=encounter_type) as next_encounter_type,
@cur_encounter_type := encounter_type as cur_encounter_type,

cast(if(@prev_id = @cur_id, if(@next_encounter_type not in (5,6,7,8,9,45,87,999),@prev_appt_date := @cur_appt_date,@prev_appt_date),@prev_appt_date:=null) as datetime) as next_appt_date,
@cur_appt_date := encounter_datetime as cur_appt_date

from ltfu_2c);

select timestampdiff(second,@start_time,now()) as 'total time elapsed';
select timestampdiff(second,@prev_time,now()) as 'time elapsed';
select @prev_time := now();

alter table ltfu_3 drop prev_id, drop cur_id, drop cur_appt_date, drop cur_encounter_type;

select timestampdiff(second,@start_time,now()) as 'total time elapsed';
select timestampdiff(second,@prev_time,now()) as 'time elapsed';
select @prev_time := now();

drop temporary table if exists ltfu_2c;



drop table if exists lost_to_follow_up;
create table lost_to_follow_up (id MEDIUMINT NOT NULL AUTO_INCREMENT, primary key(id), index person_id (person_id), index encounter_datetime (encounter_datetime), index encounter_id (encounter_id))
(select * from ltfu_3 order by person_id, encounter_datetime, encounter_id);

drop temporary table if exists ltfu_3;

select timestampdiff(second,@start_time,now()) as 'total time elapsed';
select timestampdiff(second,@prev_time,now()) as 'time elapsed';
select @prev_time := now();
