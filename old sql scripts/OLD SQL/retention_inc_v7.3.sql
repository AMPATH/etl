# HIV encounter_type_ids : (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) 
#1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21 
#1,2,3,4,10,13,14,15,17,19,22,23,26,43,47 (reporting team)

# HIV pmtct encounter_type_ids : (10,11,12,44,46,47)
# HIV adult visit : (1,2,14,17,19)
# HIV peds visit : (3,4,15,26)
# non-lab encounters : (1,2,3,4,10,11,12,16,17,19,25,27,28,29,32,33,34,37,43,47,49,40,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,73,75,76,78,79,80,83)
# lab encounters : (5,6,7,8,9,45,87)


create table if not exists flat_retention_defined
(person_id int,encounter_id int,encounter_datetime datetime,provider_id int,location_id mediumint,encounter_type mediumint,enc_date_created datetime, transfer_care int,discontinue_is_hiv_neg int,rtc_date datetime,arv_meds varchar(500),arv_plan int,arv_meds_plan varchar(500),ampath_status int,obs_date_created datetime,gender varchar(10),birth_date datetime,dead tinyint, death_date datetime, person_name varchar(500),phone_number varchar(100),identifier varchar(50), health_center int, index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime), index enc_date_created (enc_date_created), index obs_date_created (obs_date_created), index ltfu (location_id,encounter_datetime));

select @init_count := count(*) from flat_retention_defined;

select @last_enc_date_created := max(enc_date_created) from flat_retention_defined;
select @last_enc_date_created := if(@last_enc_date_created,@last_enc_date_created,'1900-01-01');
#select @last_enc_date_created := '2014-04-27';


select @last_obs_date_created := max(obs_date_created) from flat_retention_defined;
select @last_obs_date_created := if(@last_obs_date_created,@last_obs_date_created,'1900-01-01');
#select @last_obs_date_created := '2014-04-27';

# (1285,1946,5096,1502,1777,1088,1255,1250,9082)
select @concept_ids := '1285 1946 5096 1502 1777 1088 1255 1250 9082';


drop table if exists voided_obs;
create table voided_obs (index encounter_id (encounter_id), index obs_id (obs_id))
(select person_id, encounter_id, obs_id, obs_datetime, date_voided, concept_id, date_created
from amrs.obs where voided=1 and date_voided > @last_obs_date_created and date_created <= @last_obs_date_created and concept_id in (1285,1946,5096,1502,1777,1088,1255,1250,9082));

/*
drop table if exists voided_obs;
create table voided_obs (index encounter_id (encounter_id), index obs_id (obs_id))
(select person_id, encounter_id, obs_id, obs_datetime, date_voided, concept_id, date_created
from amrs.obs where voided=1 and date_voided > @last_obs_date_created and date_created <= @last_obs_date_created and @concept_ids regexp concat('[[:<:]]',concept_id,'[[:>:]]')
);
*/

drop temporary table if exists enc;
create temporary table enc (encounter_id int, encounter_datetime datetime, primary key encounter_id (encounter_id), index person_id (person_id))
(select e.patient_id as person_id, e.encounter_id, e.encounter_datetime, e.provider_id, e.location_id,e.encounter_type, e.date_created as enc_date_created
from amrs.encounter e
where e.voided=0
and e.date_created > @last_enc_date_created
#and encounter_type not in (5,6,7,8,9,45,87)
and encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21) 
);

insert ignore into enc
(select e.patient_id as person_id, e.encounter_id, e.encounter_datetime, e.provider_id, e.location_id,e.encounter_type, e.date_created as enc_date_created
from amrs.encounter e
join voided_obs v using (encounter_id)
where e.date_created <= @last_enc_date_created and e.voided=0 and v.encounter_id is not null and e.encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21) 
);

# add in encounters which have new relevant obs attached to them
insert ignore into enc
(select e.patient_id as person_id, e.encounter_id, e.encounter_datetime, e.provider_id, e.location_id,e.encounter_type, e.date_created as enc_date_created
from amrs.encounter e
join amrs.obs o using (encounter_id)
where o.date_created > @last_obs_date_created and o.voided=0 and e.date_created <= @last_enc_date_created and e.voided=0 and concept_id in (1285,1946,5096,1502,1777,1088,1255,1250,9082)  and encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21)  
);

delete t1 
from enc t1 
join amrs.person_attribute t2 using (person_id) 
where t2.person_attribute_type_id=28 and value='true';


# create a dataset of the new obs. 
drop table if exists obs_subset;
create temporary table obs_subset (primary key obs_id (obs_id), index encounter_id (encounter_id))
(select * from amrs.obs o where concept_id in (1285,1946,5096,1502,1777,1088,1255,1250,9082) and o.voided=0 and date_created > @last_obs_date_created);

# add obs of encounters with voided obs
insert ignore into obs_subset
(select t1.* from amrs.obs t1 join voided_obs t2 using (encounter_id) where t1.voided=0 and t1.date_created <= @last_obs_date_created and t2.encounter_id is not null and t1.concept_id in (1285,1946,5096,1502,1777,1088,1255,1250,9082));

# add obs for encounters which have new obs
insert ignore into obs_subset
(select o.* from amrs.encounter e join amrs.obs o using (encounter_id) where o.date_created > @last_obs_date_created and o.voided=0 and e.date_created <= @last_enc_date_created and e.voided=0 and o.concept_id in (1285,1946,5096,1502,1777,1088,1255,1250,9082));


drop temporary table if exists n_obs;
create temporary table n_obs (index encounter_id (encounter_id))
(select 
	encounter_id,
	min(if(concept_id=1285,value_coded,null)) as transfer_care,
	min(if(concept_id=1946,value_coded,null)) as discontinue_is_hiv_neg,
	min(if(concept_id in (5096,1502,1777),value_datetime,null)) as rtc_date,
	group_concat(if(concept_id=1088,value_coded,null) order by value_coded separator ' // ') as arv_meds,
	min(if(concept_id=1255,value_coded,null)) as arv_plan,
	group_concat(if(concept_id=1250,value_coded,null) order by value_coded separator ' // ') as arv_meds_plan,
	min(if(concept_id=9082,value_coded,null)) as ampath_status,
	max(date_created) as obs_date_created
	from obs_subset
	where encounter_id is not null
	group by encounter_id 
);

drop table if exists person_ids;
create temporary table person_ids (person_id int, index person_id (person_id))
(select distinct person_id from enc);

drop table if exists person_info;
create temporary table person_info (index person_id (person_id))
(select p.person_id, p.gender, p.birthdate,p.dead, p.death_date
	from amrs.person p 
	join person_ids e using (person_id)
	where p.voided=0
	group by person_id
);

drop table if exists person_name;
create temporary table person_name (index person_id (person_id))
(select person_id, group_concat(concat(`given_name`,' ',`family_name`) separator ' / ') as person_name
	from amrs.person_name n 
	join person_ids e using (person_id)
	where n.voided =0
	group by person_id
);

drop table if exists person_phone_number;
create temporary table person_phone_number (index person_id (person_id))
(select person_id, group_concat(if(value != '',value,null) separator ' / ') as phone_number
	from amrs.person_attribute
	join person_ids e using (person_id)
	where voided=0 and person_attribute_type_id=10
	group by person_id

);
select @amrs_identifier := null;
select @univ_identifier := null;


drop table if exists person_identifier;
create temporary table person_identifier (index person_id (person_id))
(select person_id,if(univ_identifier,univ_identifier,amrs_identifier) as identifier
from
(select 
	patient_id as person_id, 
	min(if(identifier_type=3,identifier,null)) as amrs_identifier,
	min(if(identifier_type=8,identifier,null)) as univ_identifier
	from amrs.patient_identifier i
	join person_ids n on n.person_id = i.patient_id
	where voided=0
	group by n.person_id
)t1
);

drop table if exists person_hc;
create temporary table person_hc (index person_id (person_id))
(select person_id,if(value,value,null) as health_center
	from amrs.person_attribute
		join person_ids using (person_id)
	where person_attribute_type_id=7 and voided=0
);

drop table if exists person;
create temporary table person (index person_id (person_id))
(select * 
	from person_info p
	left outer join person_name n using (person_id)
	left outer join person_phone_number pn using (person_id)
	left outer join person_identifier pi using (person_id)
	left outer join person_hc hc using (person_id)
	group by person_id
);


drop table person_info;
drop table person_name;
drop table person_phone_number;
drop table person_identifier;

#remove any encounters that have a voided obs. 
drop table if exists encounters_to_be_removed;
create temporary table encounters_to_be_removed (primary key encounter_id (encounter_id))
(select distinct encounter_id from voided_obs);

#remove any encounters that have been voided.
insert ignore into encounters_to_be_removed
(select encounter_id from amrs.encounter where voided=1 and date_created <= @last_enc_date_created and date_voided > @last_enc_date_created and encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21));

# remove any encounters that will be (re)inserted
insert ignore into encounters_to_be_removed
(select encounter_id from enc);


delete t1
from flat_retention_defined t1
join encounters_to_be_removed t2 using (encounter_id);


insert into flat_retention_defined
(select *
from enc e1 
left outer join n_obs n1 using (encounter_id)
left outer join person p using (person_id)
order by e1.person_id, e1.encounter_datetime
);


#********************************************************************************************************
#* CREATION OF DERIVED TABLE ****************************************************************************
#********************************************************************************************************
drop table if exists new_person_data;
create temporary table new_person_data(primary key person_id (person_id))
(select distinct person_id from voided_obs);

#remove any encounters that have been voided.
insert ignore into new_person_data
(select patient_id as person_id from amrs.encounter where voided=1 and date_created <= @last_enc_date_created and date_voided > @last_enc_date_created);

#remove any encounters with new obs as the entire encounter will be rebuilt and added back
insert ignore into new_person_data
(select person_id from obs_subset);

insert ignore into new_person_data
(select person_id from enc);

# the derived table depends on lab values, if any lab values have been changed then we need to update these patients as well
insert ignore into new_person_data
(select distinct person_id from flat_lab_data where (enc_date_created > @last_enc_date_created or obs_date_created > @last_obs_date_created));

drop table voided_obs;


drop table if exists flat_retention_derived_1;
create temporary table flat_retention_derived_1(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
(select t1.*,
	if(t3.lab_hiv_test,t3.lab_hiv_test,t3.enc_hiv_test) as hiv_test, 
	if(t3.lab_hiv_dna_pcr,t3.lab_hiv_dna_pcr,t3.enc_hiv_dna_pcr) as hiv_dna_pcr, 
	t3.cd4_1, t3.cd4_1_date,
	if(t1.encounter_type=21,1,2) as encounter_type_sort_order
	from flat_retention_defined t1 
	join new_person_data t2 using(person_id)
	left outer join flat_lab_data t3 using (encounter_id)
);

select @prev_id := null;
select @cur_id := null;
select @prev_appt_date := null;
select @cur_appt_date := null;
select @next_encounter_type := null;
select @cur_encounter_type := null;
select @cur_clinic_date := null;
select @prev_clinic_date := null;
select @cur_ampath_status := null;

drop table if exists flat_retention_derived_2;
create temporary table flat_retention_derived_2 (next_appt_date datetime)
(select *,
@prev_id := @cur_id as prev_id, 
@cur_id := person_id as cur_id,

case
	when @prev_id = @cur_id then @prev_appt_date := @cur_appt_date
	else @prev_appt_date := null
end as next_appt_date,

@cur_appt_date := encounter_datetime as cur_appt_date,

case
	when @prev_id=@cur_id then @prev_clinic_date := @cur_clinic_date
	else @prev_clinic_date := null
end as next_hiv_clinic_date,

case
	when encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47) then @cur_clinic_date := encounter_datetime
	when @prev_id = @cur_id then @cur_clinic_date
	else @cur_clinic_date := null
end as cur_hiv_clinic_date,


case
	when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
	else @next_encounter_type := null
end as next_encounter_type,

@cur_encounter_type := encounter_type as cur_encounter_type,

case
	when @prev_id = @cur_id and @cur_ampath_status is not null then @cur_ampath_status
	when ampath_status then @cur_ampath_status := ampath_status
	when transfer_care=1287 then @cur_ampath_status := transfer_care
	when encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47)   then @cur_ampath_status := 6101
	else @cur_ampath_status := null
end as cur_ampath_status


from flat_retention_derived_1
order by person_id, encounter_datetime desc, encounter_type_sort_order desc
);

alter table flat_retention_derived_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_appt_date, drop cur_hiv_clinic_date;

select @prev_id := null;
select @cur_id := null;
select @prev_encounter_type := null;
select @cur_encounter_type := null;
select @prev_appt_date :=null;
select @cur_appt_date :=null;
select @prev_rtc_date :=null;
select @cur_rtc_date :=null;
select @hiv_start_date := null;
select @prev_arv_start_date := null;
select @arv_start_date := null;
select @prev_clinic_date := null;
select @cur_clinic_date := null;
select @prev_arv_line := null;
select @cur_arv_line := null;


drop table if exists flat_retention_derived_3;
create temporary table flat_retention_derived_3 (encounter_datetime datetime, person_id int, prev_appt_date datetime, cur_appt_date datetime, prev_hiv_clinic_date datetime, next_hiv_clinic_date datetime, prev_rtc_date datetime, cur_rtc_date datetime, prev_encounter_type tinyint, arv_start_date datetime, prev_arv_start_date datetime,hiv_start_date datetime, next_encounter_type int, cur_ampath_status int, prev_arv_line int, cur_arv_line int,index person_enc (person_id, encounter_datetime desc), index encounter_id (encounter_id))
(select *,
@prev_id := @cur_id as prev_id, 
@cur_id := person_id as cur_id,

case
	when @prev_id=@cur_id then @prev_clinic_date := @cur_clinic_date
	else @prev_clinic_date := null
end as prev_hiv_clinic_date,

case
	when encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47)  then @cur_clinic_date := encounter_datetime
	when @prev_id = @cur_id then @cur_clinic_date
	else @cur_clinic_date := null
end as cur_hiv_clinic_date,


case
	when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
	else @prev_encounter_type:=null
end as prev_encounter_type,

@cur_encounter_type := encounter_type as cur_encounter_type,

case
	when @prev_id=@cur_id then @prev_appt_date := @cur_appt_date
	else @prev_appt_date := null
end as prev_appt_date,
@cur_appt_date := encounter_datetime as cur_appt_date,

case
	when @prev_id=@cur_id then @prev_rtc_date := @cur_rtc_date
	else @prev_rtc_date := null
end as prev_rtc_date,
@cur_rtc_date := rtc_date as cur_rtc_date,

case
	when discontinue_is_hiv_neg=1065 then @hiv_start_date := null
	when @prev_id != @cur_id then
		case
			when hiv_test=703 or hiv_dna_pcr=703 or encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47)  or arv_meds or arv_meds_plan then @hiv_start_date := encounter_datetime
			else @hiv_start_date := null
		end
	else 
		case
			when hiv_test=664 or hiv_dna_pcr=664 then @hiv_start_date:=null
			when @hiv_start_date then @hiv_start_date
			when hiv_test=703 or encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47)  or arv_meds or arv_meds_plan then @hiv_start_date := encounter_datetime
			else @hiv_start_date := null
		end
end as hiv_start_date,

case
	when @prev_id = @cur_id then @prev_arv_start_date := @arv_start_date
	else @prev_arv_start_date := null
end as prev_arv_start_date,

case
	when arv_plan = 1256 then @arv_start_date := encounter_datetime
	when arv_plan in (1107,1260) then @arv_start_date := null
	when arv_meds_plan is not null and @prev_arv_start_date is null then @arv_start_date := encounter_datetime
	when arv_meds is not null and @prev_arv_start_date is null then @arv_start_date := encounter_datetime
	else @arv_start_date := @prev_arv_start_date
end as arv_start_date,

case
	when @prev_id = @cur_id then @prev_arv_line := @cur_arv_line
	else @prev_arv_line := null
end as prev_arv_line,

case
	when arv_plan in (1107,1260) then @cur_arv_line := null
	when arv_meds_plan regexp '[[:<:]]6467|6964|792|633|631[[:>:]]' then @cur_arv_line := 1
	when arv_meds_plan regexp '[[:<:]]794[[:>:]]' then @cur_arv_line := 2
	when arv_meds_plan regexp '[[:<:]]6156[[:>:]]' then @cur_arv_line := 3
	when arv_meds regexp '[[:<:]]6467|6964|792|633|631[[:>:]]' then @cur_arv_line := 1
	when arv_meds regexp '[[:<:]]794[[:>:]]' then @cur_arv_line := 2
	when arv_meds regexp '[[:<:]]6156[[:>:]]' then @cur_arv_line := 3
	when @cur_arv_line then @cur_arv_line
	else @cur_arv_line := null
end as cur_arv_line



from flat_retention_derived_2
order by person_id, encounter_datetime, encounter_type_sort_order
);


#select person_id, encounter_datetime, ampath_status,cast(cur_ampath_status as unsigned) as ampath_status_2, encounter_type from flat_retention_derived_3 order by person_id, encounter_datetime;

#alter table flat_retention_derived_3 drop prev_id, drop cur_id, drop cur_appt_date, drop cur_encounter_type, drop cur_rtc_date, drop prev_arv_start_date, drop prev_hiv_start_date, drop ampath_status, change cur_ampath_status ampath_status int;
create table if not exists flat_retention_derived
(encounter_id int,person_id int,prev_encounter_type int,prev_rtc_date datetime,prev_appt_date datetime,hiv_start_date datetime,arv_start_date datetime,next_encounter_type int,next_appt_date datetime, prev_hiv_clinic_date datetime, next_hiv_clinic_date datetime, cur_ampath_status int, prev_arv_line int, cur_arv_line int, index next_appt_date (next_appt_date),index encounter_id (encounter_id), index person_id (person_id));

delete t1
from flat_retention_derived t1
join new_person_data t3 using (person_id);

insert into flat_retention_derived
(select encounter_id,person_id,prev_encounter_type,prev_rtc_date,prev_appt_date,hiv_start_date,arv_start_date,next_encounter_type,next_appt_date, prev_hiv_clinic_date, next_hiv_clinic_date, cur_ampath_status,prev_arv_line, cur_arv_line
from flat_retention_derived_3);

drop view if exists flat_retention_data;
create view flat_retention_data as
(select * 
	from flat_retention_defined t1
	join flat_retention_derived t2 using (encounter_id,person_id)	
	order by person_id, encounter_datetime
);

select (count(*) - @init_count) as 'flat_retention_defined new rows' from flat_retention_defined;