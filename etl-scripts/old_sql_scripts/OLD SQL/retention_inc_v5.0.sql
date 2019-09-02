# HIV encounter_type_ids : (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) 
# HIV pmtct encounter_type_ids : (10,11,12,44,46,47)
# HIV adult visit : (1,2,14,17,19)
# HIV peds visit : (3,4,15,26)
# non-lab encounters : (1,2,3,4,10,11,12,16,17,19,25,27,28,29,32,33,34,37,43,47,49,40,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,73,75,76,78,79,80,83)
# lab encounters : (5,6,7,8,9,45,87)

# Build LTFU table

select @last_enc_date_created := max(enc_date_created) from flat_retention_data;
select @last_enc_date_created := if(@last_enc_date_created,@last_enc_date_created,'1900-01-01');

select @last_obs_date_created := max(obs_date_created) from flat_retention_data;
select @last_obs_date_created := if(@last_obs_date_created,@last_obs_date_created,'1900-01-01');

select @concept_ids := '1285 1946 5096 1502 1777 1088 1255 1250 9082';

drop table if exists voided_obs;
create table voided_obs (index encounter_id (encounter_id), index obs_id (obs_id))
(select person_id, encounter_id, obs_id, obs_datetime, date_voided, concept_id, date_created
from amrs.obs where voided=1 and date_voided > @last_obs_date_created and date_created <= @last_obs_date_created and concept_id in (1285,1946,5096,1502,1777,1088,1255,1250,9082) );

drop table if exists voided_obs;
create table voided_obs (index encounter_id (encounter_id), index obs_id (obs_id))
(select person_id, encounter_id, obs_id, obs_datetime, date_voided, concept_id, date_created
from amrs.obs where voided=1 and date_voided > @last_obs_date_created and date_created <= @last_obs_date_created and @concept_ids regexp concat('[[:<:]]',concept_id,'[[:>:]]')
);



drop temporary table if exists enc;
create temporary table enc (encounter_id int, encounter_datetime datetime, index encounter_id (encounter_id), index person_id (person_id))

(select encounter_id,encounter_datetime,provider_id,location_id,person_id,encounter_type,enc_date_created
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

# add in encounters which have new relevant obs attached to them
(select e.encounter_id, e.encounter_datetime, e.provider_id, e.patient_id as person_id, e.location_id,e.encounter_type, e.date_created as enc_date_created
from amrs.encounter e
join amrs.obs o using (encounter_id)
where o.date_created > @last_obs_date_created and o.voided=0 and e.date_created <= @last_enc_date_created and e.voided=0 and concept_id in (1285,1946,5096,1502,1777,1088,1255,1250,9082) 
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
			where concept_id in (1285,1946,5096,1502,1777,1088,1255,1250,9082)
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



drop temporary table if exists n_obs;
create temporary table n_obs (encounter_id int, rtc_date datetime, index encounter_id (encounter_id))
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


drop table if exists new_person_data;
create temporary table new_person_data(index person_id (person_id))
(select distinct person_id from
(
	(select distinct person_id from enc)
	union
	(select patient_id as person_id from amrs.encounter where date_created <= @last_enc_date_created and date_voided > @last_enc_date_created)
	union
	(select person_id from amrs.obs where date_created <= @last_obs_date_created and date_voided > @last_obs_date_created)
) t0);


drop table if exists person_info;
create temporary table person_info (index person_id (person_id))
(select p.person_id, p.gender, p.birthdate
	from amrs.person p 
	join new_person_data using (person_id)
	where p.voided=0
	group by person_id
);

drop table if exists person_name;
create temporary table person_name (index person_id (person_id))
(select person_id, group_concat(concat(`given_name`,' ',`family_name`) separator ' / ') as person_name
	from amrs.person_name n 
	join new_person_data using (person_id)
	where n.voided =0
	group by person_id
);

drop table if exists person_phone_number;
create temporary table person_phone_number (index person_id (person_id))
(select person_id, group_concat(if(value != '',value,null) separator ' / ') as phone_number
	from amrs.person_attribute
	join new_person_data using (person_id)
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
	join new_person_data n on n.person_id = i.patient_id
	where voided=0
	group by n.person_id
)t1
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


drop table if exists new_dataset;
create table new_dataset (index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
(select person_id,encounter_id,encounter_datetime,provider_id,location_id,encounter_type,enc_date_created,transfer_care,discontinue_is_hiv_neg,rtc_date,arv_meds,arv_plan,arv_meds_plan,ampath_status,obs_date_created,gender,birthdate,person_name,phone_number,identifier
from flat_retention_data t1 join new_person_data t2 using(person_id));


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
				where o.date_created > @last_obs_date_created and o.voided=0 and e.date_created <= @last_enc_date_created and concept_id in (1285,1946,5096,1502,1777,1088,1255,1250,9082) 
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
select * from new_dataset limit 1;
describe new_dataset
insert into new_dataset
(select *
from enc e1 
left outer join n_obs n1 using (encounter_id)
left outer join person p using (person_id)
order by e1.person_id, e1.encounter_datetime
);


# for those encounters with the same datetime, we need to make sure the encounters we are removing are ordered ahead of the encounters we are keeping
# we will set the encounter_id to a high number for lab encounters and then sort by encounter_id desc.
#update ltfu_1 set encounter_id = encounter_id + 10000000 where encounter_type in (5,6,7,8,9,45,87);

drop table if exists flat_retention_data_2;
create temporary table flat_retention_data_2
(select r.*,hiv_test,cd4_1,cd4_1_date,cd4_2,cd4_2_date,vl1,vl1_date,vl2,vl2_date,
if(encounter_type=21,1,2) as encounter_type_sort_order
from flat_retention_data_1 r
left outer join flat_lab_data l using (encounter_id)
);

select @prev_id := null;
select @cur_id := null;
select @prev_appt_date := null;
select @cur_appt_date := null;
select @next_encounter_type := null;
select @cur_encounter_type := null;

drop temporary table if exists flat_retention_data_3;
create temporary table flat_retention_data_3 (next_appt_date datetime)
(select *,
@prev_id := @cur_id as prev_id, 
@cur_id := person_id as cur_id,

case
	when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
	else @next_encounter_type := null
end as next_encounter_type,

@cur_encounter_type := encounter_type as cur_encounter_type,

case
	when @prev_id = @cur_id then @prev_appt_date := @cur_appt_date
	else @prev_appt_date := null
end as next_appt_date,

@cur_appt_date := encounter_datetime as cur_appt_date

from flat_retention_data_2
order by person_id, encounter_datetime desc, encounter_type_sort_order desc
);

alter table flat_retention_data_3 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_appt_date;

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


drop table if exists flat_retention_data_4;
create table flat_retention_data_4 (encounter_datetime datetime, person_id int, prev_appt_date datetime, cur_appt_date datetime, prev_rtc_date datetime, cur_rtc_date datetime, prev_encounter_type tinyint, arv_start_date datetime, prev_arv_start_date datetime,hiv_start_date datetime, index person_enc (person_id, encounter_datetime desc), index encounter_id (encounter_id))

(select *,
	@prev_id := @cur_id as prev_id, 
	@cur_id := person_id as cur_id,

	if(@prev_id = @cur_id,@prev_encounter_type := @cur_encounter_type, @prev_encounter_type:=encounter_type) as prev_encounter_type,
	@cur_encounter_type := encounter_type as cur_encounter_type,



	case
		when @prev_id=@cur_id then @prev_appt_date := @cur_appt_date
		else @prev_appt_date := null
	end as prev_appt_date,
	@cur_appt_date := encounter_datetime as cur_appt_date,
	
#	cast(if(@prev_id = @cur_id, if(@prev_encounter_type not in (5,6,7,8,9,45,87,999),@prev_appt_date := @cur_appt_date,@prev_appt_date), @prev_appt_date:=null) as datetime) as prev_appt_date,

	case
		when @prev_id=@cur_id then @prev_rtc_date := @cur_rtc_date
		else @prev_rtc_date := null
	end as prev_rtc_date,
	@cur_rtc_date := rtc_date as cur_rtc_date,

	case
		when @prev_id = @cur_id then @prev_hiv_start_date := @hiv_start_date
		else @prev_hiv_start_date := null
	end as prev_hiv_start_date,


	case		
		when @prev_id != @cur_id then
			case
				when hiv_test=703 or encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) or arv_meds or arv_meds_plan then @hiv_start_date := encounter_datetime
				else @hiv_start_date := null
			end
		else 
			case
				when hiv_test=703 or encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) or arv_meds or arv_meds_plan then @hiv_start_date := encounter_datetime
				else @hiv_start_date := null
			end
	end as hiv_start_date,

/*
	if(@prev_id = @cur_id,
		if((encounter_datetime < @prev_hiv_start_date or @prev_hiv_start_date is null) and (hiv_test=703 or encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) or arv_meds or arv_meds_plan), @hiv_start_date := encounter_datetime,@prev_hiv_start_date),
		if(hiv_test=703 or encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) or arv_meds or arv_meds_plan,@hiv_start_date := encounter_datetime, @hiv_start_date:=null)) as hiv_start_date,
*/
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
		when ampath_status then @cur_ampath_status := ampath_status
		when transfer_care=1287 then @cur_ampath_status := transfer_care
		when @prev_id = @cur_id then @cur_ampath_status
		else @cur_ampath_status := null
	end as cur_ampath_status

	from flat_retention_data_4
	order by person_id, encounter_datetime, encounter_type_sort_order
);


alter table flat_retention_data_4 drop prev_id, drop cur_id, drop cur_appt_date, drop cur_encounter_type, drop cur_rtc_date, drop prev_arv_start_date, drop prev_hiv_start_date, drop ampath_status, change cur_ampath_status ampath_status int;
delete t1
from flat_retention_data t1
join new_person_data t3 using (person_id);
# add the new dataset into lost_to_follow_up
insert into flat_retention_data
(select * from flat_retention_data_4 order by person_id, encounter_datetime, encounter_id);



select timestampdiff(minute,@start_time,now()) as 'time to get dataset (min)';