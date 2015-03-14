#create temporary table concept_ids (concept_id int, index concept_id (concept_id));
#insert into concept_ids (concept_id) values 
#(1285),(1596),(2051),(1061),(8307),(8282),(5356),(1224),(1251),(1252),(5089),(5090),(1570),(1109),(1261),(6174),(1110),(1265),(1111),(1268),(1270),(5272),(1836),(1279),(1855),(5596),(1146),(1088),(1255),(1250),(1252),(1499),(2158)
#;

create table if not exists flat_hiv_defined
(person_id int, encounter_id int,encounter_datetime datetime,provider_id int,location_id mediumint,encounter_type mediumint,enc_date_created datetime, 
transfer int,
point_of_entry int,
method_of_hiv_exposure int,
who_stage_prior_to_ampath int,
who_stage int,
weight int,
height int,
death_date_obs datetime,
on_pcp_prophylaxis int,
pcp_prophylaxis_plan int,
on_crypto_prophylaxis int,
crypto_prophylaxis_plan int,
tb_screening_questions varchar(1000),
on_tb_prophylaxis int,
tb_prophylaxis_plan int,
on_tb_treatment int,
tb_treatment_plan int,
tb_treatment_new_drugs int,
is_pregnant int,
lmp datetime,
weeks_pregnant int,
fundal_height int,
pregnancy_edd datetime,
pregnancy_delivery_date datetime,
delivered_since_last_visit int,
arv_meds varchar(1000),
arv_plan int,
arv_meds_plan varchar(1000),
reason_for_arv_change int,
arv_start_date datetime,
reason_previously_started_on_arvs int,
obs_date_created datetime,
uuid varchar(500), 
gender varchar(10),
birth_date datetime,
dead tinyint, 
death_date datetime, 
identifier varchar(50), 
health_center int, 
index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime), index enc_date_created (enc_date_created), index obs_date_created (obs_date_created));

select @last_enc_date_created := max(enc_date_created) from flat_hiv_defined;
select @last_enc_date_created := if(@last_enc_date_created,@last_enc_date_created,'1900-01-01');
select @last_enc_date_created := '2014-08-29';


select @last_obs_date_created := max(obs_date_created) from flat_hiv_defined;
select @last_obs_date_created := if(@last_obs_date_created,@last_obs_date_created,'1900-01-01');
select @last_obs_date_created := '2014-08-29';

# concept_ids: 1285,1596,2051,1061,8307,8282,5356,1224,1251,1252,5089,5090,1570,1109,1261,6174,1110,1265,1111,1268,1270,5272,1836,1279,1855,5596,1146,1088,1255,1250,1252,1499,2158

drop table if exists voided_obs;
create table voided_obs (index encounter_id (encounter_id), index obs_id (obs_id))
(select person_id, encounter_id, obs_id, obs_datetime, date_voided, concept_id, date_created
from amrs.obs where voided=1 and date_voided > @last_obs_date_created and date_created <= @last_obs_date_created and concept_id in (1285,1596,2051,1061,8307,8282,5356,1224,1251,1252,5089,5090,1570,1109,1261,6174,1110,1265,1111,1268,1270,5272,1836,1279,1855,5596,1146,1088,1255,1250,1252,1499,2158));

drop temporary table if exists enc;
create temporary table enc (encounter_id int, encounter_datetime datetime, primary key encounter_id (encounter_id), index person_id (person_id))
(select e.patient_id as person_id, e.encounter_id, e.encounter_datetime, t2.provider_id, e.location_id,e.encounter_type, e.date_created as enc_date_created
from amrs.encounter e 
join amrs.encounter_provider t2 using (encounter_id)
where e.voided=0
and e.date_created > @last_enc_date_created
and encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21) 
);


insert ignore into enc
(select e.patient_id as person_id, e.encounter_id, e.encounter_datetime, t2.provider_id, e.location_id,e.encounter_type, e.date_created as enc_date_created
from amrs.encounter e
join amrs.encounter_provider t2 using (encounter_id)
join voided_obs v using (encounter_id)
where e.date_created <= @last_enc_date_created and e.voided=0 and v.encounter_id is not null and e.encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21) 
);

# add in encounters which have new relevant obs attached to them
insert ignore into enc
(select e.patient_id as person_id, e.encounter_id, e.encounter_datetime, t2.provider_id, e.location_id,e.encounter_type, e.date_created as enc_date_created
from amrs.encounter e
join amrs.encounter_provider t2 using (encounter_id)
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
(select * from amrs.obs o use index (date_Created) where concept_id in (1285,1596,2051,1061,8307,8282,5356,1224,1251,1252,5089,5090,1570,1109,1261,6174,1110,1265,1111,1268,1270,5272,1836,1279,1855,5596,1146,1088,1255,1250,1252,1499,2158) and o.voided=0 and date_created > @last_obs_date_created);

# add obs of encounters with voided obs
insert ignore into obs_subset
(select t1.* from amrs.obs t1 join voided_obs t2 using (encounter_id) where t1.voided=0 and t1.date_created <= @last_obs_date_created and t2.encounter_id is not null and t1.concept_id in (1285,1596,2051,1061,8307,8282,5356,1224,1251,1252,5089,5090,1570,1109,1261,6174,1110,1265,1111,1268,1270,5272,1836,1279,1855,5596,1146,1088,1255,1250,1252,1499,2158));

# add obs for encounters which have new obs
insert ignore into obs_subset
(select o.* from amrs.encounter e join amrs.obs o use index (date_created) using (encounter_id) where o.date_created > @last_obs_date_created and o.voided=0 and e.date_created <= @last_enc_date_created and e.voided=0 and o.concept_id in (1285,1596,2051,1061,8307,8282,5356,1224,1251,1252,5089,5090,1570,5497,730,856,1040,1042,1109,1261,6174,1110,1265,1111,1268,1270,5272,1836,1279,1855,5596,1146,1088,1255,1250,1252,1499,2158));


drop temporary table if exists n_obs;
create temporary table n_obs (index encounter_id (encounter_id))
(select
	encounter_id,
	max(if(concept_id=1285,value_coded,null)) as transfer,
	min(if(concept_id=2051,value_coded,null)) as point_of_entry,
	min(if(concept_id=1061,value_coded,null)) as method_of_hiv_exposure,

	min(if(concept_id in (8307,8282),value_coded,null)) as who_stage_prior_to_ampath,
	min(if(concept_id in (5356,1224),value_coded,null)) as who_stage,

	min(if(concept_id=5089,value_numeric,null)) as weight,
	min(if(concept_id=5090,value_numeric,null)) as height,
	min(if(concept_id=1570,value_datetime,null)) as death_date_obs,

	min(if(concept_id=1109,value_coded,null)) as on_pcp_prophylaxis,
	min(if(concept_id=1261,value_coded,null)) as pcp_prophylaxis_plan,

	min(if(concept_id=1110,value_coded,null)) as on_crypto_prophylaxis,
	min(if(concept_id=1277,value_coded,null)) as crpyto_prophylaxis_plan,
	group_concat(if(concept_id=6174,value_coded,null) order by value_coded separator ' // ') as tb_screening_questions,
	min(if(concept_id=1110,value_coded,null)) as on_tb_prophylaxis,
	min(if(concept_id=1265,value_coded,null)) as tb_prophylaxis_plan,
	min(if(concept_id=1111,value_coded,null)) as on_tb_treatment,
	min(if(concept_id=1268,value_coded,null)) as tb_treatment_plan,
	min(if(concept_id=1270,value_coded,null)) as tb_treatment_new_drugs,

	min(if(concept_id=5272,value_coded,null)) as is_pregnant,
	min(if(concept_id=1836,value_datetime,null)) as lmp,
	min(if(concept_id=1279,value_numeric,null)) as weeks_pregnant,
	min(if(concept_id=1855,value_numeric,null)) as fundal_height,
	min(if(concept_id=5596,value_datetime,null)) as pregnancy_edd,
	min(if(concept_id=5599,value_datetime,null)) as pregnancy_delivery_date,
	min(if(concept_id=1146,value_coded,null)) as delivered_since_last_visit,

	group_concat(if(concept_id=1088,value_coded,null) order by value_coded separator ' // ') as arv_meds,
	min(if(concept_id=1255,value_coded,null)) as arv_plan,
	group_concat(if(concept_id=1250,value_coded,null) order by value_coded separator ' // ') as arv_meds_plan,
	min(if(concept_id=1252,value_coded,null)) as reason_for_arv_change,
	min(if(concept_id=1499,value_datetime,null)) as arv_start_date,
	min(if(concept_id=2158,value_coded,null)) as reason_previously_started_on_arvs,
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
(select p.person_id, p.uuid, p.gender, p.birthdate,p.dead, p.death_date
	from amrs.person p 
	join person_ids e using (person_id)
	where p.voided=0
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
	left outer join person_identifier pi using (person_id)
	left outer join person_hc hc using (person_id)
	group by person_id
);


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
from flat_hiv_defined t1
join encounters_to_be_removed t2 using (encounter_id);


insert into flat_hiv_defined
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
(select distinct person_id from flat_lab_data where (enc_date_created > @last_enc_date_created));

drop table voided_obs;


drop table if exists flat_hiv_derived_1;
create temporary table flat_hiv_derived_1(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
(select t1.*,
	if(t3.lab_hiv_test,t3.lab_hiv_test,t3.enc_hiv_test) as hiv_test, 
	if(t3.lab_hiv_dna_pcr,t3.lab_hiv_dna_pcr,t3.enc_hiv_dna_pcr) as hiv_dna_pcr, 
	t3.cd4_1, t3.cd4_1_date,
	if(t1.encounter_type=21,1,2) as encounter_type_sort_order
	from flat_hiv_defined t1 
	join new_person_data t2 using(person_id)
	left outer join flat_lab_data t3 using (encounter_id)
);


select @prev_id := null;
select @cur_id := null;
select @prev_encounter_type := null;
select @cur_encounter_type := null;

select @prev_arv_start_date := null;
select @arv_start_date := null;

select @prev_arv_line:= null;
select @cur_arv_line := null;

select @arv_eligibility_reason  := null;

select @prev_who_stage := null;
select @who_stage := null;
select @arv_start_who_stage := null;

select @arv_start_cd4_count := null;

select @height  := null;
select @prev_height  := null;
select @arv_start_height  := null;

select @prev_weight  := null;
select @weight  := null;
select @arv_start_weight  := null;

select @pcp_prophylaxis_start_date := null;
select @inh_prophylaxis_start_date := null;
	
select @first_evidence_pt_pregnant := null;
select @edd := null;
select @preg_1 := null;
select @preg_2 := null;
select @preg_3 := null;

select @cur_arv_meds := null;
select @original_regimen := null;
select @first_substitution_date := null;
select @first_substitution := null;
select @first_substitution_reason := null;
select @second_substitution_date := null;
select @second_substitution := null;
select @second_substitution_reason := null;
select @second_line := null;
select @sl_first_substitution_date := null;
select @sl_first_substitution := null;
select @sl_first_substitution_reason := null;
select @sl_second_substitution_date := null;
select @sl_second_substitution := null;
select @sl_second_substitution_reason := null;
select @adult_age := 12;
select @adult_fl := 'A01A A01C AF1A AF1B AF2A AF2B AF3A AF3B';
select @adult_sl := 'AS1A AS1B AS1C AS2A AS2B AS2C AS2D AS2E AS3A AS4A AS4B';
select @child_fl := 'CF1A CF1B CF1C CF2A CF2B CF2C CF2D CF3A CF3B CO1A CO1B';
select @child_sl := 'CS1A CS1B CS1C CS2A CS2B CS3A CS3B';

drop temporary table if exists flat_hiv_derived_2;
create temporary table flat_hiv_derived_2 (arv_start_date datetime, who_stage integer, arv_start_cd4_count integer, arv_start_height int, arv_start_weight int, arv_eligibility_reason varchar(500), arv_start_who_stage int, pcp_prophylaxis_start_date datetime, tb_prophylaxis_start_date datetime, tb_treatment_start_date datetime, first_evidence_patient_pregnant datetime, edd datetime, preg_1 datetime, preg_2 datetime, preg_3 datetime, height int, weight int, cur_arv_meds varchar(500), first_substitution_date datetime, first_substitution varchar(500), first_substitution_reason int, second_substitution_date datetime, second_substitution varchar(500), second_substitution_reason integer,second_line varchar(100), second_line_date datetime, second_line_reason integer,sl_first_substitution_date datetime, sl_first_substitution varchar(10), sl_first_substitution_reason integer, sl_second_substitution_date datetime, sl_second_substitution varchar(10), sl_second_substitution_reason integer, index person_enc (person_id, encounter_datetime desc))
(select
person_id,
encounter_id,
encounter_datetime,
encounter_type,
birth_date,
@prev_id := @cur_id as prev_id, 
@cur_id := person_id as cur_id,

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
end as cur_arv_line,


case
	when who_stage then @who_stage := who_stage
	when @prev_id=@cur_id then @who_stage
	else @who_stage := null
end as who_stage,

cd4_1,cd4_1_date,

case 
	when @arv_start_date then
		case
			when @prev_id != @cur_id then @arv_start_who_stage := @who_stage
			when @arv_start_who_stage is null then @arv_start_who_stage := @who_stage
			else @arv_start_who_stage
		end
	else @arv_start_who_stage := null
end as arv_start_who_stage,

case 
	when @arv_start_date and @arv_start_cd4_count is null then @arv_start_cd4_count := cd4_1
	else @arv_start_cd4_count
end as arv_start_cd4_count,


case
	when height then @height := height
	when @prev_id=@cur_id then @height
	else @height := null
end as height,

case 
	when @arv_start_date then
		case
			when @prev_id != @cur_id then @arv_start_height := @height
			when @arv_start_height is null then @arv_start_height := @height
			else @arv_start_height
		end
	else @arv_start_height := null
end as arv_start_height,


case
	when weight then @weight := weight
	when @prev_id=@cur_id then @weight
	else @weight := null
end as weight,

case 
	when @arv_start_date then
		case
			when @prev_id != @cur_id then @arv_start_weight := @weight
			when @arv_start_weight is null then @arv_start_weight := @weight
			else @arv_start_weight
		end
	else @arv_start_weight := null
end as arv_start_weight,


case
	when @arv_start_date then
		case
			when @arv_eligibility_reason then @arv_eligibility_reason
			when timestampdiff(month,birth_date,@arv_start_date) <=18 then @arv_eligibility_reason := 'peds | pcr positive'
			when timestampdiff(month,birth_date,@arv_start_date) between 19 and 60 and @arv_start_cd4_percent < 25 then @arv_eligibility_reason := concat('cd4 % | ',@arv_start_cd4_percent)
			when timestampdiff(month,birth_date,@arv_start_date) > 60 and @arv_start_cd4_percent < 20 then @arv_eligibility_reason := concat('cd4 % | ',@arv_start_cd4_percent)
			when is_pregnant then @arv_eligibility_reason := 'pregnant'
			when @arv_start_who_stage in (1207,1206) and @arv_start_cd4_count <= 500 then @arv_eligibility_reason := concat(@arv_start_who_stage,' | ',@arv_start_cd4_count)
			when @arv_start_who_stage in (1207,1206) then @arv_eligibility_reason := concat('clinical | ',@arv_start_who_stage)
			when @arv_start_cd4_count <= 500 then @arv_eligibility_reason := concat('cd4 | ',@arv_start_cd4_count)
			else @arv_eligibility_reason := 'clinical | unknown'
		end
	else @arv_eligibility_reason := null
end as arv_eligibility_reason,

case 
	when pcp_prophylaxis_plan in (1256,1850) then @pcp_prophylaxis_start_date := encounter_datetime
	when pcp_prophylaxis_plan=1257 then
		case
			when @prev_id!=@cur_id or @pcp_prophylaxis_start_date is null then @pcp_prophylaxis_start_date := encounter_datetime
			else @pcp_prophylaxis_start_date
		end
	when on_pcp_prophylaxis in (916,92) and @pcp_prophylaxis_start_date is null then @pcp_prophylaxis_start_date := encounter_datetime
	when @prev_id=@cur_id then @pcp_prophylaxis_start_date
	else @pcp_prophylaxis_start_date := null
end as pcp_prophylaxis_start_date,


case 
	when tb_prophylaxis_plan in (1256,1850) then @tb_prophylaxis_start_date := encounter_datetime
	when tb_prophylaxis_plan=1257 then
		case
			when @prev_id!=@cur_id or @tb_prophylaxis_start_date is null then @tb_prophylaxis_start_date := encounter_datetime
			else @tb_prophylaxis_start_date
		end
	when on_tb_prophylaxis=656 and @tb_prophylaxis_start_date is null then @tb_prophylaxis_start_date := encounter_datetime
	when @prev_id=@cur_id then @tb_prophylaxis_start_date
	else @tb_prophylaxis_start_date := null
end as tb_prophylaxis_start_date,


case 
	when tb_treatment_plan in (1256) then @tb_treatment_start_date := encounter_datetime
	when tb_treatment_plan in (1257,1259,1849,981) then
		case
			when @prev_id!=@cur_id or @tb_treatment_start_date is null then @tb_treatment_start_date := encounter_datetime
			else @tb_treatment_start_date
		end
	when (on_tb_treatment!=1267 or tb_treatment_new_drugs!=1267) and @tb_treatment_start_date is null then @tb_treatment_start_date := encounter_datetime
	when @prev_id=@cur_id then @tb_treatment_start_date
	else @tb_treatment_start_date := null
end as tb_treatment_start_date,

case
	when @prev_id != @cur_id then
		case
			when encounter_type in (32,33,44,10) /*or is_pregnant=1065*/ or weeks_pregnant or pregnancy_edd then @first_evidence_pt_pregnant := encounter_datetime
			else @first_evidence_pt_pregnant := null
		end
	when @first_evidence_pt_pregnant is null and (encounter_type in (32,33,44,10) /*or is_pregnant=1065*/ or weeks_pregnant or pregnancy_edd) then @first_evidence_pt_pregnant := encounter_datetime
	when @first_evidence_pt_pregnant and (encounter_type in (11,47,34) or timestampdiff(week,@first_evidence_pt_pregnant,encounter_datetime) > 40 or timestampdiff(week,@edd,encounter_datetime) > 4 or pregnancy_delivery_date or delivered_since_last_visit=1065) then @first_evidence_pt_pregnant := null
	else @first_evidence_pt_pregnant
end as first_evidence_patient_pregnant,

case
	when @prev_id != @cur_id then
		case
			when @first_evidence_patient_pregnant and lmp then @edd := date_add(lmp,interval 280 day)
			when weeks_pregnant then @edd := date_add(encounter_datetime,interval (40-weeks_pregnant) week)
			when pregnancy_edd then @edd := pregnancy_edd
			when @first_evidence_pt_pregnant then @edd := date_add(@first_evidence_pt_pregnant,interval 6 month)
			else @edd := null
		end
	when @edd is null or @edd = @first_evidence_pt_pregnant then
		case
			when @first_evidence_pt_pregnant and lmp then @edd := date_add(lmp,interval 280 day)
			when weeks_pregnant then @edd := date_add(encounter_datetime,interval (40-weeks_pregnant) week)
			when pregnancy_edd then @edd := pregnancy_edd
			when @first_evidence_pt_pregnant then @edd := date_add(@first_evidence_pt_pregnant,interval 6 month)
			else @edd
		end
	when @edd and (encounter_type in (11,47,34) or timestampdiff(week,@edd,encounter_datetime) > 4 or pregnancy_delivery_date or delivered_since_last_visit=1065) then @edd := null
	else @edd	
end as edd,		
				
case
	when @prev_id=@cur_id and @preg_2 and @edd is not null and @edd != @preg_1 then @preg_3 := @preg_2
	when @prev_id=@cur_id then @preg_3
	else @preg_3 := null
end as preg_3,

case
	when @prev_id=@cur_id and @edd is not null and @edd != @preg_1 then @preg_2 := @preg_1
	when @prev_id=@cur_id then @preg_2
	else @preg_2 := null
end as preg_2,

case
	when @edd is not null and (@preg_1 is null or @edd != @preg_1) then @preg_1 := @edd
	when @prev_id = @cur_id then @preg_1
	else @preg_1 := null
end as preg_1,

case
	when arv_plan in (1107,1260) then @cur_arv_meds := null
	when arv_meds_plan then @cur_arv_meds := arv_meds_plan
	when arv_meds then @cur_arv_meds := arv_meds
	when @prev_id!=@cur_id then @cur_arv_meds := null
	else @cur_arv_meds
end as cur_arv_meds,

case 
	when timestampdiff(year,birth_date,encounter_datetime) >= @adult_age then
		case
			when @cur_arv_meds in ('628 // 631 // 814','792 // 6679') then @cur_arv_meds_code := 'A01A'
			when @cur_arv_meds = '794 // 6679' then @cur_arv_meds_code := 'A01C'
			when @cur_arv_meds in ('6467','628 // 631 // 797','628 // 631 // 802')  then @cur_arv_meds_code := 'AF1A'
			when @cur_arv_meds in ('628 // 633 // 797','630 // 633')  then @cur_arv_meds_code := 'AF1B'
			when @cur_arv_meds in ('628 // 631 // 802','631 // 1400')  then @cur_arv_meds_code := 'AF2A'
			when @cur_arv_meds in ('6964','1400 // 6964','628 // 633 //802') then @cur_arv_meds_code := 'AF2B'
			when @cur_arv_meds in ('792','625 // 628 // 631','631 // 6965') then @cur_arv_meds_code := 'AF3A'
			when @cur_arv_meds in ('625 // 628 // 633','633 // 6965') then @cur_arv_meds_code := 'AF3B'
			when @cur_arv_meds in ('628 // 794 // 797','630 // 794') then @cur_arv_meds_code := 'AS1A'
			when @cur_arv_meds in ('794 // 796 // 797') then @cur_arv_meds_code := 'AS1B'
			when @cur_arv_meds in ('817','628 // 814 // 797') then @cur_arv_meds_code := 'AS1C'
			when @cur_arv_meds in ('628 // 794 // 802','794 // 1400') then @cur_arv_meds_code := 'AS2A'
			when @cur_arv_meds in ('628 // 802 // 814','802 // 6679','814 // 1400') then @cur_arv_meds_code := 'AS2B'
			when @cur_arv_meds in ('628 // 797 // 802','797 // 1400') then @cur_arv_meds_code := 'AS2C'
			when @cur_arv_meds in ('794 // 802 // 814') then @cur_arv_meds_code := 'AS2D'
			when @cur_arv_meds in ('794 // 797 // 802') then @cur_arv_meds_code := 'AS2E'
			when @cur_arv_meds in ('794 // 796 // 814') then @cur_arv_meds_code := 'AS3A'
			when @cur_arv_meds in ('625 // 628 // 794','794 // 6965') then @cur_arv_meds_code := 'AS4A'
			when @cur_arv_meds in ('625 // 628 // 814','814 // 6965') then @cur_arv_meds_code := 'AS4B'
			when @cur_arv_meds then @cur_arv_meds_code := 'OTHER'
			else @cur_arv_meds_code := null
		end
	else
		case 
			when @cur_arv_meds in ('6467','628 // 631 // 797','628 // 631 // 802')  then @cur_arv_meds_code := 'CF1A'
			when @cur_arv_meds in ('628 // 633 // 797','630 // 633')  then @cur_arv_meds_code := 'CF1B'
			when @cur_arv_meds in ('817','628 // 814 // 797') then @cur_arv_meds_code := if(@original_regimen is null,'CF1C','CS1A')
			when @cur_arv_meds in ('628 // 631 // 814','792 // 6679') then @cur_arv_meds_code := 'CF2A'
			when @cur_arv_meds = '794 // 6679' then @cur_arv_meds_code := if(@original_regimen is null,'CF2D','CS2A')
			when @cur_arv_meds in ('792','625 // 628 // 631','631 // 6965') then @cur_arv_meds_code := 'CF3A'
			when @cur_arv_meds in ('625 // 628 // 633','633 // 6965') then @cur_arv_meds_code := 'CF3B'
			when @cur_arv_meds in ('628 // 794 // 797','630 // 794') then @cur_arv_meds_code := 'CS1A'
			when @cur_arv_meds in ('794 // 796 // 797') then @cur_arv_meds_code := 'CS1C'
			when @cur_arv_meds in ('794 // 796 // 814') then @cur_arv_meds_code := 'CS2B'
			when @cur_arv_meds in ('625 // 628 // 794','794 // 6965') then @cur_arv_meds_code := 'CS3A'
			when @cur_arv_meds then @cur_arv_meds_code := 'OTHER'
			else @cur_arv_meds_code := null
		end	
end as cur_arv_meds_code,

case
	when @original_regimen is null and @cur_arv_meds is not null then @original_regimen := @cur_arv_meds_code
	when @prev_id = @cur_id then @original_regimen
	else @original_regimen := null
end as original_regimen,



case
	when timestampdiff(year,birth_date,encounter_datetime) >= @adult_age and @original_regimen != @cur_arv_meds_code and @cur_arv_meds_code is not null and @first_substitution is null and @adult_fl like concat('%',@cur_arv_meds_code,'%') then @first_substitution_date := encounter_datetime
	when timestampdiff(year,birth_date,encounter_datetime) < @adult_age and @original_regimen != @cur_arv_meds_code and @cur_arv_meds_code is not null and @first_substitution is null and @child_fl like concat('%',@cur_arv_meds_code,'%') then @first_substitution_date := encounter_datetime
	when @prev_id = @cur_id then @first_substitution_date
	else @first_substitution_date := null
end as first_substitution_date,


case
	when timestampdiff(year,birth_date,encounter_datetime) >= @adult_age and @original_regimen != @cur_arv_meds_code and @first_substitution is null and @adult_fl like concat('%',@cur_arv_meds_code,'%') then @first_substitution := @cur_arv_meds_code
	when timestampdiff(year,birth_date,encounter_datetime) < @adult_age and @original_regimen != @cur_arv_meds_code and @first_substitution is null and @child_fl like concat('%',@cur_arv_meds_code,'%') then @first_substitution := @cur_arv_meds_code
	when @prev_id = @cur_id then @first_substitution
	else @first_substitution := null
end as first_substitution,

case
	when @first_substitution is not null and reason_for_arv_change then @first_substitution_reason := reason_for_arv_change
	when @prev_id = @cur_id then @first_substitution_reason
	else @first_substitution_reason := null
end as first_substitution_reason,


case
	when @first_substitution is not null and @cur_arv_meds_code != @first_substitution and @cur_arv_meds_code != @original_regimen and @adult_fl like concat('%',@cur_arv_meds_code,'%') then @second_substitution_date := encounter_datetime
	when @first_substitution is not null and @cur_arv_meds_code != @first_substitution and @cur_arv_meds_code != @original_regimen and @child_fl like concat('%',@cur_arv_meds_code,'%')then @second_substitution_date := encounter_datetime
	when @prev_id = @cur_id then @second_substitution_date
	else @second_substitution_date := null
end as second_substitution_date,

case
	when @first_substitution is not null and @cur_arv_meds_code != @first_substitution and @cur_arv_meds_code != @original_regimen and @adult_fl like concat('%',@cur_arv_meds_code,'%') then @second_substitution := @cur_arv_meds_code
	when @first_substitution is not null and @cur_arv_meds_code != @first_substitution and @cur_arv_meds_code != @original_regimen and @child_fl like concat('%',@cur_arv_meds_code,'%') then @second_substitution := @cur_arv_meds_code
	when @prev_id = @cur_id then @second_substitution
	else @second_substitution := null
end as second_substitution,


case
	when @second_substitution is not null and reason_for_arv_change then @second_substitution_reason := reason_for_arv_change
	when @prev_id = @cur_id then @second_substitution_reason
	else @second_substitution_reason := null
end as second_substitution_reason,

case
	when @cur_arv_meds_code != @original_regimen and @sl_first_substitution is null and @sl_second_substitution is null and @adult_sl like concat('%',@cur_arv_meds_code,'%') then @second_line_date := encounter_datetime
	when @cur_arv_meds_code != @original_regimen and @sl_first_substitution is null and @sl_second_substitution is null and @child_sl like concat('%',@cur_arv_meds_code,'%') then @second_line_date := encounter_datetime
	when @prev_id = @cur_id then @second_line_date
	else @second_line_date := null
end as second_line_date,

case
	when @cur_arv_meds != @original_regimen and @sl_first_substitution is null and @sl_second_substitution is null and @adult_sl like concat('%',@cur_arv_meds_code,'%') then @second_line := @cur_arv_meds_code
	when @cur_arv_meds != @original_regimen and @sl_first_substitution is null and @sl_second_substitution is null and @child_sl like concat('%',@cur_arv_meds_code,'%') then @second_line := @cur_arv_meds_code
	when @prev_id = @cur_id then @second_line
	else @second_line := null
end as second_line,

case
	when @second_line is not null and reason_for_arv_change then @second_line_reason := reason_for_arv_change
	when @prev_id = @cur_id then @second_line_reason
	else @second_line_reason := null
end as second_line_reason,


case
	when @second_line is not null and @cur_arv_meds is not null and @second_line != @cur_arv_meds and @sl_first_substitution is null and @adult_sl like concat('%',@cur_arv_meds_code,'%') then @sl_first_substitution_date := encounter_datetime
	when @second_line is not null and @cur_arv_meds is not null and @second_line != @cur_arv_meds and @sl_first_substitution is null and @child_sl like concat('%',@cur_arv_meds_code,'%') then @sl_first_substitution_date := encounter_datetime
	when @prev_id = @cur_id then @sl_first_substition_date
	else @sl_first_substitution_date := null
end as sl_first_substitution_date,

case
	when @second_line is not null and @cur_arv_meds is not null and @second_line != @cur_arv_meds and @sl_first_substitution is null and @adult_sl like concat('%',@cur_arv_meds_code,'%') then @sl_first_substitution := @cur_arv_meds_code
	when @second_line is not null and @cur_arv_meds is not null and @second_line != @cur_arv_meds and @sl_first_substitution is null and @child_sl like concat('%',@cur_arv_meds_code,'%') then @sl_first_substitution := @cur_arv_meds_code
	when @prev_id = @cur_id then @sl_first_substitution
	else @sl_first_substitution := null
end as sl_first_substitution,

case
	when @sl_first_substitution is not null and reason_for_arv_change then @sl_first_substitution_reason := reason_for_arv_change
	when @prev_id = @cur_id then @sl_first_substitution_reason
	else @sl_first_substitution_reason := null
end as sl_first_substitution_reason,


case
	when @sl_first_substitution is not null and @cur_arv_meds != @sl_first_substitution and @adult_sl like concat('%',@cur_arv_meds_code,'%') then @second_substitution_date := encounter_datetime
	when @sl_first_substitution is not null and @cur_arv_meds != @sl_first_substitution and @child_sl like concat('%',@cur_arv_meds_code,'%') then @second_substitution_date := encounter_datetime
	when @prev_id = @cur_id then @sl_second_substitution_date
	else @sl_second_substitution_date := null
end as sl_second_substitution_date,

case
	when @sl_first_substitution is not null and @cur_arv_meds != @sl_first_substitution and @adult_sl like concat('%',@cur_arv_meds_code,'%') then @second_substitution := @cur_arv_meds_code
	when @sl_first_substitution is not null and @cur_arv_meds != @sl_first_substitution and @child_sl like concat('%',@cur_arv_meds_code,'%') then @second_substitution := @cur_arv_meds_code
	when @prev_id = @cur_id then @sl_second_substitution
	else @sl_second_substitution := null
end as sl_second_substitution,

case
	when @sl_second_substitution is not null and reason_for_arv_change then @sl_second_substitution_reason := reason_for_arv_change
	when @prev_id = @cur_id then @sl_second_substitution_reason
	else @sl_second_substitution_reason := null
end as sl_second_substitution_reason

from flat_hiv_derived_1
order by person_id, encounter_datetime
);

create table if not exists flat_hiv_derived (encounter_id int, person_id int, arv_start_date_derived datetime, prev_arv_line int, cur_arv_line int, who_stage_derived integer, arv_start_cd4_count integer, arv_start_height double, arv_start_weight double, arv_eligibility_reason varchar(500), arv_start_who_stage integer, pcp_prophylaxis_start_date datetime, tb_prophylaxis_start_date datetime, tb_treatment_start_date datetime, first_evidence_patient_pregnant datetime, edd datetime, preg_1 datetime, preg_2 datetime, preg_3 datetime, cur_arv_meds varchar(100), first_substitution_date datetime, first_substitution text, first_substitution_reason integer, second_substitution_date datetime, second_substitution varchar(10), second_substitution_reason integer,second_line varchar(10), second_line_date datetime, second_line_reason integer,sl_first_substitution_date datetime, sl_first_substitution varchar(10), sl_first_substitution_reason integer, sl_second_substitution_date datetime, sl_second_substitution varchar(10), sl_second_substitution_reason integer, index encounter_id (encounter_id), index person_id (person_id));
delete t1
from flat_hiv_derived t1
join new_person_data t3 using (person_id);

insert into flat_hiv_derived
(select encounter_id, person_id, arv_start_date, prev_arv_line, cur_arv_line, who_stage, arv_start_cd4_count, arv_start_height, arv_start_weight, arv_eligibility_reason, arv_start_who_stage, pcp_prophylaxis_start_date, tb_prophylaxis_start_date, tb_treatment_start_date, first_evidence_patient_pregnant, edd, preg_1, preg_2, preg_3, cur_arv_meds, first_substitution_date, first_substitution, first_substitution_reason, second_substitution_date, second_substitution, second_substitution_reason,second_line, second_line_date, second_line_reason,sl_first_substitution_date, sl_first_substitution, sl_first_substitution_reason, sl_second_substitution_date, sl_second_substitution, sl_second_substitution_reason
from flat_hiv_derived_2);

drop view if exists flat_hiv_data;
create view flat_hiv_data as
(select * 
	from flat_hiv_defined t1
	join flat_hiv_derived t2 using (encounter_id,person_id)	
	order by person_id, encounter_datetime
);
