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
# 9239 = LABORATORY TEST WITH EXCEPTION
# 9020 = LAB ERROR
# 9812 = SERUM CRAG
# 10304 = GENEXPERT IMAGE
# 10313 = DRUG SENSITIVITY TEST IMAGE
# 8595 = SPEP
# 8731 = SERUM M, PROTEIN

# 1984 = PRESENCE OF PUS CELLS URINE
# 2339 = PRESENCE OF PROTEIN URINE
# 6337 = PRESENCE OF LEUCOCYTES
# 7276 = PRESENCE OF KETONES
# 2340 = PRESENCE OF SUGAR URINE
# 9307 = PRESENCE OF NITRITES
# 1327 = RETICULOCYTES
# 8732 = SERUM ALPHA-1 GLOBULIN
# 8733 = SERUM ALPHA-2 GLOBULIN
# 8734 = SERUM BETA GLOBULIN
# 8735 = SERUM GAMMA GLOBULIN
# 10195 = KAPPA LIGHT CHAINS
# 10196 = LAMBDA LIGHT CHAINS
# 10197 = RATIO OF KAPPA LAMBDA

 /*
				679	RBC
				21	HGB
				851	MCV
				1018	MCH
				1017	MCHC
				1016	RDW
				729	PLT
				678	SERUM WBC
				1330	ANC
				790	Creatinine
				1132	Sodium
				1133	Potassium
				1134	Chloride
				655	Total Bili
				1297	Direct Bili
				6123	GGT
				653	AST
				654	ALT
				717	Total Protein
				848	Albumin
				785	ALP
				1014	LDH
				10249	Total PSA
				10250	CEA
				10251	(CA 19-9)
				9010	HBF
				9011	HBA
				9699	HbS
				9012	HBA2
                                9812    SERUM CRAG
                                10304  GENEXPERT  IMAGE
                                10313  DRUG SENSITIVITY TEST IMAGE
			*/

# 1. Replace flat_lab_obs with flat_lab_obs_name
# 2. Replace concept_id in () with concept_id in (obs concept_ids)
# 3. Add column definitions
# 4. Add obs_set column definitions

# v1.2 Notes:
#      Added in concepts for lab exceptions
#      Changed logging to include timestamp

# v1.9 Notes
#      Added concepts for spep results, remission

select @table_version := "flat_lab_obs_v1.9";

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
select @last_update := (select max(date_updated) from flat_log where table_name=@table_version);

# then use the max_date_created from amrs.encounter. This takes about 10 seconds and is better to avoid.
select @last_update :=
	if(@last_update is null,
		(select max(date_created) from amrs.encounter e join flat_lab_obs using (encounter_id)),
		@last_update);

#otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
select @last_update := if(@last_update,@last_update,'1900-01-01');

#select @last_update := "2018-10-19";

drop table if exists voided_obs;
create table voided_obs (index person_datetime (person_id, obs_datetime))
(select distinct person_id, obs_datetime
	from amrs.obs
	where voided=1
		and date_voided > @last_update
		and date_created <= @last_update
		and concept_id in (856,5497,730,21,653,790,12,1030,1040,1271,9239,9020,9508, 6126, 887, 6252, 1537, 857,
				   		679,21,851,1018,1017,1016,729,678,1330,790,1132,1133,1134,655,1297,6123,
                		653,654,717,848,785,1014,10249,10250,10251,9010,9011,9699,9012,9812,10304,10313,8731,8595,
						1984,2339,6337,7276,2340,9307,1327,8732,8733,8734,8735,10195,10196,10197)
);

# delete any rows that have a voided obs
delete t1
from flat_lab_obs t1
join voided_obs t2 on date(t1.test_datetime) = date(t2.obs_datetime) and t1.person_id=t2.person_id;


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
		concept_id in (856,5497,730,21,653,790,12,1030,1040,1271,9239,9020,9508, 6126, 887, 6252, 1537, 857,
                        679,21,851,1018,1017,1016,729,678,1330,790,1132,1133,1134,655,1297,6123,
						653,654,717,848,785,1014,10249,10250,10251,9010,9011,9699,9012,9812,10304,10313,8731,8595,
						1984,2339,6337,7276,2340,9307,1327,8732,8733,8734,8735,10195,10196,10197)
		and if(concept_id=1271 and value_coded=1107,false,true)
		and voided=0
	group by person_id, date(o.obs_datetime)
);

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
		and concept_id in  (856,5497,730,21,653,790,12,1030,1040,1271,9239,9020,9508, 6126, 887, 6252, 1537, 857,
                                679,21,851,1018,1017,1016,729,678,1330,790,1132,1133,1134,655,1297,6123,
                                653,654,717,848,785,1014,10249,10250,10251,9010,9011,9699,9012,9812,10304,10313,8731,8595,
								1984,2339,6337,7276,2340,9307,1327,8732,8733,8734,8735,10195,10196,10197)

		and if(concept_id=1271 and value_coded=1107,false,true) # DO NOT INCLUDE ROWS WHERE ORDERS=NONE
	group by person_id, date(o.obs_datetime)
);
#
#select * from flat_lab_obs where person_id in (386322,152885,783334)



drop table voided_obs;
select @end := now();
select @end;

insert into flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));

select concat("Time to complete: ",timestampdiff(minute, @start, now())," minutes") as "Time to complete";