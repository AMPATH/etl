#********************************************************************************************************
#* CREATION OF MOH INDICATORS TABLE ****************************************************************************
#********************************************************************************************************

# Need to first create this temporary table to sort the data by person,encounterdateime. 
# This allows us to use the previous row's data when making calculations.
# It seems that if you don't create the temporary table first, the sort is applied 
# to the final result. Any references to the previous row will not an ordered row. 

set session sort_buffer_size=512000000;
set session group_concat_max_len=100000;

select @sep := " ## ";
select @unknown_encounter_type := 99999;

#delete from flat_log where table_name="flat_labs_and_imaging";
#drop table if exists flat_labs_and_imaging;
create table if not exists flat_labs_and_imaging (
	person_id int,
	uuid varchar(100),
    encounter_id int,
	encounter_datetime datetime,
	encounter_type int,
	location_id int,
	location_uuid varchar(100),
	hiv_viral_load int,
	cd4_count int,
	cd4_percent decimal,
	hemoglobin decimal,
	ast int,
	creatinine decimal,
	chest_xray int,
	tests_ordered varchar(1000),
    primary key encounter_id (encounter_id),
    index person_date (person_id, encounter_datetime),
	index location_uuid (location_uuid),
	index person_uuid (uuid)
);

select @start := now();

select @last_update := (select max(date_updated) from flat_log where table_name="flat_labs_and_imaging");

# then use the max_date_created from amrs.encounter. This takes about 10 seconds and is better to avoid.
select @last_update :=
	if(@last_update is null, 
		(select max(date_created) from amrs.encounter e join flat_labs_and_imaging using (encounter_id)),
		@last_update);

#otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
select @last_update := if(@last_update,@last_update,'1900-01-01');
#select @last_update := "2015-05-14";



drop table if exists new_data_person_ids;
create temporary table new_data_person_ids(person_id int, primary key (person_id))
(select distinct person_id 
	from flat_obs
	where max_date_created > @last_update
);

delete t1
from flat_labs_and_imaging t1
join new_data_person_ids t2 using (person_id);


drop table if exists flat_labs_and_imaging_0;
create temporary table flat_labs_and_imaging_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
(select 
	t1.person_id, 
	t1.encounter_id, 
	t1.encounter_datetime,
	t1.encounter_type,
	t1.location_id,
	t1.obs,
	t1.obs_datetimes
	from flat_obs t1
		join new_data_person_ids t0 using (person_id)
	where encounter_type in (1,2,3,4,5,6,7,8,9,10,13,14,15,17,19,22,23,26,32,33,43,47,@unknown_encounter_type)
	order by t1.person_id, encounter_datetime
);


select @prev_id := null;
select @cur_id := null;
select @cur_location := null;
select @vl := null;
select @cd4_count := null;
Select @cd4_percent := null;
select @hemoglobin := null;
select @ast := null;
select @creatinine := null;
select @chest_xray := null;

drop temporary table if exists flat_labs_and_imaging_1;
create temporary table flat_labs_and_imaging_1 (index encounter_id (encounter_id))
(select 
	@prev_id := @cur_id as prev_id, 
	@cur_id := t1.person_id as cur_id,
	t1.person_id,
	p.uuid,
	t1.encounter_id,
	t1.encounter_datetime,			
	t1.encounter_type,

	case
		when location_id then @cur_location := location_id
		when @prev_id = @cur_id then @cur_location
		else null
	end as location_id,

	# 856 = HIV VIRAL LOAD, QUANTITATIVE
	# 5497 = CD4, BY FACS
	# 730 = CD4%, BY FACS
	# 21 = HEMOGLOBIN
	# 653 = AST
	# 790 = SERUM CREATININE
	# 12 = X-RAY, CHEST, PRELIMINARY FINDINGS
	# 1271 = TESTS ORDERED
	
	if(obs regexp "!!856=",cast(replace(replace((substring_index(substring(obs,locate("856=",obs)),@sep,1)),"856=",""),"!!","") as unsigned),null) as hiv_viral_load,
	if(obs regexp "!!5497=",cast(replace(replace((substring_index(substring(obs,locate("5497=",obs)),@sep,1)),"5497=",""),"!!","") as unsigned),null) as cd4_count,
	if(obs regexp "!!730=",cast(replace(replace((substring_index(substring(obs,locate("730=",obs)),@sep,1)),"730=",""),"!!","") as decimal(3,1)),null) as cd4_percent,
	if(obs regexp "!!21=",cast(replace(replace((substring_index(substring(obs,locate("21=",obs)),@sep,1)),"21=",""),"!!","") as decimal(4,1)),null) as hemoglobin,
	if(obs regexp "!!653=",cast(replace(replace((substring_index(substring(obs,locate("653=",obs)),@sep,1)),"653=",""),"!!","") as unsigned),null) as ast,
	if(obs regexp "!!790=",cast(replace(replace((substring_index(substring(obs,locate("790=",obs)),@sep,1)),"790=",""),"!!","") as decimal(4,1)),null) as creatinine,
	if(obs regexp "!!12=" and not obs regexp "!!12=1107",cast(replace(replace((substring_index(substring(obs,locate("12=",obs)),@sep,1)),"12=",""),"!!","") as unsigned),null) as chest_xray,
	if(obs regexp "!!1271=" and not obs regexp "!!1271=1107",
			replace(replace((substring_index(substring(obs,locate("!!1271=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "1271=", "") ) ) / LENGTH("!!1271=") ))),"!!1271=",""),"!!",""),
			null
		) as tests_ordered	

from flat_labs_and_imaging_0 t1
	join amrs.person p using (person_id)
);



insert into flat_labs_and_imaging
(select 
	person_id,
	t1.uuid,
    encounter_id,
	encounter_datetime,
	encounter_type,
	location_id,
	t2.uuid as location_uuid,
	hiv_viral_load,
	cd4_count,
	cd4_percent,
	hemoglobin,
	ast,
	creatinine,
	chest_xray,
	tests_ordered
from flat_labs_and_imaging_1 t1
	join amrs.location t2 using (location_id)
);

insert into flat_log values (@start,"flat_labs_and_imaging");

select concat("Time to complete: ",timestampdiff(minute, @start, now())," minutes");