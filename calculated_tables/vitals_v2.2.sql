#********************************************************************************************************
#* CREATION OF MOH INDICATORS TABLE ****************************************************************************
#********************************************************************************************************

# Need to first create this temporary table to sort the data by person,encounterdateime.
# This allows us to use the previous row's data when making calculations.
# It seems that if you don't create the temporary table first, the sort is applied
# to the final result. Any references to the previous row will not an ordered row.

# v2.2 Notes:
#      Added BSA and ECOG columns
#      Added INR, LCC and RCC columns

select @start:= now();
select @table_version := "flat_vitals_v2.2";

set session sort_buffer_size=512000000;

select @sep := " ## ";

#delete from flat_log where table_name="flat_vitals";
#drop table if exists flat_vitals;
create table if not exists flat_vitals (
	person_id int,
	uuid varchar(100),
    encounter_id int,
	encounter_datetime datetime,
	location_id int,
	weight decimal,
	height decimal,
	temp decimal(4,1),
	oxygen_sat int,
	systolic_bp int,
	diastolic_bp int,
	pulse int,
	bsa int,
	ecog int,
  inr int,
  lcc int,
  rcc int,
    primary key encounter_id (encounter_id),
    index person_date (person_id, encounter_datetime),
	index person_uuid (uuid)
);


select @start := now();
select @last_date_created := (select max(max_date_created) from flat_obs);


select @last_update := (select max(date_updated) from flat_log where table_name=@table_version);

# then use the max_date_created from amrs.encounter. This takes about 10 seconds and is better to avoid.
select @last_update :=
	if(@last_update is null,
		(select max(date_created) from amrs.encounter e join flat_vitals using (encounter_id)),
		@last_update);

#otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
select @last_update := if(@last_update,@last_update,'1900-01-01');
#select @last_update := "2015-04-30";

drop table if exists new_data_person_ids;
create temporary table new_data_person_ids(person_id int, primary key (person_id))
(select distinct person_id
	from flat_obs
	where max_date_created > @last_update
);


drop table if exists flat_vitals_0;
create temporary table flat_vitals_0(encounter_id int, primary key (encounter_id), index person_enc_date (person_id,encounter_datetime))
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
	where encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,32,33,38,43,47,110,112,113,114,115,141,130,96,131,55,54,79,78,188,171,172,117,106,105,134,133,162,163,190,191)
	order by person_id, encounter_datetime
);

select @prev_id := null;
select @cur_id := null;
select @cur_location := null;
select @systolic_bp := null;
select @diastolic_bp := null;
select @pulse := null;
select @temp := null;
select @oxygen_sat := null;
select @weight := null;
select @height := null;
select @bsa := null;
select @ecog := null;
select @inr := null;
select @lcc := null;
select @rcc := null;


drop temporary table if exists flat_vitals_1;
create temporary table flat_vitals_1 (index encounter_id (encounter_id))
(select
	@prev_id := @cur_id as prev_id,
	@cur_id := t1.person_id as cur_id,
	t1.person_id,
	p.uuid,
	t1.encounter_id,
	t1.encounter_datetime,

	case
		when location_id then @cur_location := location_id
		when @prev_id = @cur_id then @cur_location
		else null
	end as location_id,

	# 5089 = WEIGHT
	# 5090 = HEIGHT (CM)
	# 5088 = TEMPERATURE (C)
	# 5092 = BLOOD OXYGEN SATURATION
	# 5085 = SYSTOLIC BLOOD PRESSURE
	# 5086 = DIASTOLIC BLOOD PRESSURE
	# 5087 = PULSE
	# 980 = BSA
	# 6584 = ECOG
  # 8167 = INR
  # 8898 = LCC
  # 8899 = RCC

	if(obs regexp "!!5089=",cast(replace(replace((substring_index(substring(obs,locate("!!5089=",obs)),@sep,1)),"!!5089=",""),"!!","") as decimal(4,1)),null) as weight,
	if(obs regexp "!!5090=",cast(replace(replace((substring_index(substring(obs,locate("!!5090=",obs)),@sep,1)),"!!5090=",""),"!!","") as decimal(4,1)),null) as height,
	if(obs regexp "!!5088=",cast(replace(replace((substring_index(substring(obs,locate("!!5088=",obs)),@sep,1)),"!!5088=",""),"!!","") as decimal(4,1)),null) as temp,
	if(obs regexp "!!5092=",cast(replace(replace((substring_index(substring(obs,locate("!!5092=",obs)),@sep,1)),"!!5092=",""),"!!","") as unsigned),null) as oxygen_sat,
	if(obs regexp "!!5085=",cast(replace(replace((substring_index(substring(obs,locate("!!5085=",obs)),@sep,1)),"!!5085=",""),"!!","") as unsigned),null) as systolic_bp,
	if(obs regexp "!!5086=",cast(replace(replace((substring_index(substring(obs,locate("!!5086=",obs)),@sep,1)),"!!5086=",""),"!!","") as unsigned),null) as diastolic_bp,
	if(obs regexp "!!5087=",cast(replace(replace((substring_index(substring(obs,locate("!!5087=",obs)),@sep,1)),"!!5087=",""),"!!","") as unsigned),null) as pulse,
	if(obs regexp "!!980=",cast(replace(replace((substring_index(substring(obs,locate("!!980=",obs)),@sep,1)),"!!980=",""),"!!","") as unsigned),null) as bsa,
	if(obs regexp "!!6584=",cast(replace(replace((substring_index(substring(obs,locate("!!6584=",obs)),@sep,1)),"!!6584=",""),"!!","") as unsigned),null) as ecog,
  if(obs regexp "!!8167=",cast(replace(replace((substring_index(substring(obs,locate("!!8167=",obs)),@sep,1)),"!!8167=",""),"!!","") as unsigned),null) as inr,
  if(obs regexp "!!8898=",cast(replace(replace((substring_index(substring(obs,locate("!!8898=",obs)),@sep,1)),"!!8898=",""),"!!","") as unsigned),null) as lcc,
  if(obs regexp "!!8899=",cast(replace(replace((substring_index(substring(obs,locate("!!8899=",obs)),@sep,1)),"!!8899=",""),"!!","") as unsigned),null) as rcc

from flat_vitals_0 t1
	join amrs.person p using (person_id)
);



delete t1
from flat_vitals t1
join new_data_person_ids t2 using (person_id);

replace into flat_vitals
(select
	person_id,
	uuid,
    encounter_id,
	encounter_datetime,
	location_id,
	weight,
	height,
	temp,
	oxygen_sat,
	systolic_bp,
	diastolic_bp,
	pulse,
	bsa,
	ecog,
  inr,
  lcc,
  rcc
from flat_vitals_1);

select @end := now();
insert into flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");
