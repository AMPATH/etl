#********************************************************************************************************
#* CREATION OF MOH INDICATORS TABLE ****************************************************************************
#********************************************************************************************************

# Need to first create this temporary table to sort the data by person,encounterdateime.
# This allows us to use the previous row's data when making calculations.
# It seems that if you don't create the temporary table first, the sort is applied
# to the final result. Any references to the previous row will not an ordered row.

# v2.1 Notes:
#      Added encounter types for GENERALNOTE (112), CLINICREVIEW (113), MOH257BLUECARD (114), HEIFOLLOWUP (115), TRANSFERFORM (116)
#      Added timestamp to log

# v2.2 Notes:
#      Add ability to handle error messages. Add columns for has_errors, vl_error, cd4_error, hiv_dna_pcr_error
#      Delete all rows for a patient before inserting new data

# v2.5 Notes:
#		use date(t1.test_datetime) = date(t2.encounter_datetime) joining column for flat_lab_obs table instead of encounter_id

set session sort_buffer_size=512000000;
set session group_concat_max_len=100000;

select @sep := " ## ";
select @unknown_encounter_type := 99999;
select @table_version := "flat_labs_and_imaging_v2.6";

#delete from flat_log where table_name=@table_version;
#drop table if exists flat_labs_and_imaging;
create table if not exists flat_labs_and_imaging (
	person_id int,
	uuid varchar(100),
    encounter_id int,
	test_datetime datetime,
	encounter_type int,
	hiv_dna_pcr int,
	hiv_rapid_test int,
	hiv_viral_load int,
	cd4_count int,
	cd4_percent decimal,
	hemoglobin decimal,
	ast int,
	creatinine decimal,
	chest_xray int,
	hba1c decimal,
	rbs decimal,
	fbs decimal,
	ecg int,
	urea decimal,
    has_errors text,
    vl_error boolean,
    cd4_error boolean,
    hiv_dna_pcr_error boolean,

	tests_ordered varchar(1000),
    primary key encounter_id (encounter_id),
    index person_date (person_id, test_datetime),
	index person_uuid (uuid)
);

select @start := now();
select @last_date_created := (select max(max_date_created) from flat_lab_obs);

select @last_update := (select max(date_updated) from flat_log where table_name=@table_version);

# then use the max_date_created from amrs.encounter. This takes about 10 seconds and is better to avoid.
select @last_update :=
	if(@last_update is null,
		(select max(date_created) from amrs.encounter e join flat_labs_and_imaging using (encounter_id)),
		@last_update);

#otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
select @last_update := if(@last_update,@last_update,'1900-01-01');
#select @last_update := "2015-05-14";

drop table if exists new_data_person_ids_0;
create temporary table new_data_person_ids_0(person_id int, primary key (person_id))
(select distinct person_id
	from flat_lab_obs
	where max_date_created > @last_update
);


delete t1
from flat_labs_and_imaging t1
join new_data_person_ids_0 t2 using (person_id);

drop table if exists flat_labs_and_imaging_0;
create temporary table flat_labs_and_imaging_0(index encounter_id (encounter_id), index person_test (person_id,test_datetime))
(select * from
((select t1.person_id,
	t1.encounter_id,
	t1.test_datetime,
	t1.encounter_type,
	t1.location_id,
	t1.obs,
	t2.orders
	from flat_lab_obs t1
		join new_data_person_ids_0 t0 using (person_id)
		left join flat_orders t2 on date(t1.test_datetime) = date(t2.encounter_datetime) and t1.person_id = t2.person_id)
        UNION ALL
( select t1.person_id,
	t1.encounter_id,
	t1.encounter_datetime as test_datetime,
	t2.encounter_type,
	t2.location_id,
	t2.obs,
	t1.orders
	from flat_orders t1
		left join flat_lab_obs t2 on date(t2.test_datetime) = date(t1.encounter_datetime) and t1.person_id = t2.person_id
        where t1.encounter_datetime >= @last_update and t2.person_id is null

) order by person_id, test_datetime) as derived );


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
	t1.test_datetime,
	t1.encounter_type,

	case
		when location_id then @cur_location := location_id
		when @prev_id = @cur_id then @cur_location
		else null
	end as location_id,

	# 1030 = HIV DNA PCR
	# 1040 = HIV Rapid test
	# 856 = HIV VIRAL LOAD, QUANTITATIVE
	# 5497 = CD4, BY FACS
	# 730 = CD4%, BY FACS
	# 21 = HEMOGLOBIN
	# 653 = AST
	# 790 = SERUM CREATININE
	# 12 = X-RAY, CHEST, PRELIMINARY FINDINGS
	# 6126 = hba1c
	# 887 = rbs
	# 6252 = fbs
	# 1537 = ecg
	# 1537 = urea
	# 1271 = TESTS ORDERED
	# 9239 = LABORATORY TEST WITH EXCEPTION
    # 9020 = LAB ERROR

	if(obs regexp "!!1030=",cast(replace(replace((substring_index(substring(obs,locate("1030=",obs)),@sep,1)),"1030=",""),"!!","") as unsigned),null) as hiv_dna_pcr,
	if(obs regexp "!!1040=",cast(replace(replace((substring_index(substring(obs,locate("1040=",obs)),@sep,1)),"1040=",""),"!!","") as unsigned),null) as hiv_rapid_test,

	case
		when obs regexp "!!856=" then cast(replace(replace((substring_index(substring(obs,locate("856=",obs)),@sep,1)),"856=",""),"!!","") as unsigned)
	end as hiv_viral_load,
	if(obs regexp "!!5497=",cast(replace(replace((substring_index(substring(obs,locate("5497=",obs)),@sep,1)),"5497=",""),"!!","") as unsigned),null) as cd4_count,
	if(obs regexp "!!730=",cast(replace(replace((substring_index(substring(obs,locate("730=",obs)),@sep,1)),"730=",""),"!!","") as decimal(3,1)),null) as cd4_percent,
	if(obs regexp "!!21=",cast(replace(replace((substring_index(substring(obs,locate("21=",obs)),@sep,1)),"21=",""),"!!","") as decimal(4,1)),null) as hemoglobin,
	if(obs regexp "!!653=",cast(replace(replace((substring_index(substring(obs,locate("653=",obs)),@sep,1)),"653=",""),"!!","") as unsigned),null) as ast,
	if(obs regexp "!!790=",cast(replace(replace((substring_index(substring(obs,locate("790=",obs)),@sep,1)),"790=",""),"!!","") as decimal(4,1)),null) as creatinine,
	if(obs regexp "!!12=" and not obs regexp "!!12=1107",cast(replace(replace((substring_index(substring(obs,locate("12=",obs)),@sep,1)),"12=",""),"!!","") as unsigned),null) as chest_xray,
	if(obs regexp "!!6126=",cast(replace(replace((substring_index(substring(obs,locate("6126=",obs)),@sep,1)),"6126=",""),"!!","") as decimal(4,1)),null) as hba1c,
	if(obs regexp "!!887=",cast(replace(replace((substring_index(substring(obs,locate("887=",obs)),@sep,1)),"887=",""),"!!","") as decimal(4,1)),null) as rbs,
	if(obs regexp "!!6252=",cast(replace(replace((substring_index(substring(obs,locate("6252=",obs)),@sep,1)),"6252=",""),"!!","") as decimal(4,1)),null) as fbs,
	if(obs regexp "!!1537=",cast(replace(replace((substring_index(substring(obs,locate("1537=",obs)),@sep,1)),"1537=",""),"!!","") as unsigned),null) as ecg,
	if(obs regexp "!!857=",cast(replace(replace((substring_index(substring(obs,locate("857=",obs)),@sep,1)),"857=",""),"!!","") as decimal(4,1)),null) as urea,
	if(obs regexp "!!9239=",obs,null) as has_errors,
    if(obs regexp "!!9239=856!!",1,null) as vl_error,
    if(obs regexp "!!9239=5497!!",1,null) as cd4_error,
    if(obs regexp "!!9239=1030",1,null) as hiv_dna_pcr_error,
    CASE
        WHEN
            (obs REGEXP '!!1271='
                AND NOT obs REGEXP '!!1271=1107')
        THEN
            CONCAT(REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(obs, LOCATE('!!1271=', obs)),
                                    @sep,
                                    ROUND((LENGTH(obs) - LENGTH(REPLACE(obs, '1271=', ''))) / LENGTH('!!1271=')))),
                            '!!1271=',
                            ''),
                        '!!',
                        ''),
                    ' ## ',
                    IFNULL(orders, ''))
        ELSE orders
    END AS tests_ordered

from flat_labs_and_imaging_0 t1
	join amrs.person p using (person_id)
);

replace into flat_labs_and_imaging
(select
	person_id,
	t1.uuid,
    encounter_id,
	test_datetime,
	encounter_type,
	hiv_dna_pcr,
	hiv_rapid_test,
	hiv_viral_load,
	cd4_count,
	cd4_percent,
	hemoglobin,
	ast,
	creatinine,
	chest_xray,
	hba1c,
	rbs,
	fbs,
	ecg,
	urea,
    has_errors,
    vl_error,
    cd4_error,
    hiv_dna_pcr_error,
	tests_ordered
from flat_labs_and_imaging_1 t1
);

# Remove test patients
delete t1
from flat_labs_and_imaging t1
join amrs.person_attribute t2 using (person_id)
where t2.person_attribute_type_id=28 and value='true' and voided=0;

#select * from flat_labs_and_imaging;

select @end := now();
insert into flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");
