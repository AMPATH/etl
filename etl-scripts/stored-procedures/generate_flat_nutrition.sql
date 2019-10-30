CREATE DEFINER=`etl_user`@`%` PROCEDURE `etl`.`generate_flat_nutrition`()
BEGIN

select @start:= now();
select @table_version := "flat_nutrition_v0";

set session sort_buffer_size=512000000;

select @sep := " ## ";

#delete from flat_log where table_name="flat_nutrition";
#drop table if exists flat_nutrition;
create table if not exists flat_nutrition (
	person_id INT,
	uuid varchar(100),
    encounter_id INT,
	encounter_datetime datetime,
	location_id INT,
	weight decimal,
	height decimal,


    weight_height_zscore decimal,
    weight_height_zscore_diagnosis_coded INT,
    weight_height_zscore_diagnosis_value varchar(100),

    h_l_for_age_zscore decimal,
    h_l_for_age_zscore_diagnosis_coded INT,
    h_l_for_age_zscore_diagnosis_value varchar(100),

    muac_value decimal,
    muac_diagnosis_coded INT,
    muac_diagnosis_value varchar(100),

    bmi decimal,
    bmi_for_age_category_coded INT,
    bmi_for_age_category_value varchar(100),
    nutrition_assessment  varchar(100),
    encounter_type INT,

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
		(select max(date_created) from amrs.encounter e join flat_nutrition using (encounter_id)),
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


drop table if exists flat_nutrition_0;
create temporary table flat_nutrition_0(encounter_id int, primary key (encounter_id), index person_enc_date (person_id,encounter_datetime))
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
	where encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,32,33,38,43,47,110,112,113,114,115,141,130,96,131,55,54,79,78,188,171,172,117,106,105,134,133,162,163,190,191,168,16,80)
	order by person_id, encounter_datetime
);

select @prev_id := null;
select @cur_id := null;
select @cur_location := null;
select @weight := null;
select @height := null;


drop temporary table if exists flat_nutrition_1;
create temporary table flat_nutrition_1 (index encounter_id (encounter_id))
(select
	@prev_id := @cur_id as prev_id,
	@cur_id := t1.person_id as cur_id,
	t1.person_id,
	p.uuid,
	t1.encounter_id,
	t1.encounter_type,
	t1.encounter_datetime,

	case
		when location_id then @cur_location := location_id
		when @prev_id = @cur_id then @cur_location
		else null
	end as location_id,

	# 5089 = WEIGHT
	# 5090 = HEIGHT (CM)
    # 8238 = weight for height z-score


	if(obs regexp "!!5089=",cast(replace(replace((substring_index(substring(obs,locate("!!5089=",obs)),@sep,1)),"!!5089=",""),"!!","") as decimal(4,1)),null) as weight,
	if(obs regexp "!!5090=",cast(replace(replace((substring_index(substring(obs,locate("!!5090=",obs)),@sep,1)),"!!5090=",""),"!!","") as decimal(4,1)),null) as height,

    if(obs regexp "!!8238=",cast(replace(replace((substring_index(substring(obs,locate("!!8238=",obs)),@sep,1)),"!!8238=",""),"!!","") as decimal(4,1)),null) as weight_height_zscore,
    
    CASE
       when obs regexp "!!10213=" then @weight_height_zscore_diagnosis_coded := GetValues(obs, 10213)
        else @weight_height_zscore_diagnosis_coded := null
    end as weight_height_zscore_diagnosis_coded,

    
    CASE
        WHEN obs regexp "!!10213=1115!!" THEN @weight_height_zscore_diagnosis_value:='Normal'
        WHEN obs regexp "!!10213=10277!!" THEN @weight_height_zscore_diagnosis_value:='Mild acute malnutrition'
        WHEN obs regexp "!!10213=9472!!" THEN @weight_height_zscore_diagnosis_value:='Moderate acute malnutrition'
        WHEN obs regexp "!!10213=9471!!" THEN @weight_height_zscore_diagnosis_value:='Severe acute malnutrition'
        ELSE @weight_height_zscore_diagnosis_value := null
    END AS weight_height_zscore_diagnosis_value,

    if(obs regexp "!!9816=",cast(replace(replace((substring_index(substring(obs,locate("!!9816=",obs)),@sep,1)),"!!9816=",""),"!!","") as decimal(4,1)),null) as h_l_for_age_zscore,
	
    CASE
        when obs regexp "!!10214=" then @h_l_for_age_zscore_diagnosis_coded := GetValues(obs, 10214)
        else @h_l_for_age_zscore_diagnosis_coded := null
    end as h_l_for_age_zscore_diagnosis_coded,

    CASE
        WHEN obs regexp "!!10214=1115!!"  THEN @h_l_for_age_zscore_diagnosis_value:='Normal'
        WHEN obs regexp "!!10214=10226!!" THEN @h_l_for_age_zscore_diagnosis_value:='Mild stunting'
        WHEN obs regexp "!!10214=10216!!" THEN @h_l_for_age_zscore_diagnosis_value:='Moderate stunting'
        WHEN obs regexp "!!10214=10217!!" THEN @h_l_for_age_zscore_diagnosis_value:='Severe stunting'
        ELSE @h_l_for_age_zscore_diagnosis_value := null
    END AS h_l_for_age_zscore_diagnosis_value,

    if(obs regexp "!!1343=",cast(replace(replace((substring_index(substring(obs,locate("!!1343=",obs)),@sep,1)),"!!1343=",""),"!!","") as decimal(4,1)),null) as muac_value,
    	
    CASE
    	when obs regexp "!!10225=" then @muac_diagnosis_coded := GetValues(obs, 10225)
        else @muac_diagnosis_coded := null
    end as muac_diagnosis_coded,

    CASE
        WHEN obs regexp "!!10225=10224!!" THEN @muac_diagnosis_value:='Normal'
        WHEN obs regexp "!!10225=10222!!" THEN @muac_diagnosis_value:='Moderate acute malnutrition'
        WHEN obs regexp "!!10225=10223!!" THEN @muac_diagnosis_value:='Severe acute malnutrition'
        ELSE @muac_diagnosis_value := null
    END AS muac_diagnosis_value,


    CASE
     	when obs regexp "!!1342=" then @bmi := GetValues(obs, 1342)
        else @bmi := null
    END as bmi,
    
    CASE
     	when obs regexp "!!7369=" then @bmi_for_age_category_coded := GetValues(obs, 7369)
        else @bmi_for_age_category_coded := null
    end as bmi_for_age_category_coded,

    CASE
        WHEN obs regexp "!!7369=1115!!" THEN @bmi_for_age_category_value:='Normal'
        WHEN obs regexp "!!7369=10277!!" THEN @bmi_for_age_category_value:='Mild acute malnutrition'
        WHEN obs regexp "!!7369=9472!!" THEN @bmi_for_age_category_value:='Moderate acute malnutrition'
        WHEN obs regexp "!!7369=9471!!" THEN @bmi_for_age_category_value:='Severe acute malnutrition'
        WHEN obs regexp "!!7369=6895!!" THEN @bmi_for_age_category_value:='Overweight'
        WHEN obs regexp "!!7369=7764!!" THEN @bmi_for_age_category_value:='Obesity'
        ELSE @bmi_for_age_category_value := null
    END AS bmi_for_age_category_value,
    
    CASE
        WHEN @weight_height_zscore_diagnosis_coded  THEN @nutrition_assessment:='Z-score (Weight for Height)'
        WHEN @h_l_for_age_zscore_diagnosis_coded  THEN @nutrition_assessment:='Z-score (Height/length for age)'
        WHEN @muac_diagnosis_coded  THEN @nutrition_assessment:='MUAC diagnosis'
        WHEN @bmi_for_age_category_coded  THEN @nutrition_assessment:='BMI for age diagnosis'
        WHEN @bmi  THEN @nutrition_assessment:='BMI assessment'
        ELSE @nutrition_assessment := null
    END AS nutrition_assessment



from flat_nutrition_0 t1
	join amrs.person p using (person_id)
);



delete t1
from flat_nutrition t1
join new_data_person_ids t2 using (person_id);

replace into flat_nutrition
(select


    person_id,
	uuid,
    encounter_id,
	encounter_datetime,
	location_id,
	weight,
	height,


    weight_height_zscore,
    weight_height_zscore_diagnosis_coded,
    weight_height_zscore_diagnosis_value,
    h_l_for_age_zscore,
    h_l_for_age_zscore_diagnosis_coded,
    h_l_for_age_zscore_diagnosis_value,

    muac_value,
    muac_diagnosis_coded,
    muac_diagnosis_value,

    bmi,
    bmi_for_age_category_coded,
    bmi_for_age_category_value,
    nutrition_assessment,
    encounter_type

from flat_nutrition_1);

select @end := now();
insert into flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");

END