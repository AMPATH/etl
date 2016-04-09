#********************************************************************************************************
#* CREATION OF MOH INDICATORS TABLE ****************************************************************************
#********************************************************************************************************

# Need to first create this temporary table to sort the data by person,encounterdateime.
# This allows us to use the previous row's data when making calculations.
# It seems that if you don't create the temporary table first, the sort is applied
# to the final result. Any references to the previous row will not an ordered row.

select @start := now();
select @table_version := "flat_hiv_summary_v2.0";

set session sort_buffer_size=512000000;

select @sep := " ## ";
select @lab_encounter_type := 99999;
select @last_date_created := (select max(max_date_created) from flat_obs);


#drop table if exists flat_hiv_summary;
#delete from flat_log where table_name="flat_hiv_summary";
create table if not exists flat_hiv_summary (
	person_id int,
	uuid varchar(100),
    encounter_id int,
	encounter_datetime datetime,
	encounter_type int,
	is_clinical_encounter int,
	location_id int,
	location_uuid varchar(100),
	visit_num int,
	enrollment_date datetime,
	hiv_start_date datetime,
	death_date datetime,
	scheduled_visit int,
	transfer_out int,
	transfer_in int,
	out_of_care int,
	prev_rtc_date datetime,
	rtc_date datetime,
	arv_start_date datetime,
	arv_first_regimen varchar(500),
	cur_arv_meds varchar(500),
	cur_arv_line int,
    first_evidence_patient_pregnant datetime,
    edd datetime,
	screened_for_tb boolean,
	tb_tx_start_date datetime,
	pcp_prophylaxis_start_date datetime,
	cd4_resulted double,
	cd4_resulted_date datetime,
    cd4_1 double,
    cd4_1_date datetime,
    cd4_2 double,
    cd4_2_date datetime,
    cd4_percent_1 double,
	cd4_percent_1_date datetime,
    cd4_percent_2 double,
	cd4_percent_2_date datetime,
	vl_resulted int,
	vl_resulted_date datetime,
    vl_1 int,
    vl_1_date datetime,
    vl_2 int,
    vl_2_date datetime,
    vl_order_date datetime,
    cd4_order_date datetime,

	# 1030 dna pcr
	hiv_dna_pcr_order_date datetime,
	hiv_dna_pcr_resulted int,
	hiv_dna_pcr_resulted_date datetime,
	hiv_dna_pcr_1 int,
	hiv_dna_pcr_1_date datetime,
	hiv_dna_pcr_2 int,
	hiv_dna_pcr_2_date datetime,

	# 1040 hiv rapid test
#	hiv_rapid_test_resulted int,
#	hiv_rapid_test_resulted_date datetime,

	condoms_provided int,
	using_modern_contraceptive_method int,
	#Current WHO Stage 5356
	cur_who_stage int,
	prev_encounter_datetime_hiv datetime,
	next_encounter_datetime_hiv datetime,
	prev_encounter_type_hiv mediumint,
	next_encounter_type_hiv mediumint,
	prev_clinical_datetime_hiv_ datetime,
	next_clinical_datetime_hiv datetime,

    primary key encounter_id (encounter_id),
    index person_date (person_id, encounter_datetime),
	index location_rtc (location_uuid,rtc_date),
	index person_uuid (uuid),
	index location_enc_date (location_uuid,encounter_datetime),
	index enc_date_location (encounter_datetime, location_uuid),
	index location_id_rtc_date (location_id,rtc_date)
);


select @last_update := (select max(date_updated) from flat_log where table_name=@table_version);

# then use the max_date_created from amrs.encounter. This takes about 10 seconds and is better to avoid.
select @last_update :=
	if(@last_update is null,
		(select max(date_created) from amrs.encounter e join flat_hiv_summary using (encounter_id)),
		@last_update);

#otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
select @last_update := if(@last_update,@last_update,'1900-01-01');
# select @last_update := "2016-03-11"; #date(now());
#select @last_date_created := "2015-11-17"; #date(now());


drop table if exists new_data_person_ids;
create temporary table new_data_person_ids(person_id int, primary key (person_id))
(select distinct person_id #, min(encounter_datetime) as start_date
	from etl.flat_obs
	where max_date_created > @last_update
#	group by person_id
#limit 10
);

replace into new_data_person_ids
(select distinct person_id
	from flat_lab_obs
	where max_date_created > @last_update
);




drop table if exists flat_hiv_summary_0a;
create temporary table flat_hiv_summary_0a
(select
	t1.person_id,
	t1.encounter_id,
	t1.encounter_datetime,
	t1.encounter_type,
	t1.location_id,
	t1.obs,
	t1.obs_datetimes,
	# in any visit, there many be multiple encounters. for this dataset, we want to include only clinical encounters (e.g. not lab or triage visit)
	case
		when encounter_type in (1,2,3,4,10,14,15,17,19,26,32,33,34,47,105,106) then 1
		else null
	end as is_clinical_encounter,

	case
		when encounter_type in (110,@lab_encounter_type,111,5,6,7,8,9,13,16,21,22,23,43) then 1
		else 10
	end as encounter_type_sort_index

	from etl.flat_obs t1
		join new_data_person_ids t0 using (person_id)
#		join new_data_person_ids t0 on t1.person_id=t0.person_id and t1.encounter_datetime >= t0.start_date
	where t1.encounter_type in (1,2,3,4,10,14,15,17,19,22,23,26,32,33,43,47,21,105,106,110,111)
);


insert into flat_hiv_summary_0a
(select
	t1.person_id,
	t1.encounter_id,
	t1.test_datetime,
	t1.encounter_type,
	null, #t1.location_id,
	t1.obs,
	null, #obs_datetimes
	# in any visit, there many be multiple encounters. for this dataset, we want to include only clinical encounters (e.g. not lab or triage visit)
	0 as is_clinical_encounter,
	1 as encounter_type_sort_index

	from flat_lab_obs t1
		join new_data_person_ids t0 using (person_id)
);


drop table if exists flat_hiv_summary_0;
create temporary table flat_hiv_summary_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
(select * from flat_hiv_summary_0a
order by person_id, date(encounter_datetime), encounter_type_sort_index
);



#select * from flat_hiv_summary_0 where person_id=1430;

select @prev_id := null;
select @cur_id := null;
select @enrollment_date := null;
select @hiv_start_date := null;
select @cur_location := null;
select @cur_rtc_date := null;
select @prev_rtc_date := null;
select @hiv_start_date := null;
select @arv_start_date := null;
select @arv_first_regimen := null;
select @cur_arv_line := null;
select @first_evidence_pt_pregnant := null;
select @edd := null;
select @cur_arv_meds := null;
select @tb_treatment_start_date := null;
select @pcp_prophylaxis_start_date := null;
select @screened_for_tb := null;
select @death_date := null;
select @vl_1:=null;
select @vl_2:=null;
select @vl_1_date:=null;
select @vl_2_date:=null;
select @vl_resulted:=null;
select @vl_resulted_date:=null;

select @cd4_resulted:=null;
select @cd4_resulted_date:=null;
select @cd4_1:=null;
select @cd4_1_date:=null;
select @cd4_2:=null;
select @cd4_2_date:=null;
select @cd4_percent_1:=null;
select @cd4_percent_1_date:=null;
select @cd4_percent_2:=null;
select @cd4_percent_2_date:=null;
select @vl_order_date := null;
select @cd4_order_date := null;

select @hiv_dna_pcr_order_date := null;
select @hiv_dna_pcr_1:=null;
select @hiv_dna_pcr_2:=null;
select @hiv_dna_pcr_1_date:=null;
select @hiv_dna_pcr_2_date:=null;

select @condoms_provided := null;
select @using_modern_contraceptive_method := null;

#Current WHO Stage
select @cur_who_stage := null;

#TO DO
# screened for cervical ca
# exposed infant

drop temporary table if exists flat_hiv_summary_1;
create temporary table flat_hiv_summary_1 (index encounter_id (encounter_id))
(select
	@prev_id := @cur_id as prev_id,
	@cur_id := t1.person_id as cur_id,
	t1.person_id,
	p.uuid,
	t1.encounter_id,
	t1.encounter_datetime,
	t1.encounter_type,
	t1.is_clinical_encounter,

	case
		when @prev_id != @cur_id and encounter_type in (21,@lab_encounter_type) then @enrollment_date := null
		when @prev_id != @cur_id then @enrollment_date := encounter_datetime
		when encounter_type not in (21,@lab_encounter_type) and @enrollment_date is null then @enrollment_date := encounter_datetime
		else @enrollment_date
	end as enrollment_date,

	#1836 = CURRENT VISIT TYPE
	#1246 = SCHEDULED VISIT
	if(obs regexp "!!1836="
		,replace(replace((substring_index(substring(obs,locate("!!1836=",obs)),@sep,1)),"!!1836=",""),"!!","")
		,null) as scheduled_visit,

	case
		when location_id then @cur_location := location_id
		when @prev_id = @cur_id then @cur_location
		else null
	end as location_id,

	case
        when @prev_id=@cur_id and encounter_type not in (5,6,7,8,9,21) then @visit_num:= @visit_num + 1
        when @prev_id!=@cur_id then @visit_num := 1
	end as visit_num,

	case
        when @prev_id=@cur_id then @prev_rtc_date := @cur_rtc_date
        else @prev_rtc_date := null
	end as prev_rtc_date,

	# 5096 = return visit date
	case
		when obs regexp "!!5096=" then @cur_rtc_date := replace(replace((substring_index(substring(obs,locate("!!5096=",obs)),@sep,1)),"!!5096=",""),"!!","")
		when @prev_id = @cur_id then if(@cur_rtc_date > encounter_datetime,@cur_rtc_date,null)
		else @cur_rtc_date := null
	end as cur_rtc_date,

	# 7015 = TRANSFER IN CARE FROM OTHER CENTER
	case
		when obs regexp "!!7015=" then @transfer_in := replace(replace((substring_index(substring(obs,locate("!!7015=",obs)),@sep,1)),"!!7015=",""),"!!","")
		else @transfer_in := null
	end as transfer_in,

	# 1285 = TRANSFER CARE TO OTHER CENTER
	# 1596 = REASON EXITED CARE
	# 9082 = PATIENT CARE STATUS

	case
		when obs regexp "!!1285=(1287|9068)!!" then 1
		when obs regexp "!!1596=1594!!" then 1
		when obs regexp "!!9082=(1287|9068)!!" then 1
		else null
	end as transfer_out,

	# 1946 = DISCONTINUE FROM CLINIC, HIV NEGATIVE
	case
		when obs regexp "!!1946=1065!!" then 1
		when obs regexp "!!1285=(1287|9068)!!" then 1
		when obs regexp "!!1596=" then 1
		when obs regexp "!!9082=(159|9036|9083|1287|9068)!!" then 1
		else null
	end as out_of_care,


	# 1946 = DISCONTINUE FROM CLINIC, HIV NEGATIVE
	# 1088 = CURRENT ANTIRETROVIRAL DRUGS USED FOR TREATMENT
	# 1255 = ANTIRETROVIRAL PLAN
	# 1040 = HIV RAPID TEST, QUALITATIVE
	# 1030 = HIV DNA POLYMERASE CHAIN REACTION, QUALITATIVE
	# 664 = POSITIVE
	case
		when obs regexp "!!1946=1065!!" then @hiv_start_date := null
		when encounter_type=@lab_encounter_type and obs regexp "!!(1040|1030)=664!!" then @hiv_start_date:=null
		when @prev_id != @cur_id or @hiv_start_date is null then
			case
				when obs regexp "!!(1040|1030)=664!!" then @hiv_start_date := date(encounter_datetime)
				when obs regexp "!!(1088|1255)=" then @hiv_start_date := date(encounter_datetime)
				else @hiv_start_date := null
			end
		else @hiv_start_date
	end as hiv_start_date,

	# 1255 = ANTIRETROVIRAL PLAN
	# 1250 = ANTIRETROVIRALS STARTED
	# 1088 = CURRENT ANTIRETROVIRAL DRUGS USED FOR TREATMENT
	# 2154 = PATIENT REPORTED CURRENT ANTIRETROVIRAL TREATMENT
	# 1260 = STOP ALL MEDICATIONS
	case
		when obs regexp "!!1255=1256!!" or (obs regexp "!!1255=(1257|1259|981|1258|1849|1850)!!" and @arv_start_date is null ) then @arv_start_date := date(t1.encounter_datetime)
		when obs regexp "!!1255=(1107|1260)!!" then @arv_start_date := null
		when @prev_id = @cur_id and obs regexp "!!(1250|1088|2154)=" and @arv_start_date is null then @arv_start_date := date(t1.encounter_datetime)
		when @prev_id != @cur_id then @arv_start_date := null
		else @arv_start_date
	end as arv_start_date,

	# 1255 = ANTIRETROVIRAL PLAN
	# 1250 = ANTIRETROVIRALS STARTED
	# 1088 = CURRENT ANTIRETROVIRAL DRUGS USED FOR TREATMENT
	# 2154 = PATIENT REPORTED CURRENT ANTIRETROVIRAL TREATMENT
	case
		when obs regexp "!!1255=(1107|1260)!!" then @cur_arv_meds := null
		when obs regexp "!!1250=" then @cur_arv_meds :=
			replace(replace((substring_index(substring(obs,locate("!!1250=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!1250=", "") ) ) / LENGTH("!!1250=") ))),"!!1250=",""),"!!","")
		when obs regexp "!!1088=" then @cur_arv_meds :=
			replace(replace((substring_index(substring(obs,locate("!!1088=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!1088=", "") ) ) / LENGTH("!!1088=") ))),"!!1088=",""),"!!","")
		when obs regexp "!!2154=" then @cur_arv_meds :=
			replace(replace((substring_index(substring(obs,locate("!!2154=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!2154=", "") ) ) / LENGTH("!!2154=") ))),"!!2154=",""),"!!","")
		when @prev_id=@cur_id then @cur_arv_meds
		else @cur_arv_meds:= null
	end as cur_arv_meds,


	case
		when @arv_first_regimen is null and @cur_arv_meds is not null then @arv_first_regimen := @cur_arv_meds
		when @prev_id = @cur_id then @arv_first_regimen
		else @arv_first_regimen := null
	end as arv_first_regimen,


	case
		when obs regexp "!!1255=(1107|1260)!!" then @cur_arv_line := null
		when obs regexp "!!1250=(6467|6964|792|633|631)!!" then @cur_arv_line := 1
		when obs regexp "!!1250=(794|635|6160|6159)!!" then @cur_arv_line := 2
		when obs regexp "!!1250=6156!!" then @cur_arv_line := 3
		when obs regexp "!!1088=(6467|6964|792|633|631)!!" then @cur_arv_line := 1
		when obs regexp "!!1088=(794|635|6160|6159)!!" then @cur_arv_line := 2
		when obs regexp "!!1088=6156!!" then @cur_arv_line := 3
		when obs regexp "!!2154=(6467|6964|792|633|631)!!" then @cur_arv_line := 1
		when obs regexp "!!2154=(794|635|6160|6159)!!" then @cur_arv_line := 2
		when obs regexp "!!2154=6156!!" then @cur_arv_line := 3
		when @prev_id = @cur_id then @cur_arv_line
		else @cur_arv_line := null
	end as cur_arv_line,

	# 1279 = NUMBER OF WEEKS PREGNANT
	# 5596 = ESTIMATED DATE OF CONFINEMENT
	# 5599 = DATE OF CONFINEMENT
	# 1146 = CONCEPTION SINCE LAST VISIT
	case
		when @prev_id != @cur_id then
			case
				when t1.encounter_type in (32,33,44,10) or obs regexp "!!(1279|5596)=" then @first_evidence_pt_pregnant := encounter_datetime
				else @first_evidence_pt_pregnant := null
			end
		when @first_evidence_pt_pregnant is null and (t1.encounter_type in (32,33,44,10) or obs regexp "!!(1279|5596)=") then @first_evidence_pt_pregnant := encounter_datetime
		when @first_evidence_pt_pregnant and (t1.encounter_type in (11,47,34) or timestampdiff(week,@first_evidence_pt_pregnant,encounter_datetime) > 40 or timestampdiff(week,@edd,encounter_datetime) > 40 or obs regexp "!!5599=|!!1156=1065!!") then @first_evidence_pt_pregnant := null
		else @first_evidence_pt_pregnant
	end as first_evidence_patient_pregnant,

	# 1836 = LAST MENSTRUAL PERIOD DATE
	# 1279 = NUMBER OF WEEKS PREGNANT
	# 5596 = ESTIMATED DATE OF CONFINEMENT
	# 5599 = DATE OF CONFINEMENT
	# 1146 = CONCEPTION SINCE LAST VISIT

	case
		when @prev_id != @cur_id then
			case
				when @first_evidence_patient_pregnant and obs regexp "!!1836=" then @edd :=
					date_add(replace(replace((substring_index(substring(obs,locate("!!1836=",obs)),@sep,1)),"!!1836=",""),"!!",""),interval 280 day)
				when obs regexp "!!1279=" then @edd :=
					date_add(encounter_datetime,interval (40-replace(replace((substring_index(substring(obs,locate("!!1279=",obs)),@sep,1)),"!!1279=",""),"!!","")) week)
				when obs regexp "!!5596=" then @edd :=
					replace(replace((substring_index(substring(obs,locate("!!5596=",obs)),@sep,1)),"!!5596=",""),"!!","")
				when @first_evidence_pt_pregnant then @edd := date_add(@first_evidence_pt_pregnant,interval 6 month)
				else @edd := null
			end
		when @edd is null or @edd = @first_evidence_pt_pregnant then
			case
				when @first_evidence_pt_pregnant then @edd := date_add(@first_evidence_pt_pregnant,interval 6 month)
				when @first_evidence_patient_pregnant and obs regexp "!!1836=" then @edd :=
					date_add(replace(replace((substring_index(substring(obs,locate("!!1836=",obs)),@sep,1)),"!!1836=",""),"!!",""),interval 280 day)
				when obs regexp "!!1279=" then @edd :=
					date_add(encounter_datetime,interval (40-replace(replace((substring_index(substring(obs,locate("!!1279=",obs)),@sep,1)),"!!1279=",""),"!!","")) week)
				when obs regexp "!!5596=" then @edd :=
					replace(replace((substring_index(substring(obs,locate("!!5596=",obs)),@sep,1)),"!!5596=",""),"!!","")
				when @first_evidence_pt_pregnant then @edd := date_add(@first_evidence_pt_pregnant,interval 6 month)
				else @edd
			end
		when @edd and (t1.encounter_type in (11,47,34) or timestampdiff(week,@edd,encounter_datetime) > 4 or obs regexp "!!5599|!!1145=1065!!") then @edd := null
		else @edd
	end as edd,

	# 6174 = REVIEW OF TUBERCULOSIS SCREENING QUESTIONS
	# 2022 = TUBERCULOSIS DIAGNOSED THIS VISIT
	# 1268 = TUBERCULOSIS TREATMENT PLAN
	# 1270 = TUBERCULOSIS TREATMENT STARTED
	# 1257 = CONTINUE REGIMEN
	# 1256 = START,
	# 1850 = RESTART
	case
		when obs regexp "!!6174=" then @screened_for_tb := true #there is an obs for "any symptoms of tb?"
		when obs regexp "!!2022=1065!!" then @screened_for_tb := true #1065 = yes
		when obs regexp "!!1268=(1256|1850)!!" then @screened_for_tb := true
		when obs regexp "!!1270=" and obs not regexp "!!1268=1257!!" then @screened_for_tb := true
	end as screened_for_tb,


	# 1111 = PATIENT REPORTED CURRENT TUBERCULOSIS TREATMENT
	# 1267 = COMPLETED
	# 1107 = NONE
	case
		when obs regexp "!!1268=(1107|1268)!!" then @tb_treatment_start_date := null
		when obs regexp "!!1268=1256!!" then @tb_treatment_start_date := encounter_datetime
		when obs regexp "!!1268=(1257|1259|1849|981)!!" then
			case
				when @prev_id!=@cur_id or @tb_treatment_start_date is null then @tb_treatment_start_date := encounter_datetime
				else @tb_treatment_start_date
			end
		when obs regexp "!!1111=" and obs not regexp "!!1111=(1267|1107)!!" and @tb_treatment_start_date is null then @tb_treatment_start_date := encounter_datetime
		when @prev_id=@cur_id then @tb_treatment_start_date
		else @tb_treatment_start_date := null
	end as tb_tx_start_date,

	# 1109 = PATIENT REPORTED CURRENT PCP PROPHYLAXIS
	# 1261 = PCP PROPHYLAXIS PLAN
	# 1193 = CURRENT MEDICATIONS
	case
		when obs regexp "!!1261=(1107|1268)!!" then @pcp_prophylaxis_start_date := null
		when obs regexp "!!1261=(1256|1850)!!" then @pcp_prophylaxis_start_date := encounter_datetime
		when obs regexp "!!1261=1257!!" then
			case
				when @prev_id!=@cur_id or @pcp_prophylaxis_start_date is null then @pcp_prophylaxis_start_date := encounter_datetime
				else @pcp_prophylaxis_start_date
			end
		when obs regexp "!!1109=(916|92)!!" and @pcp_prophylaxis_start_date is null then @pcp_prophylaxis_start_date := encounter_datetime
		when obs regexp "!!1193=916!!" and @pcp_prophylaxis_start_date is null then @pcp_prophylaxis_start_date := encounter_datetime
		when @prev_id=@cur_id then @pcp_prophylaxis_start_date
		else @pcp_prophylaxis_start_date := null
	end as pcp_prophylaxis_start_date,

	# 1570 = DATE OF DEATH
	# 1734 = DEATH REPORTED BY
	# 1573 = CAUSE FOR DEATH
	# 1733 = REASON FOR MISSED VISIT
	# 9082 = PATIENT CARE STATUS
	# 6206 = OUTCOME AT END OF TUBERCULOSIS TREATMENT

	case
		when p.dead or p.death_date then @death_date := p.death_date
		when obs regexp "!!1570=" then @death_date := replace(replace((substring_index(substring(obs,locate("!!1570=",obs)),@sep,1)),"!!1570=",""),"!!","")
		when @prev_id != @cur_id or @death_date is null then
			case
				when obs regexp "!!(1734|1573)=" then @death_date := encounter_datetime
				when obs regexp "!!(1733|9082|6206)=159!!" or encounter_type=31 then @death_date := encounter_datetime
				else @death_date := null
			end
		else @death_date
	end as death_date,

	# 5497 = CD4, BY FACS
	case
		when @prev_id=@cur_id then
			case
				when encounter_type = @lab_encounter_type and obs regexp "!!5497=[0-9]" and @cd4_1 >= 0 and date(encounter_datetime)<>@cd4_1_date then @cd4_2:= @cd4_1
				else @cd4_2
			end
		else @cd4_2:=null
	end as cd4_2,

	case
		when @prev_id=@cur_id then
			case
				when encounter_type=@lab_encounter_type and obs regexp "!!5497=[0-9]" and @cd4_1 >= 0 then @cd4_2_date:= @cd4_1_date
				else @cd4_2_date
			end
		else @cd4_2_date:=null
	end as cd4_2_date,

	case
		when encounter_type = @lab_encounter_type and obs regexp "!!5497=[0-9]" then @cd4_date_resulted := date(encounter_datetime)
		when @prev_id = @cur_id and date(encounter_datetime) = @cd4_date_resulted then @cd4_date_resulted
	end as cd4_resulted_date,

	case
		when encounter_type = @lab_encounter_type and obs regexp "!!5497=[0-9]" then @cd4_resulted := cast(replace(replace((substring_index(substring(obs,locate("!!5497=",obs)),@sep,1)),"!!5497=",""),"!!","") as unsigned)
		when @prev_id = @cur_id and date(encounter_datetime) = @cd4_date_resulted then @cd4_resulted
	end as cd4_resulted,



	case
		when encounter_type = @lab_encounter_type and obs regexp "!!5497=[0-9]" then @cd4_1:= cast(replace(replace((substring_index(substring(obs,locate("!!5497=",obs)),@sep,1)),"!!5497=",""),"!!","") as unsigned)
		when @prev_id=@cur_id then @cd4_1
		else @cd4_1:=null
	end as cd4_1,


	case
		when encounter_type = @lab_encounter_type and obs regexp "!!5497=[0-9]" then @cd4_1_date:=date(encounter_datetime)
		when @prev_id=@cur_id then @cd4_1_date
		else @cd4_1_date:=null
	end as cd4_1_date,

	# 730 = CD4%, BY FACS
	case
		when @prev_id=@cur_id then
			case
				when encounter_type=@lab_encounter_type and obs regexp "!!730=[0-9]" and @cd4_percent_1 >= 0
					then @cd4_percent_2:= @cd4_percent_1
				else @cd4_percent_2
			end
		else @cd4_percent_2:=null
	end as cd4_percent_2,

	case
		when @prev_id=@cur_id then
			case
				when obs regexp "!!730=[0-9]" and encounter_type = @lab_encounter_type and @cd4_percent_1 >= 0 then @cd4_percent_2_date:= @cd4_percent_1_date
				else @cd4_percent_2_date
			end
		else @cd4_percent_2_date:=null
	end as cd4_percent_2_date,


	case
		when encounter_type = @lab_encounter_type and obs regexp "!!730=[0-9]"
			then @cd4_percent_1:= cast(replace(replace((substring_index(substring(obs,locate("!!730=",obs)),@sep,1)),"!!730=",""),"!!","") as unsigned)
		when @prev_id=@cur_id then @cd4_percent_1
		else @cd4_percent_1:=null
	end as cd4_percent_1,

	case
		when obs regexp "!!730=[0-9]" and encounter_type = @lab_encounter_type then @cd4_percent_1_date:=date(encounter_datetime)
		when @prev_id=@cur_id then @cd4_percent_1_date
		else @cd4_percent_1_date:=null
	end as cd4_percent_1_date,


	# 856 = HIV VIRAL LOAD, QUANTITATIVE
	case
		when @prev_id=@cur_id then
			case
				when encounter_type = @lab_encounter_type and obs regexp "!!856=[0-9]" and @vl_1 >= 0  then @vl_2:= @vl_1
				else @vl_2
			end
		else @vl_2:=null
	end as vl_2,


	case
		when @prev_id=@cur_id then
			case
				when obs regexp "!!856=[0-9]" and encounter_type = @lab_encounter_type and @vl_1 and date(encounter_datetime)<>date(@vl_1_date) then @vl_2_date:= @vl_1_date
				else @vl_2_date
			end
		else @vl_2_date:=null
	end as vl_2_date,

	case
		when encounter_type = @lab_encounter_type and obs regexp "!!856=[0-9]" then @vl_date_resulted := date(encounter_datetime)
		when @prev_id = @cur_id and date(encounter_datetime) = @vl_date_resulted then @vl_date_resulted
	end as vl_resulted_date,

	case
		when encounter_type = @lab_encounter_type and obs regexp "!!856=[0-9]" then @vl_resulted := cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") as unsigned)
		when @prev_id = @cur_id and date(encounter_datetime) = @vl_date_resulted then @vl_resulted
	end as vl_resulted,



	case
		when obs regexp "!!856=[0-9]" and encounter_type = @lab_encounter_type then @vl_1:=cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") as unsigned)
		when obs regexp "!!856=[0-9]"
				and (@vl_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!",""),@vl_1_date)) > 30)
			then @vl_1 := cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") as unsigned)
		when @prev_id=@cur_id then @vl_1
		else @vl_1:=null
	end as vl_1,

	case
		when obs regexp "!!856=[0-9]" and encounter_type = @lab_encounter_type then @vl_1_date:= encounter_datetime
		when obs regexp "!!856=[0-9]"
				and (@vl_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!",""),@vl_1_date)) > 30)
			then @vl_1_date := replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!","")
		when @prev_id=@cur_id then @vl_1_date
		else @vl_1_date:=null
	end as vl_1_date,

	# 1271 = TESTS ORDERED
	# 856 = HIV VIRAL LOAD, QUANTITATIVE
	case
		when obs regexp "!!1271=856!!" then @vl_order_date := date(encounter_datetime)
		when @prev_id=@cur_id and (@vl_1_date is null or @vl_1_date < @vl_order_date) then @vl_order_date
		else @vl_order_date := null
	end as vl_order_date,

	# 657 = CD4 PANEL
	case
		when obs regexp "!!1271=657!!" then @cd4_order_date := date(encounter_datetime)
		when @prev_id=@cur_id then @cd4_order_date
		else @cd4_order_date := null
	end as cd4_order_date,

		# 1030 = HIV DNA PCR
	case
	  when obs regexp "!!1271=1030!!" then @hiv_dna_pcr_order_date := date(encounter_datetime)
	  when @prev_id=@cur_id then @hiv_dna_pcr_order_date
	  else @hiv_dna_pcr_order_date := null
	end as hiv_dna_pcr_order_date,

	case
	  when encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" then encounter_datetime
	  when obs regexp "!!1030=[0-9]"
		  and (@hiv_dna_pcr_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!1030=",obs_datetimes)),@sep,1)),"!!1030=",""),"!!",""),@hiv_dna_pcr_1_date)) > 30)
		then replace(replace((substring_index(substring(obs_datetimes,locate("1030=",obs_datetimes)),@sep,1)),"1030=",""),"!!","")
	end as hiv_dna_pcr_resulted_date,

	case
	  when @prev_id=@cur_id then
		case
		  when encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" and @hiv_dna_pcr_1 >= 0 and date(encounter_datetime)<>@hiv_dna_pcr_1_date then @hiv_dna_pcr_2:= @hiv_dna_pcr_1
		  when obs regexp "!!1030=[0-9]" and @hiv_dna_pcr_1 >= 0
			and abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!1030=",obs_datetimes)),@sep,1)),"!!1030=",""),"!!",""),@hiv_dna_pcr_1_date)) > 30 then @hiv_dna_pcr_2 := @hiv_dna_pcr_1
		  else @hiv_dna_pcr_2
		end
	  else @hiv_dna_pcr_2:=null
	end as hiv_dna_pcr_2,

	case
	  when @prev_id=@cur_id then
		case
		  when encounter_type=@lab_encounter_type and obs regexp "!!1030=[0-9]" and @hiv_dna_pcr_1 >= 0 and date(encounter_datetime)<>@hiv_dna_pcr_1_date then @hiv_dna_pcr_2_date:= @hiv_dna_pcr_1_date
		  when obs regexp "!!1030=[0-9]" and @hiv_dna_pcr_1 >= 0
			and abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("1030=",obs_datetimes)),@sep,1)),"1030=",""),"!!",""),@hiv_dna_pcr_1_date)) > 30 then @hiv_dna_pcr_2_date:= @hiv_dna_pcr_1_date
		  else @hiv_dna_pcr_2_date
		end
	  else @hiv_dna_pcr_2_date:=null
	end as hiv_dna_pcr_2_date,

	case
	  when encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" then cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
	  when obs regexp "!!1030=[0-9]"
		and (@hiv_dna_pcr_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!1030=",obs_datetimes)),@sep,1)),"!!1030=",""),"!!",""),@hiv_dna_pcr_1_date)) > 30)
		then cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
	end as hiv_dna_pcr_resulted,

	case
	  when encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" then @hiv_dna_pcr_1:= cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
	  when obs regexp "!!1030=[0-9]"
		  and (@hiv_dna_pcr_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!1030=",obs_datetimes)),@sep,1)),"!!1030=",""),"!!","") ,@hiv_dna_pcr_1_date)) > 30)
		then @hiv_dna_pcr_1 := cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
	  when @prev_id=@cur_id then @hiv_dna_pcr_1
	  else @hiv_dna_pcr_1:=null
	end as hiv_dna_pcr_1,


	case
	  when encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" then @hiv_dna_pcr_1_date:=date(encounter_datetime)
	  when obs regexp "!!1030=[0-9]"
		  and (@hiv_dna_pcr_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!1030=",obs_datetimes)),@sep,1)),"!!1030=",""),"!!","") ,@hiv_dna_pcr_1_date)) > 30)
		then @hiv_dna_pcr_1_date := replace(replace((substring_index(substring(obs_datetimes,locate("!!1030=",obs_datetimes)),@sep,1)),"!!1030=",""),"!!","")
	  when @prev_id=@cur_id then @hiv_dna_pcr_1_date
	  else @hiv_dna_pcr_1_date:=null
	end as hiv_dna_pcr_1_date,


	case
		when obs regexp "!!8302=8305!!" then @condoms_provided := 1
		else null
	end as condoms_provided,

	case
		when obs regexp "!!374=(5275|6220|780|5279)!!" then @using_modern_conctaceptive_method := 1
		else null
	end as using_modern_contraceptive_method,

	#Current WHO Stage 5356 - Adults
	#Adult Who Stage 1 - 1204
	#Adult Who Stage 2 - 1205
	#Adult Who Stage 3 - 1206
	#Adult Who Stage 4 - 1207

	#Current WHO Stage 1224 - Peds
	#Adult Who Stage 1 - 1220
	#Adult Who Stage 2 - 1221
	#Adult Who Stage 3 - 1222
	#Adult Who Stage 4 - 1223
	case
		when obs regexp "!!5356=(1204)!!" then @cur_who_stage := 1
		when obs regexp "!!5356=(1205)!!" then @cur_who_stage := 2
		when obs regexp "!!5356=(1206)!!" then @cur_who_stage := 3
		when obs regexp "!!5356=(1207)!!" then @cur_who_stage := 4
		when obs regexp "!!1224=(1220)!!" then @cur_who_stage := 1
		when obs regexp "!!1224=(1221)!!" then @cur_who_stage := 2
		when obs regexp "!!1224=(1222)!!" then @cur_who_stage := 3
		when obs regexp "!!1224=(1223)!!" then @cur_who_stage := 4
		when @prev_id = @cur_id then @cur_who_stage
		else @cur_who_stage := null
	end as cur_who_stage

from flat_hiv_summary_0 t1
	join amrs.person p using (person_id)
);


select @prev_id := null;
select @cur_id := null;
select @prev_encounter_datetime := null;
select @cur_encounter_datetime := null;

select @prev_clinical_datetime := null;
select @cur_clinical_datetime := null;

select @next_encounter_type := null;
select @cur_encounter_type := null;


alter table flat_hiv_summary_1 drop prev_id, drop cur_id;

drop table if exists flat_hiv_summary_2;
create temporary table flat_hiv_summary_2
(select *,
	@prev_id := @cur_id as prev_id,
	@cur_id := person_id as cur_id,

	case
		when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
		else @prev_encounter_datetime := null
	end as next_encounter_datetime_hiv,

	@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

	case
		when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
		else @next_encounter_type := null
	end as next_encounter_type_hiv,

	@cur_encounter_type := encounter_type as cur_encounter_type,

	case
		when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
		else @prev_clinical_datetime := null
	end as next_clinical_datetime_hiv,

	case
		when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
		when @prev_id = @cur_id then @cur_clinical_datetime
		else @cur_clinical_datetime := null
	end as cur_clinic_datetime


	from flat_hiv_summary_1
	order by person_id, encounter_datetime desc
);

alter table flat_hiv_summary_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime;

select @prev_id := null;
select @cur_id := null;
select @prev_encounter_type := null;
select @cur_encounter_type := null;
select @prev_encounter_datetime := null;
select @cur_encounter_datetime := null;
select @prev_clinical_datetime := null;
select @cur_clinical_datetime := null;

drop temporary table if exists flat_hiv_summary_3;
create temporary table flat_hiv_summary_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
(select
	*,
	@prev_id := @cur_id as prev_id,
	@cur_id := t1.person_id as cur_id,

	case
        when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
        else @prev_encounter_type:=null
	end as prev_encounter_type_hiv,

	@cur_encounter_type := encounter_type as cur_encounter_type,

	case
        when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
        else @prev_encounter_datetime := null
	end as prev_encounter_datetime_hiv,
	@cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

	case
		when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
		else @prev_clinical_datetime := null
	end as prev_clinical_datetime_hiv,

	case
		when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
		when @prev_id = @cur_id then @cur_clinical_datetime
		else @cur_clinical_datetime := null
	end as cur_clinical_datetime


	from flat_hiv_summary_2 t1
	order by person_id, encounter_datetime
);

replace into flat_hiv_summary
(select
	person_id,
	t1.uuid,
    encounter_id,
	encounter_datetime,
	encounter_type,
	is_clinical_encounter,
	location_id,
	t2.uuid as location_uuid,
	visit_num,
	enrollment_date,
	hiv_start_date,
	death_date,
	scheduled_visit,
	transfer_in,
	transfer_out,
	out_of_care,
	prev_rtc_date,
	cur_rtc_date,
	arv_start_date,
	arv_first_regimen,
	cur_arv_meds,
	cur_arv_line,
    first_evidence_patient_pregnant,
    edd,
	screened_for_tb,
	tb_tx_start_date,
	pcp_prophylaxis_start_date,
	cd4_resulted,
	cd4_resulted_date,
    cd4_1,
    cd4_1_date,
    cd4_2,
    cd4_2_date,
    cd4_percent_1,
	cd4_percent_1_date,
    cd4_percent_2,
	cd4_percent_2_date,
	vl_resulted,
	vl_resulted_date,
    vl_1,
    vl_1_date,
    vl_2,
    vl_2_date,
    vl_order_date,
    cd4_order_date,
	hiv_dna_pcr_order_date,
	hiv_dna_pcr_resulted,
	hiv_dna_pcr_resulted_date,
	hiv_dna_pcr_1,
	hiv_dna_pcr_1_date,
	hiv_dna_pcr_2,
	hiv_dna_pcr_2_date,

	condoms_provided,
	using_modern_contraceptive_method,
	cur_who_stage,
	prev_encounter_datetime_hiv,
	next_encounter_datetime_hiv,
	prev_encounter_type_hiv,
	next_encounter_type_hiv,
	prev_clinical_datetime_hiv,
	next_clinical_datetime_hiv

from flat_hiv_summary_3 t1
	join amrs.location t2 using (location_id));


#select * from flat_hiv_summary order by person_id, encounter_datetime;

select @end := now();
insert into flat_log values (@last_date_created,@table_version,timestampdiff(second,@start,@end));
select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");
