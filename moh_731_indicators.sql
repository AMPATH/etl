#********************************************************************************************************
#* CREATION OF MOH 731 INDICATORS TABLE ****************************************************************************
#********************************************************************************************************

# Need to first create this temporary table to sort the data by person,encounterdateime. 
# This allows us to use the previous row's data when making calculations.
# It seems that if you don't create the temporary table first, the sort is applied 
# to the final result. Any references to the previous row will not an ordered row. 

drop table if exists flat_moh_731_indicators_1;
create temporary table flat_moh_731_indicators_1(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
(select *
	from amrs.encounter e
		join flat_new_person_data t0 on e.patient_id = t0.person_id
	where encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21)
	order by t0.person_id, e.encounter_datetime
);

select @prev_id := null;
select @cur_id := null;
select @prev_encounter_type := null;
select @cur_encounter_type := null;

select @arv_start_date := null;
select @cur_arv_line := null;

select @first_evidence_pt_pregnant := null;
select @edd := null;
select @preg_1 := null;
select @preg_2 := null;
select @preg_3 := null;
select @cur_arv_meds := null;

#TO DO
# on pcp prophy
# eligible for pcp prophy
# way of calculating enrolled in care
# on tb tx
# way of calculating cumumlative ever
# starting regimen
# screened for tb
# screened for cervical ca
# scheduled/unscheduled
# provided with condoms
# modern contraceptive methods



drop temporary table if exists flat_moh_731_indicators_2;
create temporary table flat_moh_731_indicators_2 (arv_start_date datetime, first_evidence_patient_pregnant datetime, edd datetime, preg_1 datetime, preg_2 datetime, preg_3 datetime, cur_arv_meds varchar(500), index person_enc (person_id, encounter_datetime desc))
(select 
	t1.person_id,
	t1.encounter_id,
	t1.encounter_datetime,	
	@prev_id := @cur_id as prev_id, 
	@cur_id := t1.patient_id as cur_id,
	case
		when arv_plan = 1256 then @arv_start_date := t1.encounter_datetime
		when arv_plan in (1107,1260) then @arv_start_date := null
		when @prev_id = @cur_id and arv_started is not null and @arv_start_date is null then @arv_start_date := t1.encounter_datetime
		when @prev_id = @cur_id and arvs_current is not null and @arv_start_date is null then @arv_start_date := t1.encounter_datetime
		when @prev_id != @cur_id then @arv_start_date := null
		else @arv_start_date
	end as arv_start_date,

	case
		when arv_plan in (1107,1260) then @cur_arv_meds := null
		when arv_started then @cur_arv_meds := arv_started
		when arvs_current then @cur_arv_meds := arvs_current
		when @prev_id=@cur_id then @cur_arv_meds
		else @cur_arv_meds:= null
	end as cur_arv_meds,

	case
		when arv_plan in (1107,1260) then @cur_arv_line := null
		when arv_started regexp '[[:<:]]6467|6964|792|633|631[[:>:]]' then @cur_arv_line := 1
		when arv_started regexp '[[:<:]]794[[:>:]]' then @cur_arv_line := 2
		when arv_started regexp '[[:<:]]6156[[:>:]]' then @cur_arv_line := 3
		when arvs_current regexp '[[:<:]]6467|6964|792|633|631[[:>:]]' then @cur_arv_line := 1
		when arvs_current regexp '[[:<:]]794[[:>:]]' then @cur_arv_line := 2
		when arvs_current regexp '[[:<:]]6156[[:>:]]' then @cur_arv_line := 3
		when @prev_id = @cur_id then @cur_arv_line
		else @cur_arv_line := null
	end as cur_arv_line,

	case
		when @first_arv_regimen is null and @cur_arv_meds is not null then @first_arv_regimen := @cur_arv_meds_code
		when @prev_id = @cur_id then @first_arv_regimen
		else @first_arv_regimen := null
	end as first_arv_regimen,

	case
		when @prev_id != @cur_id then
			case
				when encounter_type in (32,33,44,10) /*or is_pregnant=1065*/ or num_weeks_preg or expected_delivery_date then @first_evidence_pt_pregnant := encounter_datetime
				else @first_evidence_pt_pregnant := null
			end
		when @first_evidence_pt_pregnant is null and (encounter_type in (32,33,44,10) /*or is_pregnant=1065*/ or num_weeks_preg or expected_delivery_date) then @first_evidence_pt_pregnant := encounter_datetime
		when @first_evidence_pt_pregnant and (encounter_type in (11,47,34) or timestampdiff(week,@first_evidence_pt_pregnant,encounter_datetime) > 40 or timestampdiff(week,@edd,encounter_datetime) > 4 or actual_delivery_date or delivered_since_last_visit=1065) then @first_evidence_pt_pregnant := null
		else @first_evidence_pt_pregnant
	end as first_evidence_patient_pregnant,
	
	case
		when @prev_id != @cur_id then
			case
				when @first_evidence_patient_pregnant and lmp then @edd := date_add(lmp,interval 280 day)
				when num_weeks_preg then @edd := date_add(encounter_datetime,interval (40-num_weeks_preg) week)
				when expected_due_date then @edd := expected_due_date
				when @first_evidence_pt_pregnant then @edd := date_add(@first_evidence_pt_pregnant,interval 6 month)
				else @edd := null
			end
		when @edd is null or @edd = @first_evidence_pt_pregnant then
			case
				when @first_evidence_pt_pregnant and lmp then @edd := date_add(lmp,interval 280 day)
				when num_weeks_preg then @edd := date_add(encounter_datetime,interval (40-num_weeks_preg) week)
				when expected_due_date then @edd := expected_due_date
				when @first_evidence_pt_pregnant then @edd := date_add(@first_evidence_pt_pregnant,interval 6 month)
				else @edd
			end
		when @edd and (encounter_type in (11,47,34) or timestampdiff(week,@edd,encounter_datetime) > 4 or actual_delivery_date or delivered_since_last_visit=1065) then @edd := null
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
		when tb_tx_plan in (1256) then @tb_treatment_start_date := encounter_datetime
		when tb_tx_plan in (1257,1259,1849,981) then
			case
				when @prev_id!=@cur_id or @tb_treatment_start_date is null then @tb_treatment_start_date := encounter_datetime
				else @tb_treatment_start_date
			end
		when (tb_tx_current_plan !=1267 or tb_tx_started !=1267) and @tb_treatment_start_date is null then @tb_treatment_start_date := encounter_datetime
		when @prev_id=@cur_id then @tb_treatment_start_date
		else @tb_treatment_start_date := null
	end as tb_treatment_start_date


	from flat_moh_731_indicators_1 t1
		left outer join flat_arvs t2 using (encounter_id, person_id)
		left outer join flat_drug t3 using (encounter_id, person_id)
		left outer join flat_maternity t4 using (encounter_id, person_id)
		left outer join flat_tb t6 using (encounter_id, person_id)
);		
	

#drop table if exists flat_moh_731_indicators;
create table if not exists flat_moh_731_indicators (
	person_id int,
    encounter_id int,
	encounter_datetime datetime,
	arv_start_date datetime,
	cur_arv_meds varchar(500),
	cur_arv_line int,
    first_evidence_patient_pregnant datetime,
    edd datetime,
    preg_1 datetime,
    preg_2 datetime,
    preg_3 datetime,
    primary key encounter_id (encounter_id),
    index person_id (person_id)
);

delete t1
from flat_moh_731_indicators t1
join flat_new_person_data t2 using (person_id);

insert into flat_moh_731_indicators
(select 
	person_id,
    encounter_id,
	encounter_datetime,
	arv_start_date,
	cur_arv_meds,
	cur_arv_line,
    first_evidence_patient_pregnant,
    edd,
    preg_1,
    preg_2,
    preg_3
from flat_moh_731_indicators_2);




