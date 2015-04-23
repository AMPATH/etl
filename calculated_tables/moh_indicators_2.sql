#********************************************************************************************************
#* CREATION OF MOH INDICATORS TABLE ****************************************************************************
#********************************************************************************************************

# Need to first create this temporary table to sort the data by person,encounterdateime. 
# This allows us to use the previous row's data when making calculations.
# It seems that if you don't create the temporary table first, the sort is applied 
# to the final result. Any references to the previous row will not an ordered row. 

/*
select @b := "a=b,5096=1,5096=2,5096=3";
select 
	replace(
		(substring_index(
			substring(@b,locate("5096=",@b)),
			",",
			ROUND (   
				(
					LENGTH(@b) - LENGTH( REPLACE ( @b, "5096", "") ) 
				) 
				/ LENGTH("5096")        
			)
		)),"5096=","")
;
*/
/*
drop table if exists flat_moh_indicators_0;
create temporary table flat_moh_indicators_0(index encounter_id (encounter_id), index person_enc (patient_id,encounter_datetime))
(select * from 
	((select t0.person_id, e.encounter_id, e.encounter_datetime, e.encounter_type,e.location_id
		from amrs.encounter e
			join flat_new_person_data t0 using (person_id)
			join flat_obs t1 using (encounter_id)
		where encounter_type in (1,2,3,4,5,6,7,8,9,10,13,14,15,17,19,22,23,26,43,47,21)
			and voided=0 
		order by t0.person_id, e.encounter_datetime
	)

	union

	(select t0.person_id, t0.encounter_id, t0.obs_datetime as encounter_datetime, 99999 as encounter_type, null as location_id
		from flat_obs_no_encounter t0
			join flat_new_person_data t1 using(person_id)
	)) t1
	order by person_id, encounter_datetime

);

*/
drop table if exists flat_moh_indicators_0;
create temporary table flat_moh_indicators_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
(select 
	t1.person_id, 
	t1.encounter_id, 
	t1.encounter_datetime,
	if(e.encounter_type,e.encounter_type,'9999') as encounter_type,
	if(e.location_id,e.location_id,null) as location_id,
	t1.obs
	from flat_obs t1
		join flat_new_person_data t0 using (person_id)
		left outer join amrs.encounter e using (encounter_id)
	where encounter_type in (1,2,3,4,5,6,7,8,9,10,13,14,15,17,19,22,23,26,43,47,21)
		and voided=0 
	order by t1.person_id, encounter_datetime
);

select @prev_id := null;
select @cur_id := null;
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


#TO DO
# enrolled in care
# screened for cervical ca
# provided with condoms
# modern contraceptive methods
# exposed infant

drop temporary table if exists flat_moh_indicators_1;
create temporary table flat_moh_indicators_1 (index encounter_id (encounter_id))
(select 
	@prev_id := @cur_id as prev_id, 
	@cur_id := t1.person_id as cur_id,
	t1.person_id,
	t1.encounter_id,
	t1.encounter_datetime,			

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
		when obs regexp "5096=" then @cur_rtc_date := SUBSTRING_INDEX(SUBSTRING_INDEX(obs, '5096=', -1), ',', 1)
		when @prev_id = @cur_id then @cur_rtc_date
		else @cur_rtc_date := null
	end as cur_rtc_date,

	# 1285 = TRANSFER CARE TO OTHER CENTER
	# 1596 = REASON EXITED CARE
	# 9082 = PATIENT CARE STATUS
	case
		when obs regexp "1285=(1287|9068)" then 1
		when obs regexp "1596=1594" then 1
		when obs regexp "9082=(1287|9068)" then 1
		else null
	end as transfer_out,

	# 1946 = DISCONTINUE FROM CLINIC, HIV NEGATIVE
	# 1088 = CURRENT ANTIRETROVIRAL DRUGS USED FOR TREATMENT
	# 1255 = ANTIRETROVIRAL PLAN
	# 1040 = HIV RAPID TEST, QUALITATIVE
	# 1030 = HIV DNA POLYMERASE CHAIN REACTION, QUALITATIVE
	# 664 = POSITIVE
	case
		when obs regexp "1946=1065" then @hiv_start_date := null
		when encounter_type='9999' and obs regexp "(1040|1030)=664" then @hiv_start_date:=null
		when @prev_id != @cur_id or @hiv_start_date is null then
			case
				when obs regexp "(1040|1030)=664" then @hiv_start_date := encounter_datetime
				when obs regexp "1088=|1255=" then @hiv_start_date := encounter_datetime
				else @hiv_start_date := null 
			end
		else @hiv_start_date
	end as hiv_start_date,

	# 1255 = ANTIRETROVIRAL PLAN
	# 1250 = ANTIRETROVIRALS STARTED
	# 1088 = CURRENT ANTIRETROVIRAL DRUGS USED FOR TREATMENT
	# 2154 = PATIENT REPORTED CURRENT ANTIRETROVIRAL TREATMENT
	case
		when obs regexp "1255=1256" then @arv_start_date := t1.encounter_datetime
		when obs regexp "1255=(1107|1260)" then @arv_start_date := null
		when @prev_id = @cur_id and obs regexp "1250=" and @arv_start_date is null then @arv_start_date := t1.encounter_datetime
		when @prev_id = @cur_id and obs regexp "1088=" and @arv_start_date is null then @arv_start_date := t1.encounter_datetime
		when @prev_id = @cur_id and obs regexp "2154=" and @arv_start_date is null then @arv_start_date := t1.encounter_datetime
		when @prev_id != @cur_id then @arv_start_date := null
		else @arv_start_date
	end as arv_start_date,

	# 1255 = ANTIRETROVIRAL PLAN
	# 1250 = ANTIRETROVIRALS STARTED
	# 1088 = CURRENT ANTIRETROVIRAL DRUGS USED FOR TREATMENT
	# 2154 = PATIENT REPORTED CURRENT ANTIRETROVIRAL TREATMENT
	case
		when obs regexp "1255=(1107|1260)" then @cur_arv_meds := null
		when obs regexp "1250=" then @cur_arv_meds :=  
			replace((substring_index(substring(obs,locate("1250=",obs)),",",ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "1250=", "") ) ) / LENGTH("1250=") ))),"1250=","")
		when obs regexp "1088=" then @cur_arv_meds := 
			replace((substring_index(substring(obs,locate("1088=",obs)),",",ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "1088=", "") ) ) / LENGTH("1250=") ))),"1088=","")
		when obs regexp "2154=" then @cur_arv_meds := 
			replace((substring_index(substring(obs,locate("2154=",obs)),",",ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "2154=", "") ) ) / LENGTH("1250=") ))),"2154=","")
		when @prev_id=@cur_id then @cur_arv_meds
		else @cur_arv_meds:= null
	end as cur_arv_meds,


	case
		when @arv_first_regimen is null and @cur_arv_meds is not null then @arv_first_regimen := @cur_arv_meds
		when @prev_id = @cur_id then @arv_first_regimen
		else @arv_first_regimen := null
	end as arv_first_regimen,


	
	case
		when obs regexp "1255=(1107|1260)" then @cur_arv_line := null
		when obs regexp "1250=(6467|6964|792|633|631)" then @cur_arv_line := 1
		when obs regexp "1250=794" then @cur_arv_line := 2
		when obs regexp "1250=6156" then @cur_arv_line := 3
		when obs regexp "1088=(6467|6964|792|633|631)" then @cur_arv_line := 1
		when obs regexp "1088=794" then @cur_arv_line := 2
		when obs regexp "1088=6156" then @cur_arv_line := 3
		when obs regexp "2154=(6467|6964|792|633|631)" then @cur_arv_line := 1
		when obs regexp "2154=794" then @cur_arv_line := 2
		when obs regexp "2154=6156" then @cur_arv_line := 3
		when @prev_id = @cur_id then @cur_arv_line
		else @cur_arv_line := null
	end as cur_arv_line,

	# 1279 = NUMBER OF WEEKS PREGNANT
	# 5596 = ESTIMATED DATE OF CONFINEMENT

	case
		when @prev_id != @cur_id then
			case
				when t1.encounter_type in (32,33,44,10) or obs regexp "1279|5596" then @first_evidence_pt_pregnant := encounter_datetime
				else @first_evidence_pt_pregnant := null
			end
		when @first_evidence_pt_pregnant is null and (t1.encounter_type in (32,33,44,10) or obs regexp "1279|5596") then @first_evidence_pt_pregnant := encounter_datetime
		when @first_evidence_pt_pregnant and (t1.encounter_type in (11,47,34) or timestampdiff(week,@first_evidence_pt_pregnant,encounter_datetime) > 40 or timestampdiff(week,@edd,encounter_datetime) > 4 or actual_delivery_date or delivered_since_last_visit=1065) then @first_evidence_pt_pregnant := null
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
				when @first_evidence_patient_pregnant and obs regexp "1836=" then @edd := 
					date_add(replace((substring_index(substring(obs,locate("1836=",obs)),",",1)),"1836=",""),interval 280 day)
				when obs regexp "1279=" then @edd := 
					date_add(encounter_datetime,interval (40-replace((substring_index(substring(obs,locate("1279=",obs)),",",1)),"1279=","")) week)
				when obs regexp "5596=" then @edd := 
					replace((substring_index(substring(obs,locate("5596=",obs)),",",1)),"5596=","")
				when @first_evidence_pt_pregnant then @edd := date_add(@first_evidence_pt_pregnant,interval 6 month)
				else @edd := null
			end
		when @edd is null or @edd = @first_evidence_pt_pregnant then
			case
				when @first_evidence_pt_pregnant then @edd := date_add(@first_evidence_pt_pregnant,interval 6 month)
				when @first_evidence_patient_pregnant and obs regexp "1836=" then @edd := 
					date_add(replace((substring_index(substring(obs,locate("1836=",obs)),",",1)),"1836=",""),interval 280 day)
				when obs regexp "1279=" then @edd := 
					date_add(encounter_datetime,interval (40-replace((substring_index(substring(obs,locate("1279=",obs)),",",1)),"1279=","")) week)
				when obs regexp "5596=" then @edd := 
					replace((substring_index(substring(obs,locate("5596=",obs)),",",1)),"5596=","")												
				when @first_evidence_pt_pregnant then @edd := date_add(@first_evidence_pt_pregnant,interval 6 month)
				else @edd
			end
		when @edd and (t1.encounter_type in (11,47,34) or timestampdiff(week,@edd,encounter_datetime) > 4 or obs regexp "5599|1145=1065") then @edd := null
		else @edd	
	end as edd		



from flat_moh_indicators_0 t1
);

/*	
			
	case 
		when tb_symptom is not null then @screened_for_tb := true #there is an obs for "any symptoms of tb?"
		when tb_dx_this_visit = 1065 then @screened_for_tb := true #1065 = yes
		when tb_tx_plan in (1256,1850) then @screened_for_tb := true #1256=start, 1850=restart
		when tb_tx_started and tb_tx_plan != 1257 then @screened_for_tb := true #1257=continue
	end as screened_for_tb,

	case 
		when tb_tx_plan in (1256) then @tb_treatment_start_date := encounter_datetime
		when tb_tx_plan in (1257,1259,1849,981) then
			case
				when @prev_id!=@cur_id or @tb_treatment_start_date is null then @tb_treatment_start_date := encounter_datetime
				else @tb_treatment_start_date
			end
		when (tb_tx_current_plan !=1267 or tb_tx_plan !=1267) and @tb_treatment_start_date is null then @tb_treatment_start_date := encounter_datetime
		when @prev_id=@cur_id then @tb_treatment_start_date
		else @tb_treatment_start_date := null
	end as tb_tx_start_date,

	case 
		when pcp_prophy_plan in (1256,1850) then @pcp_prophylaxis_start_date := encounter_datetime
		when pcp_prophy_plan=1257 then
			case
				when @prev_id!=@cur_id or @pcp_prophylaxis_start_date is null then @pcp_prophylaxis_start_date := encounter_datetime
				else @pcp_prophylaxis_start_date
			end
		when pcp_prophy_current in (916,92) and @pcp_prophylaxis_start_date is null then @pcp_prophylaxis_start_date := encounter_datetime
		when current_meds regexp '[[:<:]]916[[:>:]]' and @pcp_prophylaxis_start_date is null then @pcp_prophylaxis_start_date := encounter_datetime
		when @prev_id=@cur_id then @pcp_prophylaxis_start_date
		else @pcp_prophylaxis_start_date := null
	end as pcp_prophylaxis_start_date,

	case
		when p.dead or p.death_date then @death_date := p.death_date
		when t8.death_date then @death_date := t8.death_date
		when @prev_id != @cur_id or @death_date is null then 
			case
				when t8.death_reported_by or t8.death_cause then @death_date := encounter_datetime
				when reason_for_missed_visit = 159 or patient_care_status=159 or encounter_type=31 or tb_tx_outcome=159 then @death_date := encounter_datetime
				else @death_date := null
			end
		else @death_date
	end as death_date,

	case
		when @prev_id=@cur_id then
			case 
				when ext_cd4_count >= 0 and @cd4_1 >= 0 and date(encounter_datetime)<>@cd4_1_date then @cd4_2:= @cd4_1
				when int_cd4_count >= 0 and @cd4_1 >= 0 and datediff(int_cd4_count_date,@cd4_1_date) > 30 then @cd4_2 := @cd4_1
				else @cd4_2
			end
		else @cd4_2:=null
	end as cd4_2,

	case 
		when @prev_id=@cur_id then
			case
				when ext_cd4_count >= 0 and @cd4_1 >= 0 and date(encounter_datetime)<>@cd4_1_date then @cd4_2_date:= @cd4_1_date
				when int_cd4_count >= 0 and @cd4_1 >= 0 and datediff(int_cd4_count_date,@cd4_1_date) > 30 then @cd4_2_date:= @cd4_1_date
				else @cd4_2_date
			end
		else @cd4_2_date:=null
	end as cd4_2_date,

	case 
		when ext_cd4_count >= 0 then ext_cd4_count
		when int_cd4_count >= 0 and (@cd4_1_date is null or datediff(int_cd4_count_date,@cd4_1_date) > 30) then int_cd4_count
	end as cd4_resulted,

	case 
		when ext_cd4_count >= 0 then encounter_datetime
		when int_cd4_count >= 0 and (@cd4_1_date is null or datediff(int_cd4_count_date,@cd4_1_date) > 30) then int_cd4_count_date
	end as cd4_resulted_date,


	case 
		when ext_cd4_count >= 0 then @cd4_1:= ext_cd4_count
		when int_cd4_count >= 0 and (@cd4_1_date is null or datediff(int_cd4_count_date,@cd4_1_date) > 30) then @cd4_1 := int_cd4_count
		when @prev_id=@cur_id then @cd4_1
		else @cd4_1:=null
	end as cd4_1,

	case 
		when ext_cd4_count >= 0 then @cd4_1_date:=date(encounter_datetime)
		when int_cd4_count >= 0 and (@cd4_1_date is null or datediff(int_cd4_count_date,@cd4_1_date) > 30) then @cd4_1_date := date(int_cd4_count_date)
		when @prev_id=@cur_id then @cd4_1_date
		else @cd4_1_date:=null
	end as cd4_1_date,


	case
		when @prev_id=@cur_id then
			case 
				when ext_cd4_percent >= 0 and @cd4_percent_1 >= 0 and date(encounter_datetime)<>@cd4_percent_1_date then @cd4_percent_2:= @cd4_percent_1
				when int_cd4_percent >= 0 and @cd4_percent_1 >= 0 and datediff(int_cd4_percent_date,@cd4_percent_1_date) > 30 then @cd4_percent_2 := @cd4_percent_1
				else @cd4_percent_2
			end
		else @cd4_percent_2:=null
	end as cd4_percent_2,

	case 
		when @prev_id=@cur_id then
			case
				when ext_cd4_percent >= 0 and @cd4_percent_1 >= 0 and date(encounter_datetime)<>@cd4_percent_1_date then @cd4_percent_2_date:= @cd4_percent_1_date
				when int_cd4_percent >= 0 and @cd4_percent_1 >= 0 and datediff(int_cd4_percent_date,@cd4_percent_1_date) > 30 then @cd4_percent_2_date:= @cd4_percent_1_date
				else @cd4_percent_2_date
			end
		else @cd4_percent_2_date:=null
	end as cd4_percent_2_date,


	case
		when ext_cd4_percent >= 0 then @cd4_percent_1:= ext_cd4_percent
		when int_cd4_percent >= 0 and (@cd4_percent_1_date is null or datediff(int_cd4_percent_date,@cd4_1_date) > 30) then @cd4_percent_1 := int_cd4_percent
		when @prev_id=@cur_id then @cd4_percent_1
		else @cd4_percent_1:=null
	end as cd4_percent_1,

	case 
		when ext_cd4_percent >= 0 then @cd4_percent_1_date:=date(encounter_datetime)
		when int_cd4_percent >= 0 and (@cd4_percent_1_date is null or datediff(int_cd4_percent_date,@cd4_percent_1_date) > 30) then @cd4_percent_1_date := date(int_cd4_percent_date)
		when @prev_id=@cur_id then @cd4_percent_1_date
		else @cd4_percent_1_date:=null
	end as cd4_percent_1_date,


	case
		when @prev_id=@cur_id then
			case
				when ext_hiv_vl_quant >= 0 and @vl_1 >= 0 and date(encounter_datetime)<>@vl_1_date then @vl_2:= @vl_1
				when int_hiv_vl_quant >= 0 and @vl_1 >= 0 and datediff(int_hiv_vl_quant_date,@vl_1_date) > 30 then @vl_2 := @vl_1
				else @vl_2
			end
		else @vl_2:=null
	end as vl_2,


	case 
		when @prev_id=@cur_id then
			case 
				when ext_hiv_vl_quant >= 0 and @vl_1 and date(encounter_datetime)<>date(@vl_1_date) then @vl_2_date:= @vl_1_date
				when int_hiv_vl_quant >= 0 and @vl_1 >= 0 and datediff(int_hiv_vl_quant_date,@vl_1_date) > 30 then @vl_2_date := @vl_1_date
				else @vl_2_date
			end
		else @vl_2_date:=null
	end as vl_2_date,


	case 
		when ext_hiv_vl_quant >= 0 then ext_hiv_vl_quant
		when int_hiv_vl_quant >= 0 and (@vl_1_date is null or datediff(int_hiv_vl_quant_date,@vl_1_date) > 30) then @vl_1 := int_hiv_vl_quant
	end as vl_resulted,

	case 
		when ext_hiv_vl_quant >= 0 then encounter_datetime
		when int_hiv_vl_quant >= 0 and (@vl_1_date is null or datediff(int_hiv_vl_quant_date,@vl_1_date) > 30) then int_hiv_vl_quant_date
	end as vl_resulted_date,


	case 
		when ext_hiv_vl_quant >= 0 then @vl_1:=ext_hiv_vl_quant
		when int_hiv_vl_quant >= 0 and (@vl_1_date is null or datediff(int_hiv_vl_quant_date,@vl_1_date) > 30) then @vl_1 := int_hiv_vl_quant
		when @prev_id=@cur_id then @vl_1
		else @vl_1:=null
	end as vl_1,

	case
		when ext_hiv_vl_quant >= 0 then @vl_1_date:= encounter_datetime
		when int_hiv_vl_quant >= 0 and (@vl_1_date is null or datediff(int_hiv_vl_quant_date,@vl_1_date) > 30) then @vl_1_date := int_hiv_vl_quant_date
		when @prev_id=@cur_id then @vl_1_date
		else @vl_1_date:=null
	end as vl_1_date,
	
	case
		when tests_ordered = 856 then @vl_order_date := date(encounter_datetime)
		when @prev_id=@cur_id then @vl_order_date
		else @vl_order_date := null
	end as vl_order_date,
	
	case
		when tests_ordered = 657 then @cd4_order_date := date(encounter_datetime)
		when @prev_id=@cur_id then @cd4_order_date
		else @cd4_order_date := null
	end as cd4_order_date
*/
	
from flat_moh_indicators_0 t1
);


#drop table if exists flat_moh_indicators;
create table if not exists flat_moh_indicators (
	person_id int,
    encounter_id int,
	encounter_datetime datetime,
	location_id int,
	visit_num int,
	death_date datetime,
	scheduled_visit int,
	transfer_out int,
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
    primary key encounter_id (encounter_id),
    index person_date (person_id, encounter_datetime)
);

delete t1
from flat_moh_indicators t1
join flat_new_person_data t2 using (person_id);

insert into flat_moh_indicators
(select 
	person_id,
    encounter_id,
	encounter_datetime,
	location_id,
	visit_num,
	death_date,
	scheduled_visit,
	transfer_out,
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
    cd4_order_date

from flat_moh_indicators_1);