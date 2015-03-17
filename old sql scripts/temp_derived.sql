drop temporary table if exists flat_hiv_derived_2;
create temporary table flat_hiv_derived_2 (arv_start_date datetime, who_stage integer, arv_start_cd4_count integer, arv_start_height double, arv_start_weight double, arv_eligibility_reason integer, arv_start_who_stage integer, pcp_prophylaxis_start_date datetime, tb_prophylaxis_start_date datetime, tb_treatment_start_date datetime, first_evidence_patient_pregnant datetime, edd datetime, preg_1 datetime, preg_2 datetime, preg_3 datetime, height double, weight double, cur_arv_meds text, first_substitution_date datetime, first_substitution text, first_substitution_reason integer, second_substitution_date datetime, second_substitution varchar(10), second_substitution_reason integer,second_line varchar(10), second_line_date datetime, second_line_reason integer,sl_first_substitution_date datetime, sl_first_substitution varchar(10), sl_first_substitution_reason integer, sl_second_substitution_date datetime, sl_second_substitution varchar(10), sl_second_substitution_reason integer, index person_enc (person_id, encounter_datetime desc))
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
	when arv_start_date then @arv_start_date := arv_start_date
	when arv_plan = 1256 then @arv_start_date := encounter_datetime
	when arv_plan in (1107,1260) then @arv_start_date := null
	when arv_meds_plan is not null and @prev_arv_start_date is null then @arv_start_date := encounter_datetime
	when arv_meds is not null and @prev_arv_start_date is null then @arv_start_date := encounter_datetime
	else @arv_start_date := @prev_arv_start_date
end as arv_start_date,


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
