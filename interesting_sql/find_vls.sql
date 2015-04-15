
select @id := patient_id from amrs.patient_identifier where identifier="2169TS-8";
select person_id, obs_datetime, ext_hiv_vl_quant from flat_ext_data where person_id=@id and ext_hiv_vl_quant >= 0 ORDER BY obs_datetime desc;