CREATE DEFINER = `etl_user` @`%` PROCEDURE `generate_flat_lung_cancer_screening_v1_0`(
  IN query_type VARCHAR(50),
  IN queue_number INT,
  IN queue_size INT,
  IN cycle_size int
) BEGIN
set @primary_table := "flat_lung_cancer_screening";
set @query_type = query_type;
set @total_rows_written = 0;
set @encounter_types = "(177,185)";
set @clinical_encounter_types = "(-1)";
set @non_clinical_encounter_types = "(-1)";
set @other_encounter_types = "(-1)";
set @start = now();
set @table_version = "flat_lung_cancer_screening_v1.0";
set session sort_buffer_size = 512000000;
set @sep = " ## ";
set @boundary = "!!";
set @last_date_created = (
    select max(max_date_created)
    from etl.flat_obs
  );
CREATE TABLE IF NOT EXISTS flat_lung_cancer_screening (
  date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  person_id INT,
  encounter_id INT,
  encounter_type INT,
  encounter_datetime datetime,
  visit_id INT,
  location_id INT,
  location_uuid VARCHAR(100),
  gender CHAR(100),
  age INT,
  death_date DATE,
  cur_visit_type INT,
  referred_from INT,
  referring_health_facility VARCHAR(150),
  referral_date DATE,
  referred_by INT,
  chief_complaints VARCHAR(100),
  other_complaints_freetext VARCHAR(200),
  complaint_duration VARCHAR(100),
  no_of_days_with_complaint INT,
  no_of_weeks_with_complaint INT,
  no_of_months_with_complaint INT,
  no_of_years_with_complaint INT,
  smokes_cigarettes INT,
  sticks_smoked_per_day INT,
  years_of_smoking_cigarettes INT,
  uses_tobacco INT,
  years_of_tobacco_use INT,
  main_occupation INT,
  main_occupation_freetext VARCHAR(150),
  chemical_exposure VARCHAR(100),
  other_chemical_exposure_freetext VARCHAR(150),
  asbestos_exposure INT,
  family_history_of_cancer VARCHAR(100),
  hiv_status INT,
  previous_treatment_for_tuberculosis INT,
  duration_in_months_of_previous_tb_treatment INT,
  no_of_previous_tb_treatments VARCHAR(100),
  previous_tb_investigation_done VARCHAR(100),
  chest_xray_results VARCHAR(100),
  chest_xray_results_date DATE,
  gene_expert_results VARCHAR(100),
  gene_expert_results_date DATE,
  sputum_for_fast_acid_bacilli_results VARCHAR(100),
  sputum_for_fast_acid_bacilli_results_date DATE,
  other_previous_lab_test_done_freetext VARCHAR(150),
  procedure_ordered INT,
  imaging_test_ordered INT,
  other_radiology_orders_freetext VARCHAR(100),
  follow_up_care_plan INT,
  other_follow_up_care_plan_freetext VARCHAR(100),
  assessment_notes_screening VARCHAR(1000),
  rtc_date_screening DATE,
  chest_xray_findings INT,
  date_of_chest_xray DATE,
  chest_ct_scan_findings INT,
  lung_mass_laterality INT,
  other_imaging_findings_freetext VARCHAR(100),
  imaging_workup_date DATE,
  imaging_results_description VARCHAR(1000),
  was_biopsy_procedure_done INT,
  biopsy_workup_date DATE,
  lung_biopsy_results INT,
  diagnosis_date DATE,
  type_of_malignancy INT,
  other_malignancy_freetext VARCHAR(100),
  lung_cancer_type INT,
  non_small_cell_lung_cancer_type INT,
  non_cancer_respiratory_disease_type INT,
  other_condition_freetext VARCHAR(150),
  referrals_ordered VARCHAR(150),
  other_referrals_ordered_freetext VARCHAR(150),
  assessment_notes_screening_results VARCHAR(1000),
  rtc_date_screening_results DATE,
  prev_encounter_datetime_lung_cancer_screening datetime,
  next_encounter_datetime_lung_cancer_screening datetime,
  prev_encounter_type_lung_cancer_screening mediumint,
  next_encounter_type_lung_cancer_screening mediumint,
  prev_clinical_datetime_lung_cancer_screening datetime,
  next_clinical_datetime_lung_cancer_screening datetime,
  prev_clinical_location_id_lung_cancer_screening mediumint,
  next_clinical_location_id_lung_cancer_screening mediumint,
  prev_clinical_rtc_date_lung_cancer_screening datetime,
  next_clinical_rtc_date_lung_cancer_screening datetime,
  primary key encounter_id (encounter_id),
  index person_date (person_id, encounter_datetime),
  index location_enc_date (location_uuid, encounter_datetime),
  index enc_date_location (encounter_datetime, location_uuid),
  index loc_id_enc_date_next_clinical (
    location_id,
    encounter_datetime,
    next_clinical_datetime_lung_cancer_screening
  ),
  index encounter_type (encounter_type),
  index date_created (date_created)
);
if(@query_type = "build") then
select 'BUILDING..........................................';
set @write_table = concat(
    "flat_lung_cancer_screening_temp_",
    queue_number
  );
set @queue_table = concat(
    "flat_lung_cancer_screening_build_queue_",
    queue_number
  );
SET @dyn_sql = CONCAT(
    'Create table if not exists ',
    @write_table,
    ' like ',
    @primary_table
  );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
SET @dyn_sql = CONCAT(
    'Create table if not exists ',
    @queue_table,
    ' (select * from flat_lung_cancer_screening_build_queue limit ',
    queue_size,
    ');'
  );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
SET @dyn_sql = CONCAT(
    'delete t1 from flat_lung_cancer_screening_build_queue t1 join ',
    @queue_table,
    ' t2 using (person_id);'
  );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
end if;
if (@query_type = "sync") then
select 'SYNCING..........................................';
set @write_table = "flat_lung_cancer_screening";
set @queue_table = "flat_lung_cancer_screening_sync_queue";
create table if not exists flat_lung_cancer_screening_sync_queue (person_id int primary key);
set @last_update = null;
select max(date_updated) into @last_update
from etl.flat_log
where table_name = @table_version;
select "Finding patients in amrs.encounters...";
replace into flat_lung_cancer_screening_sync_queue (
  select distinct patient_id
  from amrs.encounter
  where date_changed > @last_update
);
select "Finding patients in flat_obs...";
replace into flat_lung_cancer_screening_sync_queue (
  select distinct person_id
  from etl.flat_obs
  where max_date_created > @last_update
);
select "Finding patients in flat_lab_obs...";
replace into flat_lung_cancer_screening_sync_queue (
  select distinct person_id
  from etl.flat_lab_obs
  where max_date_created > @last_update
);
select "Finding patients in flat_orders...";
replace into flat_lung_cancer_screening_sync_queue (
  select distinct person_id
  from etl.flat_orders
  where max_date_created > @last_update
);
replace into flat_lung_cancer_screening_sync_queue (
  select person_id
  from amrs.person
  where date_voided > @last_update
);
replace into flat_lung_cancer_screening_sync_queue (
  select person_id
  from amrs.person
  where date_changed > @last_update
);
end if;
-- Remove test patients
SET @dyn_sql = CONCAT(
    'delete t1 FROM ',
    @queue_table,
    ' t1
							join amrs.person_attribute t2 using (person_id)
							where t2.person_attribute_type_id=28 and value="true" and voided=0'
  );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
SET @person_ids_count = 0;
SET @dyn_sql = CONCAT(
    'select count(*) into @person_ids_count from ',
    @queue_table
  );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
select @person_ids_count as 'num patients to update';
SET @dyn_sql = CONCAT(
    'delete t1 from ',
    @primary_table,
    ' t1 join ',
    @queue_table,
    ' t2 using (person_id);'
  );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
DROP TEMPORARY TABLE IF EXISTS lung_cancer_freetext_obs;
SET @dyn_sql := CONCAT(
    "CREATE TEMPORARY TABLE IF NOT EXISTS lung_cancer_freetext_obs (SELECT e.patient_id,
      CONCAT('## !!', g.concept_id, '=', o.value_text, '!!') AS obs_string FROM amrs.encounter `e`
      LEFT JOIN
        amrs.obs `o` ON (o.encounter_id = e.encounter_id AND (o.concept_id = 1915))
      LEFT JOIN
        amrs.obs `g` ON (g.obs_id = o.obs_group_id)
      WHERE
        e.encounter_type IN (177 , 185)
      GROUP BY e.encounter_id);"
  );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
set @total_time = 0;
set @cycle_number = 0;
while @person_ids_count > 0 do
set @loop_start_time = now();
drop temporary table if exists flat_lung_cancer_screening_build_queue__0;
SET @dyn_sql = CONCAT(
    'create temporary table flat_lung_cancer_screening_build_queue__0 (person_id int primary key) (select * from ',
    @queue_table,
    ' limit ',
    cycle_size,
    ');'
  );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
drop temporary table if exists flat_lung_cancer_screening_0a;
SET @dyn_sql = CONCAT(
    'create temporary table flat_lung_cancer_screening_0a
    (select
      t1.person_id,
      t1.visit_id,
      t1.encounter_id,
      t1.encounter_datetime,
      t1.encounter_type,
      t1.location_id,
      t1.obs,
      t1.obs_datetimes,
      case
        when t1.encounter_type in ',
    @clinical_encounter_types,
    ' then 1
                else null
              end as is_clinical_encounter,

              case
                when t1.encounter_type in ',
    @non_clinical_encounter_types,
    ' then 20
                when t1.encounter_type in ',
    @clinical_encounter_types,
    ' then 10
                when t1.encounter_type in',
    @other_encounter_types,
    ' then 5
        else 1
      end as encounter_type_sort_index,
      t2.orders
    from etl.flat_obs t1
      join flat_lung_cancer_screening_build_queue__0 t0 using (person_id)
      left join etl.flat_orders t2 using(encounter_id)
    where t1.encounter_type in ',
    @encounter_types,
    ');'
  );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
insert into flat_lung_cancer_screening_0a (
    select t1.person_id,
      null,
      t1.encounter_id,
      t1.test_datetime,
      t1.encounter_type,
      null,
      #t1.location_id,
      t1.obs,
      null,
      #obs_datetimes
      # in any visit, there many be multiple encounters. for this dataset, we want to include only clinical encounters (e.g. not lab or triage visit)
      0 as is_clinical_encounter,
      1 as encounter_type_sort_index,
      null
    from etl.flat_lab_obs t1
      join flat_lung_cancer_screening_build_queue__0 t0 using (person_id)
  );
drop temporary table if exists flat_lung_cancer_screening_0;
create temporary table flat_lung_cancer_screening_0(
  index encounter_id (encounter_id),
  index person_enc (person_id, encounter_datetime)
) (
  select *
  from flat_lung_cancer_screening_0a
  order by person_id,
    date(encounter_datetime),
    encounter_type_sort_index
);
set @death_date = null;
set @cur_visit_type = null;
set @referred_from = null;
set @referring_health_facility = null;
set @referral_date = null;
set @referred_by = null;
set @chief_complaints = null;
set @other_complaints_freetext = null;
set @complaint_duration = null;
set @no_of_days_with_complaint = null;
set @no_of_weeks_with_complaint = null;
set @no_of_months_with_complaint = null;
set @no_of_years_with_complaint = null;
set @smokes_cigarettes = null;
set @sticks_smoked_per_day = null;
set @years_of_smoking_cigarettes = null;
set @uses_tobacco = null;
set @years_of_tobacco_use = null;
set @main_occupation = null;
set @main_occupation_freetext = null;
set @chemical_exposure = null;
set @other_chemical_exposure_freetext = null;
set @asbestos_exposure = null;
set @family_history_of_cancer = null;
set @hiv_status = null;
set @previous_treatment_for_tuberculosis = null;
set @duration_in_months_of_previous_tb_treatment = null;
set @no_of_previous_tb_treatments = null;
set @previous_tb_investigation_done = null;
set @chest_xray_results = null;
set @chest_xray_results_date = null;
set @gene_expert_results = null;
set @gene_expert_results_date = null;
set @sputum_for_fast_acid_bacilli_results = null;
set @sputum_for_fast_acid_bacilli_results_date = null;
set @other_previous_lab_test_done_freetext = null;
set @procedure_ordered = null;
set @imaging_test_ordered = null;
set @other_radiology_orders_freetext = null;
set @follow_up_care_plan = null;
set @other_follow_up_care_plan_freetext = null;
set @assessment_notes_screening = null;
set @rtc_date_screening = null;
set @chest_xray_findings = null;
set @date_of_chest_xray = null;
set @chest_ct_scan_findings = null;
set @lung_mass_laterality = null;
set @other_imaging_findings_freetext = null;
set @imaging_workup_date = null;
set @imaging_results_description = null;
set @was_biopsy_procedure_done = null;
set @biopsy_workup_date = null;
set @lung_biopsy_results = null;
set @diagnosis_date = null;
set @type_of_malignancy = null;
set @other_malignancy_freetext = null;
set @lung_cancer_type = null;
set @non_small_cell_lung_cancer_type = null;
set @non_cancer_respiratory_disease_type = null;
set @other_condition_freetext = null;
set @referrals_ordered = null;
set @other_referrals_ordered_freetext = null;
set @assessment_notes_screening_results = null;
set @rtc_date_screening_result = null;
drop temporary table if exists flat_lung_cancer_screening_1;
create temporary table flat_lung_cancer_screening_1 #(index encounter_id (encounter_id))
(
  select obs,
    encounter_type_sort_index,
    @prev_id := @cur_id as prev_id,
    @cur_id := t1.person_id as cur_id,
    t1.person_id,
    t1.encounter_id,
    t1.encounter_type,
    t1.encounter_datetime,
    t1.visit_id,
    t1.location_id,
    t1.is_clinical_encounter,
    p.gender,
    case
      when timestampdiff(year, birthdate, curdate()) > 0 then round(timestampdiff(year, birthdate, curdate()), 0)
      else round(
        timestampdiff(month, birthdate, curdate()) / 12,
        2
      )
    end as age,
    p.death_date,
    case
      when obs regexp "!!1839=7037!!" then @cur_visit_type := 1
      when obs regexp "!!1839=7875!!" then @cur_visit_type := 2
      else @cur_visit_type = null
    end as cur_visit_type,
    case
      when obs regexp "!!6749=6572!!" then @referred_from := 1
      when obs regexp "!!6749=8161!!" then @referred_from := 2
      when obs regexp "!!6749=2242!!" then @referred_from := 3
      when obs regexp "!!6749=978!!" then @referred_from := 4
      when obs regexp "!!6749=5487!!" then @referred_from := 5
      when obs regexp "!!6749=1275!!" then @referred_from := 6
      when obs regexp "!!6749=5622!!" then @referred_from := 7
      else @referred_from = null
    end as referred_from,
    case
      when obs regexp "!!6748=" then @referring_health_facility := GetValues(obs, 6748)
      else @referring_health_facility = null
    end as referring_health_facility,
    case
      when obs regexp "!!9158=" then @referral_date := GetValues(obs, 9158)
      else @referral_date = null
    end as referral_date,
    case
      when obs regexp "!!10135=1496!!" then @referred_by := 1
      when obs regexp "!!10135=6280!!" then @referred_by := 2
      when obs regexp "!!10135=5507!!" then @referred_by := 3
      when obs regexp "!!10135=5622!!" then @referred_by := 4
      else @referred_by = null
    end as referred_by,
    case
      when obs regexp "!!5219=" then @chief_complaints := GetValues(obs, 5219)
      else @chief_complaints = null
    end as chief_complaints,
    case
      when obs regexp "!!1915=" then @other_complaints_freetext := GetValues(lcfo.obs_string, 8916)
      else @other_complaints_freetext = null
    end as other_complaints_freetext,
    case
      when obs regexp "!!8777=" then @complaint_duration := GetValues(obs, 8777)
      else @complaint_duration = null
    end as complaint_duration,
    case
      when obs regexp "!!1892=" then @no_of_days_with_complaint := GetValues(obs, 1892)
      else @no_of_days_with_complaint = null
    end as no_of_days_with_complaint,
    case
      when obs regexp "!!1893=" then @no_of_weeks_with_complaint := GetValues(obs, 1893)
      else @no_of_weeks_with_complaint = null
    end as no_of_weeks_with_complaint,
    case
      when obs regexp "!!1894=" then @no_of_months_with_complaint := GetValues(obs, 1894)
      else @no_of_months_with_complaint = null
    end as no_of_months_with_complaint,
    case
      when obs regexp "!!7953=" then @no_of_years_with_complaint := GetValues(obs, 7953)
      else @no_of_years_with_complaint = null
    end as no_of_years_with_complaint,
    case
      when obs regexp "!!2065=1065!!" then @smokes_cigarettes := 1
      when obs regexp "!!2065=1066!!" then @smokes_cigarettes := 2
      when obs regexp "!!2065=1679!!" then @smokes_cigarettes := 3
      else @smokes_cigarettes = null
    end as smokes_cigarettes,
    case
      when obs regexp "!!2069=" then @sticks_smoked_per_day := GetValues(obs, 2069)
      else @sticks_smoked_per_day = null
    end as sticks_smoked_per_day,
    case
      when obs regexp "!!2070=" then @years_of_smoking_cigarettes := GetValues(obs, 2070)
      else @years_of_smoking_cigarettes = null
    end as years_of_smoking_cigarettes,
    case
      when obs regexp "!!7973=1065!!" then @uses_tobacco := 1
      when obs regexp "!!7973=1066!!" then @uses_tobacco := 2
      when obs regexp "!!7973=1679!!" then @uses_tobacco := 3
      else @uses_tobacco = null
    end as uses_tobacco,
    case
      when obs regexp "!!8144=" then @years_of_tobacco_use := GetValues(obs, 8144)
      else @years_of_tobacco_use = null
    end as years_of_tobacco_use,
    case
      when obs regexp "!!1972=1967!!" then @main_occupation := 1
      when obs regexp "!!1972=1966!!" then @main_occupation := 2
      when obs regexp "!!1972=1971!!" then @main_occupation := 3
      when obs regexp "!!1972=8710!!" then @main_occupation := 4
      when obs regexp "!!1972=10369!!" then @main_occupation := 5
      when obs regexp "!!1972=10368!!" then @main_occupation := 6
      when obs regexp "!!1972=5622!!" then @main_occupation := 7
      else @main_occupation = null
    end as main_occupation,
    case
      when obs regexp "!!1915=" then @main_occupation_freetext := GetValues(lcfo.obs_string, 1973)
      else @main_occupation_freetext = null
    end as main_occupation_freetext,
    case
      when obs regexp "!!10310=" then @chemical_exposure := GetValues(obs, 10310)
      else @chemical_exposure = null
    end as chemical_exposure,
    case
      when obs regexp "!!1915=" then @other_chemical_exposure_freetext := GetValues(lcfo.obs_string, 10311)
      else @other_chemical_exposure_freetext = null
    end as other_chemical_exposure_freetext,
    case
      when obs regexp "!!10312=1065!!" then @asbestos_exposure := 1
      when obs regexp "!!10312=1066!!" then @asbestos_exposure := 2
      else @asbestos_exposure = null
    end as asbestos_exposure,
    case
      when obs regexp "!!9635=" then @family_history_of_cancer := GetValues(obs, 9635)
      else @family_history_of_cancer = null
    end as family_history_of_cancer,
    case
      when obs regexp "!!6709=1067!!" then @hiv_status := 1
      when obs regexp "!!6709=664!!" then @hiv_status := 2
      when obs regexp "!!6709=703!!" then @hiv_status := 3
      else @hiv_status = null
    end as hiv_status,
    case
      when obs regexp "!!10242=1065!!" then @previous_treatment_for_tuberculosis := 1
      when obs regexp "!!10242=1066!!" then @previous_treatment_for_tuberculosis := 2
      else @previous_treatment_for_tuberculosis = null
    end as previous_treatment_for_tuberculosis,
    case
      when obs regexp "!!1894=" then @duration_in_months_of_previous_tb_treatment := GetValues(obs, 1894)
      else @duration_in_months_of_previous_tb_treatment = null
    end as duration_in_months_of_previous_tb_treatment,
    case
      when obs regexp "!!9304=" then @no_of_previous_tb_treatments := GetValues(obs, 9304)
      else @no_of_previous_tb_treatments = null
    end as no_of_previous_tb_treatments,
    case
      when obs regexp "!!2028=" then @previous_tb_investigation_done := GetValues(obs, 2028)
      else @previous_tb_investigation_done = null
    end as previous_tb_investigation_done,
    case
      when obs regexp "!!12=" then @chest_xray_results := GetValues(obs, 12)
      else @chest_xray_results = null
    end as chest_xray_results,
    case
      when obs regexp "!!12=" then @chest_xray_results_date := GetValues(obs_datetimes, 12)
      else @chest_xray_results_date = null
    end as chest_xray_results_date,
    case
      when obs regexp "!!8070=" then @gene_expert_results := GetValues(obs, 8070)
      else @gene_expert_results = null
    end as gene_expert_results,
    case
      when obs regexp "!!8070=" then @gene_expert_results_date := GetValues(obs_datetimes, 8070)
      else @gene_expert_results_date = null
    end as gene_expert_results_date,
    case
      when obs regexp "!!307=" then @sputum_for_fast_acid_bacilli_results := GetValues(obs, 307)
      else @sputum_for_fast_acid_bacilli_results = null
    end as sputum_for_fast_acid_bacilli_results,
    case
      when obs regexp "!!307=" then @sputum_for_fast_acid_bacilli_results_date := GetValues(obs_datetimes, 307)
      else @sputum_for_fast_acid_bacilli_results_date = null
    end as sputum_for_fast_acid_bacilli_results_date,
    case
      when obs regexp "!!=9538" then @other_previous_lab_test_done_freetext := GetValues(obs, 9538)
      else @other_previous_lab_test_done_freetext = null
    end as other_previous_lab_test_done_freetext,
    case
      when obs regexp "!!10127=1107!!" then @procedure_ordered := 1
      when obs regexp "!!10127=10075!!" then @procedure_ordered := 2
      when obs regexp "!!10127=10076!!" then @procedure_ordered := 3
      when obs regexp "!!10127=10126!!" then @procedure_ordered := 4
      when obs regexp "!!10127=5622!!" then @procedure_ordered := 5
      else @procedure_ordered = null
    end as procedure_ordered,
    case
      when obs regexp "!!1271=12!!" then @imaging_test_ordered := 1
      when obs regexp "!!1271=7113!!" then @imaging_test_ordered := 2
      when obs regexp "!!1271=5622!!" then @imaging_test_ordered := 3
      else @imaging_test_ordered = null
    end as imaging_test_ordered,
    case
      when obs regexp "!!8190=" then @other_radiology_orders_freetext := GetValues(obs, 8190)
      else @other_radiology_orders_freetext = null
    end as other_radiology_orders_freetext,
    case
      when obs regexp "!!9930=1107!!" then @follow_up_care_plan := 1
      when obs regexp "!!9930=9725!!" then @follow_up_care_plan := 2
      when obs regexp "!!9930=5622!!" then @follow_up_care_plan := 3
      else @follow_up_care_plan = null
    end as follow_up_care_plan,
    case
      when obs regexp "!!8190=" then @other_follow_up_care_plan_freetext := GetValues(obs, 8190)
      else @other_follow_up_care_plan_freetext = null
    end as other_follow_up_care_plan_freetext,
    case
      when obs regexp "!!7222=" then @assessment_notes_screening = GetValues(obs, 10124)
      else @assessment_notes_screening = null
    end as assessment_notes_screening,
    case
      when obs regexp "!!5096=" then @rtc_date_screening := GetValues(obs, 5096)
      else @rtc_date_screening = null
    end as rtc_date_screening,
    case
      when obs regexp "!!12=1115!!" then @chest_xray_findings := 1
      when obs regexp "!!12=1116!!" then @chest_xray_findings := 2
      when obs regexp "!!12=5622!!" then @chest_xray_findings := 3
      else @chest_xray_findings = null
    end as chest_xray_findings,
    case
      when obs regexp "!!12=" then @date_of_chest_xray := GetValues(obs_datetimes, 12)
      else @date_of_chest_xray = null
    end as date_of_chest_xray,
    case
      when obs regexp "!!12=1115!!" then @chest_ct_scan_findings := 1
      when obs regexp "!!12=10318!!" then @chest_ct_scan_findings := 2
      when obs regexp "!!12=2418!!" then @chest_ct_scan_findings := 3
      when obs regexp "!!12=1136!!" then @chest_ct_scan_findings := 4
      when obs regexp "!!12=5622!!" then @chest_ct_scan_findings := 5
      else @chest_ct_scan_findings = null
    end as chest_ct_scan_findings,
    case
      when obs regexp "!!8264=5139!!" then @lung_mass_laterality := 1 -- Left
      when obs regexp "!!8264=5141!!" then @lung_mass_laterality := 2 -- Right 
      when obs regexp "!!8264=2399!!" then @lung_mass_laterality := 3 -- Bilateral
      else @lung_mass_laterality = null
    end as lung_mass_laterality,
    case
      when obs regexp "!!10124=" then @other_imaging_findings_freetext := GetValues(obs, 10124)
      else @other_imaging_findings_freetext = null
    end as other_imaging_findings_freetext,
    case
      when obs regexp "!!9708=" then @imaging_workup_date := GetValues(obs, 9708)
      else @imaging_workup_date = null
    end as imaging_workup_date,
    case
      when obs regexp "!!10077=" then @imaging_results_description := GetValues(obs, 10077)
      else @imaging_results_description = null
    end as imaging_results_description,
    case
      when obs regexp "!!10391=1065!!" then @was_biopsy_procedure_done := 1
      when obs regexp "!!10391=1066!!" then @was_biopsy_procedure_done := 2
      else @was_biopsy_procedure_done = null
    end as was_biopsy_procedure_done,
    case
      when obs regexp "!!10060=" then @biopsy_workup_date := GetValues(obs, 10060)
      else @biopsy_workup_date = null
    end as biopsy_workup_date,
    case
      when obs regexp "!!10231=10052!!" then @lung_biopsy_results := 1
      when obs regexp "!!10231=10212!!" then @lung_biopsy_results := 2
      when obs regexp "!!10231=5622!!" then @lung_biopsy_results := 3
      else @lung_biopsy_results = null
    end as lung_biopsy_results,
    case
      when obs regexp "!!9728=" then @diagnosis_date := GetValues(obs, 9728)
      else @diagnosis_date = null
    end as diagnosis_date,
    case
      when obs regexp "!!9846=9845" then @type_of_malignancy := 1
      when obs regexp "!!9846=5622" then @type_of_malignancy := 2
      else @type_of_malignancy = null
    end as type_of_malignancy,
    case
      when obs regexp "!!10390=" then @other_malignancy_freetext := GetValues(obs, 10390)
      else @other_malignancy_freetext = null
    end as other_malignancy_freetext,
    case
      when obs regexp "!!7176=10129!!" then @lung_cancer_type := 1
      when obs regexp "!!7176=10130!!" then @lung_cancer_type := 2
      when obs regexp "!!7176=5622!!" then @lung_cancer_type := 3
      else @lung_cancer_type = null
    end as lung_cancer_type,
    case
      when obs regexp "!!10132=7421!!" then @non_small_cell_lung_cancer_type := 1
      when obs regexp "!!10132=7422!!" then @non_small_cell_lung_cancer_type := 2
      when obs regexp "!!10132=10131!!" then @non_small_cell_lung_cancer_type := 3
      when obs regexp "!!10132=10209!!" then @non_small_cell_lung_cancer_type := 4
      else @non_small_cell_lung_cancer_type = null
    end as non_small_cell_lung_cancer_type,
    case
      when obs regexp "!!10319=1295!!" then @non_cancer_respiratory_disease_type := 1
      when obs regexp "!!10319=58!!" then @non_cancer_respiratory_disease_type := 2
      when obs regexp "!!10319=10211!!" then @non_cancer_respiratory_disease_type := 3
      when obs regexp "!!10319=10210!!" then @non_cancer_respiratory_disease_type := 4
      when obs regexp "!!10319=5622!!" then @non_cancer_respiratory_disease_type := 5
      else @non_cancer_respiratory_disease_type = null
    end as non_cancer_respiratory_disease_type,
    case
      when obs regexp "!!1915=" then @other_condition_freetext := GetValues(lcfo.obs_string, 10391)
      else @other_condition_freetext = null
    end as other_condition_freetext,
    case
      when obs regexp "!!1272=" then @referrals_ordered := GetValues(obs, 1272)
      else @referrals_ordered = null
    end as referrals_ordered,
    case
      when obs regexp "!!1915=" then @other_referrals_ordered_freetext := GetValues(lcfo.obs_string, 1932)
      else @other_referrals_ordered_freetext = null
    end as other_referrals_ordered_freetext,
    case
      when obs regexp "!!7222=" then @assessment_notes_screening_results := GetValues(obs, 7222)
      else @assessment_notes_screening_results = null
    end as assessment_notes_screening_results,
    case
      when obs regexp "!!5096=" then @rtc_date_screening_results := GetValues(obs, 5096)
      else @rtc_date_screening_results = null
    end as rtc_date_screening_results
  from flat_lung_cancer_screening_0 t1
    join amrs.person p using (person_id)
    left join lung_cancer_freetext_obs `lcfo` ON t1.person_id = lcfo.patient_id
  order by person_id,
    date(encounter_datetime) desc,
    encounter_type_sort_index desc
);
set @prev_id = null;
set @cur_id = null;
set @prev_encounter_datetime = null;
set @cur_encounter_datetime = null;
set @prev_clinical_datetime = null;
set @cur_clinical_datetime = null;
set @next_encounter_type = null;
set @cur_encounter_type = null;
set @prev_clinical_location_id = null;
set @cur_clinical_location_id = null;
alter table flat_lung_cancer_screening_1 drop prev_id,
  drop cur_id;
drop table if exists flat_lung_cancer_screening_2;
create temporary table flat_lung_cancer_screening_2 (
  select *,
    @prev_id := @cur_id as prev_id,
    @cur_id := person_id as cur_id,
    case
      when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
      else @prev_encounter_datetime := null
    end as next_encounter_datetime_lung_cancer_screening,
    @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,
    case
      when @prev_id = @cur_id then @next_encounter_type := @cur_encounter_type
      else @next_encounter_type := null
    end as next_encounter_type_lung_cancer_screening,
    @cur_encounter_type := encounter_type as cur_encounter_type,
    case
      when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
      else @prev_clinical_datetime := null
    end as next_clinical_datetime_lung_cancer_screening,
    case
      when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
      else @prev_clinical_location_id := null
    end as next_clinical_location_id_lung_cancer_screening,
    case
      when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
      when @prev_id = @cur_id then @cur_clinical_datetime
      else @cur_clinical_datetime := null
    end as cur_clinic_datetime,
    case
      when is_clinical_encounter then @cur_clinical_location_id := location_id
      when @prev_id = @cur_id then @cur_clinical_location_id
      else @cur_clinical_location_id := null
    end as cur_clinic_location_id,
    case
      when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
      else @prev_clinical_rtc_date := null
    end as next_clinical_rtc_date_lung_cancer_screening,
    case
      when @prev_id = @cur_id then @cur_clinical_rtc_date
      else @cur_clinical_rtc_date := null
    end as cur_clinical_rtc_date
  from flat_lung_cancer_screening_1
  order by person_id,
    date(encounter_datetime) desc,
    encounter_type_sort_index desc
);
alter table flat_lung_cancer_screening_2 drop prev_id,
  drop cur_id,
  drop cur_encounter_type,
  drop cur_encounter_datetime,
  drop cur_clinical_rtc_date;
set @prev_id = null;
set @cur_id = null;
set @prev_encounter_type = null;
set @cur_encounter_type = null;
set @prev_encounter_datetime = null;
set @cur_encounter_datetime = null;
set @prev_clinical_datetime = null;
set @cur_clinical_datetime = null;
set @prev_clinical_location_id = null;
set @cur_clinical_location_id = null;
drop temporary table if exists flat_lung_cancer_screening_3;
create temporary table flat_lung_cancer_screening_3 (
  prev_encounter_datetime datetime,
  prev_encounter_type INT,
  index person_enc (person_id, encounter_datetime desc)
) (
  select *,
    @prev_id := @cur_id as prev_id,
    @cur_id := t1.person_id as cur_id,
    case
      when @prev_id = @cur_id then @prev_encounter_type := @cur_encounter_type
      else @prev_encounter_type := null
    end as prev_encounter_type_lung_cancer_screening,
    @cur_encounter_type := encounter_type as cur_encounter_type,
    case
      when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
      else @prev_encounter_datetime := null
    end as prev_encounter_datetime_lung_cancer_screening,
    @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,
    case
      when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
      else @prev_clinical_datetime := null
    end as prev_clinical_datetime_lung_cancer_screening,
    case
      when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
      else @prev_clinical_location_id := null
    end as prev_clinical_location_id_lung_cancer_screening,
    case
      when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
      when @prev_id = @cur_id then @cur_clinical_datetime
      else @cur_clinical_datetime := null
    end as cur_clinical_datetime,
    case
      when is_clinical_encounter then @cur_clinical_location_id := location_id
      when @prev_id = @cur_id then @cur_clinical_location_id
      else @cur_clinical_location_id := null
    end as cur_clinical_location_id,
    case
      when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
      else @prev_clinical_rtc_date := null
    end as prev_clinical_rtc_date_lung_cancer_screening,
    case
      when @prev_id = @cur_id then @cur_clinical_rtc_date
      else @cur_clinical_rtc_date := null
    end as cur_clinic_rtc_date
  from flat_lung_cancer_screening_2 t1
  order by person_id,
    date(encounter_datetime),
    encounter_type_sort_index
);
select count(*) into @new_encounter_rows
from flat_lung_cancer_screening_3;
select @new_encounter_rows;
set @total_rows_written = @total_rows_written + @new_encounter_rows;
select @total_rows_written;
SET @dyn_sql = CONCAT(
    'replace into ',
    @write_table,
    '(select
          null,
          person_id,
          encounter_id,
          encounter_type,
          encounter_datetime,
          visit_id,
          location_id,
          t2.uuid as location_uuid,
          gender,
          age,
          death_date,
          cur_visit_type,
          referred_from,
          referring_health_facility,
          referral_date,
          referred_by,
          chief_complaints,
          other_complaints_freetext,
          complaint_duration,
          no_of_days_with_complaint,
          no_of_weeks_with_complaint,
          no_of_months_with_complaint,
          no_of_years_with_complaint,
          smokes_cigarettes,
          sticks_smoked_per_day,
          years_of_smoking_cigarettes,
          uses_tobacco,
          years_of_tobacco_use,
          main_occupation,
          main_occupation_freetext,
          chemical_exposure,
          other_chemical_exposure_freetext,
          asbestos_exposure,
          family_history_of_cancer,
          hiv_status,
          previous_treatment_for_tuberculosis,
          duration_in_months_of_previous_tb_treatment,
          no_of_previous_tb_treatments,
          previous_tb_investigation_done,
          chest_xray_results,
          chest_xray_results_date,
          gene_expert_results,
          gene_expert_results_date,
          sputum_for_fast_acid_bacilli_results,
          sputum_for_fast_acid_bacilli_results_date,
          other_previous_lab_test_done_freetext,
          procedure_ordered,
          imaging_test_ordered,
          other_radiology_orders_freetext,
          follow_up_care_plan,
          other_follow_up_care_plan_freetext,
          assessment_notes_screening,
          rtc_date_screening,
          chest_xray_findings,
          date_of_chest_xray,
          chest_ct_scan_findings,
          lung_mass_laterality,
          other_imaging_findings_freetext,
          imaging_workup_date,
          imaging_results_description,
          was_biopsy_procedure_done,
          biopsy_workup_date,
          lung_biopsy_results,
          diagnosis_date,
          type_of_malignancy,
          other_malignancy_freetext,
          lung_cancer_type,
          non_small_cell_lung_cancer_type,
          non_cancer_respiratory_disease_type,
          other_condition_freetext,
          referrals_ordered,
          other_referrals_ordered_freetext,
          assessment_notes_screening_results,
          rtc_date_screening_results,
          prev_encounter_datetime_lung_cancer_screening,
          next_encounter_datetime_lung_cancer_screening,
          prev_encounter_type_lung_cancer_screening,
          next_encounter_type_lung_cancer_screening,
          prev_clinical_datetime_lung_cancer_screening,
          next_clinical_datetime_lung_cancer_screening,
          prev_clinical_location_id_lung_cancer_screening,
          next_clinical_location_id_lung_cancer_screening,
          prev_clinical_rtc_date_lung_cancer_screening,
          next_clinical_rtc_date_lung_cancer_screening

          from flat_lung_cancer_screening_3 t1
          join amrs.location t2 using (location_id))'
  );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
SET @dyn_sql = CONCAT(
    'delete t1 from ',
    @queue_table,
    ' t1 join flat_lung_cancer_screening_build_queue__0 t2 using (person_id);'
  );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
SET @dyn_sql = CONCAT(
    'select count(*) into @person_ids_count from ',
    @queue_table,
    ';'
  );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
set @cycle_length = timestampdiff(second, @loop_start_time, now());
set @total_time = @total_time + @cycle_length;
set @cycle_number = @cycle_number + 1;
set @remaining_time = ceil(
    (@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60
  );
select @person_ids_count as 'persons remaining',
  @cycle_length as 'Cycle time (s)',
  ceil(@person_ids_count / cycle_size) as remaining_cycles,
  @remaining_time as 'Est time remaining (min)';
end while;
if(@query_type = "build") then #select 1;
SET @dyn_sql = CONCAT('drop table ', @queue_table, ';');
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
SET @total_rows_to_write = 0;
SET @dyn_sql = CONCAT(
    "Select count(*) into @total_rows_to_write from ",
    @write_table
  );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
set @start_write = now();
select concat(
    @start_write,
    " : Writing ",
    @total_rows_to_write,
    ' to ',
    @primary_table
  );
SET @dyn_sql = CONCAT(
    'replace into ',
    @primary_table,
    '(select * from ',
    @write_table,
    ');'
  );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
set @finish_write = now();
set @time_to_write = timestampdiff(second, @start_write, @finish_write);
select concat(
    @finish_write,
    ' : Completed writing rows. Time to write to primary table: ',
    @time_to_write,
    ' seconds '
  );
SET @dyn_sql = CONCAT('drop table ', @write_table, ';');
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
end if;
set @ave_cycle_length = ceil(@total_time / @cycle_number);
select CONCAT(
    'Average Cycle Length: ',
    @ave_cycle_length,
    ' second(s)'
  );
set @end = now();
insert into etl.flat_log
values (
    @start,
    @last_date_created,
    @table_version,
    timestampdiff(second, @start, @end)
  );
select concat(
    @table_version,
    " : Time to complete: ",
    timestampdiff(minute, @start, @end),
    " minutes"
  );
END