CREATE PROCEDURE `generate_flat_lung_cancer_treatment_v1_0`(
  IN query_type varchar(50),
  IN queue_number int,
  IN queue_size int,
  IN cycle_size int
) BEGIN
SET @primary_table := "flat_lung_cancer_treatment";
SET @query_type := query_type;
SET @total_rows_written := 0;
SET @encounter_types := "(169,170)";
SET @clinical_encounter_types := "(-1)";
SET @non_clinical_encounter_types := "(-1)";
SET @other_encounter_types := "(-1)";
SET @start := now();
SET @table_version := "flat_lung_cancer_treatment_v1_0";
SET session sort_buffer_size := 512000000;
SET @sep := " ## ";
SET @boundary := "!!";
SET @last_date_created := (
    select max(max_date_created)
    from etl.flat_obs
  );
CREATE TABLE IF NOT EXISTS flat_lung_cancer_treatment (
  date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  person_id INT,
  encounter_id INT,
  encounter_type INT,
  encounter_datetime DATETIME,
  visit_id INT,
  location_id INT,
  location_uuid VARCHAR(100),
  facility_name VARCHAR(200),
  gender CHAR(100),
  age INT,
  birthdate DATE,
  death_date DATE,
  cur_visit_type INT,
  referred_from INT,
  referring_health_facility VARCHAR(200),
  referral_date DATE,
  referred_by INT,
  referring_health_provider_freetext VARCHAR(150),
  marital_status INT,
  main_occupation INT,
  main_occupation_freetext VARCHAR(200),
  patient_education_level INT,
  smokes_cigarettes INT,
  sticks_smoked_per_day INT,
  years_of_smoking_cigarettes INT,
  years_since_last_use_of_cigarettes INT,
  tobacco_use INT,
  years_of_tobacco_use INT,
  drinks_alcohol INT,
  chemical_exposure VARCHAR(200),
  other_chemical_exposure_freetext VARCHAR(200),
  asbestos_exposure INT,
  any_family_member_with_cancer_or_specific_chronic_illness INT,
  family_member_with_specific_chronic_illness VARCHAR(500),
  specific_family_member_chronic_illness_freetext VARCHAR(500),
  chief_complaints VARCHAR(100),
  other_complaints_freetext VARCHAR(200),
  complaint_duration VARCHAR(100),
  no_of_days_with_complaint INT,
  no_of_weeks_with_complaint INT,
  no_of_months_with_complaint INT,
  no_of_years_with_complaint INT,
  ecog_performance_index INT,
  general_exam VARCHAR(100),
  heent_exam VARCHAR(100),
  chest_exam VARCHAR(100),
  heart_exam VARCHAR(100),
  abdomen_exam VARCHAR(100),
  urogenital_exam VARCHAR(100),
  extremities_exam VARCHAR(100),
  testicular_exam VARCHAR(100),
  nodal_survey VARCHAR(100),
  musculo_skeletal_exam VARCHAR(100),
  neurologic_exam VARCHAR(100),
  physical_exam_notes VARCHAR(500),
  no_of_hospitalizations_last_12_months INT,
  diagnosis_history INT,
  year_diagnosed INT,
  hiv_status INT,
  chest_xray_results INT,
  chest_ct_scan_results INT,
  pet_scan_results INT,
  abdominal_ultrasound_result INT,
  other_imaging_results INT,
  tests_ordered VARCHAR(100),
  other_radiology_orders VARCHAR(500),
  lab_results_notes VARCHAR(500),
  procedure_ordered INT,
  other_procedure_ordered_freetext VARCHAR(500),
  diagnosis_procedure_performed VARCHAR(100),
  procedure_date DATE,
  lung_cancer_type INT,
  small_cell_lung_cancer_staging INT,
  non_small_cell_lung_cancer_type INT,
  date_of_diagnosis DATE,
  cancer_staging INT,
  cancer_staging_date DATE,
  metastasis_region VARCHAR(500),
  treatment_plan INT,
  treatment_intent INT,
  chemotherapy_plan INT,
  current_chemo_cycle INT,
  chemotherapy_regimen VARCHAR(500),
  chemo_start_date DATE,
  chemo_stop_date DATE,
  total_planned_chemo_cycles INT,
  reasons_for_changing_or_stopping_chemo INT,
  other_chemo_stop_reason_freetext VARCHAR(500),
  other_chemo_drug_started_freetext VARCHAR(500),
  surgery_date DATE,
  radiotherapy_sessions INT,
  other_treatment_plan VARCHAR(500),
  chemo_drug INT,
  dosage_in_milligrams INT,
  drug_route VARCHAR(100),
  other_drugs VARCHAR(500),
  other_post_chemo_or_non_chemo_drug VARCHAR(500),
  drug_purpose VARCHAR(100),
  referrals_ordered VARCHAR(100),
  other_referrals_freetext VARCHAR(100),
  toxicity_assessment VARCHAR(100),
  other_toxicity_assessment_freetext VARCHAR(1000),
  therapeutic_plan_notes VARCHAR(1000),
  rtc_date DATE,
  prev_encounter_datetime_lung_cancer_treatment DATETIME,
  next_encounter_datetime_lung_cancer_treatment DATETIME,
  prev_encounter_type_lung_cancer_treatment MEDIUMINT,
  next_encounter_type_lung_cancer_treatment MEDIUMINT,
  prev_clinical_datetime_lung_cancer_treatment DATETIME,
  next_clinical_datetime_lung_cancer_treatment DATETIME,
  prev_clinical_location_id_lung_cancer_treatment MEDIUMINT,
  next_clinical_location_id_lung_cancer_treatment MEDIUMINT,
  prev_clinical_rtc_date_lung_cancer_treatment DATETIME,
  next_clinical_rtc_date_lung_cancer_treatment DATETIME,
  PRIMARY KEY encounter_id (encounter_id),
  INDEX person_date (person_id, encounter_datetime),
  INDEX location_enc_date (location_uuid, encounter_datetime),
  INDEX enc_date_location (encounter_datetime, location_uuid),
  INDEX loc_id_enc_date_next_clinical (
    location_id,
    encounter_datetime,
    next_clinical_datetime_lung_cancer_treatment
  ),
  INDEX encounter_type (encounter_type),
  INDEX date_created (date_created)
);
if (@query_type = "build") then
select 'BUILDING..........................................';
set @write_table = concat(
    "flat_lung_cancer_treatment_temp_",
    queue_number
  );
set @queue_table = concat(
    "flat_lung_cancer_treatment_build_queue_",
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
    ' (select * from flat_lung_cancer_treatment_build_queue limit ',
    queue_size,
    ');'
  );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
SET @dyn_sql = CONCAT(
    'delete t1 from flat_lung_cancer_treatment_build_queue t1 join ',
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
set @write_table = "flat_lung_cancer_treatment";
set @queue_table = "flat_lung_cancer_treatment_sync_queue";
CREATE TABLE IF NOT EXISTS flat_lung_cancer_treatment_sync_queue (person_id INT PRIMARY KEY);
set @last_update = null;
SELECT MAX(date_updated) INTO @last_update
FROM etl.flat_log
WHERE table_name = @table_version;
SELECT 'Finding patients in amrs.encounters...';
replace into flat_lung_cancer_treatment_sync_queue (
  select distinct patient_id
  from amrs.encounter
  where date_changed > @last_update
);
SELECT 'Finding patients in flat_obs...';
replace into flat_lung_cancer_treatment_sync_queue (
  select distinct person_id
  from etl.flat_obs
  where max_date_created > @last_update
);
SELECT 'Finding patients in flat_lab_obs...';
replace into flat_lung_cancer_treatment_sync_queue (
  select distinct person_id
  from etl.flat_lab_obs
  where max_date_created > @last_update
);
SELECT 'Finding patients in flat_orders...';
replace into flat_lung_cancer_treatment_sync_queue (
  select distinct person_id
  from etl.flat_orders
  where max_date_created > @last_update
);
replace into flat_lung_cancer_treatment_sync_queue (
  select person_id
  from amrs.person
  where date_voided > @last_update
);
replace into flat_lung_cancer_treatment_sync_queue (
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
SELECT @person_ids_count AS 'num patients to update';
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
        e.encounter_type IN (169 , 170)
      GROUP BY e.encounter_id);"
  );
PREPARE s1
FROM @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
set @total_time = 0;
set @cycle_number = 0;
while @person_ids_count > 0 do
set @loop_start_time = now();
drop temporary table if exists flat_lung_cancer_treatment_build_queue__0;
SET @dyn_sql = CONCAT(
    'create temporary table flat_lung_cancer_treatment_build_queue__0 (person_id int primary key) (select * from ',
    @queue_table,
    ' limit ',
    cycle_size,
    ');'
  );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
drop temporary table if exists flat_lung_cancer_treatment_0a;
SET @dyn_sql = CONCAT(
    'create temporary table flat_lung_cancer_treatment_0a
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
        join flat_lung_cancer_treatment_build_queue__0 t0 using (person_id)
        left join etl.flat_orders t2 using(encounter_id)
        where t1.encounter_type in ',
    @encounter_types,
    ');'
  );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
insert into flat_lung_cancer_treatment_0a (
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
      join flat_lung_cancer_treatment_build_queue__0 t0 using (person_id)
  );
drop temporary table if exists flat_lung_cancer_treatment_0;
create temporary table flat_lung_cancer_treatment_0(
  index encounter_id (encounter_id),
  index person_enc (person_id, encounter_datetime)
) (
  select *
  from flat_lung_cancer_treatment_0a
  order by person_id,
    date(encounter_datetime),
    encounter_type_sort_index
);
set @birthdate = null;
set @death_date = null;
set @cur_visit_type = null;
set @referred_from = null;
set @referring_health_facility = null;
set @referral_date = null;
set @referred_by = null;
set @referring_health_provider_freetext := null;
set @marital_status = null;
set @main_occupation = null;
set @main_occupation_freetext = null;
set @patient_education_level = null;
set @smokes_cigarettes = null;
set @sticks_smoked_per_day = null;
set @years_of_smoking_cigarettes = null;
set @years_since_last_use_of_cigarettes = null;
set @tobacco_use = null;
set @years_of_tobacco_use = null;
set @drinks_alcohol = null;
set @chemical_exposure = null;
set @other_chemical_exposure_freetext = null;
set @asbestos_exposure = null;
set @any_family_member_with_cancer_or_specific_chronic_illness = null;
set @family_member_with_specific_chronic_illness_freetext = null;
set @specific_family_member_chronic_illness_freetext = null;
set @chief_complaints = null;
set @other_complaints_freetext = null;
set @complaint_duration = null;
set @no_of_days_with_complaint = null;
set @no_of_weeks_with_complaint = null;
set @no_of_months_with_complaint = null;
set @no_of_years_with_complaint = null;
set @ecog_performance_index = null;
set @general_exam = null;
set @heent_exam = null;
set @chest_exam = null;
set @heart_exam = null;
set @abdomen_exam = null;
set @urogenital_exam = null;
set @extremities_exam = null;
set @testicular_exam = null;
set @nodal_survey = null;
set @musculo_skeletal_exam = null;
set @neurologic_exam = null;
set @physical_exam_notes = null;
set @no_of_hospitalizations_last_12_months = null;
set @diagnosis_history = null;
set @year_diagnosed = null;
set @hiv_status = null;
set @chest_xray_results = null;
set @chest_ct_scan_results = null;
set @pet_scan_results = null;
set @abdominal_ultrasound_result = null;
set @other_imaging_results = null;
set @tests_ordered = null;
set @other_radiology_orders = null;
set @lab_results_notes = null;
set @procedure_ordered = null;
set @other_procedure_ordered_freetext = null;
set @diagnosis_procedure_performed = null;
set @procedure_date = null;
set @lung_cancer_type = null;
set @small_cell_lung_cancer_staging = null;
set @non_small_cell_lung_cancer_type = null;
set @date_of_diagnosis = null;
set @cancer_staging = null;
set @cancer_staging_date = null;
set @metastasis_region = null;
set @treatment_plan = null;
set @treatment_intent = null;
set @chemotherapy_plan = null;
set @current_chemo_cycle = null;
set @chemotherapy_regimen = null;
set @chemo_start_date = null;
set @chemo_stop_date = null;
set @total_planned_chemo_cycles = null;
set @reasons_for_changing_or_stopping_chemo = null;
set @other_chemo_stop_reason_freetext = null;
set @other_chemo_drug_started_freetext = null;
set @surgery_date = null;
set @radiotherapy_sessions = null;
set @other_treatment_plan = null;
set @chemo_drug = null;
set @dosage_in_milligrams = null;
set @drug_route = null;
set @other_drugs = null;
set @other_post_chemo_or_non_chemo_drug = null;
set @drug_purpose = null;
set @referrals_ordered = null;
set @other_referrals_freetext = null;
set @toxicity_assessment = null;
set @other_toxicity_assessment_freetext = null;
set @therapeutic_plan_notes = null;
set @rtc_date = null;
drop temporary table if exists flat_lung_cancer_treatment_1;
create temporary table flat_lung_cancer_treatment_1 #(index encounter_id (encounter_id))
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
    l.name as facility_name,
    p.gender,
    case
      when timestampdiff(year, birthdate, curdate()) > 0 then round(timestampdiff(year, birthdate, curdate()), 0)
      else round(
        timestampdiff(month, birthdate, curdate()) / 12,
        2
      )
    end as age,
    p.birthdate,
    p.death_date,
    case
      when obs regexp "!!1839=7037!!" then @cur_visit_type := 1
      when obs regexp "!!1839=7875!!" then @cur_visit_type := 2
      else @cur_visit_type := null
    end as cur_visit_type,
    case
      when obs regexp "!!6749=8161!!" then @referred_from := 1
      when obs regexp "!!6749=5487!!" then @referred_from := 2
      when obs regexp "!!6749=1275!!" then @referred_from := 3
      when obs regexp "!!6749=2242!!" then @referred_from := 4
      when obs regexp "!!6749=6572!!" then @referred_from := 5
      when obs regexp "!!6749=978!!" then @referred_from := 6
      when obs regexp "!!6749=5622!!" then @referred_from := 7
      else @referred_from := null
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
      else @referred_by := null
    end as referred_by,
    case
      when obs regexp "!!1915=" then @referring_health_provider_freetext := GetValues(lcfo.obs_string, 10136)
      else @referring_health_provider_freetext = null
    end as referring_health_provider_freetext,
    case
      when obs regexp "!!1054=1175!!" then @marital_status := 1
      when obs regexp "!!1054=1057!!" then @marital_status := 2
      when obs regexp "!!1054=5555!!" then @marital_status := 3
      when obs regexp "!!1054=6290!!" then @marital_status := 4
      when obs regexp "!!1054=1060!!" then @marital_status := 5
      when obs regexp "!!1054=1056!!" then @marital_status := 6
      when obs regexp "!!1054=1058!!" then @marital_status := 7
      when obs regexp "!!1054=1059!!" then @marital_status := 8
      else @marital_status := null
    end as marital_status,
    case
      when obs regexp "!!1972=1967!!" then @main_occupation := 1
      when obs regexp "!!1972=6401!!" then @main_occupation := 2
      when obs regexp "!!1972=6408!!" then @main_occupation := 3
      when obs regexp "!!1972=6966!!" then @main_occupation := 4
      when obs regexp "!!1972=1969!!" then @main_occupation := 5
      when obs regexp "!!1972=1968!!" then @main_occupation := 6
      when obs regexp "!!1972=1966!!" then @main_occupation := 7
      when obs regexp "!!1972=8407!!" then @main_occupation := 8
      when obs regexp "!!1972=6284!!" then @main_occupation := 9
      when obs regexp "!!1972=8589!!" then @main_occupation := 10
      when obs regexp "!!1972=5619!!" then @main_occupation := 11
      when obs regexp "!!1972=5622!!" then @main_occupation := 12
      else @main_occupation = null
    end as main_occupation,
    case
      when obs regexp "!!1915=" then @main_occupation_freetext := GetValues(lcfo.obs_string, 1973)
      else @main_occupation_freetext = null
    end as main_occupation_freetext,
    case
      when obs regexp "!!1605=1107!!" then @patient_education_level := 1
      when obs regexp "!!1605=6214!!" then @patient_education_level := 2
      when obs regexp "!!1605=6215!!" then @patient_education_level := 3
      when obs regexp "!!1605=1604!!" then @patient_education_level := 4
      else @patient_education_level := null
    end as patient_education_level,
    case
      when obs regexp "!!2065=1065!!" then @smokes_cigarettes := 1
      when obs regexp "!!2065=1066!!" then @smokes_cigarettes := 2
      when obs regexp "!!2065=1679!!" then @smokes_cigarettes := 3
      else @smokes_cigarettes := null
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
      when obs regexp "!!2068=" then @years_since_last_use_of_cigarettes := GetValues(obs, 2068)
      else @years_since_last_use_of_cigarettes = null
    end as years_since_last_use_of_cigarettes,
    case
      when obs regexp "!!7973=1065!!" then @tobacco_use := 1
      when obs regexp "!!7973=1066!!" then @tobacco_use := 2
      when obs regexp "!!7973=1679!!" then @tobacco_use := 3
      else @tobacco_use = null
    end as tobacco_use,
    case
      when obs regexp "!!8144=" then @years_of_tobacco_use := GetValues(obs, 8144)
      else @years_of_tobacco_use = null
    end as years_of_tobacco_use,
    case
      when obs regexp "!!1684=1065!!" then @drinks_alcohol := 1
      when obs regexp "!!1684=1066!!" then @drinks_alcohol := 2
      when obs regexp "!!1684=1679!!" then @drinks_alcohol := 3
      else @drinks_alcohol := null
    end as drinks_alcohol,
    case
      when obs regexp "!!10310=" then @chemical_exposure := GetValues(obs, 10310)
      else @chemical_exposure := null
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
      when obs regexp "!!6802=1065!!" then @any_family_member_with_cancer_or_specific_chronic_illness := 1
      when obs regexp "!!6802=1066!!" then @any_family_member_with_cancer_or_specific_chronic_illness := 2
      else @any_family_member_with_cancer_or_specific_chronic_illness = null
    end as any_family_member_with_cancer_or_specific_chronic_illness,
    case
      when obs regexp "!!8254=" then @family_member_with_specific_chronic_illness := GetValues(obs, 8254)
      else @family_member_with_specific_chronic_illness := null
    end as family_member_with_specific_chronic_illness,
    case
      when obs regexp "!!1915=" then @specific_chronic_illness_freetext := GetValues(lcfo.obs_string, 6805)
      else @specific_family_member_chronic_illness_freetext = null
    end as specific_family_member_chronic_illness_freetext,
    case
      when obs regexp "!!5219=" then @chief_complaints := GetValues(obs, 5219)
      else @chief_complaints := null
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
      when obs regexp "!!6584=1115!!" then @ecog_performance_index := 1
      when obs regexp "!!6584=6585!!" then @ecog_performance_index := 2
      when obs regexp "!!6584=6586!!" then @ecog_performance_index := 3
      when obs regexp "!!6584=6587!!" then @ecog_performance_index := 4
      when obs regexp "!!6584=6588!!" then @ecog_performance_index := 5
      else @ecog_performance_index = null
    end as ecog_performance_index,
    case
      when obs regexp "!!1119=" then @general_exam := GetValues(obs, 1119)
      else @general_exam = null
    end as general_exam,
    case
      when obs regexp "!!1122=" then @heent_exam := GetValues(obs, 1122)
      else @heent_exam = null
    end as heent_exam,
    case
      when obs regexp "!!1123=" then @chest_exam := GetValues(obs, 1123)
      else @chest_exam = null
    end as chest_exam,
    case
      when obs regexp "!!1124=" then @heart_exam := GetValues(obs, 1124)
      else @heart_exam = null
    end as heart_exam,
    case
      when obs regexp "!!1125=" then @abdomen_exam := GetValues(obs, 1125)
      else @abdomen_exam = null
    end as abdomen_exam,
    case
      when obs regexp "!!1126=" then @urogenital_exam := GetValues(obs, 1126)
      else @urogenital_exam = null
    end as urogenital_exam,
    case
      when obs regexp "!!1127=" then @extremities_exam := GetValues(obs, 1127)
      else @extremities_exam = null
    end as extremities_exam,
    case
      when obs regexp "!!8420=" then @testicular_exam := GetValues(obs, 8420)
      else @testicular_exam = null
    end as testicular_exam,
    case
      when obs regexp "!!1121=" then @nodal_survey := GetValues(obs, 1121)
      else @nodal_survey = null
    end as nodal_survey,
    case
      when obs regexp "!!1128=" then @musculo_skeletal_exam := GetValues(obs, 1128)
      else @musculo_skeletal_exam = null
    end as musculo_skeletal_exam,
    case
      when obs regexp "!!1129=" then @neurologic_exam := GetValues(obs, 1129)
      else @neurologic_exam = null
    end as neurologic_exam,
    case
      when obs regexp "!!2018=" then @physical_exam_notes := GetValues(obs, 2018)
      else @physical_exam_notes = null
    end as physical_exam_notes,
    case
      when obs regexp "!!5704=" then @no_of_hospitalizations_last_12_months := GetValues(obs, 5704)
      else @no_of_hospitalizations_last_12_months = null
    end as no_of_hospitalizations_last_12_months,
    case
      when obs regexp "!!6245=" then @diagnosis_history := GetValues(obs, 6245)
      else @diagnosis_history = null
    end as diagnosis_history,
    case
      when obs regexp "!!9281=" then @year_diagnosed := GetValues(obs, 9281)
      else @year_diagnosed = null
    end as year_diagnosed,
    case
      when obs regexp "!!6709=1067!!" then @hiv_status := 1
      when obs regexp "!!6709=664!!" then @hiv_status := 2
      when obs regexp "!!6709=703!!" then @hiv_status := 3
      else @hiv_status = null
    end as hiv_status,
    case
      when obs regexp "!!12=1115!!" then @chest_xray_results := 1
      when obs regexp "!!12=1116!!" then @chest_xray_results := 2
      else @chest_xray_results = null
    end as chest_xray_results,
    case
      when obs regexp "!!7113=1115!!" then @chest_ct_scan_results := 1
      when obs regexp "!!7113=1116!!" then @chest_ct_scan_results := 2
      else @chest_ct_scan_results = null
    end as chest_ct_scan_results,
    case
      when obs regexp "!!10123=1115!!" then @pet_scan_results := 1
      when obs regexp "!!10123=1116!!" then @pet_scan_results := 2
      else @pet_scan_results = null
    end as pet_scan_results,
    case
      when obs regexp "!!845=1115!!" then @abdominal_ultrasound_result := 1
      when obs regexp "!!845=1116!!" then @abdominal_ultrasound_result := 2
      else @abdominal_ultrasound_result = null
    end as abdominal_ultrasound_result,
    case
      when obs regexp "!!10124=" then @other_imaging_results = GetValues(obs, 10124)
      else @other_imaging_results = null
    end as other_imaging_results,
    case
      when obs regexp "!!1271=" then @tests_ordered := GetValues(obs, 1271)
      else @tests_ordered = null
    end as tests_ordered,
    case
      when obs regexp "!!8190=" then @other_radiology_orders := GetValues(obs, 8190)
      else @other_radiology_orders = null
    end as other_radiology_orders,
    case
      when obs regexp "!!9538=" then @lab_results_notes := GetValues(obs, 9538)
      else @lab_results_notes = null
    end as lab_results_notes,
    case
      when obs regexp "!!10127=1107!!" then @procedure_ordered := 1
      when obs regexp "!!10127=10075!!" then @procedure_ordered := 2
      when obs regexp "!!10127=10076!!" then @procedure_ordered := 3
      when obs regexp "!!10127=10126!!" then @procedure_ordered := 4
      when obs regexp "!!10127=5622!!" then @procedure_ordered := 5
      else @procedure_ordered = null
    end as procedure_ordered,
    case
      when obs regexp "!!1915=" then @other_procedure_ordered_freetext := GetValues(lcfo.obs_string, 10128)
      else @other_procedure_ordered_freetext = null
    end as other_procedure_ordered_freetext,
    case
      when obs regexp "!!10442=" then @diagnosis_procedure_performed := GetValues(obs, 10442)
      else @diagnosis_procedure_performed = null
    end as diagnosis_procedure_performed,
    case
      when obs regexp "!!10443=" then @procedure_date := GetValues(obs, 10443)
      else @procedure_date = null
    end as procedure_date,
    case
      when obs regexp "!!7176=10129!!" then @lung_cancer_type := 1
      when obs regexp "!!7176=10130!!" then @lung_cancer_type := 2
      when obs regexp "!!7176=5622!!" then @lung_cancer_type := 3
      else @lung_cancer_type = null
    end as lung_cancer_type,
    case
      when obs regexp "!!10137=6563!!" then @small_cell_lung_cancer_staging := 1
      when obs regexp "!!10137=6564!!" then @small_cell_lung_cancer_staging := 2
      else @small_cell_lung_cancer_staging = null
    end as small_cell_lung_cancer_staging,
    case
      when obs regexp "!!10132=7421!!" then @non_small_cell_lung_cancer_type := 1
      when obs regexp "!!10132=7422!!" then @non_small_cell_lung_cancer_type := 2
      when obs regexp "!!10132=10131!!" then @non_small_cell_lung_cancer_type := 3
      when obs regexp "!!10132=10209!!" then @non_small_cell_lung_cancer_type := 3
      else @non_small_cell_lung_cancer_type = null
    end as non_small_cell_lung_cancer_type,
    case
      when obs regexp "!!9728=" then @date_of_diagnosis := DATE(
        replace(
          replace(
            (
              substring_index(substring(obs, locate("!!9728=", obs)), @sep, 1)
            ),
            "!!9728=",
            ""
          ),
          "!!",
          ""
        )
      )
      else @date_of_diagnosis = null
    end as date_of_diagnosis,
    case
      when obs regexp "!!9868=9852!!" then @cancer_staging := 1
      when obs regexp "!!9868=9856!!" then @cancer_staging := 2
      when obs regexp "!!9868=9860!!" then @cancer_staging := 3
      when obs regexp "!!9868=9864!!" then @cancer_staging := 4
      when obs regexp "!!9868=6563!!" then @cancer_staging := 5
      when obs regexp "!!9868=6564!!" then @cancer_staging := 6
      else @cancer_staging = null
    end as cancer_staging,
    case
      when obs regexp "!!10441=" then @cancer_staging_date := GetValues(obs, 10441)
      else @cancer_staging_date = null
    end as cancer_staging_date,
    case
      when obs regexp "!!10445=" then @metastasis_region := GetValues(obs, 10445)
      else @metastasis_region = null
    end as metastasis_region,
    case
      when obs regexp "!!8723=6575!!" then @treatment_plan := 1
      when obs regexp "!!8723=8427!!" then @treatment_plan := 2
      when obs regexp "!!8723=7465!!" then @treatment_plan := 3
      when obs regexp "!!8723=5622!!" then @treatment_plan := 4
      else @treatment_plan = null
    end as treatment_plan,
    case
      when obs regexp "!!2206=9218!!" then @treatment_intent := 1
      when obs regexp "!!2206=8428!!" then @treatment_intent := 2
      when obs regexp "!!2206=8724!!" then @treatment_intent := 3
      when obs regexp "!!2206=9219!!" then @treatment_intent := 4
      else @treatment_intent = null
    end as treatment_intent,
    case
      when obs regexp "!!9869=1256!!" then @chemotherapy_plan := 1
      when obs regexp "!!9869=1259!!" then @chemotherapy_plan := 2
      when obs regexp "!!9869=1260!!" then @chemotherapy_plan := 3
      when obs regexp "!!9869=1257!!" then @chemotherapy_plan := 4
      else @chemotherapy_plan = null
    end as chemotherapy_plan,
    case
      when obs regexp "!!6643=" then @current_chemo_cycle := GetValues(obs, 6643)
      else @current_chemo_cycle = null
    end as current_chemo_cycle,
    case
      when obs regexp "!!9946=" then @chemotherapy_regimen := GetValues(obs, 9946)
      else @chemotherapy_regimen = null
    end as chemotherapy_regimen,
    case
      when obs regexp "!!1190=" then @chemo_start_date := GetValues(obs, 1190)
      else @chemo_start_date = null
    end as chemo_start_date,
    case
      when obs regexp "!!9869=1260!!" then @chemo_start_date := GetValues(obs_datetimes, 9869)
      else @chemo_stop_date = null
    end as chemo_stop_date,
    case
      when obs regexp "!!6644=" then @total_planned_chemo_cycles := GetValues(obs, 6644)
      else @total_planned_chemo_cycles = null
    end as total_planned_chemo_cycles,
    case
      when obs regexp "!!9927=1267!!" then @reasons_for_changing_or_stopping_chemo := 1 -- Completed
      when obs regexp "!!9927=7391!!" then @reasons_for_changing_or_stopping_chemo := 2 -- No response / Resistant
      when obs regexp "!!9927=6629!!" then @reasons_for_changing_or_stopping_chemo := 3 -- Progression
      when obs regexp "!!9927=6627!!" then @reasons_for_changing_or_stopping_chemo := 4 -- Partial response
      when obs regexp "!!9927=1879!!" then @reasons_for_changing_or_stopping_chemo := 5 -- Toxicity, cause
      when obs regexp "!!9927=5622!!" then @reasons_for_changing_or_stopping_chemo := 6 -- Other (non-coded)
      else @reasons_for_changing_or_stopping_chemo = null
    end as reasons_for_changing_or_stopping_chemo,
    case
      when obs regexp "!!1915=" then @other_chemo_stop_reason_freetext := GetValues(lcfo.obs_string, 9928)
      else @other_chemo_stop_reason_freetext = null
    end as other_chemo_stop_reason_freetext,
    case
      when obs regexp "!!1915=" then @other_chemo_drug_started_freetext_started := GetValues(lcfo.obs_string, 9923)
      else @other_chemo_drug_started_freetext = null
    end as other_chemo_drug_started_freetext,
    case
      when obs regexp "!!10134=" then @surgery_date := GetValues(obs, 10134)
      else @surgery_date = null
    end as surgery_date,
    case
      when obs regexp "!!10133=" then @radiotherapy_sessions := GetValues(obs, 10133)
      else @radiotherapy_sessions = null
    end as radiotherapy_sessions,
    case
      when obs regexp "!!10039=" then @other_treatment_plan := GetValues(obs, 10039)
      else @other_treatment_plan = null
    end as other_treatment_plan,
    case
      when obs regexp "!!9918=" then @chemo_drug := GetValues(obs, 9918)
      else @chemo_drug = null
    end as chemo_drug,
    case
      when obs regexp "!!1899=" then @dosage_in_milligrams := GetValues(obs, 1899)
      else @dosage_in_milligrams = null
    end as dosage_in_milligrams,
    case
      when obs regexp "!!7463=" then @drug_route := GetValues(obs, 7463)
      else @drug_route = null
    end as drug_route,
    case
      when obs regexp "!!1895=" then @other_drugs := GetValues(obs, 1895)
      else @other_drugs = null
    end as other_drugs,
    case
      when obs regexp "!!1779=" then @other_post_chemo_or_non_chemo_drug := GetValues(obs, 1779)
      else @other_post_chemo_or_non_chemo_drug = null
    end as other_post_chemo_or_non_chemo_drug,
    case
      when obs regexp "!!2206=" then @drug_purpose := GetValues(obs, 2206)
      else @drug_purpose = null
    end as drug_purpose,
    case
      when obs regexp "!!1272=" then @referrals_ordered := GetValues(obs, 1272)
      else @referrals_ordered = null
    end as referrals_ordered,
    case
      when obs regexp "!!1915=" then @other_referrals_freetext := GetValues(lcfo.obs_string, 1932)
      else @other_referrals_freetext = null
    end as other_referrals_freetext,
    case
      when obs regexp "!!6634=" then @toxicity_assessment := GetValues(obs, 6634)
      else @toxicity_assessment = null
    end as toxicity_assessment,
    case
      when obs regexp "!!1915=" then @other_toxicity_assessment_freetext := GetValues(lcfo.obs_string, 9929)
      else @other_toxicity_assessment_freetext = null
    end as other_toxicity_assessment_freetext,
    case
      when obs regexp "!!7222=" then @therapeutic_plan_notes := GetValues(obs, 7222)
      else @therapeutic_plan_notes = null
    end as therapeutic_plan_notes,
    case
      when obs regexp "!!5096=" then @rtc_date := GetValues(obs, 5096)
      else @rtc_date = null
    end as rtc_date
  from flat_lung_cancer_treatment_0 t1
    join amrs.person p using (person_id)
    join amrs.location l using (location_id)
    LEFT JOIN lung_cancer_freetext_obs `lcfo` ON t1.person_id = lcfo.patient_id
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
alter table flat_lung_cancer_treatment_1 drop prev_id,
  drop cur_id;
drop table if exists flat_lung_cancer_treatment_2;
create temporary table flat_lung_cancer_treatment_2 (
  select *,
    @prev_id := @cur_id as prev_id,
    @cur_id := person_id as cur_id,
    case
      when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
      else @prev_encounter_datetime := null
    end as next_encounter_datetime_lung_cancer_treatment,
    @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,
    case
      when @prev_id = @cur_id then @next_encounter_type := @cur_encounter_type
      else @next_encounter_type := null
    end as next_encounter_type_lung_cancer_treatment,
    @cur_encounter_type := encounter_type as cur_encounter_type,
    case
      when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
      else @prev_clinical_datetime := null
    end as next_clinical_datetime_lung_cancer_treatment,
    case
      when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
      else @prev_clinical_location_id := null
    end as next_clinical_location_id_lung_cancer_treatment,
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
    end as next_clinical_rtc_date_lung_cancer_treatment,
    case
      when @prev_id = @cur_id then @cur_clinical_rtc_date
      else @cur_clinical_rtc_date := null
    end as cur_clinical_rtc_date
  from flat_lung_cancer_treatment_1
  order by person_id,
    date(encounter_datetime) desc,
    encounter_type_sort_index desc
);
alter table flat_lung_cancer_treatment_2 drop prev_id,
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
drop temporary table if exists flat_lung_cancer_treatment_3;
create temporary table flat_lung_cancer_treatment_3 (
  prev_encounter_datetime datetime,
  prev_encounter_type int,
  index person_enc (person_id, encounter_datetime desc)
) (
  select *,
    @prev_id := @cur_id as prev_id,
    @cur_id := t1.person_id as cur_id,
    case
      when @prev_id = @cur_id then @prev_encounter_type := @cur_encounter_type
      else @prev_encounter_type := null
    end as prev_encounter_type_lung_cancer_treatment,
    @cur_encounter_type := encounter_type as cur_encounter_type,
    case
      when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
      else @prev_encounter_datetime := null
    end as prev_encounter_datetime_lung_cancer_treatment,
    @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,
    case
      when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
      else @prev_clinical_datetime := null
    end as prev_clinical_datetime_lung_cancer_treatment,
    case
      when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
      else @prev_clinical_location_id := null
    end as prev_clinical_location_id_lung_cancer_treatment,
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
    end as prev_clinical_rtc_date_lung_cancer_treatment,
    case
      when @prev_id = @cur_id then @cur_clinical_rtc_date
      else @cur_clinical_rtc_date := null
    end as cur_clinic_rtc_date
  from flat_lung_cancer_treatment_2 t1
  order by person_id,
    date(encounter_datetime),
    encounter_type_sort_index
);
SELECT COUNT(*) INTO @new_encounter_rows
FROM flat_lung_cancer_treatment_3;
SELECT @new_encounter_rows;
set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;
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
                facility_name,
                gender,
                age,
                birthdate,
                death_date,
                cur_visit_type,
                referred_from,
                referring_health_facility,
                referral_date,
                referred_by,
                referring_health_provider_freetext,
                marital_status,
                main_occupation,
                main_occupation_freetext,
                patient_education_level,
                smokes_cigarettes,
                sticks_smoked_per_day,
                years_of_smoking_cigarettes,
                years_since_last_use_of_cigarettes,
                tobacco_use,
                years_of_tobacco_use,
                drinks_alcohol,
                chemical_exposure,
                other_chemical_exposure_freetext,
                asbestos_exposure,
                any_family_member_with_cancer_or_specific_chronic_illness,
                family_member_with_specific_chronic_illness,
                specific_family_member_chronic_illness_freetext,
                chief_complaints,
                other_complaints_freetext,
                complaint_duration,
                no_of_days_with_complaint,
                no_of_weeks_with_complaint,
                no_of_months_with_complaint,
                no_of_years_with_complaint,
                ecog_performance_index,
                general_exam,
                heent_exam,
                chest_exam,
                heart_exam,
                abdomen_exam,
                urogenital_exam,
                extremities_exam,
                testicular_exam,
                nodal_survey,
                musculo_skeletal_exam,
                neurologic_exam,
                physical_exam_notes,
                no_of_hospitalizations_last_12_months,
                diagnosis_history,
                year_diagnosed,
                hiv_status,
                chest_xray_results,
                chest_ct_scan_results,
                pet_scan_results,
                abdominal_ultrasound_result,
                other_imaging_results,
                tests_ordered,
                other_radiology_orders,
                lab_results_notes,
                procedure_ordered,
                other_procedure_ordered_freetext,
                diagnosis_procedure_performed,
                procedure_date,
                lung_cancer_type,
                small_cell_lung_cancer_staging,
                non_small_cell_lung_cancer_type,
                date_of_diagnosis,
                cancer_staging,
                cancer_staging_date,
                metastasis_region,
                treatment_plan,
                treatment_intent,
                chemotherapy_plan,
                current_chemo_cycle,
                chemotherapy_regimen,
                chemo_start_date,
                chemo_stop_date,
                total_planned_chemo_cycles,
                reasons_for_changing_or_stopping_chemo,
                other_chemo_stop_reason_freetext,
                other_chemo_drug_started_freetext,
                surgery_date,
                radiotherapy_sessions,
                other_treatment_plan,
                chemo_drug,
                dosage_in_milligrams,
                drug_route,
                other_drugs,
                other_post_chemo_or_non_chemo_drug,
                drug_purpose,
                referrals_ordered,
                other_referrals_freetext,
                toxicity_assessment,
                other_toxicity_assessment_freetext,
                therapeutic_plan_notes,
                rtc_date,
                prev_encounter_datetime_lung_cancer_treatment,
                next_encounter_datetime_lung_cancer_treatment,
                prev_encounter_type_lung_cancer_treatment,
                next_encounter_type_lung_cancer_treatment,
                prev_clinical_datetime_lung_cancer_treatment,
                next_clinical_datetime_lung_cancer_treatment,
                prev_clinical_location_id_lung_cancer_treatment,
                next_clinical_location_id_lung_cancer_treatment,
                prev_clinical_rtc_date_lung_cancer_treatment,
                next_clinical_rtc_date_lung_cancer_treatment

                from flat_lung_cancer_treatment_3 t1
                join amrs.location t2 using (location_id))'
  );
PREPARE s1
from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;
SET @dyn_sql = CONCAT(
    'delete t1 from ',
    @queue_table,
    ' t1 join flat_lung_cancer_treatment_build_queue__0 t2 using (person_id);'
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
SELECT @person_ids_count AS 'persons remaining',
  @cycle_length AS 'Cycle time (s)',
  CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
  @remaining_time AS 'Est time remaining (min)';
end while;
if (@query_type = "build") then
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
SELECT CONCAT(
    @start_write,
    ' : Writing ',
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
SELECT CONCAT(
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
SELECT CONCAT(
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
SELECT CONCAT(
    @table_version,
    ' : Time to complete: ',
    TIMESTAMPDIFF(MINUTE, @start, @end),
    ' minutes'
  );
END