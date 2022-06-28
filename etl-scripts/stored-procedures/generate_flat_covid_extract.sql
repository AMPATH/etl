DELIMITER $$
CREATE  PROCEDURE `generate_flat_covid_extract`(IN query_type varchar(50),IN queue_number int, IN queue_size int, IN cycle_size int,IN log BOOLEAN)
BEGIN

					set @primary_table := "flat_covid_extract";
                    set @total_rows_written = 0;
					set @start = now();
					set @table_version = "flat_covid_v1.0";
                    set @query_type=query_type;
                    set @last_date_created := null;
                    -- set @last_date_created = (select max(DateCreated) from ndwr.flat_covid_extract);
                    set @endDate := LAST_DAY(CURDATE());
                    set @boundary := '!!';
                    set @log_build = log;

CREATE TABLE IF NOT EXISTS flat_covid_extract (
    `person_id` INT NOT NULL,
    `location_id` INT NOT NULL,
    `encounter_id` INT NOT NULL,
    `encounter_datetime` DATETIME NULL,
    `next_encounter_datetime` DATETIME NULL,
    `received_covid_19_vaccine` VARCHAR(100) NULL,
    `date_given_first_dose` DATETIME NULL,
    `first_dose_vaccine_administered` VARCHAR(100) NULL,
    `date_given_second_dose` DATETIME NULL,
    `second_dose_vaccine_administered` VARCHAR(100) NULL,
    `vaccination_status` VARCHAR(100) NULL,
    `vaccine_verification` VARCHAR(100) NULL,
    `vaccine_verification_second_dose` VARCHAR(100) NULL,
    `booster_given` VARCHAR(10) NULL,
    `booster_vaccine` VARCHAR(30) NULL,
    `booster_dose_date` DATETIME NULL,
    `booster_dose` INT NULL,
    `booster_dose_verified` VARCHAR(50) NULL,
    `sequence` VARCHAR(50) NULL,
    `covid_19_test_result` VARCHAR(20) NULL,
    `covid_19_test_date` DATETIME NULL,
    `patient_status` VARCHAR(50) NULL,
    `hospital_admission` VARCHAR(50) NULL,
    `admission_unit` VARCHAR(100) NULL,
    `missed_appointment_due_to_covid_19` VARCHAR(100) NULL,
    `covid_19_positive_since_last_visit` VARCHAR(100) NULL,
    `covid_19_test_date_since_last_visit` VARCHAR(100) NULL,
    `patient_status_since_last_visit` VARCHAR(100) NULL,
    `admission_status_since_last_visit` VARCHAR(100) NULL,
    `admission_start_date` DATETIME NULL,
    `admission_end_date` DATETIME NULL,
    `admission_unit_since_last_visit` VARCHAR(50) NULL,
    `supplemental_oxygen_received` VARCHAR(10) NULL,
    `patient_ventilated` VARCHAR(10) NULL,
    `ever_covid_19_positive` VARCHAR(50) NULL,
    `tracing_final_outcome` VARCHAR(100) NULL,
    `cause_of_death` VARCHAR(100) NULL,
    `DateCreated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    ever_hopsitalized smallint,
    INDEX patient_covid_person_id (person_id),
    INDEX patient_covid_location_id (location_id),
    INDEX patient_covid_encounter_date (encounter_datetime),
    INDEX patient_covid_date_created (DateCreated),
    INDEX patient_patient_covid_facility (person_id,location_id),
    INDEX covid_status(vaccination_status),
    INDEX patient_patient_covid_status (person_id,vaccination_status),
    INDEX (ever_hopsitalized)
);

                    if(@query_type="build") then

							              select 'BUILDING..........................................';
                            set @write_table = concat("flat_covid_extract_temp_",queue_number);
                            set @queue_table = concat("flat_covid_extract_build_queue_",queue_number);

										  SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_covid_extract_build_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from flat_covid_extract_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                                          PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1; 
                                          
										 
                                          
                                         
                                          
				  end if;
                  
                                    if(@query_type="sync") then

							              select 'BUILDING..........................................';
										  set @write_table = concat("flat_covid_extract_temp_",queue_number);
											set @queue_table = concat("flat_covid_extract_build_queue_",queue_number);

										  SET @dyn_sql=CONCAT('create table if not exists ',@write_table,' like ',@primary_table);
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  


							              SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_covid_extract_sync_queue limit ', queue_size, ');'); 
							              PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1;  

							              SET @dyn_sql=CONCAT('delete t1 from flat_covid_extract_sync_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                                          PREPARE s1 from @dyn_sql; 
							              EXECUTE s1; 
							              DEALLOCATE PREPARE s1; 
                                          
										 
                                          
                                         
                                          
				               end if;
                  
SELECT 
    CONCAT('Deleting test patients from ',
            @queue_table);
                  
                  
                   SET @dyn_sql=CONCAT('delete t1 FROM ',@queue_table,' t1
                            join amrs.person_attribute t2 using (person_id)
                            where t2.person_attribute_type_id=28 and value="true" and voided=0');
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;
                    
                  
                  SET @person_ids_count = 0;
				  SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
				  PREPARE s1 from @dyn_sql; 
				  EXECUTE s1; 
				  DEALLOCATE PREPARE s1;

SELECT @person_ids_count AS 'num patients to build';
                  
SELECT CONCAT('Deleting data from ', @primary_table);
                    
					SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 on (t1.person_id = t2.person_id);'); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;

                    set @total_time=0;
                    set @cycle_number = 0;
                    set @last_encounter_date=null;
                    set @status=null;                            
					set @last_encounter_date=null;                            
					set @rtc_date=null; 

                    while @person_ids_count > 0 do

                        	set @loop_start_time = now();
							drop  table if exists flat_covid_extract_build_queue__0;

                                      SET @dyn_sql=CONCAT('create temporary table if not exists flat_covid_extract_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
						              PREPARE s1 from @dyn_sql; 
						              EXECUTE s1; 
						              DEALLOCATE PREPARE s1;


                         ## create covid_screenings temporary table
                         drop table if exists etl.flat_covid_encounters_temp;
CREATE TABLE etl.flat_covid_encounters_temp (SELECT o.patient_id AS person_id,
    o.encounter_id,
    o.encounter_datetime,
    o.location_id FROM
    etl.flat_covid_extract_build_queue__0 q
        JOIN
    amrs.encounter o ON (o.patient_id = q.person_id)
WHERE
    o.encounter_type IN (208)
    and o.encounter_datetime >= '2022-04-01 00:00:00'
        AND o.voided = 0);
                         
SELECT CONCAT('Creating flat_covid_immunization_obs_test table');
drop temporary table if exists etl.flat_covid_obs_test;
create temporary table etl.flat_covid_obs_test(
SELECT 
    og.person_id,
    t.encounter_id,
    t.encounter_datetime,
    og.obs_id as 'obs_group_id',
    o.obs_id,
    o.concept_id,
    o.value_coded,
    o.value_datetime,
    o.value_numeric,
        GROUP_CONCAT(CASE
            WHEN
                o.value_coded IS NOT NULL
            THEN
                CONCAT(@boundary,
                        o.concept_id,
                        '=',
                        o.value_coded,
                        @boundary)
            WHEN
                o.value_numeric IS NOT NULL
            THEN
                CONCAT(@boundary,
                        o.concept_id,
                        '=',
                        o.value_numeric,
                        @boundary)
            WHEN
                o.value_datetime IS NOT NULL
            THEN
                CONCAT(@boundary,
                        o.concept_id,
                        '=',
                        DATE(o.value_datetime),
                        @boundary)
            WHEN
                o.value_text IS NOT NULL
            THEN
                CONCAT(@boundary,
                        o.concept_id,
                        '=',
                        o.value_text,
                        @boundary)
            WHEN
                o.value_modifier IS NOT NULL
            THEN
                CONCAT(@boundary,
                        o.concept_id,
                        '=',
                        o.value_modifier,
                        @boundary)
        END
        ORDER BY o.concept_id , o.value_coded
        SEPARATOR ' ## ') AS obs,
         GROUP_CONCAT(CASE
            WHEN
                o.value_coded IS NOT NULL
                    OR o.value_numeric IS NOT NULL
                    OR o.value_datetime IS NOT NULL
                    OR o.value_text IS NOT NULL
                    OR o.value_drug IS NOT NULL
                    OR o.value_modifier IS NOT NULL
            THEN
                CONCAT(@boundary,
                        o.concept_id,
                        '=',
                        DATE(o.obs_datetime),
                        @boundary)
        END
        ORDER BY o.concept_id , o.value_coded
        SEPARATOR ' ## ') AS obs_datetimes
FROM
    flat_covid_encounters_temp t
        JOIN
    amrs.obs og ON (t.encounter_id = og.encounter_id
        AND og.voided = 0)
   join amrs.obs o on (o.obs_group_id = og.obs_id)
WHERE
         og.concept_id IN(1390,1944)
		AND og.voided = 0
        AND o.voided = 0
GROUP BY og.obs_id
ORDER BY og.obs_datetime asc, og.encounter_id);


SELECT CONCAT('Creating ndwr_immunization_data');

                            SET @DategivenFirstDose:= NULL;
                            SET @FirstDoseVaccineAdministered := NULL;
                            SET @DateGivenSecondDose := NULL;
                            SET @SecondDoseVaccineAdministered := null;
                            set @VaccinationStatus := null;
                            SET @VaccineVerification:= NULL;
                            SET @VaccineVerificationSecondDose := NULL;
                            set @prev_id = -1;
                            set @cur_id = -1;

drop  table if exists etl.flat_immunization_data;
CREATE TABLE etl.flat_immunization_data (SELECT i.*,
    @prev_id:=@cur_id AS prev_id,
    @cur_id:=i.person_id AS cur_id,
    CASE
        WHEN i.obs REGEXP '!!10485=1!!' THEN 1
        WHEN i.obs REGEXP '!!10485=2!!' THEN 2
        WHEN i.obs REGEXP '!!10485=3!!' THEN 3
        ELSE 0
    END AS vaccince_sort_index,
    CASE
        WHEN i.obs REGEXP '!!10485=1!!' THEN @DategivenFirstDose:=etl.GetValues(i.obs, 10958)
        WHEN @prev_id = @cur_id THEN @DategivenFirstDose
        ELSE @DategivenFirstDose:=NULL
    END AS date_given_first_dose,
    CASE
        WHEN i.obs REGEXP '!!10485=1!!' THEN @FirstDoseVaccineAdministered:=etl.GetValues(i.obs, 984)
        WHEN @prev_id = @cur_id THEN @FirstDoseVaccineAdministered
        ELSE @FirstDoseVaccineAdministered:=NULL
    END AS first_dose_vaccine_administered,
    CASE
        WHEN i.obs REGEXP '!!10485=2!!' THEN @DateGivenSecondDose:=etl.GetValues(i.obs, 10958)
        WHEN @prev_id = @cur_id THEN @DateGivenSecondDose
        ELSE @DateGivenSecondDose:=NULL
    END AS date_given_second_dose,
    CASE
        WHEN i.obs REGEXP '!!10485=2!!' THEN @SecondDoseVaccineAdministered:=etl.GetValues(i.obs, 984)
        WHEN @prev_id = @cur_id THEN @SecondDoseVaccineAdministered
        ELSE @SecondDoseVaccineAdministered:=NULL
    END AS second_dose_vaccine_administered,
    CASE
        WHEN i.obs REGEXP '!!2300=' THEN @VaccinationStatus:=etl.GetValues(i.obs, 2300)
        WHEN @prev_id = @cur_id THEN @VaccinationStatus
        ELSE @VaccinationStatus:=NULL
    END AS vaccination_status,
    CASE
        WHEN i.obs REGEXP '!!11906=' THEN @VaccineVerification:=etl.GetValues(i.obs, 11906)
        WHEN @prev_id = @cur_id THEN @VaccineVerification
        ELSE @VaccineVerification:=NULL
    END AS vaccine_verification,
    CASE
        WHEN
            i.obs REGEXP '!!11906='
                AND i.obs REGEXP '!!10485=2!!'
        THEN
            @VaccineVerificationSecondDose:=etl.GetValues(i.obs, 11906)
        WHEN @prev_id = @cur_id THEN @VaccineVerificationSecondDose
        ELSE @VaccineVerificationSecondDose:=NULL
    END AS vaccine_verification_second_dose 
    FROM
    flat_covid_obs_test i
ORDER BY i.person_id ,i.encounter_datetime asc);
 
SELECT CONCAT('Creating flat_vaccination_encounter_summary table');
 drop  table if exists etl.flat_vaccination_encounter_summary;
 create  table etl.flat_vaccination_encounter_summary(
 SELECT 
    t.*,
    fd.date_given_first_dose,
	fd.first_dose_vaccine_administered,
	sd.date_given_second_dose,
	sd.second_dose_vaccine_administered,
    CASE
      WHEN sd.vaccination_status IS NOT NULL THEN sd.vaccination_status
      WHEN fd.vaccination_status IS NOT NULL  AND  sd.vaccination_status is null THEN fd.vaccination_status
      ELSE sd.vaccination_status
    END AS vaccination_status,
     CASE
      WHEN sd.vaccine_verification IS NOT NULL THEN sd.vaccine_verification
      WHEN fd.vaccine_verification IS NOT NULL AND sd.vaccine_verification is null THEN fd.vaccine_verification
      ELSE sd.vaccine_verification
    END AS vaccine_verification,
	sd.vaccine_verification_second_dose
FROM
    etl.flat_covid_encounters_temp t
        LEFT JOIN
    etl.flat_immunization_data fd ON (t.person_id = fd.person_id
        AND t.encounter_id = fd.encounter_id
        AND fd.vaccince_sort_index = 1)
        LEFT JOIN
    etl.flat_immunization_data sd ON (t.person_id = sd.person_id
        AND t.encounter_id = sd.encounter_id
        AND sd.vaccince_sort_index = 2)
        group by t.person_id, t.encounter_id
 );
 
 
SELECT CONCAT('Creating flat_covid_booster_obs_test table');

drop temporary table if exists etl.flat_covid_booster_obs_test;
create temporary table etl.flat_covid_booster_obs_test(
SELECT 
    og.person_id,
    t.encounter_id,
    t.encounter_datetime,
    og.obs_id as 'obs_group_id',
    o.obs_id,
    o.concept_id,
    o.value_coded,
    o.value_datetime,
    o.value_numeric,
        GROUP_CONCAT(CASE
            WHEN
                o.value_coded IS NOT NULL
            THEN
                CONCAT(@boundary,
                        o.concept_id,
                        '=',
                        o.value_coded,
                        @boundary)
            WHEN
                o.value_numeric IS NOT NULL
            THEN
                CONCAT(@boundary,
                        o.concept_id,
                        '=',
                        o.value_numeric,
                        @boundary)
            WHEN
                o.value_datetime IS NOT NULL
            THEN
                CONCAT(@boundary,
                        o.concept_id,
                        '=',
                        DATE(o.value_datetime),
                        @boundary)
            WHEN
                o.value_text IS NOT NULL
            THEN
                CONCAT(@boundary,
                        o.concept_id,
                        '=',
                        o.value_text,
                        @boundary)
            WHEN
                o.value_modifier IS NOT NULL
            THEN
                CONCAT(@boundary,
                        o.concept_id,
                        '=',
                        o.value_modifier,
                        @boundary)
        END
        ORDER BY o.concept_id , o.value_coded
        SEPARATOR ' ## ') AS obs,
         GROUP_CONCAT(CASE
            WHEN
                o.value_coded IS NOT NULL
                    OR o.value_numeric IS NOT NULL
                    OR o.value_datetime IS NOT NULL
                    OR o.value_text IS NOT NULL
                    OR o.value_drug IS NOT NULL
                    OR o.value_modifier IS NOT NULL
            THEN
                CONCAT(@boundary,
                        o.concept_id,
                        '=',
                        DATE(o.obs_datetime),
                        @boundary)
        END
        ORDER BY o.concept_id , o.value_coded
        SEPARATOR ' ## ') AS obs_datetimes
FROM
    flat_covid_encounters_temp t
        JOIN
    amrs.obs og ON (t.encounter_id = og.encounter_id
        AND og.voided = 0)
   join amrs.obs o on (o.obs_group_id = og.obs_id)
WHERE
        og.concept_id = 1944
        AND og.voided = 0
        AND o.voided = 0
GROUP BY og.obs_id
ORDER BY og.obs_datetime asc, og.encounter_id);

SELECT CONCAT('Creating flat_booster_data');


 SET @BoosterVaccine:= NULL;
 SET @BoosterDoseDate := NULL;
 SET @BoosterDose := NULL;
 SET @BoosterDoseVerified := null;
 set @prev_id = -1;
 set @cur_id = -1;

drop temporary table if exists etl.flat_booster_data;
create temporary table etl.flat_booster_data(
select
i.*,
 @prev_id := @cur_id as prev_id,
 @cur_id := i.person_id as cur_id,
 CASE
  WHEN i.obs regexp "!!10485=1!!" THEN 1
  WHEN i.obs regexp "!!10485=2!!" THEN 2
  WHEN i.obs regexp "!!10485=3!!" THEN 3
  else 0
END AS booster_sort_index,
CASE
  WHEN i.obs regexp "!!10485=1!!" THEN @BoosterVaccine := etl.GetValues(i.obs,984)
  when @prev_id = @cur_id then @BoosterVaccine
  else @BoosterVaccine := null
END AS booster_vaccine,
CASE
  WHEN i.obs regexp "!!10485=1!!" THEN @BoosterDoseDate := etl.GetValues(i.obs,10958)
  when @prev_id = @cur_id then @BoosterDoseDate
  else @BoosterDoseDate := null
END AS booster_dose_date,
CASE
  WHEN i.obs regexp "!!10485=1!!" THEN @BoosterDose := etl.GetValues(i.obs,10485)
  when @prev_id = @cur_id then @BoosterDose
  else @BoosterDose:= null
END AS booster_dose,
CASE
  WHEN i.obs regexp "!!11906=" THEN @BoosterDoseVerified := etl.GetValues(i.obs,11906)
  when @prev_id = @cur_id then @BoosterDoseVerified
  else @BoosterDoseVerified:= null
END AS booster_dose_verified

FROM
 flat_covid_booster_obs_test i
 order by i.encounter_datetime, booster_sort_index asc);
 
SELECT CONCAT('Creating flat_booster_encounter_summary table');
 drop temporary table if exists etl.flat_booster_encounter_summary;
 create temporary table flat_booster_encounter_summary(
 SELECT 
    t.*,
    fd.booster_vaccine,
    fd.booster_dose_date,
	fd.booster_dose,
	fd.booster_dose_verified
FROM
    etl.flat_covid_encounters_temp t
        LEFT JOIN
    etl.flat_booster_data fd ON (t.person_id = fd.person_id
        AND t.encounter_id = fd.encounter_id
        AND fd.booster_sort_index = 1)
        group by t.person_id, t.encounter_id
 );
 
 
SELECT CONCAT('Creating flat_covid_screening_data');
 
 set @ReceivedCOVID19Vaccine:= null;
 set @COVID19TestEver:= null;
 set @COVID19TestResult:= null;
 set @COVID19TestDate:= null;
 set @COVID19Presentation:= null;
 set @HospitalAdmission:= null;
 set @AdmissionUnit:= null;
 set @COVID19PositiveSinceLasVisit:= null;
 set @COVID19TestDateSinceLastVisit:= null;
 set @COVID19PresentationSinceLastVisit:= null;
 set @AdmissionStatusSinceLastVisit:= null;
 set @AdmissionStartDate:= null;
 set @AdmissionEndDate:= null;
 set @AdmissionUnitSinceLastVisit:= null;
 set @SupplementalOxygenReceived:= null;
 set @BoosterGiven:= null;
 set @EverCovid19positive:= null;
 set @EverHospitalized := null;
 set @prev_id = -1;
 set @cur_id = -1;
 
  drop temporary table if exists etl.flat_covid_screening_data;
 create  temporary table etl.flat_covid_screening_data(
    select 
    t.*,
	@prev_id := @cur_id as prev_id,
	@cur_id := t.person_id as cur_id,
    CASE
	  WHEN o.obs regexp "!!11899=" THEN @ReceivedCOVID19Vaccine := etl.GetValues(o.obs,11899)
      WHEN o.obs regexp "!!10485=(1|2)!!" THEN @ReceivedCOVID19Vaccine := 1065
	  when @prev_id = @cur_id then @ReceivedCOVID19Vaccine
	  else @ReceivedCOVID19Vaccine := null
	END AS received_covid_19_vaccine,
    CASE
	  WHEN o.obs regexp "!!11911=" THEN @BoosterGiven := etl.GetValues(o.obs,11911)
      when @prev_id = @cur_id then @BoosterGiven
	  else @BoosterGiven := null
	END AS booster_given,
	CASE
	  WHEN o.obs regexp "!!11909=" THEN @COVID19TestEver := etl.GetValues(o.obs,11909)
	  when @prev_id = @cur_id then @COVID19TestEver
	  else @COVID19TestEver := null
	END AS 'covid_19_test_ever',
    CASE
	  WHEN o.obs regexp "!!11909=" THEN @COVID19TestResult := etl.GetValues(o.obs,11908)
	  when @prev_id = @cur_id then @COVID19TestResult
	  else @COVID19TestResult := null
	END AS 'covid_19_test_result',
    CASE
	  WHEN o.obs regexp "!!9728=" THEN @COVID19TestDate := etl.GetValues(o.obs,9728)
	  when @prev_id = @cur_id then @COVID19TestDate
	  else @COVID19TestDate := null
	END AS 'covid_19_test_date',
    CASE
	  WHEN o.obs regexp "!!11124=" THEN @COVID19Presentation := etl.GetValues(o.obs,11124)
      when @prev_id = @cur_id then @COVID19Presentation
	  else @COVID19Presentation := null
	END AS 'covid_19_presentation',
	CASE
	  WHEN o.obs regexp "!!6419=" THEN @HospitalAdmission := etl.GetValues(o.obs,6419)
	  when @prev_id = @cur_id then @HospitalAdmission
	  else @HospitalAdmission := null
	END AS 'hospital_admission',
    CASE
	  WHEN o.obs regexp "!!11912=" THEN @AdmissionUnit := etl.GetValues(o.obs,11912)
	  when @prev_id = @cur_id then @AdmissionUnit
	  else @AdmissionUnit := null
	END AS 'admission_unit',
    CASE
	  WHEN o.obs regexp "!!11916=" THEN @SupplementalOxygenReceived := etl.GetValues(o.obs,11916)
      when @prev_id = @cur_id then @SupplementalOxygenReceived
	  else @SupplementalOxygenReceived := null
	END AS 'supplemental_oxygen_received',
    CASE
	  WHEN o.obs regexp "!!11908=703!!" THEN 1
	  when @prev_id = @cur_id then @EverCovid19positive
	  else @EverCovid19positive := null
	END AS 'ever_covid_19_positive',
    CASE
	  WHEN o.obs regexp "!!6419=1065!!" THEN 1
	  when @prev_id = @cur_id then @EverHospitalized 
	  else @EverHospitalized  := null
	END AS 'ever_hopsitalized'
    from  
    etl.flat_covid_encounters_temp t
    join etl.flat_obs o on (o.person_id = t.person_id AND t.encounter_id = o.encounter_id)
    order by t.person_id,t.encounter_datetime asc
 
 );
 
 
 
 
 
 
 
                         
SELECT CONCAT('Creating flat_covid_immunization_booster_screening table');

drop temporary table if exists flat_covid_immunization_booster_screening;
create temporary table flat_covid_immunization_booster_screening(
    SELECT 
     q.person_id,
     t.location_id,
     t.encounter_id,
     t.encounter_datetime,
     s.received_covid_19_vaccine,
     v.date_given_first_dose,
     v.first_dose_vaccine_administered,
     v.date_given_second_dose,
     v.second_dose_vaccine_administered,
     v.vaccination_status,
     v.vaccine_verification,
     v.vaccine_verification_second_dose,
     s.booster_given,
     b.booster_vaccine,
     b.booster_dose_date,
     b.booster_dose,
     NULL AS 'sequence',
     s.covid_19_test_result,
     b.booster_dose_verified,
     s.covid_19_test_date,
     NULL AS patient_status,
     s.hospital_admission,
     s.admission_unit,
     NULL AS missed_appointment_due_to_covid_19,
     NULL AS covid_19_positive_since_last_visit,
     NULL AS covid_19_test_date_since_last_visit,
     NULL AS patient_status_since_last_visit,
     NULL AS admission_status_since_last_visit,
     NULL AS admission_start_date,
     NULL AS admission_end_date,
     NULL AS admission_unit_since_last_visit,
     s.supplemental_oxygen_received,
     NULL AS patient_ventilated,
     s.ever_covid_19_positive,
     NULL AS tracing_final_outcome,
     NULL AS cause_of_death,
     s.ever_hopsitalized
    FROM
    etl.flat_covid_extract_build_queue__0 q
    join etl.flat_covid_encounters_temp t on (t.person_id = q.person_id)
    left join flat_vaccination_encounter_summary v on (v.person_id = q.person_id AND v.encounter_id = t.encounter_id)
    left join flat_booster_encounter_summary b on (b.person_id = q.person_id AND b.encounter_id = t.encounter_id)
    left join etl.flat_covid_screening_data s on (s.person_id = q.person_id AND s.encounter_id = t.encounter_id)

);


SELECT CONCAT('Creating ndwr_covid_next');

 set @prev_id = -1;
 set @cur_id = -1;
 set @prev_screening_date = null;
 set @cur_screening_date = null;
 set @cur_screening_date = null;

drop temporary table if exists etl.covid_next;
create temporary table etl.covid_next(
select 
@prev_id := @cur_id as prev_id,
@cur_id := t.person_id as cur_id,
 case
	when @prev_id = @cur_id then @prev_screening_date := @cur_screening_date
	else @prev_screening_date := null
end as next_encounter_datetime,
@cur_screening_date := t.encounter_datetime as cur_encounter_datetime,
t.* 
from 
flat_covid_immunization_booster_screening t 
order by t.encounter_datetime DESC
);

SET @DateGivenFirstDose = NULL;
SET @FirstDoseVaccineAdministered:= null;
SET @DateGivenSecondDose := NULL;
SET @SecondDoseVaccineAdministered := null;
set @VaccinationStatus:= null;
set @VaccineVerification:= null;
set @BoosterGiven:= null;
set @BoosterVaccine:= null;
set @BoosterDoseDate:= null;
set @BoosterDose:= null;
set @BoosterDoseVerified:= null;
set @prev_id = -1;
set @cur_id = -1;

alter table etl.covid_next drop column prev_id, drop column cur_id;

SELECT CONCAT('Creating lag table');
drop temporary table if exists etl.covid_lag;
create temporary table etl.covid_lag(
SELECT 
    @prev_id:=@cur_id AS prev_id,
    @cur_id:=e.person_id AS cur_id,
    CASE
        WHEN
            @DateGivenFirstDose IS NULL
                AND @prev_id != @cur_id
                AND e.date_given_first_dose IS NOT NULL
        THEN
            @DateGivenFirstDose:=e.date_given_first_dose
		WHEN
            @DateGivenFirstDose IS NULL
                AND @prev_id = @cur_id
                AND e.date_given_first_dose IS NOT NULL
        THEN
            @DateGivenFirstDose:=e.date_given_first_dose
        WHEN @prev_id = @cur_id THEN @DateGivenFirstDose
        ELSE @DateGivenFirstDose:=NULL
    END AS date_given_first_dose_lag,
    CASE
        WHEN
            @FirstDoseVaccineAdministered IS NULL
                AND @prev_id != @cur_id
                AND e.first_dose_vaccine_administered IS NOT NULL
        THEN
            @FirstDoseVaccineAdministered:=e.first_dose_vaccine_administered
		 WHEN
            @FirstDoseVaccineAdministered IS NULL
                AND @prev_id = @cur_id
                AND e.first_dose_vaccine_administered IS NOT NULL
        THEN
            @FirstDoseVaccineAdministered:=e.first_dose_vaccine_administered
        WHEN @prev_id = @cur_id THEN @FirstDoseVaccineAdministered
        ELSE @FirstDoseVaccineAdministered:=NULL
    END AS first_dose_vaccine_administered_lag,
    CASE
        WHEN
            @DateGivenSecondDose IS NULL
                AND @prev_id != @cur_id
                AND e.date_given_second_dose IS NOT NULL
        THEN
            @DateGivenSecondDose:=e.date_given_second_dose
		WHEN
            @DateGivenSecondDose IS NULL
                AND @prev_id = @cur_id
                AND e.date_given_second_dose IS NOT NULL
        THEN
            @DateGivenSecondDose:=e.date_given_second_dose
        WHEN @prev_id = @cur_id THEN @DateGivenSecondDose
        ELSE @DateGivenSecondDose:=NULL
    END AS date_given_second_dose_lag,
    CASE
        WHEN
            @SecondDoseVaccineAdministered IS NULL
                AND @prev_id != @cur_id
                AND e.second_dose_vaccine_administered IS NOT NULL
        THEN
            @SecondDoseVaccineAdministered:=e.second_dose_vaccine_administered
		 WHEN
            @SecondDoseVaccineAdministered IS NULL
                AND @prev_id = @cur_id
                AND e.second_dose_vaccine_administered IS NOT NULL
        THEN
            @SecondDoseVaccineAdministered:=e.second_dose_vaccine_administered
        WHEN @prev_id = @cur_id THEN @SecondDoseVaccineAdministered
        ELSE @SecondDoseVaccineAdministered:=NULL
    END AS second_dose_vaccine_administered_lag,
    CASE
        WHEN
            @VaccinationStatus IS NULL
                AND @prev_id != @cur_id
                AND e.vaccination_status IS NOT NULL
        THEN
            @VaccinationStatus:=e.vaccination_status
		 WHEN
            @VaccinationStatus IS NULL
                AND @prev_id = @cur_id
                AND e.vaccination_status IS NOT NULL
        THEN
            @VaccinationStatus:=e.vaccination_status
		WHEN @VaccinationStatus is NOT NULL AND e.vaccination_status = 2208 THEN @VaccinationStatus:=e.vaccination_status
        WHEN @prev_id = @cur_id THEN @VaccinationStatus
        ELSE @VaccinationStatus:=NULL
    END AS vaccination_status_lag,
    CASE
        WHEN
            @VaccineVerification IS NULL
                AND @prev_id != @cur_id
                AND e.vaccine_verification IS NOT NULL
        THEN
            @VaccineVerification:=e.vaccine_verification
		 WHEN
            @VaccineVerification IS NULL
                AND @prev_id = @cur_id
                AND e.vaccine_verification IS NOT NULL
        THEN
            @VaccineVerification := e.vaccine_verification
		WHEN @VaccineVerification is NOT NULL AND e.vaccine_verification = 2300 THEN @VaccineVerification := e.vaccine_verification
        WHEN @prev_id = @cur_id THEN @VaccineVerification
        ELSE @VaccineVerification:=NULL
    END AS vaccine_verification_lag,
    CASE
        WHEN
            @BoosterGiven IS NULL
                AND @prev_id != @cur_id
                AND e.booster_given IS NOT NULL
        THEN
            @BoosterGiven:=e.booster_given
		 WHEN
            @BoosterGiven IS NULL
                AND @prev_id = @cur_id
                AND e.booster_given IS NOT NULL
        THEN
            @BoosterGiven:=e.booster_given
		WHEN @BoosterGiven is NOT NULL AND e.booster_given = 1065 THEN @BoosterGiven:=e.booster_given
        WHEN @prev_id = @cur_id THEN @BoosterGiven
        ELSE @BoosterGiven:=NULL
    END AS booster_given_lag,
    CASE
        WHEN
            @BoosterVaccine IS NULL
                AND @prev_id != @cur_id
                AND e.booster_vaccine IS NOT NULL
        THEN
            @BoosterVaccine:=e.booster_vaccine
		 WHEN
            @BoosterVaccine IS NULL
                AND @prev_id = @cur_id
                AND e.booster_vaccine IS NOT NULL
        THEN
            @BoosterVaccine:=e.booster_vaccine
        WHEN @prev_id = @cur_id THEN @BoosterVaccine
        ELSE @BoosterVaccine:=NULL
    END AS booster_vaccine_lag,
    CASE
        WHEN
            @BoosterDoseDate IS NULL
                AND @prev_id != @cur_id
                AND e.booster_dose_date IS NOT NULL
        THEN
            @BoosterDoseDate:=e.booster_dose_date
		WHEN
            @BoosterDoseDate IS NULL
                AND @prev_id = @cur_id
                AND e.booster_dose_date IS NOT NULL
        THEN
            @BoosterDoseDate:=e.booster_dose_date
        WHEN @prev_id = @cur_id THEN @BoosterDoseDate
        ELSE @BoosterDoseDate:=NULL
    END AS booster_dose_date_lag,
    CASE
        WHEN
            @BoosterDose IS NULL
                AND @prev_id != @cur_id
                AND e.booster_dose IS NOT NULL
        THEN
            @BoosterDose:=e.booster_dose
		WHEN
            @BoosterDose IS NULL
                AND @prev_id = @cur_id
                AND e.booster_dose IS NOT NULL
        THEN
            @BoosterDose:=e.booster_dose
        WHEN @prev_id = @cur_id THEN @BoosterDose
        ELSE @BoosterDose:=NULL
    END AS booster_dose_lag,
    CASE
        WHEN
            @BoosterDoseVerified IS NULL
                AND @prev_id != @cur_id
                AND e.booster_dose_verified IS NOT NULL
        THEN
            @BoosterDoseVerified:=e.booster_dose_verified
		WHEN
            @BoosterDoseVerified IS NULL
                AND @prev_id = @cur_id
                AND e.booster_dose_verified IS NOT NULL
        THEN
            @BoosterDoseVerified:=e.booster_dose_verified
        WHEN @prev_id = @cur_id THEN @BoosterDoseVerified
        ELSE @BoosterDoseVerified:=NULL
    END AS booster_dose_verified_lag,
    e.*
FROM
   etl.covid_next e
ORDER BY e.person_id,e.encounter_datetime ASC
);


SELECT CONCAT('Creating interim table');
drop temporary table if exists etl.covid_interim;
create temporary table etl.covid_interim(
select
	 person_id,
     location_id,
     encounter_id,
     encounter_datetime,
     next_encounter_datetime,
     received_covid_19_vaccine,
     date_given_first_dose_lag as date_given_first_dose,
     first_dose_vaccine_administered_lag as first_dose_vaccine_administered,
     date_given_second_dose_lag as date_given_second_dose,
     second_dose_vaccine_administered_lag as second_dose_vaccine_administered,
     vaccination_status_lag as vaccination_status,
     vaccine_verification_lag as vaccine_verification,
     vaccine_verification_second_dose,
     booster_given_lag as booster_given,
     booster_vaccine_lag as booster_vaccine,
     booster_dose_date_lag as booster_dose_date,
     booster_dose_lag as booster_dose,
     booster_dose_verified_lag as booster_dose_verified,
     sequence,
     covid_19_test_result,
     covid_19_test_date,
     patient_status,
     hospital_admission,
     admission_unit,
     missed_appointment_due_to_covid_19,
     covid_19_positive_since_last_visit,
     covid_19_test_date_since_last_visit,
     patient_status_since_last_visit,
     admission_status_since_last_visit,
     admission_start_date,
     admission_end_date,
     admission_unit_since_last_visit,
     supplemental_oxygen_received,
     patient_ventilated,
     ever_covid_19_positive,
     tracing_final_outcome,
     NULL AS cause_of_death,
     NULL AS DateCreated,
     ever_hopsitalized
     from etl.covid_lag
);







                        
                          

SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    etl.covid_interim;
SELECT @new_encounter_rows;                    
                          set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;

                          SET @dyn_sql=CONCAT('replace into ',@write_table,'(select * from etl.covid_interim)');

                          PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
                          DEALLOCATE PREPARE s1;

                          SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_covid_extract_build_queue__0 t2 using (person_id);'); 
						  PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
						  DEALLOCATE PREPARE s1;
                          
                          
						  SET @dyn_sql=CONCAT('drop table etl.flat_immunization_data;'); 
						  PREPARE s1 from @dyn_sql; 
                          EXECUTE s1; 
						  DEALLOCATE PREPARE s1;
                          
                        

						 SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';'); 
                         PREPARE s1 from @dyn_sql; 
                         EXECUTE s1; 
                         DEALLOCATE PREPARE s1;
                         
                         set @cycle_length = timestampdiff(second,@loop_start_time,now());
                         set @total_time = @total_time + @cycle_length;
                         set @cycle_number = @cycle_number + 1;
                         set @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);
                         
SELECT 
    @person_ids_count AS 'persons remaining',
    @cycle_length AS 'Cycle time (s)',
    CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
    @remaining_time AS 'Est time remaining (min)';


                    end while;

                         SET @dyn_sql=CONCAT('drop table ',@queue_table,';');
                         PREPARE s1 from @dyn_sql;
                         EXECUTE s1;
                         DEALLOCATE PREPARE s1;  

                         SET @total_rows_to_write=0;
                         SET @dyn_sql=CONCAT("Select count(*) into @total_rows_to_write from ",@write_table);
                         PREPARE s1 from @dyn_sql; 
                         EXECUTE s1; 
                         DEALLOCATE PREPARE s1;

                         set @start_write = now();
                         
SELECT 
    CONCAT(@start_write,
            ' : Writing ',
            @total_rows_to_write,
            ' to ',
            @primary_table);

                        SET @dyn_sql=CONCAT('replace into ', @primary_table,'(select * from ',@write_table,');');
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1;
                        DEALLOCATE PREPARE s1;

SELECT 
    CONCAT(@finish_write,
            ' : Completed writing rows. Time to write to primary table: ',
            @time_to_write,
            ' seconds ');
            
                        SET @dyn_sql=CONCAT('drop table ',@write_table,';');
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;
                        
                        
                        set @ave_cycle_length = ceil(@total_time/@cycle_number);
SELECT 
    CONCAT('Average Cycle Length: ',
            @ave_cycle_length,
            'second(s)');
                        set @end = now();
                        
                        if(@log_build=1) then
                             insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
                        end if;
                        
                        
SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');


END$$
DELIMITER ;
