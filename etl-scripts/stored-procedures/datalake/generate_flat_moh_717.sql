CREATE DEFINER=`openmrs_user`@`%` PROCEDURE `etl`.`generate_flat_moh_717_v1`(
    IN query_type   VARCHAR(50),
    IN queue_number INT,
    IN queue_size   INT,
    IN cycle_size   INT
)
BEGIN
    # MOH 717 Report ETL - v1.0
    # Reads from flat_obs and extracts indicator columns for MOH 717
    # (General Outpatient, Special Clinics, MCH/FP, Dental, Other Services, Inpatient, Maternity, Operations, Orthopaedic Trauma/Removal, Special Services, Pharmacy, Mortuary, Medical Records).
    #
    # Indicator logic translated from the etl-rest-server json-report
    # definitions at:
    #   app/reporting-framework/json-reports/moh-717/aggregations/*.json
    #
    # Translation rules (flat_obs packs each encounter's obs as
    # !!concept_id=value!! tokens joined by " ## "):
    #   o.concept_id = C AND o.value_coded = V    -> obs REGEXP "!!C=V!!"
    #   o.concept_id = C AND o.value_coded != V   -> obs REGEXP "!!C=" AND obs NOT REGEXP "!!C=V!!"
    #   o.concept_id = C AND o.value_coded IN(..) -> obs REGEXP "!!C=V1!!|!!C=V2!!|..."
    #   o.concept_id = C                          -> obs REGEXP "!!C="
    #   numeric comparisons use the first packed value of the concept,
    #   guarded by a numeric-format check (dirty values -> not counted)
    #   age bands are computed at encounter_datetime instead of the
    #   upstream CURDATE() so rows stay deterministic once written
    #
    # Each indicator column holds a per-encounter 0/1 flag (NULL when the
    # upstream base reports "1, NULL").  SUM(<col>) grouped by location_id
    # and period downstream reproduces the MOH 717 aggregate report.
    #
    # Columns whose upstream expression is a "null" placeholder are emitted
    # as NULL columns (they appear as null in the live report output too).
    # Upstream quirks are preserved verbatim and flagged with NOTE comments
    # so this table can be reconciled 1:1 against the etl-rest-server report.
    #
    # 261 of the 304 indicator columns are upstream "null" placeholders
    # (the whole Inpatient ward block, Operations, Orthopaedics, Special
    # Services, Pharmacy, Mortuary, Medical Records, and parts of Special
    # Clinics / Maternity / Other Services); they are emitted as NULL.
    SET session sort_buffer_size     = 512000000;
    SET session group_concat_max_len = 100000;

    SET @start              = NOW();
    SET @primary_table      = 'flat_moh_717_report';
    SET @table_version      = 'flat_moh_717_report_v1.0';
    SET @total_rows_written = 0;
    SET @query_type         = query_type;
    SET @queue_number       = queue_number;
    SET @queue_size         = queue_size;
    SET @cycle_size         = cycle_size;
    SET @boundary           = '!!';
    SET @sep                = ' ## ';

    -- ------------------------------------------------------------------
    -- Primary table (all VARCHAR to avoid CAST failures on dirty data).
    -- Indicator columns are 0/1 flags, so VARCHAR(5) is used instead of
    -- the house-style VARCHAR(100): hundreds of VARCHAR(100) columns
    -- would exceed the MySQL row-size limit on utf8mb4 schemas.
    -- ------------------------------------------------------------------
    SET @dyn_sql = CONCAT('CREATE TABLE IF NOT EXISTS ', @primary_table, ' (
        date_created                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        person_id                      INT,
        uuid                           VARCHAR(100),
        encounter_id                   INT,
        encounter_datetime             DATETIME,
        encounter_type                 INT,
        location_id                    INT,
        birth_date                     DATE,
        gender                         VARCHAR(10),


        -- General Outpatient (encounter type 167)
        opd_attendance_greater_5yrs_male_new                  VARCHAR(5),
        opd_attendance_greater_5yrs_male_revisit              VARCHAR(5),
        opd_attendance_greater_5yrs_female_new                VARCHAR(5),
        opd_attendance_greater_5yrs_female_revisit            VARCHAR(5),
        opd_attendance_less_5yrs_male_new                     VARCHAR(5),
        opd_attendance_less_5yrs_male_revisit                 VARCHAR(5),
        opd_attendance_less_5yrs_female_new                   VARCHAR(5),
        opd_attendance_less_5yrs_female_revisit               VARCHAR(5),
        over_60_years_new                                     VARCHAR(5),
        over_60_years_revisit                                 VARCHAR(5),
        casualty_attendance_new                               VARCHAR(5),
        casualty_attendance_revisit                           VARCHAR(5),

        -- Special Clinics (encounter types 1, 2, 38, 39, 168)
        ent_clinic_attendance_new                             VARCHAR(5),
        ent_clinic_attendance_revisit                         VARCHAR(5),
        eye_clinic_attendance_new                             VARCHAR(5),
        eye_clinic_attendance_revisit                         VARCHAR(5),
        tb_and_leprosy_attendance_new                         VARCHAR(5),
        tb_and_leprosy_attendance_revisit                     VARCHAR(5),
        ccc_new                                               VARCHAR(5),
        ccc_revisit                                           VARCHAR(5),
        psychiatry_attendance_new                             VARCHAR(5),
        psychiatry_attendance_revisit                         VARCHAR(5),
        orthopaedic_new                                       VARCHAR(5),
        orthopaedic_revisit                                   VARCHAR(5),
        occupational_therapy_new                              VARCHAR(5),
        occupational_therapy_revisit                          VARCHAR(5),
        physiotherapy_new                                     VARCHAR(5),
        physiotherapy_revisit                                 VARCHAR(5),
        medical_attendance_new                                VARCHAR(5),
        medical_attendance_revisit                            VARCHAR(5),
        surgical_clinics_new                                  VARCHAR(5),
        surgical_clinics_revisit                              VARCHAR(5),
        paediatrics_attendance_new                            VARCHAR(5),
        paediatrics_attendance_revisit                        VARCHAR(5),
        obstetrics_gynaecology_new                            VARCHAR(5),
        obstetrics_gynaecology_revisit                        VARCHAR(5),
        nutrition_new                                         VARCHAR(5),
        nutrition_revisit                                     VARCHAR(5),
        oncology_new                                          VARCHAR(5),
        oncology_revisit                                      VARCHAR(5),
        renal_new                                             VARCHAR(5),
        renal_revisit                                         VARCHAR(5),
        other_special_clinics_new                             VARCHAR(5),
        other_special_clinics_revisit                         VARCHAR(5),

        -- MCH / Family Planning (encounter types 179, 264, 265, 266, 267, 167)
        cwc_attendance_new                                    VARCHAR(5),
        cwc_attendance_revisit                                VARCHAR(5),
        anc_new                                               VARCHAR(5),
        anc_revisit                                           VARCHAR(5),
        pnc_new                                               VARCHAR(5),
        pnc_revisit                                           VARCHAR(5),
        fp_attendance_new                                     VARCHAR(5),
        fp_attendance_revisit                                 VARCHAR(5),

        -- Dental (encounter type 329)
        dental_attendance_ex_fillings_extractions_new         VARCHAR(5),
        dental_attendance_ex_fillings_extractions_revisit     VARCHAR(5),
        dental_fillings_new                                   VARCHAR(5),
        dental_fillings_revisit                               VARCHAR(5),
        dental_extractions_new                                VARCHAR(5),
        dental_extractions_revisit                            VARCHAR(5),

        -- Other Services - TODO upstream: all placeholders in the moh-717 json definitions
        medical_examinations_except_p3                        VARCHAR(5),
        opd_medical_reports                                   VARCHAR(5),
        opd_dressing_done                                     VARCHAR(5),
        opd_removal_of_stitches                               VARCHAR(5),
        opd_injections_given                                  VARCHAR(5),
        opd_stitching                                         VARCHAR(5),

        -- Inpatient wards - TODO upstream: all placeholders in the moh-717 json definitions
        medical_discharges                                    VARCHAR(5),
        surgical_discharges                                   VARCHAR(5),
        obst_gyn_discharges                                   VARCHAR(5),
        paediatrics_discharges                                VARCHAR(5),
        maternity_discharges                                  VARCHAR(5),
        eye_discharges                                        VARCHAR(5),
        nursery_newborn_discharges                            VARCHAR(5),
        orthopaedic_discharges                                VARCHAR(5),
        medical_deaths                                        VARCHAR(5),
        surgical_deaths                                       VARCHAR(5),
        obst_gyn_deaths                                       VARCHAR(5),
        paediatrics_deaths                                    VARCHAR(5),
        maternity_deaths                                      VARCHAR(5),
        eye_deaths                                            VARCHAR(5),
        nursery_newborn_deaths                                VARCHAR(5),
        orthopaedic_deaths                                    VARCHAR(5),
        medical_deaths_malaria                                VARCHAR(5),
        surgical_deaths_malaria                               VARCHAR(5),
        obst_gyn_deaths_malaria                               VARCHAR(5),
        paediatrics_deaths_malaria                            VARCHAR(5),
        maternity_deaths_malaria                              VARCHAR(5),
        eye_deaths_malaria                                    VARCHAR(5),
        nursery_newborn_deaths_malaria                        VARCHAR(5),
        orthopaedic_deaths_malaria                            VARCHAR(5),
        medical_abscondees                                    VARCHAR(5),
        surgical_abscondees                                   VARCHAR(5),
        obst_gyn_abscondees                                   VARCHAR(5),
        paediatrics_abscondees                                VARCHAR(5),
        maternity_abscondees                                  VARCHAR(5),
        eye_abscondees                                        VARCHAR(5),
        nursery_newborn_abscondees                            VARCHAR(5),
        orthopaedic_abscondees                                VARCHAR(5),
        medical_referrals_out_of_facility                     VARCHAR(5),
        surgical_referrals_out_of_facility                    VARCHAR(5),
        obst_gyn_referrals_out_of_facility                    VARCHAR(5),
        paediatrics_referrals_out_of_facility                 VARCHAR(5),
        maternity_referrals_out_of_facility                   VARCHAR(5),
        eye_referrals_out_of_facility                         VARCHAR(5),
        nursery_newborn_referrals_out_of_facility             VARCHAR(5),
        orthopaedic_referrals_out_of_facility                 VARCHAR(5),
        medical_admissions_0_28_days                          VARCHAR(5),
        surgical_admissions_0_28_days                         VARCHAR(5),
        obst_gyn_admissions_0_28_days                         VARCHAR(5),
        paediatrics_admissions_0_28_days                      VARCHAR(5),
        maternity_admissions_0_28_days                        VARCHAR(5),
        eye_admissions_0_28_days                              VARCHAR(5),
        nursery_newborn_admissions_0_28_days                  VARCHAR(5),
        orthopaedic_admissions_0_28_days                      VARCHAR(5),
        medical_admissions_under_five                         VARCHAR(5),
        surgical_admissions_under_five                        VARCHAR(5),
        obst_gyn_admissions_under_five                        VARCHAR(5),
        paediatrics_admissions_under_five                     VARCHAR(5),
        maternity_admissions_under_five                       VARCHAR(5),
        eye_admissions_under_five                             VARCHAR(5),
        nursery_newborn_admissions_under_five                 VARCHAR(5),
        orthopaedic_admissions_under_five                     VARCHAR(5),
        medical_admissions_over_five                          VARCHAR(5),
        surgical_admissions_over_five                         VARCHAR(5),
        obst_gyn_admissions_over_five                         VARCHAR(5),
        paediatrics_admissions_over_five                      VARCHAR(5),
        maternity_admissions_over_five                        VARCHAR(5),
        eye_admissions_over_five                              VARCHAR(5),
        nursery_newborn_admissions_over_five                  VARCHAR(5),
        orthopaedic_admissions_over_five                      VARCHAR(5),
        medical_admissions_under_five_severe_malaria          VARCHAR(5),
        surgical_admissions_under_five_severe_malaria         VARCHAR(5),
        obst_gyn_admissions_under_five_severe_malaria         VARCHAR(5),
        paediatrics_admissions_under_five_severe_malaria      VARCHAR(5),
        maternity_admissions_under_five_severe_malaria        VARCHAR(5),
        eye_admissions_under_five_severe_malaria              VARCHAR(5),
        nursery_newborn_admissions_under_five_severe_malaria  VARCHAR(5),
        orthopaedic_admissions_under_five_severe_malaria      VARCHAR(5),
        medical_admissions_over_five_severe_malaria           VARCHAR(5),
        surgical_admissions_over_five_severe_malaria          VARCHAR(5),
        obst_gyn_admissions_over_five_severe_malaria          VARCHAR(5),
        paediatrics_admissions_over_five_severe_malaria       VARCHAR(5),
        maternity_admissions_over_five_severe_malaria         VARCHAR(5),
        eye_admissions_over_five_severe_malaria               VARCHAR(5),
        nursery_newborn_admissions_over_five_severe_malaria   VARCHAR(5),
        orthopaedic_admissions_over_five_severe_malaria       VARCHAR(5),
        medical_paroles                                       VARCHAR(5),
        surgical_paroles                                      VARCHAR(5),
        obst_gyn_paroles                                      VARCHAR(5),
        paediatrics_paroles                                   VARCHAR(5),
        maternity_paroles                                     VARCHAR(5),
        eye_paroles                                           VARCHAR(5),
        nursery_newborn_paroles                               VARCHAR(5),
        orthopaedic_paroles                                   VARCHAR(5),
        medical_occupied_bed_days_sha_members                 VARCHAR(5),
        surgical_occupied_bed_days_sha_members                VARCHAR(5),
        obst_gyn_occupied_bed_days_sha_members                VARCHAR(5),
        paediatrics_occupied_bed_days_sha_members             VARCHAR(5),
        maternity_occupied_bed_days_sha_members               VARCHAR(5),
        eye_occupied_bed_days_sha_members                     VARCHAR(5),
        nursery_newborn_occupied_bed_days_sha_members         VARCHAR(5),
        orthopaedic_occupied_bed_days_sha_members             VARCHAR(5),
        medical_occupied_bed_days_non_sha_members             VARCHAR(5),
        surgical_occupied_bed_days_non_sha_members            VARCHAR(5),
        obst_gyn_occupied_bed_days_non_sha_members            VARCHAR(5),
        paediatrics_occupied_bed_days_non_sha_members         VARCHAR(5),
        maternity_occupied_bed_days_non_sha_members           VARCHAR(5),
        eye_occupied_bed_days_non_sha_members                 VARCHAR(5),
        nursery_newborn_occupied_bed_days_non_sha_members     VARCHAR(5),
        orthopaedic_occupied_bed_days_non_sha_members         VARCHAR(5),
        medical_well_persons_days                             VARCHAR(5),
        surgical_well_persons_days                            VARCHAR(5),
        obst_gyn_well_persons_days                            VARCHAR(5),
        paediatrics_well_persons_days                         VARCHAR(5),
        maternity_well_persons_days                           VARCHAR(5),
        eye_well_persons_days                                 VARCHAR(5),
        nursery_newborn_well_persons_days                     VARCHAR(5),
        orthopaedic_well_persons_days                         VARCHAR(5),
        medical_authorised_beds                               VARCHAR(5),
        surgical_authorised_beds                              VARCHAR(5),
        obst_gyn_authorised_beds                              VARCHAR(5),
        paediatrics_authorised_beds                           VARCHAR(5),
        maternity_authorised_beds                             VARCHAR(5),
        eye_authorised_beds                                   VARCHAR(5),
        nursery_newborn_authorised_beds                       VARCHAR(5),
        orthopaedic_authorised_beds                           VARCHAR(5),
        medical_actual_physical_beds                          VARCHAR(5),
        surgical_actual_physical_beds                         VARCHAR(5),
        obst_gyn_actual_physical_beds                         VARCHAR(5),
        paediatrics_actual_physical_beds                      VARCHAR(5),
        maternity_actual_physical_beds                        VARCHAR(5),
        eye_actual_physical_beds                              VARCHAR(5),
        nursery_newborn_actual_physical_beds                  VARCHAR(5),
        orthopaedic_actual_physical_beds                      VARCHAR(5),
        medical_authorised_cots                               VARCHAR(5),
        surgical_authorised_cots                              VARCHAR(5),
        obst_gyn_authorised_cots                              VARCHAR(5),
        paediatrics_authorised_cots                           VARCHAR(5),
        maternity_authorised_cots                             VARCHAR(5),
        eye_authorised_cots                                   VARCHAR(5),
        nursery_newborn_authorised_cots                       VARCHAR(5),
        orthopaedic_authorised_cots                           VARCHAR(5),
        medical_actual_physical_cots                          VARCHAR(5),
        surgical_actual_physical_cots                         VARCHAR(5),
        obst_gyn_actual_physical_cots                         VARCHAR(5),
        paediatrics_actual_physical_cots                      VARCHAR(5),
        maternity_actual_physical_cots                        VARCHAR(5),
        eye_actual_physical_cots                              VARCHAR(5),
        nursery_newborn_actual_physical_cots                  VARCHAR(5),
        orthopaedic_actual_physical_cots                      VARCHAR(5),
        medical_authorised_incubator                          VARCHAR(5),
        surgical_authorised_incubator                         VARCHAR(5),
        obst_gyn_authorised_incubator                         VARCHAR(5),
        paediatrics_authorised_incubator                      VARCHAR(5),
        maternity_authorised_incubator                        VARCHAR(5),
        eye_authorised_incubator                              VARCHAR(5),
        nursery_newborn_authorised_incubator                  VARCHAR(5),
        orthopaedic_authorised_incubator                      VARCHAR(5),
        medical_actual_Physical_incubator                     VARCHAR(5),
        surgical_actual_Physical_incubator                    VARCHAR(5),
        obst_gyn_actual_Physical_incubator                    VARCHAR(5),
        paediatrics_actual_Physical_incubator                 VARCHAR(5),
        maternity_actual_Physical_incubator                   VARCHAR(5),
        eye_actual_Physical_incubator                         VARCHAR(5),
        nursery_newborn_actual_Physical_incubator             VARCHAR(5),
        orthopaedic_actual_Physical_incubator                 VARCHAR(5),

        -- Maternity (encounter types 196, 269, 273, 274)
        normal_deliveries                                     VARCHAR(5),
        caesarean_sections                                    VARCHAR(5),
        breach_deliveries                                     VARCHAR(5),
        assisted_vaginal_deliveries                           VARCHAR(5),
        born_before_arrival                                   VARCHAR(5),
        maternal_deaths                                       VARCHAR(5),
        maternal_deaths_audited_within_7_days                 VARCHAR(5),
        live_births                                           VARCHAR(5),
        still_births                                          VARCHAR(5),
        neonatal_deaths_0_28_days                             VARCHAR(5),
        neonatal_deaths_audits                                VARCHAR(5),
        low_birth_weight_less_2500gms                         VARCHAR(5),
        babies_discharged_alive                               VARCHAR(5),

        -- Operations - TODO upstream: all placeholders in the moh-717 json definitions
        minor_surgeries_booked                                VARCHAR(5),
        minor_surgeries_operated                              VARCHAR(5),
        emergencies_booked                                    VARCHAR(5),
        emergencies_operated                                  VARCHAR(5),
        cold_cases_booked                                     VARCHAR(5),
        cold_surgical_cases                                   VARCHAR(5),
        circumcisions_booked                                  VARCHAR(5),
        circumcisions_operated                                VARCHAR(5),
        major_surgeries_booked                                VARCHAR(5),
        major_surgeries_operated                              VARCHAR(5),

        -- Orthopaedic Trauma - TODO upstream: all placeholders in the moh-717 json definitions
        casts_fixed_less_5_years_new                          VARCHAR(5),
        casts_fixed_less_5_years_revisit                      VARCHAR(5),
        casts_fixed_over_5_years_new                          VARCHAR(5),
        casts_fixed_over_5_years_revisit                      VARCHAR(5),
        tractions_fixed_less_5_years_new                      VARCHAR(5),
        tractions_fixed_less_5_years_revisit                  VARCHAR(5),
        tractions_fixed_over_5_years_new                      VARCHAR(5),
        tractions_fixed_over_5_years_revisit                  VARCHAR(5),
        closed_reductions_less_5_years_new                    VARCHAR(5),
        closed_reductions_less_5_years_revisit                VARCHAR(5),
        closed_reductions_over_5_years_new                    VARCHAR(5),
        closed_reductions_over_5_years_revisit                VARCHAR(5),
        orthopaedic_assisted_in_theatre_less_5_years_new      VARCHAR(5),
        orthopaedic_assisted_in_theatre_less_5_years_revisit  VARCHAR(5),
        orthopaedic_assisted_in_theatre_over_5_years_new      VARCHAR(5),
        orthopaedic_assisted_in_theatre_over_5_years_revisit  VARCHAR(5),
        club_foot_seen_less_5_years_new                       VARCHAR(5),
        club_foot_seen_less_5_years_revisit                   VARCHAR(5),
        club_foot_seen_over_5_years_new                       VARCHAR(5),
        club_foot_seen_over_5_years_revisit                   VARCHAR(5),
        crepe_bandages_applied_less_5_years_new               VARCHAR(5),
        crepe_bandages_applied_less_5_years_revisit           VARCHAR(5),
        crepe_bandages_applied_over_5_years_new               VARCHAR(5),
        crepe_bandages_applied_over_5_years_revisit           VARCHAR(5),

        -- Orthopaedic Removal - TODO upstream: all placeholders in the moh-717 json definitions
        removals_done_cast_less_5_years                       VARCHAR(5),
        removals_done_cast_over_5_years                       VARCHAR(5),
        removals_done_tractions_less_5_years                  VARCHAR(5),
        removals_done_tractions_over_5_years                  VARCHAR(5),
        ex_fixator_removed_less_5_years                       VARCHAR(5),
        ex_fixator_removed_over_5_years                       VARCHAR(5),

        -- Special Services - TODO upstream: all placeholders in the moh-717 json definitions
        lab_routine_test                                      VARCHAR(5),
        lab_special_test                                      VARCHAR(5),
        plain_xray_without_enhancement                        VARCHAR(5),
        contrast_enhancement_examination                      VARCHAR(5),
        magnetic_resonance_imaging                            VARCHAR(5),
        computerized_tomography                               VARCHAR(5),
        mammography                                           VARCHAR(5),
        chest_xray_for_ptb                                    VARCHAR(5),
        number_of_ultrasound_examinations                     VARCHAR(5),
        general_ultrasound                                    VARCHAR(5),
        obstetric_ultrasound                                  VARCHAR(5),
        total_routine_xray_and_imaging                        VARCHAR(5),
        total_special_exams_xray_and_imaging                  VARCHAR(5),
        physiotherapy_number_of_treatments                    VARCHAR(5),
        occupational_therapy_number_of_treatments             VARCHAR(5),
        orthopaedic_technology_prepared                       VARCHAR(5),
        orthopaedic_technology_issued                         VARCHAR(5),

        -- Pharmacy - TODO upstream: all placeholders in the moh-717 json definitions
        prescriptions_issued_common_drugs                     VARCHAR(5),
        prescriptions_issued_antibiotics                      VARCHAR(5),
        prescriptions_issued_special_drugs                    VARCHAR(5),
        prescriptions_issued_drugs_children                   VARCHAR(5),

        -- Mortuary - TODO upstream: all placeholders in the moh-717 json definitions
        mortuary_body_days                                    VARCHAR(5),
        mortuary_embalment                                    VARCHAR(5),
        mortuary_postmortem                                   VARCHAR(5),
        mortuary_unclaimed_bodies                             VARCHAR(5),

        -- Medical Records - TODO upstream: all placeholders in the moh-717 json definitions
        new_patient_files_issued                              VARCHAR(5),
        outpatient_records_cards_issued                       VARCHAR(5),

        PRIMARY KEY encounter_id (encounter_id),
        INDEX person_date (person_id, encounter_datetime),
        INDEX person_uuid (uuid),
        INDEX location (location_id)
    )');
    PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

    -- ------------------------------------------------------------------
    -- BUILD mode
    -- Requires a table named `<primary_table>_build_queue` to exist and
    -- contain the person_ids to process.  For this procedure that's
    -- `flat_moh_717_report_build_queue`.
    -- ------------------------------------------------------------------
    IF (@query_type = 'build') THEN
        SELECT CONCAT('BUILDING ', @primary_table, '..........................................');
        SET @write_table = CONCAT(@primary_table, '_temp_', @queue_number);
        SET @build_queue = CONCAT(@primary_table, '_build_queue');
        SET @queue_table = CONCAT(@build_queue, '_', @queue_number);

        SET @dyn_sql = CONCAT('CREATE TABLE IF NOT EXISTS ', @write_table, ' LIKE ', @primary_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT(
            'CREATE TABLE IF NOT EXISTS ', @queue_table,
            ' (person_id INT PRIMARY KEY) AS (SELECT * FROM ', @build_queue,
            ' LIMIT ', @queue_size, ')'
        );
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT('DELETE t1 FROM ', @build_queue, ' t1 JOIN ', @queue_table, ' t2 USING (person_id)');
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT('SELECT COUNT(*) INTO @queue_count FROM ', @queue_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;
    END IF;

    -- ------------------------------------------------------------------
    -- SYNC mode
    -- ------------------------------------------------------------------
    IF (@query_type = 'sync') THEN
        SELECT CONCAT('SYNCING ', @primary_table, '..........................................');
        SET @write_table = @primary_table;
        SET @queue_table = CONCAT(@primary_table, '_sync_queue');
        SET @last_update = NULL;

        SELECT MAX(date_updated) INTO @last_update
        FROM etl.flat_log
        WHERE table_name = @table_version;

        -- If no prior run, treat every row as new
        SET @last_update = IFNULL(@last_update, '1900-01-01');

        CREATE TABLE IF NOT EXISTS flat_moh_717_report_sync_queue (
            person_id INT PRIMARY KEY
        );

        REPLACE INTO flat_moh_717_report_sync_queue
            (SELECT DISTINCT patient_id FROM amrs.encounter WHERE date_changed > @last_update);
        REPLACE INTO flat_moh_717_report_sync_queue
            (SELECT DISTINCT person_id FROM etl.flat_obs WHERE max_date_created > @last_update);
        REPLACE INTO flat_moh_717_report_sync_queue
            (SELECT person_id FROM amrs.person WHERE date_voided > @last_update);
        REPLACE INTO flat_moh_717_report_sync_queue
            (SELECT person_id FROM amrs.person WHERE date_changed > @last_update);
    END IF;

    -- Delete stale rows for queued persons
    SET @dyn_sql = CONCAT('DELETE t1 FROM ', @primary_table, ' t1 JOIN ', @queue_table, ' t2 USING (person_id)');
    PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

    SET @dyn_sql = CONCAT('SELECT COUNT(*) INTO @queue_count FROM ', @queue_table);
    PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

    SET @total_time   = 0;
    SET @cycle_number = 0;

    -- ------------------------------------------------------------------
    -- Main processing loop
    -- Rows are restricted to the union of the MOH 717 section encounter
    -- types: GOPD (167), Special Clinics (1,2,38,39,168), MCH/FP
    -- (179,264,265,266,267,167), Dental and the remaining service
    -- sections (329), Maternity (196,269,273,274).
    -- flat_obs synthetic obs-set rows (encounter_type 99999) are excluded.
    -- ------------------------------------------------------------------
    WHILE @queue_count > 0 DO

        SET @loop_start_time = NOW();

        DROP TEMPORARY TABLE IF EXISTS temp_queue_table;
        SET @dyn_sql = CONCAT('CREATE TEMPORARY TABLE temp_queue_table LIKE ', @queue_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT('REPLACE INTO temp_queue_table (SELECT * FROM ', @queue_table, ' LIMIT ', @cycle_size, ')');
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        DROP TEMPORARY TABLE IF EXISTS flat_moh_717_report_0;

        SET @dyn_sql = CONCAT(
            'CREATE TEMPORARY TABLE flat_moh_717_report_0
                (PRIMARY KEY (encounter_id), INDEX person_date (person_id, encounter_datetime))
            AS (SELECT
                t1.person_id,
                t2.uuid,
                t1.encounter_id,
                t1.encounter_datetime,
                t1.encounter_type,
                t1.location_id,
                t2.birthdate AS birth_date,
                t2.gender,

                -- General Outpatient (encounter type 167)
                IF(t1.encounter_type = 167 AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) > 5 AND t2.gender = \"M\" AND t1.obs REGEXP \"!!1839=7850!!\", 1, 0) AS opd_attendance_greater_5yrs_male_new,
                IF(t1.encounter_type = 167 AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) > 5 AND t2.gender = \"M\" AND t1.obs REGEXP \"!!1839=11233!!\", 1, 0) AS opd_attendance_greater_5yrs_male_revisit,
                IF(t1.encounter_type = 167 AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) > 5 AND t2.gender = \"F\" AND t1.obs REGEXP \"!!1839=7850!!\", 1, 0) AS opd_attendance_greater_5yrs_female_new,
                IF(t1.encounter_type = 167 AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) > 5 AND t2.gender = \"F\" AND t1.obs REGEXP \"!!1839=11233!!\", 1, 0) AS opd_attendance_greater_5yrs_female_revisit,
                IF(t1.encounter_type = 167 AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) <= 5 AND t2.gender = \"M\" AND t1.obs REGEXP \"!!1839=7850!!\", 1, 0) AS opd_attendance_less_5yrs_male_new,
                IF(t1.encounter_type = 167 AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) <= 5 AND t2.gender = \"M\" AND t1.obs REGEXP \"!!1839=11233!!\", 1, 0) AS opd_attendance_less_5yrs_male_revisit,
                IF(t1.encounter_type = 167 AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) <= 5 AND t2.gender = \"F\" AND t1.obs REGEXP \"!!1839=7850!!\", 1, 0) AS opd_attendance_less_5yrs_female_new,
                IF(t1.encounter_type = 167 AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) <= 5 AND t2.gender = \"F\" AND t1.obs REGEXP \"!!1839=11233!!\", 1, 0) AS opd_attendance_less_5yrs_female_revisit,
                IF(t1.encounter_type = 167 AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) > 60 AND t1.obs REGEXP \"!!1839=7850!!\", 1, 0) AS over_60_years_new,
                IF(t1.encounter_type = 167 AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) > 60 AND t1.obs REGEXP \"!!1839=11233!!\", 1, 0) AS over_60_years_revisit,
                IF(t1.encounter_type = 167 AND t1.obs REGEXP \"!!1839=7850!!\", 1, 0) AS casualty_attendance_new,
                IF(t1.encounter_type = 167 AND t1.obs REGEXP \"!!1839=11233!!\", 1, 0) AS casualty_attendance_revisit,

                -- Special Clinics (encounter types 1, 2, 38, 39, 168)
                -- NOTE (upstream quirk preserved): paediatrics_attendance_* test
                -- encounter types 3 and 4, which the upstream base filter excludes
                -- (1,2,38,39,168) - dead columns, always NULL, as in the live report.
                NULL AS ent_clinic_attendance_new,
                NULL AS ent_clinic_attendance_revisit,
                NULL AS eye_clinic_attendance_new,
                NULL AS eye_clinic_attendance_revisit,
                NULL AS tb_and_leprosy_attendance_new,
                NULL AS tb_and_leprosy_attendance_revisit,
                IF(t1.encounter_type IN (1,2,38,39,168) AND t1.encounter_type = 1, 1, NULL) AS ccc_new,
                IF(t1.encounter_type IN (1,2,38,39,168) AND t1.encounter_type = 2, 1, NULL) AS ccc_revisit,
                NULL AS psychiatry_attendance_new,
                NULL AS psychiatry_attendance_revisit,
                NULL AS orthopaedic_new,
                NULL AS orthopaedic_revisit,
                NULL AS occupational_therapy_new,
                NULL AS occupational_therapy_revisit,
                NULL AS physiotherapy_new,
                NULL AS physiotherapy_revisit,
                NULL AS medical_attendance_new,
                NULL AS medical_attendance_revisit,
                NULL AS surgical_clinics_new,
                NULL AS surgical_clinics_revisit,
                IF(t1.encounter_type IN (1,2,38,39,168) AND t1.encounter_type = 3, 1, NULL) AS paediatrics_attendance_new,
                IF(t1.encounter_type IN (1,2,38,39,168) AND t1.encounter_type = 4, 1, NULL) AS paediatrics_attendance_revisit,
                NULL AS obstetrics_gynaecology_new,
                NULL AS obstetrics_gynaecology_revisit,
                IF(t1.encounter_type IN (1,2,38,39,168) AND t1.encounter_type = 168 AND t1.obs REGEXP \"!!1839=7850!!\", 1, NULL) AS nutrition_new,
                IF(t1.encounter_type IN (1,2,38,39,168) AND t1.encounter_type = 168 AND t1.obs REGEXP \"!!1839=11233!!\", 1, NULL) AS nutrition_revisit,
                IF(t1.encounter_type IN (1,2,38,39,168) AND t1.encounter_type = 38, 1, NULL) AS oncology_new,
                IF(t1.encounter_type IN (1,2,38,39,168) AND t1.encounter_type = 39, 1, NULL) AS oncology_revisit,
                NULL AS renal_new,
                NULL AS renal_revisit,
                NULL AS other_special_clinics_new,
                NULL AS other_special_clinics_revisit,

                -- MCH / Family Planning (encounter types 179, 264, 265, 266, 267, 167)
                -- NOTE (upstream quirk preserved): this report maps anc_new to
                -- encounter type 264 and anc_revisit to 265 - the opposite of
                -- the MOH 711 report. Each report keeps its own logic.
                IF(t1.encounter_type IN (179,264,265,266,267,167) AND t1.encounter_type = 167 AND t1.obs REGEXP \"!!1839=7850!!\", 1, 0) AS cwc_attendance_new,
                IF(t1.encounter_type IN (179,264,265,266,267,167) AND t1.encounter_type = 167 AND t1.obs REGEXP \"!!1839=11233!!\", 1, 0) AS cwc_attendance_revisit,
                IF(t1.encounter_type IN (179,264,265,266,267,167) AND t1.encounter_type = 264, 1, 0) AS anc_new,
                IF(t1.encounter_type IN (179,264,265,266,267,167) AND t1.encounter_type = 265, 1, 0) AS anc_revisit,
                IF(t1.encounter_type IN (179,264,265,266,267,167) AND t1.encounter_type = 266, 1, 0) AS pnc_new,
                IF(t1.encounter_type IN (179,264,265,266,267,167) AND t1.encounter_type = 267, 1, 0) AS pnc_revisit,
                IF(t1.encounter_type IN (179,264,265,266,267,167) AND t1.obs REGEXP \"!!1724=10647!!\", 1, 0) AS fp_attendance_new,
                IF(t1.encounter_type IN (179,264,265,266,267,167) AND t1.obs REGEXP \"!!1724=2345!!\", 1, 0) AS fp_attendance_revisit,

                -- Dental (encounter type 329)
                IF(t1.encounter_type = 329 AND t1.obs REGEXP \"!!1839=7850!!\", 1, NULL) AS dental_attendance_ex_fillings_extractions_new,
                IF(t1.encounter_type = 329 AND t1.obs REGEXP \"!!1839=11233!!\", 1, NULL) AS dental_attendance_ex_fillings_extractions_revisit,
                IF(t1.encounter_type = 329 AND t1.obs REGEXP \"!!1840=7850!!\", 1, NULL) AS dental_fillings_new,
                IF(t1.encounter_type = 329 AND t1.obs REGEXP \"!!1840=11233!!\", 1, NULL) AS dental_fillings_revisit,
                IF(t1.encounter_type = 329 AND t1.obs REGEXP \"!!1841=7850!!\", 1, NULL) AS dental_extractions_new,
                IF(t1.encounter_type = 329 AND t1.obs REGEXP \"!!1841=11233!!\", 1, NULL) AS dental_extractions_revisit,

                -- Other Services - TODO upstream: all placeholders in the moh-717 json definitions
                NULL AS medical_examinations_except_p3,
                NULL AS opd_medical_reports,
                NULL AS opd_dressing_done,
                NULL AS opd_removal_of_stitches,
                NULL AS opd_injections_given,
                NULL AS opd_stitching,

                -- Inpatient wards - TODO upstream: all placeholders in the moh-717 json definitions
                NULL AS medical_discharges,
                NULL AS surgical_discharges,
                NULL AS obst_gyn_discharges,
                NULL AS paediatrics_discharges,
                NULL AS maternity_discharges,
                NULL AS eye_discharges,
                NULL AS nursery_newborn_discharges,
                NULL AS orthopaedic_discharges,
                NULL AS medical_deaths,
                NULL AS surgical_deaths,
                NULL AS obst_gyn_deaths,
                NULL AS paediatrics_deaths,
                NULL AS maternity_deaths,
                NULL AS eye_deaths,
                NULL AS nursery_newborn_deaths,
                NULL AS orthopaedic_deaths,
                NULL AS medical_deaths_malaria,
                NULL AS surgical_deaths_malaria,
                NULL AS obst_gyn_deaths_malaria,
                NULL AS paediatrics_deaths_malaria,
                NULL AS maternity_deaths_malaria,
                NULL AS eye_deaths_malaria,
                NULL AS nursery_newborn_deaths_malaria,
                NULL AS orthopaedic_deaths_malaria,
                NULL AS medical_abscondees,
                NULL AS surgical_abscondees,
                NULL AS obst_gyn_abscondees,
                NULL AS paediatrics_abscondees,
                NULL AS maternity_abscondees,
                NULL AS eye_abscondees,
                NULL AS nursery_newborn_abscondees,
                NULL AS orthopaedic_abscondees,
                NULL AS medical_referrals_out_of_facility,
                NULL AS surgical_referrals_out_of_facility,
                NULL AS obst_gyn_referrals_out_of_facility,
                NULL AS paediatrics_referrals_out_of_facility,
                NULL AS maternity_referrals_out_of_facility,
                NULL AS eye_referrals_out_of_facility,
                NULL AS nursery_newborn_referrals_out_of_facility,
                NULL AS orthopaedic_referrals_out_of_facility,
                NULL AS medical_admissions_0_28_days,
                NULL AS surgical_admissions_0_28_days,
                NULL AS obst_gyn_admissions_0_28_days,
                NULL AS paediatrics_admissions_0_28_days,
                NULL AS maternity_admissions_0_28_days,
                NULL AS eye_admissions_0_28_days,
                NULL AS nursery_newborn_admissions_0_28_days,
                NULL AS orthopaedic_admissions_0_28_days,
                NULL AS medical_admissions_under_five,
                NULL AS surgical_admissions_under_five,
                NULL AS obst_gyn_admissions_under_five,
                NULL AS paediatrics_admissions_under_five,
                NULL AS maternity_admissions_under_five,
                NULL AS eye_admissions_under_five,
                NULL AS nursery_newborn_admissions_under_five,
                NULL AS orthopaedic_admissions_under_five,
                NULL AS medical_admissions_over_five,
                NULL AS surgical_admissions_over_five,
                NULL AS obst_gyn_admissions_over_five,
                NULL AS paediatrics_admissions_over_five,
                NULL AS maternity_admissions_over_five,
                NULL AS eye_admissions_over_five,
                NULL AS nursery_newborn_admissions_over_five,
                NULL AS orthopaedic_admissions_over_five,
                NULL AS medical_admissions_under_five_severe_malaria,
                NULL AS surgical_admissions_under_five_severe_malaria,
                NULL AS obst_gyn_admissions_under_five_severe_malaria,
                NULL AS paediatrics_admissions_under_five_severe_malaria,
                NULL AS maternity_admissions_under_five_severe_malaria,
                NULL AS eye_admissions_under_five_severe_malaria,
                NULL AS nursery_newborn_admissions_under_five_severe_malaria,
                NULL AS orthopaedic_admissions_under_five_severe_malaria,
                NULL AS medical_admissions_over_five_severe_malaria,
                NULL AS surgical_admissions_over_five_severe_malaria,
                NULL AS obst_gyn_admissions_over_five_severe_malaria,
                NULL AS paediatrics_admissions_over_five_severe_malaria,
                NULL AS maternity_admissions_over_five_severe_malaria,
                NULL AS eye_admissions_over_five_severe_malaria,
                NULL AS nursery_newborn_admissions_over_five_severe_malaria,
                NULL AS orthopaedic_admissions_over_five_severe_malaria,
                NULL AS medical_paroles,
                NULL AS surgical_paroles,
                NULL AS obst_gyn_paroles,
                NULL AS paediatrics_paroles,
                NULL AS maternity_paroles,
                NULL AS eye_paroles,
                NULL AS nursery_newborn_paroles,
                NULL AS orthopaedic_paroles,
                NULL AS medical_occupied_bed_days_sha_members,
                NULL AS surgical_occupied_bed_days_sha_members,
                NULL AS obst_gyn_occupied_bed_days_sha_members,
                NULL AS paediatrics_occupied_bed_days_sha_members,
                NULL AS maternity_occupied_bed_days_sha_members,
                NULL AS eye_occupied_bed_days_sha_members,
                NULL AS nursery_newborn_occupied_bed_days_sha_members,
                NULL AS orthopaedic_occupied_bed_days_sha_members,
                NULL AS medical_occupied_bed_days_non_sha_members,
                NULL AS surgical_occupied_bed_days_non_sha_members,
                NULL AS obst_gyn_occupied_bed_days_non_sha_members,
                NULL AS paediatrics_occupied_bed_days_non_sha_members,
                NULL AS maternity_occupied_bed_days_non_sha_members,
                NULL AS eye_occupied_bed_days_non_sha_members,
                NULL AS nursery_newborn_occupied_bed_days_non_sha_members,
                NULL AS orthopaedic_occupied_bed_days_non_sha_members,
                NULL AS medical_well_persons_days,
                NULL AS surgical_well_persons_days,
                NULL AS obst_gyn_well_persons_days,
                NULL AS paediatrics_well_persons_days,
                NULL AS maternity_well_persons_days,
                NULL AS eye_well_persons_days,
                NULL AS nursery_newborn_well_persons_days,
                NULL AS orthopaedic_well_persons_days,
                NULL AS medical_authorised_beds,
                NULL AS surgical_authorised_beds,
                NULL AS obst_gyn_authorised_beds,
                NULL AS paediatrics_authorised_beds,
                NULL AS maternity_authorised_beds,
                NULL AS eye_authorised_beds,
                NULL AS nursery_newborn_authorised_beds,
                NULL AS orthopaedic_authorised_beds,
                NULL AS medical_actual_physical_beds,
                NULL AS surgical_actual_physical_beds,
                NULL AS obst_gyn_actual_physical_beds,
                NULL AS paediatrics_actual_physical_beds,
                NULL AS maternity_actual_physical_beds,
                NULL AS eye_actual_physical_beds,
                NULL AS nursery_newborn_actual_physical_beds,
                NULL AS orthopaedic_actual_physical_beds,
                NULL AS medical_authorised_cots,
                NULL AS surgical_authorised_cots,
                NULL AS obst_gyn_authorised_cots,
                NULL AS paediatrics_authorised_cots,
                NULL AS maternity_authorised_cots,
                NULL AS eye_authorised_cots,
                NULL AS nursery_newborn_authorised_cots,
                NULL AS orthopaedic_authorised_cots,
                NULL AS medical_actual_physical_cots,
                NULL AS surgical_actual_physical_cots,
                NULL AS obst_gyn_actual_physical_cots,
                NULL AS paediatrics_actual_physical_cots,
                NULL AS maternity_actual_physical_cots,
                NULL AS eye_actual_physical_cots,
                NULL AS nursery_newborn_actual_physical_cots,
                NULL AS orthopaedic_actual_physical_cots,
                NULL AS medical_authorised_incubator,
                NULL AS surgical_authorised_incubator,
                NULL AS obst_gyn_authorised_incubator,
                NULL AS paediatrics_authorised_incubator,
                NULL AS maternity_authorised_incubator,
                NULL AS eye_authorised_incubator,
                NULL AS nursery_newborn_authorised_incubator,
                NULL AS orthopaedic_authorised_incubator,
                NULL AS medical_actual_Physical_incubator,
                NULL AS surgical_actual_Physical_incubator,
                NULL AS obst_gyn_actual_Physical_incubator,
                NULL AS paediatrics_actual_Physical_incubator,
                NULL AS maternity_actual_Physical_incubator,
                NULL AS eye_actual_Physical_incubator,
                NULL AS nursery_newborn_actual_Physical_incubator,
                NULL AS orthopaedic_actual_Physical_incubator,

                -- Maternity (encounter types 196, 269, 273, 274)
                -- Output names follow the aggregate (which renames the base:
                -- caesarian_sections -> caesarean_sections, breach_delivery ->
                -- breach_deliveries, assisted_vaginal_delivery -> plural,
                -- live_birth -> live_births, fresh_still_birth -> still_births).
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!5630=1170!!\", 1, NULL) AS normal_deliveries,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!5630=1171!!\", 1, NULL) AS caesarean_sections,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!5630=1172!!\", 1, NULL) AS breach_deliveries,
                IF(t1.encounter_type IN (196,269,273,274) AND (t1.obs REGEXP \"!!5630=2167!!\" OR t1.obs REGEXP \"!!5630=2166!!\"), 1, NULL) AS assisted_vaginal_deliveries,
                NULL AS born_before_arrival,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!10423=159!!\", 1, NULL) AS maternal_deaths,
                NULL AS maternal_deaths_audited_within_7_days,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!6224=1843!!\", 1, NULL) AS live_births,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!6224=10877!!\", 1, NULL) AS still_births,
                NULL AS neonatal_deaths_0_28_days,
                NULL AS neonatal_deaths_audits,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!6433=\" AND SUBSTRING_INDEX(getValues(t1.obs, 6433), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 6433), \" ## \", 1) AS DECIMAL(10,4)) < 2500, 1, NULL) AS low_birth_weight_less_2500gms,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!10938=8701!!\", 1, NULL) AS babies_discharged_alive,

                -- Operations - TODO upstream: all placeholders in the moh-717 json definitions
                NULL AS minor_surgeries_booked,
                NULL AS minor_surgeries_operated,
                NULL AS emergencies_booked,
                NULL AS emergencies_operated,
                NULL AS cold_cases_booked,
                NULL AS cold_surgical_cases,
                NULL AS circumcisions_booked,
                NULL AS circumcisions_operated,
                NULL AS major_surgeries_booked,
                NULL AS major_surgeries_operated,

                -- Orthopaedic Trauma - TODO upstream: all placeholders in the moh-717 json definitions
                NULL AS casts_fixed_less_5_years_new,
                NULL AS casts_fixed_less_5_years_revisit,
                NULL AS casts_fixed_over_5_years_new,
                NULL AS casts_fixed_over_5_years_revisit,
                NULL AS tractions_fixed_less_5_years_new,
                NULL AS tractions_fixed_less_5_years_revisit,
                NULL AS tractions_fixed_over_5_years_new,
                NULL AS tractions_fixed_over_5_years_revisit,
                NULL AS closed_reductions_less_5_years_new,
                NULL AS closed_reductions_less_5_years_revisit,
                NULL AS closed_reductions_over_5_years_new,
                NULL AS closed_reductions_over_5_years_revisit,
                NULL AS orthopaedic_assisted_in_theatre_less_5_years_new,
                NULL AS orthopaedic_assisted_in_theatre_less_5_years_revisit,
                NULL AS orthopaedic_assisted_in_theatre_over_5_years_new,
                NULL AS orthopaedic_assisted_in_theatre_over_5_years_revisit,
                NULL AS club_foot_seen_less_5_years_new,
                NULL AS club_foot_seen_less_5_years_revisit,
                NULL AS club_foot_seen_over_5_years_new,
                NULL AS club_foot_seen_over_5_years_revisit,
                NULL AS crepe_bandages_applied_less_5_years_new,
                NULL AS crepe_bandages_applied_less_5_years_revisit,
                NULL AS crepe_bandages_applied_over_5_years_new,
                NULL AS crepe_bandages_applied_over_5_years_revisit,

                -- Orthopaedic Removal - TODO upstream: all placeholders in the moh-717 json definitions
                NULL AS removals_done_cast_less_5_years,
                NULL AS removals_done_cast_over_5_years,
                NULL AS removals_done_tractions_less_5_years,
                NULL AS removals_done_tractions_over_5_years,
                NULL AS ex_fixator_removed_less_5_years,
                NULL AS ex_fixator_removed_over_5_years,

                -- Special Services - TODO upstream: all placeholders in the moh-717 json definitions
                NULL AS lab_routine_test,
                NULL AS lab_special_test,
                NULL AS plain_xray_without_enhancement,
                NULL AS contrast_enhancement_examination,
                NULL AS magnetic_resonance_imaging,
                NULL AS computerized_tomography,
                NULL AS mammography,
                NULL AS chest_xray_for_ptb,
                NULL AS number_of_ultrasound_examinations,
                NULL AS general_ultrasound,
                NULL AS obstetric_ultrasound,
                NULL AS total_routine_xray_and_imaging,
                NULL AS total_special_exams_xray_and_imaging,
                NULL AS physiotherapy_number_of_treatments,
                NULL AS occupational_therapy_number_of_treatments,
                NULL AS orthopaedic_technology_prepared,
                NULL AS orthopaedic_technology_issued,

                -- Pharmacy - TODO upstream: all placeholders in the moh-717 json definitions
                NULL AS prescriptions_issued_common_drugs,
                NULL AS prescriptions_issued_antibiotics,
                NULL AS prescriptions_issued_special_drugs,
                NULL AS prescriptions_issued_drugs_children,

                -- Mortuary - TODO upstream: all placeholders in the moh-717 json definitions
                NULL AS mortuary_body_days,
                NULL AS mortuary_embalment,
                NULL AS mortuary_postmortem,
                NULL AS mortuary_unclaimed_bodies,

                -- Medical Records - TODO upstream: all placeholders in the moh-717 json definitions
                NULL AS new_patient_files_issued,
                NULL AS outpatient_records_cards_issued

            FROM flat_obs t1
            JOIN temp_queue_table t3 USING (person_id)
            JOIN amrs.person t2 USING (person_id)
            WHERE t1.encounter_type IN (1,2,38,39,167,168,179,196,264,265,266,267,269,273,274,329)
            )'
        );
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT(
            'INSERT INTO ', @write_table,
            ' (person_id, uuid, encounter_id, encounter_datetime, encounter_type, location_id, birth_date, gender,
               opd_attendance_greater_5yrs_male_new, opd_attendance_greater_5yrs_male_revisit, opd_attendance_greater_5yrs_female_new, opd_attendance_greater_5yrs_female_revisit, opd_attendance_less_5yrs_male_new,
               opd_attendance_less_5yrs_male_revisit, opd_attendance_less_5yrs_female_new, opd_attendance_less_5yrs_female_revisit, over_60_years_new, over_60_years_revisit,
               casualty_attendance_new, casualty_attendance_revisit, ent_clinic_attendance_new, ent_clinic_attendance_revisit, eye_clinic_attendance_new,
               eye_clinic_attendance_revisit, tb_and_leprosy_attendance_new, tb_and_leprosy_attendance_revisit, ccc_new, ccc_revisit,
               psychiatry_attendance_new, psychiatry_attendance_revisit, orthopaedic_new, orthopaedic_revisit, occupational_therapy_new,
               occupational_therapy_revisit, physiotherapy_new, physiotherapy_revisit, medical_attendance_new, medical_attendance_revisit,
               surgical_clinics_new, surgical_clinics_revisit, paediatrics_attendance_new, paediatrics_attendance_revisit, obstetrics_gynaecology_new,
               obstetrics_gynaecology_revisit, nutrition_new, nutrition_revisit, oncology_new, oncology_revisit,
               renal_new, renal_revisit, other_special_clinics_new, other_special_clinics_revisit, cwc_attendance_new,
               cwc_attendance_revisit, anc_new, anc_revisit, pnc_new, pnc_revisit,
               fp_attendance_new, fp_attendance_revisit, dental_attendance_ex_fillings_extractions_new, dental_attendance_ex_fillings_extractions_revisit, dental_fillings_new,
               dental_fillings_revisit, dental_extractions_new, dental_extractions_revisit, medical_examinations_except_p3, opd_medical_reports,
               opd_dressing_done, opd_removal_of_stitches, opd_injections_given, opd_stitching, medical_discharges,
               surgical_discharges, obst_gyn_discharges, paediatrics_discharges, maternity_discharges, eye_discharges,
               nursery_newborn_discharges, orthopaedic_discharges, medical_deaths, surgical_deaths, obst_gyn_deaths,
               paediatrics_deaths, maternity_deaths, eye_deaths, nursery_newborn_deaths, orthopaedic_deaths,
               medical_deaths_malaria, surgical_deaths_malaria, obst_gyn_deaths_malaria, paediatrics_deaths_malaria, maternity_deaths_malaria,
               eye_deaths_malaria, nursery_newborn_deaths_malaria, orthopaedic_deaths_malaria, medical_abscondees, surgical_abscondees,
               obst_gyn_abscondees, paediatrics_abscondees, maternity_abscondees, eye_abscondees, nursery_newborn_abscondees,
               orthopaedic_abscondees, medical_referrals_out_of_facility, surgical_referrals_out_of_facility, obst_gyn_referrals_out_of_facility, paediatrics_referrals_out_of_facility,
               maternity_referrals_out_of_facility, eye_referrals_out_of_facility, nursery_newborn_referrals_out_of_facility, orthopaedic_referrals_out_of_facility, medical_admissions_0_28_days,
               surgical_admissions_0_28_days, obst_gyn_admissions_0_28_days, paediatrics_admissions_0_28_days, maternity_admissions_0_28_days, eye_admissions_0_28_days,
               nursery_newborn_admissions_0_28_days, orthopaedic_admissions_0_28_days, medical_admissions_under_five, surgical_admissions_under_five, obst_gyn_admissions_under_five,
               paediatrics_admissions_under_five, maternity_admissions_under_five, eye_admissions_under_five, nursery_newborn_admissions_under_five, orthopaedic_admissions_under_five,
               medical_admissions_over_five, surgical_admissions_over_five, obst_gyn_admissions_over_five, paediatrics_admissions_over_five, maternity_admissions_over_five,
               eye_admissions_over_five, nursery_newborn_admissions_over_five, orthopaedic_admissions_over_five, medical_admissions_under_five_severe_malaria, surgical_admissions_under_five_severe_malaria,
               obst_gyn_admissions_under_five_severe_malaria, paediatrics_admissions_under_five_severe_malaria, maternity_admissions_under_five_severe_malaria, eye_admissions_under_five_severe_malaria, nursery_newborn_admissions_under_five_severe_malaria,
               orthopaedic_admissions_under_five_severe_malaria, medical_admissions_over_five_severe_malaria, surgical_admissions_over_five_severe_malaria, obst_gyn_admissions_over_five_severe_malaria, paediatrics_admissions_over_five_severe_malaria,
               maternity_admissions_over_five_severe_malaria, eye_admissions_over_five_severe_malaria, nursery_newborn_admissions_over_five_severe_malaria, orthopaedic_admissions_over_five_severe_malaria, medical_paroles,
               surgical_paroles, obst_gyn_paroles, paediatrics_paroles, maternity_paroles, eye_paroles,
               nursery_newborn_paroles, orthopaedic_paroles, medical_occupied_bed_days_sha_members, surgical_occupied_bed_days_sha_members, obst_gyn_occupied_bed_days_sha_members,
               paediatrics_occupied_bed_days_sha_members, maternity_occupied_bed_days_sha_members, eye_occupied_bed_days_sha_members, nursery_newborn_occupied_bed_days_sha_members, orthopaedic_occupied_bed_days_sha_members,
               medical_occupied_bed_days_non_sha_members, surgical_occupied_bed_days_non_sha_members, obst_gyn_occupied_bed_days_non_sha_members, paediatrics_occupied_bed_days_non_sha_members, maternity_occupied_bed_days_non_sha_members,
               eye_occupied_bed_days_non_sha_members, nursery_newborn_occupied_bed_days_non_sha_members, orthopaedic_occupied_bed_days_non_sha_members, medical_well_persons_days, surgical_well_persons_days,
               obst_gyn_well_persons_days, paediatrics_well_persons_days, maternity_well_persons_days, eye_well_persons_days, nursery_newborn_well_persons_days,
               orthopaedic_well_persons_days, medical_authorised_beds, surgical_authorised_beds, obst_gyn_authorised_beds, paediatrics_authorised_beds,
               maternity_authorised_beds, eye_authorised_beds, nursery_newborn_authorised_beds, orthopaedic_authorised_beds, medical_actual_physical_beds,
               surgical_actual_physical_beds, obst_gyn_actual_physical_beds, paediatrics_actual_physical_beds, maternity_actual_physical_beds, eye_actual_physical_beds,
               nursery_newborn_actual_physical_beds, orthopaedic_actual_physical_beds, medical_authorised_cots, surgical_authorised_cots, obst_gyn_authorised_cots,
               paediatrics_authorised_cots, maternity_authorised_cots, eye_authorised_cots, nursery_newborn_authorised_cots, orthopaedic_authorised_cots,
               medical_actual_physical_cots, surgical_actual_physical_cots, obst_gyn_actual_physical_cots, paediatrics_actual_physical_cots, maternity_actual_physical_cots,
               eye_actual_physical_cots, nursery_newborn_actual_physical_cots, orthopaedic_actual_physical_cots, medical_authorised_incubator, surgical_authorised_incubator,
               obst_gyn_authorised_incubator, paediatrics_authorised_incubator, maternity_authorised_incubator, eye_authorised_incubator, nursery_newborn_authorised_incubator,
               orthopaedic_authorised_incubator, medical_actual_Physical_incubator, surgical_actual_Physical_incubator, obst_gyn_actual_Physical_incubator, paediatrics_actual_Physical_incubator,
               maternity_actual_Physical_incubator, eye_actual_Physical_incubator, nursery_newborn_actual_Physical_incubator, orthopaedic_actual_Physical_incubator, normal_deliveries,
               caesarean_sections, breach_deliveries, assisted_vaginal_deliveries, born_before_arrival, maternal_deaths,
               maternal_deaths_audited_within_7_days, live_births, still_births, neonatal_deaths_0_28_days, neonatal_deaths_audits,
               low_birth_weight_less_2500gms, babies_discharged_alive, minor_surgeries_booked, minor_surgeries_operated, emergencies_booked,
               emergencies_operated, cold_cases_booked, cold_surgical_cases, circumcisions_booked, circumcisions_operated,
               major_surgeries_booked, major_surgeries_operated, casts_fixed_less_5_years_new, casts_fixed_less_5_years_revisit, casts_fixed_over_5_years_new,
               casts_fixed_over_5_years_revisit, tractions_fixed_less_5_years_new, tractions_fixed_less_5_years_revisit, tractions_fixed_over_5_years_new, tractions_fixed_over_5_years_revisit,
               closed_reductions_less_5_years_new, closed_reductions_less_5_years_revisit, closed_reductions_over_5_years_new, closed_reductions_over_5_years_revisit, orthopaedic_assisted_in_theatre_less_5_years_new,
               orthopaedic_assisted_in_theatre_less_5_years_revisit, orthopaedic_assisted_in_theatre_over_5_years_new, orthopaedic_assisted_in_theatre_over_5_years_revisit, club_foot_seen_less_5_years_new, club_foot_seen_less_5_years_revisit,
               club_foot_seen_over_5_years_new, club_foot_seen_over_5_years_revisit, crepe_bandages_applied_less_5_years_new, crepe_bandages_applied_less_5_years_revisit, crepe_bandages_applied_over_5_years_new,
               crepe_bandages_applied_over_5_years_revisit, removals_done_cast_less_5_years, removals_done_cast_over_5_years, removals_done_tractions_less_5_years, removals_done_tractions_over_5_years,
               ex_fixator_removed_less_5_years, ex_fixator_removed_over_5_years, lab_routine_test, lab_special_test, plain_xray_without_enhancement,
               contrast_enhancement_examination, magnetic_resonance_imaging, computerized_tomography, mammography, chest_xray_for_ptb,
               number_of_ultrasound_examinations, general_ultrasound, obstetric_ultrasound, total_routine_xray_and_imaging, total_special_exams_xray_and_imaging,
               physiotherapy_number_of_treatments, occupational_therapy_number_of_treatments, orthopaedic_technology_prepared, orthopaedic_technology_issued, prescriptions_issued_common_drugs,
               prescriptions_issued_antibiotics, prescriptions_issued_special_drugs, prescriptions_issued_drugs_children, mortuary_body_days, mortuary_embalment,
               mortuary_postmortem, mortuary_unclaimed_bodies, new_patient_files_issued, outpatient_records_cards_issued)
            SELECT
                person_id, uuid, encounter_id, encounter_datetime, encounter_type, location_id, birth_date, gender,
               opd_attendance_greater_5yrs_male_new, opd_attendance_greater_5yrs_male_revisit, opd_attendance_greater_5yrs_female_new, opd_attendance_greater_5yrs_female_revisit, opd_attendance_less_5yrs_male_new,
               opd_attendance_less_5yrs_male_revisit, opd_attendance_less_5yrs_female_new, opd_attendance_less_5yrs_female_revisit, over_60_years_new, over_60_years_revisit,
               casualty_attendance_new, casualty_attendance_revisit, ent_clinic_attendance_new, ent_clinic_attendance_revisit, eye_clinic_attendance_new,
               eye_clinic_attendance_revisit, tb_and_leprosy_attendance_new, tb_and_leprosy_attendance_revisit, ccc_new, ccc_revisit,
               psychiatry_attendance_new, psychiatry_attendance_revisit, orthopaedic_new, orthopaedic_revisit, occupational_therapy_new,
               occupational_therapy_revisit, physiotherapy_new, physiotherapy_revisit, medical_attendance_new, medical_attendance_revisit,
               surgical_clinics_new, surgical_clinics_revisit, paediatrics_attendance_new, paediatrics_attendance_revisit, obstetrics_gynaecology_new,
               obstetrics_gynaecology_revisit, nutrition_new, nutrition_revisit, oncology_new, oncology_revisit,
               renal_new, renal_revisit, other_special_clinics_new, other_special_clinics_revisit, cwc_attendance_new,
               cwc_attendance_revisit, anc_new, anc_revisit, pnc_new, pnc_revisit,
               fp_attendance_new, fp_attendance_revisit, dental_attendance_ex_fillings_extractions_new, dental_attendance_ex_fillings_extractions_revisit, dental_fillings_new,
               dental_fillings_revisit, dental_extractions_new, dental_extractions_revisit, medical_examinations_except_p3, opd_medical_reports,
               opd_dressing_done, opd_removal_of_stitches, opd_injections_given, opd_stitching, medical_discharges,
               surgical_discharges, obst_gyn_discharges, paediatrics_discharges, maternity_discharges, eye_discharges,
               nursery_newborn_discharges, orthopaedic_discharges, medical_deaths, surgical_deaths, obst_gyn_deaths,
               paediatrics_deaths, maternity_deaths, eye_deaths, nursery_newborn_deaths, orthopaedic_deaths,
               medical_deaths_malaria, surgical_deaths_malaria, obst_gyn_deaths_malaria, paediatrics_deaths_malaria, maternity_deaths_malaria,
               eye_deaths_malaria, nursery_newborn_deaths_malaria, orthopaedic_deaths_malaria, medical_abscondees, surgical_abscondees,
               obst_gyn_abscondees, paediatrics_abscondees, maternity_abscondees, eye_abscondees, nursery_newborn_abscondees,
               orthopaedic_abscondees, medical_referrals_out_of_facility, surgical_referrals_out_of_facility, obst_gyn_referrals_out_of_facility, paediatrics_referrals_out_of_facility,
               maternity_referrals_out_of_facility, eye_referrals_out_of_facility, nursery_newborn_referrals_out_of_facility, orthopaedic_referrals_out_of_facility, medical_admissions_0_28_days,
               surgical_admissions_0_28_days, obst_gyn_admissions_0_28_days, paediatrics_admissions_0_28_days, maternity_admissions_0_28_days, eye_admissions_0_28_days,
               nursery_newborn_admissions_0_28_days, orthopaedic_admissions_0_28_days, medical_admissions_under_five, surgical_admissions_under_five, obst_gyn_admissions_under_five,
               paediatrics_admissions_under_five, maternity_admissions_under_five, eye_admissions_under_five, nursery_newborn_admissions_under_five, orthopaedic_admissions_under_five,
               medical_admissions_over_five, surgical_admissions_over_five, obst_gyn_admissions_over_five, paediatrics_admissions_over_five, maternity_admissions_over_five,
               eye_admissions_over_five, nursery_newborn_admissions_over_five, orthopaedic_admissions_over_five, medical_admissions_under_five_severe_malaria, surgical_admissions_under_five_severe_malaria,
               obst_gyn_admissions_under_five_severe_malaria, paediatrics_admissions_under_five_severe_malaria, maternity_admissions_under_five_severe_malaria, eye_admissions_under_five_severe_malaria, nursery_newborn_admissions_under_five_severe_malaria,
               orthopaedic_admissions_under_five_severe_malaria, medical_admissions_over_five_severe_malaria, surgical_admissions_over_five_severe_malaria, obst_gyn_admissions_over_five_severe_malaria, paediatrics_admissions_over_five_severe_malaria,
               maternity_admissions_over_five_severe_malaria, eye_admissions_over_five_severe_malaria, nursery_newborn_admissions_over_five_severe_malaria, orthopaedic_admissions_over_five_severe_malaria, medical_paroles,
               surgical_paroles, obst_gyn_paroles, paediatrics_paroles, maternity_paroles, eye_paroles,
               nursery_newborn_paroles, orthopaedic_paroles, medical_occupied_bed_days_sha_members, surgical_occupied_bed_days_sha_members, obst_gyn_occupied_bed_days_sha_members,
               paediatrics_occupied_bed_days_sha_members, maternity_occupied_bed_days_sha_members, eye_occupied_bed_days_sha_members, nursery_newborn_occupied_bed_days_sha_members, orthopaedic_occupied_bed_days_sha_members,
               medical_occupied_bed_days_non_sha_members, surgical_occupied_bed_days_non_sha_members, obst_gyn_occupied_bed_days_non_sha_members, paediatrics_occupied_bed_days_non_sha_members, maternity_occupied_bed_days_non_sha_members,
               eye_occupied_bed_days_non_sha_members, nursery_newborn_occupied_bed_days_non_sha_members, orthopaedic_occupied_bed_days_non_sha_members, medical_well_persons_days, surgical_well_persons_days,
               obst_gyn_well_persons_days, paediatrics_well_persons_days, maternity_well_persons_days, eye_well_persons_days, nursery_newborn_well_persons_days,
               orthopaedic_well_persons_days, medical_authorised_beds, surgical_authorised_beds, obst_gyn_authorised_beds, paediatrics_authorised_beds,
               maternity_authorised_beds, eye_authorised_beds, nursery_newborn_authorised_beds, orthopaedic_authorised_beds, medical_actual_physical_beds,
               surgical_actual_physical_beds, obst_gyn_actual_physical_beds, paediatrics_actual_physical_beds, maternity_actual_physical_beds, eye_actual_physical_beds,
               nursery_newborn_actual_physical_beds, orthopaedic_actual_physical_beds, medical_authorised_cots, surgical_authorised_cots, obst_gyn_authorised_cots,
               paediatrics_authorised_cots, maternity_authorised_cots, eye_authorised_cots, nursery_newborn_authorised_cots, orthopaedic_authorised_cots,
               medical_actual_physical_cots, surgical_actual_physical_cots, obst_gyn_actual_physical_cots, paediatrics_actual_physical_cots, maternity_actual_physical_cots,
               eye_actual_physical_cots, nursery_newborn_actual_physical_cots, orthopaedic_actual_physical_cots, medical_authorised_incubator, surgical_authorised_incubator,
               obst_gyn_authorised_incubator, paediatrics_authorised_incubator, maternity_authorised_incubator, eye_authorised_incubator, nursery_newborn_authorised_incubator,
               orthopaedic_authorised_incubator, medical_actual_Physical_incubator, surgical_actual_Physical_incubator, obst_gyn_actual_Physical_incubator, paediatrics_actual_Physical_incubator,
               maternity_actual_Physical_incubator, eye_actual_Physical_incubator, nursery_newborn_actual_Physical_incubator, orthopaedic_actual_Physical_incubator, normal_deliveries,
               caesarean_sections, breach_deliveries, assisted_vaginal_deliveries, born_before_arrival, maternal_deaths,
               maternal_deaths_audited_within_7_days, live_births, still_births, neonatal_deaths_0_28_days, neonatal_deaths_audits,
               low_birth_weight_less_2500gms, babies_discharged_alive, minor_surgeries_booked, minor_surgeries_operated, emergencies_booked,
               emergencies_operated, cold_cases_booked, cold_surgical_cases, circumcisions_booked, circumcisions_operated,
               major_surgeries_booked, major_surgeries_operated, casts_fixed_less_5_years_new, casts_fixed_less_5_years_revisit, casts_fixed_over_5_years_new,
               casts_fixed_over_5_years_revisit, tractions_fixed_less_5_years_new, tractions_fixed_less_5_years_revisit, tractions_fixed_over_5_years_new, tractions_fixed_over_5_years_revisit,
               closed_reductions_less_5_years_new, closed_reductions_less_5_years_revisit, closed_reductions_over_5_years_new, closed_reductions_over_5_years_revisit, orthopaedic_assisted_in_theatre_less_5_years_new,
               orthopaedic_assisted_in_theatre_less_5_years_revisit, orthopaedic_assisted_in_theatre_over_5_years_new, orthopaedic_assisted_in_theatre_over_5_years_revisit, club_foot_seen_less_5_years_new, club_foot_seen_less_5_years_revisit,
               club_foot_seen_over_5_years_new, club_foot_seen_over_5_years_revisit, crepe_bandages_applied_less_5_years_new, crepe_bandages_applied_less_5_years_revisit, crepe_bandages_applied_over_5_years_new,
               crepe_bandages_applied_over_5_years_revisit, removals_done_cast_less_5_years, removals_done_cast_over_5_years, removals_done_tractions_less_5_years, removals_done_tractions_over_5_years,
               ex_fixator_removed_less_5_years, ex_fixator_removed_over_5_years, lab_routine_test, lab_special_test, plain_xray_without_enhancement,
               contrast_enhancement_examination, magnetic_resonance_imaging, computerized_tomography, mammography, chest_xray_for_ptb,
               number_of_ultrasound_examinations, general_ultrasound, obstetric_ultrasound, total_routine_xray_and_imaging, total_special_exams_xray_and_imaging,
               physiotherapy_number_of_treatments, occupational_therapy_number_of_treatments, orthopaedic_technology_prepared, orthopaedic_technology_issued, prescriptions_issued_common_drugs,
               prescriptions_issued_antibiotics, prescriptions_issued_special_drugs, prescriptions_issued_drugs_children, mortuary_body_days, mortuary_embalment,
               mortuary_postmortem, mortuary_unclaimed_bodies, new_patient_files_issued, outpatient_records_cards_issued
            FROM flat_moh_717_report_0'
        );
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT('DELETE t1 FROM ', @queue_table, ' t1 JOIN temp_queue_table t2 USING (person_id)');
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT('SELECT COUNT(*) INTO @queue_count FROM ', @queue_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @cycle_length   = TIMESTAMPDIFF(SECOND, @loop_start_time, NOW());
        SET @total_time     = @total_time + @cycle_length;
        SET @cycle_number   = @cycle_number + 1;
        SET @remaining_time = CEIL((@total_time / @cycle_number) * CEIL(@queue_count / @cycle_size) / 60);

        SELECT
            @queue_count                     AS '# in queue',
            @cycle_length                    AS 'Cycle Time (s)',
            CEIL(@queue_count / @cycle_size) AS remaining_cycles,
            @remaining_time                  AS 'Est time remaining (min)';

    END WHILE;

    -- Remove voided persons
    SET @dyn_sql = CONCAT(
        'DELETE t1 FROM ', @primary_table, ' t1
         JOIN amrs.person t2 USING (person_id)
         WHERE t2.voided = 1'
    );
    PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

    -- ------------------------------------------------------------------
    -- BUILD mode: merge temp table -> primary
    -- ------------------------------------------------------------------
    IF (@query_type = 'build') THEN
        SET @dyn_sql = CONCAT('DROP TABLE ', @queue_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @total_rows_to_write = 0;
        SET @dyn_sql = CONCAT('SELECT COUNT(*) INTO @total_rows_to_write FROM ', @write_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @start_write = NOW();
        SELECT CONCAT(@start_write, ' : Writing ', @total_rows_to_write, ' to ', @primary_table);

        SET @dyn_sql = CONCAT('REPLACE INTO ', @primary_table, ' (SELECT * FROM ', @write_table, ')');
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @finish_write  = NOW();
        SET @time_to_write = TIMESTAMPDIFF(SECOND, @start_write, @finish_write);
        SELECT CONCAT(@finish_write, ' : Completed. Time to write: ', @time_to_write, ' seconds');

        SET @dyn_sql = CONCAT('DROP TABLE ', @write_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;
    END IF;

    SELECT @end := NOW();

    -- Advance the sync watermark so next sync run only processes new rows.
    -- Positional INSERT — matches the pattern used in generate_flat_moh_706_v1
    -- which writes: (start_time, date_updated, table_name, completion_seconds).
    INSERT INTO etl.flat_log
    VALUES (@start, NOW(), @table_version, TIMESTAMPDIFF(SECOND, @start, @end));

    SELECT CONCAT(@table_version, ' : Time to complete: ', TIMESTAMPDIFF(MINUTE, @start, @end), ' minutes');

END
