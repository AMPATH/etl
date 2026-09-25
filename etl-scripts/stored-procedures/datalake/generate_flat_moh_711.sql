CREATE DEFINER=`openmrs_user`@`%` PROCEDURE `etl`.`generate_flat_moh_711_v1`(
    IN query_type   VARCHAR(50),
    IN queue_number INT,
    IN queue_size   INT,
    IN cycle_size   INT
)
BEGIN
    # MOH 711 Report ETL - v1.0
    # Reads from flat_obs and extracts indicator columns for MOH 711
    # (ANC, SGBV, Maternity, Cervical Cancer, Family Planning, PNC,
    #  Psychosocial, TB Screening, Child Welfare/Nutrition).
    #
    # Indicator logic translated from the etl-rest-server json-report
    # definitions at:
    #   app/reporting-framework/json-reports/moh-711/aggregations/*.json
    # (ancBase/gbvBase/maternityBase/cervicalCancerBase/familyPlanningBase/
    #  pncBase/socialWorkBase/tbScreeningBase/chanisBase + aggregates)
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
    # and period downstream reproduces the MOH 711 aggregate report.
    #
    # Columns whose upstream expression is a "null" placeholder are emitted
    # as NULL columns (they appear as null in the live report output too).
    # Upstream quirks are preserved verbatim and flagged with NOTE comments
    # so this table can be reconciled 1:1 against the etl-rest-server report.
    #
    # One rename: the PNC section's referrals_from_community collides with
    # the Maternity section's identical column name, so it is emitted as
    # pnc_referrals_from_community (same concept logic, PNC encounter gate).

    SET session sort_buffer_size     = 512000000;
    SET session group_concat_max_len = 100000;

    SET @start              = NOW();
    SET @primary_table      = 'flat_moh_711_report';
    SET @table_version      = 'flat_moh_711_report_v1.0';
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
    -- the house-style VARCHAR(100): 284 x VARCHAR(100) would exceed the
    -- MySQL row-size limit on utf8mb4 schemas.
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

        -- ANC (encounter types 264, 265)
        new_anc_clients                VARCHAR(5),
        revisit_anc_clients            VARCHAR(5),
        first_ipt_dose                 VARCHAR(5),
        second_ipt_dose                VARCHAR(5),
        third_ipt_dose                 VARCHAR(5),
        Hb_less_11g                    VARCHAR(5),
        first_anc_contact_12_or_less_weeks VARCHAR(5),
        completing_4_anc_visits        VARCHAR(5),
        completing_8_anc_contact       VARCHAR(5),
        llins_children_less_than_one   VARCHAR(5),
        llins_anc_clients              VARCHAR(5),
        anc_clients_tested_syphilis    VARCHAR(5),
        syphilis_positive              VARCHAR(5),
        breast_exam                    VARCHAR(5),
        adolesc_10_14                  VARCHAR(5),
        adolesc_15_19                  VARCHAR(5),
        youth_20_24                    VARCHAR(5),
        iron                           VARCHAR(5),
        folic                          VARCHAR(5),
        iron_and_folate                VARCHAR(5),
        fgm_complications              VARCHAR(5),

        -- SGBV (encounter types 56, 57, 133, 134, 264, 265, 179)
        sgbv_total_survivors_0_9       VARCHAR(5),
        sgbv_total_survivors_10_17     VARCHAR(5),
        sgbv_total_survivors_18_49     VARCHAR(5),
        sgbv_total_survivors_50_plus   VARCHAR(5),
        sgbv_within_72_hours_0_9       VARCHAR(5),
        sgbv_within_72_hours_10_17     VARCHAR(5),
        sgbv_within_72_hours_18_49     VARCHAR(5),
        sgbv_within_72_hours_50_above  VARCHAR(5),
        sgbv_initiated_on_pep_0_9      VARCHAR(5),
        sgbv_initiated_on_pep_10_17    VARCHAR(5),
        sgbv_initiated_on_pep_18_49    VARCHAR(5),
        sgbv_initiated_on_pep_50_above VARCHAR(5),
        sgbv_completed_pep_0_9         VARCHAR(5),
        sgbv_completed_pep_10_17       VARCHAR(5),
        sgbv_completed_pep_18_49       VARCHAR(5),
        sgbv_completed_pep_50_above    VARCHAR(5),
        sgbv_seroconverting_0_9        VARCHAR(5),
        sgbv_seroconverting_10_17      VARCHAR(5),
        sgbv_seroconverting_18_49      VARCHAR(5),
        sgbv_seroconverting_50_above   VARCHAR(5),
        sgbv_eligible_for_ecp_0_9      VARCHAR(5),
        sgbv_eligible_for_ecp_10_17    VARCHAR(5),
        sgbv_eligible_for_ecp_18_49    VARCHAR(5),
        sgbv_eligible_for_ecp_50_above VARCHAR(5),
        sgbv_receiving_ecp_0_9         VARCHAR(5),
        sgbv_receiving_ecp_10_17       VARCHAR(5),
        sgbv_receiving_ecp_18_49       VARCHAR(5),
        sgbv_receiving_ecp_50_above    VARCHAR(5),
        sgbv_pregnant_0_9              VARCHAR(5),
        sgbv_pregnant_10_17            VARCHAR(5),
        sgbv_pregnant_18_49            VARCHAR(5),
        sgbv_pregnant_50_above         VARCHAR(5),
        sgbv_rc_seen_0_9               VARCHAR(5),
        sgbv_rc_seen_10_17             VARCHAR(5),
        sgbv_rc_seen_18_49             VARCHAR(5),
        sgbv_rc_seen_50_above          VARCHAR(5),
        sgbv_with_disability_0_9       VARCHAR(5),
        sgbv_with_disability_10_17     VARCHAR(5),
        sgbv_with_disability_18_49     VARCHAR(5),
        sgbv_with_disability_50_above  VARCHAR(5),

        -- Maternity / Delivery (encounter types 196, 269, 273, 274)
        normal_deliveries              VARCHAR(5),
        caesarian_sections             VARCHAR(5),
        breach_delivery                VARCHAR(5),
        assisted_vaginal_delivery      VARCHAR(5),
        oxytocin_uterotonic            VARCHAR(5),
        carbatocin_uterotonic          VARCHAR(5),
        live_birth                     VARCHAR(5),
        low_birth_weight               VARCHAR(5),
        low_apgar_score                VARCHAR(5),
        birth_with_deformity           VARCHAR(5),
        chlorhexidine_applied          VARCHAR(5),
        vitamin_k                      VARCHAR(5),
        tetracycline_given             VARCHAR(5),
        pre_term_babies                VARCHAR(5),
        discharge_alive                VARCHAR(5),
        bf_within_1_hour               VARCHAR(5),
        deliveries_from_positive_women VARCHAR(5),
        fresh_still_birth              VARCHAR(5),
        macerated_still_birth          VARCHAR(5),
        perinatal_deaths_0_7_days      VARCHAR(5),
        neonatal_deaths_0_28_days      VARCHAR(5),
        maternal_deaths_10_14_years    VARCHAR(5),
        maternal_deaths_15_19_years    VARCHAR(5),
        maternal_deaths_20_24_years    VARCHAR(5),
        maternal_deaths_25_above_years VARCHAR(5),
        maternal_deaths_audited_within_7_days VARCHAR(5),
        neonatal_deaths_audited_within_7_days VARCHAR(5),
        ante_partum_haemorrage         VARCHAR(5),
        post_partum_haemorrage         VARCHAR(5),
        eclampsia                      VARCHAR(5),
        ruptured_uterus                VARCHAR(5),
        obstructed_labour              VARCHAR(5),
        sepsis                         VARCHAR(5),
        fgm_delivery_complications     VARCHAR(5),
        neonatal_deaths_sepsis         VARCHAR(5),
        neonatal_deaths_prematurity    VARCHAR(5),
        neonatal_deaths_asphyxia       VARCHAR(5),
        kangaroo_mother_care           VARCHAR(5),
        referrals_from_other_health_facility VARCHAR(5),
        referrals_to_other_health_facility   VARCHAR(5),
        referrals_from_community       VARCHAR(5),
        referrals_to_community         VARCHAR(5),

        -- Cervical Cancer Screening (encounter type 69)
        via_villi_hpv_less_25          VARCHAR(5),
        via_villi_hpv_25_49            VARCHAR(5),
        via_villi_hpv_50_above         VARCHAR(5),
        pap_smear_less_25              VARCHAR(5),
        pap_smear_25_49                VARCHAR(5),
        pap_smear_50_above             VARCHAR(5),
        hpv_test_less_25               VARCHAR(5),
        hpv_test_25_49                 VARCHAR(5),
        hpv_test_50_above              VARCHAR(5),
        positive_via_villi_less_25     VARCHAR(5),
        positive_via_villi_25_49       VARCHAR(5),
        positive_via_villi_50_above    VARCHAR(5),
        positive_cytology_less_25      VARCHAR(5),
        positive_cytology_25_49        VARCHAR(5),
        positive_cytology_50_above     VARCHAR(5),
        positive_hpv_less_25           VARCHAR(5),
        positive_hpv_25_49             VARCHAR(5),
        positive_hpv_50_above          VARCHAR(5),
        suspicious_cancer_lessions_less_25 VARCHAR(5),
        suspicious_cancer_lessions_25_49   VARCHAR(5),
        suspicious_cancer_lessions_50_above VARCHAR(5),
        cryotherapy_treatment_less_25  VARCHAR(5),
        cryotherapy_treatment_25_49    VARCHAR(5),
        cryotherapy_treatment_50_above VARCHAR(5),
        leep_treatment_less_25         VARCHAR(5),
        leep_treatment_25_49           VARCHAR(5),
        leep_treatment_50_above        VARCHAR(5),
        hiv_positive_screened_cervical_cancer_less_25 VARCHAR(5),
        hiv_positive_screened_cervical_cancer_25_49   VARCHAR(5),
        hiv_positive_screened_cervical_cancer_50_above VARCHAR(5),

        -- Family Planning (encounter type 179)
        first_users_contraceptive_new          VARCHAR(5),
        first_users_contraceptive_revisit      VARCHAR(5),
        pills_progestine_new                   VARCHAR(5),
        pills_progestine_revisit               VARCHAR(5),
        pills_combined_oral_contraceptive_new  VARCHAR(5),
        pills_combined_oral_contraceptive_revisit VARCHAR(5),
        emergency_contraceptive_pill_new       VARCHAR(5),
        emergency_contraceptive_pill_revisit   VARCHAR(5),
        fp_injections_dmpa_im                  VARCHAR(5),
        fp_injections_dmpa_sc                  VARCHAR(5),
        male_condoms_new                       VARCHAR(5),
        male_condoms_revisit                   VARCHAR(5),
        female_condoms_new                     VARCHAR(5),
        female_condoms_revisit                 VARCHAR(5),
        male_and_female_condoms_new            VARCHAR(5),
        male_and_female_condoms_revisit        VARCHAR(5),
        counselled_for_natural_family_planning_new    VARCHAR(5),
        counselled_for_natural_family_planning_revisit VARCHAR(5),
        cycle_beads_new                        VARCHAR(5),
        cycle_beads_revisit                    VARCHAR(5),
        implants_insertion_1_rod               VARCHAR(5),
        implants_insertion_2_rod               VARCHAR(5),
        iucd_hormonal                          VARCHAR(5),
        iucd_non_hormonal                      VARCHAR(5),
        surgical_contraception_btl             VARCHAR(5),
        vasectomy                              VARCHAR(5),
        iucd_removals                          VARCHAR(5),
        implants_removal                       VARCHAR(5),
        fp_adolescent_10_14_new                VARCHAR(5),
        fp_adolescent_10_14_revisit            VARCHAR(5),
        fp_adolescent_15_19_new                VARCHAR(5),
        fp_adolescent_15_19_revisit            VARCHAR(5),
        fp_adolescent_20_24_new                VARCHAR(5),
        fp_adolescent_20_24_revisit            VARCHAR(5),
        fp_adolescent_25_plus_new              VARCHAR(5),
        fp_adolescent_25_plus_revisit          VARCHAR(5),
        post_partum_fp_48_hours_new            VARCHAR(5),
        post_partum_fp_48_hours_revisit        VARCHAR(5),
        post_partum_fp_4_6_weeks_new           VARCHAR(5),
        post_partum_fp_4_6_weeks_revisit       VARCHAR(5),
        post_abortion_fp_new                   VARCHAR(5),
        post_abortion_fp_revisit               VARCHAR(5),

        -- PNC (encounter types 266, 267)
        pnc_new_clients                       VARCHAR(5),
        pnc_revisit_clients                   VARCHAR(5),
        women_couselled_on_post_partum_fp     VARCHAR(5),
        women_received_post_partum_fp         VARCHAR(5),
        mothers_post_partum_care_48_hours     VARCHAR(5),
        mothers_post_partum_care_3_6_weeks    VARCHAR(5),
        mothers_post_partum_care_after_6_weeks VARCHAR(5),
        infants_post_partum_care_48_hours     VARCHAR(5),
        infants_post_partum_care_3_6_weeks    VARCHAR(5),
        infants_post_partum_care_after_6_weeks VARCHAR(5),
        fistula_cases                         VARCHAR(5),
        pnc_referrals_from_community          VARCHAR(5),

        -- Psychosocial / Social Work (encounter types 1, 2)
        psycho_social_counselling             VARCHAR(5),
        alcohol_and_drug_abuse                VARCHAR(5),
        mental_illness                        VARCHAR(5),
        adolescent_issues                     VARCHAR(5),
        psycho_social_economic_assessment     VARCHAR(5),
        social_investigations                 VARCHAR(5),
        psycho_social_rehabilitation          VARCHAR(5),
        outreach_services                     VARCHAR(5),
        mental_health_referral                VARCHAR(5),

        -- TB Screening (encounter types 1, 2, 264, 265, 266, 267)
        total_screened_for_tb                 VARCHAR(5),
        total_presumptive_tb_cases            VARCHAR(5),
        already_on_tb_treatment               VARCHAR(5),
        not_screened_for_tb                   VARCHAR(5),

        -- Child Welfare / Nutrition - chanis (encounter types 4, 110, 167, 313)
        normal_weight_0_6_months_male         VARCHAR(5),
        normal_weight_0_6_months_female       VARCHAR(5),
        underweight_0_6_months_male           VARCHAR(5),
        underweight_0_6_months_female         VARCHAR(5),
        severely_underweight_0_6_months_male  VARCHAR(5),
        severely_underweight_0_6_months_female VARCHAR(5),
        overweight_0_6_months_male            VARCHAR(5),
        overweight_0_6_months_female          VARCHAR(5),
        obese_0_6_months_male                 VARCHAR(5),
        obese_0_6_months_female               VARCHAR(5),
        normal_weight_6_23_months_male        VARCHAR(5),
        normal_weight_6_23_months_female      VARCHAR(5),
        underweight_6_23_months_male          VARCHAR(5),
        underweight_6_23_months_female        VARCHAR(5),
        severely_underweight_6_23_months_male VARCHAR(5),
        severely_underweight_6_23_months_female VARCHAR(5),
        overweight_6_23_months_male           VARCHAR(5),
        overweight_6_23_months_female         VARCHAR(5),
        obese_6_23_months_male                VARCHAR(5),
        obese_6_23_months_female              VARCHAR(5),
        normal_weight_24_59_months_male       VARCHAR(5),
        normal_weight_24_59_months_female     VARCHAR(5),
        underweight_24_59_months_male         VARCHAR(5),
        underweight_24_59_months_female       VARCHAR(5),
        severely_underweight_24_59_months_male VARCHAR(5),
        severely_underweight_24_59_months_female VARCHAR(5),
        overweight_24_59_months_male          VARCHAR(5),
        overweight_24_59_months_female        VARCHAR(5),
        obese_24_59_months_male               VARCHAR(5),
        obese_24_59_months_female             VARCHAR(5),
        muac_6_59_months_normal_male          VARCHAR(5),
        muac_6_59_months_normal_female        VARCHAR(5),
        muac_6_59_months_moderate_male        VARCHAR(5),
        muac_6_59_months_moderate_female      VARCHAR(5),
        muac_6_59_months_severe_male          VARCHAR(5),
        muac_6_59_months_severe_female        VARCHAR(5),
        height_for_age_0_6_months_normal_male VARCHAR(5),
        height_for_age_0_6_months_normal_female VARCHAR(5),
        stunted_0_6_months_male               VARCHAR(5),
        stunted_0_6_months_female             VARCHAR(5),
        severely_stunted_0_6_months_male      VARCHAR(5),
        severely_stunted_0_6_months_female    VARCHAR(5),
        normal_height_for_age_6_23_months_male VARCHAR(5),
        normal_height_for_age_6_23_months_female VARCHAR(5),
        stunted_6_23_months_male              VARCHAR(5),
        stunted_6_23_months_female            VARCHAR(5),
        severely_stunted_6_23_months_male     VARCHAR(5),
        severely_stunted_6_23_months_female   VARCHAR(5),
        normal_height_for_age_24_59_months_male VARCHAR(5),
        normal_height_for_age_24_59_months_female VARCHAR(5),
        stunted_24_59_months_male             VARCHAR(5),
        stunted_24_59_months_female           VARCHAR(5),
        severely_stunted_24_59_months_male    VARCHAR(5),
        severely_stunted_24_59_months_female  VARCHAR(5),
        new_visits_0_59_months_attending_cwc_male   VARCHAR(5),
        new_visits_0_59_months_attending_cwc_female VARCHAR(5),
        kwashiokor_0_59_months_male           VARCHAR(5),
        kwashiokor_0_59_months_female         VARCHAR(5),
        marasmus_0_59_months_male             VARCHAR(5),
        marasmus_0_59_months_female           VARCHAR(5),
        faltering_growth_0_59_months_male     VARCHAR(5),
        faltering_growth_0_59_months_female   VARCHAR(5),
        exclusive_breastfeeding_0_6_months_male VARCHAR(5),
        exclusive_breastfeeding_0_6_months_female VARCHAR(5),
        dewormed_12_59_months_male            VARCHAR(5),
        dewormed_12_59_months_female          VARCHAR(5),
        mnps_supplimentation_6_23_months_male VARCHAR(5),
        mnps_supplimentation_6_23_months_female VARCHAR(5),
        diarhoea_severe_dehydration_male      VARCHAR(5),
        diarhoea_severe_dehydration_female    VARCHAR(5),
        diarhoea_some_dehydration_male        VARCHAR(5),
        diarhoea_some_dehydration_female      VARCHAR(5),
        diarhoea_no_dehydration_male          VARCHAR(5),
        diarhoea_no_dehydration_female        VARCHAR(5),
        diarhoea_treated_with_ors_zinc_dehydration_male   VARCHAR(5),
        diarhoea_treated_with_ors_zinc_dehydration_female VARCHAR(5),
        under_five_pneumomia_male             VARCHAR(5),
        under_five_pneumomia_female           VARCHAR(5),
        under_five_deaths_male                VARCHAR(5),
        under_five_deaths_female              VARCHAR(5),
        under_five_disability_male            VARCHAR(5),
        under_five_disability_female          VARCHAR(5),
        under_five_delayed_development_male   VARCHAR(5),
        under_five_delayed_development_female VARCHAR(5),

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
    -- `flat_moh_711_report_build_queue`.
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

        CREATE TABLE IF NOT EXISTS flat_moh_711_report_sync_queue (
            person_id INT PRIMARY KEY
        );

        REPLACE INTO flat_moh_711_report_sync_queue
            (SELECT DISTINCT patient_id FROM amrs.encounter WHERE date_changed > @last_update);
        REPLACE INTO flat_moh_711_report_sync_queue
            (SELECT DISTINCT person_id FROM etl.flat_obs WHERE max_date_created > @last_update);
        REPLACE INTO flat_moh_711_report_sync_queue
            (SELECT person_id FROM amrs.person WHERE date_voided > @last_update);
        REPLACE INTO flat_moh_711_report_sync_queue
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
    -- Rows are restricted to the union of the MOH 711 section encounter
    -- types: ANC (264,265), SGBV (56,57,133,134,179), Maternity
    -- (196,269,273,274), Cervical Cancer (69), FP (179), PNC (266,267),
    -- Psychosocial (1,2), TB (1,2,264,265,266,267), CWC (4,110,167,313).
    -- flat_obs synthetic obs-set rows (encounter_type 99999) are excluded.
    -- ------------------------------------------------------------------
    WHILE @queue_count > 0 DO

        SET @loop_start_time = NOW();

        DROP TEMPORARY TABLE IF EXISTS temp_queue_table;
        SET @dyn_sql = CONCAT('CREATE TEMPORARY TABLE temp_queue_table LIKE ', @queue_table);
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT('REPLACE INTO temp_queue_table (SELECT * FROM ', @queue_table, ' LIMIT ', @cycle_size, ')');
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        DROP TEMPORARY TABLE IF EXISTS flat_moh_711_report_0;

        SET @dyn_sql = CONCAT(
            'CREATE TEMPORARY TABLE flat_moh_711_report_0
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

                -- --------------------------------------------------------
                -- ANC (encounter types 264, 265)
                -- The ancBase join only keeps encounters that carry at
                -- least one of the ANC data concepts, so the pure
                -- encounter-type flags below are guarded the same way.
                -- --------------------------------------------------------
                IF(t1.encounter_type IN (264,265) AND t1.obs REGEXP \"!!21=|!!299=|!!2356=|!!6429=|!!7086=|!!10919=|!!9487=|!!12081=\", 1, 0) AS new_anc_clients,
                IF(t1.encounter_type = 264 AND t1.obs REGEXP \"!!21=|!!299=|!!2356=|!!6429=|!!7086=|!!10919=|!!9487=|!!12081=\", 1, 0) AS revisit_anc_clients,

                -- NOTE (upstream quirk preserved): the ancBase join does
                -- not include concept 2366 (IPT dose), so the live report
                -- under-counts IPT; here the indicator logic is applied
                -- directly to the packed obs.
                IF(t1.encounter_type IN (264,265) AND t1.obs REGEXP \"!!2366=2364!!\", 1, 0) AS first_ipt_dose,
                IF(t1.encounter_type IN (264,265) AND t1.obs REGEXP \"!!2366=2365!!\", 1, 0) AS second_ipt_dose,
                IF(t1.encounter_type IN (264,265) AND t1.obs REGEXP \"!!2366=8526!!\", 1, 0) AS third_ipt_dose,

                IF(t1.encounter_type IN (264,265) AND t1.obs REGEXP \"!!21=\" AND SUBSTRING_INDEX(getValues(t1.obs, 21), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\", IF(CAST(SUBSTRING_INDEX(getValues(t1.obs, 21), \" ## \", 1) AS DECIMAL(10,4)) < 11, 1, 0), 0) AS Hb_less_11g,
                IF(t1.encounter_type IN (264,265) AND t1.obs REGEXP \"!!6429=\" AND SUBSTRING_INDEX(getValues(t1.obs, 6429), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\", IF(CAST(SUBSTRING_INDEX(getValues(t1.obs, 6429), \" ## \", 1) AS DECIMAL(10,4)) <= 12, 1, 0), 0) AS first_anc_contact_12_or_less_weeks,
                IF(t1.encounter_type IN (264,265) AND t1.obs REGEXP \"!!7086=\" AND SUBSTRING_INDEX(getValues(t1.obs, 7086), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\", IF(CAST(SUBSTRING_INDEX(getValues(t1.obs, 7086), \" ## \", 1) AS DECIMAL(10,4)) = 4, 1, 0), 0) AS completing_4_anc_visits,
                IF(t1.encounter_type IN (264,265) AND t1.obs REGEXP \"!!7086=\" AND SUBSTRING_INDEX(getValues(t1.obs, 7086), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\", IF(CAST(SUBSTRING_INDEX(getValues(t1.obs, 7086), \" ## \", 1) AS DECIMAL(10,4)) = 8, 1, 0), 0) AS completing_8_anc_contact,
                IF(t1.encounter_type IN (264,265) AND t1.obs REGEXP \"!!2356=2354!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 1, 1, 0) AS llins_children_less_than_one,
                IF(t1.encounter_type IN (264,265) AND t1.obs REGEXP \"!!2356=2354!!\", 1, 0) AS llins_anc_clients,
                IF(t1.encounter_type IN (264,265) AND t1.obs REGEXP \"!!299=\", 1, NULL) AS anc_clients_tested_syphilis,
                IF(t1.encounter_type IN (264,265) AND t1.obs REGEXP \"!!299=703!!\", 1, NULL) AS syphilis_positive,
                IF(t1.encounter_type IN (264,265) AND t1.obs REGEXP \"!!10919=1065!!\", 1, NULL) AS breast_exam,
                IF(t1.encounter_type = 265 AND t1.obs REGEXP \"!!21=|!!299=|!!2356=|!!6429=|!!7086=|!!10919=|!!9487=|!!12081=\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 10 AND 14, 1, 0) AS adolesc_10_14,
                IF(t1.encounter_type = 265 AND t1.obs REGEXP \"!!21=|!!299=|!!2356=|!!6429=|!!7086=|!!10919=|!!9487=|!!12081=\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 15 AND 19, 1, 0) AS adolesc_15_19,

                -- NOTE (upstream quirk preserved): youth_20_24 upstream
                -- is the breast_exam expression (concept 10919 = 1065 on
                -- a new ANC visit), not an age band.
                IF(t1.encounter_type = 265 AND t1.obs REGEXP \"!!10919=1065!!\", 1, NULL) AS youth_20_24,

                IF(t1.encounter_type IN (264,265) AND t1.obs REGEXP \"!!9487=2363!!\", 1, NULL) AS iron,
                IF(t1.encounter_type IN (264,265) AND t1.obs REGEXP \"!!9487=257!!\", 1, NULL) AS folic,
                IF(t1.encounter_type IN (264,265) AND t1.obs REGEXP \"!!9487=9497!!\", 1, NULL) AS iron_and_folate,
                IF(t1.encounter_type IN (264,265) AND t1.obs REGEXP \"!!12081=\" AND t1.obs NOT REGEXP \"!!12081=1107!!\", 1, NULL) AS fgm_complications,

                -- --------------------------------------------------------
                -- SGBV (encounter types 56, 57, 133, 134, 264, 265, 179)
                -- Age bands: 0-9, 10-17, 18-49, 50+ at the encounter.
                -- NOTE (upstream naming preserved): total_survivors uses
                -- the _50_plus suffix, all other indicators _50_above.
                -- --------------------------------------------------------
                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!11866=|!!11865=|!!9303=|!!12054=\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) <= 9, 1, 0) AS sgbv_total_survivors_0_9,
                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!11866=|!!11865=|!!9303=|!!12054=\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 10 AND 17, 1, 0) AS sgbv_total_survivors_10_17,
                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!11866=|!!11865=|!!9303=|!!12054=\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 18 AND 49, 1, 0) AS sgbv_total_survivors_18_49,
                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!11866=|!!11865=|!!9303=|!!12054=\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 50, 1, 0) AS sgbv_total_survivors_50_plus,

                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!7134=\" AND SUBSTRING_INDEX(getValues(t1.obs, 7134), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 7134), \" ## \", 1) AS DECIMAL(10,4)) <= 72 AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) <= 9, 1, 0) AS sgbv_within_72_hours_0_9,
                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!7134=\" AND SUBSTRING_INDEX(getValues(t1.obs, 7134), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 7134), \" ## \", 1) AS DECIMAL(10,4)) <= 72 AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 10 AND 17, 1, 0) AS sgbv_within_72_hours_10_17,
                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!7134=\" AND SUBSTRING_INDEX(getValues(t1.obs, 7134), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 7134), \" ## \", 1) AS DECIMAL(10,4)) <= 72 AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 18 AND 49, 1, 0) AS sgbv_within_72_hours_18_49,
                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!7134=\" AND SUBSTRING_INDEX(getValues(t1.obs, 7134), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 7134), \" ## \", 1) AS DECIMAL(10,4)) <= 72 AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 50, 1, 0) AS sgbv_within_72_hours_50_above,

                IF(t1.encounter_type = 56 AND t1.obs REGEXP \"!!1705=1149!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) <= 9, 1, 0) AS sgbv_initiated_on_pep_0_9,
                IF(t1.encounter_type = 56 AND t1.obs REGEXP \"!!1705=1149!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 10 AND 17, 1, 0) AS sgbv_initiated_on_pep_10_17,
                IF(t1.encounter_type = 56 AND t1.obs REGEXP \"!!1705=1149!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 18 AND 49, 1, 0) AS sgbv_initiated_on_pep_18_49,
                IF(t1.encounter_type = 56 AND t1.obs REGEXP \"!!1705=1149!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 50, 1, 0) AS sgbv_initiated_on_pep_50_above,

                IF(t1.encounter_type = 57 AND t1.obs REGEXP \"!!7087=1065!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) <= 9, 1, 0) AS sgbv_completed_pep_0_9,
                IF(t1.encounter_type = 57 AND t1.obs REGEXP \"!!7087=1065!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 10 AND 17, 1, 0) AS sgbv_completed_pep_10_17,
                IF(t1.encounter_type = 57 AND t1.obs REGEXP \"!!7087=1065!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 18 AND 49, 1, 0) AS sgbv_completed_pep_18_49,
                IF(t1.encounter_type = 57 AND t1.obs REGEXP \"!!7087=1065!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 50, 1, 0) AS sgbv_completed_pep_50_above,

                IF(t1.encounter_type = 57 AND t1.obs REGEXP \"!!1040=703!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) <= 9, 1, 0) AS sgbv_seroconverting_0_9,
                IF(t1.encounter_type = 57 AND t1.obs REGEXP \"!!1040=703!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 10 AND 17, 1, 0) AS sgbv_seroconverting_10_17,
                IF(t1.encounter_type = 57 AND t1.obs REGEXP \"!!1040=703!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 18 AND 49, 1, 0) AS sgbv_seroconverting_18_49,
                IF(t1.encounter_type = 57 AND t1.obs REGEXP \"!!1040=703!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 50, 1, 0) AS sgbv_seroconverting_50_above,

                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!9303=1065!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) <= 9, 1, 0) AS sgbv_eligible_for_ecp_0_9,
                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!9303=1065!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 10 AND 17, 1, 0) AS sgbv_eligible_for_ecp_10_17,
                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!9303=1065!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 18 AND 49, 1, 0) AS sgbv_eligible_for_ecp_18_49,
                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!9303=1065!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 50, 1, 0) AS sgbv_eligible_for_ecp_50_above,

                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!9610=6725!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) <= 9, 1, 0) AS sgbv_receiving_ecp_0_9,
                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!9610=6725!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 10 AND 17, 1, 0) AS sgbv_receiving_ecp_10_17,
                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!9610=6725!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 18 AND 49, 1, 0) AS sgbv_receiving_ecp_18_49,
                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!9610=6725!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 50, 1, 0) AS sgbv_receiving_ecp_50_above,

                -- TODO upstream: sgbv_pregnant has no expression in gbvBase
                NULL AS sgbv_pregnant_0_9,
                NULL AS sgbv_pregnant_10_17,
                NULL AS sgbv_pregnant_18_49,
                NULL AS sgbv_pregnant_50_above,

                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!1061=5564!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) <= 9, 1, 0) AS sgbv_rc_seen_0_9,
                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!1061=5564!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 10 AND 17, 1, 0) AS sgbv_rc_seen_10_17,
                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!1061=5564!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 18 AND 49, 1, 0) AS sgbv_rc_seen_18_49,
                IF(t1.encounter_type IN (56,57,133,134,264,265,179) AND t1.obs REGEXP \"!!1061=5564!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 50, 1, 0) AS sgbv_rc_seen_50_above,

                -- TODO upstream: sgbv_with_disability has no expression in gbvBase
                NULL AS sgbv_with_disability_0_9,
                NULL AS sgbv_with_disability_10_17,
                NULL AS sgbv_with_disability_18_49,
                NULL AS sgbv_with_disability_50_above,

                -- --------------------------------------------------------
                -- Maternity / Delivery (encounter types 196, 269, 273, 274)
                -- --------------------------------------------------------
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!5630=1170!!\", 1, NULL) AS normal_deliveries,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!5630=1171!!\", 1, NULL) AS caesarian_sections,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!5630=1172!!\", 1, NULL) AS breach_delivery,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!5630=2167!!|!!5630=2166!!\", 1, NULL) AS assisted_vaginal_delivery,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!12086=7593!!\", 1, NULL) AS oxytocin_uterotonic,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!12086=12085!!\", 1, NULL) AS carbatocin_uterotonic,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!6224=1843!!\", 1, NULL) AS live_birth,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!6433=\" AND SUBSTRING_INDEX(getValues(t1.obs, 6433), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 6433), \" ## \", 1) AS DECIMAL(10,4)) < 2500, 1, NULL) AS low_birth_weight,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!6431=\" AND SUBSTRING_INDEX(getValues(t1.obs, 6431), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 6431), \" ## \", 1) AS DECIMAL(10,4)) <= 6, 1, NULL) AS low_apgar_score,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!10429=1065!!\", 1, NULL) AS birth_with_deformity,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!12090=1065!!\", 1, NULL) AS chlorhexidine_applied,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!12091=1065!!\", 1, NULL) AS vitamin_k,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!10426=1065!!\", 1, NULL) AS tetracycline_given,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!6224=6648!!\", 1, NULL) AS pre_term_babies,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!10938=8701!!\", 1, NULL) AS discharge_alive,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!10425=1065!!\", 1, NULL) AS bf_within_1_hour,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!1357=703!!\", 1, NULL) AS deliveries_from_positive_women,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!6224=10877!!\", 1, NULL) AS fresh_still_birth,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!6224=10424!!\", 1, NULL) AS macerated_still_birth,

                -- TODO upstream: no expression in maternityBase
                NULL AS perinatal_deaths_0_7_days,
                NULL AS neonatal_deaths_0_28_days,
                NULL AS maternal_deaths_10_14_years,
                NULL AS maternal_deaths_15_19_years,
                NULL AS maternal_deaths_20_24_years,
                NULL AS maternal_deaths_25_above_years,
                NULL AS maternal_deaths_audited_within_7_days,
                NULL AS neonatal_deaths_audited_within_7_days,

                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!6653=228!!\", 1, NULL) AS ante_partum_haemorrage,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!6653=49!!\", 1, NULL) AS post_partum_haemorrage,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!6653=6457!!\", 1, NULL) AS eclampsia,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!6653=6458!!\", 1, NULL) AS ruptured_uterus,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!6653=6651!!\", 1, NULL) AS obstructed_labour,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!6653=1473!!\", 1, NULL) AS sepsis,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!6653=10424!!\", 1, NULL) AS fgm_delivery_complications,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!1573=2375!!\", 1, NULL) AS neonatal_deaths_sepsis,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!1573=6648!!\", 1, NULL) AS neonatal_deaths_prematurity,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!1573=12156!!\", 1, NULL) AS neonatal_deaths_asphyxia,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!12089=1065!!\", 1, NULL) AS kangaroo_mother_care,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!6749=1275!!\", 1, NULL) AS referrals_from_other_health_facility,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!10927=7308!!\", 1, NULL) AS referrals_to_other_health_facility,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!6749=1862!!\", 1, NULL) AS referrals_from_community,
                IF(t1.encounter_type IN (196,269,273,274) AND t1.obs REGEXP \"!!10927=9613!!\", 1, NULL) AS referrals_to_community,

                -- --------------------------------------------------------
                -- Cervical Cancer Screening (encounter type 69)
                -- Age bands: <25, 25-49, 50+ at the encounter.
                -- NOTE (upstream quirks preserved): hpv_test_50_above uses
                -- age > 50; positive_hpv_50_above, cryotherapy_50_above,
                -- leep_50_above and hiv_positive_..._50_above use >= 25
                -- upstream (they also capture the 25-49 band).
                -- --------------------------------------------------------
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!10402=9434!!|!!10402=2322!!|!!10402=10420!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 25, 1, NULL) AS via_villi_hpv_less_25,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!10402=9434!!|!!10402=2322!!|!!10402=10420!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 25 AND 49, 1, NULL) AS via_villi_hpv_25_49,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!10402=9434!!|!!10402=2322!!|!!10402=10420!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 50, 1, NULL) AS via_villi_hpv_50_above,

                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!10402=885!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 25, 1, NULL) AS pap_smear_less_25,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!10402=885!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 25 AND 49, 1, NULL) AS pap_smear_25_49,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!10402=885!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 50, 1, NULL) AS pap_smear_50_above,

                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!10402=2322!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 25, 1, NULL) AS hpv_test_less_25,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!10402=2322!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 25 AND 49, 1, NULL) AS hpv_test_25_49,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!10402=2322!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) > 50, 1, NULL) AS hpv_test_50_above,

                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!9434=703!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 25, 1, NULL) AS positive_via_villi_less_25,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!9434=703!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 25 AND 49, 1, NULL) AS positive_via_villi_25_49,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!9434=703!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 50, 1, NULL) AS positive_via_villi_50_above,

                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!7423=7417!!|!!7423=7418!!|!!7423=7419!!|!!7423=7421!!|!!7423=7420!!|!!7423=10055!!|!!7423=7422!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 25, 1, NULL) AS positive_cytology_less_25,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!7423=7417!!|!!7423=7418!!|!!7423=7419!!|!!7423=7421!!|!!7423=7420!!|!!7423=10055!!|!!7423=7422!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 25 AND 49, 1, NULL) AS positive_cytology_25_49,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!7423=7417!!|!!7423=7418!!|!!7423=7419!!|!!7423=7421!!|!!7423=7420!!|!!7423=10055!!|!!7423=7422!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 50, 1, NULL) AS positive_cytology_50_above,

                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!2322=703!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 25, 1, NULL) AS positive_hpv_less_25,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!2322=703!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 25 AND 49, 1, NULL) AS positive_hpv_25_49,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!2322=703!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 25, 1, NULL) AS positive_hpv_50_above,

                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!9434=6971!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 25, 1, NULL) AS suspicious_cancer_lessions_less_25,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!9434=6971!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 25 AND 49, 1, NULL) AS suspicious_cancer_lessions_25_49,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!9434=6971!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 50, 1, NULL) AS suspicious_cancer_lessions_50_above,

                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!10380=7466!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 25, 1, NULL) AS cryotherapy_treatment_less_25,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!10380=7466!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 25 AND 49, 1, NULL) AS cryotherapy_treatment_25_49,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!10380=7466!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 25, 1, NULL) AS cryotherapy_treatment_50_above,

                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!10380=7147!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 25, 1, NULL) AS leep_treatment_less_25,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!10380=7147!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 25 AND 49, 1, NULL) AS leep_treatment_25_49,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!10380=7147!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 25, 1, NULL) AS leep_treatment_50_above,

                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!6709=703!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 25, 1, NULL) AS hiv_positive_screened_cervical_cancer_less_25,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!6709=703!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 25 AND 49, 1, NULL) AS hiv_positive_screened_cervical_cancer_25_49,
                IF(t1.encounter_type = 69 AND t1.obs REGEXP \"!!6709=703!!\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 25, 1, NULL) AS hiv_positive_screened_cervical_cancer_50_above,

                -- --------------------------------------------------------
                -- Family Planning (encounter type 179)
                -- new/revisit split comes from concept 1724 (10647 = new,
                -- 2345 = revisit) recorded on the same encounter; the
                -- upstream self-join collapses into AND-ed REGEXPs.
                -- --------------------------------------------------------
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!8355=1065!!\" AND t1.obs REGEXP \"!!1724=10647!!\", 1, 0) AS first_users_contraceptive_new,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!8355=1065!!\" AND t1.obs REGEXP \"!!1724=2345!!\", 1, 0) AS first_users_contraceptive_revisit,

                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=6217!!\" AND t1.obs REGEXP \"!!1724=10647!!\", 1, 0) AS pills_progestine_new,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=6217!!\" AND t1.obs REGEXP \"!!1724=2345!!\", 1, 0) AS pills_progestine_revisit,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=6218!!\" AND t1.obs REGEXP \"!!1724=10647!!\", 1, 0) AS pills_combined_oral_contraceptive_new,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=6218!!\" AND t1.obs REGEXP \"!!1724=2345!!\", 1, 0) AS pills_combined_oral_contraceptive_revisit,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=6725!!\" AND t1.obs REGEXP \"!!1724=10647!!\", 1, 0) AS emergency_contraceptive_pill_new,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=6725!!\" AND t1.obs REGEXP \"!!1724=2345!!\", 1, 0) AS emergency_contraceptive_pill_revisit,

                -- TODO upstream: no expression in familyPlanningBase
                NULL AS fp_injections_dmpa_im,
                NULL AS fp_injections_dmpa_sc,

                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!7495=6718!!\" AND t1.obs REGEXP \"!!1724=10647!!\", 1, 0) AS male_condoms_new,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!7495=6718!!\" AND t1.obs REGEXP \"!!1724=2345!!\", 1, 0) AS male_condoms_revisit,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!7495=6717!!\" AND t1.obs REGEXP \"!!1724=10647!!\", 1, 0) AS female_condoms_new,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!7495=6717!!\" AND t1.obs REGEXP \"!!1724=2345!!\", 1, 0) AS female_condoms_revisit,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=190!!\" AND t1.obs REGEXP \"!!1724=10647!!\", 1, 0) AS male_and_female_condoms_new,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=190!!\" AND t1.obs REGEXP \"!!1724=2345!!\", 1, 0) AS male_and_female_condoms_revisit,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!15171=1065!!\" AND t1.obs REGEXP \"!!1724=10647!!\", 1, 0) AS counselled_for_natural_family_planning_new,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!15171=1065!!\" AND t1.obs REGEXP \"!!1724=2345!!\", 1, 0) AS counselled_for_natural_family_planning_revisit,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!10536=1065!!\" AND t1.obs REGEXP \"!!1724=10647!!\", 1, 0) AS cycle_beads_new,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!10536=1065!!\" AND t1.obs REGEXP \"!!1724=2345!!\", 1, 0) AS cycle_beads_revisit,

                -- TODO upstream: no expression in familyPlanningBase
                NULL AS implants_insertion_1_rod,
                NULL AS implants_insertion_2_rod,

                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=9735!!\", 1, 0) AS iucd_hormonal,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=5275!!\", 1, 0) AS iucd_non_hormonal,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=5276!!\", 1, 0) AS surgical_contraception_btl,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=6701!!\", 1, 0) AS vasectomy,

                -- NOTE (upstream quirk preserved): iucd_removals and
                -- implants_removal have identical expressions upstream.
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!10442=10535!!\", 1, 0) AS iucd_removals,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!10442=10535!!\", 1, 0) AS implants_removal,

                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 10 AND 14 AND t1.obs REGEXP \"!!1724=10647!!\", 1, 0) AS fp_adolescent_10_14_new,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 10 AND 14 AND t1.obs REGEXP \"!!1724=2345!!\", 1, 0) AS fp_adolescent_10_14_revisit,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 15 AND 19 AND t1.obs REGEXP \"!!1724=10647!!\", 1, 0) AS fp_adolescent_15_19_new,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 15 AND 19 AND t1.obs REGEXP \"!!1724=2345!!\", 1, 0) AS fp_adolescent_15_19_revisit,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 20 AND 24 AND t1.obs REGEXP \"!!1724=10647!!\", 1, 0) AS fp_adolescent_20_24_new,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) BETWEEN 20 AND 24 AND t1.obs REGEXP \"!!1724=2345!!\", 1, 0) AS fp_adolescent_20_24_revisit,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 25 AND t1.obs REGEXP \"!!1724=10647!!\", 1, 0) AS fp_adolescent_25_plus_new,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!374=\" AND TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) >= 25 AND t1.obs REGEXP \"!!1724=2345!!\", 1, 0) AS fp_adolescent_25_plus_revisit,

                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!10448=10438!!\" AND t1.obs REGEXP \"!!1724=10647!!\", 1, 0) AS post_partum_fp_48_hours_new,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!10448=10438!!\" AND t1.obs REGEXP \"!!1724=2345!!\", 1, 0) AS post_partum_fp_48_hours_revisit,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!10448=10440!!\" AND t1.obs REGEXP \"!!1724=10647!!\", 1, 0) AS post_partum_fp_4_6_weeks_new,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!10448=10440!!\" AND t1.obs REGEXP \"!!1724=2345!!\", 1, 0) AS post_partum_fp_4_6_weeks_revisit,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!10448=5277!!\" AND t1.obs REGEXP \"!!1724=10647!!\", 1, 0) AS post_abortion_fp_new,
                IF(t1.encounter_type = 179 AND t1.obs REGEXP \"!!10448=5277!!\" AND t1.obs REGEXP \"!!1724=2345!!\", 1, 0) AS post_abortion_fp_revisit,

                -- --------------------------------------------------------
                -- PNC (encounter types 266, 267)
                -- --------------------------------------------------------
                IF(t1.encounter_type = 266, 1, 0) AS pnc_new_clients,
                IF(t1.encounter_type = 267, 1, 0) AS pnc_revisit_clients,
                IF(t1.encounter_type IN (266,267) AND t1.obs REGEXP \"!!6681=1065!!\", 1, 0) AS women_couselled_on_post_partum_fp,
                IF(t1.encounter_type IN (266,267) AND t1.obs REGEXP \"!!7240=\" AND t1.obs NOT REGEXP \"!!7240=1107!!\", 1, 0) AS women_received_post_partum_fp,
                IF(t1.encounter_type IN (266,267) AND t1.obs REGEXP \"!!10448=10438!!\", 1, 0) AS mothers_post_partum_care_48_hours,
                IF(t1.encounter_type IN (266,267) AND t1.obs REGEXP \"!!10448=10439!!\", 1, 0) AS mothers_post_partum_care_3_6_weeks,
                IF(t1.encounter_type IN (266,267) AND t1.obs REGEXP \"!!10448=10437!!\", 1, 0) AS mothers_post_partum_care_after_6_weeks,
                IF(t1.encounter_type IN (266,267) AND t1.obs REGEXP \"!!12093=10438!!\", 1, 0) AS infants_post_partum_care_48_hours,
                IF(t1.encounter_type IN (266,267) AND t1.obs REGEXP \"!!12093=10439!!\", 1, 0) AS infants_post_partum_care_3_6_weeks,
                IF(t1.encounter_type IN (266,267) AND t1.obs REGEXP \"!!12093=10437!!\", 1, 0) AS infants_post_partum_care_after_6_weeks,
                IF(t1.encounter_type IN (266,267) AND t1.obs REGEXP \"!!10473=1218!!|!!10473=10474!!|!!10473=12453!!\", 1, 0) AS fistula_cases,

                -- Renamed from referrals_from_community (collides with the
                -- Maternity column of the same name).
                IF(t1.encounter_type IN (266,267) AND t1.obs REGEXP \"!!6749=1862!!\", 1, 0) AS pnc_referrals_from_community,

                -- --------------------------------------------------------
                -- Psychosocial / Social Work (encounter types 1, 2)
                -- NOTE (upstream quirk preserved): psycho_social_counselling
                -- uses the TB-screening logic (concept 6174 != 1107).
                -- --------------------------------------------------------
                IF(t1.encounter_type IN (1,2) AND t1.obs REGEXP \"!!6174=\" AND t1.obs NOT REGEXP \"!!6174=1107!!\", 1, NULL) AS psycho_social_counselling,
                IF(t1.encounter_type IN (1,2) AND t1.obs REGEXP \"!!10835=1065!!\", 1, NULL) AS alcohol_and_drug_abuse,
                IF(t1.encounter_type IN (1,2) AND t1.obs REGEXP \"!!1629=1628!!|!!1629=1627!!\", 1, NULL) AS mental_illness,

                -- TODO upstream: no expression in socialWorkBase
                NULL AS adolescent_issues,

                IF(t1.encounter_type IN (1,2) AND t1.obs REGEXP \"!!11866=1065!!|!!12054=1065!!|!!11865=1065!!\", 1, NULL) AS psycho_social_economic_assessment,
                IF(t1.encounter_type IN (1,2) AND t1.obs REGEXP \"!!10085=1065!!\", 1, NULL) AS social_investigations,

                -- TODO upstream: no expression in socialWorkBase
                NULL AS psycho_social_rehabilitation,
                NULL AS outreach_services,

                IF(t1.encounter_type IN (1,2) AND t1.obs REGEXP \"!!1272=5489!!\", 1, NULL) AS mental_health_referral,

                -- --------------------------------------------------------
                -- TB Screening (encounter types 1, 2, 264, 265, 266, 267)
                -- --------------------------------------------------------
                IF(t1.encounter_type IN (1,2,264,265,266,267) AND t1.obs REGEXP \"!!6174=\" AND t1.obs NOT REGEXP \"!!6174=1107!!\", 1, NULL) AS total_screened_for_tb,
                IF(t1.encounter_type IN (1,2,264,265,266,267) AND t1.obs REGEXP \"!!8292=6971!!\", 1, NULL) AS total_presumptive_tb_cases,
                IF(t1.encounter_type IN (1,2,264,265,266,267) AND t1.obs REGEXP \"!!1268=1257!!|!!1268=1259!!|!!1268=981!!|!!1268=1850!!|!!1268=2161!!|!!1268=2160!!|!!1268=1406!!|!!1268=1849!!|!!1268=8352!!\", 1, NULL) AS already_on_tb_treatment,

                -- TODO upstream: no expression in tbScreeningBase
                NULL AS not_screened_for_tb,

                -- --------------------------------------------------------
                -- Child Welfare / Nutrition - chanis
                -- (encounter types 4, 110, 167, 313)
                -- Age bands are in MONTHS at the encounter (except the
                -- under-five diarrhoea/pneumonia/deaths/disability/
                -- development rows, which use YEARS).
                -- Weight status is concept 10213; upstream uses concept
                -- 1343 (MUAC) for the height-for-age z-score rows.
                -- --------------------------------------------------------
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) <= 6 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=1115!!\", 1, 0) AS normal_weight_0_6_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) <= 6 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=1115!!\", 1, 0) AS normal_weight_0_6_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) <= 6 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=10277!!|!!10213=9472!!\", 1, 0) AS underweight_0_6_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 6 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=10277!!|!!10213=9472!!\", 1, 0) AS underweight_0_6_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 6 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=9471!!\", 1, NULL) AS severely_underweight_0_6_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 6 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=9471!!\", 1, NULL) AS severely_underweight_0_6_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 6 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=6895!!\", 1, NULL) AS overweight_0_6_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 6 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=6895!!\", 1, NULL) AS overweight_0_6_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 6 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=7764!!\", 1, NULL) AS obese_0_6_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) < 6 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=7764!!\", 1, NULL) AS obese_0_6_months_female,

                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 23 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=1115!!\", 1, NULL) AS normal_weight_6_23_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 23 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=1115!!\", 1, NULL) AS normal_weight_6_23_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 23 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=10277!!|!!10213=9472!!\", 1, NULL) AS underweight_6_23_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 23 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=10277!!|!!10213=9472!!\", 1, NULL) AS underweight_6_23_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 23 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=9471!!\", 1, NULL) AS severely_underweight_6_23_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 23 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=9471!!\", 1, NULL) AS severely_underweight_6_23_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 23 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=6895!!\", 1, NULL) AS overweight_6_23_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 23 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=6895!!\", 1, NULL) AS overweight_6_23_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 23 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=7764!!\", 1, NULL) AS obese_6_23_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 23 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=7764!!\", 1, NULL) AS obese_6_23_months_female,

                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 24 AND 59 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=1115!!\", 1, NULL) AS normal_weight_24_59_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 24 AND 59 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=1115!!\", 1, NULL) AS normal_weight_24_59_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 24 AND 59 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=10277!!|!!10213=9472!!\", 1, NULL) AS underweight_24_59_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 24 AND 59 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=10277!!|!!10213=9472!!\", 1, NULL) AS underweight_24_59_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 24 AND 59 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=9471!!\", 1, NULL) AS severely_underweight_24_59_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 24 AND 59 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=9471!!\", 1, NULL) AS severely_underweight_24_59_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 24 AND 59 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=6895!!\", 1, NULL) AS overweight_24_59_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 24 AND 59 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=6895!!\", 1, NULL) AS overweight_24_59_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 24 AND 59 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=7764!!\", 1, NULL) AS obese_24_59_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 24 AND 59 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10213=7764!!\", 1, NULL) AS obese_24_59_months_female,

                -- MUAC (concept 1343, age 6-59 months).
                -- NOTE (upstream quirk preserved): normal_male uses
                -- >= 12.5 while normal_female uses >= 125.
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 59 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) >= 12.5, 1, NULL) AS muac_6_59_months_normal_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 59 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) >= 125, 1, NULL) AS muac_6_59_months_normal_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 59 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) > 115 AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) < 125, 1, NULL) AS muac_6_59_months_moderate_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 59 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) > 115 AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) < 125, 1, NULL) AS muac_6_59_months_moderate_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 59 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) < 115, 1, NULL) AS muac_6_59_months_severe_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 59 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) < 115, 1, NULL) AS muac_6_59_months_severe_female,

                -- Height-for-age (upstream reads the z-score from concept
                -- 1343; values can be negative).
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 0 AND 6 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) >= -2, 1, NULL) AS height_for_age_0_6_months_normal_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 0 AND 6 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) >= -2, 1, NULL) AS height_for_age_0_6_months_normal_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 0 AND 6 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) > -3 AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) < -2, 1, NULL) AS stunted_0_6_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 0 AND 6 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) > -3 AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) < -2, 1, NULL) AS stunted_0_6_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 0 AND 6 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) < -3, 1, NULL) AS severely_stunted_0_6_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 0 AND 6 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) < -3, 1, NULL) AS severely_stunted_0_6_months_female,

                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 23 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) >= -2, 1, NULL) AS normal_height_for_age_6_23_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 23 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) >= -2, 1, NULL) AS normal_height_for_age_6_23_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 23 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) > -3 AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) < -2, 1, NULL) AS stunted_6_23_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 23 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) > -3 AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) < -2, 1, NULL) AS stunted_6_23_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 23 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) < -3, 1, NULL) AS severely_stunted_6_23_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 23 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) < -3, 1, NULL) AS severely_stunted_6_23_months_female,

                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 24 AND 59 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) >= -2, 1, NULL) AS normal_height_for_age_24_59_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 24 AND 59 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) >= -2, 1, NULL) AS normal_height_for_age_24_59_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 24 AND 59 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) > -3 AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) < -2, 1, NULL) AS stunted_24_59_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 24 AND 59 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) > -3 AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) < -2, 1, NULL) AS stunted_24_59_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 24 AND 59 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) < -3, 1, NULL) AS severely_stunted_24_59_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 24 AND 59 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1343=\" AND SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) REGEXP \"^-?[0-9]+([.][0-9]+)?$\" AND CAST(SUBSTRING_INDEX(getValues(t1.obs, 1343), \" ## \", 1) AS DECIMAL(10,4)) < -3, 1, NULL) AS severely_stunted_24_59_months_female,

                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 0 AND 59 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1838=7850!!\", 1, NULL) AS new_visits_0_59_months_attending_cwc_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 0 AND 59 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1838=7850!!\", 1, NULL) AS new_visits_0_59_months_attending_cwc_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 0 AND 59 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10218=\", 1, NULL) AS kwashiokor_0_59_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 0 AND 59 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10218=\", 1, NULL) AS kwashiokor_0_59_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 0 AND 59 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10219=\", 1, NULL) AS marasmus_0_59_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 0 AND 59 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!10219=\", 1, NULL) AS marasmus_0_59_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 0 AND 59 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!2187=1065!!\", 1, NULL) AS faltering_growth_0_59_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 0 AND 59 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!2187=1065!!\", 1, NULL) AS faltering_growth_0_59_months_female,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 0 AND 6 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!985=5526!!\", 1, NULL) AS exclusive_breastfeeding_0_6_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 0 AND 6 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!985=5526!!\", 1, NULL) AS exclusive_breastfeeding_0_6_months_female,

                -- TODO upstream: no expression in chanisBase
                NULL AS dewormed_12_59_months_male,
                NULL AS dewormed_12_59_months_female,

                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 23 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!9487=9488!!\", 1, NULL) AS mnps_supplimentation_6_23_months_male,
                IF(TIMESTAMPDIFF(MONTH, t2.birthdate, t1.encounter_datetime) BETWEEN 6 AND 23 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!9487=9488!!\", 1, NULL) AS mnps_supplimentation_6_23_months_female,

                IF(TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 5 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!7457=\", 1, NULL) AS diarhoea_severe_dehydration_male,
                IF(TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 5 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!7457=\", 1, NULL) AS diarhoea_severe_dehydration_female,

                -- TODO upstream: no expression in chanisBase
                NULL AS diarhoea_some_dehydration_male,
                NULL AS diarhoea_some_dehydration_female,

                IF(TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 5 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!13128=\", 1, NULL) AS diarhoea_no_dehydration_male,
                IF(TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 5 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!13128=\", 1, NULL) AS diarhoea_no_dehydration_female,
                IF(TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 5 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!7586=|!!351=\", 1, NULL) AS diarhoea_treated_with_ors_zinc_dehydration_male,
                IF(TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 5 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!7586=|!!351=\", 1, NULL) AS diarhoea_treated_with_ors_zinc_dehydration_female,

                -- NOTE (upstream quirk preserved): under_five_pneumomia
                -- uses the same expression as mnps_supplimentation
                -- (concept 9487 = 9488).
                IF(TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 5 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!9487=9488!!\", 1, NULL) AS under_five_pneumomia_male,
                IF(TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 5 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!9487=9488!!\", 1, NULL) AS under_five_pneumomia_female,
                IF(TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 5 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!9082=159!!\", 1, NULL) AS under_five_deaths_male,
                IF(TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 5 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!9082=159!!\", 1, NULL) AS under_five_deaths_female,
                IF(TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 5 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!12248=7900!!\", 1, NULL) AS under_five_disability_male,
                IF(TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 5 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!12248=7900!!\", 1, NULL) AS under_five_disability_female,
                IF(TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 5 AND t2.gender = \"M\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1189=6022!!\", 1, NULL) AS under_five_delayed_development_male,
                IF(TIMESTAMPDIFF(YEAR, t2.birthdate, t1.encounter_datetime) < 5 AND t2.gender = \"F\" AND t1.encounter_type IN (4,110,167,313) AND t1.obs REGEXP \"!!1189=6022!!\", 1, NULL) AS under_five_delayed_development_female

            FROM flat_obs t1
            JOIN temp_queue_table t3 USING (person_id)
            JOIN amrs.person t2 USING (person_id)
            WHERE t1.encounter_type IN (1,2,4,56,57,69,110,133,134,167,179,196,264,265,266,267,269,273,274,313)
            )'
        );
        PREPARE s1 FROM @dyn_sql; EXECUTE s1; DEALLOCATE PREPARE s1;

        SET @dyn_sql = CONCAT(
            'INSERT INTO ', @write_table,
            ' (person_id, uuid, encounter_id, encounter_datetime, encounter_type, location_id, birth_date, gender,
               new_anc_clients, revisit_anc_clients, first_ipt_dose, second_ipt_dose, third_ipt_dose, Hb_less_11g,
               first_anc_contact_12_or_less_weeks, completing_4_anc_visits, completing_8_anc_contact,
               llins_children_less_than_one, llins_anc_clients, anc_clients_tested_syphilis, syphilis_positive,
               breast_exam, adolesc_10_14, adolesc_15_19, youth_20_24, iron, folic, iron_and_folate, fgm_complications,
               sgbv_total_survivors_0_9, sgbv_total_survivors_10_17, sgbv_total_survivors_18_49, sgbv_total_survivors_50_plus,
               sgbv_within_72_hours_0_9, sgbv_within_72_hours_10_17, sgbv_within_72_hours_18_49, sgbv_within_72_hours_50_above,
               sgbv_initiated_on_pep_0_9, sgbv_initiated_on_pep_10_17, sgbv_initiated_on_pep_18_49, sgbv_initiated_on_pep_50_above,
               sgbv_completed_pep_0_9, sgbv_completed_pep_10_17, sgbv_completed_pep_18_49, sgbv_completed_pep_50_above,
               sgbv_seroconverting_0_9, sgbv_seroconverting_10_17, sgbv_seroconverting_18_49, sgbv_seroconverting_50_above,
               sgbv_eligible_for_ecp_0_9, sgbv_eligible_for_ecp_10_17, sgbv_eligible_for_ecp_18_49, sgbv_eligible_for_ecp_50_above,
               sgbv_receiving_ecp_0_9, sgbv_receiving_ecp_10_17, sgbv_receiving_ecp_18_49, sgbv_receiving_ecp_50_above,
               sgbv_pregnant_0_9, sgbv_pregnant_10_17, sgbv_pregnant_18_49, sgbv_pregnant_50_above,
               sgbv_rc_seen_0_9, sgbv_rc_seen_10_17, sgbv_rc_seen_18_49, sgbv_rc_seen_50_above,
               sgbv_with_disability_0_9, sgbv_with_disability_10_17, sgbv_with_disability_18_49, sgbv_with_disability_50_above,
               normal_deliveries, caesarian_sections, breach_delivery, assisted_vaginal_delivery,
               oxytocin_uterotonic, carbatocin_uterotonic, live_birth, low_birth_weight, low_apgar_score,
               birth_with_deformity, chlorhexidine_applied, vitamin_k, tetracycline_given, pre_term_babies,
               discharge_alive, bf_within_1_hour, deliveries_from_positive_women, fresh_still_birth, macerated_still_birth,
               perinatal_deaths_0_7_days, neonatal_deaths_0_28_days,
               maternal_deaths_10_14_years, maternal_deaths_15_19_years, maternal_deaths_20_24_years, maternal_deaths_25_above_years,
               maternal_deaths_audited_within_7_days, neonatal_deaths_audited_within_7_days,
               ante_partum_haemorrage, post_partum_haemorrage, eclampsia, ruptured_uterus, obstructed_labour, sepsis,
               fgm_delivery_complications, neonatal_deaths_sepsis, neonatal_deaths_prematurity, neonatal_deaths_asphyxia,
               kangaroo_mother_care, referrals_from_other_health_facility, referrals_to_other_health_facility,
               referrals_from_community, referrals_to_community,
               via_villi_hpv_less_25, via_villi_hpv_25_49, via_villi_hpv_50_above,
               pap_smear_less_25, pap_smear_25_49, pap_smear_50_above,
               hpv_test_less_25, hpv_test_25_49, hpv_test_50_above,
               positive_via_villi_less_25, positive_via_villi_25_49, positive_via_villi_50_above,
               positive_cytology_less_25, positive_cytology_25_49, positive_cytology_50_above,
               positive_hpv_less_25, positive_hpv_25_49, positive_hpv_50_above,
               suspicious_cancer_lessions_less_25, suspicious_cancer_lessions_25_49, suspicious_cancer_lessions_50_above,
               cryotherapy_treatment_less_25, cryotherapy_treatment_25_49, cryotherapy_treatment_50_above,
               leep_treatment_less_25, leep_treatment_25_49, leep_treatment_50_above,
               hiv_positive_screened_cervical_cancer_less_25, hiv_positive_screened_cervical_cancer_25_49,
               hiv_positive_screened_cervical_cancer_50_above,
               first_users_contraceptive_new, first_users_contraceptive_revisit,
               pills_progestine_new, pills_progestine_revisit,
               pills_combined_oral_contraceptive_new, pills_combined_oral_contraceptive_revisit,
               emergency_contraceptive_pill_new, emergency_contraceptive_pill_revisit,
               fp_injections_dmpa_im, fp_injections_dmpa_sc,
               male_condoms_new, male_condoms_revisit, female_condoms_new, female_condoms_revisit,
               male_and_female_condoms_new, male_and_female_condoms_revisit,
               counselled_for_natural_family_planning_new, counselled_for_natural_family_planning_revisit,
               cycle_beads_new, cycle_beads_revisit,
               implants_insertion_1_rod, implants_insertion_2_rod,
               iucd_hormonal, iucd_non_hormonal, surgical_contraception_btl, vasectomy, iucd_removals, implants_removal,
               fp_adolescent_10_14_new, fp_adolescent_10_14_revisit,
               fp_adolescent_15_19_new, fp_adolescent_15_19_revisit,
               fp_adolescent_20_24_new, fp_adolescent_20_24_revisit,
               fp_adolescent_25_plus_new, fp_adolescent_25_plus_revisit,
               post_partum_fp_48_hours_new, post_partum_fp_48_hours_revisit,
               post_partum_fp_4_6_weeks_new, post_partum_fp_4_6_weeks_revisit,
               post_abortion_fp_new, post_abortion_fp_revisit,
               pnc_new_clients, pnc_revisit_clients, women_couselled_on_post_partum_fp, women_received_post_partum_fp,
               mothers_post_partum_care_48_hours, mothers_post_partum_care_3_6_weeks, mothers_post_partum_care_after_6_weeks,
               infants_post_partum_care_48_hours, infants_post_partum_care_3_6_weeks, infants_post_partum_care_after_6_weeks,
               fistula_cases, pnc_referrals_from_community,
               psycho_social_counselling, alcohol_and_drug_abuse, mental_illness, adolescent_issues,
               psycho_social_economic_assessment, social_investigations, psycho_social_rehabilitation,
               outreach_services, mental_health_referral,
               total_screened_for_tb, total_presumptive_tb_cases, already_on_tb_treatment, not_screened_for_tb,
               normal_weight_0_6_months_male, normal_weight_0_6_months_female,
               underweight_0_6_months_male, underweight_0_6_months_female,
               severely_underweight_0_6_months_male, severely_underweight_0_6_months_female,
               overweight_0_6_months_male, overweight_0_6_months_female,
               obese_0_6_months_male, obese_0_6_months_female,
               normal_weight_6_23_months_male, normal_weight_6_23_months_female,
               underweight_6_23_months_male, underweight_6_23_months_female,
               severely_underweight_6_23_months_male, severely_underweight_6_23_months_female,
               overweight_6_23_months_male, overweight_6_23_months_female,
               obese_6_23_months_male, obese_6_23_months_female,
               normal_weight_24_59_months_male, normal_weight_24_59_months_female,
               underweight_24_59_months_male, underweight_24_59_months_female,
               severely_underweight_24_59_months_male, severely_underweight_24_59_months_female,
               overweight_24_59_months_male, overweight_24_59_months_female,
               obese_24_59_months_male, obese_24_59_months_female,
               muac_6_59_months_normal_male, muac_6_59_months_normal_female,
               muac_6_59_months_moderate_male, muac_6_59_months_moderate_female,
               muac_6_59_months_severe_male, muac_6_59_months_severe_female,
               height_for_age_0_6_months_normal_male, height_for_age_0_6_months_normal_female,
               stunted_0_6_months_male, stunted_0_6_months_female,
               severely_stunted_0_6_months_male, severely_stunted_0_6_months_female,
               normal_height_for_age_6_23_months_male, normal_height_for_age_6_23_months_female,
               stunted_6_23_months_male, stunted_6_23_months_female,
               severely_stunted_6_23_months_male, severely_stunted_6_23_months_female,
               normal_height_for_age_24_59_months_male, normal_height_for_age_24_59_months_female,
               stunted_24_59_months_male, stunted_24_59_months_female,
               severely_stunted_24_59_months_male, severely_stunted_24_59_months_female,
               new_visits_0_59_months_attending_cwc_male, new_visits_0_59_months_attending_cwc_female,
               kwashiokor_0_59_months_male, kwashiokor_0_59_months_female,
               marasmus_0_59_months_male, marasmus_0_59_months_female,
               faltering_growth_0_59_months_male, faltering_growth_0_59_months_female,
               exclusive_breastfeeding_0_6_months_male, exclusive_breastfeeding_0_6_months_female,
               dewormed_12_59_months_male, dewormed_12_59_months_female,
               mnps_supplimentation_6_23_months_male, mnps_supplimentation_6_23_months_female,
               diarhoea_severe_dehydration_male, diarhoea_severe_dehydration_female,
               diarhoea_some_dehydration_male, diarhoea_some_dehydration_female,
               diarhoea_no_dehydration_male, diarhoea_no_dehydration_female,
               diarhoea_treated_with_ors_zinc_dehydration_male, diarhoea_treated_with_ors_zinc_dehydration_female,
               under_five_pneumomia_male, under_five_pneumomia_female,
               under_five_deaths_male, under_five_deaths_female,
               under_five_disability_male, under_five_disability_female,
               under_five_delayed_development_male, under_five_delayed_development_female)
            SELECT
                person_id, uuid, encounter_id, encounter_datetime, encounter_type, location_id, birth_date, gender,
                new_anc_clients, revisit_anc_clients, first_ipt_dose, second_ipt_dose, third_ipt_dose, Hb_less_11g,
                first_anc_contact_12_or_less_weeks, completing_4_anc_visits, completing_8_anc_contact,
                llins_children_less_than_one, llins_anc_clients, anc_clients_tested_syphilis, syphilis_positive,
                breast_exam, adolesc_10_14, adolesc_15_19, youth_20_24, iron, folic, iron_and_folate, fgm_complications,
                sgbv_total_survivors_0_9, sgbv_total_survivors_10_17, sgbv_total_survivors_18_49, sgbv_total_survivors_50_plus,
                sgbv_within_72_hours_0_9, sgbv_within_72_hours_10_17, sgbv_within_72_hours_18_49, sgbv_within_72_hours_50_above,
                sgbv_initiated_on_pep_0_9, sgbv_initiated_on_pep_10_17, sgbv_initiated_on_pep_18_49, sgbv_initiated_on_pep_50_above,
                sgbv_completed_pep_0_9, sgbv_completed_pep_10_17, sgbv_completed_pep_18_49, sgbv_completed_pep_50_above,
                sgbv_seroconverting_0_9, sgbv_seroconverting_10_17, sgbv_seroconverting_18_49, sgbv_seroconverting_50_above,
                sgbv_eligible_for_ecp_0_9, sgbv_eligible_for_ecp_10_17, sgbv_eligible_for_ecp_18_49, sgbv_eligible_for_ecp_50_above,
                sgbv_receiving_ecp_0_9, sgbv_receiving_ecp_10_17, sgbv_receiving_ecp_18_49, sgbv_receiving_ecp_50_above,
                sgbv_pregnant_0_9, sgbv_pregnant_10_17, sgbv_pregnant_18_49, sgbv_pregnant_50_above,
                sgbv_rc_seen_0_9, sgbv_rc_seen_10_17, sgbv_rc_seen_18_49, sgbv_rc_seen_50_above,
                sgbv_with_disability_0_9, sgbv_with_disability_10_17, sgbv_with_disability_18_49, sgbv_with_disability_50_above,
                normal_deliveries, caesarian_sections, breach_delivery, assisted_vaginal_delivery,
                oxytocin_uterotonic, carbatocin_uterotonic, live_birth, low_birth_weight, low_apgar_score,
                birth_with_deformity, chlorhexidine_applied, vitamin_k, tetracycline_given, pre_term_babies,
                discharge_alive, bf_within_1_hour, deliveries_from_positive_women, fresh_still_birth, macerated_still_birth,
                perinatal_deaths_0_7_days, neonatal_deaths_0_28_days,
                maternal_deaths_10_14_years, maternal_deaths_15_19_years, maternal_deaths_20_24_years, maternal_deaths_25_above_years,
                maternal_deaths_audited_within_7_days, neonatal_deaths_audited_within_7_days,
                ante_partum_haemorrage, post_partum_haemorrage, eclampsia, ruptured_uterus, obstructed_labour, sepsis,
                fgm_delivery_complications, neonatal_deaths_sepsis, neonatal_deaths_prematurity, neonatal_deaths_asphyxia,
                kangaroo_mother_care, referrals_from_other_health_facility, referrals_to_other_health_facility,
                referrals_from_community, referrals_to_community,
                via_villi_hpv_less_25, via_villi_hpv_25_49, via_villi_hpv_50_above,
                pap_smear_less_25, pap_smear_25_49, pap_smear_50_above,
                hpv_test_less_25, hpv_test_25_49, hpv_test_50_above,
                positive_via_villi_less_25, positive_via_villi_25_49, positive_via_villi_50_above,
                positive_cytology_less_25, positive_cytology_25_49, positive_cytology_50_above,
                positive_hpv_less_25, positive_hpv_25_49, positive_hpv_50_above,
                suspicious_cancer_lessions_less_25, suspicious_cancer_lessions_25_49, suspicious_cancer_lessions_50_above,
                cryotherapy_treatment_less_25, cryotherapy_treatment_25_49, cryotherapy_treatment_50_above,
                leep_treatment_less_25, leep_treatment_25_49, leep_treatment_50_above,
                hiv_positive_screened_cervical_cancer_less_25, hiv_positive_screened_cervical_cancer_25_49,
                hiv_positive_screened_cervical_cancer_50_above,
                first_users_contraceptive_new, first_users_contraceptive_revisit,
                pills_progestine_new, pills_progestine_revisit,
                pills_combined_oral_contraceptive_new, pills_combined_oral_contraceptive_revisit,
                emergency_contraceptive_pill_new, emergency_contraceptive_pill_revisit,
                fp_injections_dmpa_im, fp_injections_dmpa_sc,
                male_condoms_new, male_condoms_revisit, female_condoms_new, female_condoms_revisit,
                male_and_female_condoms_new, male_and_female_condoms_revisit,
                counselled_for_natural_family_planning_new, counselled_for_natural_family_planning_revisit,
                cycle_beads_new, cycle_beads_revisit,
                implants_insertion_1_rod, implants_insertion_2_rod,
                iucd_hormonal, iucd_non_hormonal, surgical_contraception_btl, vasectomy, iucd_removals, implants_removal,
                fp_adolescent_10_14_new, fp_adolescent_10_14_revisit,
                fp_adolescent_15_19_new, fp_adolescent_15_19_revisit,
                fp_adolescent_20_24_new, fp_adolescent_20_24_revisit,
                fp_adolescent_25_plus_new, fp_adolescent_25_plus_revisit,
                post_partum_fp_48_hours_new, post_partum_fp_48_hours_revisit,
                post_partum_fp_4_6_weeks_new, post_partum_fp_4_6_weeks_revisit,
                post_abortion_fp_new, post_abortion_fp_revisit,
                pnc_new_clients, pnc_revisit_clients, women_couselled_on_post_partum_fp, women_received_post_partum_fp,
                mothers_post_partum_care_48_hours, mothers_post_partum_care_3_6_weeks, mothers_post_partum_care_after_6_weeks,
                infants_post_partum_care_48_hours, infants_post_partum_care_3_6_weeks, infants_post_partum_care_after_6_weeks,
                fistula_cases, pnc_referrals_from_community,
                psycho_social_counselling, alcohol_and_drug_abuse, mental_illness, adolescent_issues,
                psycho_social_economic_assessment, social_investigations, psycho_social_rehabilitation,
                outreach_services, mental_health_referral,
                total_screened_for_tb, total_presumptive_tb_cases, already_on_tb_treatment, not_screened_for_tb,
                normal_weight_0_6_months_male, normal_weight_0_6_months_female,
                underweight_0_6_months_male, underweight_0_6_months_female,
                severely_underweight_0_6_months_male, severely_underweight_0_6_months_female,
                overweight_0_6_months_male, overweight_0_6_months_female,
                obese_0_6_months_male, obese_0_6_months_female,
                normal_weight_6_23_months_male, normal_weight_6_23_months_female,
                underweight_6_23_months_male, underweight_6_23_months_female,
                severely_underweight_6_23_months_male, severely_underweight_6_23_months_female,
                overweight_6_23_months_male, overweight_6_23_months_female,
                obese_6_23_months_male, obese_6_23_months_female,
                normal_weight_24_59_months_male, normal_weight_24_59_months_female,
                underweight_24_59_months_male, underweight_24_59_months_female,
                severely_underweight_24_59_months_male, severely_underweight_24_59_months_female,
                overweight_24_59_months_male, overweight_24_59_months_female,
                obese_24_59_months_male, obese_24_59_months_female,
                muac_6_59_months_normal_male, muac_6_59_months_normal_female,
                muac_6_59_months_moderate_male, muac_6_59_months_moderate_female,
                muac_6_59_months_severe_male, muac_6_59_months_severe_female,
                height_for_age_0_6_months_normal_male, height_for_age_0_6_months_normal_female,
                stunted_0_6_months_male, stunted_0_6_months_female,
                severely_stunted_0_6_months_male, severely_stunted_0_6_months_female,
                normal_height_for_age_6_23_months_male, normal_height_for_age_6_23_months_female,
                stunted_6_23_months_male, stunted_6_23_months_female,
                severely_stunted_6_23_months_male, severely_stunted_6_23_months_female,
                normal_height_for_age_24_59_months_male, normal_height_for_age_24_59_months_female,
                stunted_24_59_months_male, stunted_24_59_months_female,
                severely_stunted_24_59_months_male, severely_stunted_24_59_months_female,
                new_visits_0_59_months_attending_cwc_male, new_visits_0_59_months_attending_cwc_female,
                kwashiokor_0_59_months_male, kwashiokor_0_59_months_female,
                marasmus_0_59_months_male, marasmus_0_59_months_female,
                faltering_growth_0_59_months_male, faltering_growth_0_59_months_female,
                exclusive_breastfeeding_0_6_months_male, exclusive_breastfeeding_0_6_months_female,
                dewormed_12_59_months_male, dewormed_12_59_months_female,
                mnps_supplimentation_6_23_months_male, mnps_supplimentation_6_23_months_female,
                diarhoea_severe_dehydration_male, diarhoea_severe_dehydration_female,
                diarhoea_some_dehydration_male, diarhoea_some_dehydration_female,
                diarhoea_no_dehydration_male, diarhoea_no_dehydration_female,
                diarhoea_treated_with_ors_zinc_dehydration_male, diarhoea_treated_with_ors_zinc_dehydration_female,
                under_five_pneumomia_male, under_five_pneumomia_female,
                under_five_deaths_male, under_five_deaths_female,
                under_five_disability_male, under_five_disability_female,
                under_five_delayed_development_male, under_five_delayed_development_female
            FROM flat_moh_711_report_0'
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
