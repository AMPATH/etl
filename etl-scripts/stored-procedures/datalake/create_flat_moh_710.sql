# DDL for the MOH 710 datalake tables.
# Run once (idempotent) before deploying / calling etl.generate_flat_moh_710_v1.
#
# - flat_moh_710_report: static mirror of the CREATE TABLE the procedure
#   issues dynamically; kept byte-identical so the procedure's
#   CREATE TABLE IF NOT EXISTS no-ops when this script has run first.
# - flat_moh_710_report_build_queue: master build queue consumed by
#   build mode ('CALL etl.generate_flat_moh_710_v1("build", ...)').
#   Nothing else creates it - populate it with the person_ids to
#   (re)build, then run the procedure in build mode.
# - flat_moh_710_report_sync_queue: also auto-created by the procedure;
#   defined here so the whole 710 table family lives in one file.
#
# Remaining dependencies (defined elsewhere, not duplicated here):
#   etl.flat_obs     -> etl-scripts/stored-procedures/datalake/generate_flat_obs.sql
#   etl.flat_log     -> etl-scripts/flat_tables/flat_log_v1.0.sql
#   etl.GetValues    -> etl-scripts/stored-procedures/datalake/getValues.sql
#   amrs.person / amrs.encounter -> OpenMRS source schema

create table if not exists flat_moh_710_report (

        date_created                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        person_id                      INT,
        uuid                           VARCHAR(100),
        encounter_id                   INT,
        encounter_datetime             DATETIME,
        encounter_type                 INT,
        location_id                    INT,
        birth_date                     DATE,
        gender                         VARCHAR(10),


        -- Immunization (encounter types 105, 106, 115, 195, 313)
        bcg_vaccine_age_less_than_1yr                   VARCHAR(5),
        bcg_vaccine_age_greater_than_1yr                VARCHAR(5),
        opv_vaccine_age_less_than_1yr                   VARCHAR(5),
        opv_vaccine_age_greater_than_1yr                VARCHAR(5),
        opv1_vaccine_age_less_than_1yr                  VARCHAR(5),
        opv1_vaccine_age_greater_than_1yr               VARCHAR(5),
        opv2_vaccine_age_less_than_1yr                  VARCHAR(5),
        opv2_vaccine_age_greater_than_1yr               VARCHAR(5),
        opv3_vaccine_age_less_than_1yr                  VARCHAR(5),
        opv3_vaccine_age_greater_than_1yr               VARCHAR(5),
        ipv1_vaccine_age_less_than_1yr                  VARCHAR(5),
        ipv1_vaccine_age_greater_than_1yr               VARCHAR(5),
        ipv2_vaccine_age_less_than_1yr                  VARCHAR(5),
        ipv2_vaccine_age_greater_than_1yr               VARCHAR(5),
        dpt_hep_vaccine1_age_less_than_1yr              VARCHAR(5),
        dpt_hep_vaccine1_age_greater_than_1yr           VARCHAR(5),
        dpt_hep_vaccine2_age_less_than_1yr              VARCHAR(5),
        dpt_hep_vaccine2_age_greater_than_1yr           VARCHAR(5),
        dpt_hep_vaccine3_age_less_than_1yr              VARCHAR(5),
        dpt_hep_vaccine3_age_greater_than_1yr           VARCHAR(5),
        pneumococal_vaccine1_age_less_than_1yr          VARCHAR(5),
        pneumococal_vaccine1_age_greater_than_1yr       VARCHAR(5),
        pneumococal_vaccine2_age_less_than_1yr          VARCHAR(5),
        pneumococal_vaccine2_age_greater_than_1yr       VARCHAR(5),
        pneumococal_vaccine3_age_less_than_1yr          VARCHAR(5),
        pneumococal_vaccine3_age_greater_than_1yr       VARCHAR(5),
        rotavirus_vaccine1_age_less_than_1yr            VARCHAR(5),
        rotavirus_vaccine1_age_greater_than_1yr         VARCHAR(5),
        rotavirus_vaccine2_age_less_than_1yr            VARCHAR(5),
        rotavirus_vaccine2_age_greater_than_1yr         VARCHAR(5),
        rotavirus_vaccine3_age_less_than_1yr            VARCHAR(5),
        rotavirus_vaccine3_age_greater_than_1yr         VARCHAR(5),
        vitaminA_vaccine_age_less_than_1yr              VARCHAR(5),
        vitaminA_vaccine_age_greater_than_1yr           VARCHAR(5),
        yellow_fever_vaccine_age_greater_than_1yr       VARCHAR(5),
        yellow_fever_vaccine_age_less_than_1yr          VARCHAR(5),
        measles_vaccine_age_greater_than_1yr            VARCHAR(5),
        measles_vaccine_age_less_than_1yr               VARCHAR(5),
        typhoid_conjugate_vaccine_age_greater_than_1yr  VARCHAR(5),
        typhoid_conjugate_vaccine_age_less_than_1yr     VARCHAR(5),
        fully_immunized_children                        VARCHAR(5),
        vitaminA_12_59_months                           VARCHAR(5),
        measles_rubella_1_2_years                       VARCHAR(5),
        measles_rubella_greater_2_years                 VARCHAR(5),
        covid_19_vaccine_12_17                          VARCHAR(5),
        covid_19_vaccine_18_59                          VARCHAR(5),
        covid_19_vaccine_60_above                       VARCHAR(5),

        -- Tetanus / HPV (encounter types 265, 313)
        tetanus_toxoid1                                 VARCHAR(5),
        tetanus_toxoid2                                 VARCHAR(5),
        tetanus_toxoid3                                 VARCHAR(5),
        tetanus_toxoid4                                 VARCHAR(5),
        tetanus_toxoid5                                 VARCHAR(5),
        hpv_vaccine1_10_14_years                        VARCHAR(5),
        hpv_vaccine2                                    VARCHAR(5),
        hpv_vaccine1_10_years                           VARCHAR(5),
        hpv_vaccine2_10_years                           VARCHAR(5),
        hpv_vaccine1_greater_10_years                   VARCHAR(5),
        hpv_vaccine2_greater_10_years                   VARCHAR(5),

        PRIMARY KEY encounter_id (encounter_id),
        INDEX person_date (person_id, encounter_datetime),
        INDEX person_uuid (uuid),
        INDEX location (location_id)
);

create table if not exists flat_moh_710_report_build_queue (
    person_id INT PRIMARY KEY
);

create table if not exists flat_moh_710_report_sync_queue (
    person_id INT PRIMARY KEY
);
