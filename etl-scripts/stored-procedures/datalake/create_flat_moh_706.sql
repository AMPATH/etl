-- ---------------------------------------------------------------------------
-- MOH 706 laboratory register (order grain, v3.0)
--
-- One row per laboratory order, not per encounter.  lab_no is the order's
-- amrs.orders.order_number (NULL for the order-less legacy obs rows), results
-- come from amrs.obs linked by obs.order_id (falling back to the
-- encounter-level obs for the same concept), and lab obs with no order at all
-- are kept as synthetic rows (negative report_id, lab_no NULL,
-- encounter_id NULL when the obs itself has no encounter).
--
-- v3.0 is grain + raw facts only.  Every presentation concern - names,
-- coded-answer translation, register slot mapping and layout - moved to the
-- retrieval layer (lab-706-base.json / lab-706-patient-list.json in
-- etl-rest-server), which translates through etl.concept_value_map at request
-- time.  The register columns A-AA and the 49 test-slot columns of v2.0 were
-- therefore dropped:
--
--   uuid, opd_ip_ref_no, revisit_no, full_name, age, sex, county_sub_county,
--   village_estate_landmark, telephone_number, clinical_diagnosis,
--   type_of_specimen, investigation_required, clinician_name, referrals,
--   comments, results, signature, blood_sugar ... tb_culture, and the
--   always-NULL prior_treatment / condition_of_specimen / amount_charged /
--   receipt_number.
--
--   prior_treatment / condition_of_specimen / signature have no standard-model
--   source; the retrieval JSON emits them as NULL aliases.  DECIDED
--   2026-10-21 (report owner): there are no concepts for these three fields
--   yet, so they stay blank by design - do not wire up a substitute.  The
--   other blank register columns (dates, charges, receipt, diagnosis,
--   specimen, clinician, referrals, comments) DO have sources and are
--   blank for a fixable reason; see
--   etl-rest-server/MOH-706-REGISTER-BLANK-COLUMNS-PLAN.md.
--
--   amount_charged and receipt_number are resolved at retrieval time instead:
--   the billing module's cashier_bill_line_item links to the order via
--   order_id, so the lab-706-patient-list JSON derives them there (charge =
--   SUM(price * quantity) of the order's non-voided line items, receipt from
--   the bill).
--
-- What stayed, and why:
--   report_id/order_id/lab_no/person_id/encounter_id/visit_id/test_datetime/
--   encounter_type/location_id          - grain identity.
--   concept_id + result triple (result_concept_id/result_numeric/result_text/
--   result_datetime) + obs_datetime     - the raw answer; aggregates compare
--                                         these, displays translate them.
--   sample_collection_datetime          - SPECIMEN COLLECTION DATE (10253).
--   instructions                        - order comment, composed with the
--                                         encounter comments obs in the JSON.
--   diagnostic_report_id + report_*     - the fhir report link and stamps;
--                                         received/analysed/dispatched dates
--                                         are DATE()d at display time.
--   testing_officer                     - fhir performer name; it is keyed to
--                                         the report row, so it stays here.
--
-- Only consumer of the presentation columns was the lab-706 JSON report chain;
-- no other downstream reader exists, so no translation view is kept.
--
-- Migration from the v2.0 table: this table is derived and can be rebuilt, so
-- drop the old definition before installing this one:
--   DROP TABLE IF EXISTS etl.flat_moh_706_report;
-- then rebuild (the procedure's watermark version is bumped to v3.0, so the
-- first sync re-queues every person seen since 1900-01-01).
--
-- Install:
--   mysql etl < create_flat_moh_706.sql
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl.flat_moh_706_report (
    date_created            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                                         ON UPDATE CURRENT_TIMESTAMP,

    -- grain identity -------------------------------------------------------
    report_id               BIGINT       NOT NULL,
    order_id                INT          DEFAULT NULL,
    lab_no                  VARCHAR(50)  DEFAULT NULL,
    person_id               INT          DEFAULT NULL,
    encounter_id            INT          DEFAULT NULL,
    visit_id                INT          DEFAULT NULL,
    test_datetime           DATETIME     DEFAULT NULL,
    encounter_type          INT          DEFAULT NULL,
    location_id             INT          DEFAULT NULL,

    -- the test and its raw answer ------------------------------------------
    concept_id              INT          DEFAULT NULL,
    result_concept_id       INT          DEFAULT NULL,
    result_numeric          DOUBLE       DEFAULT NULL,
    result_text             TEXT,
    result_datetime         DATETIME     DEFAULT NULL,
    obs_datetime            DATETIME     DEFAULT NULL,
    sample_collection_datetime DATETIME  DEFAULT NULL,
    instructions            VARCHAR(255) DEFAULT NULL,

    -- the fhir diagnostic report this order resulted in ---------------------
    diagnostic_report_id    BIGINT       DEFAULT NULL,
    report_date_created     DATETIME     DEFAULT NULL,
    report_issued           DATETIME     DEFAULT NULL,
    report_date_changed     DATETIME     DEFAULT NULL,
    testing_officer         VARCHAR(255) DEFAULT NULL,

    PRIMARY KEY (report_id),
    KEY order_id (order_id),
    KEY person_date (person_id, test_datetime),
    KEY location (location_id),
    KEY encounter (encounter_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;

CREATE TABLE IF NOT EXISTS etl.flat_moh_706_report_build_queue (
    person_id INT PRIMARY KEY
) ENGINE = InnoDB DEFAULT CHARSET = utf8;

CREATE TABLE IF NOT EXISTS etl.flat_moh_706_report_sync_queue (
    person_id INT PRIMARY KEY
) ENGINE = InnoDB DEFAULT CHARSET = utf8;
