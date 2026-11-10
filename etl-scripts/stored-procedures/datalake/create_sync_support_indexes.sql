# Supporting indexes for the sync-refill arms of the report procedures
# (generate_moh_505_weekly_report_v2, generate_flat_moh_706_v1, and the
# generate_flat_moh_710/711/717 family that shares the flat_obs arm).
#
# Run ONCE per server, OFF-PEAK, before deploying the perf sync refills.
# These are plain CREATE INDEX statements - InnoDB builds them online
# (ALGORITHM=INPLACE, LOCK=NONE); the amrs.obs one is the big table and the
# one to schedule.  MariaDB syntax (IF NOT EXISTS) - matches the analytics
# replica.
#
# IF NOT EXISTS guards by INDEX NAME only: if the same columns are already
# indexed under a different name, creating these duplicates the work without
# breaking anything.  Check first:
#
#   SHOW INDEX FROM amrs.obs;
#   SHOW INDEX FROM amrs.orders;
#   SHOW INDEX FROM amrs.fhir_diagnostic_report;
#   SHOW INDEX FROM amrs.encounter;
#   SHOW INDEX FROM amrs.person;
#   SHOW INDEX FROM amrs.person_attribute;
#   SHOW INDEX FROM etl.flat_obs;
#
# What each index buys:
#   flat_obs(max_date_created, person_id)
#       -> the flat_obs refill arm becomes a COVERING index range scan; no
#          row lookups into the packed-obs TEXT rows.  Also covers the
#          person_date lookups.  Shared by 505/710/711/717.
#   obs(date_created, person_id, concept_id, order_id, obs_group_id)
#       -> all three raw-obs refill arms of the 706 sync become covering
#          index range scans (order-linked + 10253 arm, grouped-obs arm,
#          concept_set_map join arm).  date_created first for the range;
#          the rest carried so arms 2/3/4 never touch the row itself.
#   orders(date_created | date_stopped | date_voided)
#       -> one index per mutation column; the refill queries one arm per
#          column (the previous single OR-of-ranges form defeated index use).
#   encounter(date_changed), person(date_changed), person(date_voided)
#       -> the person/encounter refill arms; without them each nightly sync
#          full-scans the table.
#   person_attribute(person_id), person_attribute(person_attribute_type_id)
#       -> the Test-or-Fake-Patient exclusion: the tmp_test_patients build
#          joins the dictionary to the attribute rows on the type id, and
#          the end-of-run purge joins on person_id.

CREATE INDEX IF NOT EXISTS idx_flat_obs_max_date_person ON etl.flat_obs (max_date_created, person_id);

CREATE INDEX IF NOT EXISTS idx_obs_date_covering ON amrs.obs (date_created, person_id, concept_id, order_id, obs_group_id);

CREATE INDEX IF NOT EXISTS idx_orders_date_created ON amrs.orders (date_created);
CREATE INDEX IF NOT EXISTS idx_orders_date_stopped ON amrs.orders (date_stopped);
CREATE INDEX IF NOT EXISTS idx_orders_date_voided ON amrs.orders (date_voided);

CREATE INDEX IF NOT EXISTS idx_fhir_diag_report_date_changed ON amrs.fhir_diagnostic_report (date_changed);

CREATE INDEX IF NOT EXISTS idx_encounter_date_changed ON amrs.encounter (date_changed);
CREATE INDEX IF NOT EXISTS idx_person_date_changed ON amrs.person (date_changed);
CREATE INDEX IF NOT EXISTS idx_person_date_voided ON amrs.person (date_voided);

CREATE INDEX IF NOT EXISTS idx_person_attribute_person ON amrs.person_attribute (person_id);
CREATE INDEX IF NOT EXISTS idx_person_attribute_type ON amrs.person_attribute (person_attribute_type_id);
