# DDL for the master build queues consumed by the source-table procedures
# when run in build mode.  Nothing else creates these tables - populate
# each with the ids to (re)build, then call the matching procedure:
#
#   flat_obs_build_queue               (person_id)    -> etl.generate_flat_obs_v_4_0
#   flat_orders_build_queue            (encounter_id) -> etl.generate_flat_orders_v2_0
#   flat_labs_and_imaging_build_queue  (person_id)    -> etl.generate_flat_labs_and_imaging
#
# e.g.  INSERT INTO flat_obs_build_queue (person_id) SELECT person_id FROM amrs.person;
#       CALL etl.generate_flat_obs_v_4_0('build', 1, 1000, 5000);
#
# The procedures create their primary tables (flat_obs, flat_orders_2,
# flat_labs_and_imaging), their per-worker queue slices and their sync
# queues themselves.
#
# flat_hiv_summary_sync_queue belongs to the HIV summary tooling
# (../generate_flat_hiv_summary_sync_queue.sql) but generate_flat_obs
# pushes every person it rebuilds into it on each cycle, so it must
# exist before flat_obs build/sync runs.

create table if not exists flat_obs_build_queue (
    person_id INT PRIMARY KEY
);

create table if not exists flat_orders_build_queue (
    encounter_id INT PRIMARY KEY
);

create table if not exists flat_labs_and_imaging_build_queue (
    person_id INT PRIMARY KEY
);

create table if not exists flat_hiv_summary_sync_queue (
    person_id INT PRIMARY KEY
);
