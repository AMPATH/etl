# MOH 505 (IDSR weekly epidemic monitoring) flat table + build/sync queues.
#
# The table is encounter-grain: one row per (year_week, person, encounter),
# written by generate_moh_505_weekly_report_v2.  Indicator columns are
# per-encounter 0/1 flags; the etl-rest-server moh505ReportBase /
# moh505AgeDisaggregation JSON reports read them and count DISTINCT persons.
#
# Run this once before the first build: it creates the primary table, the
# master build queue (INSERT person_ids into it, then
# CALL etl.generate_moh_505_weekly_report_v2('build', 1, 1000, 5000, 1)) and
# the sync queue (refilled automatically by 'sync' runs).

create table if not exists flat_moh_505_weekly_v2 (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
    elastic_id VARCHAR(50),
    person_uuid CHAR(38),
    person_id INT,
    encounter_id INT,
    encounter_datetime DATETIME,
    location_id INT,
    year_week VARCHAR(10),
    age DECIMAL(23 , 2 ),
    gender VARCHAR(50),
    aefi INT(1) DEFAULT 0,
    acute_jaundice INT(1) DEFAULT 0,
    acute_malnutrition INT(1) DEFAULT 0,
    anthrax INT(1) DEFAULT 0,
    cholera INT(1) DEFAULT 0,
    dengue INT(1) DEFAULT 0,
    dysentery_bacillary INT(1) DEFAULT 0,
    measles INT(1) DEFAULT 0,
    maternal_deaths INT(1) DEFAULT 0,
    neonatal_deaths INT(1) DEFAULT 0,
    neonatal_tetanus INT(1) DEFAULT 0,
    plague INT(1) DEFAULT 0,
    rabies INT(1) DEFAULT 0,
    typhoid INT(1) DEFAULT 0,
    yellow_fever INT(1) DEFAULT 0,
    malaria_microscopy_tested INT(1) DEFAULT 0,
    malaria_microscopy_positive INT(1) DEFAULT 0,
    malaria_mrdt_tested INT(1) DEFAULT 0,
    malaria_mrdt_positive INT(1) DEFAULT 0,
    bacterial_meningitis INT(1) DEFAULT 0,
    meningitis_no_csf INT(1) DEFAULT 0,
    typhoid_lab_tested INT(1) DEFAULT 0,
    typhoid_lab_positive INT(1) DEFAULT 0,
    PRIMARY KEY elastic_id (elastic_id),
    INDEX encounter_id (encounter_id),
    INDEX person_id (person_id),
    INDEX year_week (year_week),
    INDEX location_id (location_id),
    INDEX encounter_datetime (encounter_datetime)
);

create table if not exists flat_moh_505_weekly_v2_build_queue (
    person_id INT PRIMARY KEY
);

create table if not exists flat_moh_505_weekly_v2_sync_queue (
    person_id INT PRIMARY KEY
);
