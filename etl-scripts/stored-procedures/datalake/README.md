# Datalake stored procedures

Procedures that build flat (one row per person-encounter) datalake tables in
the `etl` schema, extracted from the packed `flat_obs` table, plus DDL for the
tables they target. The MOH report procedures materialise, per encounter, the
same indicators the etl-rest-server json-reports compute at request time, so
`SUM(<column>)` grouped by location and period downstream reproduces the
official MOH report output.

## Contents

| File | Procedure / object | Writes table | Reads |
|---|---|---|---|
| `getValues.sql` | function `etl.GetValues(obs, concept_id)` | — | packed obs strings |
| `create_concept_value_map.sql` | `etl.concept_value_map`, `etl.refresh_concept_value_map()` | `concept_value_map` | `amrs.concept`, `amrs.concept_name`, `amrs.concept_answer` |
| `getCodedValue.sql` | functions `etl.GetCodedValue`, `etl.GetConceptName` | — | `etl.concept_value_map` |
| `generate_flat_obs.sql` | `etl.generate_flat_obs_v_4_0` | `flat_obs` | `amrs.obs`, `amrs.encounter` |
| `generate_flat_lab_obs.sql` | `etl.generate_flat_lab_obs` | `flat_lab_obs` (v2) | `amrs.obs`, `amrs.encounter` |
| `generate_flat_orders.sql` | `etl.generate_flat_orders_v2_0` | `flat_orders_2` | `amrs.orders` |
| `generate_flat_labs_and_imaging.sql` | `etl.generate_flat_labs_and_imaging` | `flat_labs_and_imaging` | `flat_lab_obs`, `flat_orders` |
| `generate_flat_moh_706.sql` | `etl.generate_flat_moh_706_v1` | `flat_moh_706_report` | `amrs.orders`, `amrs.obs`, `amrs.encounter`, `amrs.fhir_diagnostic_report*` |
| `generate_flat_moh_710.sql` | `etl.generate_flat_moh_710_v1` | `flat_moh_710_report` | `flat_obs` |
| `generate_flat_moh_711.sql` | `etl.generate_flat_moh_711_v1` | `flat_moh_711_report` | `flat_obs` |
| `generate_flat_moh_717.sql` | `etl.generate_flat_moh_717_v1` | `flat_moh_717_report` | `flat_obs` |
| `create_flat_moh_706.sql` | DDL | `flat_moh_706_report` + build/sync queues | — |
| `create_flat_moh_706_results.sql` | `concept_set_map`, `refresh_concept_set_map()`, results DDL | `flat_moh_706_report_results`, `concept_set_map` | `amrs.obs`, `amrs.concept`, `amrs.concept_set` |
| `create_flat_moh_710.sql` | DDL | `flat_moh_710_report` + build/sync queues | — |
| `create_flat_moh_711.sql` | DDL | `flat_moh_711_report` + build/sync queues | — |
| `create_flat_moh_717.sql` | DDL | `flat_moh_717_report` + build/sync queues | — |

All procedures share the same 4-parameter signature:

```sql
CALL etl.<procedure>(query_type, queue_number, queue_size, cycle_size);
-- e.g. CALL etl.generate_flat_moh_711_v1('sync', 0, 1000, 1000);
```

## Prerequisites and install order

1. An `etl` database and read access to the OpenMRS `amrs` schema.
2. `etl.flat_log` — create it first with
   `mysql etl < ../flat_tables/flat_log_v1.0.sql` (used as the sync watermark).
3. `etl.GetValues` — the procedure files have no `DELIMITER` directives, so
   wrap them when using the mysql CLI:

   ```sh
   { echo 'DELIMITER $$'; cat getValues.sql; echo '$$'; echo 'DELIMITER ;'; } | mysql etl
   ```

3a. Concept translation for coded answers (MOH 706). These two files already
   contain `DELIMITER` directives, so pipe them directly:

   ```sh
   mysql etl < create_concept_value_map.sql
   mysql etl < getCodedValue.sql
   CALL etl.refresh_concept_value_map();
   ```

   Refresh the map after adding or renaming concepts in `amrs`.

4. The MOH report tables (idempotent, plain SQL — pipe directly):

   ```sh
   mysql etl < create_flat_moh_706.sql
   mysql etl < create_flat_moh_710.sql
   mysql etl < create_flat_moh_711.sql
   mysql etl < create_flat_moh_717.sql
   ```

   `create_flat_moh_706.sql` defines the order-grain MOH 706 table. The old
   encounter-grain table cannot be migrated in place; drop it first and let
   the build recreate it:

   ```sql
   DROP TABLE IF EXISTS etl.flat_moh_706_report;
   ```

   `create_flat_moh_706_results.sql` (v3.1) adds the panel-member results
   table and the empirical panel→member map. Install it, then run ONE build
   of MOH 706 before any sync: the v3.1 procedure keeps its own
   `flat_moh_706_report_v3.1` watermark and sync refuses to run until a
   build has logged it (a cold sync would scan all of `amrs.obs` by date).

   ```sh
   mysql etl < create_flat_moh_706_results.sql
   mysql etl -e "CALL etl.refresh_concept_set_map(NULL);"
   ```

   Then populate the build queue (the DDL creates it **empty**, and every
   build drains it — a build over an empty queue SIGNALs rather than
   silently logging the v3.1 watermark with nothing built) and run one
   build before any sync:

   ```sql
   INSERT INTO etl.flat_moh_706_report_build_queue (person_id)
   SELECT person_id FROM amrs.person;
   CALL etl.generate_flat_moh_706_v1('build', 1, 1000, 5000);
   ```

5. Install each procedure the same DELIMITER-wrapped way as `getValues.sql`.
   Note the `DEFINER` clauses (`openmrs_user`@`%` / `replication`@`%`): either
   create those users or adjust the definers, and set
   `log_bin_trust_function_creators = 1` if binary logging is enabled.
6. Build `flat_obs` before the 710/711/717 procedures. MOH 706 reads
   `amrs.orders` and `amrs.obs` directly and has no `flat_obs` dependency.

## Running

**Build (initial / full backfill).** Populate the report's build queue with
the person_ids to process, then run build mode; it slices the queue across
`queue_number` workers, processes in `cycle_size` batches, merges into the
primary table and empties the queue:

```sql
INSERT INTO flat_moh_711_report_build_queue (person_id) SELECT person_id FROM amrs.person;
CALL etl.generate_flat_moh_711_v1('build', 1, 1000, 5000);  -- worker 1
CALL etl.generate_flat_moh_711_v1('build', 2, 1000, 5000);  -- worker 2, parallel
```

**Sync (incremental).** Uses the `etl.flat_log` watermark and only reprocesses
persons with encounters/obs/person records changed since the last run:

```sql
CALL etl.generate_flat_moh_711_v1('sync', 0, 1000, 5000);
```

Run order for a scheduled sync: `flat_obs` first, then the report procedures.
Each run prints progress rows per cycle and appends a `flat_log` entry.

## Getting report numbers out

The tables are per-encounter flags (`1` / `0` / `NULL`), so the MOH aggregate
for a facility and month is a plain grouped SUM:

```sql
SELECT location_id,
       SUM(new_anc_clients)      AS new_anc_clients,
       SUM(syphilis_positive)    AS syphilis_positive,
       SUM(normal_deliveries)    AS normal_deliveries
FROM etl.flat_moh_711_report
WHERE encounter_datetime >= '2026-08-01' AND encounter_datetime < '2026-09-01'
GROUP BY location_id;
```

MOH 706 is at order grain and stores raw facts (the test's `concept_id` plus
the raw result), so aggregate over the raw columns rather than test columns:

```sql
SELECT location_id,
       COUNT(*) AS urine_protein_tests,
       SUM(result_concept_id = 703) AS positive
FROM etl.flat_moh_706_report
WHERE test_datetime >= '2026-08-01' AND test_datetime < '2026-09-01'
  AND concept_id = 2339                      -- URINE PROTEIN
  AND result_concept_id IS NOT NULL          -- a result exists
GROUP BY location_id;
```

Coded answers stay ids in the table (`703` = POSITIVE); translate for display
through `etl.concept_value_map`, the same way the lab-706 json-reports do.

## Conventions

- **Grain**: one row per person-encounter for MOH 710/711/717; one row per
  laboratory order for MOH 706 (see its per-report note).
- **Indicator columns** are `VARCHAR(5)` holding `1`, `0` or `NULL`
  (`NULL` matches the upstream base reports' "1, NULL" style, and placeholder
  columns are always `NULL`). All-VARCHAR keeps dirty data from failing CASTs;
  `VARCHAR(5)` keeps the very wide tables under the MySQL row-size limit.
- **Translation rules** from the etl-rest-server json definitions
  (`!!concept_id=value!!` tokens in the packed obs):
  `concept = C AND value_coded = V` → `obs REGEXP "!!C=V!!"`;
  value sets → alternations; `!= V` → presence minus `!!C=V!!`;
  numeric comparisons take the first packed value, guarded by a numeric-format
  check so dirty values are simply not counted.
- **Age bands** are computed at `encounter_datetime` rather than the upstream
  `CURDATE()`, so written rows stay deterministic.
- **Fidelity**: column names are copied verbatim from the report output
  (misspellings included), and upstream quirks are preserved and flagged with
  `NOTE` comments so each table reconciles 1:1 with the live report.

## Per-report notes

- **MOH 706** (`flat_moh_706_report`, v3.0) is **order grain and raw facts
  only**: one row per `amrs.orders.order_id`, and `lab_no` is the order's
  `order_number`. Each row carries the test's `concept_id` and the raw result
  (`result_concept_id` / `result_numeric` / `result_text` /
  `result_datetime`) resolved through the `order → any-order-obs →
  encounter-obs` fallback chain, plus the sample-collection datetime
  (obs 10253), the order instructions, the fhir diagnostic-report link and
  stamps, and the fhir performer name (`testing_officer`). Lab obs with no
  order at all are still emitted as synthetic rows with a negative
  `report_id`, `lab_no` NULL — including obs with no encounter, which land
  with `encounter_id` NULL. Every presentation concern — names, addresses,
  coded-answer translation (through `etl.concept_value_map`), the 49 test
  slots and the register columns A–AA — is derived at request time by the
  lab-706 json-reports in etl-rest-server (`lab-706-base.json`,
  `lab-706-patient-list.json`); `prior_treatment`,
  `condition_of_specimen` and `signature` are emitted as NULL aliases there
  (no standard-model source), while `amount_charged` and `receipt_number`
  are derived from the billing module (`cashier_bill_line_item` links to the
  order via `order_id`). `1985` is red blood cells in this dictionary, so it fills `urine_rbc` (the legacy
  `urine_s_haematobium` slot is left NULL). Sync queues watch only orders,
  order-linked/specimen obs and fhir reports — person/encounter/diagnosis
  changes no longer requeue since those fields are derived at retrieval.
- **MOH 710** (58 columns): Immunization + Tetanus/HPV. Vaccine dose numbers
  (concept 10888) come from same-encounter co-occurrence instead of the
  upstream `obs_group_id` self-join, which the flat_obs packing loses; see the
  NOTE in the file. `hpv_vaccine1`/`hpv_vaccine2` are identical upstream.
- **MOH 711** (284 columns, 9 sections): PNC's `referrals_from_community` is
  renamed `pnc_referrals_from_community` (name collision with Maternity).
  Placeholder columns (audits, `not_screened_for_tb`, FP implant/DMPA doses,
  SGBV pregnancy/disability, etc.) are NULL — unimplemented upstream.
- **MOH 717** (304 columns, 14 sections): 261 columns are upstream `"null"`
  placeholders (the entire Inpatient ward block, Operations, Orthopaedics,
  Special Services, Pharmacy, Mortuary, Medical Records, ...) and are emitted
  as NULL. This report maps `anc_new` to encounter type 264 — the opposite of
  MOH 711's mapping; each report's own logic is kept. Maternity output names
  follow the aggregate's corrected plurals (`caesarean_sections`,
  `live_births`, `still_births`).

The report indicator logic was translated from
[etl-rest-server](https://github.com/AMPATH/etl-rest-server/tree/master/app/reporting-framework/json-reports)
(`moh-710`, `moh-711`, `moh-717` folders); see each procedure's header comment
for its exact source files and preserved quirks.
