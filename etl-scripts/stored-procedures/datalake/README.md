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
| `generate_flat_obs.sql` | `etl.generate_flat_obs_v_4_0` | `flat_obs` | `amrs.obs`, `amrs.encounter` |
| `generate_flat_lab_obs.sql` | `etl.generate_flat_lab_obs` | `flat_lab_obs` (v2) | `amrs.obs`, `amrs.encounter` |
| `generate_flat_orders.sql` | `etl.generate_flat_orders_v2_0` | `flat_orders_2` | `amrs.orders` |
| `generate_flat_labs_and_imaging.sql` | `etl.generate_flat_labs_and_imaging` | `flat_labs_and_imaging` | `flat_lab_obs`, `flat_orders` |
| `generate_flat_moh_706.sql` | `etl.generate_flat_moh_706_v1` | `flat_moh_706_report` | `flat_lab_obs_v2` |
| `generate_flat_moh_710.sql` | `etl.generate_flat_moh_710_v1` | `flat_moh_710_report` | `flat_obs` |
| `generate_flat_moh_711.sql` | `etl.generate_flat_moh_711_v1` | `flat_moh_711_report` | `flat_obs` |
| `generate_flat_moh_717.sql` | `etl.generate_flat_moh_717_v1` | `flat_moh_717_report` | `flat_obs` |
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

4. The MOH report tables (idempotent, plain SQL — pipe directly):

   ```sh
   mysql etl < create_flat_moh_710.sql
   mysql etl < create_flat_moh_711.sql
   mysql etl < create_flat_moh_717.sql
   ```

5. Install each procedure the same DELIMITER-wrapped way as `getValues.sql`.
   Note the `DEFINER` clauses (`openmrs_user`@`%` / `replication`@`%`): either
   create those users or adjust the definers, and set
   `log_bin_trust_function_creators = 1` if binary logging is enabled.
6. Build `flat_obs` (and `flat_lab_obs` for 706) before the report
   procedures — they are the extraction source.

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

## Conventions

- **Grain**: one row per person-encounter, restricted to the union of the
  report section encounter types; key columns are
  `person_id, uuid, encounter_id, encounter_datetime, encounter_type,
  location_id, birth_date, gender`.
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

- **MOH 706** (`flat_moh_706_report`, ~47 lab columns): reads
  `flat_lab_obs_v2` via `GetValues`. There is no `create_flat_moh_706.sql` —
  create `flat_moh_706_report_build_queue (person_id INT PRIMARY KEY)` by hand
  before build mode.
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
