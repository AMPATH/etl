# Plan: additional reportable lab concepts for the MOH 706 register (v2.5)

Source: `additional_concepts.tsv` (44 rows) supplied by the lab-report owner.
Scope: the full MOH 706 / MOH 240 chain — `etl.flat_moh_706_report` (data),
`lab-706-*` JSON templates (API), moh-706 aggregate screen (view).

## Evaluation of the TSV

44 rows → **42 net-new concept ids**. Findings:

1. **Duplicate id 53316.** Both "5.14 V. cholerae O1" and "5.15
   V. cholerae O139" map to `53316`. One concept id can only produce one
   register line, so 53316 is listed once (line 5.14) and **5.15 stays
   unreported until its own concept exists**. The discovery sweep below
   looks for the missing id (e.g. 53317); if found, it is a one-tuple
   addition in all three lists + one base slot + one aggregate pair.
2. **49245 (6.10 CSF) is already reportable** (v3.2 additions). Re-inserting
   it would violate the `flat_moh_706_lab_concepts` PRIMARY KEY and kill the
   procedure at seed time — it is excluded everywhere.
3. Whitespace/typos in the TSV ("Prostrate") are irrelevant: register names
   come from `amrs` via `etl.concept_value_map`, never from the file.

### The 42 ids by register section

| Section | Ids | Slot/indicator shape shipped |
|---|---|---|
| 1.10 Bacteria (urine) | 53247 | coded slot `bacteria` + `total_/positive_` |
| 2.24 / 2.26 tumour markers | 53248 CA 15-3, 53249 CA 125 | numeric slots + `max_/min_/total_` + `*_high` banding |
| 4.8 Coagulation profile | 53250 | panel — counted via member EXISTS (`total_coagulation_profile`) |
| 5.2–5.8 bacteriology specimens | 53311 pus, 53312 throat, 53313 rectal, 53314 water, 53315 food | `total_bact_*` (4-way has-result) |
| 5.12–5.28 organisms | 53269, 53270, 53316, 53279–53284, 53285, 53286, 53287, 53288 | coded slots + `total_/positive_bact_*` (positive = result_concept_id 703) |
| 6.4–6.25 cytology sites | 53289–53292, 53293–53307 (19 ids) | `total_cyt_*` (4-way has-result) |
| 7.8 Hepatitis A | 53309 | coded slot `hep_a` + `total_/positive_` |

Pending dictionary confirmation (discovery SQL below): datatypes for
53247/organism/53309 ids (slots assume coded; **total_ flags use the 4-way
has-result check, so numeric or text answers still count either way**),
`is_set` for 53250 (treated as a panel; if it is a leaf, member EXISTS is
simply never true and the self-row check still counts it), and reference
ranges for 53248/53249 (banding seeded from `amrs.concept_numeric`; NULL
bounds read 0).

## Changes shipped

### Data layer — `/media/etl` (branch `chore-add-more-concepts`, rebased onto `feat-moh-706-register-fixes` v3.3)

The reportable list lives in **three** places on v3.3; all three got the 42
ids:

- `generate_flat_moh_706.sql` — `flat_moh_706_lab_concepts` INSERT, appended
  in form-section order with a dated comment (f932b01 pattern). No watermark
  bump: pure concept addition on top of v3.3.
- `create_flat_moh_706_results.sql` — both `refresh_concept_set_map()` IN
  lists (head pass + dictionary pass). The `is_set` join admits only actual
  panels, so plain ids are inert there.
- Sync coverage is automatic for new orders (the orders REPLACE clause joins
  the concept list); panel heads flow via `concept_set_map`.

### API layer — `/media/etl-rest-server` (templates `lab-706-base` / `lab-706-aggregate` → v2.5)

- `lab-706-base.json` (+80 columns): 15 raw-value slots (numeric
  `ca_15_3`/`ca_125`; coded `bacteria`, 13 organisms, `hep_a`; specimens and
  cytology sites are flag-only, matching the `total_bact_blood` precedent);
  `total_*` flags for all 42; `positive_*` for the 15 coded ids; panel-style
  `total_coagulation_profile` (4-way OR member EXISTS);
  `max_/min_/total_ca_15_3`/`ca_125`; `ca_15_3_high`/`ca_125_high` vs
  `etl.reference_ranges`.
- `lab-706-aggregate.json` (+58 columns): matching `SUM/MAX/MIN` indicators
  grouped by register section; version **2.4 → 2.5** in lockstep with base
  (own version + `uses` entry + changelog in `description`).
- `lab-706-patient-list.json` **unchanged** (v1.5): `results` and
  `investigation_required` are generic through `concept_value_map` /
  `concept_set_map` — no `useTemplateVersion` edit, no version skew.

### View layer — `/media/ampath-esm-reports` (moh-706 aggregate screen)

Wired the existing blank register cells to the new aggregate aliases
(`getCell` keys):

- urine-analysis 1.10 Bacteria → `total_bacteria` / `positive_bacteria`
- blood-chemistry 2.24 / 2.26 → `total_ca_15_3` + `ca_15_3_high`,
  `total_ca_125` + `ca_125_high` (the "Number Positive" column carries the
  high count for these numeric markers — `positive_` on a numeric slot is
  always 0, as the existing CA 19-9 row demonstrates)
- haematology 4.8 → `total_coagulation_profile`
- bacteriology 5.2/5.4/5.5/5.7/5.8 → `total_bact_pus_swab` … `total_bact_food`;
  5.12–5.14, 5.17–5.20, 5.22–5.25, 5.27, 5.28 → `positive_bact_*`.
  **Left blank on purpose**: 5.15 V. cholerae O139 (no concept), 5.21
  N. meningitidis X and 5.26 Cryptococcal meningitis (not in the TSV).
- histology-and-cytology 6.4–6.6, 6.9, 6.11–6.25 → `total_cyt_*`
- serology 7.8 → `total_hep_a` / `positive_hep_a`

MOH-240 patient register: **no change** (renders the generic `results`
string; alias contract documented in MOH-706-REGISTER-BLANK-COLUMNS-PLAN F7).

## Discovery SQL (run against the analytics replica; paste results here)

```sql
-- 1. Datatypes / classes / volume for all 42 ids
SELECT c.concept_id, cn.name, cd.name AS datatype, cc.name AS class, c.is_set,
       (SELECT COUNT(*) FROM amrs.orders o WHERE o.concept_id = c.concept_id
          AND o.voided = 0) AS orders,
       (SELECT COUNT(*) FROM amrs.obs x WHERE x.concept_id = c.concept_id
          AND x.voided = 0) AS obs
FROM amrs.concept c
JOIN amrs.concept_datatype cd ON cd.concept_datatype_id = c.datatype_id
JOIN amrs.concept_class   cc ON cc.concept_class_id   = c.class_id
LEFT JOIN amrs.concept_name cn ON cn.concept_id = c.concept_id
     AND cn.locale = 'en' AND cn.locale_preferred = 1 AND cn.voided = 0
WHERE c.concept_id IN (53247, 53248, 53249, 53250,
  53311, 53312, 53313, 53314, 53315,
  53269, 53270, 53316, 53279, 53280, 53281, 53282, 53283, 53284,
  53285, 53286, 53287, 53288,
  53289, 53290, 53291, 53292, 53293, 53294, 53295, 53296, 53297,
  53298, 53299, 53300, 53301, 53302, 53303, 53304, 53305, 53306, 53307,
  53309);

-- 2. Missing V. cholerae O139 concept? (fixes the 53316 duplicate)
SELECT concept_id, name, voided FROM amrs.concept_name
WHERE concept_id BETWEEN 53300 AND 53330 OR name LIKE '%cholera%';

-- 3. Answer sets for the coded-slot ids (confirms positive_ = 703 reads real data)
SELECT concept_id, value_coded, COUNT(*) FROM amrs.obs
WHERE concept_id IN (53247, 53269, 53270, 53316, 53279, 53280, 53281,
                     53282, 53283, 53284, 53285, 53286, 53287, 53288, 53309)
  AND voided = 0 GROUP BY 1, 2;

-- 4. Coagulation profile panel shape (set members + observed groups)
SELECT cs.concept_set, cn.name FROM amrs.concept_set cs
LEFT JOIN amrs.concept_name cn ON cn.concept_id = cs.concept_set
     AND cn.locale = 'en' AND cn.locale_preferred = 1 AND cn.voided = 0
WHERE cs.concept_id = 53250 AND cs.retired = 0;
SELECT ch.concept_id, c.concept_id AS member, COUNT(*) FROM amrs.obs ch
JOIN amrs.obs c ON c.obs_group_id = ch.obs_id AND c.voided = 0
WHERE ch.concept_id = 53250 AND ch.voided = 0 GROUP BY 1, 2;

-- 5. Reference ranges for the new numeric banding
SELECT concept_id, low_value, high_value FROM amrs.concept_numeric
WHERE concept_id IN (53248, 53249);
```

If (1) shows any organism/hep-a/bacteria id is **Numeric**, change only its
base slot to `result_numeric` (the `total_` flags already count it either
way); if **Text**, leave the slot (always NULL, harmless) — counts and the
patient list still work.

## Migration / validation

1. Install `create_flat_moh_706.sql` (no change), `create_flat_moh_706_results.sql`,
   then `generate_flat_moh_706.sql` (v3.3 watermark already forces the
   build-first flow). `CALL etl.refresh_concept_set_map(NULL);` after install.
2. **One full `build` backfill** — historical orders of the new concepts are
   never requeued by sync (f932b01 precedent).
3. Validate: per-concept counts vs `amrs.orders` for a sample period; panel
   53250 members vs grouped `amrs.obs`; aggregate smoke
   `GET /etl/lab-706?indicators=total_bact_pus_swab,positive_hep_a,total_cyt_thyroid,total_ca_15_3&...`.
4. Rollback: drop the new base/aggregate columns by reinstalling v2.4
   templates; data layer is inert without the templates.

## Verification performed (2026-10-23)

- `ampath-json2sql-sim`: aggregate + patient-list SQL compile with the pinned
  compilers; 11/11 checks PASS (every `t1.*` ref resolves in base; every
  indicator alias has a base drill-down flag; new drill indicators become
  `1 = (expr)` row filters).
- `contract-audit.py`: all 106 moh-706 screen keys resolve app ↔ aggregate ↔
  base after the sub-report wiring.
- `ampath-etl-test-rig` (stubbed MariaDB): v3.3 procedure + 42-id list
  install clean; scenarios for each family build correctly — coded/text/
  numeric order rows, obs-only synthetic row, 53250 ObsTree panel (members
  `53301=12.5`, `53302=30` in `flat_moh_706_report_results`, empirical
  `concept_set_map` edges, anti-overlap clean), sync mode OK; base-template
  expressions executed against the built tables (text pus-swab counts via the
  4-way check, panel counts via member EXISTS); patient-list renders
  `Coagulation profile` as `PT: 12.5; APTT: 30 | 30` and translates
  POSITIVE/NEGATIVE via `concept_value_map`.
