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

## Follow-up additions (2026-10-27, register recheck)

Two register lines flagged in the owner's screen review traced to missing
source concepts and missing indicator wiring; both now captured end-to-end
(ETL list → base/aggregate templates → moh-706 screen).

### 4.3 Hemoglobin A1c (6126) — blank instead of 0

6126 was absent from the entire chain. Added:

- `flat_moh_706_lab_concepts` + the three `refresh_concept_set_map()` scopes
  (leaf, inert in the map — kept for the keep-lists-in-sync invariant).
- base v2.9: `hba1c` numeric slot; `total_hba1c` plus the register bands
  `hba1c_pre_diabetes` (5.7 <= v < 6.5) and `hba1c_diabetes` (v >= 6.5, ADA
  cutoffs — fixed-band precedent like fbc_hb_*/cd4, NOT the lab reference
  interval, which would band at the normal range instead).
- aggregate v2.8: the three SUMs.
- screen (haematology): the three blank `getCell()`s wired.

### 1.5 Urine microscopy (1986) — total wired, positive missing

`total_urine_microscopy` existed (v2.3.1) but only counted 1984/1985/15086/
15084; 1986 URINALYSIS, MICROSCOPIC (coded leaf, also an observed 302 panel
member) was invisible, and the "Number Positive" cell held a bold header text
instead of a data cell. Added:

- 1986 to the concept list + map scopes; member edges flow via the empirical
  head pass (map gains (302, 1986), so the patient-list `results` readout now
  lists the analyte for urinalysis panels).
- base v2.9: `urine_microscopy` slot; `total_urine_microscopy` widened to
  (1986, 1984, 1985, 15086, 15084) in BOTH arms — count rises once 1986 rows
  exist, flag it in the release note; new `positive_urine_microscopy`
  (any of them coded POSITIVE 703, standalone or as panel member).
- aggregate v2.8: `positive_urine_microscopy` SUM.
- screen (urine-analysis): 1.5 third cell → `positive_urine_microscopy`.

### 2.3 Renal function test (6898) — total tests only, then panel nesting

6898 was absent from the chain. Added:

- `flat_moh_706_lab_concepts` + the three `refresh_concept_set_map()` scopes.
- base v2.9: `renal_function_test` numeric slot (obs strings `!!6898=` parse
  as numeric urea-style results in the oncology flows) and
  `total_renal_function_test` via the **4-way has-result check** so the total
  counts any answer datatype.
- aggregate v2.8: `total_renal_function_test` SUM.
- screen (blood-chemistry): the 2.3 Total Exam cell wired; its Low/High cells
  stay unwired (not in the ask, and no reference ranges are defined for 6898).

**Panel nesting (owner feedback 2026-10-27):** RFT results are grouped under
a valueless 6898 obs head, with Sodium/Potassium nested one level deeper —
under a 5473 sub-panel contained in 6898 — so 2.4–2.8 (Creatinine 790, Urea
857, Sodium 1132, Potassium 1133, Chlorides 1134) read 0 for tests that were
actually done. Fixed without new aliases (existing SUMs pick everything up):

- base v2.9: the five analyte value slots COALESCE the standalone value with
  the 6898-panel member value (lipid v2.7 pattern) — the aggregate's
  slot-based totals and MAX/MIN become member-aware automatically; the
  `_low/_high` flags band that same combined value; the `total_` flags count
  standalone orders OR 6898 member results, keeping drill-down filters
  consistent with the register numbers; `total_renal_function_test` counts a
  6898 row with its own result OR any member result (panel-total pattern,
  self-row excluded via `member_concept_id <> 6898`).
- ETL: the `refresh_concept_set_map()` head pass admits 6898 unconditionally
  (`OR o.concept_id = 6898`) — its dictionary `is_set` flag is unconfirmed,
  and a childless admission contributes no edges. Member capture already
  handles the nesting: Path G excludes set-flagged sub-heads, v3.4 Path G2
  captures their leaves, and the map lists leaf analytes only (the
  (6898, 5473) edge is deleted as a set-member).
- Counts for these analytes rise to include panel-recorded results —
  release note. Discovery (replica): confirm 6898/5473 `is_set` and the
  observed member set:
  `SELECT concept_id, is_set FROM amrs.concept WHERE concept_id IN (6898,5473);`
  `SELECT ch.concept_id, c.concept_id AS member, COUNT(*) FROM amrs.obs ch JOIN amrs.obs c ON c.obs_group_id = ch.obs_id AND c.voided = 0 WHERE ch.concept_id IN (6898,5473) AND ch.voided = 0 GROUP BY 1,2;`

### 2.8 Liver function test (953) — total tests only

953 was absent from the chain and is datatype-unused elsewhere in the
codebase. Same treatment as 6898 (2026-10-27):

- concept list + map scopes; the head pass admits 953 unconditionally
  alongside 6898 (`OR o.concept_id IN (6898, 953)` — LFT is a probable
  panel; its member analytes 653/654/655/1297/717/785/848 are already
  reportable).
- base v2.9: `liver_function_test` slot via
  `COALESCE(result_concept_id, result_numeric)` (tb_culture precedent) and
  `total_liver_function_test` = 4-way own-result check OR any member result
  (`member_concept_id <> 953`).
- aggregate v2.8: `total_liver_function_test` SUM.
- screen (blood-chemistry): the 2.8 Total Exam cell wired; Low/High stay
  unwired. Verified by sim/audit only — the mechanism is the rig-proven 6898
  path; no separate rig run (discovery on the replica can confirm the 953
  recording shape with the same query pair as for 6898).

### Stool examination (53310) — total tests only (parasitology row)

53310 is the stool-examination concept (owner correction 2026-10-27: the
initial wiring used 304, which was erroneously tied to 5.16 CSF in the
dictionary and is not stool — replaced everywhere before any deploy).
Datatype-unused in the codebase; same treatment as 953:

- concept list + map scopes; head-pass admission extended to
  `OR o.concept_id IN (6898, 953, 53310)`.
- base v2.9: `stool_examination` slot via
  `COALESCE(result_concept_id, result_numeric)` and
  `total_stool_examination` = 4-way own-result check OR any member result
  (`member_concept_id <> 53310`).
- aggregate v2.8: `total_stool_examination` SUM.
- screen (parasitology): the Stool Examination Total Exam cell wired (alias
  unchanged by the concept correction). **Number Positive wired 2026-10-27**
  after the owner defined positivity: `positive_stool_examination` = the
  row's own answer coded POSITIVE (703) or any member result coded POSITIVE
  (the `positive_urinalysis` shape, self-row excluded); Total Exam counts
  stool tests with a recorded result per the register-wide has-result
  convention.

### clinician_name orderer-only (MOH 240 register, owner decision 2026-10-27)

`lab-706-patient-list.json` v1.9: `clinician_name` exclusively reads the
orderer of the row's order (amrs.orders.orderer → provider → person_name,
preferred-first) — the encounter-provider and obs-1496 fallbacks are dropped,
so a row whose order has no provider (or an obs-only row with no adoptable
order) prints a blank clinician; ideally no order should miss a provider.
`testing_officer` (D11) is untouched. The compiled patient-list SQL contains
no `encounter_provider` reference; the fixture's orderer arm ("Jane Doc")
was already the first arm, so the rig-covered path is unchanged.

### Screen cleanup (owner 2026-10-27)

Removed the erroneous "1.4 Proteins" row from the blood-chemistry
(renal-function) block — a copy-paste leftover with three blank cells that
duplicated the urine-analysis row number. The legitimate 1.4 Proteins row in
urine-analysis (wired to the with_members proteins flags) is untouched.

### Graded urinalysis positivity (owner 2026-10-27)

The dictionary defines urinalysis positivity as POSITIVE (703) **or any
graded answer** — TRACE 10900, 1+ 2301, 2+ 2302, 3+ 2303. The graded set is
applied in the lab-706 templates to `positive_urinalysis` (both arms) and
the glucose/ketones/proteins/pus-cells `*_with_members` flags,
`positive_urine_t_vaginalis`, `positive_urine_yeast_cells` and
`positive_bacteria`, plus the aggregate's t. vaginalis / yeast cells SUMs
(they read the base value slots directly). This was why the panel Number
Positive cells read 0: results are recorded as graded answers, which
`= 703` never matched.

- Screen: the panel rows **1.1 Urine Chemistry** and **1.5 Urine
  Microscopy** stop displaying Number Positive (a panel-level positive
  count was confusing); their indicators stay in the templates for
  drill-downs.
- **1.7 Schistosoma haematobium keeps its hardcoded-0 flags** — no source
  concept exists yet (v2.3 note). The observed urinalysis members 6337 and
  9307 are the likely candidates; wire `urine_s_haematobium` +
  `total_/positive_urine_s_haematobium` (graded set) once the owner
  confirms which one it is.
- ETL: no change — member capture and the results table are
  definition-agnostic; only the retrieval-layer code sets moved.

### Verification (2026-10-27)

- `ampath-json2sql-sim`: 19/19 PASS — every new aggregate alias resolves in
  the base and becomes a `1 = (expr)` drill-down row filter; versions in
  lockstep (base 2.9 / aggregate 2.8 / uses 2.9); RFT slots/banding/total
  flags verified member-aware and the aggregate creatinine total still
  slot-based; 953 and 304 slot/flag shapes asserted.
- `contract-audit.py`: all 112 moh-706 screen keys resolve
  app ↔ aggregate ↔ base.
- `ampath-etl-test-rig` (stubbed MariaDB, v3.4): 6126 builds order-backed
  (5.9) and obs-only (6.2, 5.0) rows with correct bands (5.0 total-only,
  5.9/6.2 pre-diabetes); 1986 standalone POSITIVE/NEGATIVE and a 302 panel
  with a 1986 member POSITIVE land with correct total_/positive_ flags; the
  nested RFT shape (6898 order → valueless 6898 head → valueless 5473
  sub-head → 1132/1133 leaves, plus a leaf directly under the head, with
  6898 deliberately NOT set-flagged) builds one 6898 register row carrying
  member results 857/1132/1133, no synthetic leaf rows (F3b), map gains
  (6898 → 857/1132/1133) with the (6898, 5473) edge deleted, and the shipped
  slot/total/banding expressions read the member values (sodium 110 in-range,
  standalone 150 flags high); anti-overlap clean; build + sync both clean.
- Migration: install `create_flat_moh_706_results.sql` + the procedure, then
  **one full `build` backfill** — historical 6126/1986 orders are not
  requeued by sync — then `CALL etl.refresh_concept_set_map(NULL);`.
