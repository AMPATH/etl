# Plan v3: panel-test (multi-analyte) results in the MOH lab report

Scope: `etl.flat_moh_706_report` + `etl.generate_flat_moh_706_v1`
(referred to downstream as the MOH 240 / lab-706 lab report).

## Status

- Phase 0 discovery: DONE (panels, members, linkage, nesting — all three
  nesting checks agree on depth-2). Phases 1–2 implemented 2026-10-20:
  `create_flat_moh_706_results.sql` (results table + concept_set_map +
  heads-first `refresh_concept_set_map()`) and v3.1 changes in
  `generate_flat_moh_706.sql` (anchor carry, Path G/U member capture,
  self-rows, sync-queue additions, child-first cascades, build-first
  migration guard).
- Phase 3 implemented 2026-10-20 (etl-rest-server):
  `lab-706-base.json` v2.2 — member-aware flags (PK probes on
  report_id[, member_concept_id], no grain change), new `fbc_hgb` slot for
  the HGB member, `*_with_members` pairs for the four urine analytes that
  double as urinalysis members (sign-off item; legacy aliases untouched);
  `lab-706-aggregate.json` v2.2 — panel totals/positives read the base
  member-flags, Hb bands read `fbc_hgb` (previously banded one arbitrary
  member as if it were Hb), with_members SUMs added; aliases stable;
  `lab-706-patient-list.json` — `results` composes all analytes
  ("ANALYTE: value; ...", translated via concept_value_map) with the
  single value as fallback; alias unchanged, so no frontend change
  (moh-240.columns.ts reads `results`). Remaining: deploy + one `build`
  run, Phase-4 validation, owner sign-off on with_members counting, then
  (optionally) retire the legacy urine aliases.

## Problem

An order for a panel test (e.g. CBC, concept `1019`) results in `amrs.obs`
as an **obs group** (ObsTree): a valueless group-head obs plus one obs per
analyte (HGB `21`, WBC `678`, PLT `729`, …), linked to the head through
`obs.obs_group_id`. The v3.0 ETL resolves **one** result obs per order
(`ro → ra → re` fallback) into a single result triple, so panels show at
most one arbitrary member value.

## Phase 0 — Discovery: DONE

Panels among the 45 reportable concepts (orders in parentheses):

| Panel | Concept | Orders | Grouped member obs | Reality |
|---|---|---|---|---|
| COMPLETE BLOOD COUNT | `1019` | 4,313 | ~23.1k obs × ~23 analytes (~23k groups) | members AND heads almost never carry `order_id` (9–13 of 23k) |
| URINALYSIS | `302` | 1,932 | ~1.4k obs × ~19 analytes | same linkage pattern |
| LIPID PANEL | `1010` | 2,691 | ~9 obs total | results mostly recorded as **ungrouped analyte obs** (1006–1009) |

Confirmed members (empirical, via `obs_group_id`):
- **1019**: 21, 678, 679, 729, 851, 952, 1015, 1016, 1017, 1018, 1021–1025,
  1307–1309, 1327, 1330–1335, 15015, 15016
- **302**: 2339, 7276, 1984, 6337, 9307, 2340, 56, 1985, 1986, 10893,
  10903–10907, 10913, 10914, 10918, 12806, 12807
- **1010**: 1006, 1007, 1008, 1009

`amrs.concept_set` is materially incomplete for these panels (4 rows,
`12793` claimed as member of all three) → **the member map must be
empirical**, not dictionary-driven.

Design consequences:

1. **Anchor is the encounter-level head.** `heads_with_order ≈ 0` ⇒
   `anchor_obs_id = COALESCE(ro_obs_id, re_obs_id)` where `re` (encounter
   obs of the panel concept) does nearly all the work.
2. **Obs-only legacy path is dominant** (23k CBC groups vs 4.3k orders).
   Member capture must run from the obs-only synthetic rows too — their
   anchor is the head obs itself.
3. **Second collection path (ungrouped analytes)** for the lipid-panel
   pattern: encounter-level obs of member concepts with no `obs_group_id`
   and not already consumed as some group's child.
4. **Members double as standalone tests** (urinalysis analytes are
   individually orderable) → retrieval-layer counting decision (below).

Nesting — RESOLVED (2026-10-20), all three checks agree on depth-2 ObsTrees:

- **Q1 (dictionary)**: all 46 member concepts are leaf datatypes
  (Numeric/Coded/Text, `is_set = 0`). Even the suspicious names are leaves:
  56 URINE MICROSCOPY is `Test/Text`; 1986 URINALYSIS, MICROSCOPIC is class
  LabSet but a coded leaf (4 obs). No set-like heads hide among members.
- **Q2 (empirical, 302/1010 branches)**: EMPTY — no member of urinalysis or
  lipid groups has children of its own.
- **Q3 (CBC sample, 1k children)**: 0 — no grandchildren under CBC heads.

⇒ member pass is a single `obs_group_id` join, as implemented in v3.1.
<details><summary>Original three-question decomposition (for reference)</summary>

```sql
-- Q1 dictionary (instant): any member concept that is set-like (is_set=1,
-- datatype N/A, class ConvSet/LabSet) is a potential sub-head; none = flat.
SELECT c.concept_id, cn.name, c.is_set, cc.name AS class, cd.name AS datatype
FROM amrs.concept c
JOIN amrs.concept_class    cc ON cc.concept_class_id = c.class_id
JOIN amrs.concept_datatype cd ON cd.concept_datatype_id = c.datatype_id
LEFT JOIN amrs.concept_name cn ON cn.concept_id = c.concept_id
     AND cn.locale = 'en' AND cn.locale_preferred = 1 AND cn.voided = 0
WHERE c.concept_id IN (21,678,679,729,851,952,1015,1016,1017,1018,1021,1022,
  1023,1024,1025,1307,1308,1309,1327,1330,1331,1332,1333,1334,1335,15015,15016,
  2339,7276,1984,1985,6337,9307,2340,56,1986,10893,10903,10904,10905,10906,
  10907,10913,10914,10918,12806,12807,1006,1007,1008,1009);

-- Q2 empirical probe, small panels only (~1.4k children, sub-second):
SELECT p.concept_id AS panel, c.concept_id AS member, COUNT(*) AS grandchild_obs
FROM amrs.obs p
JOIN amrs.obs c  ON c.obs_group_id = p.obs_id AND c.voided = 0
JOIN amrs.obs gc ON gc.obs_group_id = c.obs_id AND gc.voided = 0
WHERE p.voided = 0 AND p.concept_id IN (302, 1010)
GROUP BY 1, 2;

-- Q3 CBC flatness sample (1k children, early-stop EXISTS):
SELECT COUNT(*) AS cbc_children_with_children
FROM (SELECT c.obs_id
      FROM amrs.obs p
      JOIN amrs.obs c ON c.obs_group_id = p.obs_id AND c.voided = 0
      WHERE p.voided = 0 AND p.concept_id = 1019
      LIMIT 1000) s
WHERE EXISTS (SELECT 1 FROM amrs.obs gc WHERE gc.obs_group_id = s.obs_id);
```
</details>

## Design

Child results table (Option A — panels/members need no DDL; raw facts
only; translation stays at retrieval).

### Phase 1 — Schema (`create_flat_moh_706_results.sql`)

```sql
-- Empirical panel → member map: concept_set ∪ observed obs-group edges
CREATE TABLE IF NOT EXISTS etl.concept_set_map (
    concept_id  INT NOT NULL,          -- panel (302, 1010, 1019, …)
    concept_set INT NOT NULL,          -- member analyte (21, 678, …)
    sort_weight DOUBLE DEFAULT NULL,
    PRIMARY KEY (concept_id, concept_set)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- etl.refresh_concept_set_map() — heads-first, never an obs↔obs full join:
--   1. temp panel_heads = obs with concept IN (302,1010,1019) via concept_id
--      index (~25k rows); incremental runs AND date_created >= last refresh.
--   2. REPLACE INTO … SELECT p.concept_id, c.concept_id FROM panel_heads p
--      JOIN amrs.obs c ON c.obs_group_id = p.obs_id (indexed probes)
--      UNION amrs.concept_set rows.
-- See "Performance" section for the full procedure body.

CREATE TABLE IF NOT EXISTS etl.flat_moh_706_report_results (
    date_created      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                        ON UPDATE CURRENT_TIMESTAMP,
    report_id         BIGINT NOT NULL,   -- = flat_moh_706_report.report_id
    member_concept_id INT NOT NULL,
    result_obs_id     BIGINT DEFAULT NULL,
    result_concept_id INT DEFAULT NULL,
    result_numeric    DOUBLE DEFAULT NULL,
    result_text       TEXT,
    result_datetime   DATETIME DEFAULT NULL,
    obs_datetime      DATETIME DEFAULT NULL,
    PRIMARY KEY (report_id, member_concept_id),
    KEY member_concept (member_concept_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```

Policies: non-panel tests also write their single row (one consumer path);
latest-per-member wins (`MAX(obs_id)`); `result_obs_id` = audit lineage.

### Phase 2 — ETL v3.1 (`generate_flat_moh_706_v1`, watermark `…_v3.1`)

1. Refresh/guard `concept_set_map` (same pattern as `concept_value_map`).
2. **Sync queue (mandatory)**: member/head obs carry no `order_id` →
   ```sql
   REPLACE INTO flat_moh_706_report_sync_queue
       (SELECT DISTINCT person_id FROM amrs.obs
        WHERE date_created >= @last_update
          AND obs_group_id IS NOT NULL);
   ```
   Also queue person-day obs for the 3 panel concepts (encounter-level
   heads of concept 302/1010/1019 with no order): the existing
   `concept_id = 10253` branch extends to `OR concept_id IN (302,1010,1019)`
   — cheap, and it covers head-obs edits.
3. **Anchor column** on temp `flat_moh_706_order_0`:
   `anchor_obs_id = COALESCE(ro_obs_id, re_obs_id)` (ro almost never fires
   for panels; `ra` is never an anchor). Obs-only rows: anchor = own obs id.
4. **Member pass — two paths, one grouped scan per cycle:**
   ```sql
   -- Path G: children of the anchored group head (CBC, urinalysis)
   INSERT INTO flat_moh_706_members   -- (report_id, member_concept_id, MAX(obs_id))
   SELECT t.anchor_obs_id /* report key */, c.concept_id, MAX(c.obs_id)
   FROM flat_moh_706_order_0 t
   JOIN amrs.obs c ON c.obs_group_id = t.anchor_obs_id AND c.voided = 0
   GROUP BY 1, 2;
   -- (if nesting query returns rows: UNION a child-of-child level)

   -- Path U: ungrouped encounter-level analytes — GATED so it only fires
   -- for panel orders with NO group head (lipid/urinalysis recording
   -- pattern); near-zero rows for CBC.
   INSERT INTO flat_moh_706_members
   SELECT t.order_id, x.concept_id, MAX(x.obs_id)
   FROM flat_moh_706_order_0 t
   JOIN etl.concept_set_map cs ON cs.concept_id = t.concept_id
   JOIN amrs.obs x ON x.person_id = t.person_id AND x.voided = 0
                  AND x.concept_id = cs.concept_set
                  AND x.obs_group_id IS NULL
                  AND (x.encounter_id = t.encounter_id
                       OR (x.order_id IS NOT NULL AND x.order_id = t.order_id))
   WHERE t.anchor_obs_id IS NULL
   GROUP BY 1, 2;
   ```
   Caveat: an ungrouped analyte obs on a panel-order encounter may also
   legitimately exist as its own obs-only register row (today's behaviour
   for 1006/1008/1009); the member row is additive display data — counting
   stays on parent rows, per Phase 3.
   Then join the winning obs for the raw triple; write per-worker temp +
   REPLACE merge, mirroring the parent writes. (Path U refines the exact
   anti-overlap guard at implementation; the key is: never let one obs
   surface under two report rows.)
5. **Deletes cascade** to results (join on `report_id`) for both the
   person-queue delete and the voided-person sweep.
6. **Parent row unchanged.** Member concepts stay OUT of
   `flat_moh_706_lab_concepts` (no new standalone register rows).
7. **Accepted limitation (as today)**: repeat panels on one order →
   members come from the latest head only (`re`/`ro` are `MAX`-chosen).

### Phase 3 — Retrieval (`lab-706-base.json`, `lab-706-patient-list.json`)

- Pivot `flat_moh_706_report_results` into per-analyte columns per panel
  (order by `sort_weight`); translate coded members via
  `etl.concept_value_map`.
- Test counts stay `COUNT(DISTINCT report_id)` on the parent.
- **Counting decision (needs lab-report-owner sign-off):** urinalysis
  analytes count toward their own test rows (e.g. `2339` URINE PROTEIN
  total = standalone orders + member occurrences). v3.1 counts will
  legitimately rise vs v3.0 for those tests — more complete, not a bug;
  flag it in the release note.
- A panel is "done" when ≥ 1 member row exists; keep parent-triple display
  as fallback for v3.0-only periods.

### Performance on a multi-million-row amrs.obs

Everything is keyed to indexes; nothing scans the whole table:

1. **Heads-first everywhere.** The only obs-wide operation ever needed is
   `concept_id IN (302,1010,1019)` (concept_id index, ~25k rows). All
   obs↔obs movement is indexed probes on `obs_group_id` from those heads
   (or from per-cycle anchors), never a self-join scan.
2. **concept_set_map refresh**: two-step heads-first procedure (Phase 1);
   incremental runs add `AND date_created >= <last refresh>` (date_created
   index already used by generate_flat_lab_obs).
3. **Per-cycle ETL stays person-bounded.** The existing obs_latest pass
   already restricts obs to queued persons via the person_id index; the
   member passes inherit that bound (anchor probes + small temp tables).
   Optional extra cut: filter Path G's scan with `obs_group_id IS NOT NULL
   OR concept_id IN (<member universe>)`, or materialize one filtered
   cycle_obs slice per cycle and run both passes off it.
4. **Path U is gated on `anchor_obs_id IS NULL`** → fires only for
   group-less panel orders (lipid style), not for the 23k CBC groups.
5. **Sync windows are small** (`date_created >= watermark` on its index);
   the added `obs_group_id IS NOT NULL` / panel-head clauses filter within
   that range only.
6. **Migration uses build mode** (queue = amrs.person), NOT the v3.0-style
   watermark bump — a bumped first sync would range-scan the whole obs
   table by date_created. Build only ever hits obs via person_id.
7. **Index checklist (verify before build)**: `SHOW INDEX FROM amrs.obs`
   must show person_id, concept_id, order_id, obs_group_id, encounter_id,
   date_created. If obs_group_id is missing:
   `ALTER TABLE amrs.obs ADD INDEX idx_obs_group (obs_group_id),
   ALGORITHM=INPLACE, LOCK=NONE`. Keep cycle_size modest (≈1000 persons) so
   temp tables stay small.

### Phase 4 — Migration / validation / rollout

Install tables + refresh map → install v3.1 → **full `build`** (see
Performance §6) rather than a watermark bump. Validate:
- member rows per report vs grouped `amrs.obs` counts for sample orders;
- 10–20 CBC/urinalysis/lipid orders end-to-end against the printed report;
- non-panel aggregates identical pre/post v3.1;
- cycle-time on one build worker before parallel run.
Rollback: drop results table, reinstall v3.0 (parent schema untouched).

## Risks

- Empirical map drift is benign (stale members yield no rows); re-run
  `refresh_concept_set_map` alongside `concept_value_map`.
- Deep ObsTrees: flattened ≤ 2 levels pending the nesting query.
- Path U must not double-surface obs already captured via Path G — enforce
  the anti-overlap guard; covered by the per-order validation.
- Expect results-table volume ≈ 23k CBC × ~23 + misc ⇒ ~0.6–0.7M rows on
  build; PK `(report_id, member_concept_id)` keeps this cheap.
