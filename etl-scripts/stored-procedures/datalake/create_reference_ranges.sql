-- ---------------------------------------------------------------------------
-- Global reference ranges for the MOH 706 numeric indicators (v1, 2026-10-21)
--
-- One dictionary-level (global) range per analyte, consumed at request time by
-- lab-706-base.json v2.4 (<slot>_low / <slot>_high flags) and summed by
-- lab-706-aggregate.json v2.4.  NULL bounds disable that side (flag is 0), so
-- filling or correcting a range is a one-row UPDATE here - no template
-- redeploy, no rebuild (the register reads this table at request time).
--
-- Sourcing (owner decision 2026-10-21: "use international ranges, adjust
-- later"):
--   * Ranges the AMPATH dictionary (amrs.concept_numeric, fetched 2026-10-21)
--     carries and that looked sound are seeded verbatim, source-tagged
--     'amrs.concept_numeric 2026-10-21'.
--   * Everything else - concepts with no dictionary range, plus the two
--     dictionary rows that were structurally wrong (887 fasting range on a
--     random sugar; 1009 upper threshold stored as low_normal) and the wide
--     AST bound - uses international standard ranges (WHO / ADA / IFCC
--     reference intervals), source-tagged 'international v1'.  All are
--     provisional pending owner adjustment; only this table changes.
--
-- International values used (v1):
--   887   random plasma glucose 3.0-7.8 mmol/L (hypoglycaemia < 3.0 WHO;
--         normal random < 7.8 / 140 mg/dL)
--   653   AST 0-40 U/L (IFCC; sex-split 35/40 flattened to one global range)
--   1009  triglycerides < 1.7 mmol/L (150 mg/dL), no low bound
--   10627 OGTT (2-h post-load) 3.0-7.8 mmol/L (IGT starts at 7.8)
--   6124  CSF glucose 2.2-4.4 mmol/L (40-80 mg/dL)
--   1327  reticulocyte % 0.5-2.5 % (adult)
--   15092 ASO titre < 200 IU/mL (adult)
--   10249 PSA <= 4.0 ng/mL (age-adjusted ranges are a later refinement)
--   10250 CEA < 5.0 ng/mL (non-smoker; smokers < 10)
--   10251 CA 19-9 < 37 U/mL
--   10570 AFP < 10 ng/mL (some references use 15)
--
-- concept_numeric also carries critical bounds (K 2.0/6.5, Na 124/155,
-- cholesterol 6.2, ALT 220, ALP 541) - not consumed by v1 banding; noted for
-- a possible future *_critical pair.
--
-- Install:
--   mysql etl < create_reference_ranges.sql     (REPLACE => idempotent)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl.reference_ranges (
    concept_id INT        NOT NULL,
    low_value  DOUBLE     DEFAULT NULL,
    high_value DOUBLE     DEFAULT NULL,
    units      VARCHAR(20) DEFAULT NULL,
    source     VARCHAR(50) DEFAULT NULL,
    PRIMARY KEY (concept_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;

REPLACE INTO etl.reference_ranges (concept_id, low_value, high_value, units, source) VALUES
 -- verbatim from the AMPATH dictionary ------------------------------------------------
 (654,   0.0,  46.0, 'U/L',    'amrs.concept_numeric 2026-10-21'),
 (6132, 15.0,  60.0, 'mg/dL',  'amrs.concept_numeric 2026-10-21'),
 (7882,  0.8,   2.0, 'ng/dL',  'amrs.concept_numeric 2026-10-21'),
 (7881,  1.4,   4.2, 'pg/L',   'amrs.concept_numeric 2026-10-21'),
 (848,  28.0,  54.0, 'g/L',    'amrs.concept_numeric 2026-10-21'),
 (785,   0.0, 390.0, 'U/L',    'amrs.concept_numeric 2026-10-21'),
 (857,   0.0,  11.9, 'mmol/L', 'amrs.concept_numeric 2026-10-21'),
 (1134, 95.0, 110.0, 'mmol/L', 'amrs.concept_numeric 2026-10-21'),
 (790,   0.0, 106.0, 'umol/L', 'amrs.concept_numeric 2026-10-21'),
 (1297,  0.0,   3.4, 'umol/L', 'amrs.concept_numeric 2026-10-21'),
 (1008,  0.0,   4.0, 'mmol/L', 'amrs.concept_numeric 2026-10-21'),
 (1133,  3.3,   5.3, 'mmol/L', 'amrs.concept_numeric 2026-10-21'),
 (1132,132.0, 145.0, 'mmol/L', 'amrs.concept_numeric 2026-10-21'),
 (655,   0.0,  17.1, 'umol/L', 'amrs.concept_numeric 2026-10-21'),
 (1006,  0.0,   5.2, 'mmol/L', 'amrs.concept_numeric 2026-10-21'),
 (717,  60.0,  80.0, 'g/L',    'amrs.concept_numeric 2026-10-21'),
 (7880,  0.3,   4.0, 'mIU/L',  'amrs.concept_numeric 2026-10-21'),
 -- international standards (owner: "use international ranges, adjust later") ------------
 (887,   3.0,   7.8, 'mmol/L', 'international v1'),  -- was dictionary 2.0-5.5 (fasting range)
 (653,   0.0,  40.0, 'U/L',    'international v1'),  -- was dictionary 0-89 (wide)
 (1009,  NULL,  1.7, 'mmol/L', 'international v1'),  -- dictionary had low=3.7, no high (inverted)
 (10627, 3.0,   7.8, 'mmol/L', 'international v1'),
 (6124,  2.2,   4.4, 'mmol/L', 'international v1'),
 (1327,  0.5,   2.5, '%',      'international v1'),
 (15092, NULL, 200.0, 'IU/mL', 'international v1'),
 -- tumour markers: high-only cutoffs (plan D3)
 (10249, NULL,  4.0, 'ng/ML',  'international v1'),
 (10250, NULL,  5.0, 'ng/ML',  'international v1'),
 (10251, NULL, 37.0, 'U/mL',   'international v1'),
 (10570, NULL, 10.0, 'ng/ML',  'international v1');

SELECT COUNT(*) AS reference_ranges_rows FROM etl.reference_ranges;
