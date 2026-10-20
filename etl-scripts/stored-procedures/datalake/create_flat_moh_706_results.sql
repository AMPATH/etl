-- ---------------------------------------------------------------------------
-- Panel-test (multi-analyte) results support for the MOH lab register (v3.1)
--
-- Companion DDL to create_flat_moh_706.sql.  Adds:
--
--   etl.concept_set_map
--       Panel -> member concept map (1019 COMPLETE BLOOD COUNT -> 21
--       HEMOGLOBIN, ...).  amrs.concept_set proved materially incomplete in
--       this dictionary (4 rows covering 3 panels; the true ~46 analytes
--       absent), so the map is EMPIRICAL: refresh_concept_set_map() finds
--       group-head obs via the concept index and walks one obs_group_id hop
--       (indexed), then unions in whatever concept_set does know (for its
--       sort_weight).  Heads are scoped to the three reportable panels
--       (302, 1010, 1019): is_set alone matches thousands of concepts and
--       would drag every obs group in the database (heads plus children)
--       through the refresh (see the plan's Performance section).
--
--   etl.flat_moh_706_report_results
--       One row per report row x analyte (plus one self-row per plain test):
--       the raw member results the order-grain flat_moh_706_report cannot
--       hold.  Grains join on report_id (= flat_moh_706_report.report_id,
--       negative for obs-only legacy rows).  member_concept_id is the
--       analyte's concept_id; for a plain (non-panel) test it equals the
--       test concept.  result_obs_id is lineage back to amrs.obs (NULL for
--       self-rows, whose lineage is the parent row's resolved result).
--
-- Discovery (2026-10-20, see moh-706-panel-results-plan.md):
--   * Panels in the reportable concept list: 302 URINALYSIS, 1010 LIPID
--     PANEL, 1019 COMPLETE BLOOD COUNT (~7.3k orders; ~23k CBC obs groups,
--     so the obs-only legacy path dominates).
--   * Member/head obs essentially never carry order_id -> resolution
--     anchors on the group-head obs (ObsTree) found at encounter level.
--   * Q1: all 46 member concepts are leaf datatypes (Numeric/Coded/Text),
--     none set-like -> depth-2 trees; member pass is a single join.
--
-- Install (this file has DELIMITER blocks -- pipe it directly):
--   mysql etl < create_flat_moh_706_results.sql
--   mysql etl -e "CALL etl.refresh_concept_set_map(NULL);"
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl.concept_set_map (
    concept_id  INT    NOT NULL,               -- panel (302, 1010, 1019, ...)
    concept_set INT    NOT NULL,               -- member analyte (21, 678, ...)
    sort_weight DOUBLE DEFAULT NULL,           -- display order, when the
                                               -- dictionary knows one
    PRIMARY KEY (concept_id, concept_set),
    KEY concept_set_idx (concept_set)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;

CREATE TABLE IF NOT EXISTS etl.flat_moh_706_report_results (
    date_created      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                                     ON UPDATE CURRENT_TIMESTAMP,
    report_id         BIGINT NOT NULL,
    member_concept_id INT    NOT NULL,
    result_obs_id     BIGINT DEFAULT NULL,
    result_concept_id INT    DEFAULT NULL,
    result_numeric    DOUBLE DEFAULT NULL,
    result_text       TEXT,
    result_datetime   DATETIME DEFAULT NULL,
    obs_datetime      DATETIME DEFAULT NULL,
    PRIMARY KEY (report_id, member_concept_id),
    KEY member_concept (member_concept_id),
    KEY result_obs (result_obs_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;

DROP PROCEDURE IF EXISTS etl.refresh_concept_set_map;

DELIMITER $$
CREATE PROCEDURE etl.refresh_concept_set_map(IN since_datetime DATETIME)
BEGIN
    -- Panel -> member map, heads-first (never an obs<->obs self-join scan):
    --
    --   1. candidate group heads = obs of the three reportable panel
    --      concepts, found via the concept index (~25k rows, three index
    --      ranges -- is_set alone matches thousands of concepts and would
    --      pull every obs group in the database into the steps below);
    --   2. children reached by indexed obs_group_id probes;
    --   3. dictionary rows merged LAST so concept_set.sort_weight survives
    --      (empirical rows carry sort_weight NULL; REPLACE would lose it).
    --
    -- since_datetime NULL = full refresh (build-time; also clears the table
    -- so rows outside the panel scope never linger); pass the last run
    -- time for an incremental top-up.  Empirical membership is stable, so a
    -- weekly full refresh is fine and cheap.

    DROP TEMPORARY TABLE IF EXISTS panel_heads;
    CREATE TEMPORARY TABLE panel_heads (
        obs_id     BIGINT NOT NULL,
        concept_id INT    NOT NULL,
        PRIMARY KEY (obs_id),
        KEY concept_id (concept_id)
    );

    -- Reportable panels only (the set-flagged entries of
    -- flat_moh_706_lab_concepts in generate_flat_moh_706_v1; keep the lists
    -- in sync): every consumer of this map already intersects it with the
    -- lab concept list, so the scope costs nothing and keeps the head pass
    -- off the rest of amrs.obs.
    INSERT INTO panel_heads (obs_id, concept_id)
    SELECT o.obs_id, o.concept_id
    FROM amrs.obs o
    JOIN amrs.concept c
      ON c.concept_id = o.concept_id
     AND c.is_set = 1
     AND c.retired  = 0
     AND c.concept_id IN (302, 1010, 1019)
    WHERE o.voided = 0
      AND (since_datetime IS NULL OR o.date_created >= since_datetime);

    -- Full refresh rebuilds from scratch: REPLACE never deletes, so rows
    -- left by an older (wider) scope or dictionary change would linger
    -- forever and keep widening the sync-queue join that reads this map.
    IF since_datetime IS NULL THEN
        TRUNCATE TABLE etl.concept_set_map;
    END IF;

    -- Empirical edges: head concept -> every observed child concept.
    REPLACE INTO etl.concept_set_map (concept_id, concept_set, sort_weight)
    SELECT h.concept_id, ch.concept_id, NULL
    FROM panel_heads h
    JOIN amrs.obs ch ON ch.obs_group_id = h.obs_id AND ch.voided = 0
    GROUP BY h.concept_id, ch.concept_id;

    -- Dictionary edges last: fills gaps the empirical pass missed and keeps
    -- the real sort_weight where known.  Same panel scope as the head pass:
    -- unscoped, this would re-insert every set membership in the dictionary
    -- and re-widen the map the head pass just kept narrow.
    REPLACE INTO etl.concept_set_map (concept_id, concept_set, sort_weight)
    SELECT concept_id, concept_set, sort_weight
      FROM amrs.concept_set
     WHERE concept_id IN (302, 1010, 1019);

    DROP TEMPORARY TABLE panel_heads;

    SELECT COUNT(*) AS concept_set_map_rows FROM etl.concept_set_map;
END$$
DELIMITER ;
