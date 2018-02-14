replace into etl.flat_hiv_summary_queue (SELECT
    DISTINCT fo.person_id
FROM
    etl.flat_obs fo left outer join etl.flat_hiv_summary hs on fo.person_id = hs.person_id
WHERE
    fo.encounter_type IN (1 , 2,
        3,
        4,
        10,
        14,
        15,
        17,
        19,
        26,
        32,
        33,
        34,
        47,
        105,
        106,
        112,
        113,
        114,
        117,
        120,
        127,
        128,
        129,
        138,
        153,
        154,
        158) and hs.person_id is null limit 300);