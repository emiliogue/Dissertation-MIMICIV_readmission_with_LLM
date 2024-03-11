--WITH count_query as(

WITH admissions AS (SELECT hadm_id, admission_type, race, insurance
                    FROM `physionet-data.mimiciv_hosp.admissions`
                    WHERE hadm_id IN (SELECT hadm_id FROM `physionet-data.mimiciv_icu.icustays`)
                      AND admission_type IN
                          ('AMBULATORY OBSERVATION', 'DIRECT EMER.', 'URGENT', 'EW EMER.', 'DIRECT OBSERVATION',
                           'EU OBSERVATION', 'OBSERVATION ADMIT')),

     Emergency_ICU_Stays AS (SELECT i.stay_id,
                                    i.hadm_id,
                                    i.subject_id,
                                    i.intime,
                                    i.outtime,
                                    i.first_careunit,
                                    i.last_careunit,
                                    i.los,
                             FROM `physionet-data.mimiciv_icu.icustays` AS i
                             WHERE hadm_id IN (SELECT hadm_id FROM admissions)),

     ICU_Stays AS (SELECT i.stay_id,
                          i.hadm_id,
                          i.subject_id,
                          i.intime,
                          i.outtime,
                          i.first_careunit,
                          i.last_careunit,
                          i.los,
                          LEAD(i.intime) OVER (PARTITION BY i.subject_id ORDER BY i.intime)   AS next_intime,
                          LAG(i.outtime) OVER (PARTITION BY i.subject_id ORDER BY i.intime)   AS prev_outtime,
                          ROW_NUMBER() OVER (PARTITION BY i.subject_id ORDER BY i.intime) - 1 AS prior_admissions
                   FROM Emergency_ICU_Stays AS i),

     ICU_Readmission_Status AS (SELECT stay_id,
                                       hadm_id,
                                       subject_id,
                                       intime,
                                       outtime,
                                       first_careunit,
                                       last_careunit,
                                       los,
                                       TIMESTAMP_DIFF(next_intime, outtime, DAY)                         AS days_between_admissions,
                                       TIMESTAMP_DIFF(intime, prev_outtime, DAY)                         AS days_since_last_admission,
                                       CASE
                                           WHEN TIMESTAMP_DIFF(next_intime, outtime, DAY) < 30 THEN 1
                                           ELSE 0
                                           END                                                           AS readmitted_next,
                                       CASE
                                           WHEN TIMESTAMP_DIFF(intime, prev_outtime, DAY) < 30 THEN 1
                                           ELSE 0
                                           END                                                           AS is_readmission,
                                       prior_admissions,
                                       ROW_NUMBER() OVER (PARTITION BY subject_id ORDER BY outtime DESC) AS rn
                                FROM ICU_Stays),


     ranked_charts AS (SELECT d.text,
                              d.charttime,
                              d.subject_id,
                              d.note_id,
                              s.stay_id,
                              s.hadm_id,
                              s.intime,
                              RANK() OVER (PARTITION BY s.stay_id ORDER BY d.charttime DESC) AS rank
                       FROM `physionet-data.mimiciv_note.discharge` d
                                FULL OUTER JOIN
                            `physionet-data.mimiciv_icu.icustays` s
                            ON
                                s.subject_id = d.subject_id
                       WHERE d.charttime < s.intime),
     discharge AS (SELECT r.stay_id,
                          r.subject_id,
                          r.hadm_id,
                          r.note_id,
                          r.intime,
                          r.charttime,
                          r.text
                   FROM ranked_charts r
                   WHERE r.rank = 1),

     Vital_Signs_Aggregated AS (SELECT stay_id,
                                       AVG(heart_rate)  AS avg_heart_rate,
                                       MIN(heart_rate)  AS min_heart_rate,
                                       MAX(heart_rate)  AS max_heart_rate,

                                       AVG(sbp)         AS avg_sbp,
                                       MIN(sbp)         AS min_sbp,
                                       MAX(sbp)         AS max_sbp,

                                       AVG(dbp)         AS avg_dbp,
                                       MIN(dbp)         AS min_dbp,
                                       MAX(dbp)         AS max_dbp,

                                       AVG(mbp)         AS avg_mbp,
                                       MIN(mbp)         AS min_mbp,
                                       MAX(mbp)         AS max_mbp,

                                       AVG(sbp_ni)      AS avg_sbp_ni,
                                       MIN(sbp_ni)      AS min_sbp_ni,
                                       MAX(sbp_ni)      AS max_sbp_ni,

                                       AVG(dbp_ni)      AS avg_dbp_ni,
                                       MIN(dbp_ni)      AS min_dbp_ni,
                                       MAX(dbp_ni)      AS max_dbp_ni,

                                       AVG(mbp_ni)      AS avg_mbp_ni,
                                       MIN(mbp_ni)      AS min_mbp_ni,
                                       MAX(mbp_ni)      AS max_mbp_ni,

                                       AVG(resp_rate)   AS avg_resp_rate,
                                       MIN(resp_rate)   AS min_resp_rate,
                                       MAX(resp_rate)   AS max_resp_rate,

                                       AVG(temperature) AS avg_temperature,
                                       MIN(temperature) AS min_temperature,
                                       MAX(temperature) AS max_temperature,

                                       AVG(spo2)        AS avg_spo2,
                                       MIN(spo2)        AS min_spo2,
                                       MAX(spo2)        AS max_spo2,

                                       AVG(glucose)     AS avg_glucose,
                                       MIN(glucose)     AS min_glucose,
                                       MAX(glucose)     AS max_glucose
                                FROM `physionet-data.mimiciv_derived.vitalsign`
                                WHERE stay_id IN (SELECT stay_id FROM ICU_Stays)
                                GROUP BY stay_id),

     Oasis_Scores AS (SELECT stay_id,
                             AVG(oasis) AS oasis
                      FROM `physionet-data.mimiciv_derived.oasis`
                      WHERE stay_id IN (SELECT stay_id FROM ICU_Stays)
                      GROUP BY stay_id),

     sofa AS (SELECT stay_id,
                     AVG(sofa_24hours) AS sofa
              FROM `physionet-data.mimiciv_derived.sofa`
              WHERE stay_id IN (SELECT stay_id FROM ICU_Stays)
              GROUP BY stay_id),

     gcs AS (SELECT stay_id,
                    AVG(gcs) AS gcs
             FROM `physionet-data.mimiciv_derived.gcs`
             WHERE stay_id IN (SELECT stay_id FROM ICU_Stays)
             GROUP BY stay_id),

     apsiii AS (SELECT stay_id,
                       AVG(apsiii) AS apsiii
                FROM `physionet-data.mimiciv_derived.apsiii`
                WHERE stay_id IN (SELECT stay_id FROM ICU_Stays)
                GROUP BY stay_id),

     lods AS (SELECT stay_id,
                     AVG(lods) AS lods
              FROM `physionet-data.mimiciv_derived.lods`
              WHERE stay_id IN (SELECT stay_id FROM ICU_Stays)
              GROUP BY stay_id),

     kdigo_stages AS (SELECT stay_id,
                             AVG(aki_stage_smoothed) AS aki_stage
                      FROM `physionet-data.mimiciv_derived.kdigo_stages`
                      WHERE stay_id IN (SELECT stay_id FROM ICU_Stays)
                      GROUP BY stay_id),

     charlson AS (SELECT hadm_id,
                         charlson_comorbidity_index AS charlson
                  FROM `physionet-data.mimiciv_derived.charlson`),
     scores AS (SELECT o.stay_id,
                       oasis,
                       sofa,
                       gcs,
                       apsiii,
                       lods,
                       aki_stage
                FROM Oasis_Scores AS o
                         LEFT JOIN
                     sofa AS s
                     ON o.stay_id = s.stay_id
                         LEFT JOIN
                     gcs AS g
                     ON o.stay_id = g.stay_id
                         LEFT JOIN
                     apsiii AS a
                     ON o.stay_id = a.stay_id
                         LEFT JOIN
                     lods AS l
                     ON o.stay_id = l.stay_id
                         LEFT JOIN
                     kdigo_stages AS k
                     ON o.stay_id = k.stay_id),

     lab AS (SELECT stay_id,
                    (hemoglobin_min + hemoglobin_max) / 2                 AS hemoglobin,
                    (platelets_min + platelets_max) / 2                   AS platelets,
                    (wbc_min + wbc_max) / 2                               AS wbc,
                    (albumin_min + albumin_max) / 2                       AS albumin,
                    (aniongap_min + aniongap_max) / 2                     AS aniongap,
                    (bicarbonate_min + bicarbonate_max) / 2               AS bicarbonate,
                    (bun_min + bun_max) / 2                               AS bun,
                    (calcium_min + calcium_max) / 2                       AS calcium,
                    (chloride_min + chloride_max) / 2                     AS chloride,
                    (creatinine_min + creatinine_max) / 2                 AS creatinine,
                    (glucose_min + glucose_max) / 2                       AS glucose,
                    (sodium_min + sodium_max) / 2                         AS sodium,
                    (potassium_min + potassium_max) / 2                   AS potassium,
                    (d_dimer_min + d_dimer_max) / 2                       AS d_dimer,
                    (inr_min + inr_max) / 2                               AS inr,
                    (alt_min + alt_max) / 2                               AS alt,
                    (alp_min + alp_max) / 2                               AS alp,
                    (ast_min + ast_max) / 2                               AS ast,
                    (amylase_min + amylase_max) / 2                       AS amylase,
                    (bilirubin_total_min + bilirubin_total_max) / 2       AS bilirubin_total,
                    (bilirubin_direct_min + bilirubin_direct_max) / 2     AS bilirubin_direct,
                    (bilirubin_indirect_min + bilirubin_indirect_max) / 2 AS bilirubin_indirect,
                    (ck_cpk_min + ck_cpk_max) / 2                         AS ck_cpk,
                    (ck_mb_min + ck_mb_max) / 2                           AS ck_mb,
                    (ggt_min + ggt_max) / 2                               AS ggt
             FROM `physionet-data.mimiciv_derived.first_day_lab`
             WHERE stay_id IN (SELECT stay_id FROM ICU_Stays)),

     bmi AS (SELECT w.stay_id, w.weight, h.height, (weight / (POW((height / 100), 2))) AS bmi
             FROM `physionet-data.mimiciv_derived.first_day_weight` AS w
                      JOIN
                  `physionet-data.mimiciv_derived.first_day_height` AS h
                  ON
                      w.stay_id = h.stay_id),

     Vital_Signs_Last_Available AS (WITH LastNonNullValues AS (SELECT stay_id,
                                                                      LAST_VALUE(heart_rate IGNORE NULLS)
                                                                                 OVER (PARTITION BY stay_id ORDER BY charttime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_heart_rate,
                                                                      LAST_VALUE(sbp IGNORE NULLS)
                                                                                 OVER (PARTITION BY stay_id ORDER BY charttime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_sbp,
                                                                      LAST_VALUE(dbp IGNORE NULLS)
                                                                                 OVER (PARTITION BY stay_id ORDER BY charttime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_dbp,
                                                                      LAST_VALUE(mbp IGNORE NULLS)
                                                                                 OVER (PARTITION BY stay_id ORDER BY charttime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_mbp,
                                                                      LAST_VALUE(sbp_ni IGNORE NULLS)
                                                                                 OVER (PARTITION BY stay_id ORDER BY charttime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_sbp_ni,
                                                                      LAST_VALUE(dbp_ni IGNORE NULLS)
                                                                                 OVER (PARTITION BY stay_id ORDER BY charttime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_dbp_ni,
                                                                      LAST_VALUE(mbp_ni IGNORE NULLS)
                                                                                 OVER (PARTITION BY stay_id ORDER BY charttime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_mbp_ni,
                                                                      LAST_VALUE(resp_rate IGNORE NULLS)
                                                                                 OVER (PARTITION BY stay_id ORDER BY charttime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_resp_rate,
                                                                      LAST_VALUE(temperature IGNORE NULLS)
                                                                                 OVER (PARTITION BY stay_id ORDER BY charttime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_temperature,
                                                                      LAST_VALUE(spo2 IGNORE NULLS)
                                                                                 OVER (PARTITION BY stay_id ORDER BY charttime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_spo2,
                                                                      LAST_VALUE(glucose IGNORE NULLS)
                                                                                 OVER (PARTITION BY stay_id ORDER BY charttime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_glucose
                                                               FROM `physionet-data.mimiciv_derived.vitalsign`
                                                               WHERE stay_id IN (SELECT stay_id FROM ICU_Stays))


                                    SELECT DISTINCT stay_id,
                                                    last_heart_rate,
                                                    last_sbp,
                                                    last_dbp,
                                                    last_mbp,
                                                    last_sbp_ni,
                                                    last_dbp_ni,
                                                    last_mbp_ni,
                                                    last_resp_rate,
                                                    last_temperature,
                                                    last_spo2,
                                                    last_glucose
                                    FROM LastNonNullValues)
SELECT DISTINCT d.hadm_id,
                a.admission_type,
                d.subject_id,
                d.charttime,
                rs.outtime,
                d.note_id,
                rs.stay_id,
                p.anchor_age AS age,
                p.gender,
                d.text,
                rs.los,
                rs.readmitted_next,
                rs.is_readmission,
                rs.days_between_admissions,
                rs.days_since_last_admission,
                rs.prior_admissions,
                b.weight,
                b.height,
                b.bmi,
                a.race,
                a.insurance,
                vs.avg_heart_rate,
                vs.avg_sbp,
                vs.avg_dbp,
                vs.avg_mbp,
                vs.avg_sbp_ni,
                vs.avg_dbp_ni,
                vs.avg_mbp_ni,
                vs.avg_resp_rate,
                vs.avg_temperature,
                vs.avg_spo2,
                vs.avg_glucose,
                vs.max_heart_rate,
                vs.max_sbp,
                vs.max_dbp,
                vs.max_mbp,
                vs.max_sbp_ni,
                vs.max_dbp_ni,
                vs.max_mbp_ni,
                vs.max_resp_rate,
                vs.max_temperature,
                vs.max_spo2,
                vs.max_glucose,
                vs.min_heart_rate,
                vs.min_sbp,
                vs.min_dbp,
                vs.min_mbp,
                vs.min_sbp_ni,
                vs.min_dbp_ni,
                vs.min_mbp_ni,
                vs.min_resp_rate,
                vs.min_temperature,
                vs.min_spo2,
                vs.min_glucose,
                lvs.last_heart_rate,
                lvs.last_sbp,
                lvs.last_dbp,
                lvs.last_mbp,
                lvs.last_sbp_ni,
                lvs.last_dbp_ni,
                lvs.last_mbp_ni,
                lvs.last_resp_rate,
                lvs.last_temperature,
                lvs.last_spo2,
                lvs.last_glucose,
                lab.hemoglobin,
                lab.platelets,
                lab.wbc,
                lab.albumin,
                lab.aniongap,
                lab.bicarbonate,
                lab.bun,
                lab.calcium,
                lab.chloride,
                lab.creatinine,
                lab.glucose,
                lab.sodium,
                lab.potassium,
                lab.d_dimer,
                lab.inr,
                lab.alt,
                lab.alp,
                lab.ast,
                lab.amylase,
                lab.bilirubin_total,
                lab.bilirubin_direct,
                lab.bilirubin_indirect,
                lab.ck_cpk,
                lab.ck_mb,
                lab.ggt,
                s.oasis,
                s.sofa,
                s.gcs,
                s.apsiii,
                s.lods,
                s.aki_stage,
                c.charlson
FROM
    --discharge AS d
    `physionet-data.mimiciv_note.discharge` AS d
        JOIN
    ICU_Readmission_Status AS rs
    ON
        d.hadm_id = rs.hadm_id AND d.subject_id = rs.subject_id
        JOIN
    `physionet-data.mimiciv_hosp.patients` AS p
    ON
        d.subject_id = p.subject_id
        JOIN
    Vital_Signs_Aggregated AS vs
    ON
        rs.stay_id = vs.stay_id
        JOIN
    Vital_Signs_Last_Available AS lvs
    ON
        rs.stay_id = lvs.stay_id
        JOIN
    lab AS lab
    ON
        rs.stay_id = lab.stay_id
        JOIN
    scores AS s
    ON
        rs.stay_id = s.stay_id
        JOIN
    charlson AS c
    ON
        d.hadm_id = c.hadm_id
        JOIN
    admissions AS a
    ON
        d.hadm_id = a.hadm_id
        JOIN
    bmi AS b
    ON
        rs.stay_id = b.stay_id
WHERE (p.dod IS NULL OR p.dod >= rs.outtime)                       ---------------Exclude death in ward
  AND (p.dod IS NULL OR
       (TIMESTAMP_DIFF(p.dod, rs.outtime, DAY) > 30 OR rs.rn > 1)) ------------- Exclude the last admission if followed by a death in under 30 days
  AND readmitted_next = 0
--AND d.subject_id = 19998330
--AND (d.hadm_id IN(SELECT
--hadm_id
--  FROM
--    `physionet-data.mimiciv_hosp.transfers`
-- WHERE careunit = 'Medical Intensive Care Unit (MICU)'))
--AND (rs.stay_id IN(SELECT
--stay_id
--FROM
--discharge
--))
--)
--SELECT COUNT(DISTINCT subject_id) FROM count_query
;