-- The File C GrossOutlayByAward_CPE balance for a TAS/DEFC/Award combination should continue to be reported in
-- subsequent periods during the FY, once it has been submitted to DATA Act, unless the most recently reported outlay
-- balance for this award breakdown was zero. This only applies to File C outlays, not TOA.
WITH c27_prev_sub_{0} AS
    (SELECT CASE WHEN sub.is_quarter_format
            THEN sub_q.submission_id
            ELSE sub_p.submission_id
            END AS "submission_id"
    FROM submission AS sub
    LEFT JOIN submission AS sub_p
        ON sub_p.reporting_fiscal_year = sub.reporting_fiscal_year
        AND sub_p.reporting_fiscal_period = sub.reporting_fiscal_period - 1
        AND COALESCE(sub_p.cgac_code, sub_p.frec_code) = COALESCE(sub.cgac_code, sub.frec_code)
        AND sub_p.publish_status_id != 1
        AND sub_p.d2_submission IS FALSE
    LEFT JOIN submission AS sub_q
        ON sub_q.reporting_fiscal_year = sub.reporting_fiscal_year
        AND sub_q.reporting_fiscal_period = sub.reporting_fiscal_period - 3
        AND COALESCE(sub_q.cgac_code, sub_q.frec_code) = COALESCE(sub.cgac_code, sub.frec_code)
        AND sub_q.publish_status_id != 1
        AND sub_q.d2_submission IS FALSE
    WHERE sub.d2_submission IS FALSE
        AND sub.submission_id = {0}),
c27_prev_outlays_{0} AS (
    SELECT tas,
        disaster_emergency_fund_code,
        fain,
        uri,
        piid,
        parent_award_id,
        gross_outlay_amount_by_awa_cpe
    FROM certified_award_financial AS caf
    JOIN c27_prev_sub_{0} AS sub
        ON sub.submission_id = caf.submission_id
    WHERE COALESCE(gross_outlay_amount_by_awa_cpe, 0) <> 0)
SELECT
    NULL AS "row_number",
    po.tas,
    po.disaster_emergency_fund_code,
    po.fain,
    po.uri,
    po.piid,
    po.parent_award_id,
    po.gross_outlay_amount_by_awa_cpe,
    po.tas AS "uniqueid_TAS",
    po.disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode",
    po.fain AS "uniqueid_FAIN",
    po.uri AS "uniqueid_URI",
    po.piid AS "uniqueid_PIID",
    po.parent_award_id AS "uniqueid_ParentAwardId"
FROM c27_prev_outlays_{0} AS po
WHERE NOT EXISTS (
    SELECT 1
    FROM award_financial AS af
    WHERE UPPER(COALESCE(po.fain, '')) = UPPER(COALESCE(af.fain, ''))
        AND UPPER(COALESCE(po.uri, '')) = UPPER(COALESCE(af.uri, ''))
        AND UPPER(COALESCE(po.piid, '')) = UPPER(COALESCE(af.piid, ''))
        AND UPPER(COALESCE(po.parent_award_id, '')) = UPPER(COALESCE(af.parent_award_id, ''))
        AND UPPER(COALESCE(po.tas, '')) = UPPER(COALESCE(af.tas, ''))
        AND (UPPER(COALESCE(po.disaster_emergency_fund_code, '')) = UPPER(COALESCE(af.disaster_emergency_fund_code, ''))
            OR UPPER(COALESCE(po.disaster_emergency_fund_code, '')) = '9')
        AND af.submission_id = {0}
        AND af.gross_outlay_amount_by_awa_cpe IS NOT NULL
);
