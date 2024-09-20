-- DeobligationsRecoveriesRefundsOfPriorYearByProgramObjectClass_CPE = value for GTAS SF 133 lines #1021+1033 for the
-- same reporting period for the TAS and DEFC combination where PYA = "X".
WITH object_class_program_activity_b26_{0} AS
    (SELECT submission_id,
        SUM(COALESCE(deobligations_recov_by_pro_cpe, 0)) AS "deobligations_recov_by_pro_cpe_sum",
        tas,
        display_tas,
        disaster_emergency_fund_code
    FROM object_class_program_activity
    WHERE submission_id = {0}
        AND COALESCE(UPPER(prior_year_adjustment), '') = 'X'
    GROUP BY submission_id,
        tas,
        display_tas,
        disaster_emergency_fund_code)
SELECT
    NULL AS "row_number",
    op.deobligations_recov_by_pro_cpe_sum,
    SUM(sf.amount) AS "expected_value_SUM of GTAS SF133 Lines 1021, 1033",
    op.deobligations_recov_by_pro_cpe_sum - SUM(sf.amount) AS "difference",
    op.display_tas AS "uniqueid_TAS",
    UPPER(op.disaster_emergency_fund_code) AS "uniqueid_DisasterEmergencyFundCode"
FROM object_class_program_activity_b26_{0} AS op
    INNER JOIN sf_133 AS sf
        ON op.tas = sf.tas
        AND UPPER(op.disaster_emergency_fund_code) = COALESCE(sf.disaster_emergency_fund_code, '')
    INNER JOIN submission AS sub
        ON op.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE sf.line IN (1021, 1033)
GROUP BY op.display_tas,
    UPPER(op.disaster_emergency_fund_code),
    op.deobligations_recov_by_pro_cpe_sum
HAVING op.deobligations_recov_by_pro_cpe_sum <> SUM(sf.amount);
