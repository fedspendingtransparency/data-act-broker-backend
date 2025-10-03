-- ObligationsIncurredByProgramObjectClass_CPE = the negative (additive inverse) value for GTAS SF 133 line #2190
-- for the same reporting period for the TAS and DEFC combination where PYA = "X".
WITH object_class_program_activity_b25_{0} AS
    (SELECT submission_id,
        row_number,
        obligations_incurred_by_pr_cpe,
        display_tas,
        disaster_emergency_fund_code,
        prior_year_adjustment
    FROM object_class_program_activity
    WHERE submission_id = {0}
        AND UPPER(prior_year_adjustment) = 'X')
SELECT
    NULL AS "row_number",
    UPPER(op.prior_year_adjustment) AS "prior_year_adjustment",
    SUM(COALESCE(op.obligations_incurred_by_pr_cpe, 0)) AS "obligations_incurred_by_pr_cpe_sum",
    sf.amount AS "expected_value_GTAS SF133 Line 2190",
    SUM(COALESCE(op.obligations_incurred_by_pr_cpe, 0)) + sf.amount AS "difference",
    op.display_tas AS "uniqueid_TAS",
    UPPER(op.disaster_emergency_fund_code) AS "uniqueid_DisasterEmergencyFundCode"
FROM object_class_program_activity_b25_{0} AS op
    INNER JOIN sf_133 AS sf
        ON op.display_tas = sf.display_tas
        AND UPPER(op.disaster_emergency_fund_code) = COALESCE(sf.disaster_emergency_fund_code, '')
    INNER JOIN submission AS sub
        ON op.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE sf.line = 2190
GROUP BY op.display_tas,
    UPPER(op.disaster_emergency_fund_code),
    sf.amount,
    UPPER(op.prior_year_adjustment)
HAVING SUM(COALESCE(op.obligations_incurred_by_pr_cpe, 0)) <> (-1 * sf.amount);
