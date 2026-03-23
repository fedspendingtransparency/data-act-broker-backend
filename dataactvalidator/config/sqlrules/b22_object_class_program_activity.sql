-- GrossOutlayAmountByProgramObjectClass_CPE = value for GTAS SF 133 line #3020 for the same reporting period for the
-- TAS and DEFC combination where PYA = "X".
WITH object_class_program_activity_b22_{0} AS
    (SELECT submission_id,
        SUM(COALESCE(gross_outlay_amount_by_pro_cpe, 0)) AS "gross_outlay_amount_by_pro_cpe_sum",
        UPPER(display_tas) AS "display_tas",
        UPPER(disaster_emergency_fund_code) AS "disaster_emergency_fund_code",
        UPPER(prior_year_adjustment) AS "prior_year_adjustment"
    FROM object_class_program_activity
    WHERE submission_id = {0}
        AND UPPER(prior_year_adjustment) = 'X'
    GROUP BY submission_id,
        UPPER(display_tas),
        UPPER(disaster_emergency_fund_code),
        UPPER(prior_year_adjustment))
SELECT
    NULL AS "row_number",
    UPPER(op.prior_year_adjustment) AS "prior_year_adjustment",
    op.gross_outlay_amount_by_pro_cpe_sum AS "gross_outlay_amount_by_pro_cpe_sum",
    SUM(COALESCE(sf.amount, 0)) AS "expected_value_GTAS SF133 Line 3020",
    op.gross_outlay_amount_by_pro_cpe_sum - SUM(COALESCE(sf.amount, 0)) AS "difference",
    op.display_tas AS "uniqueid_TAS",
    UPPER(op.disaster_emergency_fund_code) AS "uniqueid_DisasterEmergencyFundCode"
FROM object_class_program_activity_b22_{0} AS op
    INNER JOIN submission AS sub
        ON op.submission_id = sub.submission_id
    LEFT OUTER JOIN sf_133 AS sf
        ON op.display_tas = UPPER(sf.display_tas)
        AND UPPER(op.disaster_emergency_fund_code) = UPPER(COALESCE(sf.disaster_emergency_fund_code, ''))
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
        AND sf.line = 3020
GROUP BY op.display_tas,
    op.gross_outlay_amount_by_pro_cpe_sum,
    UPPER(op.disaster_emergency_fund_code),
    UPPER(op.prior_year_adjustment)
HAVING op.gross_outlay_amount_by_pro_cpe_sum <> SUM(COALESCE(sf.amount, 0));
