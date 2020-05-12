-- GrossOutlayAmountByProgramObjectClass_CPE = value for GTAS SF 133 line #3020 for the same reporting period for the
-- TAS and DEFC combination (except for when '9' is provided as DEFC).
WITH object_class_program_activity_b22_{0} AS
    (SELECT submission_id,
        row_number,
        gross_outlay_amount_by_pro_cpe,
        tas,
        display_tas,
        disaster_emergency_fund_code
    FROM object_class_program_activity
    WHERE submission_id = {0}
        AND UPPER(disaster_emergency_fund_code) <> '9')
SELECT
    op.row_number,
    op.gross_outlay_amount_by_pro_cpe,
    sf.amount AS "expected_value_GTAS SF133 Line 3020",
    COALESCE(op.gross_outlay_amount_by_pro_cpe, 0) - sf.amount AS "difference",
    op.display_tas AS "uniqueid_TAS",
    op.disaster_emergency_fund_code AS "uniqueid_DisasterEmergencyFundCode"
FROM object_class_program_activity_b22_{0} AS op
    INNER JOIN sf_133 AS sf
        ON op.tas = sf.tas
        AND UPPER(op.disaster_emergency_fund_code) = sf.disaster_emergency_fund_code
    INNER JOIN submission AS sub
        ON op.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE sf.line = 3020
    AND COALESCE(op.gross_outlay_amount_by_pro_cpe, 0) <> sf.amount;
