-- UnobligatedBalance_CPE = CPE value for GTAS SF-133 line #2490
WITH appropriation_a15_{0} AS
    (SELECT submission_id,
        row_number,
        unobligated_balance_cpe,
        tas
    FROM appropriation
    WHERE submission_id = {0})
SELECT
    approp.row_number,
    approp.unobligated_balance_cpe,
    sf.amount AS "expected_value_GTAS SF133 Line 2490",
    approp.unobligated_balance_cpe - sf.amount AS "variance"
FROM appropriation_a15_{0} AS approp
    INNER JOIN sf_133 AS sf
        ON approp.tas = sf.tas
    INNER JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE sf.line = 2490
    AND approp.unobligated_balance_cpe <> sf.amount;
