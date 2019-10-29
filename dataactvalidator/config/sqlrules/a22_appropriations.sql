-- ObligationsIncurredTotalByTAS_CPE = CPE value for GTAS SF-133 line #2190
SELECT
    approp.row_number,
    approp.obligations_incurred_total_cpe,
    sf.amount AS "expected_value_GTAS SF133 Line 2190",
    approp.obligations_incurred_total_cpe - sf.amount AS "difference",
    approp.tas AS "uniqueid_TAS"
FROM appropriation AS approp
    INNER JOIN sf_133 AS sf
        ON approp.tas = sf.tas
    INNER JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE approp.submission_id = {0}
    AND sf.line = 2190
    AND approp.obligations_incurred_total_cpe <> sf.amount;
