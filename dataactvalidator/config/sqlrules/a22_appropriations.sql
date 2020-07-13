-- ObligationsIncurredTotalByTAS_CPE = CPE value for GTAS SF-133 line #2190
SELECT
    approp.row_number,
    approp.obligations_incurred_total_cpe,
    SUM(sf.amount) AS "expected_value_GTAS SF133 Line 2190",
    approp.obligations_incurred_total_cpe - SUM(sf.amount) AS "difference",
    approp.display_tas AS "uniqueid_TAS"
FROM appropriation AS approp
    INNER JOIN sf_133 AS sf
        ON approp.tas = sf.tas
    INNER JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE approp.submission_id = {0}
    AND sf.line = 2190
GROUP BY approp.row_number,
    approp.obligations_incurred_total_cpe,
    approp.display_tas
HAVING approp.obligations_incurred_total_cpe <> SUM(sf.amount);
