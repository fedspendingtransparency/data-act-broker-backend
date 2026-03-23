-- UnobligatedBalance_CPE = CPE value for GTAS SF-133 line #2490
WITH appropriation_a15_{0} AS
    (SELECT submission_id,
        row_number,
        unobligated_balance_cpe,
        display_tas
    FROM appropriation
    WHERE submission_id = {0})
SELECT
    approp.row_number,
    approp.unobligated_balance_cpe,
    SUM(COALESCE(sf.amount, 0)) AS "expected_value_GTAS SF133 Line 2490",
    approp.unobligated_balance_cpe - SUM(COALESCE(sf.amount, 0)) AS "difference",
    approp.display_tas AS "uniqueid_TAS"
FROM appropriation_a15_{0} AS approp
    INNER JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
    LEFT OUTER JOIN sf_133 AS sf
        ON approp.display_tas = sf.display_tas
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
        AND sf.line = 2490
GROUP BY approp.row_number,
    approp.unobligated_balance_cpe,
    approp.display_tas
HAVING approp.unobligated_balance_cpe <> SUM(COALESCE(sf.amount, 0));
