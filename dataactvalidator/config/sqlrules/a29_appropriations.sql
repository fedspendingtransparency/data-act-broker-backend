-- DeobligationsRecoveriesRefundsByTAS_CPE = CPE aggregate value for GTAS SF-133 line 1021+1033
SELECT
    approp.row_number,
    approp.deobligations_recoveries_r_cpe,
    SUM(sf.amount) AS "expected_value_SUM of GTAS SF133 Lines 1021, 1033",
    approp.deobligations_recoveries_r_cpe - SUM(sf.amount) AS "variance"
FROM appropriation AS approp
    INNER JOIN sf_133 AS sf
        ON approp.tas = sf.tas
    INNER JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE approp.submission_id = {0}
    AND sf.line IN (1021, 1033)
GROUP BY approp.row_number,
    approp.deobligations_recoveries_r_cpe
HAVING approp.deobligations_recoveries_r_cpe <> SUM(sf.amount);
