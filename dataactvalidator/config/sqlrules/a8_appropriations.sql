-- BudgetAuthorityAppropriatedAmount_CPE = CPE aggregate value for GTAS SF-133 line #1160 + #1180 + #1260 + #1280
WITH appropriation_a8_{0} AS
    (SELECT row_number,
        budget_authority_appropria_cpe,
        tas,
        display_tas,
        submission_id
    FROM appropriation
    WHERE submission_id = {0})
SELECT
    approp.row_number,
    approp.budget_authority_appropria_cpe,
    SUM(sf.amount) AS "expected_value_SUM of GTAS SF133 Lines 1160, 1180, 1260, 1280",
    approp.budget_authority_appropria_cpe - SUM(sf.amount) AS "difference",
    approp.display_tas AS "uniqueid_TAS"
FROM appropriation_a8_{0} AS approp
    INNER JOIN sf_133 AS sf
        ON approp.tas = sf.tas
    INNER JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE sf.line IN (1160, 1180, 1260, 1280)
GROUP BY approp.row_number,
    approp.budget_authority_appropria_cpe,
    approp.display_tas
HAVING approp.budget_authority_appropria_cpe <> SUM(sf.amount);
