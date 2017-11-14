-- BudgetAuthorityAppropriatedAmount_CPE = CPE aggregate value for GTAS SF 133 line #1160 + #1180 + #1260 + #1280
WITH appropriation_a8_{0} AS
	(SELECT row_number,
		budget_authority_appropria_cpe,
		tas,
		submission_id
	FROM appropriation
    WHERE submission_id = {0})
SELECT
    approp.row_number,
    approp.budget_authority_appropria_cpe,
    SUM(sf.amount) AS sf_133_amount_sum
FROM appropriation_a8_{0} as approp
    INNER JOIN sf_133 AS sf
        ON approp.tas = sf.tas
    INNER JOIN submission AS sub
        ON approp.submission_id = sub.submission_id
        AND sf.period = sub.reporting_fiscal_period
        AND sf.fiscal_year = sub.reporting_fiscal_year
WHERE sf.line IN (1160, 1180, 1260, 1280)
GROUP BY approp.row_number,
    approp.budget_authority_appropria_cpe
HAVING approp.budget_authority_appropria_cpe <> SUM(sf.amount);
