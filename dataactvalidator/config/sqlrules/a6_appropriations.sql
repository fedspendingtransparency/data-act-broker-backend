WITH appropriation_a6 AS 
	(SELECT submission_id,
		row_number,
		budget_authority_available_cpe,
		tas
	FROM appropriation)
SELECT
    approp.row_number,
    approp.budget_authority_available_cpe,
    sf.amount as sf_133_amount
FROM appropriation_a6 as approp
    INNER JOIN sf_133 as sf ON approp.tas = sf.tas
    INNER JOIN submission as sub ON approp.submission_id = sub.submission_id AND
        sf.period = sub.reporting_fiscal_period AND
        sf.fiscal_year = sub.reporting_fiscal_year
WHERE approp.submission_id = {} AND
    sf.line = 1910 AND
    approp.budget_authority_available_cpe <> sf.amount