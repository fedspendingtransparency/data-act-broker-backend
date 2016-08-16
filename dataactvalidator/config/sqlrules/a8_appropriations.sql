SELECT
    approp.row_number,
    approp.budget_authority_appropria_cpe,
    SUM(sf.amount) as sf_133_amount_sum
FROM appropriation as approp
    INNER JOIN sf_133 as sf ON approp.tas = sf.tas
    INNER JOIN submission as sub ON approp.submission_id = sub.submission_id AND
        sf.period = sub.reporting_fiscal_period AND
        sf.fiscal_year = sub.reporting_fiscal_year
WHERE approp.submission_id = {} AND
    sf.line in (1160, 1180, 1260, 1280)
GROUP BY approp.row_number, approp.budget_authority_appropria_cpe
HAVING approp.budget_authority_appropria_cpe <> SUM(sf.amount)