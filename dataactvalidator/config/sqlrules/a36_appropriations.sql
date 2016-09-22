SELECT
    approp.row_number,
    approp.budget_authority_unobligat_fyb,
    sf.amount as sf_133_amount
FROM appropriation as approp
    INNER JOIN sf_133 as sf ON approp.tas = sf.tas
    INNER JOIN submission as sub ON approp.submission_id = sub.submission_id AND
        sf.period = sub.reporting_fiscal_period AND
        sf.fiscal_year = sub.reporting_fiscal_year
WHERE approp.submission_id = {} AND
    sf.line = 1000 AND
    sf.amount <> 0 AND
    approp.budget_authority_unobligat_fyb IS NULL