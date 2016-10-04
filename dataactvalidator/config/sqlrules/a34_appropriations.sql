SELECT
    approp.row_number,
    approp.budget_authority_unobligat_fyb,
    sf.amount as sf_133_amount
FROM appropriation as approp
    JOIN sf_133 as sf ON approp.tas = sf.tas
    JOIN submission as sub ON approp.submission_id = sub.submission_id AND
        sf.period = 12 AND
        sf.fiscal_year = (sub.reporting_fiscal_year - 1)
WHERE approp.submission_id = {} AND
    sf.line = 2490 AND
    approp.budget_authority_unobligat_fyb <> sf.amount