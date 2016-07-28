SELECT
    approp.row_number,
    approp.budget_authority_unobligat_fyb,
    sf.amount
FROM appropriation as approp
    INNER JOIN sf_133 as sf ON approp.tas = sf.tas
WHERE approp.submission_id = {} AND
    sf.line = 1000 AND
    COALESCE(approp.budget_authority_unobligat_fyb, 0) <> sf.amount