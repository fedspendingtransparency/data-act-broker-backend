SELECT
    ap.row_number,
    ap.budget_authority_unobligat_fyb,
    sf.amount
FROM appropriation as ap
    INNER JOIN sf_133 as sf ON ap.tas = sf.tas
WHERE ap.submission_id = {} AND
    sf.line = 1000 AND
    COALESCE(ap.budget_authority_unobligat_fyb, 0) <> sf.amount