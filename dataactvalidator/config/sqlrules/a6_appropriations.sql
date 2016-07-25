SELECT
    ap.row_number,
    ap.budget_authority_available_cpe,
    sf.amount
FROM ap.appropriation as ap
    INNER JOIN sf_133 as sf ON (ap.tas = sf.tas)
WHERE submission_id = {} AND
    sf.line = 1910 AND
    COALESCE(ap.budget_authority_available_cpe, 0) <> sf.amount