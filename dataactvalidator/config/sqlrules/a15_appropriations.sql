SELECT
    ap.row_number,
    ap.unobligated_balance_cpe,
    sf.amount
FROM appropriation as ap
    INNER JOIN sf_133 as sf ON (ap.tas = sf.tas)
WHERE ap.submission_id = {} AND
    sf.line = 2490 AND
    COALESCE(ap.unobligated_balance_cpe, 0) <> sf.amount