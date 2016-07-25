SELECT
    ap.row_number,
    ap.budget_authority_appropria_cpe,
    SUM(sf.amount) as total_amount
FROM appropriation as ap
    INNER JOIN sf_133 as sf ON ap.tas = sf.tas
WHERE ap.submission_id = {} AND
    sf.line in (1160, 1180, 1260, 1280)
GROUP BY ap.row_number, ap.budget_authority_appropria_cpe
HAVING COALESCE(ap.budget_authority_appropria_cpe, 0) <> SUM(sf.amount)