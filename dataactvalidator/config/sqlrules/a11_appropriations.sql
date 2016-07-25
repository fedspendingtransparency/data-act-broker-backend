SELECT
    ap.row_number,
    ap.spending_authority_from_of_cpe,
    SUM(sf.amount) as total_amount
FROM appropriation as ap
    INNER JOIN sf_133 as sf ON ap.tas = sf.tas
WHERE ap.submission_id = {} AND
    sf.line in (1750, 1850)
GROUP BY ap.row_number, ap.spending_authority_from_of_cpe
HAVING COALESCE(ap.spending_authority_from_of_cpe, 0) <> SUM(sf.amount)